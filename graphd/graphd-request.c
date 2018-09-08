/*
Copyright 2015 Google Inc. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#include "graphd/graphd.h"

#include <errno.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

static bool graphd_request_is_netlogged(graphd_request const *const greq) {
  return greq != NULL && greq->greq_request != GRAPHD_REQUEST_WRITETHROUGH;
}

/*  Clear a reference-counted request pointer.
 */
void graphd_request_unlink_pointer(graphd_request **req) {
  if (*req != NULL) {
    graphd_request *r = *req;

    *req = NULL;
    srv_request_unlink(&r->greq_req);
  }
}

void graphd_request_link_pointer(graphd_request *val, graphd_request **loc) {
  if (val != NULL) srv_request_link(&val->greq_req);

  if (*loc != NULL) graphd_request_unlink_pointer(loc);

  *loc = val;
}

static char const *graphd_request_name(graphd_request const *const greq) {
  static char error_buf[200];

  if (greq == NULL) return "null";

  if (greq->greq_type != NULL && greq->greq_type->grt_name != NULL)
    return greq->greq_type->grt_name;

  switch (greq->greq_request) {
    case GRAPHD_REQUEST_UNSPECIFIED:
      return "unspecified";
    case GRAPHD_REQUEST_CRASH:
      return "crash";
    case GRAPHD_REQUEST_DUMP:
      return "dump";
    case GRAPHD_REQUEST_ERROR:
      return "error";
    case GRAPHD_REQUEST_ITERATE:
      return "iterate";
    case GRAPHD_REQUEST_ISLINK:
      return "islink";
    case GRAPHD_REQUEST_READ:
      return "read";
    case GRAPHD_REQUEST_RESTORE:
      return "restore";
    case GRAPHD_REQUEST_SET:
      return "set";
    case GRAPHD_REQUEST_SKIP:
      return "skip";
    case GRAPHD_REQUEST_SMP:
      return "smp (IN)";
    case GRAPHD_REQUEST_SMP_OUT:
      return "smp (OUT)";
    case GRAPHD_REQUEST_STATUS:
      return "status";
    case GRAPHD_REQUEST_SYNC:
      return "sync";
    case GRAPHD_REQUEST_WRITE:
      return "write";
    case GRAPHD_REQUEST_VERIFY:
      return "verify";
    case GRAPHD_REQUEST_REPLICA:
      return "replica";
    case GRAPHD_REQUEST_WRITETHROUGH:
      return "writethrough";
    case GRAPHD_REQUEST_REPLICA_WRITE:
      return "replica-write";
    case GRAPHD_REQUEST_CLIENT_REPLICA:
      return "client-replica";
    case GRAPHD_REQUEST_ASYNC_REPLICA_WRITE:
      return "async-replica-write";
    case GRAPHD_REQUEST_ASYNC_REPLICA_RESTORE:
      return "async replica-restore";
    case GRAPHD_REQUEST_ASYNC_REPLICA_CATCH_UP:
      return "async replica-catch-up";
    default:
      break;
  }

  snprintf(error_buf, sizeof error_buf, "unexpected request type %d",
           (int)greq->greq_request);
  return error_buf;
}

/*  Initialize-method - called by libsrv when srv_request_create is called.
 *	It already allocated the request data; we just have to fill it.
 */
int graphd_request_initialize(void *data, srv_handle *srv, void *session_data,
                              void *request_data) {
  int err;
  graphd_request *greq = request_data;
  graphd_session *gses = session_data;
  pdb_handle *pdb = gses->gses_graphd->g_pdb;

  /* Initialize the iterator base for this request, and
   * seed it with the request pointer.
   */
  err =
      pdb_iterator_base_initialize(pdb, greq->greq_req.req_cm, &greq->greq_pib);
  if (err != 0) return err;

  err = pdb_iterator_base_set(pdb, &greq->greq_pib, "graphd.request", greq);
  if (err != 0) {
    pdb_iterator_base_finish(gses->gses_graphd->g_pdb, &greq->greq_pib);
    return err;
  }

  /* initialize micro-parser state */
  gdp_micro_init(&greq->greq_micro);

  greq->greq_error_message = NULL;
  greq->greq_error_token.tkn_start = NULL;
  greq->greq_error_substitute = 0;
  greq->greq_error_state = GRAPHD_ERRORSTATE_INITIAL;

  greq->greq_end = PDB_ID_NONE;
  greq->greq_start = PDB_ID_NONE;
  greq->greq_loglevel_valid = false;
  greq->greq_dateline_wanted = false;

  greq->greq_dateline = NULL;
  greq->greq_runtime_statistics_started = false;
  greq->greq_completed = false;
  greq->greq_request_size = 0;

  greq->greq_request = GRAPHD_REQUEST_UNSPECIFIED;

  greq->greq_parameter_head = NULL;
  greq->greq_parameter_tail = &greq->greq_parameter_head;

  greq->greq_iterator_chain.pic_head = greq->greq_iterator_chain.pic_tail =
      NULL;

  greq->greq_indent = 0;

  greq->greq_soft_timeout_triggered = NULL;
  greq->greq_soft_timeout = 0;

  graphd_runtime_statistics_max(&greq->greq_runtime_statistics_allowance);

  cm_resource_manager_initialize(&greq->greq_resource, greq);

  graphd_stack_alloc(&greq->greq_stack, &greq->greq_resource,
                     greq->greq_req.req_cm);

  return 0;
}

static void graphd_request_skip_input_arrived(graphd_request *greq) {
  srv_request_complete(&greq->greq_req);
}

graphd_request_type const graphd_request_skip = {
    .grt_name = "skip", .grt_input_arrived = graphd_request_skip_input_arrived};

/*  Turn an unspecified request into a specific type.
 */
int graphd_request_become(graphd_request *greq, graphd_command new_request) {
  /*  Possible transitions:
   *
   *  GRAPHD_REQUEST_UNSPECIFIED -> anything
   *  anything                   -> GRAPHD_REQUEST_ERROR
   */
  if (greq->greq_request == new_request) return 0;

  if (new_request == GRAPHD_REQUEST_ERROR) {
    graphd_request_free_specifics(greq);
    greq->greq_request = new_request;
  } else {
    if (greq->greq_request != GRAPHD_REQUEST_UNSPECIFIED)
      return GRAPHD_ERR_ALREADY;

    switch (new_request) {
      case GRAPHD_REQUEST_SKIP:

        /*  We're just skipping input; we're not writing
         *  anything, and not running.
         */
        greq->greq_request = new_request;
        srv_request_output_done(&greq->greq_req);
        srv_request_run_done(&greq->greq_req);
        greq->greq_type = &graphd_request_skip;
        break;

      case GRAPHD_REQUEST_SET:
        return graphd_set_initialize(greq);

      case GRAPHD_REQUEST_STATUS:
        return graphd_status_initialize(greq);

      case GRAPHD_REQUEST_WRITE:
        return graphd_write_initialize(greq);

      case GRAPHD_REQUEST_SYNC:
        graphd_sync_initialize(greq);
        return 0;

      case GRAPHD_REQUEST_DUMP:
        graphd_dump_initialize(greq);
        return 0;

      case GRAPHD_REQUEST_SMP:
        return graphd_smp_initialize(greq);

      case GRAPHD_REQUEST_WRITETHROUGH:
        graphd_writethrough_initialize(greq);
        return 0;

      case GRAPHD_REQUEST_PASSTHROUGH:
        graphd_leader_passthrough_initialize(greq);
        return 0;

      default:
        greq->greq_request = new_request;
        break;
    }
  }
  return 0;
}

void graphd_request_reply_as_string(graphd_request *greq, char *buf,
                                    size_t size, char const **s_out, int *n_out,
                                    bool *incomplete_out) {
  srv_request *req;

  req = &greq->greq_req;

  if (req->req_first != NULL && req->req_last != NULL && req->req_last_n > 0) {
    return graphd_request_as_string(greq, buf, size, s_out, n_out,
                                    incomplete_out);
  }

  /*  There's no reply.  Was there an error on
   *  the connection?
   */
  if (req->req_session->ses_bc.bc_error) {
    *incomplete_out = false;

    if (req->req_session->ses_bc.bc_errno != 0)
      snprintf(buf, size, "connection error: %s",
               srv_xstrerror(req->req_session->ses_bc.bc_errno));
    else
      snprintf(buf, size, "unspecified connection error");
  } else {
    if (!(req->req_done & 1 << SRV_INPUT))
      snprintf(buf, size, "no reply");
    else
      *buf = '\0';
  }

  buf[size - 1] = '\0';

  *s_out = buf;
  *n_out = strlen(buf);
  *incomplete_out = *n_out == size - 1;
}

void graphd_request_as_string(graphd_request *greq, char *buf, size_t size,
                              char const **s_out, int *n_out,
                              bool *incomplete_out) {
  char const *s, *s_prev;
  size_t n, n_prev;
  void *state = NULL;

  *incomplete_out = false;
  if (srv_request_text_next(&greq->greq_req, &s_prev, &n_prev, &state) != 0) {
    *s_out = "[request text unavailable]";
    *n_out = strlen(*s_out);

    return;
  }

  /* Frequent case: one request, no copying. */

  if (n_prev >= size ||
      srv_request_text_next(&greq->greq_req, &s, &n, &state) != 0) {
    if (n_prev >= size) *incomplete_out = true;

    *s_out = s_prev;
    *n_out = n_prev < size ? n_prev : size;

    return;
  }

  *s_out = buf;
  *n_out = n_prev;

  memcpy(buf, s_prev, n_prev);
  do {
    if (n + *n_out >= size) {
      *incomplete_out = true;
      memcpy(buf + *n_out, s, size - *n_out);
      *n_out = size;

      return;
    }
    memcpy(buf + *n_out, s, n);
    *n_out += n;

  } while (srv_request_text_next(&greq->greq_req, &s, &n, &state) == 0);
}

int graphd_request_as_malloced_string(graphd_request *greq, char **buf_out,
                                      char const **s_out, int *n_out) {
  char *w;
  char const *s, *s_prev;
  size_t n, n_prev;
  void *state = NULL;

  *buf_out = NULL;

  if (srv_request_text_next(&greq->greq_req, &s_prev, &n_prev, &state) != 0) {
    *s_out = "[request text unavailable]";
    *n_out = strlen(*s_out);

    return 0;
  }

  /* Frequent case: one request, no copying. */

  if (srv_request_text_next(&greq->greq_req, &s, &n, &state) != 0) {
    *s_out = s_prev;
    *n_out = n_prev;

    return 0;
  }

  /*  Find out what the total size is, so we only have to
   *  allocate once.
   */
  do
    n_prev += n;
  while (srv_request_text_next(&greq->greq_req, &s, &n, &state) == 0);

  *s_out = *buf_out = cm_malloc(greq->greq_req.req_cm, n_prev + 1);
  if (*buf_out == NULL) return ENOMEM;

  state = NULL;
  w = *buf_out;
  while (srv_request_text_next(&greq->greq_req, &s, &n, &state) == 0) {
    cl_assert(graphd_request_cl(greq), (w - *buf_out) + n <= n_prev);

    memcpy(w, s, n);
    w += n;
  }
  *w = '\0';
  *n_out = w - *s_out;

  return 0;
}

/**
 * @brief Allocate and return a request parameter.
 *
 * @param greq	Request in which to allocate the parameter.
 * @param format_callback	parameter's formatting function
 * @param size			how much memory to allocate (for
 *				wrapper structures with common generic lead)
 *
 * @return NULL on allocation failure, otherwise the new structure.
 */
graphd_request_parameter *graphd_request_parameter_append(
    graphd_request *greq, graphd_request_parameter_format *format_callback,
    size_t size) {
  graphd_request_parameter *p;

  cl_assert(graphd_request_cl(greq), size >= sizeof(*p));

  p = cm_malloc(greq->greq_req.req_cm, size);
  if (p == NULL) return NULL;
  memset(p, 0, size);

  p->grp_next = NULL;
  p->grp_format = format_callback;

  *greq->greq_parameter_tail = p;
  greq->greq_parameter_tail = &p->grp_next;

  return p;
}

/**
 * @brief Abort processing, free resources, for a possibly ongoing request.
 *
 *  The request that was going on needs to stop, typically to be
 *  replaced by an error request.  Call its cleanup handler.
 *
 *  This may be called more than once.
 *
 * @param greq	Request that is stopping.
 */
void graphd_request_free_specifics(graphd_request *greq) {
  cl_handle *const cl = graphd_request_cl(greq);

  if (greq->greq_xstate_ticket != NULL)
    graphd_xstate_ticket_delete(graphd_request_graphd(greq),
                                &greq->greq_xstate_ticket);

  if (greq->greq_type != NULL) {
    if (greq->greq_type->grt_free != NULL) (*greq->greq_type->grt_free)(greq);
    greq->greq_type = NULL;
  }

  if (greq->greq_session_wait != NULL) graphd_session_request_wait_abort(greq);

  graph_dateline_destroy(greq->greq_asof);
  greq->greq_asof = NULL;

  graphd_value_finish(cl, &greq->greq_reply);
  graphd_format_value_records_finish(greq);
  graphd_stack_free(&greq->greq_stack);

  greq->greq_xstate = GRAPHD_XSTATE_NONE;
  greq->greq_request = GRAPHD_REQUEST_UNSPECIFIED;
}

void graphd_request_error_loc(graphd_request *greq, char const *message,
                              char const *file, int line) {
  cl_handle *const cl = graphd_request_cl(greq);

  if (greq->greq_error_message != NULL) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_request_error_loc: ignoring secondary "
           "error message %s [%s:%d]",
           message, file, line);
    return;
  }
  /*  This is the only place that assigns
   *  GRAPHD_REQUEST_ERROR.
   */
  graphd_request_become(greq, GRAPHD_REQUEST_ERROR);

  greq->greq_error_message = cm_strmalcpy(greq->greq_req.req_cm, message);
  if (greq->greq_error_message == NULL)
    greq->greq_error_message =
        "SYSTEM "
        "\"out of memory while allocating error message\"";
  graphd_request_xstate_set(greq, GRAPHD_XSTATE_NONE);

  /*  We're done with input and running.
   */
  if (!(greq->greq_req.req_done & (1 << SRV_INPUT)))
    srv_request_input_done(&greq->greq_req);
  if (!(greq->greq_req.req_done & (1 << SRV_RUN))) graphd_request_served(greq);

  graphd_request_completed_log(greq, "error");

  if (srv_request_error(&greq->greq_req)) {
    cl_log(cl, CL_LEVEL_FAIL,
           "graphd_request_error_loc: set error message, "
           "to \"%s\", but there's been a connection error - it's "
           "unlinkely to be sent [%s:%d]",
           message, file, line);
    return;
  }

  if (graphd_request_session(greq) ==
      graphd_request_graphd(greq)->g_rep_master) {
    cl_log(cl, CL_LEVEL_ERROR,
           "graphd_request_error_loc: We marked a request as "
           "an error, and it came from the master. This is "
           "clearly wrong.");
    cl_notreached(cl,
                  "Replica connection gave us a message, "
                  "but it was an error: %s",
                  message);
  }

  if (greq->greq_req.req_done & (1 << SRV_OUTPUT)) {
    cl_log(cl, CL_LEVEL_FAIL,
           "graphd_request_error_loc: set error message, "
           "to \"%s\", but the request is already done - it's "
           "unlinkely to go anywhere [%s:%d]",
           message, file, line);
    return;
  }
  srv_request_output_ready(&greq->greq_req);

  cl_log(cl, CL_LEVEL_DEBUG, "graphd_request_error_loc: %s [%s:%d]",
         greq->greq_error_message, file, line);
}

bool graphd_request_has_error(graphd_request const *greq) {
  return greq->greq_error_message != NULL ||
         greq->greq_request == GRAPHD_REQUEST_ERROR;
}

void graphd_request_errprintf_loc(graphd_request *greq, int substitute,
                                  char const *file, int line, char const *fmt,
                                  ...) {
  char errbuf[1024 * 4];
  va_list ap;

  va_start(ap, fmt);
  vsnprintf(errbuf, sizeof errbuf, fmt, ap);
  va_end(ap);

  graphd_request_error_loc(greq, errbuf, file, line);
  greq->greq_error_substitute = substitute;
}

void graphd_request_completed_log(graphd_request *greq, const char *status) {
  graphd_session *const gses = graphd_request_session(greq);
  cl_handle *const cl = gses->gses_cl;
  graphd_handle *const graphd = gses->gses_graphd;
  cl_handle *const netlog_cl = srv_netlog(gses->gses_ses.ses_srv);

  if (greq->greq_completed) return;

  graphd_runtime_statistics_accumulate(
      greq, &greq->greq_runtime_statistics_accumulated,
      &greq->greq_runtime_statistics);

  greq->greq_runtime_statistics = greq->greq_runtime_statistics_accumulated;

  cl_log(cl, CL_LEVEL_DEBUG | GRAPHD_FACILITY_COST,
         "%s cost: "
         "tu=%llu " /* time/user			*/
         "ts=%llu " /* time/system			*/
         "tr=%llu " /* time/real			*/
         "te=%llu " /* time elapsed	(real)		*/
         "pr=%llu " /* page reclaims		*/
         "pf=%llu " /* page faults			*/
         "dw=%llu " /* primitive writes 		*/
         "dr=%llu " /* primitive reads 		*/
         "in=%llu " /* gmap size reads		*/
         "ir=%llu " /* gmap reads			*/
         "iw=%llu " /* gmap writes			*/
         "va=%llu", /* values allocated		*/

         gses->gses_ses.ses_displayname,
         greq->greq_runtime_statistics.grts_user_micros / 1000ull,
         greq->greq_runtime_statistics.grts_system_micros / 1000ull,
         greq->greq_runtime_statistics.grts_wall_micros / 1000ull,
         greq->greq_runtime_statistics.grts_endtoend_micros / 1000ull,
         greq->greq_runtime_statistics.grts_minflt,
         greq->greq_runtime_statistics.grts_majflt,
         greq->greq_runtime_statistics.grts_pdb.rts_primitives_written,
         greq->greq_runtime_statistics.grts_pdb.rts_primitives_read,
         greq->greq_runtime_statistics.grts_pdb.rts_index_extents_read,
         greq->greq_runtime_statistics.grts_pdb.rts_index_elements_read,
         greq->greq_runtime_statistics.grts_pdb.rts_index_elements_written,
         greq->greq_runtime_statistics.grts_values_allocated);

  if (graphd->g_diary_cl != NULL) {
    cl_log(graphd->g_diary_cl, CL_LEVEL_DEBUG | GRAPHD_FACILITY_COST,
           "REQUEST(%s,%llu,%llu).cost="
           "tu=%llu " /* time/user			*/
           "ts=%llu " /* time/system			*/
           "tr=%llu " /* time/real			*/
           "pr=%llu " /* page reclaims		*/
           "pf=%llu " /* page faults			*/
           "dw=%llu " /* primitive writes 		*/
           "dr=%llu " /* primitive reads 		*/
           "in=%llu " /* gmap size reads		*/
           "ir=%llu " /* gmap reads			*/
           "iw=%llu " /* gmap writes			*/
           "va=%llu", /* values allocated		*/

           greq->greq_req.req_session->ses_displayname,
           greq->greq_req.req_session->ses_id, greq->greq_req.req_id,
           greq->greq_runtime_statistics.grts_user_micros / 1000ull,
           greq->greq_runtime_statistics.grts_system_micros / 1000ull,
           greq->greq_runtime_statistics.grts_wall_micros / 1000ull,
           greq->greq_runtime_statistics.grts_minflt,
           greq->greq_runtime_statistics.grts_majflt,
           greq->greq_runtime_statistics.grts_pdb.rts_primitives_written,
           greq->greq_runtime_statistics.grts_pdb.rts_primitives_read,
           greq->greq_runtime_statistics.grts_pdb.rts_index_extents_read,
           greq->greq_runtime_statistics.grts_pdb.rts_index_elements_read,
           greq->greq_runtime_statistics.grts_pdb.rts_index_elements_written,
           greq->greq_runtime_statistics.grts_values_allocated);
  }

  /*  We do not netlog outgoing forwarded write requests
   *  from the replica to the server.
   */
  if (netlog_cl != NULL && graphd_request_is_netlogged(greq)) {
    char write_buf[200];

    write_buf[0] = '\0';
    if (greq->greq_request == GRAPHD_REQUEST_WRITE ||
        greq->greq_request == GRAPHD_REQUEST_RESTORE) {
      snprintf(write_buf, sizeof write_buf, "(l)graphd.istore.n: %llu ",
               pdb_primitive_n(graphd->g_pdb));
    }
    cl_log(
        netlog_cl, CL_LEVEL_INFO | GRAPHD_FACILITY_COST,
        "graphd.request.%s: "
        "TID: %s "               /* transaction ID 	*/
        "%s"                     /* address_parts	*/
        "(i)duration: %llu "     /* duration in millis  */
        "(l)graphd.sesid: %llu " /* local session ID	*/
        "(l)graphd.reqid: %llu " /* local request ID 	*/
        "%s"                     /* write_buf 		*/
        "graphd.request.cost: "
        "tu=%llu " /* time/user		*/
        "ts=%llu " /* time/system		*/
        "tr=%llu " /* time/real		*/
        "te=%llu " /* time/end-to-end	*/
        "pr=%llu " /* page reclaims	*/
        "pf=%llu " /* page faults		*/
        "dw=%llu " /* primitive writes 	*/
        "dr=%llu " /* primitive reads 	*/
        "in=%llu " /* gmap size reads	*/
        "ir=%llu " /* gmap reads		*/
        "iw=%llu " /* gmap writes		*/
        "va=%llu", /* values allocated	*/

        status,
        greq->greq_req.req_display_id ? greq->greq_req.req_display_id : "???",
        greq->greq_req.req_session->ses_netlog_header
            ? greq->greq_req.req_session->ses_netlog_header
            : "",
        greq->greq_runtime_statistics.grts_endtoend_micros / 1000ull,
        greq->greq_req.req_session->ses_id, greq->greq_req.req_id, write_buf,
        greq->greq_runtime_statistics.grts_user_micros / 1000ull,
        greq->greq_runtime_statistics.grts_system_micros / 1000ull,
        greq->greq_runtime_statistics.grts_wall_micros / 1000ull,
        greq->greq_runtime_statistics.grts_endtoend_micros / 1000ull,
        greq->greq_runtime_statistics.grts_minflt,
        greq->greq_runtime_statistics.grts_majflt,
        greq->greq_runtime_statistics.grts_pdb.rts_primitives_written,
        greq->greq_runtime_statistics.grts_pdb.rts_primitives_read,
        greq->greq_runtime_statistics.grts_pdb.rts_index_extents_read,
        greq->greq_runtime_statistics.grts_pdb.rts_index_elements_read,
        greq->greq_runtime_statistics.grts_pdb.rts_index_elements_written,
        greq->greq_runtime_statistics.grts_values_allocated);
  }
  greq->greq_completed = true;
}

/**
 *  @brief Mark a request as served.
 *
 *   Having been served means that the computation for the
 *   request has completed.  The remaining formatting turns
 *   precomputed results into bytes in a buffer, but we
 *   basically know what the results are.  No primitives will
 *   be created after a request has been served.
 *
 *   Various bookkeeping tasks happen when a request is marked
 *   as served:
 *
 *	- a dateline is created and assigned.
 *
 *      - the cost of the request is computed (as a difference
 *	  of the cumulative session cost before and after the
 *	  request).
 *
 *  @param greq the request that's done.
 */
void graphd_request_finish_running(graphd_request *greq) {
  if (greq == NULL) return;

  /*  Mark us as done running.
   */
  srv_request_run_done(&greq->greq_req);

  /*  If the request wants a dateline, give it one.
   */
  if (greq->greq_dateline_wanted) {
    cl_handle *cl = graphd_request_cl(greq);
    char b1[200], b2[200];

    if (greq->greq_dateline) {
      graph_dateline_destroy(greq->greq_dateline);
      greq->greq_dateline = NULL;
    }
    greq->greq_dateline = graphd_dateline(graphd_request_graphd(greq));
    if (greq->greq_dateline == NULL) {
      greq->greq_dateline_wanted = false;
      graphd_request_error(greq,
                           "graphd_request_finish_running: "
                           "out of memory while allocating dateline");
    }

    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_request_finish_running: got dateline %s for "
           "request %s",
           graph_dateline_to_string(greq->greq_dateline, b1, sizeof b1),
           graphd_request_to_string(greq, b2, sizeof b2));
  }
}

/**
 *  @brief Mark a request as served.
 *
 *   Having been served means that the computation for the
 *   request has completed.  The remaining formatting turns
 *   precomputed results into bytes in a buffer, but we
 *   basically know what the results are.  No primitives will
 *   be created after a request has been served.
 *
 *   Various bookkeeping tasks happen when a request is marked
 *   as served:
 *
 *	- a dateline is created and assigned.
 *
 *      - the cost of the request is computed (as a difference
 *	  of the cumulative session cost before and after the
 *	  request).
 *
 *  @param greq the request that's done.
 */
void graphd_request_served(graphd_request *greq) {
  if (greq == NULL) return;

  if (!(greq->greq_req.req_done & (1 << SRV_RUN))) {
    cl_handle *cl = graphd_request_cl(greq);
    graphd_request_finish_running(greq);
    cl_assert(cl, greq->greq_req.req_done & (1 << SRV_RUN));
  }

  /*  If we wanted to produce output at some point,
   *  say we're ready for it.
   */
  if (!(greq->greq_req.req_done & (1 << SRV_OUTPUT)))
    srv_request_output_ready(&greq->greq_req);

  graphd_request_completed_log(greq, "end");
}

void graphd_request_finish(void *data, srv_handle *srv, void *session_data,
                           void *request_data) {
  graphd_request *greq = request_data;
  graphd_handle *g = graphd_request_graphd(greq);
  graphd_session *gses = session_data;

  cl_enter(gses->gses_cl, CL_LEVEL_VERBOSE, "%p", (void *)request_data);

  /*  If the session has requests waiting for it,
   *  at it can now accomodate them, go wake them up.
   */
  if (gses->gses_request_wait_head != NULL &&
      srv_session_n_requests(&gses->gses_ses) < GRAPHD_OUTGOING_REQUESTS_MAX)
    graphd_session_request_wait_wakeup(gses);

  if (!greq->greq_completed && greq->greq_runtime_statistics_started) {
    /* If an unserved (but started) request is finished,
     * it means that the request was canceled. Log that.
     */
    graphd_request_completed_log(greq, "cancel");
  }

  if (g->g_smp_request == greq) {
    (void)graphd_smp_resume_for_write(greq);
    g->g_smp_request = NULL;
  }

  graphd_request_free_specifics(greq);

  if (greq->greq_dateline) {
    graph_dateline_destroy(greq->greq_dateline);
    greq->greq_dateline = NULL;
  }

  cm_resource_manager_finish(&greq->greq_resource);
  pdb_iterator_base_finish(g->g_pdb, &greq->greq_pib);

  graphd_constraint_free(greq, greq->greq_constraint);
  greq->greq_constraint = NULL;

  /*  At this time, all the iterators in the request
   *  must have been free'd.
   */
  pdb_iterator_chain_finish(
      graphd_request_graphd(greq)->g_pdb, &greq->greq_iterator_chain,
      greq->greq_req.req_display_id == NULL
          ? graphd_request_session(greq)->gses_ses.ses_displayname
          : greq->greq_req.req_display_id);

  /*  Tiles, too.
   */
  cl_leave(gses->gses_cl, CL_LEVEL_VERBOSE,
           "-- finish request %p, session %s --", (void *)request_data,
           gses->gses_ses.ses_displayname);
}

/*
 * @brief Log request activity.
 * @param greq 		the request that's arrived.
 * @param millis	NULL or the current time in milliseconds
 * @param activity 	What activity are we logging?
 */
static char const *graphd_request_type_to_string(int type) {
  switch (type) {
    case GRAPHD_REQUEST_DUMP:
      return "DUMP";
    case GRAPHD_REQUEST_READ:
      return "READ";
    case GRAPHD_REQUEST_ITERATE:
      return "ITERATE";
    case GRAPHD_REQUEST_ISLINK:
      return "ISLINK";
    case GRAPHD_REQUEST_SET:
      return "SET";
    case GRAPHD_REQUEST_STATUS:
      return "STATUS";
    case GRAPHD_REQUEST_ERROR:
      return "ERROR";
    case GRAPHD_REQUEST_SKIP:
      return "SKIP";
    case GRAPHD_REQUEST_CRASH:
      return "CRASH";
    case GRAPHD_REQUEST_REPLICA:
      return "REPLICA";
    case GRAPHD_REQUEST_RESTORE:
      return "RESTORE";
    case GRAPHD_REQUEST_SYNC:
      return "SYNC";
    case GRAPHD_REQUEST_VERIFY:
      return "VERIFY";
    case GRAPHD_REQUEST_WRITE:
      return "WRITE";
    case GRAPHD_REQUEST_WRITETHROUGH:
      return "WRITETHROUGH";
    case GRAPHD_REQUEST_REPLICA_WRITE:
      return "REPLICA-WRITE";
    case GRAPHD_REQUEST_CLIENT_REPLICA:
      return "(-> MASTER) REPLICA";
    case GRAPHD_REQUEST_ASYNC_REPLICA_WRITE:
      return "(ASYNC -> REPLICA) WRITE";
    case GRAPHD_REQUEST_ASYNC_REPLICA_RESTORE:
      return "(ASYNC -> REPLICA) RESTORE";
    case GRAPHD_REQUEST_ASYNC_REPLICA_CATCH_UP:
      return "(ASYNC -> REPLICA) CATCH-UP";
  }
  return "???";
}

/**
 * @brief Log request activity.
 * @param greq 		the request that's arrived.
 * @param millis	NULL or the current time in milliseconds
 * @param activity 	What activity are we logging?
 */
void graphd_request_diary_log(graphd_request *greq, unsigned long long millis,
                              char const *activity) {
  char const *name;
  graphd_handle *g = graphd_request_graphd(greq);

  if (g->g_diary_cl == NULL) return;

  name = graphd_request_type_to_string(greq->greq_request);
  if (millis)
    cl_log(g->g_diary_cl, CL_LEVEL_DETAIL, "request(%s,%llu,%llu,%s).%s=%llu",
           greq->greq_req.req_session->ses_displayname,
           greq->greq_req.req_session->ses_id, greq->greq_req.req_id, name,
           activity, millis);
  else
    cl_log(g->g_diary_cl, CL_LEVEL_DETAIL, "request(%s,%llu,%llu,%s).%s",
           greq->greq_req.req_session->ses_displayname,
           greq->greq_req.req_session->ses_id, greq->greq_req.req_id, name,
           activity);
}

/**
 *  @brief Mark a request as arrived or begun.
 *   The parser has just finished reading a line from the buffer.
 *  @param greq the request that's arrived.
 */
void graphd_request_start(graphd_request *greq) {
  graphd_handle *g;
  cl_handle *cl;

  g = graphd_request_graphd(greq);

  /*  If the client didn't give this request an ID,
   *  make one up.
   */
  if (greq->greq_req.req_display_id == NULL) {
    char isodate[100];
    struct tm tmbuf, *tm;
    time_t t;
    char const *interface_id;

    interface_id = graphd_interface_id(g);

    time(&t);
    tm = gmtime_r(&t, &tmbuf);
    if (tm == NULL ||
        strftime(isodate, sizeof isodate, "%Y-%m-%dT%H:%M:%S", tm) == 0)
      strcpy(isodate, "???");

    greq->greq_req.req_display_id =
        cm_sprintf(greq->greq_req.req_cm, "%s;%lu;%sZ;%llu", interface_id,
                   (unsigned long)getpid(), isodate, greq->greq_req.req_id);
  }

  /*  Initialize the end-to-end timer of the request.  The remaing
   *  statistics will begin with its first graphd_serve() call.
   */
  if (greq->greq_runtime_statistics.grts_endtoend_micros_start == 0) {
    struct timeval tv;
    if (gettimeofday(&tv, NULL) == 0)
      greq->greq_runtime_statistics.grts_endtoend_micros_start =
          (unsigned long long)tv.tv_sec * 1000000ull + tv.tv_usec;
  }

  if ((cl = srv_netlog(graphd_request_srv(greq))) != NULL &&
      graphd_request_is_netlogged(greq)) {
    char request_errbuf[1024];
    char const *request_s;
    int request_n;
    bool request_incomplete = false;
    char *request_buf = NULL;

    /*  Get the text of this rqeuest.
     */
    if (greq->greq_request_start_hint != NULL) {
      request_s = greq->greq_request_start_hint;
      request_n = strlen(request_s);
    } else if (graphd_request_as_malloced_string(greq, &request_buf, &request_s,
                                                 &request_n))
      graphd_request_as_string(greq, request_errbuf, sizeof(request_errbuf),
                               &request_s, &request_n, &request_incomplete);

    cl_log(
        cl, CL_LEVEL_INFO,
        "graphd.request.start "
        "TID: %s "               /* transaction ID 	*/
        "%s "                    /* address parts	*/
        "(l)graphd.sesid: %llu " /* local session ID	*/
        "(l)graphd.reqid: %llu " /* local request ID 	*/
        "graphd.request.type: %s "
        "graphd.request.text:: %.*s%s",

        greq->greq_req.req_display_id ? greq->greq_req.req_display_id : "???",
        greq->greq_req.req_session->ses_netlog_header,
        greq->greq_req.req_session->ses_id, greq->greq_req.req_id,
        graphd_request_type_to_string(greq->greq_request), request_n, request_s,
        request_incomplete ? "..." : "");

    if (request_buf != NULL) cm_free(greq->greq_req.req_cm, request_buf);
  }

  /*  Override the request's cost settings with the
   *  server settings.
   */
  graphd_runtime_statistics_limit(&greq->greq_runtime_statistics_allowance,
                                  &g->g_runtime_statistics_allowance);
}

/**
 *  @brief Mark a request as arrived.
 *   The parser has just finished reading a line (or an error) from the buffer.
 *  @param greq the request that's arrived.
 */
void graphd_request_arrived(graphd_request *greq) {
  graphd_handle *g = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  char buf[200];

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_request_arrived %s",
         graphd_request_to_string(greq, buf, sizeof buf));

  greq->greq_xstate = graphd_request_xstate_type(greq);
  if (!(greq->greq_req.req_done & (1 << SRV_OUTPUT)))
    graphd_request_start(greq);

  /*  Tell the libsrv layer that we're done reading input,
   *  and connect our buffers to the session's
   */
  srv_request_arrived(&greq->greq_req);

  if (g->g_diary_cl != NULL) graphd_request_diary_log(greq, 0, "ARRIVED");

  /*  If there's a request type, and it has an "I just read input"
   *  method, run that.
   */
  if (greq->greq_type != NULL && greq->greq_type->grt_input_arrived != NULL)

    (*greq->greq_type->grt_input_arrived)(greq);
}

/**
 * @brief Push back a request.
 *
 *  The request greq has been running for a while.  There
 *  are other requests that are waiting for a chance to run,
 *
 * @param greq	The request we're asking about.
 * @return 0 on success, a nonzero error code for error.
 */
int graphd_request_push_back(graphd_request *greq) {
  graphd_handle *g = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  int err = 0;

  if (greq->greq_request == GRAPHD_REQUEST_VERIFY) return 0;

  if (greq->greq_request != GRAPHD_REQUEST_READ) return PDB_ERR_MORE;

  /*  Suspend all iterators that have signed up for
   *  that service.
   */
  err = pdb_iterator_suspend_all(g->g_pdb);
  if (err != 0) return err;

  if (!greq->greq_pushed_back) {
    /*  If the running read request has value pointers
     *  that point to the primitive table, duplicate those
     *  values into private storage.
     */
    err = graphd_read_suspend(greq);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_read_freeze", err, "cant freeze");
      return err;
    }
    greq->greq_pushed_back = true;
    graphd_suspend_a_read(g, srv_msclock(g->g_srv), true);
  }
  return 0;
}

/**
 * @brief Work on a request that has been pushed back.
 *
 *  It had been pushed back - suspended - a while ago; now
 *  it is getting to run again.
 *
 * @param greq	The request.
 * @return 0 on success, a nonzero error code for error.
 */
int graphd_request_push_back_resume(graphd_request *greq) {
  int err;

  /*  Unsuspend all the iterators in the request's
   *  suspend chain.
   */
  if (greq->greq_pushed_back) greq->greq_pushed_back = false;

  if (greq->greq_iterator_chain.pic_n_suspended) {
    err = pdb_iterator_unsuspend_chain(graphd_request_graphd(greq)->g_pdb,
                                       &greq->greq_iterator_chain);
    if (err != 0) return err;
    cl_assert(graphd_request_cl(greq),
              greq->greq_iterator_chain.pic_n_suspended == 0);
  }

  return 0;
}

graphd_request *graphd_request_create_asynchronous(
    graphd_session *gses, enum graphd_command type,
    graphd_request_format *callback) {
  graphd_request *greq;
  cl_handle *cl = gses->gses_cl;

  greq = (graphd_request *)srv_request_create_asynchronous(&gses->gses_ses);
  if (greq == NULL) {
    cl_log(cl, CL_LEVEL_FAIL,
           "graphd_request_create_asynchronous: failed to "
           "allocate asynchronous request from "
           "srv_request_create_asynchronous(): %s",
           graphd_strerror(errno));
    return NULL;
  }

  greq->greq_request = type;
  greq->greq_format = callback;

  return greq;
}

graphd_request *graphd_request_create_outgoing(graphd_session *gses,
                                               enum graphd_command command) {
  graphd_request *greq;
  cl_handle *cl = gses->gses_cl;

  greq = (graphd_request *)srv_request_create_outgoing(&gses->gses_ses);
  if (greq == NULL) {
    cl_log(cl, CL_LEVEL_FAIL,
           "graphd_request_create_outgoing: failed to "
           "allocate outgoing request from "
           "srv_request_create_outgoing(): %s",
           graphd_strerror(errno));
    return NULL;
  }

  /*  The specific command can override this - but by
   *  default, we're not running; we just output, wait,
   *  and then input.
   */
  srv_request_run_done(&greq->greq_req);
  graphd_request_become(greq, command);

  return greq;
}

/**
 * @brief For debugging, report this request's details
 *
 * @param greq	request to dump
 * @param buf	buffer to dump into
 * @param size	number of bytes pointed to by buf.
 *
 * @return a pointer to buf.
 */
char const *graphd_request_to_string(graphd_request const *const greq,
                                     char *const buf, size_t const size) {
  char b2[200];

  snprintf(buf, size, "%s %s", graphd_request_name(greq),
           srv_request_to_string(&greq->greq_req, b2, sizeof b2));
  return buf;
}

void graphd_request_cancel(graphd_request *greq) {
  if (greq->greq_type != NULL && greq->greq_type->grt_cancel != NULL)
    (*greq->greq_type->grt_cancel)(greq);

  srv_request_complete(&greq->greq_req);
}

/* This request's session needs to wait until writes up to dateline_id arrive.
 */
void graphd_request_suspend_for_dateline(graphd_request *greq,
                                         pdb_id dateline_id) {
  graphd_session *gses = graphd_request_session(greq);
  graphd_handle *g = gses->gses_graphd;

  if (pdb_primitive_n(g->g_pdb) < dateline_id) {
    cl_log(g->g_cl, CL_LEVEL_DEBUG,
           "suspend session %llu to wait for dateline %llu",
           (unsigned long long)gses->gses_ses.ses_id,
           (unsigned long long)dateline_id);

    /*  Remember what we were waiting for.
     */
    gses->gses_dateline_id = dateline_id;
    graphd_request_suspend(greq, GRAPHD_SUSPEND_DATELINE);

    /*  If that's later than what we're currently
     *  waiting for, expand that, too.
     */
    if (g->g_dateline_suspended_max == PDB_ID_NONE ||
        g->g_dateline_suspended_max < dateline_id)
      g->g_dateline_suspended_max = dateline_id;
  }
}

void graphd_request_suspend(graphd_request *greq,
                            graphd_suspend_reason reason) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_session *gses = graphd_request_session(greq);

  cl_assert(cl, GRAPHD_SUSPEND_REASON_VALID(reason));
  cl_assert(cl, GRAPHD_SUSPEND_NOTHING == gses->gses_suspend_reason);
  cl_assert(cl, !(greq->greq_req.req_done & (1 << SRV_RUN)));

  if (greq->greq_xstate_ticket != NULL)
    graphd_xstate_ticket_delete(graphd_request_graphd(greq),
                                &greq->greq_xstate_ticket);

  gses->gses_suspend_reason = reason;
  srv_request_suspend(&greq->greq_req);
}

void graphd_request_resume(graphd_request *greq) {
  graphd_session *gses;

  gses = graphd_request_session(greq);
  gses->gses_suspend_reason = GRAPHD_SUSPEND_NOTHING;

  srv_request_run_ready(&greq->greq_req);
}
