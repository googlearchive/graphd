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
#include <stdio.h>    /* EOF */
#include <sysexits.h> /* EX_OSERR */

#include "libsrv/srv.h"

/*  Create a two-way link between write (client)
 *  and (master) writethrough.
 */
static void link_writethrough(graphd_request *src, graphd_request *dst) {
  cl_handle *const cl = graphd_request_cl(src);

  cl_assert(cl, src->greq_request == GRAPHD_REQUEST_WRITE);
  cl_assert(cl, dst->greq_request == GRAPHD_REQUEST_WRITETHROUGH);

  graphd_request_link_pointer(dst, &src->greq_master_req);
  graphd_request_link_pointer(src, &dst->greq_data.gd_writethrough.gdwt_client);

  /*  Mark the source (write) as dependent
   *  on the destination (writethrough).  That will
   *  keep us from deadlocking if the source is
   *  priorized over the destination.
   */
  srv_request_depend(&src->greq_req, &dst->greq_req);
}

/*  Disconnect the writethrough to the master from the client.
 */
static void unlink_writethrough(graphd_request *src) {
  cl_handle *const cl = graphd_request_cl(src);
  graphd_request *dst;

  cl_assert(cl, src->greq_request == GRAPHD_REQUEST_WRITE);

  if ((dst = src->greq_master_req) != NULL) {
    cl_assert(cl, dst->greq_request == GRAPHD_REQUEST_WRITETHROUGH);
    cl_assert(cl, src->greq_req.req_refcount > 1);

    /*  Remove the priorization dependency of the
     *  writethrough on us.
     */
    srv_request_depend(NULL, &dst->greq_req);

    /*  Unlink the pointers between us and the writethrough.
     */
    graphd_request_unlink_pointer(&dst->greq_data.gd_writethrough.gdwt_client);
    graphd_request_unlink_pointer(&src->greq_master_req);
  }
}

void graphd_writethrough_session_fail(graphd_handle *const g) {
  graphd_session *gses = g->g_rep_write;
  graphd_request *greq;

  greq = (graphd_request *)gses->gses_ses.ses_request_head;
  while (greq != NULL) {
    graphd_request *client_greq, *next;

    /*  XXX Has any of these requests been sent?
     */

    next = (graphd_request *)greq->greq_req.req_next;

    cl_log(g->g_cl, CL_LEVEL_FAIL, "Failing write(through) on \"%s\"",
           gses->gses_ses.ses_displayname);

    if (greq->greq_request == GRAPHD_REQUEST_WRITETHROUGH &&
        (client_greq = greq->greq_data.gd_writethrough.gdwt_client) != NULL) {
      graphd_request_error(client_greq, "SYSTEM unable to write at this time");
      graphd_session_resume(graphd_request_session(client_greq));
    }

    srv_request_complete(&greq->greq_req);
    greq = next;
  }
}

static int graphd_write_master_connect(graphd_handle *g) {
  int err = 0;

  cl_assert(g->g_cl, GRAPHD_ACCESS_REPLICA == g->g_access ||
                         GRAPHD_ACCESS_REPLICA_SYNC == g->g_access);

  if (g->g_smp_proc_type == GRAPHD_SMP_PROCESS_FOLLOWER) {
    err = graphd_leader_passthrough_connect(g);
    if (!g->g_smp_leader_passthrough || err) {
      cl_log(g->g_cl, CL_LEVEL_ERROR,
             "graphd_write_master_connect: "
             "couldn't connect to SMP leader on passthrough");
    } else {
      g->g_rep_write = g->g_smp_leader_passthrough;
    }
    return err;
  }

  cl_assert(g->g_cl, g->g_rep_master_address);
  if (!g->g_rep_write_address) {
    cl_log(g->g_cl, CL_LEVEL_ERROR,
           "graphd_write_master_connect: "
           "don't have a g->g_rep_write_address to connect to!"
           " (returning EINVAL)");

    return EINVAL; /* no write-master address */
  }

  if (!g->g_rep_write) {
    cl_log(g->g_cl, CL_LEVEL_INFO, "Initiating write-master connection to: %s",
           g->g_rep_write_address->addr_url);

    cl_assert(g->g_cl, g->g_srv != NULL);
    err = srv_interface_connect(g->g_srv, g->g_rep_write_address->addr_url,
                                (void *)&g->g_rep_write);
  }

  return err;
}

bool graphd_request_copy_request_text(graphd_handle *g, graphd_request *dst,
                                      graphd_request *src, char **s, char *e) {
  size_t offset = dst->greq_offset;
  char const *segment;
  size_t segment_n;
  void *state = NULL;

  while (!srv_request_text_next(&src->greq_req, &segment, &segment_n, &state)) {
    /*  Fast-forward past the first <offset> characters -
     *  we already wrote those.
     */
    if (offset > 0) {
      if (offset > segment_n) {
        offset -= segment_n;
        continue;
      } else {
        segment += offset;
        segment_n -= offset;
        offset = 0;
      }
    }

    if (segment_n > 0) {
      size_t const sz = e - *s;
      size_t const copy_n = sz > segment_n ? segment_n : sz;

      memcpy(*s, segment, copy_n);
      *s += copy_n;
      dst->greq_offset += copy_n;
    }
    if (e == *s) {
      cl_log(g->g_cl, CL_LEVEL_SPEW,
             "graphd_request_copy_request_text %llu -> %llu: filled buffer",
             src->greq_req.req_id, dst->greq_req.req_id);
      return false;
    }
  }
  return true;
}

/*  This is called in the client_request to pull data out of
 *  its master request's reply.
 */
static void format_writethrough_response(void *data, srv_handle *srv,
                                         void *session_data, void *request_data,
                                         char **s, char *e) {
  graphd_handle *const g = data;
  graphd_request *const client_request = request_data;
  graphd_request *const master_request = client_request->greq_master_req;
  cl_handle *const cl = graphd_request_cl(client_request);

  if (master_request == NULL) {
    /*  Something bad happened.  We don't know what.
     */
    graphd_request_error(client_request,
                         "SYSTEM unexpected error while forwarding "
                         "write request");
    return;
  }

  cl_log(cl, CL_LEVEL_SPEW,
         "format_writethrough_response forwarding response "
         "from %llu to %llu",
         master_request->greq_req.req_id, client_request->greq_req.req_id);

  cl_assert(cl, client_request->greq_request == GRAPHD_REQUEST_WRITE);
  cl_assert(cl, master_request->greq_request == GRAPHD_REQUEST_WRITETHROUGH);
  cl_assert(cl, graphd_request_session(master_request) == g->g_rep_write);

  if (!graphd_request_copy_request_text(g, client_request, master_request, s,
                                        e))
    return;

  /*  That's all the output this request will produce.
   */

  /* The writethrough is done.
   */
  srv_request_reply_received(&master_request->greq_req);
  srv_request_complete(&master_request->greq_req);
  graphd_request_completed_log(master_request, "end");

  /* Our write is now done, too.
   */
  srv_request_reply_sent(&client_request->greq_req);
  unlink_writethrough(client_request);
  srv_request_complete(&client_request->greq_req);
}

/*  Called at the beginning of the writethrough request to copy
 *  data to the master's outgoing connection.
 */
static void format_writethrough(void *data, srv_handle *srv, void *session_data,
                                void *request_data, char **s, char *e) {
  graphd_handle *const g = data;
  graphd_request *const master_request = request_data;
  graphd_request *const client_request =
      master_request->greq_data.gd_writethrough.gdwt_client;

  if (s == NULL) {
    /*  The line dropped.
     */
    return;
  }

  cl_log(g->g_cl, CL_LEVEL_VERBOSE,
         "format_writethrough forwarding command from %llu to %llu",
         client_request->greq_req.req_id, master_request->greq_req.req_id);

  /*  Are we finished copying yet?
   */
  if (!graphd_request_copy_request_text(g, master_request, client_request, s,
                                        e))
    return;

  srv_request_sent(&master_request->greq_req);
}

static void graphd_writethrough_input_arrived(graphd_request *greq) {
  graphd_request *client_request = NULL;
  graphd_handle *g = graphd_request_graphd(greq);

  client_request = greq->greq_data.gd_writethrough.gdwt_client;

  cl_log(graphd_request_cl(greq), CL_LEVEL_SPEW,
         "graphd_writethrough_input_arrived begin");

  if (client_request != NULL) {
    char buf[200];

    if (client_request->greq_request == GRAPHD_REQUEST_WRITE) {
      /* Is this greq's session the write master? That's the only
       * reason we're doing this write. If not, then we're in trouble,
       * so let's change the output */
      if (graphd_request_session(greq) == g->g_rep_write &&
          graphd_request_session(greq) != NULL) {
        client_request->greq_format = format_writethrough_response;

        cl_log(graphd_request_cl(greq), CL_LEVEL_VERBOSE,
               "graphd_writethrough_input_arrived: delivered "
               "response to %s",
               graphd_request_to_string(client_request, buf, sizeof buf));

        srv_request_output_ready(&client_request->greq_req);
      } else {
        unlink_writethrough(client_request);
        graphd_request_error(client_request, "SYSTEM writethrough cancelled");

        cl_log(graphd_request_cl(greq), CL_LEVEL_VERBOSE,
               "graphd_writethrough_input_arrived: cancelling "
               "response to %s",
               graphd_request_to_string(client_request, buf, sizeof buf));
      }

    } else {
      cl_log(graphd_request_cl(greq), CL_LEVEL_ERROR,
             "graphd_writethrough_input_arrived: dropping "
             "response nominally to %s - this isn't a "
             "writethrough",
             graphd_request_to_string(client_request, buf, sizeof buf));
    }
  } else {
    cl_log(graphd_request_cl(greq), CL_LEVEL_FAIL,
           "graphd_writethrough_input_arrived: dropping "
           "response - no client!");
  }
  /*  This request is done, as far as the session is concerned.
   *  If there is a receiving client request (normally),
   *  this writethrough request won't actually be destroyed yet,
   *  because the receiving client request holds another link to
   *  the writethrough request while copying from its buffers.
   *  Once that is done, the whole writethrough request is destroyed.
   */
  srv_request_complete(&greq->greq_req);
}

/*  The writethrough connection has dropped.  Best we can do is
 *  to drop our own client connection as well - we don't actually
 *  know whether the write made it or not, and neither can our client.
 */
static void graphd_writethrough_cancel(graphd_request *greq) {
  graphd_request *client_request;

  client_request = greq->greq_data.gd_writethrough.gdwt_client;
  if (client_request != NULL) {
    srv_session_abort(client_request->greq_req.req_session);
    unlink_writethrough(client_request);
  }
}

static void graphd_writethrough_free(graphd_request *greq) {
  graphd_request *client_request;

  client_request = greq->greq_data.gd_writethrough.gdwt_client;
  if (client_request != NULL) unlink_writethrough(client_request);
}

static graphd_request_type graphd_request_writethrough = {
    "writethrough",

    graphd_writethrough_input_arrived,
    NULL,                       /* output sent	*/
    NULL,                       /* run 		*/
    graphd_writethrough_cancel, /* cancel 	*/
    graphd_writethrough_free    /* free 	*/

};

void graphd_writethrough_initialize(graphd_request *greq) {
  if (greq != NULL) {
    greq->greq_request = GRAPHD_REQUEST_WRITETHROUGH;
    greq->greq_type = &graphd_request_writethrough;
    greq->greq_format = format_writethrough;
  }
}

/*  Handle a write at a replica by forwarding it to the write master.
 *
 * @return 0 on successful sent
 * @return GRAPHD_ERR_SUSPEND to suspend the run attempt until there
 * 		is space in the replica request queue
 * @return GRAPHD_ERR_MORE to get called again
 * @return other errors in unexpected cases.
 */
int graphd_writethrough(graphd_request *greq) {
  graphd_session *const gses = graphd_request_session(greq);
  graphd_handle *const g = gses->gses_graphd;
  graphd_request *master_greq;
  cl_handle *const cl = graphd_request_cl(greq);
  int err;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");

  cl_log(cl, CL_LEVEL_SPEW,
         "graphd_writethrough: Attempting to connect to write master (if not "
         "already open)");

  err = graphd_write_master_connect(g);
  if (err) {
    if (g->g_rep_write_address == NULL) {
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "graphd_writethrough: no write master "
             "address - misconfigured or "
             "disconnected master \"%s\"?",
             g->g_rep_master_address ? g->g_rep_master_address->addr_url
                                     : "(unknown)");
    } else {
      cl_log_errno(cl, CL_LEVEL_OPERATOR_ERROR, "graphd_write_master_connect",
                   err, "Unable to connect to write-master at \"%s\"",
                   g->g_rep_write_address->addr_url);
    }
    goto cancel_write;
  }
  cl_assert(cl, g->g_rep_write);

  /* Is our wait-through channel full?  If yes, suspend.
   */
  if (!graphd_session_has_room_for_request(g->g_rep_write)) {
    char buf[200];

    /*  We'll be rescheduled to run once the connection drains,
     *  or if something goes wrong.
     */
    graphd_session_request_wait_add(g->g_rep_write, greq, 1 << SRV_RUN);

    cl_leave(cl, CL_LEVEL_VERBOSE,
             "graphd_writethrough: suspend (write channel to %s is full)",
             graphd_session_to_string(g->g_rep_write, buf, sizeof buf));

    return GRAPHD_ERR_SUSPEND;
  }

  /* If the replica write session is writeable we go
   * ahead and forward the write request now.  Otherwise,
   * the replica write session will handle the forwarding
   * when outstanding writes return.
   */
  cl_log(cl, CL_LEVEL_SPEW, "graphd_writethrough: Creating outgoing request");

  master_greq = graphd_request_create_outgoing(g->g_rep_write,
                                               GRAPHD_REQUEST_WRITETHROUGH);
  if (master_greq == NULL) goto cancel_write;

  cl_log(cl, CL_LEVEL_SPEW, "graphd_writethrough: Starting outgoing request");
  graphd_request_start(master_greq);
  link_writethrough(greq, master_greq);

  cl_leave(cl, CL_LEVEL_VERBOSE, "ok");
  return 0;

cancel_write:
  graphd_request_error(greq, "SYSTEM unable to write at this time");
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));

  return err;
}
