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
#include "graphd/graphd-ast-debug.h"
#include "graphd/graphd.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sysexits.h> /* EX_UNAVAILABLE */

#define DEADBEEF 1

/*  If a read request has been running for longer
 *  than this, it should freeze itself and let
 *  the other requests run for a while.
 */
#define GRAPHD_FREEZE_TIMESLICE_MILLIS 50

/**
 * @brief Should this request take a break?
 * @param greq	The request that might get suspsended.
 *
 *
 */
static bool graphd_serve_break(graphd_request* greq) {
  unsigned long long run_millis;

  /*  We can only suspend reads or verifys and
   *  only while they're still thinking.
   */
  if (greq->greq_req.req_done & (1 << SRV_RUN)) return false;

  if (greq->greq_request != GRAPHD_REQUEST_READ &&
      greq->greq_request != GRAPHD_REQUEST_VERIFY)
    return false;

  /*  Has this been running longer than very briefly?
   * (Say, 50 millis)
   */
  run_millis =
      (greq->greq_runtime_statistics_accumulated.grts_wall_micros) / 1000;

  if (run_millis <= GRAPHD_FREEZE_TIMESLICE_MILLIS) return false;

  return graphd_request_xstate_break(greq);
}

#ifdef DEADBEEF

void Oxdeadbeef(void);
void Oxdeadbeef(void) {
  char buf[1024 * 16];

  memset(buf, 0xdb, sizeof buf);
  if (!buf[1025]) printf("%s\n", buf);
}

#endif /* DEADBEEF */

/*
 * Does the instance ID for the dateline in greq match this
 * graphd instance ID?
 */
static bool dateline_compatible(graphd_handle* g, graphd_request* greq) {
  char const* our_instance_id;

  our_instance_id = graph_dateline_instance_id(greq->greq_dateline);
  if (our_instance_id == NULL) return g->g_instance_id[0] == 0;

  return strcmp(our_instance_id, g->g_instance_id) == 0;
}

/**
 * @brief Go compute results for this request.
 *
 * @param greq 		The request we're asking about.
 * @param deadline 	Run until this many milliseconds
 */

int graphd_request_run(void* data, srv_handle* srv, void* session_data,
                       void* request_data, unsigned long long deadline) {
  graphd_request* greq = request_data;
  graphd_session* gses = session_data;
  int err = 0;
  graphd_handle* g = gses->gses_graphd;
  cl_handle* cl = gses->gses_cl;
  cl_loglevel_configuration clc;
  bool clc_saved = false;
  bool served = false;
  unsigned long long my_dateline;

  /* Only if we don't have an error yet...
   */
  if (greq->greq_error_message == NULL) {
    /*  Does the global access mode allow this?  Global
     *  access mode is things like "read-only".
     */
    if (!graphd_access_allow_global(g, greq)) {
      /*  If our access was denied, we have received
       *  an explanation just now.
       */
      cl_assert(cl, greq->greq_error_message != NULL);
    }
  }

  if (greq->greq_xstate_ticket == NULL &&
      (err = graphd_request_xstate_get_ticket(greq))) {
    char buf[200];
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_request_xstate_get_ticket", err,
                 "req=%s", graphd_request_to_string(greq, buf, sizeof buf));
    goto err;
  }

  if (greq->greq_xstate_ticket != NULL &&
      !graphd_xstate_ticket_is_running(g, greq->greq_xstate_ticket)) {
    /* We can't run yet.  Wait our turn.
     */
    cl_assert(cl, !(greq->greq_req.req_done & (1 << SRV_RUN)));
    srv_request_suspend(&greq->greq_req);

    cl_log(cl, CL_LEVEL_DEBUG,
           "graphd_request_run: suspending (waiting for "
           "xstate ticket)");
    return 0;
  }

  graphd_ast_debug_serving(greq);
  graphd_request_timer_start(greq, 1ul * 1000 * 1000);

  graphd_runtime_statistics_start_request(greq);

  if (greq->greq_loglevel_valid || gses->gses_loglevel_valid) {
    cl_loglevel_configuration lev;

    clc_saved = true;
    cl_get_loglevel_configuration(cl, &clc);
    lev = clc;

    if (gses->gses_loglevel_valid)
      cl_loglevel_configuration_max(&lev, &gses->gses_loglevel, &lev);

    if (greq->greq_loglevel_valid)
      cl_loglevel_configuration_max(&lev, &greq->greq_loglevel, &lev);

    cl_set_loglevel_configuration(cl, &lev);
  }

  /*  Does this request require a dateline - partial server replication
   *  state - that we don't ourselves have, yet?
   *
   *  If the instance IDs do not match, ignore the dateline and do
   *  the request anyways.
   */
  if (greq->greq_error_message == NULL && greq->greq_dateline != NULL &&
      graph_dateline_get(greq->greq_dateline, pdb_database_id(g->g_pdb),
                         &my_dateline) == 0 &&
      my_dateline != PDB_ITERATOR_HIGH_ANY) {
    if (!dateline_compatible(g, greq)) {
      graphd_request_error(greq,
                           "DATELINE your dateline is not valid"
                           " on this server");

      /* Fallthrough to the error message
       * printing below
       */

    } else if (pdb_primitive_n(g->g_pdb) < my_dateline) {
      /* Fail the request now if we're not likely to
       * get up to the dateline anytime soon....
       */

      if (g->g_access == GRAPHD_ACCESS_REPLICA_SYNC ||
          g->g_access == GRAPHD_ACCESS_READ_ONLY) {
        cl_log(cl, CL_LEVEL_VERBOSE,
               "graphd_request_run: would suspend for "
               "dateline %llx, but we are read-only",
               my_dateline);
        graphd_request_error(greq,
                             "AGAIN graph is currently not accepting "
                             "future datelines (read-only)");

        /* Fallthrough to the error message
         * printing below
         */
      } else {
        cl_log(cl, CL_LEVEL_VERBOSE,
               "graphd_request_run: suspending for "
               "dateline %llx",
               my_dateline);

        /*  If we had a ticket, drop it - we'll reacquire
         *  one once our dateline has arrived.  No point in
         *  holding up other sessions!
         */
        graphd_xstate_ticket_delete(g, &greq->greq_xstate_ticket);

        graphd_request_suspend_for_dateline(greq, my_dateline);
        graphd_request_diary_log(
            greq, greq->greq_runtime_statistics.grts_wall_micros / 1000ull,
            "DATELINE");

        return 0;
      }
    }
  }

  pdb_iterator_chain_set(g->g_pdb, &greq->greq_iterator_chain);

  if (greq->greq_pushed_back || greq->greq_iterator_chain.pic_n_suspended) {
    err = graphd_request_push_back_resume(greq);
    if (err != 0) goto err;
  }

  graphd_request_diary_log(
      greq, greq->greq_runtime_statistics.grts_wall_micros / 1000ull, "RUN");

  /*  If we have an error message, we're ready to print it.
   */
  if (greq->greq_error_message != NULL) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_request_run: non-NULL error message \"%s\"",
           greq->greq_error_message);

    if (!(greq->greq_req.req_done & (1 << SRV_OUTPUT)))
      srv_request_output_ready(&greq->greq_req);

    served = true;
    goto done;
  }

  /*  If we have a per-request-type run function, call it.
   */
  if (greq->greq_type != NULL && greq->greq_type->grt_run != NULL) {
    char const* const name = greq->greq_type->grt_name;
    char buf[200];

    gses->gses_last_action = greq->greq_type->grt_name;

    cl_enter(cl, CL_LEVEL_VERBOSE, "graphd_request_run: %s->grt_run(%s, %llu)",
             name, graphd_request_to_string(greq, buf, sizeof buf),
             (unsigned long long)deadline);

    err = (*greq->greq_type->grt_run)(greq, deadline);

    /*  In case of error, the request type may be NULL after
     *  the request has run - that's why we saved the constant
     *  name pointer before the call.
     */

    cl_leave(cl, CL_LEVEL_VERBOSE, "graphd_request_run: %s->grt_run(%s): %s",
             name, graphd_request_to_string(greq, buf, sizeof buf),
             err ? graphd_strerror(err) : "ok");

    if (err == GRAPHD_ERR_MORE || err == GRAPHD_ERR_SUSPEND) goto done;

    served = true;
  } else {
    char buf[200];
    cl_log(cl, CL_LEVEL_VERBOSE, "graphd_request_run %s %llu",
           graphd_request_to_string(greq, buf, sizeof buf),
           (unsigned long long)deadline);

    switch (greq->greq_request) {
      case GRAPHD_REQUEST_SKIP:
        gses->gses_last_action = "skip";
        srv_request_run_done(&greq->greq_req);
        srv_request_output_done(&greq->greq_req);
        goto done;

      /* FALLTHROUGH */

      case GRAPHD_REQUEST_UNSPECIFIED:
        /* An empty request. */
        served = true;
        break;

      case GRAPHD_REQUEST_READ:
        gses->gses_last_action = "read";

        err = graphd_read(greq, deadline);
        if (err == GRAPHD_ERR_MORE) {
          static size_t count = 0;

          if (g->g_freeze <= 0) goto done;

          if (++count >= g->g_freeze) {
            count = 0;

            err = graphd_request_push_back(greq);
            if (err != 0) {
              cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_request_freeze", err,
                           "unexpected error freezing "
                           "request");
              goto err;
            }
            err = GRAPHD_ERR_MORE;
          }
          goto done;
        }
        if (g->g_sabotage != NULL && g->g_sabotage->gs_deadbeef) Oxdeadbeef();
        served = true;
        break;

      case GRAPHD_REQUEST_ISLINK:
        gses->gses_last_action = "islink";
        err = graphd_islink(greq, deadline);
        if (err == GRAPHD_ERR_MORE) goto done;
        served = true;
        break;

      case GRAPHD_REQUEST_ITERATE:
        gses->gses_last_action = "iterate";
        err = graphd_read(greq, deadline);

        /* Sic.  After parsing its value and setting up,
         * the "read" request will check whether it is
         * really an "iterate" and then calls a different
         * handler, graphd_iterate_constraint_push(),
         * instead of graphd_read_constraint_push().
         */
        if (err == GRAPHD_ERR_MORE) goto done;
        served = true;
        break;

      case GRAPHD_REQUEST_RESTORE:

        gses->gses_last_action = "restore";

        err = graphd_restore(greq);
        if ((err == EFAULT || err == GRAPHD_ERR_RESTORE_MISMATCH) &&
            g->g_rep_master == gses) {
          cl_log(gses->gses_cl, CL_LEVEL_FATAL,
                 "replicated data from master %s "
                 "clashes with on-disk data in %s - "
                 "fatal configuration or database "
                 "error!",
                 gses->gses_ses.ses_displayname, pdb_database_path(g->g_pdb));

          /*  Don't restart!
           */
          srv_shared_set_restart(gses->gses_ses.ses_srv, false);
          srv_epitaph_print(gses->gses_ses.ses_srv, EX_GRAPHD_REPLICA_STREAM,
                            "Replicated data from master %s "
                            "clashes with on-disk data in %s - "
                            "fatal configuration or database "
                            "error!",
                            gses->gses_ses.ses_displayname,
                            pdb_database_path(g->g_pdb));
          exit(EX_GRAPHD_REPLICA_STREAM);
        }
        if (err == GRAPHD_ERR_SUSPEND || err == GRAPHD_ERR_MORE) goto done;

        served = true;
        break;

      case GRAPHD_REQUEST_SET:
        gses->gses_last_action = "set";
        err = graphd_set(greq);
        served = true;
        break;

      case GRAPHD_REQUEST_SMP:
        served = true;
        break;

      case GRAPHD_REQUEST_ERROR:
        gses->gses_last_action = "error";
        err = GRAPHD_ERR_SYNTAX;
        served = true;
        break;

      case GRAPHD_REQUEST_VERIFY:
        gses->gses_last_action = "verify";
        err = graphd_verify(greq);
        if (err == GRAPHD_ERR_MORE) goto done;
        served = true;
        break;

      case GRAPHD_REQUEST_REPLICA:
        gses->gses_last_action = "replica";
        err = graphd_replica(greq);
        served = true;
        break;

      case GRAPHD_REQUEST_REPLICA_WRITE:
        if (!graphd_session_receives_replica_write(gses)) {
          err = GRAPHD_ERR_SEMANTICS;
          cl_log(gses->gses_cl, CL_LEVEL_FAIL,
                 "attempted \"replica-write\" on an "
                 "ordinary session: \"%s\"",
                 gses->gses_ses.ses_displayname);
          graphd_request_error(greq,
                               "NOTREPLICA attempted replica-write "
                               "on an ordinary session");
        } else {
          gses->gses_last_action = "replica-write";
          err = graphd_replica_write(greq);
          if (err == GRAPHD_ERR_SUSPEND) goto done;
          served = true;
        }
        break;

      default:
        cl_notreached(cl, "Unexpected request type %d", greq->greq_request);
    }

    /*  Freebie for grandfathered commands: if it will
     *  run some time in the future, and it's done serving,
     *  mark it as ready to output.
     */
    if (served) {
      if (!(greq->greq_req.req_done & (1 << SRV_OUTPUT)))
        srv_request_output_ready(&greq->greq_req);
    }
  }

/* TODO: handle replication errors */

err:
  if (err != 0 && greq->greq_error_message == NULL) {
    if (err == GRAPHD_ERR_SYNTAX || err == GRAPHD_ERR_LEXICAL ||
        err == PDB_ERR_SYNTAX) {
      graphd_request_error(greq, "SYNTAX bad arguments to server request");
    } else if (err == GRAPHD_ERR_NO) {
      graphd_request_error(greq, "EMPTY not found");
    } else if (err == GRAPHD_ERR_SMP_WRITE) {
      graphd_request_error(greq, "SMP writing to a follower");
    } else if (err == ENOMEM)
      graphd_request_error(greq, "SYSTEM out of memory");
    else
      graphd_request_errprintf(greq, 0, "SYSTEM unexpected error: %s",
                               graphd_strerror(err));
  }
done:
  if (served) {
    graphd_ast_debug_finished(greq);
    graphd_request_finish_running(greq);
    graphd_xstate_ticket_delete(g, &greq->greq_xstate_ticket);
    graphd_request_completed_log(greq, "end");
  } else {
    graphd_runtime_statistics report;
    graphd_runtime_statistics my_use;

    graphd_runtime_statistics_accumulate(
        greq, &greq->greq_runtime_statistics_accumulated,
        &greq->greq_runtime_statistics);

    my_use = greq->greq_runtime_statistics_accumulated;

    if (greq->greq_soft_timeout) graphd_runtime_statistics_max(&report);

    if (graphd_runtime_statistics_exceeds(
            &my_use, &greq->greq_runtime_statistics_allowance, &report)) {
      if (greq->greq_soft_timeout) {
        char buf[200];
        char const* ex;

        ex = graphd_cost_limit_to_string(&report, buf, sizeof buf);
        greq->greq_soft_timeout_triggered =
            cm_strmalcpy(greq->greq_req.req_cm, ex);
        if (greq->greq_soft_timeout_triggered == NULL)
          graphd_request_error(greq, "SYSTEM out of memory");
      } else {
        /* Fail the request with a
           "took too long" error.
         */

        cl_assert(cl, !graphd_replica_protocol_session(gses));
        graphd_request_error(greq, "COST allowance exceeded");
        graphd_request_served(greq);
      }
    } else if (!greq->greq_pushed_back && graphd_serve_break(greq)) {
      /*  This request took a while, and there
       *  are others waiting in line behind it.
       *  Suspend ourselves, let the other guys
       *  have a turn, and resume after that.
       */
      err = graphd_request_push_back(greq);
      if (err != 0) {
        graphd_request_errprintf(greq, 0, "SYSTEM unexpected error: %s",
                                 graphd_strerror(err));

        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_request_push_back", err,
                     "(request canceled)");
      }
    }
  }

  pdb_iterator_chain_clear(g->g_pdb, &greq->greq_iterator_chain);

  if (clc_saved) cl_set_loglevel_configuration(cl, &clc);
  return 0;
}
