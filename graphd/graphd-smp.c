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
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sysexits.h>
#include <sys/resource.h>
#include <sys/time.h>

static const cm_list_offsets graphd_ses_offsets =
    CM_LIST_OFFSET_INIT(graphd_session, gses_data.gd_smp_follower.gdsf_next,
                        gses_data.gd_smp_follower.gdsf_prev);

static int smp_get_num_followers(graphd_handle* g) {
  int count = 0;
  graphd_session* gses;
  gses = g->g_smp_sessions;
  while (gses) {
    graphd_session* const next_ses = gses->gses_data.gd_smp_follower.gdsf_next;
    count += 1;
    gses = next_ses;
  }
  return count;
}

static size_t smp_get_follower_status(graphd_handle* g) {
  size_t count = 0;
  graphd_session* gses;

  for (gses = g->g_smp_sessions; gses != NULL;
       gses = gses->gses_data.gd_smp_follower.gdsf_next) {
    count = count << 4;
    count += gses->gses_data.gd_smp_follower.gdsf_smp_state;
  }
  return count;
}

static size_t smp_get_count_followers_in_state(
    graphd_handle* g, graphd_session_smp_state const state) {
  size_t count = 0;
  graphd_session* gses;

  for (gses = g->g_smp_sessions; gses != NULL;
       gses = gses->gses_data.gd_smp_follower.gdsf_next) {
    if (state == gses->gses_data.gd_smp_follower.gdsf_smp_state) count += 1;
  }
  return count;
}

static void smp_set_timeout_on_unpaused_followers(graphd_handle* g,
                                                  srv_timeout* timeout) {
  graphd_session* gses;
  for (gses = g->g_smp_sessions; gses != NULL;
       gses = gses->gses_data.gd_smp_follower.gdsf_next) {
    if (gses->gses_data.gd_smp_follower.gdsf_smp_state !=
        GRAPHD_SMP_SESSION_PAUSE) {
      cl_log(g->g_cl, CL_LEVEL_DEBUG, "Setting follower timeout on %s",
             gses->gses_ses.ses_displayname);
      srv_session_set_timeout(&gses->gses_ses, timeout);
    }
  }
}

int graphd_smp_test_follower_state(graphd_handle* g,
                                   graphd_session_smp_state state) {
  // Returns true if all the followers are in a given state.
  graphd_session* gses;
  for (gses = g->g_smp_sessions; gses != NULL;
       gses = gses->gses_data.gd_smp_follower.gdsf_next) {
    if (gses->gses_data.gd_smp_follower.gdsf_smp_state != state) {
      return false;
    }
  }
  cl_log(g->g_cl, CL_LEVEL_DEBUG,
         "graphd_smp_test_follower_state: all followers now in state %llu",
         (unsigned long long)state);
  return true;
}

int graphd_suspend_for_smp(graphd_request* greq) {
  graphd_session* gses = graphd_request_session(greq);
  graphd_handle* g = graphd_request_graphd(greq);
  cl_handle* cl = graphd_request_cl(greq);

  cl_assert(cl, gses->gses_suspend_reason == GRAPHD_SUSPEND_NOTHING);
  if (g->g_smp_request != NULL)
    cl_notreached(
        cl,
        "Already have an SMP request in the queue. This should never happen.");
  cl_assert(cl, g->g_smp_request == NULL);
  graphd_request_suspend(greq, GRAPHD_SUSPEND_SMP);
  g->g_smp_request = greq;
  srv_request_link(&greq->greq_req);

  cl_log(gses->gses_cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_SCHEDULER,
         "xstate: session %llu: suspend for SMP",
         (long long)gses->gses_ses.ses_id);

  return 0;
}

int graphd_resume_from_smp(graphd_session* gses) {
  cl_handle* cl = gses->gses_cl;

  cl_assert(cl, GRAPHD_SUSPEND_SMP == gses->gses_suspend_reason);
  graphd_session_resume(gses);

  return 0;
}

static int resume_smp_suspension(void* data, srv_session* ses) {
  graphd_session* const gses = (graphd_session*)ses;

  /* Ignore all the other suspended sessions */
  if (gses->gses_suspend_reason != GRAPHD_SUSPEND_SMP) return 0;

  return graphd_resume_from_smp(gses);
}

void graphd_smp_update_followers(graphd_handle* g) {
  if (g->g_smp_state == GRAPHD_SMP_SESSION_SENT_PAUSE &&
      graphd_smp_test_follower_state(g, GRAPHD_SMP_SESSION_PAUSE)) {
    // If all the followers are paused, then go ahead and
    // unsuspend the request nything waiting for them to be paused

    g->g_smp_state = GRAPHD_SMP_SESSION_PAUSE;
    g->g_smp_cycles++;

    (void)srv_session_list(g->g_srv, resume_smp_suspension, 0);
  } else if (g->g_smp_state == GRAPHD_SMP_SESSION_SENT_RUN &&
             graphd_smp_test_follower_state(g, GRAPHD_SMP_SESSION_RUN)) {
    g->g_smp_state = GRAPHD_SMP_SESSION_RUN;
    g->g_smp_cycles++;
  }
}

static void kill_smp_follower(graphd_session* gses) {
  int err;

  cl_assert(gses->gses_cl, gses->gses_data.gd_smp_follower.gdsf_smp_pid != 0);

  cl_log(gses->gses_cl, CL_LEVEL_OPERATOR_ERROR,
         "We are attempting to kill a "
         "follower process with PID=%d -- "
         "A new one will be spawned.",
         gses->gses_data.gd_smp_follower.gdsf_smp_pid);

  if (!kill(gses->gses_data.gd_smp_follower.gdsf_smp_pid, SIGQUIT)) return;

  err = errno;

  /* There is only one error case we want to recover from,
   * namely, the process is already dead. Shooting a
   * dead horse should work, all others fail assertion
   */
  cl_assert(gses->gses_cl, err == ESRCH);
  cl_log(gses->gses_cl, CL_LEVEL_INFO,
         "Process %u was already dead! Continuing...",
         (unsigned int)gses->gses_data.gd_smp_follower.gdsf_smp_pid);
}

static void smp_ticket_callback(void* data) {
  graphd_handle* g = data;
  cl_handle* cl = g->g_cl;
  srv_request* req;

  /*  I am a follower.
   */
  cl_assert(cl, g->g_smp_proc_type == GRAPHD_SMP_PROCESS_FOLLOWER);
  cl_assert(cl, g->g_smp_leader != NULL);

  cl_log(cl, CL_LEVEL_VERBOSE, "smp_ticket_callback");

  /*  My exclusive ticket is up - nobody else is writing or reading.
   *  (This can be called more than once for the same ticket.)
   */

  /*  If there's a SMP PAUSED response to the leader waiting to
   *  be sent in response to an SMP PREWRITE, enable that response.
   *  Otherwise, just sit there and block everybody else.
   */
  for (req = g->g_smp_leader->gses_ses.ses_request_head; req != NULL;
       req = req->req_next) {
    graphd_request* greq = (graphd_request*)req;

    if (greq->greq_request == GRAPHD_REQUEST_SMP &&
        greq->greq_data.gd_smp.gds_smpcmd == GRAPHD_SMP_PREWRITE &&
        !(req->req_done & (1 << SRV_OUTPUT)) &&
        !(req->req_ready & (1 << SRV_OUTPUT)) &&
        !(req->req_done & (1 << SRV_RUN))) {
      srv_request_run_ready(req);
      break;
    }
  }
}

/* Try to establish an smp connection
 */
int graphd_smp_connect(graphd_handle* g) {
  int err;

  cl_assert(g->g_cl, g->g_smp_proc_type == GRAPHD_SMP_PROCESS_FOLLOWER);

  /* If I have no current SMP connection */

  if (!g->g_smp_leader) {
    cl_assert(g->g_cl, g->g_smp_leader_address != NULL);
    cl_log(g->g_cl, CL_LEVEL_INFO, "Initiating smp connection to: %s",
           g->g_smp_leader_address);

    cl_assert(g->g_cl, g->g_srv != NULL);

    err = srv_interface_connect(g->g_srv, g->g_smp_leader_address,
                                (void*)&g->g_smp_leader);
    if (err) return err;

    cl_assert(g->g_cl, g->g_smp_leader != NULL);
    g->g_smp_leader->gses_type = GRAPHD_SESSION_SMP_LEADER;

    /*  Manually grab a ticket for the session.
     *  This will prevent writes or reads from running while we
     *  haven't received a go-ahead from the server.  The go-ahead
     *  will clear the session's ticket, allowing everybody else
     *  to run.
     */
    cl_assert(g->g_cl, g->g_smp_xstate_ticket == NULL);

    err = graphd_xstate_ticket_get_exclusive(g, smp_ticket_callback, g,
                                             &g->g_smp_xstate_ticket);
    if (err != 0) {
      cl_log_errno(g->g_cl, CL_LEVEL_FAIL, "graphd_xstate_ticket_get_exclusive",
                   err, "couldn't get an SMP connect ticket.");
      return err;
    }

    /* Send an SMP (connect) request to the leader.
     */

    if (graphd_smp_out_request(g, g->g_smp_leader, GRAPHD_SMP_CONNECT) ==
        NULL) {
      srv_session_abort(&g->g_smp_leader->gses_ses);
      g->g_smp_leader = 0;

      return err;
    }

    /* Make it so that all commands coming in will get
       parsed and made into requests */
    srv_session_set_server(&g->g_smp_leader->gses_ses, true);

    cl_log(g->g_cl, CL_LEVEL_SPEW,
           "Sent smp connection to: %s. g_smp_leader = %ld",
           g->g_smp_leader_address, (size_t)g->g_smp_leader);
  }
  return 0;
}

bool graphd_smp_leader_state_machine(graphd_handle* g,
                                     graphd_session_smp_state desired_state) {
  int err = 0;

  cl_assert(g->g_cl, g->g_smp_proc_type == GRAPHD_SMP_PROCESS_LEADER);

  cl_log(g->g_cl, CL_LEVEL_DEBUG,
         "graphd_smp_leader_state_machine: desired: %d, current: %d",
         desired_state, g->g_smp_state);

  graphd_session_smp_state current_state = g->g_smp_state;
  if (desired_state == current_state) return true;

  switch (desired_state) {
    case GRAPHD_SMP_SESSION_RUN:
      // Accepted current states
      //*
      // Guards
      // Entry functions
      err = graphd_smp_broadcast(g, GRAPHD_SMP_POSTWRITE);
      if (err) return false;
      break;

    case GRAPHD_SMP_SESSION_PAUSE:
      // Accepted current states
      if (current_state != GRAPHD_SMP_SESSION_SENT_PAUSE) return false;
      // Guards
      if (!graphd_smp_test_follower_state(g, GRAPHD_SMP_SESSION_PAUSE)) {
        if (smp_get_count_followers_in_state(g, GRAPHD_SMP_SESSION_PAUSE) ==
            (g->g_smp_processes >> 1)) {
          smp_set_timeout_on_unpaused_followers(g, g->g_smp_follower_timeout);
        }
        return false;
      }
      // Entry functions
      /* Test that the paused request is still useful. If it is not,
       * immediately jump to run state (and unpause everyone) */
      if (srv_request_is_complete(&g->g_smp_request->greq_req)) {
        cl_log(g->g_cl, CL_LEVEL_ERROR,
               "Request waiting for SMP cancelled; unpausing");
        g->g_smp_state = desired_state;
        g->g_smp_cycles++;
        /* Mark the request in error -- it will be cleaned up up the stack */
        srv_request_error(&g->g_smp_request->greq_req);
        return graphd_smp_leader_state_machine(g, GRAPHD_SMP_SESSION_RUN);
      }
      graphd_request_resume(g->g_smp_request);
      srv_request_unlink(&g->g_smp_request->greq_req);
      break;

    case GRAPHD_SMP_SESSION_SENT_PAUSE:
      // Accepted current states
      if (current_state != GRAPHD_SMP_SESSION_RUN) return false;
      // Guards
      // Entry functions
      err = graphd_smp_broadcast(g, GRAPHD_SMP_PREWRITE);
      if (err) return false;
      break;

    default:
      return false;
  }

  /* Change State
   */
  g->g_smp_state = desired_state;
  g->g_smp_cycles++;

  cl_log(g->g_cl, CL_LEVEL_DEBUG,
         "graphd_smp_leader_state_machine finished: desired: %d == current: %d",
         desired_state, g->g_smp_state);

  return true;
}

static int graphd_smp_in_connect(graphd_request* greq) {
  // I am the SMP Leader. I have gotten a connection
  // And now need to add it to my list of open followers

  graphd_session* gses = graphd_request_session(greq);
  graphd_handle* g = gses->gses_graphd;
  cl_handle* cl = gses->gses_cl;
  graphd_request* out_greq;

  if (g->g_smp_state != GRAPHD_SMP_SESSION_RUN) return GRAPHD_ERR_SUSPEND;

  cl_log(cl, CL_LEVEL_INFO, "New smp session, %s (id=%llx)",
         gses->gses_ses.ses_displayname, greq->greq_req.req_id);

  cm_list_push(graphd_session, graphd_ses_offsets, &g->g_smp_sessions, 0, gses);

  gses->gses_type = GRAPHD_SESSION_SMP_FOLLOWER;
  gses->gses_data.gd_smp_follower.gdsf_smp_state = GRAPHD_SMP_SESSION_RUN;
  gses->gses_data.gd_smp_follower.gdsf_smp_pid =
      (pid_t)greq->greq_data.gd_smp.gds_smppid;

  srv_session_set_server(&gses->gses_ses, false);

  out_greq = graphd_smp_out_request(g, gses, GRAPHD_SMP_POSTWRITE);
  gses->gses_data.gd_smp_follower.gdsf_smp_state = GRAPHD_SMP_SESSION_RUN;
  if (!out_greq)
    cl_notreached(cl,
                  "Could not create outgoing response to smp-connect. "
                  "This failure is critical.");

  srv_request_complete(&greq->greq_req);

  return 0;
}

int graphd_smp_broadcast(graphd_handle* g, graphd_smp_command cmd) {
  graphd_session* gses;

  cl_assert(g->g_cl, cmd == GRAPHD_SMP_PREWRITE || cmd == GRAPHD_SMP_POSTWRITE);

  for (gses = g->g_smp_sessions; gses != NULL;
       gses = gses->gses_data.gd_smp_follower.gdsf_next) {
    graphd_request* greq;

    if (cmd == GRAPHD_SMP_POSTWRITE) {
      gses->gses_data.gd_smp_follower.gdsf_smp_state = GRAPHD_SMP_SESSION_RUN;
    }
    if (cmd == GRAPHD_SMP_PREWRITE) {
    }

    greq = graphd_smp_out_request(g, gses, cmd);
    if (!greq) return ENOMEM;
  }

  return 0;
}

static int graphd_smp_in_postwrite(graphd_request* greq) {
  int err = 0;
  graphd_handle* g = graphd_request_graphd(greq);
  cl_handle* cl = graphd_request_cl(greq);
  pdb_handle* pdb = g->g_pdb;
  pdb_id start, end;

  graphd_dateline_expire(g);

  start = pdb_primitive_n(pdb);

  cl_log(cl, CL_LEVEL_VERBOSE, "running pdb_refresh!");

  if ((err = pdb_refresh(pdb))) {
    /* XXX */
    cl_notreached(cl,
                  "graphd_smp_in_postwrite: error %s while trying"
                  " to refresh database. Giving up.",
                  graphd_strerror(err));
  }

  end = pdb_primitive_n(pdb);

  graphd_replicate_primitives(g, start, end);

  graphd_xstate_ticket_delete(g, &g->g_smp_xstate_ticket);

  /* srv_request_complete(&greq->greq_req);*/

  graphd_request_output_text(greq, NULL, "ok (running)\n");

  srv_request_output_ready(&greq->greq_req);

  return 0;
}

/*  I am the leader.  greq is an incoming request from one of my followers.
 */
static int graphd_smp_in_paused(graphd_request* greq) {
  graphd_session* gses = graphd_request_session(greq);
  graphd_handle* g = graphd_request_graphd(greq);

  gses->gses_data.gd_smp_follower.gdsf_smp_state = GRAPHD_SMP_SESSION_PAUSE;

  cl_log(g->g_cl, CL_LEVEL_DEBUG, "Removing follower timeout on %s",
         gses->gses_ses.ses_displayname);
  srv_session_set_timeout(&gses->gses_ses, NULL);

  graphd_smp_leader_state_machine(g, GRAPHD_SMP_SESSION_PAUSE);

  return 0;
}

static int graphd_smp_in_prewrite(graphd_request* greq) {
  graphd_handle* g = graphd_request_graphd(greq);

  /*  Pre-write only moves into its output phase once
   *  its ticket is running.
   */
  if (g->g_smp_xstate_ticket == NULL) {
    int err = graphd_xstate_ticket_get_exclusive(g, smp_ticket_callback, g,
                                                 &g->g_smp_xstate_ticket);
    if (err) return err;
    /*  We're not running yet.  Sleep!  If our ticket becomes
     *  due, the notify function will find this request and
     *  wake it up.
     */
    srv_request_suspend(&greq->greq_req);
  }

  if (graphd_xstate_ticket_is_running(g, g->g_smp_xstate_ticket)) {
    graphd_request_output_text(greq, NULL, "ok (paused)\n");
    srv_request_output_ready(&greq->greq_req);
    return 0;
  } else
    return GRAPHD_ERR_SUSPEND;
}

void graphd_smp_session_shutdown(graphd_session* gses) {
  graphd_handle* const g = gses->gses_graphd;

  bool state_transitioned = false;

  if (gses->gses_type == GRAPHD_SESSION_SMP_FOLLOWER) {
    /* We are the leader and this is a dying follower
     */
    cl_log(gses->gses_cl, CL_LEVEL_FAIL, "SMP connection dropped: %s",
           gses->gses_ses.ses_displayname);
    cm_list_remove(graphd_session, graphd_ses_offsets, &g->g_smp_sessions, 0,
                   gses);

    gses->gses_type = GRAPHD_SESSION_UNSPECIFIED;

    cl_log(g->g_cl, CL_LEVEL_DEBUG, "Removing follower timeout on %s",
           gses->gses_ses.ses_displayname);
    srv_session_set_timeout(&gses->gses_ses, NULL);

    kill_smp_follower(gses);

    /* Check to see if things can run now!
     */
    graphd_smp_leader_state_machine(g, GRAPHD_SMP_SESSION_PAUSE);
  } else if (g->g_smp_proc_type == GRAPHD_SMP_PROCESS_FOLLOWER &&
             g->g_smp_leader == gses) {
    /*
     * We are a follower and our leader has left us
     * This is usually not good, unless we're shutting down anyway.
     * In that case, it's normal.
     */

    if (!srv_is_shutting_down(g->g_srv)) {
      cl_log(g->g_cl, CL_LEVEL_DEBUG, "Removing follower timeout on %s",
             gses->gses_ses.ses_displayname);
      /* Exit abnormally. The process will be restarted, but
       * a SIGABRT core file at this line has proven useless
       */
      exit(EX_TEMPFAIL);
    }
  } else if (gses->gses_suspend_reason == GRAPHD_SUSPEND_SMP) {
    /* We're dying and we were suspended for SMP.
     * We should make the follows run again
     */
    state_transitioned =
        graphd_smp_leader_state_machine(g, GRAPHD_SMP_SESSION_RUN);
    cl_assert(g->g_cl, state_transitioned == true);
  }
}

static int graphd_smp_in_run(graphd_request* greq,
                             unsigned long long deadline) {
  graphd_handle* g = graphd_request_graphd(greq);
  cl_handle* cl = graphd_request_cl(greq);
  int err = 0;

  (void)deadline;

  switch (greq->greq_data.gd_smp.gds_smpcmd) {
    case GRAPHD_SMP_PREWRITE:
      err = graphd_smp_in_prewrite(greq);
      break;

    case GRAPHD_SMP_POSTWRITE:
      err = graphd_smp_in_postwrite(greq);
      break;

    case GRAPHD_SMP_STATUS:
      if (g->g_smp_proc_type == GRAPHD_SMP_PROCESS_LEADER) {
        graphd_request_output_text(
            greq, greq->greq_req.req_cm,
            cm_sprintf(greq->greq_req.req_cm,
                       "smp (status -- LEADER pid: %lu "
                       "followers: %d, bitmask: %zu, "
                       "cycles: %llx)\n",
                       (unsigned long)getpid(), smp_get_num_followers(g),
                       smp_get_follower_status(g), g->g_smp_cycles));
      } else {
        graphd_request_output_text(
            greq, greq->greq_req.req_cm,
            cm_sprintf(greq->greq_req.req_cm,
                       "smp (status -- FOLLOWER pid: %u)\n",
                       (unsigned int)getpid()));
      }
      srv_request_run_done(&greq->greq_req);
      srv_request_output_ready(&greq->greq_req);
      break;
    case GRAPHD_SMP_CONNECT:
      err = graphd_smp_in_connect(greq);
      break;
    default:
      /*  PAUSED and CONNECT are handled upon input.
       */
      cl_notreached(cl, "unexpected SMP (in) run for %d",
                    (int)greq->greq_data.gd_smp.gds_smpcmd);
      break;
  }
  return err;
}

static void graphd_smp_in_input_arrived(graphd_request* greq) {
  cl_handle* cl;

  cl = graphd_request_cl(greq);

  switch (greq->greq_data.gd_smp.gds_smpcmd) {
    case GRAPHD_SMP_PREWRITE:
    case GRAPHD_SMP_POSTWRITE:
    case GRAPHD_SMP_CONNECT:
      srv_request_input_done(&greq->greq_req);
      srv_request_run_ready(&greq->greq_req);
      break;

    case GRAPHD_SMP_PAUSED:
      cl_notreached(cl, "Paused command is depricated");
      (void)graphd_smp_in_paused(greq);
      srv_request_complete(&greq->greq_req);
      break;

    case GRAPHD_SMP_STATUS:
      srv_request_input_done(&greq->greq_req);
      srv_request_run_ready(&greq->greq_req);
      break;

    case GRAPHD_SMP_RUNNING:
      /* Ignore */
      srv_request_complete(&greq->greq_req);
      break;

    default:
      cl_notreached(cl, "unexpected smp command type %d",
                    greq->greq_data.gd_smp.gds_smpcmd);
  }
}

static graphd_request_type graphd_smp_in_request = {
    "smp (in)", graphd_smp_in_input_arrived,
    /* graphd_set_output_sent */ NULL, graphd_smp_in_run,
    /* graphd_set_free */ NULL};

int graphd_smp_initialize(graphd_request* greq) {
  greq->greq_request = GRAPHD_REQUEST_SMP;
  greq->greq_type = &graphd_smp_in_request;

  return 0;
}

static void graphd_smp_out_input_arrived(graphd_request* greq) {
  cl_handle* cl = graphd_request_cl(greq);

  switch (greq->greq_data.gd_smp.gds_smpcmd) {
    case GRAPHD_SMP_PREWRITE:
      (void)graphd_smp_in_paused(greq);
      srv_request_complete(&greq->greq_req);
      break;
    case GRAPHD_SMP_POSTWRITE:
      srv_request_complete(&greq->greq_req);
      break;
    case GRAPHD_SMP_CONNECT:
      cl_notreached(cl, "We're running an smp (connect)? Fail.");
      break;
    default:
      cl_notreached(cl,
                    "We didn't create this outgoing SMP request. Epic Fail.");
      break;
  }
  return;
}

/*  Outgoing (asynchronous) SMP commands.
 *
 *  SMP (paused) is not actually an outgoing asynchronous
 *  command; it's a response to SMP (pre-write).
 */

static graphd_request_type graphd_smp_out_request_type = {
    "smp (out)", graphd_smp_out_input_arrived,
    /* graphd_smp_out_output_sent */ NULL,
    /* graphd_smp_out_run */ NULL,
    /* graphd_smp_out_free */ NULL};

graphd_request* graphd_smp_out_request(graphd_handle* g, graphd_session* gses,
                                       graphd_smp_command smpcmd) {
  cl_handle* cl = gses->gses_cl;
  int err;

  graphd_request* const greq =
      (graphd_request*)srv_request_create_outgoing(&gses->gses_ses);
  if (!greq) return NULL;

  greq->greq_request = GRAPHD_REQUEST_SMP_OUT;
  greq->greq_type = &graphd_smp_out_request_type;
  greq->greq_xstate = GRAPHD_XSTATE_NONE;
  greq->greq_data.gd_smp_out.gdso_smpcmd = smpcmd;

  switch (smpcmd) {
    case GRAPHD_SMP_PREWRITE:
      err = graphd_request_output_text(greq, NULL, "smp (pre-write)\n");
      cl_assert(cl, err == 0);
      break;

    case GRAPHD_SMP_POSTWRITE:
      err = graphd_request_output_text(greq, NULL, "smp (post-write)\n");
      cl_assert(cl, err == 0);
      break;

    case GRAPHD_SMP_CONNECT:
      /* This command is asynchronus. After we send it, we become a
       * normal, receiving, server session
       * */
      greq->greq_req.req_done = (1 << SRV_RUN) | (1 << SRV_INPUT);
      err = graphd_request_output_text(
          greq, greq->greq_req.req_cm,
          cm_sprintf(greq->greq_req.req_cm, "smp (connect %lu)\n",
                     (unsigned long)getpid()));
      if (err != 0) {
        srv_request_unlink(&greq->greq_req);
        return NULL;
      }
      break;

    default:
      cl_notreached(cl,
                    "graphd_smp_out_request: "
                    "unexpected cmd %u",
                    (unsigned int)smpcmd);
  }
  return greq;
}

/*
 * @brief Are we paused for writing? If so, return 0.
 *	If not, tell everyone to shut up.
 *	This is tied to the error semantics of write requests
 * @return 0 if we should continue, an error code otherwise
 */

int graphd_smp_pause_for_write(graphd_request* greq) {
  bool state_changed;
  cl_handle* cl = graphd_request_cl(greq);
  graphd_handle* g = graphd_request_graphd(greq);

  cl_log(cl, CL_LEVEL_DEBUG, "smp_pause_for_write in state: %d",
         g->g_smp_state);

  if (g->g_smp_proc_type == GRAPHD_SMP_PROCESS_LEADER) {
    switch (g->g_smp_state) {
      case GRAPHD_SMP_SESSION_RUN:
        if (g->g_smp_sessions) {
          graphd_suspend_for_smp(greq);
          state_changed = graphd_smp_leader_state_machine(
              graphd_request_graphd(greq), GRAPHD_SMP_SESSION_SENT_PAUSE);
          if (!state_changed) {
            /* We couldn't send for some reason
             */
            cl_leave(cl, CL_LEVEL_VERBOSE, "GRAPHD_ERR_SMP");
            return GRAPHD_ERR_SMP;
          }
          cl_leave(cl, CL_LEVEL_VERBOSE, "GRAPHD_ERR_SUSPEND");
          return GRAPHD_ERR_SUSPEND;
        }
        break;
      case GRAPHD_SMP_SESSION_SENT_PAUSE:

        /* We're waiting for the followers to get back
         * to us, and yet the request runs again.
         */
        if (g->g_smp_request != greq) {
          char b1[200];
          char b2[200];
          cl_log(cl, CL_LEVEL_ERROR, "greq = %s, smp_req = %s",
                 graphd_request_to_string(greq, b1, 200),
                 graphd_request_to_string(g->g_smp_request, b2, 200));
          cl_notreached(cl,
                        "A different request is trying to run. "
                        "This is unsupported");
        } else {
          cl_log(cl, CL_LEVEL_INFO,
                 "Request became unsuspended; "
                 "Telling it to go back to sleep.");
          graphd_request_suspend(greq, GRAPHD_SUSPEND_SMP);
        }
        break;
      case GRAPHD_SMP_SESSION_PAUSE:
      default:
        /* Do nothing -- we're paused (return 0 at end
         * of function
         */
        break;
    }
  } else if (g->g_smp_proc_type == GRAPHD_SMP_PROCESS_FOLLOWER) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "GRAPHD_ERR_SMP_WRITE");
    return GRAPHD_ERR_SMP_WRITE;
  }

  return 0;
}

/*
 * @brief  We have finished writing, with the given error code
 *	This is tied to the error semantics of write requests
 *
 * @return 0 if nothing goes wrong with SMP. Another error otherwise
 */

int graphd_smp_resume_for_write(graphd_request* greq) {
  graphd_handle* g = graphd_request_graphd(greq);
  bool state_changed;

  if (g->g_smp_proc_type == GRAPHD_SMP_PROCESS_LEADER) {
    if (g->g_smp_sessions) {
      state_changed =
          graphd_smp_leader_state_machine(g, GRAPHD_SMP_SESSION_RUN);
      if (!state_changed) return GRAPHD_ERR_SMP;
    }
  }
  return 0;
}
