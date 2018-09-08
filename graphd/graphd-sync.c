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

/**
 * Execute `sync' command.
 */
int graphd_sync(graphd_request *greq) {
  graphd_value *val = &greq->greq_reply;
  graphd_session *gses = graphd_request_session(greq);
  graphd_handle *g = gses->gses_graphd;
  pdb_configuration *pcf = pdb_config(g->g_pdb);
  bool const sync_tmp = pcf->pcf_sync;
  int err;
  cl_handle *const cl = gses->gses_cl;

  cl_enter(cl, CL_LEVEL_VERBOSE, "graphd_sync (checkpoint state: %d)",
           g->g_checkpoint_state);

  switch (g->g_checkpoint_state) {
    case GRAPHD_CHECKPOINT_CURRENT:
      /* we are starting a checkpoint */
      g->g_checkpoint_req = greq;
      break;

    case GRAPHD_CHECKPOINT_PENDING:

      /* a checkpoint is currently pending; if our sync() request did
       * not start it, then wait for it to complete
       */
      if (g->g_checkpoint_req != greq) {
        /* if the checkpoint was _NOT_ initiated by a sync()
         * command, then do some work on it
         */
        if (g->g_checkpoint_req == NULL) graphd_checkpoint_work(g);

        cl_leave(cl, CL_LEVEL_VERBOSE,
                 "graphd_sync - not my request; do some work");

        return GRAPHD_ERR_MORE;
      }
      break;

    default:
      cl_notreached(g->g_cl, "invalid checkpoint state");
  }

  /* start or continue a checkpoint..
   */
  pcf->pcf_sync = true;  // force hard syncs

  if ((err = graphd_checkpoint_optional(g)) == 0) {
    /* done! reply with the horizon
     */
    addb_istore_id horizon = pdb_checkpoint_horizon(g->g_pdb);
    graphd_value_number_set(val, horizon);

    /* disown the checkpoint
     */
    g->g_checkpoint_req = NULL;
  }

  pcf->pcf_sync = sync_tmp;

  cl_leave(cl, CL_LEVEL_VERBOSE, "leave");
  return err;
}

static int graphd_sync_run(graphd_request *greq, unsigned long long deadline) {
  graphd_session *gses = graphd_request_session(greq);
  graphd_handle *g = gses->gses_graphd;
  int err = 0;

  if (g->g_smp_proc_type == GRAPHD_SMP_PROCESS_FOLLOWER &&
      gses != g->g_smp_leader) {
    /*  We're a follower, forwarding this request.
     *  We're done running, but we won't be ready for
     *  output until the passthrough request is.
     */

    err = graphd_leader_passthrough(greq);

    if (err != GRAPHD_ERR_MORE && err != GRAPHD_ERR_SUSPEND)
      srv_request_run_done(&greq->greq_req);

    else if (err == GRAPHD_ERR_SUSPEND) {
      srv_request_suspend(&greq->greq_req);
      err = GRAPHD_ERR_MORE;
    }
    return err;
  }
  gses->gses_last_action = "sync";
  err = graphd_sync(greq);
  if (err == GRAPHD_ERR_MORE || err == GRAPHD_ERR_SUSPEND) {
    /* We're not ready yet.
     */
    if (err == GRAPHD_ERR_SUSPEND) srv_request_suspend(&greq->greq_req);

    err = GRAPHD_ERR_MORE;
  } else {
    if (err != 0)
      cl_log_errno(gses->gses_cl, CL_LEVEL_FAIL, "graphd_write", err,
                   "unexpected write error");

    /*  Even in the error case, we're ready to
     *  send a reply now.
     */
    graphd_request_served(greq);
  }
  return err;
}

static graphd_request_type graphd_sync_request = {
    "sync",
    /* input-arrived */ NULL,
    /* output-sent 	 */ NULL,
    graphd_sync_run,
    /* graphd_write_cancel */ NULL,
    /* graphd_set_free */ NULL};

void graphd_sync_initialize(graphd_request *greq) {
  if (greq != NULL) {
    greq->greq_request = GRAPHD_REQUEST_SYNC;
    greq->greq_type = &graphd_sync_request;
  }
}
