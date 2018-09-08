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

#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>

int graphd_set(graphd_request *greq) {
  graphd_set_subject *su;
  const graphd_property *gp;
  int err;

  for (su = greq->greq_data.gd_set.gds_setqueue.setqueue_head; su;
       su = su->set_next) {
    cl_log(graphd_request_cl(greq), CL_LEVEL_DEBUG, "graphd_set %.*s=%.*s",
           (int)(su->set_name_e - su->set_name_s), su->set_name_s,
           (int)(su->set_value_e - su->set_value_s), su->set_value_s);

    gp = graphd_property_by_name(su->set_name_s, su->set_name_e);

    if (!gp) {
      graphd_request_errprintf(
          greq, 0, "SEMANTICS cannot set '%.*s': unknown property",
          (int)(su->set_name_e - su->set_name_s), su->set_name_s);
      return 0;
    }
    if (gp->prop_set) {
      /*
       * GRAPHD_ERR_SEMANTICS means that an error message
       * has already been formated
       */
      err = gp->prop_set(gp, greq, su);
      if (err == GRAPHD_ERR_SEMANTICS)
        return 0;
      else if (err)
        return err;
      if (err) return err;
    } else {
      graphd_request_error(greq, "SEMANTICS cannot set a read-only property");
      return 0;
    }
  }

  return 0;
}

static int graphd_set_run(graphd_request *greq, unsigned long long deadline) {
  graphd_session *gses = graphd_request_session(greq);
  graphd_handle *g = graphd_request_graphd(greq);
  int err;

  (void)deadline;
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
  if (g->g_smp_proc_type == GRAPHD_SMP_PROCESS_LEADER) {
    if (!greq->greq_smp_forward_started) {
      err = graphd_smp_start_forward_outgoing(greq);
      if (err) return err;
      /* suspend ourselves -- our subrequests will
       * wake us up
       */

      return GRAPHD_ERR_MORE;
    } else {
      if (!graphd_smp_finished_forward_outgoing(greq)) return GRAPHD_ERR_MORE;

      /* fallthrough */
    }
  }
  err = graphd_set(greq);
  if (err != GRAPHD_ERR_MORE) {
    graphd_request_served(greq);
    graphd_smp_forward_unlink_all(greq);
  }

  return err;
}

static void graphd_set_input_arrived(graphd_request *greq) {
  srv_request_run_ready(&greq->greq_req);
}

static graphd_request_type graphd_set_request = {
    "set", graphd_set_input_arrived,
    /* graphd_set_output_sent */ NULL, graphd_set_run,
    /* graphd_set_free */ NULL};

int graphd_set_initialize(graphd_request *greq) {
  graphd_set_queue *q;

  greq->greq_request = GRAPHD_REQUEST_SET;
  greq->greq_type = &graphd_set_request;

  q = &greq->greq_data.gd_set.gds_setqueue;

  q->setqueue_head = NULL;
  q->setqueue_tail = &q->setqueue_head;

  return 0;
}
