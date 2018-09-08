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

/**
 * @brief To sleep, perchance to dream.
 *
 *  This calback is invoked once every second on all the head
 *  requests of all non-CPU-queued sessions, to take care of
 *  time outs while the session is off doing something else
 *  or waiting for something.
 *
 *  If the session times out, it must be unsuspended by this
 *  callback.
 *
 * @param data	opaque application data; in our case, the graphd handle
 * @param srv	service module pointer
 * @param now 	current time in microseconds
 * @param session_data	opaque application per-session data, gses
 * @param request_data	opaque application per-request data, greq
 * @return 0 on success, nonzero error codes on unexpected
 * 	system errors.
 */
int graphd_sleep(void* data, srv_handle* srv, unsigned long long now,
                 void* session_data, void* request_data) {
  graphd_handle* g = data;
  graphd_session* gses = session_data;
  graphd_request* greq = request_data;
  cl_handle* cl = gses->gses_cl;

  graphd_runtime_statistics report;
  graphd_runtime_statistics my_use;

  cl_assert(cl, gses != NULL);
  cl_assert(cl, greq != NULL);
  cl_assert(cl, g != NULL);

  /*  Only if we don't have an error yet and haven't been served yet...
   */
  if (greq->greq_error_message != NULL ||
      greq->greq_req.req_done & (1 << SRV_RUN))
    return 0;

  /* Haven't started processing this yet?
   */
  if (!greq->greq_runtime_statistics_started) return 0;

  cl_enter(cl, CL_LEVEL_SPEW, "req_id=%llu",
           (unsigned long long)greq->greq_req.req_id);

  /* cumulative - saved starting point = my use */

  greq->greq_runtime_statistics_accumulated.grts_endtoend_micros =
      now -
      greq->greq_runtime_statistics_accumulated.grts_endtoend_micros_start;
  greq->greq_runtime_statistics_accumulated.grts_endtoend_millis =
      greq->greq_runtime_statistics_accumulated.grts_endtoend_micros / 1000;

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
    }

    /*  Returning an error does not require exclusive access
     *  to the database.   Or shared access, for that matter!
     */
    graphd_request_xstate_set(greq, GRAPHD_XSTATE_NONE);
  }
  cl_leave(cl, CL_LEVEL_SPEW, "leave");
  return 0;
}
