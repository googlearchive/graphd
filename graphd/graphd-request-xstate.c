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
#include <stdio.h>
#include <string.h>
#include <sysexits.h> /* EX_OSERR */

static void activate_request(void* request_data) {
  graphd_request* greq = request_data;
  cl_handle* cl = graphd_request_cl(greq);

  cl_assert(cl, greq->greq_xstate_ticket != NULL);

  if (!(greq->greq_req.req_done & (1 << SRV_INPUT)))
    srv_request_input_ready(&greq->greq_req);

  else if (!(greq->greq_req.req_done & (1 << SRV_RUN)))
    srv_request_run_ready(&greq->greq_req);

  else if (!(greq->greq_req.req_done & (1 << SRV_OUTPUT)))
    srv_request_output_ready(&greq->greq_req);
  else
    graphd_xstate_ticket_delete(graphd_request_graphd(greq),
                                &greq->greq_xstate_ticket);
}

/*  If the caller's session doesn't have the
 *  appropriate ticket, assign one.
 *
 *  If it's an exclusive ticket, guarantee that
 *  nobody else has the same ticket.
 */
int graphd_request_xstate_get_ticket(graphd_request* greq) {
  graphd_handle* g = graphd_request_graphd(greq);
  int err = 0;

  /*  Already has a ticket?
   */
  if (greq->greq_xstate_ticket == NULL) switch (greq->greq_xstate) {
      case GRAPHD_XSTATE_SHARED:
        err = graphd_xstate_ticket_get_shared(g, activate_request, greq,
                                              &greq->greq_xstate_ticket);
        break;

      case GRAPHD_XSTATE_EXCLUSIVE:
        err = graphd_xstate_ticket_get_exclusive(g, activate_request, greq,
                                                 &greq->greq_xstate_ticket);
        break;

      default:
        break;
    }

  return err;
}

/*  Set the "exclusiveness" state of a request.
 *  (I.e., acquire or give up an exclusive lock).
 *
 *  Sessions can change exclusiveness multiple times;
 *  transaction boundaries do not last past one
 *  exclusiveness change.  (That is, things that weren't
 *  true earlier must be remeasured.)
 */
int graphd_request_xstate_set(graphd_request* greq, int type) {
  graphd_handle* g = graphd_request_graphd(greq);
  int err = 0;

  /* Already set that way?
   */
  if (greq->greq_xstate == type) return 0;

  /* Suspend the request.
   */
  graphd_request_suspend(greq, GRAPHD_SUSPEND_XSTATE);

  /* Free the old ticket.
   */
  if (greq->greq_xstate_ticket != NULL)
    graphd_xstate_ticket_delete(g, &greq->greq_xstate_ticket);

  /* Allocate a new ticket
   */
  greq->greq_xstate = type;

  err = graphd_request_xstate_get_ticket(greq);
  if (err != 0) return err;

  /*  The request is only as running as its ticket is -
   *  if it still requires one.
   */
  if (greq->greq_xstate_ticket == NULL ||
      graphd_xstate_ticket_is_running(g, greq->greq_xstate_ticket))
    graphd_request_resume(greq);
  return 0;
}

/*  If the caller has been running for a while, would it help if
 *  it took a break?  If yes, take one and return true.
 */
bool graphd_request_xstate_break(graphd_request* greq) {
  graphd_handle* g = graphd_request_graphd(greq);

  if (greq->greq_xstate_ticket == NULL ||
      greq->greq_xstate == GRAPHD_XSTATE_EXCLUSIVE ||
      !graphd_xstate_ticket_is_running(g, greq->greq_xstate_ticket) ||
      !graphd_xstate_any_waiting_behind(greq->greq_xstate_ticket))

    return false;

  /*  Go back to the end of the line.
   */
  graphd_xstate_ticket_reissue(g, greq->greq_xstate_ticket, greq->greq_xstate);

  return !graphd_xstate_ticket_is_running(g, greq->greq_xstate_ticket);
}

/* What kind of ticket does this request need?
 */
int graphd_request_xstate_type(graphd_request const* greq) {
  graphd_handle const* g = graphd_request_graphd(greq);

  if (srv_request_error(&greq->greq_req)) return GRAPHD_XSTATE_NONE;

  switch (greq->greq_request) {
    case GRAPHD_REQUEST_VERIFY:
    case GRAPHD_REQUEST_READ:
    case GRAPHD_REQUEST_ITERATE:
    case GRAPHD_REQUEST_DUMP:
      return GRAPHD_XSTATE_SHARED;

    case GRAPHD_REQUEST_SYNC:
      return g->g_smp_proc_type == GRAPHD_SMP_PROCESS_FOLLOWER
                 ? GRAPHD_XSTATE_SHARED
                 : GRAPHD_XSTATE_EXCLUSIVE;

    case GRAPHD_REQUEST_WRITE:
      return g->g_access != GRAPHD_ACCESS_REPLICA &&
                     g->g_access != GRAPHD_ACCESS_REPLICA_SYNC

                 ? GRAPHD_XSTATE_EXCLUSIVE
                 : GRAPHD_XSTATE_NONE;

    case GRAPHD_REQUEST_STATUS:
      return g->g_smp_proc_type == GRAPHD_SMP_PROCESS_LEADER
                 ? GRAPHD_XSTATE_SHARED
                 : GRAPHD_XSTATE_NONE;

    case GRAPHD_REQUEST_RESTORE:
    case GRAPHD_REQUEST_REPLICA_WRITE:
      return GRAPHD_XSTATE_EXCLUSIVE;

    default:
      return GRAPHD_XSTATE_NONE;
  }
}
