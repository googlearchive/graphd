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

/* This is unabashedly a writethrough to the leader.
 * Except it will forward any request you choose to give it.
 * In general, this escalates a request to be answered by another process,
 * or all other processes.
 */

static void link_passthrough(graphd_request* src, graphd_request* dst) {
  cl_handle* const cl = graphd_request_cl(src);

  cl_assert(cl, dst->greq_request == GRAPHD_REQUEST_PASSTHROUGH);

  graphd_request_link_pointer(dst, &src->greq_master_req);
  graphd_request_link_pointer(src, &dst->greq_data.gd_passthrough.gdpt_client);

  srv_request_depend(&src->greq_req, &dst->greq_req);
}

static void unlink_passthrough(graphd_request* src) {
  cl_handle* const cl = graphd_request_cl(src);
  graphd_request* dst;

  if ((dst = src->greq_master_req) != NULL) {
    cl_assert(cl, dst->greq_request == GRAPHD_REQUEST_PASSTHROUGH);
    cl_assert(cl, src->greq_req.req_refcount > 1);

    /*  Remove the priorization dependency of the
     *  passthrough on us.
     */
    srv_request_depend(NULL, &dst->greq_req);

    /*  Unlink the pointers between us and the passthrough.
     */
    graphd_request_unlink_pointer(&dst->greq_data.gd_passthrough.gdpt_client);
    graphd_request_unlink_pointer(&src->greq_master_req);
  }
}

int graphd_leader_passthrough_connect(graphd_handle* g) {
  int err = 0;

  cl_assert(g->g_cl, GRAPHD_SMP_PROCESS_FOLLOWER == g->g_smp_proc_type);
  cl_assert(g->g_cl, g->g_smp_leader_address);
  cl_assert(g->g_cl, strlen(g->g_smp_leader_address) != 0);

  if (!g->g_smp_leader_passthrough) {
    cl_log(g->g_cl, CL_LEVEL_INFO,
           "Initiating leader passthrough connection to: %s",
           g->g_smp_leader_address);

    err = srv_interface_connect(g->g_srv, g->g_smp_leader_address,
                                (void*)&g->g_smp_leader_passthrough);
  }

  return err;
}

/*  This is called in the client_request to pull data out of
 *  its leader request's reply.
 */
static void format_passthrough_response(void* data, srv_handle* srv,
                                        void* session_data, void* request_data,
                                        char** s, char* e) {
  graphd_handle* const g = data;
  graphd_request* const client_request = request_data;
  graphd_request* const leader_request = client_request->greq_master_req;
  cl_handle* const cl = graphd_request_cl(client_request);

  if (leader_request == NULL) {
    /*  Something bad happened.  We don't know what.
     */
    graphd_request_error(client_request,
                         "SYSTEM unexpected error while forwarding "
                         "request to leader");
    return;
  }

  cl_log(cl, CL_LEVEL_SPEW,
         "format_passthrough_response forwarding response "
         "from %llu to %llu",
         leader_request->greq_req.req_id, client_request->greq_req.req_id);

  cl_assert(cl, leader_request->greq_request == GRAPHD_REQUEST_PASSTHROUGH);
  cl_assert(cl, graphd_request_session(leader_request) ==
                    g->g_smp_leader_passthrough);

  if (!graphd_request_copy_request_text(g, client_request, leader_request, s,
                                        e))
    return;

  /*  That's all the output this request will produce.
   */

  /* The passthrough is done.
   */
  srv_request_reply_received(&leader_request->greq_req);
  srv_request_complete(&leader_request->greq_req);
  graphd_request_completed_log(leader_request, "end");

  /* Respond to the original requester
   */
  srv_request_reply_sent(&client_request->greq_req);
  unlink_passthrough(client_request);
  srv_request_complete(&client_request->greq_req);
}

/*  Called at the beginning of the passthrough request to copy
 *  data to the leader's outgoing connection.
 */
static void format_passthrough(void* data, srv_handle* srv, void* session_data,
                               void* request_data, char** s, char* e) {
  graphd_handle* const g = data;
  graphd_request* const leader_request = request_data;
  graphd_request* const client_request =
      leader_request->greq_data.gd_passthrough.gdpt_client;

  if (s == NULL) {
    /*  The line dropped.
     */
    return;
  }

  cl_log(g->g_cl, CL_LEVEL_VERBOSE,
         "format_passthrough forwarding command from %llu to %llu",
         client_request->greq_req.req_id, leader_request->greq_req.req_id);

  /*  Are we finished copying yet?
   */
  if (!graphd_request_copy_request_text(g, leader_request, client_request, s,
                                        e))
    return;

  srv_request_sent(&leader_request->greq_req);
}

static void graphd_leader_passthrough_input_arrived(graphd_request* greq) {
  graphd_request* client_request = NULL;

  client_request = greq->greq_data.gd_passthrough.gdpt_client;
  if (client_request != NULL) {
    char buf[200];

    client_request->greq_format = format_passthrough_response;

    srv_request_output_ready(&client_request->greq_req);

    cl_log(graphd_request_cl(greq), CL_LEVEL_VERBOSE,
           "graphd_leader_passthrough_input_arrived: delivered "
           "response to %s",
           graphd_request_to_string(client_request, buf, sizeof buf));
  } else {
    cl_log(graphd_request_cl(greq), CL_LEVEL_FAIL,
           "graphd_leader_passthrough_input_arrived: dropping "
           "response - no client!");
  }
  /*  This request is done, as far as the session is concerned.
   */
  srv_request_complete(&greq->greq_req);
}

/*  The passthrough connection has dropped.  Best we can do is
 *  to drop our own client connection as well - we don't actually
 *  know whether the request made it or not, and neither can our client.
 */
static void graphd_leader_passthrough_cancel(graphd_request* greq) {
  graphd_request* client_request;

  client_request = greq->greq_data.gd_passthrough.gdpt_client;
  if (client_request != NULL) {
    srv_session_abort(client_request->greq_req.req_session);
    unlink_passthrough(client_request);
  }
}

static void graphd_leader_passthrough_free(graphd_request* greq) {
  graphd_request* client_request;

  client_request = greq->greq_data.gd_passthrough.gdpt_client;
  if (client_request != NULL) unlink_passthrough(client_request);
}

static graphd_request_type graphd_request_leader_passthrough = {
    "leader-passthrough",

    graphd_leader_passthrough_input_arrived,
    NULL,                             /* output sent	*/
    NULL,                             /* run 		*/
    graphd_leader_passthrough_cancel, /* cancel 	*/
    graphd_leader_passthrough_free    /* free 	*/

};

void graphd_leader_passthrough_initialize(graphd_request* greq) {
  if (greq != NULL) {
    greq->greq_request = GRAPHD_REQUEST_PASSTHROUGH;
    greq->greq_type = &graphd_request_leader_passthrough;
    greq->greq_format = format_passthrough;
  }
}

/*  Handle a command at a follower by forwarding it to the leader.
 *
 * @return 0 on successful sent
 * @return other errors in unexpected cases.
 */
int graphd_leader_passthrough(graphd_request* greq) {
  graphd_session* const gses = graphd_request_session(greq);
  graphd_handle* const g = gses->gses_graphd;
  cl_handle* const cl = graphd_request_cl(greq);

  graphd_request* leader_greq;
  int err;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");
  cl_assert(cl, g->g_smp_proc_type == GRAPHD_SMP_PROCESS_FOLLOWER);
  cl_assert(cl, g->g_smp_leader_address != NULL);

  /* Force a connnection to the leader, if one doesn't exist
   * This is a separate connection because it cannot answer an smp request if
   * it is sending down the same connection
   */
  err = graphd_leader_passthrough_connect(g);
  if (err) {
    cl_log_errno(
        cl, CL_LEVEL_OPERATOR_ERROR, "graphd_leader_passthrough_connect", err,
        "Unable to connect to leader at \"%s\"", g->g_smp_leader_address);
    goto cancel;
  }
  cl_assert(cl, g->g_smp_leader_passthrough);

  leader_greq = graphd_request_create_outgoing(g->g_smp_leader_passthrough,
                                               GRAPHD_REQUEST_PASSTHROUGH);
  if (leader_greq == NULL) goto cancel;

  graphd_request_start(leader_greq);
  link_passthrough(greq, leader_greq);

  gses->gses_last_action = "passthrough";
  cl_leave(cl, CL_LEVEL_VERBOSE, "ok");
  return 0;

cancel:
  graphd_request_error(greq, "SYSTEM unable to contact leader");
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));

  return err;
}
