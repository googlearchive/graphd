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
#include <sys/resource.h>
#include <sys/time.h>

static int graphd_smp_forward_run(graphd_request *greq,
                                  unsigned long long deadline) {
  greq->greq_data.gd_smp_forward.gdsf_finished = true;
  srv_request_run_ready(&greq->greq_master_req->greq_req);
  srv_request_complete(&greq->greq_req);
  return 0;
}

static void format_outgoing_smp_forward(void *data, srv_handle *srv,
                                        void *session_data, void *request_data,
                                        char **s, char *e) {
  graphd_handle *const g = data;
  graphd_request *const master_request = request_data;
  graphd_request *const client_request = master_request->greq_master_req;

  if (s == NULL) {
    /*  The line dropped.
     */
    return;
  }

  cl_log(g->g_cl, CL_LEVEL_VERBOSE,
         "forwarding status command from %llu to %llu",
         client_request->greq_req.req_id, master_request->greq_req.req_id);

  /*  Are we finished copying yet?
   */
  if (!graphd_request_copy_request_text(g, master_request, client_request, s,
                                        e))
    return;

  srv_request_sent(&master_request->greq_req);
}

static graphd_request_type graphd_smp_forward_request = {
    "smp-forward",
    /*graphd_status_input_arrived */ NULL,
    /* graphd_status_output_sent */ NULL, graphd_smp_forward_run,
    /* graphd_status_free */ NULL};

graphd_request *graphd_smp_forward_outgoing_request(
    graphd_handle *g, graphd_session *gses, graphd_request *client_req) {
  graphd_request *greq =
      (graphd_request *)srv_request_create_outgoing(&gses->gses_ses);

  if (!greq) return NULL;

  greq->greq_request = GRAPHD_REQUEST_SMP_FORWARD;
  greq->greq_type = &graphd_smp_forward_request;
  greq->greq_xstate = GRAPHD_XSTATE_NONE;
  greq->greq_format = format_outgoing_smp_forward;
  greq->greq_data.gd_smp_forward.gdsf_finished = false;

  greq->greq_master_req = client_req;

  return greq;
}

int graphd_smp_start_forward_outgoing(graphd_request *greq) {
  graphd_handle *g = graphd_request_graphd(greq);
  graphd_session *out_ses;
  graphd_request **chain_tail;

  cl_assert(g->g_cl, g->g_smp_proc_type == GRAPHD_SMP_PROCESS_LEADER);
  cl_assert(g->g_cl, greq->greq_smp_request_collection_chain == NULL);

  chain_tail = &greq->greq_smp_request_collection_chain;

  for (out_ses = g->g_smp_sessions; out_ses != NULL;
       out_ses = out_ses->gses_data.gd_smp_follower.gdsf_next) {
    graphd_request *out_req;

    out_req = graphd_smp_forward_outgoing_request(g, out_ses, greq);
    if (!out_req) {
      graphd_request_error(greq, "SYSTEM unable to contact all followers");
      return GRAPHD_ERR_NO;
    }
    srv_request_link(&out_req->greq_req);
    srv_request_depend(&greq->greq_req, &out_req->greq_req);
    *chain_tail = out_req;
    chain_tail =
        &out_req->greq_data.gd_smp_forward.gdsf_request_collection_next;
  }

  greq->greq_smp_forward_started = true;
  *chain_tail = NULL;
  return 0;
}

/* Given a malloced string ready to be tokenized, find the body of the response
 * and put the first token in the token pair
 */

int graphd_smp_status_init_tokens(graphd_request *greq) {
  graphd_handle *g = graphd_request_graphd(greq);
  graphd_request *out_req = NULL;
  int err = 0;
  int n;
  char const *tok_s;
  char const *tok_e;

  cl_assert(g->g_cl, g->g_smp_proc_type == GRAPHD_SMP_PROCESS_LEADER);

  for (out_req = greq->greq_smp_request_collection_chain; out_req != NULL;
       out_req =
           out_req->greq_data.gd_smp_forward.gdsf_request_collection_next) {
    cl_assert(g->g_cl, out_req->greq_request == GRAPHD_REQUEST_SMP_FORWARD);

    if (out_req->greq_data.gd_smp_forward.gdsf_malloced_buf != NULL)
      cm_free(out_req->greq_req.req_cm,
              out_req->greq_data.gd_smp_forward.gdsf_malloced_buf);

    graphd_request_as_malloced_string(
        out_req, &out_req->greq_data.gd_smp_forward.gdsf_malloced_buf,
        &out_req->gdsf_s, &n);

    out_req->gdsf_e = (char *)(out_req->gdsf_s + n);

    do {
      err = graphd_next_expression(&out_req->gdsf_s, out_req->gdsf_e, &tok_s,
                                   &tok_e);
      if (err) return err;

    } while (*tok_s != '(');

    out_req->gdsf_s = tok_s + 1;
    out_req->gdsf_e = tok_e - 1;
  }
  return 0;
}

int graphd_smp_status_append_to_list(graphd_request *greq, graphd_value *list) {
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  graphd_handle *g = graphd_request_graphd(greq);
  graphd_value *val;
  int err = 0;
  char const *s, *e, *tok_s, *tok_e;
  graphd_request *out_req;

  for (out_req = greq->greq_smp_request_collection_chain; out_req != NULL;
       out_req =
           out_req->greq_data.gd_smp_forward.gdsf_request_collection_next) {
    cl_assert(cl, out_req->greq_request == GRAPHD_REQUEST_SMP_FORWARD);
    cl_assert(cl, *out_req->gdsf_tok_s == '(');

    s = out_req->greq_data.gd_smp_forward.gdsf_response_tok_s + 1;
    e = (char *)out_req->greq_data.gd_smp_forward.gdsf_response_tok_e - 1;

    /* skip the conn_version */

    err = graphd_next_expression(&s, e, &tok_s, &tok_e);
    while (err == 0) {
      err = graphd_next_expression(&s, e, &tok_s, &tok_e);
      if (err) continue;

      val = graphd_value_array_alloc(g, cl, list, 1);
      if (val == NULL) return ENOMEM;

      graphd_value_text_strdup(cm, val, GRAPHD_VALUE_ATOM, tok_s, tok_e);

      graphd_value_array_alloc_commit(cl, list, 1);
    }

    if (err != GRAPHD_ERR_NO) return err;
  }
  return 0;
}

bool graphd_smp_finished_forward_outgoing(graphd_request *greq) {
  graphd_handle *g = graphd_request_graphd(greq);
  graphd_request *out_req = NULL;

  cl_assert(g->g_cl, g->g_smp_proc_type == GRAPHD_SMP_PROCESS_LEADER);

  for (out_req = greq->greq_smp_request_collection_chain; out_req != NULL;
       out_req =
           out_req->greq_data.gd_smp_forward.gdsf_request_collection_next) {
    if (out_req->greq_data.gd_smp_forward.gdsf_finished == false) return false;
  }
  return true;
}

int graphd_smp_status_next_tokens(graphd_request *greq) {
  graphd_request *out_req = NULL;
  int err = 0;

  for (out_req = greq->greq_smp_request_collection_chain; out_req != NULL;
       out_req =
           out_req->greq_data.gd_smp_forward.gdsf_request_collection_next) {
    err = graphd_next_expression(&out_req->gdsf_s, out_req->gdsf_e,
                                 &out_req->gdsf_tok_s, &out_req->gdsf_tok_e);
    if (err) return err;
  }
  return err;
}

void graphd_smp_forward_unlink_all(graphd_request *greq) {
  graphd_request *out_req = NULL;
  graphd_request *next = NULL;

  out_req = greq->greq_smp_request_collection_chain;
  while (out_req != NULL) {
    next = out_req->greq_data.gd_smp_forward.gdsf_request_collection_next;
    srv_request_unlink(&out_req->greq_req);
    out_req = next;
  }
}
