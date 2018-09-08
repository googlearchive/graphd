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
#include "graphd/graphd-ast.h"
#include "graphd/graphd.h"

/**
 * This function invokes the new recursive-descent parser.
 */
int graphd_request_input(void* data, srv_handle* srv, void* session_data,
                         void* request_data, char** s, char* e,
                         srv_msclock_t deadline) {
  graphd_handle* g = data;
  graphd_session* gses = session_data;
  cl_handle* cl = gses->gses_cl;
  graphd_request* greq = request_data;
  int err;

  cl_assert(cl, data != NULL);
  cl_assert(cl, greq != NULL);

  gses->gses_time_active = g->g_now;

  graphd_ast_debug_reading(greq);
  graphd_runtime_statistics_start_request(greq);

  if (s == NULL) {
    /* end-of-file... whatever we've got so far is junk
     */
    graphd_ast_debug_received(greq, true /* eof */);

    if (greq->greq_request == GRAPHD_REQUEST_UNSPECIFIED)
      graphd_request_become(greq, GRAPHD_REQUEST_SKIP);
    else if (!srv_request_error(&greq->greq_req))
      graphd_request_errprintf(greq, 0, "SYNTAX EOF in request");

    greq->greq_response_ok = false;
    greq->greq_transmission_error = greq->greq_req.req_session->ses_bc.bc_errno;
    if (greq->greq_transmission_error == 0)
      greq->greq_transmission_error = GRAPHD_ERR_NO;

    graphd_request_arrived(greq);
  } else {
    char const* const s0 = *s;

    /* There is at least *some* input!
     */
    cl_assert(cl, *s < e);
    cl_assert(cl, !greq->greq_micro.micro_ready);

    /* find the end of the request
     */
    err = gdp_micro_parse(&greq->greq_micro, s, e);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "gdb_micro_parse", err,
                   "input \"%.*s%s\"", (int)(e - s0 > 4096 ? 4096 : e - s0), s0,
                   (e - s0 > 4096 ? "..." : ""));
      return err;
    }

    /* get the ready flag from the micro_state */
    bool ready = greq->greq_micro.micro_ready;

    /* We learned something new, right?
     */
    cl_assert(cl, *s > s0 || ready);

    /* commit input */
    greq->greq_request_size += *s - s0;
    srv_session_input_commit(session_data, *s);

    /*  If the request is going on for too long, return
     *  a low-level error; libsrv will abort the session.
     */
    if (greq->greq_request_size > GRAPHD_MAX_REQUEST_LENGTH) {
      cl_log(
          cl, CL_LEVEL_OPERATOR_ERROR,
          "graphd_session_parse: request too long: "
          "%zu or more octets - maximum allowed is %zu! "
          "(Request starts: \"%.*s...\")",
          greq->greq_request_size, (size_t)GRAPHD_MAX_REQUEST_LENGTH,
          GRAPHD_MAX_REQUEST_LENGTH > 4096 ? 4096 : GRAPHD_MAX_REQUEST_LENGTH,
          s0);

      return SRV_ERR_REQUEST_TOO_LONG;
    }

    /* have we reached the end of the request?
     */
    if (ready) {
      graphd_ast_debug_received(greq, /* eof? */ false);
      /* this is where the request ends */
      greq->greq_req.req_last_n = greq->greq_req.req_last->b_i;

      /* parse request */
      err = graphd_ast_parse(greq);
      if (err != 0)
        graphd_request_error(greq, "SYNTAX error while parsing request");

      graphd_ast_debug_parsed(greq, err);
      graphd_request_arrived(greq);
    }
  }
  return 0;
}
