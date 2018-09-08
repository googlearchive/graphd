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

#include <string.h>
#include <stdio.h>
#include <errno.h>

/*
 * Setup a verify request once we've parsed all of the arguments.
 */
int graphd_verify_setup(graphd_request *greq) {
  int err;
  pdb_handle *pdb;
  graphd_verify_query *q;
  char buf[GRAPH_GUID_SIZE];

  pdb = graphd_request_graphd(greq)->g_pdb;
  q = &greq->greq_verifyquery;

  err = graphd_value_list_alloc(graphd_request_graphd(greq),
                                greq->greq_req.req_cm, graphd_request_cl(greq),
                                &greq->greq_reply, 1);
  if (err) {
    cl_log_errno(graphd_request_cl(greq), CL_LEVEL_ERROR,
                 "graphd_value_list_alloc", err,
                 "Can't allocate result value list");
    return ENOMEM;
  }

  /*
   * The first array slot stores the number of bad primitives so
   * bad primitives start at slot #1.
   */
  q->verify_result_slot = 1;
  q->verify_count = 0;

  if (GRAPH_GUID_IS_NULL(q->verify_guid_low)) {
    q->verify_pdb_low = 0;
  } else {
    err = pdb_id_from_guid(pdb, &q->verify_pdb_low, &q->verify_guid_low);
    if (err) {
      graphd_request_errprintf(
          greq, false, "%s low=%s: %s",
          err == GRAPHD_ERR_NO ? "SEMANTICS" : "SYSTEM",
          graph_guid_to_string(&q->verify_guid_low, buf, sizeof buf),
          err == GRAPHD_ERR_NO ? "GUID does not exist" : graphd_strerror(err));
      return 0;
    }
  }

  /*
   * q->verify_guid_high is one after the last guid to check.
   */
  if (GRAPH_GUID_IS_NULL(q->verify_guid_high)) {
    q->verify_pdb_high = pdb_primitive_n(pdb);
  } else {
    err = pdb_id_from_guid(pdb, &q->verify_pdb_high, &q->verify_guid_high);
    if (err) {
      graphd_request_errprintf(
          greq, false, "%s high=%s: %s",
          err == GRAPHD_ERR_NO ? "SEMANTICS" : "SYSTEM",
          graph_guid_to_string(&q->verify_guid_high, buf, sizeof buf),
          err == GRAPHD_ERR_NO ? "GUID does not exist" : graphd_strerror(err));
      return 0;
    }
    q->verify_pdb_high++;
  }

  q->verify_id = q->verify_pdb_low;
  if (q->verify_pdb_low >= q->verify_pdb_high) {
    graphd_request_errprintf(
        greq, false, "%s high=%s: %s",
        err == GRAPHD_ERR_NO ? "SEMANTICS" : "SYSTEM",
        graph_guid_to_string(&q->verify_guid_high, buf, sizeof buf),
        err == GRAPHD_ERR_NO ? "GUID does not exist" : graphd_strerror(err));
    return 0;
  }
  return 0;
}

/*
 * Check up to 1000 primitives at a time for correct indexing.
 * After we've found 1000 bad primitives, stop reporting them but keep
 * counting.
 *
 * Generate output that looks like
 *
 * (n (guid errors) (guid errors) (guid errors) ...)
 *
 * n is the number of broken GUIDs and errors is a string defined in
 * pdb-verify.c
 */
int graphd_verify(graphd_request *greq) {
  pdb_handle *pdb;
  int i;
  unsigned long error;
  int err;
  char error_string[100];
  graphd_value *v;
  graph_guid bad_guid;
  cl_handle *cl;

  graphd_verify_query *q;

  q = &greq->greq_verifyquery;
  pdb = graphd_request_graphd(greq)->g_pdb;
  cl = graphd_request_cl(greq);

  for (i = 0; i < 1000; i++, q->verify_id++) {
    /*
     * Are we done?
     */
    if (q->verify_id >= q->verify_pdb_high) {
      /*
       * Set the count for the number of broken
       * primitives.
       */
      graphd_value_number_set(greq->greq_reply.val_list_contents,
                              q->verify_count);

      return 0;
    }

    /*
     * Check one primitive
     */
    err = pdb_verify_id(pdb, q->verify_id, &error);
    if (err) q->verify_count++;

    if (err && q->verify_result_slot <= q->verify_pagesize) {
      v = graphd_value_array_alloc(graphd_request_graphd(greq),
                                   graphd_request_cl(greq), &greq->greq_reply,
                                   q->verify_result_slot + 1);
      if (!v) {
        err = errno ?: ENOMEM;
        cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_value_array_alloc", err,
                     "Can't increase result array size");
        return err;
      }

      err = pdb_verify_render_error(error_string, sizeof error_string, error);
      if (err) {
        cl_log_errno(cl, CL_LEVEL_ERROR, "pdb_verify_render_error", err,
                     "can't render: %lx", error);
        return err;
      }

      err = graphd_value_list_alloc(graphd_request_graphd(greq),
                                    greq->greq_req.req_cm,
                                    graphd_request_cl(greq), v, 2);
      if (err) {
        cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_value_list_alloc", err,
                     "Can't make value list for 2 items");
        return err;
      }

      err = graphd_value_text_strdup(
          greq->greq_req.req_cm, v->val_list_contents + 1, GRAPHD_VALUE_ATOM,
          error_string, error_string + strlen(error_string));
      if (err) {
        cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_value_text_strdup", err,
                     "Can't make value for %zi length "
                     "string",
                     strlen(error_string));
        return err;
      }

      err = pdb_id_to_guid(pdb, q->verify_id, &bad_guid);
      /*
       * This can fail legitimatly if the istore is
       * corrupt. Fake up GUID using only the
       * local ID.
       */

      if (err) {
        bad_guid.guid_a = 0;
        bad_guid.guid_b = q->verify_id;
      }
      graphd_value_guid_set(v->val_list_contents, &bad_guid);

      graphd_value_array_alloc_commit(graphd_request_cl(greq),
                                      &greq->greq_reply, 1);

      q->verify_result_slot++;
    }
  }

  /*
   * Let other requests run for a while.
   */
  return GRAPHD_ERR_MORE;
}
