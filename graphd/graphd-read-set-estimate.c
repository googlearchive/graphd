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
#include "graphd/graphd-read.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>

/**
 * @brief What are the performance estimates for this constraint?
 *
 *  An "estimate" is the technical term for a list that encodes
 *  the optimizer metrics for a constraint.  It's at the constraint
 *  expression level (like cursor and count), not at the primitive level.
 *
 *  estimate := ("string" is-sorted check-cost next-cost find-cost n)
 *
 * @param greq		request we're working for
 * @param it 		The constraint iterator the caller is asking about.
 * @param val_out	Assign the estimate list to this.
 *
 * @return 0 on success, an error code on resource failure.
 */
int graphd_read_set_estimate_get(graphd_request* greq, pdb_iterator* it,
                                 graphd_value* val_out) {
  int err;
  graphd_handle* g = graphd_request_graphd(greq);
  cl_handle* cl = graphd_request_cl(greq);
  cm_handle* cm = greq->greq_req.req_cm;
  graphd_value* el;
  char buf[200];
  char const* str;

  err = graphd_value_list_alloc(g, cm, cl, val_out, 6);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_value_list_alloc", err,
                 "can't allocate six elements for an estimate?");
    return err;
  }

  el = val_out->val_list_contents;

  /* estimate[0] - the iterator string.
   */
  str = pdb_iterator_to_string(g->g_pdb, it, buf, sizeof buf);
  if (str == NULL)
    graphd_value_null_set(el);
  else {
    err = graphd_value_text_strdup(cm, el, GRAPHD_VALUE_STRING, str,
                                   str + strlen(str));
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_value_list_alloc", err,
                   "can't duplicate iterator string?");
      graphd_value_finish(cl, val_out);
      return err;
    }
  }
  el++;

  /* estimate[1] - is_sorted: bool
   */
  if (pdb_iterator_sorted_valid(g->g_pdb, it))
    graphd_value_boolean_set(el, pdb_iterator_sorted(g->g_pdb, it));
  else
    graphd_value_null_set(el);
  el++;

  /* estimate[2] - check-cost
   */
  if (pdb_iterator_check_cost_valid(g->g_pdb, it))
    graphd_value_number_set(el, pdb_iterator_check_cost(g->g_pdb, it));
  else
    graphd_value_null_set(el);
  el++;

  /* estimate[3] - next-cost
   */
  if (pdb_iterator_next_cost_valid(g->g_pdb, it))
    graphd_value_number_set(el, pdb_iterator_next_cost(g->g_pdb, it));
  else
    graphd_value_null_set(el);
  el++;

  /* estimate[4] - find-cost
   */
  if (pdb_iterator_find_cost_valid(g->g_pdb, it))
    graphd_value_number_set(el, pdb_iterator_find_cost(g->g_pdb, it));
  else
    graphd_value_null_set(el);
  el++;

  /* estimate[5] - n
   */
  if (pdb_iterator_n_valid(g->g_pdb, it))
    graphd_value_number_set(el, pdb_iterator_n(g->g_pdb, it));
  else
    graphd_value_null_set(el);
  el++;

  return 0;
}
