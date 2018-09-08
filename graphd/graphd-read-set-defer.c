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

/*  This module is involved with producing a set of results,
 *  given a constraint and an environment.
 *
 *  That involves
 *  	- producing a set of candidate IDs
 * 	- evaluating each individual ID against the constraint set
 *	- collecting and summarizing results.
 *	- collecting data about the set as a whole.
 *
 *  The production happens on an explicit run stack.
 *
 *  The production has two phases:
 *
 *   1 check acceptance
 *   2 fill in all values.
 *
 *  The result of a positive acceptance check can be a deferred value
 *  (GRAPHD_VALUE_DEFERRED) that triggers further evaluation.
 */

#define GRAPHD_NEXT_BUDGET 10000
#define GRAPHD_STATISTICS_BUDGET 10000

/*  graphd_value.val_deferred methods and type.
 */
static int grsc_deferred_push(graphd_request *greq, graphd_value *);
static int grsc_deferred_suspend(cm_handle *cm, cl_handle *cl,
                                 graphd_value *val);
static int grsc_deferred_unsuspend(cm_handle *cm, cl_handle *cl,
                                   graphd_value *val);
static void grsc_deferred_finish(graphd_value *);

static const graphd_deferred_type grsc_deferred_type = {
    "graphd-read-set (deferred)", grsc_deferred_push, grsc_deferred_suspend,
    grsc_deferred_unsuspend, grsc_deferred_finish

};

static int grsc_deferred_replace(graphd_value *val) {
  graphd_value tmp;
  graphd_deferred_base *db = val->val_deferred_base;
  graphd_read_set_context *grsc = db->db_data;
  graphd_read_base *const grb = grsc->grsc_base;
  graphd_request *const greq = grb->grb_greq;
  graphd_constraint *const con = grsc->grsc_con;
  graphd_handle *const g = graphd_request_graphd(greq);
  cl_handle *const cl = graphd_request_cl(greq);
  cm_handle *const cm = greq->greq_req.req_cm;
  int err;

  cl_assert(cl, grsc->grsc_evaluated);

  if (grsc->grsc_err) return grsc->grsc_err;

  /*  Copy the result out of grsc, as addressed by val.
   */
  cl_assert(cl, val != NULL);
  cl_assert(cl, val->val_type == GRAPHD_VALUE_DEFERRED);

  tmp = *val;
  err = graphd_value_copy(g, cm, cl, val, db->db_result + con->con_pframe_n +
                                              tmp.val_deferred_index);
  if (err) {
    *val = tmp;
    return err;
  }

  {
    char buf[200];
    cl_log(cl, CL_LEVEL_VERBOSE, "grsc_deferred_replace: copied %s to %p",
           graphd_value_to_string(grsc->grsc_result + tmp.val_deferred_index,
                                  buf, sizeof buf),
           (void *)val);
  }

  /*  Free the value we just overwrote.
   */
  graphd_value_finish(cl, &tmp);
  return 0;
}

static void grsc_deferred_deliver(void *data, int err,
                                  graphd_constraint const *con,
                                  graphd_value *res) {
  graphd_value *val = data;
  graphd_deferred_base *db = val->val_deferred_base;
  graphd_read_set_context *grsc = db->db_data;
  cl_handle *const cl = graphd_request_cl(grsc->grsc_base->grb_greq);
  char buf[200];

  cl_log(cl, CL_LEVEL_VERBOSE, "grsc_deferred_deliver val=%s",
         graphd_value_to_string(val, buf, sizeof buf));

  {
    size_t i;
    for (i = 0; i < con->con_pframe_n; i++)
      cl_log(cl, CL_LEVEL_VERBOSE, "grsc_deferred_deliver[%zu] := %s", i,
             graphd_value_to_string(res + i, buf, sizeof buf));
  }

  grsc->grsc_evaluated = true;
  if (err) {
    grsc->grsc_err = err;
  } else {
    memcpy(db->db_result + con->con_pframe_n, res,
           sizeof(*res) * con->con_pframe_n);
    memset(res, 0, sizeof(*res) * con->con_pframe_n);

    grsc->grsc_err = grsc_deferred_replace(val);
  }
}

/*  Evaluate a deferred value.
 */
static int grsc_deferred_push(graphd_request *greq, graphd_value *val) {
  graphd_deferred_base *db = val->val_deferred_base;
  graphd_read_set_context *grsc = db->db_data;
  cl_handle *const cl = graphd_request_cl(grsc->grsc_base->grb_greq);

  /*  If the frame has already been evaluated, we just
   *  need to overwrite the result <val> with the part
   *  addressed by this particular deferred value.
   */
  if (grsc->grsc_evaluated) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "grsc_deferred_push: already "
           "evaluated; replacing...");
    return grsc_deferred_replace(val);
  }

  cl_log(cl, CL_LEVEL_VERBOSE, "grsc_deferred_push: resuming read set %p",
         (void *)grsc);
  graphd_read_set_resume(grsc, grsc_deferred_deliver, val);

  return 0;
}

static int grsc_deferred_suspend(cm_handle *cm, cl_handle *cl,
                                 graphd_value *val) {
  graphd_deferred_base *db = val->val_deferred_base;
  graphd_read_set_context *grsc = db->db_data;
  graphd_constraint *con = grsc->grsc_con;
  size_t i;
  int err;

  if (db->db_suspended) return 0;
  db->db_suspended = true;

  /*  Suspend the saved and temporary values.
   */
  for (i = 0; i < 2 * con->con_pframe_n; i++) {
    err = graphd_value_suspend(cm, cl, db->db_result + i);
    if (err != 0) return err;
  }

  /*  Freeze the values inside the deferred base.
   */
  if (db->db_data != NULL) {
    graphd_read_set_context *grsc = db->db_data;

    err = graphd_read_set_context_suspend(grsc);
    if (err != 0) return err;
  }
  return 0;
}

static int grsc_deferred_unsuspend(cm_handle *cm, cl_handle *cl,
                                   graphd_value *val) {
  graphd_deferred_base *db = val->val_deferred_base;

  if (!db->db_suspended) return 0;
  db->db_suspended = false;

  return 0;
}

/*  The last instance referring to this deferred base
 *  has been finished (probably in the process of being
 *  replaced by a non-deferred result).
 *  Free the base itself.
 */
static void grsc_deferred_finish(graphd_value *val) {
  graphd_deferred_base *db = val->val_deferred_base;
  graphd_read_set_context *grsc = db->db_data;
  graphd_constraint *con = grsc->grsc_con;
  graphd_read_base *const grb = grsc->grsc_base;
  graphd_request *const greq = grb->grb_greq;
  cm_handle *const cm = greq->greq_req.req_cm;
  cl_handle *const cl = graphd_request_cl(greq);
  size_t i;

  cl_log(cl, CL_LEVEL_VERBOSE,
         "grsc_deferred_finish "
         "(db=%p grsc=%p, grsc_link %zu, db_link=%zu->%zu)",
         (void *)db, (void *)grsc, (size_t)grsc->grsc_link, db->db_link,
         db->db_link - 1);

  cl_assert(cl, db->db_link >= 1);
  if (db->db_link-- > 1) return;

  for (i = 0; i < 2 * con->con_pframe_n; i++)
    graphd_value_finish(cl, db->db_result + i);

  cl_assert(cl, grsc != NULL);
  graphd_read_set_free(grsc);

  /*  Free the base itself.
   */
  cm_free(cm, db);
}

/*  Replace the partial lists in grsc with links to deferred
 *  values that push, evaluate, and extract from the grsc frame
 *  when they are evaluated.
 */
int graphd_read_set_defer_results(graphd_read_set_context *grsc,
                                  graphd_value **res_out) {
  graphd_read_base *const grb = grsc->grsc_base;
  graphd_request *const greq = grb->grb_greq;
  graphd_constraint *const con = grsc->grsc_con;
  cm_handle *const cm = greq->greq_req.req_cm;
  cl_handle *const cl = graphd_request_cl(greq);
  graphd_deferred_base *db;
  size_t i;

  cl_enter(cl, CL_LEVEL_VERBOSE,
           "grsc=%p, grsc_link %zu, con=%s, con_pframe_n=%d", (void *)grsc,
           (size_t)grsc->grsc_link, graphd_constraint_to_string(con),
           (int)con->con_pframe_n);
  cl_assert(cl, res_out != NULL);

  /*  Allocate the deferred base to which all those deferred
   *  values will point.
   */
  db = cm_zalloc(cm,
                 sizeof(*db) + 2 * con->con_pframe_n * sizeof(*db->db_result));
  if (db == NULL) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "malloc fails");
    return ENOMEM;
  }
  db->db_result = (graphd_value *)(db + 1);
  db->db_type = &grsc_deferred_type;

  /*  In db, make an array of deferred values.
   */
  for (i = 0; i < con->con_pframe_n; i++) {
    graphd_pattern_frame *pf = con->con_pframe + i;

    if (pf->pf_set == NULL) continue;

    /*  Store an evaluation trigger for grsc in db.
     *  Access of the first of any number of these
     *  will cause evaluation of the deferred base's
     *  context.  Access to the others will copy out
     *  of the results of that deferred evaluation.
     */
    graphd_value_deferred_set(db->db_result + i, i, db);
  }

  /*  If we didn't actually have any values to return
   *  later, free the base and return the normal values.
   */
  if (db->db_link == 0) {
    cm_free(cm, db);
    *res_out = grsc->grsc_result;
  } else {
    /*  Add a link to the stack context, so it won't
     *  be free'ed when it is popped after the deferred
     *  values are returned.  The link is held by db.
     */
    db->db_data = grsc;
    grsc->grsc_link++;

    /* Setting the grb_deferred flag causes the grb
     * stack context to look for, and evaluate, deferred
     * value parts prior to returning its complete results.
     */
    grb->grb_deferred = true;

    /*  Point the grsc results to the values we've just
     *  created in the db.
     *
     *  Now grsc will return deferral triggers instead
     *  of the real values!
     */
    *res_out = db->db_result;
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "db=%p, db_link=%zu, %u grsc link(s)",
           (void *)db, db->db_link, grsc->grsc_link);
  return 0;
}
