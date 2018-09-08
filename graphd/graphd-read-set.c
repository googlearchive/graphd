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
 *	- grouping results from the individual ids.
 *	- sorting
 *	- collecting data about the set as a whole.
 *
 *  The production happens on an explicit run stack.
 *
 *  The result of a positive acceptance check can be a deferred value
 *  (GRAPHD_VALUE_DEFERRED) that triggers further evaluation.
 */

#define GRAPHD_NEXT_BUDGET 10000
#define GRAPHD_STATISTICS_BUDGET 10000

/*  cm_resource methods and type.
 */

static void grsc_resource_free(void *resource_manager_data,
                               void *resource_data);

static void grsc_resource_list(void *log_data, void *resource_manager_data,
                               void *resource_data);

static const cm_resource_type grsc_resource_type = {
    "constraint read set context", grsc_resource_free, grsc_resource_list};

/*  graphd_stack methods and type.
 */

static int grsc_stack_suspend(graphd_stack *stack,
                              graphd_stack_context *stack_context);

static int grsc_stack_unsuspend(graphd_stack *stack,
                                graphd_stack_context *stack_context);

static int grsc_stack_run(graphd_stack *stack,
                          graphd_stack_context *stack_context);

static const graphd_stack_type grsc_stack_type = {
    grsc_stack_run, grsc_stack_suspend, grsc_stack_unsuspend};

static void grsc_one_deliver(void *data, int err, pdb_id id,
                             graphd_constraint const *con, graphd_value *res);

static int grsc_are_we_done(/* part II */
                            graphd_stack *stack,
                            graphd_stack_context *stack_context);

static int grsc_next(/* part III */
                     graphd_stack *stack, graphd_stack_context *stack_context);

static unsigned long long grsc_absolute_count(
    graphd_read_set_context const *grsc) {
  return grsc->grsc_count + grsc->grsc_con->con_cursor_offset;
}

/*  Given the constraint <con> and the parent ID <id>, what is
 *  the read-set path of the constraint's grsc context?
 */
int graphd_read_set_path(graphd_request *greq, graphd_constraint *con,
                         pdb_id id, cm_buffer *buf) {
  cl_handle *const cl = graphd_request_cl(greq);
  int err;

  cl_assert(cl, con != NULL);
  cl_assert(cl, buf != NULL);

  /*  Does the sort have a sort root
   *  that isn't its own constraint?
   */
  if (con->con_sort == NULL || !con->con_sort_valid ||
      con->con_sort_root.sr_con == con || con->con_sort_root.sr_con == NULL)
    return GRAPHD_ERR_NO;

  /*  The pathname for this grsc consists of
   *  - the path of the constraint we're for
   *  - # as a separator
   *  - our parent GUID, if any.
   */
  if ((err = graphd_constraint_path(cl, con, buf)) != 0) return err;

  if (id == PDB_ID_NONE) return 0;

  return cm_buffer_sprintf(buf, "#%llu", (unsigned long long)id);
}

static void grsc_release_id(graphd_read_set_context *grsc) {
  if (grsc->grsc_pc.pc_id != PDB_ID_NONE) {
    graphd_read_base *grb = grsc->grsc_base;
    cl_handle *cl = graphd_request_cl(grb->grb_greq);

    cl_log(cl, CL_LEVEL_VERBOSE, "grsc_release_id %llx%s",
           (unsigned long long)grsc->grsc_pc.pc_id,
           grsc->grsc_pc.pc_pr_valid ? "+pr" : "");

    if (grsc->grsc_pc.pc_pr_valid) {
      pdb_primitive_finish(graphd_request_graphd(grb->grb_greq)->g_pdb,
                           &grsc->grsc_pc.pc_pr);
      grsc->grsc_pc.pc_pr_valid = false;
    }
    grsc->grsc_pc.pc_id = PDB_ID_NONE;
  }
}

static int grsc_set_id(graphd_read_set_context *grsc, pdb_id id) {
  graphd_read_base *grb = grsc->grsc_base;
  cl_handle *cl = graphd_request_cl(grb->grb_greq);
  int err;

  grsc_release_id(grsc);

  cl_log(cl, CL_LEVEL_VERBOSE, "grsc_set_id %llx", (unsigned long long)id);

  err = pdb_id_read(graphd_request_graphd(grb->grb_greq)->g_pdb, id,
                    &grsc->grsc_pc.pc_pr);
  if (err != 0) return err;
  grsc->grsc_pc.pc_id = id;
  grsc->grsc_pc.pc_pr_valid = true;

  return 0;
}

/*  Free the stack context.
 *
 *  This is called both from the stack free and from the
 *  deferred value free.  (The deferred values can hold
 *  pointers to linkcounted stack contexts.)
 */
void graphd_read_set_free(graphd_read_set_context *grsc) {
  graphd_read_base *grb = grsc->grsc_base;
  graphd_request *greq = grb->grb_greq;
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  graphd_value *val;
  size_t i;

  cl_enter(cl, CL_LEVEL_VERBOSE,
           "(grsc=%p; grsc_link=%u; %zu result(s) at %p (%p))", (void *)grsc,
           grsc->grsc_link, grsc->grsc_con->con_pframe_n, grsc->grsc_result,
           grsc + 1);

  cl_assert(cl, grsc->grsc_link >= 1);
  if (grsc->grsc_link > 1) {
    grsc->grsc_link--;
    cl_leave(cl, CL_LEVEL_VERBOSE,
             "graphd_read_set_free: "
             "unlink %p to %u",
             (void *)grsc, grsc->grsc_link);
    return;
  }

  /* Free results.
   */
  val = grsc->grsc_result;
  for (i = grsc->grsc_con->con_pframe_n; i > 0; i--) {
    char buf[200];
    cl_log(cl, CL_LEVEL_VERBOSE, "graphd_read_set_free: free value %s",
           graphd_value_to_string(val, buf, sizeof buf));

    graphd_value_finish(cl, val++);
  }

  /* Free the current primitive.
   */
  grsc_release_id(grsc);

  /* Free the "or" context.
   */
  graphd_read_or_finish(greq, &grsc->grsc_rom);

  /*  Free the iterator, if any.
   */
  pdb_iterator_destroy(graphd_request_graphd(greq)->g_pdb, &grsc->grsc_it);

  /*  Free the context itself.
   */
  cm_free(cm, grsc);
  cl_leave(cl, CL_LEVEL_VERBOSE, "(destroyed)");
}

static void grsc_resource_free(void *mgr, void *data) {
  graphd_read_set_free((graphd_read_set_context *)data);
}

static void grsc_resource_list(void *log_data, void *resource_manager_data,
                               void *resource_data) {
  cl_handle *cl = log_data;
  graphd_read_set_context *grsc = resource_data;

  cl_log(cl, CL_LEVEL_VERBOSE, "read set context: %s",
         graphd_constraint_to_string(grsc->grsc_con));
}

/* Stack context methods.  Freeze and thaw can be called
 * from the generic graphd_stack handler or from the
 * deferred freeze/thaw handlers.
 */

int graphd_read_set_context_unsuspend(graphd_read_set_context *grsc) {
  graphd_read_base *grb = grsc->grsc_base;
  graphd_request *const greq = grb->grb_greq;
  graphd_handle *g = graphd_request_graphd(greq);
  pdb_handle *pdb = g->g_pdb;
  cl_handle *cl = graphd_request_cl(greq);
  int err;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");

  if (!grsc->grsc_sc.sc_suspended) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "not frozen");
    return 0;
  }
  grsc->grsc_sc.sc_suspended = false;

  if (grsc->grsc_pc.pc_id != PDB_ID_NONE) {
    err = pdb_id_read(pdb, grsc->grsc_pc.pc_id, &grsc->grsc_pc.pc_pr);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err, "id=%lld",
                   (long long)grsc->grsc_pc.pc_id);
      goto err;
    }
    grsc->grsc_pc.pc_pr_valid = true;
  }

  err = graphd_sort_unsuspend(greq->greq_req.req_cm, cl, grsc->grsc_sort);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_sort_unsuspend", err,
                 "failed to thaw sort constraint");
    goto err;
  }

  cl_leave(cl, CL_LEVEL_VERBOSE, "leave");
  return 0;

err:
  cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
  return err;
}

void graphd_read_set_context_link(graphd_read_set_context *grsc) {
  if (grsc != NULL) grsc->grsc_link++;
}

int graphd_read_set_context_suspend(graphd_read_set_context *grsc) {
  graphd_read_base *grb = grsc->grsc_base;
  graphd_request *const greq = grb->grb_greq;
  graphd_handle *g = graphd_request_graphd(greq);
  pdb_handle *pdb = g->g_pdb;
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  int err;
  size_t i;

  if (grsc->grsc_sc.sc_suspended) return 0;
  grsc->grsc_sc.sc_suspended = true;

  for (i = 0; i < grsc->grsc_con->con_pframe_n; i++) {
    err = graphd_value_suspend(cm, cl, grsc->grsc_result + i);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_value_suspend", err,
                   "failed to suspend grsc->grsc_result");
      return err;
    }
  }

  if ((err = graphd_sort_suspend(cm, cl, grsc->grsc_sort)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_sort_suspend", err,
                 "failed to freeze sort context");
    return err;
  }

  if (grsc->grsc_pc.pc_id != PDB_ID_NONE && grsc->grsc_pc.pc_pr_valid) {
    /*  Thaw will reload the primitive from the ID.
     */
    pdb_primitive_finish(pdb, &grsc->grsc_pc.pc_pr);
    grsc->grsc_pc.pc_pr_valid = false;
  }

  return 0;
}

static int grsc_stack_suspend(graphd_stack *stack,
                              graphd_stack_context *stack_context) {
  return graphd_read_set_context_suspend(
      (graphd_read_set_context *)stack_context);
}

static int grsc_stack_unsuspend(graphd_stack *stack,
                                graphd_stack_context *stack_context) {
  return graphd_read_set_context_unsuspend(
      (graphd_read_set_context *)stack_context);
}

static int grsc_stack_run(graphd_stack *stack,
                          graphd_stack_context *stack_context) {
  /* This never runs directly - it gets pushed with a
   * specific run function.
   */
  return GRAPHD_ERR_NO;
}

/**
 * @brief What is the size estimate for this constraint?
 *
 *  An "estimate" is the technical term for a string that encodes
 *  the optimizer metrics for a constraint.  It's at the constraint
 *  expression level (like cursor and count), not at the primitive level.
 *
 * @param greq		request we're working for
 * @param it 		The constraint iterator the caller is asking about.
 * @param val_out	Assign the count to this.
 *
 * @return 0 on success, an error code on resource failure.
 */
static int grsc_estimate_count(graphd_request *greq, pdb_iterator *it,
                               graphd_value *val_out) {
  graphd_handle *g = graphd_request_graphd(greq);

  cl_assert(g->g_cl, it != NULL);

  if (pdb_iterator_n_valid(g->g_pdb, it))
    graphd_value_number_set(val_out, pdb_iterator_n(g->g_pdb, it));
  else
    graphd_value_null_set(val_out);

  return 0;
}

/**
 * @brief What are the performance estimates for this constraint?
 *
 *  An "estimate" is the technical term for a string that encodes
 *  the optimizer metrics for a constraint.  It's at the constraint
 *  expression level (like cursor and count), not at the primitive level.
 *
 * @param greq		request we're working for
 * @param it 		The constraint iterator the caller is asking about.
 * @param val_out	Assign the string value to this.
 *
 * @return 0 on success, an error code on resource failure.
 */
static int grsc_estimate(graphd_request *greq, pdb_iterator *it,
                         graphd_value *val_out) {
  int err;
  graphd_handle *g = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  graphd_value *el;
  char buf[200];
  char const *str;

  err = graphd_value_list_alloc(g, cm, cl, val_out, 5);
  if (err != 0) return err;

  el = val_out->val_list_contents;
  str = pdb_iterator_to_string(g->g_pdb, it, buf, sizeof buf);
  if (str == NULL)
    graphd_value_null_set(el);
  else {
    err = graphd_value_text_strdup(cm, el, GRAPHD_VALUE_STRING, str,
                                   str + strlen(str));
    if (err != 0) goto err;
  }
  el++;
  if (pdb_iterator_sorted_valid(g->g_pdb, it))
    graphd_value_boolean_set(el, pdb_iterator_sorted(g->g_pdb, it));
  else
    graphd_value_null_set(el);
  el++;

  if (pdb_iterator_check_cost_valid(g->g_pdb, it))
    graphd_value_number_set(el, pdb_iterator_check_cost(g->g_pdb, it));
  else
    graphd_value_null_set(el);
  el++;

  if (pdb_iterator_next_cost_valid(g->g_pdb, it))
    graphd_value_number_set(el, pdb_iterator_next_cost(g->g_pdb, it));
  else
    graphd_value_null_set(el);
  el++;

  if (pdb_iterator_n_valid(g->g_pdb, it))
    graphd_value_number_set(el, pdb_iterator_n(g->g_pdb, it));
  else
    graphd_value_null_set(el);
  el++;

err:
  if (err != 0) graphd_value_finish(cl, val_out);

  return err;
}

/**
 * @brief We've visited all alternatives for this constraint; fill in values.
 *
 * 	This function takes care of the values that have to wait
 *	until the end of a page or the end of a traversal - the count
 * 	and cursor values.
 *
 * 	This is also the time where anything that isn't assigned
 *	gets a NULL value.
 *
 * @param grsc	read context
 * @param pat	result template (or the default); never a list.
 * @param val	pointer to the individual value we're filling in.
 *
 * @return 0 on success, a nonzero error code on unexpected error.
 */
static int grsc_complete_atom(graphd_read_set_context *grsc,
                              graphd_pattern const *pat, graphd_value *val) {
  graphd_request *greq = grsc->grsc_base->grb_greq;
  cl_handle *cl = graphd_request_cl(greq);
  graphd_value const *li;
  size_t i;
  int err;

  cl_assert(cl, val != NULL);
  cl_assert(cl, pat != NULL);
  cl_assert(cl, pat->pat_type != GRAPHD_PATTERN_LIST);

  /*  We need to know whether or not a sample value is
   *  unspecified in order to know whether or not to overwrite it.
   *  So, deferred values must be evaluated at this point.
   */
  if (val->val_type != GRAPHD_VALUE_UNSPECIFIED) {
    char buf[200];
    cl_log(cl, CL_LEVEL_VERBOSE, "grsc_complete_atom: already have value %s",
           graphd_value_to_string(val, buf, sizeof buf));
    return 0;
  }

  if (!GRAPHD_VALUE_IS_TYPE(val->val_type))
    cl_notreached(cl, "unexpected type %d (%x) in token %p", val->val_type,
                  val->val_type, val);

  {
    char buf[200];
    cl_log(cl, CL_LEVEL_VERBOSE, "grsc_complete_atom: pat=%s",
           graphd_pattern_dump(pat, buf, sizeof buf));
  }

  /*  In most cases, patterns are sampled as matching
   *  instances are found; if we don't have something by
   *  now, the value is simply null.
   *
   *  A sampling pattern is marked as "deferred" if it gets
   *  filled in only *after*  the sorting is finished.
   *  That allows us to use sorting and sampling
   *  to retrieve minima and maxima of a data range as
   *  single elements.
   */
  if (grsc->grsc_sort && pat->pat_sample) {
    /*  Use result #pat_result_offset,
     *  element #pat_element_offset of the first
     *  result element that has a non-unspecified one.
     */
    size_t i;
    for (i = 0; i < grsc->grsc_count; i++) {
      graphd_value const *source =
          (grsc->grsc_sort ? graphd_sort_value(grsc->grsc_sort, pat, i)
                           : grsc->grsc_result + i);

      if (source != NULL && source->val_type != GRAPHD_VALUE_UNSPECIFIED) {
        err = graphd_value_copy(graphd_request_graphd(greq),
                                greq->greq_req.req_cm, cl, val, source);
        if (err != 0) {
          char buf[200];
          cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_value_copy", err, "val=%s",
                       graphd_value_to_string(source, buf, sizeof buf));
          return err;
        }
        {
          char b1[200], b2[200];
          cl_log(cl, CL_LEVEL_VERBOSE,
                 "result_complete_atom: filled in "
                 "deferred pattern %s from source %s",
                 graphd_pattern_dump(pat, b1, sizeof(b1)),
                 graphd_value_to_string(val, b2, sizeof(b2)));
        }
        break;
      }
    }
  } else {
    switch (pat->pat_type) {
      default:
        cl_cover(cl);
        break;

      case GRAPHD_PATTERN_ESTIMATE_COUNT:
        err = grsc_estimate_count(greq, grsc->grsc_it, val);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "grsc_estimate_count", err,
                       "unexpected error");
          return err;
        }
        break;

      case GRAPHD_PATTERN_ESTIMATE:
        err = grsc_estimate(greq, grsc->grsc_it, val);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "grsc_estimate", err,
                       "unexpected error");
          return err;
        }
        break;

      case GRAPHD_PATTERN_ITERATOR:
        err = graphd_iterator_dump(greq, grsc->grsc_it, val);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_dump", err,
                       "unexpected error");
          return err;
        }
        break;

      case GRAPHD_PATTERN_TIMEOUT:
        if (greq->greq_soft_timeout_triggered) {
          char const *trig = greq->greq_soft_timeout_triggered;

          graphd_value_text_set_cm(val, GRAPHD_VALUE_STRING, (char *)trig,
                                   strlen(trig), NULL);
        } else
          graphd_value_null_set(val);
        cl_cover(cl);
        break;

      case GRAPHD_PATTERN_COUNT:
        graphd_read_set_count_get_atom(grsc, val);
        break;

      case GRAPHD_PATTERN_CURSOR:
        err = graphd_read_set_cursor_get_value(grsc, val);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL,
                       grsc->grsc_sort ? "graphd_sort_cursor_get"
                                       : "graphd_read_set_cursor_get_atom",
                       err, "unexpected error");
          return err;
        }
        break;

      case GRAPHD_PATTERN_VARIABLE:
        if (grsc->grsc_sort == NULL) break;

        li = grsc->grsc_result + pat->pat_result_offset;
        if (li->val_type != GRAPHD_VALUE_LIST) {
          char b1[200], b2[200], b3[200], b4[200];
          graphd_constraint *con = grsc->grsc_con;
          cl_notreached(cl,
                        "unexpected value %s at "
                        "result offset %zu (pattern %s, pframe %s/%s) "
                        "-- expected a list",
                        graphd_value_to_string(li, b1, sizeof b1),
                        pat->pat_result_offset,
                        graphd_pattern_dump(pat, b2, sizeof b2),
                        graphd_pattern_dump(
                            con->con_pframe[pat->pat_result_offset].pf_one, b3,
                            sizeof b3),
                        graphd_pattern_dump(
                            con->con_pframe[pat->pat_result_offset].pf_set, b4,
                            sizeof b4));
        }
        cl_assert(cl, li->val_type == GRAPHD_VALUE_LIST);

        for (i = 0; i < li->val_list_n; i++) {
          graphd_value *source;

          source = (grsc->grsc_sort != NULL
                        ? graphd_sort_value(grsc->grsc_sort, pat, i)
                        : li[i].val_list_contents + pat->pat_element_offset);

          if (source->val_type == GRAPHD_VALUE_UNSPECIFIED) continue;

          err = graphd_value_copy(graphd_request_graphd(greq),
                                  greq->greq_req.req_cm, cl, val, source);
          if (err != 0) {
            char buf[200];
            cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_value_copy", err, "val=%s",
                         graphd_value_to_string(source, buf, sizeof buf));
            return err;
          }

          {
            char b1[200], b2[200];
            cl_log(cl, CL_LEVEL_VERBOSE,
                   "result_complete_atom: filled in "
                   "variable pattern %s from source %s "
                   "[res %zu, elm %hu]",
                   graphd_pattern_dump(pat, b1, sizeof(b1)),
                   graphd_value_to_string(val, b2, sizeof(b2)),
                   pat->pat_result_offset, pat->pat_element_offset);
          }
          break;
        }
    }
  }

  if (val->val_type == GRAPHD_VALUE_UNSPECIFIED) {
    err = graphd_pattern_from_null(cl, pat, val);
    cl_assert(cl, err == 0);
  }

  {
    char b1[200], b2[200];
    cl_log(cl, CL_LEVEL_VERBOSE, "result_complete_atom: %s := %s",
           graphd_pattern_dump(pat, b1, sizeof b1),
           graphd_value_to_string(val, b2, sizeof b2));
  }

  return 0;
}

/**
 * @brief Finish returning a result.
 *
 *  The server has traversed all alternatives that
 *  can be returned for a query, and computed the "contents"
 *  set of per-subconstraint result values.
 *
 *  Now, result values that depend on more than just a single
 *  value must be filled in (the "set" part of a pframe).
 *
 * @param grsc	overall query context
 * @param pf	pattern frame for this result
 * @param res	value pointer corresponding to pf->pf_set
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
static int grsc_complete_frame(graphd_read_set_context *const grsc,
                               graphd_pattern_frame *const pf,
                               graphd_value *const val) {
  graphd_request *const greq = grsc->grsc_base->grb_greq;
  graphd_constraint *const con = grsc->grsc_con;
  cl_handle *const cl = graphd_request_cl(greq);
  graphd_pattern const *pat;
  int err = 0;

  {
    char b1[200], b2[200];
    cl_enter(
        cl, CL_LEVEL_VERBOSE, "result pat %s, frame %s",
        pf->pf_set ? graphd_pattern_dump(pf->pf_set, b1, sizeof b1) : "null",
        graphd_value_to_string(val, b2, sizeof b2));
  }

  if ((pat = pf->pf_set) == NULL) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "no pattern");
    return 0;
  }

  if (val->val_type == GRAPHD_VALUE_DEFERRED) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "deferred");
    return 0;
  }

  if (con->con_pframe_want_cursor &&
      (!con->con_resultpagesize_valid ||
       grsc->grsc_count <= con->con_start + con->con_resultpagesize))

    graphd_read_set_cursor_clear(grsc, pf, val);

  /*  Complete the result - fill in "count", post-sorting samples
   */
  if (pat->pat_type != GRAPHD_PATTERN_LIST) {
    if (val->val_type == GRAPHD_VALUE_UNSPECIFIED) {
      err = grsc_complete_atom(grsc, pat, val);
      if (err != 0)
        cl_log_errno(cl, CL_LEVEL_FAIL, "grsc_complete_atom", err,
                     "unexpected error");
    }
  } else {
    graphd_pattern const *ric;
    graphd_value *valc;

    cl_assert(cl, GRAPHD_VALUE_IS_ARRAY(*val));

    /*  Replace list elements in the frame with corresponding
     *  data -- other than a contained list,  which has been
     *  replaced with a sequence of alternative results by the
     *  alternative evaluation.
     */
    for (ric = pat->pat_list_head, valc = val->val_list_contents; ric != NULL;
         ric = ric->pat_next, valc++) {
      cl_assert(cl, valc < val->val_list_contents + val->val_list_n);

      if (ric->pat_type == GRAPHD_PATTERN_LIST) continue;

      if ((valc->val_type == GRAPHD_VALUE_UNSPECIFIED) &&
          (err = grsc_complete_atom(grsc, ric, valc)) != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "grsc_complete_atom", err,
                     "unexpected error");
        break;
      }
    }
  }
  cl_leave_err(cl, CL_LEVEL_VERBOSE, err, "leave");
  return err;
}

/*  Deliver results to the callback and pop.
 *  The grsc context must have been pushed on the stack.
 */

/*  Deliver results to the callback.
 *
 *  The error code is in grsc->grsc_err; the results are the passed-in
 *  results - either grsc->grsc_result, or a set of deferred values
 *  constructed on the fly by graphd_read_set_defer_results().
 */
static void grsc_deliver(graphd_read_set_context *grsc, graphd_value *res) {
  graphd_request *greq = grsc->grsc_base->grb_greq;
  graphd_constraint *con = grsc->grsc_con;
  cl_handle *cl = graphd_request_cl(greq);
  size_t i;

  cl_enter(cl, CL_LEVEL_VERBOSE,
           "%s; grsc_link %d; error %d; %zu pframe(s); %s",
           graphd_constraint_to_string(con), grsc->grsc_link, grsc->grsc_err,
           con->con_pframe_n,
           grsc->grsc_err ? graphd_strerror(grsc->grsc_err) : "ok");

  /* Unless we're returning deferred values ...
   */
  if (res == grsc->grsc_result) {
    /* If we have a sort, finish sorting.
     */
    if (grsc->grsc_err == 0 && grsc->grsc_sort != NULL)
      graphd_sort_finish(grsc->grsc_sort);
  }

  /*  If we didn't find enough alternatives, fail.
   * (This is true even if we're deferring - we don't
   *  defer before reaching the minimum count.)
   */
  if (grsc->grsc_err == 0 &&
      grsc_absolute_count(grsc) < con->con_count.countcon_min &&
      greq->greq_soft_timeout_triggered == NULL) {
    cl_log(cl, CL_LEVEL_SPEW,
           "grsc_deliver: "
           "count %lu < atleast: %lu",
           (unsigned long)grsc_absolute_count(grsc),
           (unsigned long)con->con_count.countcon_min);
    grsc->grsc_err = GRAPHD_ERR_NO;
  }

  /* If we found too many, fail too!
   */
  if (grsc->grsc_err == 0 && con->con_count.countcon_max_valid &&
      grsc_absolute_count(grsc) > con->con_count.countcon_max) {
    cl_log(cl, CL_LEVEL_SPEW,
           "grsc_deliver: "
           "count %lu > atmost: %lu",
           (unsigned long)grsc_absolute_count(grsc),
           (unsigned long)con->con_count.countcon_max);
    grsc->grsc_err = GRAPHD_ERR_NO;
  }

  /*  Only when not deferring...
   */
  if (res == grsc->grsc_result && grsc->grsc_err == 0) {
    /*  Complete frames - sampling and filling
     *  in counts.
     *
     *  Where we can't sample because the sampling
     *  material is deferred, schedule the deferred
     *  frame for evaluation and resume later.
     */
    for (i = 0; i < con->con_pframe_n; i++) {
      char b1[200], b2[200];

      int err = grsc_complete_frame(grsc, con->con_pframe + i, res + i);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "grsc_complete_frame", err,
                     "pframe[%zu]", i);
        grsc->grsc_err = err;
        break;
      }

      cl_log(cl, CL_LEVEL_VERBOSE, "[%zu] %s%s: %s", i,
             con->con_pframe[i].pf_set ? "" : "(one)",
             graphd_pattern_dump(con->con_pframe[i].pf_set
                                     ? con->con_pframe[i].pf_set
                                     : con->con_pframe[i].pf_one,
                                 b1, sizeof b1),
             graphd_value_to_string(res + i, b2, sizeof b2));
    }
  }

  if (greq->greq_indent > 0) greq->greq_indent--;

  cl_log(cl, CL_LEVEL_DEBUG, "RXN%*s](D) grsc_deliver: constraint=%s, %s",
         2 * greq->greq_indent, "", graphd_constraint_to_string(con),
         grsc->grsc_err ? graphd_strerror(grsc->grsc_err) : "ok");

  /* Deliver the results
   */
  (*grsc->grsc_callback)(grsc->grsc_callback_data, grsc->grsc_err, con,
                         grsc->grsc_err ? NULL : res);

  /*  Now that we've delivered these values,
   *  free those that the recipient didn't move.
   */
  for (i = 0; i < con->con_pframe_n; i++) graphd_value_finish(cl, res + i);

  /*  Pop ourselves, returning control to the frame below ours.
   *  This will call the grsc free function.
   */
  graphd_stack_remove(&greq->greq_stack, &grsc->grsc_sc);
  cl_leave(cl, CL_LEVEL_VERBOSE, "leave");
}

static int grsc_initialize_sort(graphd_read_set_context *grsc) {
  graphd_read_base *grb = grsc->grsc_base;
  graphd_request *greq = grb->grb_greq;
  cl_handle *cl = graphd_request_cl(greq);
  graphd_constraint *con = grsc->grsc_con;
  int err = 0;

  /*  If there is no sort, or the iterator returns ids
   *  already in the order that the sort wants, don't sort!
   */
  if (con->con_sort == NULL || !con->con_sort_valid ||
      (con->con_resultpagesize == 0 && con->con_resultpagesize_valid) ||
      !graphd_sort_needed(greq, con, grsc->grsc_it)) {
    grsc->grsc_sort = NULL;
    cl_cover(cl);

    return 0;
  }

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");

  errno = 0;
  grsc->grsc_sort = graphd_sort_create(greq, con, grsc->grsc_result);
  if (grsc->grsc_sort == NULL) {
    err = errno ? errno : ENOMEM;
    cl_leave(cl, CL_LEVEL_VERBOSE, "graphd_sort_create: %s",
             graphd_strerror(err));
    return errno ? errno : ENOMEM;
  }

  /*  If we have a stored sort cursor, feed it to the sort.
   */
  if (con->con_cursor_s != NULL &&
      graphd_sort_is_cursor(con->con_cursor_s, con->con_cursor_e)) {
    err = graphd_sort_cursor_set(grsc->grsc_sort, con->con_cursor_s,
                                 con->con_cursor_e);

    if (err == GRAPHD_ERR_LEXICAL)
      graphd_request_errprintf(
          grb->grb_greq, false, "BADCURSOR cannot resume at \"%.*s\"",
          (int)(con->con_cursor_e - con->con_cursor_s), con->con_cursor_s);
    else if (err) {
      cl_log_errno(
          cl, CL_LEVEL_FAIL, "graphd_sort_cursor_set", err, "cursor=\"%.*s\"",
          (int)(con->con_cursor_e - con->con_cursor_s), con->con_cursor_s);
      cl_cover(cl);
    }
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s", err ? graphd_strerror(err) : "ok");
  return err;
}

/*  Is the caller asking for a fixed count that we already have
*   in our indices?
 *
 *  If so, just return the count now -- don't actually do the
 *  work of walking the primitives.
 *
 *  This allows graphd applications who know what they're doing
 *  to get database-level metrics without incurring the penalty
 *  of iterating over primitives one by one.
 */
static int fast_count(graphd_read_set_context *grsc,
                      unsigned long long *count_out) {
  graphd_constraint *const con = grsc->grsc_con;
  graphd_request *const greq = grsc->grsc_base->grb_greq;
  cl_handle *const cl = graphd_request_cl(greq);
  graphd_handle *const g = graphd_request_graphd(greq);
  int linkage;
  int n_approaches = 0;
  char buf[200];

  /*  Caller checked all these.
   */
  cl_assert(cl, !con->con_newest.gencon_valid);
  cl_assert(cl, !con->con_oldest.gencon_valid);
  cl_assert(cl, con->con_subcon_n == 0);
  cl_assert(cl, con->con_live == GRAPHD_FLAG_DONTCARE);
  cl_assert(cl, con->con_archival == GRAPHD_FLAG_DONTCARE);
  cl_assert(cl, con->con_valuetype == GRAPH_DATA_UNSPECIFIED);
  cl_assert(cl, con->con_cursor_s == NULL);
  cl_assert(cl, con->con_guid.guidcon_include.gs_n == 0);

  /*  Constraints as evaluated by the optimizer were too complicated?
   */
  if (!pdb_iterator_n_valid(g->g_pdb, grsc->grsc_it)) {
    cl_log(cl, CL_LEVEL_VERBOSE, "fast_count: pdb_iterator_n is invalid for %s",
           pdb_iterator_to_string(g->g_pdb, grsc->grsc_it, buf, sizeof buf));
    return PDB_ERR_MORE;
  }

  /*  Currently, we don't pre-count values or names.
   */
  if (con->con_name.strqueue_head != NULL ||
      con->con_value.strqueue_head != NULL) {
    cl_log(cl, CL_LEVEL_VERBOSE, "fast_count: can't count name/value");
    return PDB_ERR_MORE;
  }

  /* Good: we have a count from the iterator.
   *  	But is that actually the final count?  Or are
   *  	there little extra constraints that *do* require
   *  	primitive-by-primitive testing?
   */
  *count_out = pdb_iterator_n(graphd->g_pdb, grsc->grsc_it);

  /*  If we know we've got a VIP iterator, we can stomach two
   *  constraints -- typeguid and an endpoint.
   *  Otherwise, we can stomach one.
   */
  if (graphd_iterator_vip_is_instance(g->g_pdb, grsc->grsc_it))
    n_approaches = -1;
  else
    n_approaches = 0;

  /*  Count things that we *know* are single-index constraints
   *  in the expression.  (I.e., "approaches".)  If we only find
   *  0 or 1 of them, the fast-count works - we know the optimizer
   *  didn't do any worse than the obvious case.
   *  If we find more than 1, it's too complicated, and we give up.
   */

  /*  Case: we know the parent, and the parent is at one
   *  	end of a linkage relationship.
   */
  if (grsc->grsc_parent_id != PDB_ID_NONE &&
      graphd_linkage_is_my(con->con_linkage)) {
    if (n_approaches++ > 0) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "fast_count: "
             "too many constraints (at parent linkage)");
      return PDB_ERR_MORE;
    }
  }

  /*  Case: we have one specified linkage relationship.
   */
  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++) {
    if (graphd_linkage_is_my(con->con_linkage) &&
        graphd_linkage_my(con->con_linkage) == linkage)
      continue;

    if (con->con_linkcon[linkage].guidcon_include_valid &&
        con->con_linkcon[linkage].guidcon_include.gs_n == 1 &&
        !GRAPH_GUID_IS_NULL(
            con->con_linkcon[linkage].guidcon_include.gs_guid[0])) {
      if (n_approaches++ > 0) {
        cl_log(
            cl, CL_LEVEL_VERBOSE,
            "fast_count: "
            "too many constraints (linkage %s) against %s",
            pdb_linkage_to_string(linkage),
            pdb_iterator_to_string(g->g_pdb, grsc->grsc_it, buf, sizeof buf));
        return PDB_ERR_MORE;
      }

      cl_log(cl, CL_LEVEL_VERBOSE, "fast_count: count linkage %s",
             pdb_linkage_to_string(linkage));
    } else if (con->con_linkcon[linkage].guidcon_include_valid ||
               con->con_linkcon[linkage].guidcon_exclude_valid ||
               con->con_linkcon[linkage].guidcon_match_valid) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "fast_count: "
             "too many constraints (include/exclude/match)");
      return PDB_ERR_MORE;
    }
  }

  cl_log(cl, CL_LEVEL_DEBUG,
         "fast_count: "
         "getting a fast count of %llu for %s from %s",
         *count_out, graphd_constraint_to_string(con),
         pdb_iterator_to_string(g->g_pdb, grsc->grsc_it, buf, sizeof buf));
  return 0;
}

/**
 * @brief I: Compile statistics for this read context
 *
 *  Compiling statistics means that the iterator figures out
 *  internally how to actually get us its values.  Only after
 *  statistics have taken place do we know, e.g., whether the
 *  iterator is sorted, and in what direction (if any).
 *
 *   This may be called twice when resuming a suspended iterator.
 *
 * @param stack		execution stack in the request
 * @param stack_context	current read constraint context
 *
 * @return 0 on successful, possibly partial, execution
 * @return other nonzero errors on system/resource error.
 */
static int grsc_statistics(graphd_stack *stack,
                           graphd_stack_context *stack_context) {
  graphd_read_set_context *grsc = (void *)stack_context;
  graphd_read_base *const grb = grsc->grsc_base;
  graphd_request *const greq = grb->grb_greq;
  graphd_handle *const g = graphd_request_graphd(greq);
  cl_handle *const cl = graphd_request_cl(greq);
  graphd_constraint *const con = grsc->grsc_con;
  pdb_budget budget = GRAPHD_STATISTICS_BUDGET;
  pdb_budget budget_in = budget;
  int err;
  char buf[200];

  PDB_IS_ITERATOR(cl, grsc->grsc_it);

  cl_enter(cl, CL_LEVEL_VERBOSE, "it=%s",
           pdb_iterator_to_string(g->g_pdb, grsc->grsc_it, buf, sizeof buf));

  err = pdb_iterator_statistics(g->g_pdb, grsc->grsc_it, &budget);
  if (err == PDB_ERR_MORE) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "(suspended; $%lld)",
             (long long)(budget_in - budget));
    return 0;
  } else if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_statistics", err,
                 "unexpected error");
    cl_leave(cl, CL_LEVEL_VERBOSE, "pdb_iterator_statistics: %s",
             graphd_strerror(err));
    return err;
  }

  /*  Only now that we know our iterator statistics can
   *  we initialize the sort and figure out whether we
   *  actually *need* to sort (after extracting the values)
   *  or not.
   */
  if (grsc->grsc_sort == NULL && (err = grsc_initialize_sort(grsc)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "grsc_initialize_sort", err,
                 "unexpected error");
    cl_leave(cl, CL_LEVEL_VERBOSE, "grsc_initialize_sort: %s",
             graphd_strerror(err));
    return err;
  }

  PDB_IS_ITERATOR(cl, grsc->grsc_it);

  /*  Now that there's an iterator in grsc->grsc_it,
   *  is there a request for a count that we can satisfy
   *  without looking at the individual primitives?
   */
  if (con->con_pframe_want_count && !con->con_newest.gencon_valid &&
      !con->con_oldest.gencon_valid && con->con_subcon_n == 0 &&
      con->con_live == GRAPHD_FLAG_DONTCARE &&
      con->con_archival == GRAPHD_FLAG_DONTCARE &&
      con->con_valuetype == GRAPH_DATA_UNSPECIFIED &&
      con->con_cursor_s == NULL && !con->con_guid.guidcon_include_valid &&
      !con->con_guid.guidcon_exclude_valid &&
      !con->con_guid.guidcon_match_valid) {
    unsigned long long count;
    int err;

    if ((err = fast_count(grsc, &count)) == 0) {
      /*  Yes!
       */
      grsc->grsc_count_total = count;

      if (grsc->grsc_count_total < con->con_count.countcon_min ||
          (con->con_count.countcon_max_valid &&
           grsc->grsc_count_total > con->con_count.countcon_max)) {
        cl_log(cl, CL_LEVEL_VERBOSE,
               "grsc_statistics: fast count %llu "
               "is out of range",
               count);
        grsc->grsc_err = GRAPHD_ERR_NO;
        grsc_deliver(grsc, grsc->grsc_result);

        cl_leave(cl, CL_LEVEL_VERBOSE, "too big/small: %s",
                 graphd_strerror(grsc->grsc_err));
        return 0;
      }
    } else if (err != PDB_ERR_MORE) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "fast_count", err, "unexpected error");
      cl_leave(cl, CL_LEVEL_VERBOSE, "fast_count: %s", graphd_strerror(err));
      return err;
    }
    err = 0;
  }

  cl_log(cl, CL_LEVEL_DEBUG,
         "RXN%*s[(A) grsc_statistics: "
         "constraint=%s, iterator=%s, n=%llu, nc=%lu fc=%lu cc=%lu",
         2 * greq->greq_indent, "", graphd_constraint_to_string(con),
         pdb_iterator_to_string(g->g_pdb, grsc->grsc_it, buf, sizeof buf),
         pdb_iterator_n(g->g_pdb, grsc->grsc_it),
         (unsigned long)pdb_iterator_next_cost(g->g_pdb, grsc->grsc_it),
         (unsigned long)pdb_iterator_find_cost(g->g_pdb, grsc->grsc_it),
         (unsigned long)pdb_iterator_check_cost(g->g_pdb, grsc->grsc_it));

  PDB_IS_ITERATOR(cl, grsc->grsc_it);
  greq->greq_indent++;

  graphd_stack_resume(stack, stack_context, grsc_are_we_done);
  cl_leave(cl, CL_LEVEL_VERBOSE, "-> see you in grsc_are_we_done ($%lld)",
           (long long)(budget_in - budget));

  return 0;
}

/**
 * @brief Should we do statistics on this?
 *
 *  Called before pushing a set context on stack.
 *
 * @param stack		execution stack in the request
 * @param stack_context	current read constraint context
 *
 * @return 0 if we should defer this
 * @return GRAPHD_ERR_NO if we should go ahead with the execution
 * @return other nonzero errors on system/resource error.
 */
static bool grsc_should_do_statistics(graphd_read_set_context *const grsc) {
  graphd_read_base *const grb = grsc->grsc_base;
  graphd_request *const greq = grb->grb_greq;
  cl_handle *const cl = graphd_request_cl(greq);
  graphd_constraint *const con = grsc->grsc_con;
  bool counting;

  PDB_IS_ITERATOR(cl, grsc->grsc_it);

  /*  Did we hit a soft timeout?
   */
  if (con->con_resumable && grsc->grsc_sort == NULL &&
      greq->greq_soft_timeout_triggered != NULL)
    return false;

  /*  Did we hit an error?
   */
  if (grsc->grsc_err != 0) return false;

  /*  Did we go over?
   */
  if (con->con_count.countcon_max_valid &&
      grsc_absolute_count(grsc) > con->con_count.countcon_max)
    return false;

  /* We're still counting if .. we don't have a fast count yet, and ... */

  counting = grsc->grsc_count_total == (unsigned long long)-1

             /*  We want to count
              *  ... everything,
              *  ... or up to a limit we haven't reached yet.
              */
             &&
             ((con->con_pframe_want_count &&
               (!con->con_countlimit_valid ||
                grsc_absolute_count(grsc) < con->con_countlimit))

              /*  Or we need to find more than a minimum
               *  we haven't reached yet...
               */
              || grsc_absolute_count(grsc) < con->con_count.countcon_min

              /*  Or we need to stay below a maximum
               *  we haven't reached yet...
               */
              || (con->con_count.countcon_max_valid &&
                  (grsc_absolute_count(grsc) < con->con_count.countcon_max)));

  /*  We're done if we have {start} + {pagesize} elements
   *  ({pagesize}+1 if a cursor is asked for), and if we
   *  have enough for our minimum count if one was required.
   */
  if (con->con_resultpagesize_valid && grsc->grsc_sort == NULL &&
      grsc->grsc_count >= (con->con_start + con->con_resultpagesize +
                           !!con->con_pframe_want_cursor) &&
      !counting)

    return false;

  /*  We're also done if we're not (or no longer) sampling
   *  and neither count nor per-element data are requested.
   */
  if (!grsc->grsc_sampling && !counting && !con->con_pframe_want_data &&
      (!con->con_pframe_want_count ||
       grsc->grsc_count_total != (unsigned long long)-1) &&
      !con->con_pframe_want_cursor)

    return false;

  /*  Finally, we're done if the sort thinks we're done
   *  (all the remaining values will be larger than the
   *  current sort array end).
   */
  if (!counting && grsc->grsc_sort != NULL &&
      graphd_sort_accept_ended(grsc->grsc_sort))

    return false;

  /*  In the first round ("verify"), we only run until we
   *  know whether or not this whole constraint is matched
   *  or not.
   *
   *  If there is a minimum count, that minimum must be filled
   *  (usually, it's 1.)
   *
   *  If there is a maximum count, we must have stopped prior
   *  to the maximum (i.e. we can't resume.)
   */
  if (grsc->grsc_verify &&
      grsc_absolute_count(grsc) >= con->con_count.countcon_min &&
      !con->con_count.countcon_max_valid)
    return false;

  return true;
}

/**
 * @brief Part II: Are we done?
 *
 *  Read through the next matching records for this constraint.
 *
 *  Once the algorithm in this module completes, it'll call
 *  graphd_read_set_finish(stack, stack_context) and resume
 *  work in graphd-read-constraint.
 *
 * @param stack		execution stack in the request
 * @param stack_context	current read constraint context
 *
 * @return 0 on successful partial execution
 * @return other nonzero errors on system/resource error.
 */
static int grsc_are_we_done(graphd_stack *const stack,
                            graphd_stack_context *const stack_context) {
  graphd_read_set_context *const grsc = (void *)stack_context;
  graphd_read_base *const grb = grsc->grsc_base;
  graphd_request *const greq = grb->grb_greq;
  cl_handle *const cl = graphd_request_cl(greq);
  graphd_constraint *const con = grsc->grsc_con;
  bool counting;

  cl_enter(cl, CL_LEVEL_VERBOSE,
           "%s, grsc %p, error %d; "
           "link %zu, count %zu, start %zu, pagesize %zu",
           graphd_constraint_to_string(con), (void *)grsc, grsc->grsc_err,
           (size_t)grsc->grsc_link, (size_t)grsc->grsc_count,
           (size_t)con->con_start, (size_t)con->con_resultpagesize);

  PDB_IS_ITERATOR(cl, grsc->grsc_it);

  /*  Did we hit a soft timeout?
   */
  if (con->con_resumable && grsc->grsc_sort == NULL &&
      greq->greq_soft_timeout_triggered != NULL) {
    cl_log(cl, CL_LEVEL_DEBUG,
           "RXN%*s(B) grsc_are_we_done: yes (soft timeout).",
           2 * greq->greq_indent, "");
    grsc_deliver(grsc, grsc->grsc_result);
    cl_leave(cl, CL_LEVEL_VERBOSE, "soft timeout");

    return 0;
  }

  /*  Did we hit an error?
   */
  if (grsc->grsc_err != 0) {
    cl_log(cl, CL_LEVEL_DEBUG, "RXN%*s(B) grsc_are_we_done: yes (error: %s).",
           2 * greq->greq_indent, "", graphd_strerror(grsc->grsc_err));
    grsc_deliver(grsc, grsc->grsc_result);
    cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s",
             graphd_strerror(grsc->grsc_err));

    return 0;
  }

  /*  Did we go over?
   */
  if (con->con_count.countcon_max_valid &&
      grsc_absolute_count(grsc) > con->con_count.countcon_max) {
    cl_log(cl, CL_LEVEL_DEBUG,
           "RXN%*s(B) grsc_are_we_done: yes "
           "(too many matches - more than %llu).",
           2 * greq->greq_indent, "",
           (unsigned long long)(con->con_count.countcon_max + con->con_start));
    grsc->grsc_err = GRAPHD_ERR_NO;
    grsc_deliver(grsc, grsc->grsc_result);

    /* grsc may now have been free'd. */

    cl_leave(cl, CL_LEVEL_VERBOSE, "over: %s", graphd_strerror(GRAPHD_ERR_NO));

    return 0;
  }

  /* We're still counting if .. we don't have a fast count yet, and ... */

  counting = grsc->grsc_count_total == (unsigned long long)-1

             /*  We want to count
              *  ... everything,
              *  ... or up to a limit we haven't reached yet.
              */
             &&
             ((con->con_pframe_want_count &&
               (!con->con_countlimit_valid ||
                grsc_absolute_count(grsc) < con->con_countlimit))

              /*  Or we need to find more than a minimum
               *  we haven't reached yet...
               */
              || grsc_absolute_count(grsc) < con->con_count.countcon_min

              /*  Or we need to stay below a maximum
               *  we haven't reached yet...
               */
              || (con->con_count.countcon_max_valid &&
                  (grsc_absolute_count(grsc) <= con->con_count.countcon_max)));

  /*  We're done if we have {start} + {pagesize} elements
   *  ({pagesize}+1 if a cursor is asked for), and if we
   *  have enough for our minimum count if one was required.
   */
  if (con->con_resultpagesize_valid && grsc->grsc_sort == NULL &&
      grsc->grsc_count >= (con->con_start + con->con_resultpagesize +
                           !!con->con_pframe_want_cursor) &&
      !counting) {
    cl_log(cl, CL_LEVEL_DEBUG,
           "RXN%*s(B) grsc_are_we_done: yes "
           "(found %llu matches - pagesize=%llu).",
           2 * greq->greq_indent, "", (unsigned long long)grsc->grsc_count,
           (unsigned long long)con->con_resultpagesize);
    grsc->grsc_err = 0;
    grsc_deliver(grsc, grsc->grsc_result);
    cl_leave(cl, CL_LEVEL_VERBOSE, "done");

    return 0;
  }

  /*  We're also done if we're not (or no longer) sampling
   *  and neither count nor per-element data are requested.
   */
  if (!grsc->grsc_sampling && !counting && !con->con_pframe_want_data &&
      (!con->con_pframe_want_count ||
       grsc->grsc_count_total != (unsigned long long)-1) &&
      !con->con_pframe_want_cursor) {
    cl_log(cl, CL_LEVEL_DEBUG,
           "RXN%*s(B) grsc_are_we_done: yes "
           "(done sampling).",
           2 * greq->greq_indent, "");
    grsc->grsc_err = 0;
    grsc_deliver(grsc, grsc->grsc_result);
    cl_leave(cl, CL_LEVEL_VERBOSE, "done");

    return 0;
  }

  /*  Finally, we're done if the sort thinks we're done
   *  (all the remaining values will be larger than the
   *  current sort array end).
   */
  if (!counting && grsc->grsc_sort != NULL &&
      graphd_sort_accept_ended(grsc->grsc_sort)) {
    cl_log(cl, CL_LEVEL_DEBUG,
           "RXN%*s(B) grsc_are_we_done: yes "
           "(done sorting).",
           2 * greq->greq_indent, "");

    grsc->grsc_err = 0;
    grsc_deliver(grsc, grsc->grsc_result);

    cl_leave(cl, CL_LEVEL_VERBOSE, "done");
    return 0;
  }

  cl_log(cl, CL_LEVEL_VERBOSE,
         "grsc_are_we_done: grsc %p; sort %p; counting: %d; ended %d",
         (void *)grsc, (void *)grsc->grsc_sort, counting,
         grsc->grsc_sort ? graphd_sort_accept_ended(grsc->grsc_sort) : -1);

  /*  In the first round ("verify"), we only continue until we
   *  know whether or not this whole constraint is matched or not.
   *
   *  If there is a minimum count, that minimum must be filled
   *  (usually, it's 1.)
   *
   *  If there is a maximum count, we must have stopped prior
   *  to the maximum (i.e. we can't resume.)
   */
  if (grsc->grsc_verify &&
      grsc_absolute_count(grsc) >= con->con_count.countcon_min &&
      !con->con_count.countcon_max_valid) {
    graphd_value *res = NULL;

    /*  Defer results.  We've run long enough to know that
     *  there will be *some* results - we can fill in the
     *  details later, when all the other stuff in the
     *  subtree has matched.
     */
    cl_log(cl, CL_LEVEL_DEBUG,
           "RXN%*s(B) grsc_are_we_done: yes (defer results).",
           2 * greq->greq_indent, "");

    grsc->grsc_err = graphd_read_set_defer_results(grsc, &res);
    grsc_deliver(grsc, res);

    cl_leave(cl, CL_LEVEL_VERBOSE, "deferring results");
    return 0;
  }

  /*  In the second round and while we're still working in the
   *  first round, we continue until we've filled the page size.
   *  If the client wants a cursor, continue until one more (to know
   *  whether to return the cursor or a well-defined null cursor).
   *
   *  In case of a sort that hasn't been optimized out in favor
   *  of the iterator's natural order, we continue indefinitely.
   */
  cl_log(cl, CL_LEVEL_VERBOSE,
         "grsc_are_we_done: not yet; see you in grsc_next");
  graphd_stack_resume(stack, stack_context, grsc_next);
  cl_leave(cl, CL_LEVEL_VERBOSE, "-> grsc_next");

  return 0;
}

/**
 * @brief Evaluate deferred samples
 *
 *  Find deferred samples in the values corresponding to pf_set
 *  and evaluate them.  We need to know what something is in order
 *  to know whether to keep reading or not!
 *
 * @param stack		execution stack in the request
 * @param stack_context	current read constraint context
 *
 * @return 0 on successful partial execution
 * @return other nonzero errors on system/resource error.
 */
static int grsc_evaluate_deferred_samples(
    graphd_stack *const stack, graphd_stack_context *const stack_context) {
  graphd_read_set_context *const grsc = (void *)stack_context;
  graphd_read_base *const grb = grsc->grsc_base;
  graphd_request *const greq = grb->grb_greq;
  cl_handle *const cl = graphd_request_cl(greq);
  graphd_constraint *const con = grsc->grsc_con;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");

  for (; grsc->grsc_deferred_samples_i < con->con_pframe_n;
       grsc->grsc_deferred_samples_i++) {
    graphd_value *const v = grsc->grsc_result + grsc->grsc_deferred_samples_i;

    if (v->val_type == GRAPHD_VALUE_DEFERRED) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "evaluate element [%zu]",
               grsc->grsc_deferred_samples_i);
      return graphd_value_deferred_push(greq, v);
    } else if (v->val_type == GRAPHD_VALUE_LIST ||
               v->val_type == GRAPHD_VALUE_SEQUENCE) {
      size_t i;

      for (i = 0; i < v->val_list_n; i++) {
        if (v->val_list_contents[i].val_type == GRAPHD_VALUE_DEFERRED) {
          cl_leave(cl, CL_LEVEL_VERBOSE, "evaluate element [%zu,%zu]",
                   grsc->grsc_deferred_samples_i, i);

          return graphd_value_deferred_push(greq, v->val_list_contents + i);
        } else if (v->val_list_contents[i].val_type == GRAPHD_VALUE_UNSPECIFIED)
          grsc->grsc_sampling = true;
      }
    } else if (v->val_type == GRAPHD_VALUE_UNSPECIFIED) {
      /* The value we just evaluated became unspecified -
       * we need to keep sampling.
       */
      grsc->grsc_sampling = true;
    }
  }

  grsc->grsc_deferred_samples = false;
  graphd_stack_resume(stack, stack_context, grsc_are_we_done);
  cl_leave(cl, CL_LEVEL_VERBOSE, "done; -> grsc_are_we_done");

  return 0;
}

/**
 * @brief Part III: Read an ID and match its primitive intrinsics
 *
 *  Read through the next matching records for this constraint.
*
*   Once the algorithm in this module completes, it'll call
*   graphd_read_set_finish(stack, stack_context) and resume
*   work in graphd-read-constraint.
 *
 * @param stack		execution stack in the request
 * @param stack_context	current read constraint context
 *
 * @return 0 on successful partial execution
 * @return other nonzero errors on system/resource error.
 */
static int grsc_next(graphd_stack *stack, graphd_stack_context *stack_context) {
  graphd_read_set_context *grsc = (void *)stack_context;
  graphd_request *greq = grsc->grsc_base->grb_greq;
  graphd_handle *g = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  int err = 0;
  pdb_budget budget = GRAPHD_NEXT_BUDGET;
  pdb_id id;
  char buf[200];

  /*  Get the local ID of the next primitive into grsc_pc.pc_id.
   */
  cl_enter(cl, CL_LEVEL_VERBOSE, "grsc=%p it=%p: %s", (void *)grsc,
           (void *)grsc->grsc_it,
           pdb_iterator_to_string(g->g_pdb, grsc->grsc_it, buf, sizeof buf));

  PDB_IS_ITERATOR(cl, grsc->grsc_it);

  /* Read the ID.
   */
  err = pdb_iterator_next(g->g_pdb, grsc->grsc_it, &id, &budget);
  if (err != 0) {
    if (err == PDB_ERR_MORE) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "(in progress)");
      return 0;
    }
    if (err != GRAPHD_ERR_NO) {
      char buf[200];
      cl_log_errno(
          cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "it=%s",
          pdb_iterator_to_string(g->g_pdb, grsc->grsc_it, buf, sizeof buf));
      grsc->grsc_err = err;
    }
    grsc_deliver(grsc, grsc->grsc_result);

    cl_log(cl, CL_LEVEL_DEBUG, "RXN%*s(C) grsc_next: %s", 2 * greq->greq_indent,
           "",
           err == GRAPHD_ERR_NO ? "out of candidates" : graphd_strerror(err));
    cl_leave(cl, CL_LEVEL_VERBOSE, "%s",
             err == GRAPHD_ERR_NO ? "out of candidates" : graphd_strerror(err));
    return 0;
  }

  /* If we know that this ID doesn't match this constraint,
   * continue immediately.
   */
  if (graphd_bad_cache_member(&grsc->grsc_con->con_bad_cache, id)) {
    graphd_stack_resume(stack, stack_context, grsc_are_we_done);
    cl_leave(cl, CL_LEVEL_VERBOSE, "graphd_bad_cache_member rejects %llx",
             (unsigned long long)id);
    return 0;
  }

  /* Read the primitive corresponding to the ID.
   */
  err = grsc_set_id(grsc, id);
  if (err != 0) {
    if (err != GRAPHD_ERR_NO || pdb_primitive_n(g->g_pdb) <= id) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err, "id=%llx",
                   (unsigned long long)id);
      grsc->grsc_err = err;
    } else {
      cl_log(cl, CL_LEVEL_DEBUG,
             "RXN%*s(C) grsc_next: ID %llx doesn't "
             "exist (skip)",
             2 * greq->greq_indent, "", (unsigned long long)id);
    }
    graphd_stack_resume(stack, stack_context, grsc_are_we_done);
    cl_leave(cl, CL_LEVEL_VERBOSE, "grsc_set_id fails: %s",
             graphd_strerror(err));
    return 0;
  }
  cl_log(cl, CL_LEVEL_DEBUG, "RXN%*s[(C) grsc_next(it=%p): ID %llx",
         2 * greq->greq_indent, "", (void *)grsc->grsc_it,
         (unsigned long long)grsc->grsc_pc.pc_id);
  greq->greq_indent++;

  /* Initialize the data structure we use to keep track of the
   * "OR" state.
   */
  graphd_read_or_initialize(greq, grsc->grsc_con, &grsc->grsc_rom);

  /*  Match the primitive and compute its result:
   *
   *  1. Without looking at the rest of the graph, does the
   *     primitive itself match our constraints?
   *
   *     If no, we will neither count nor store it.
   */
  err = graphd_match(
      greq, grsc->grsc_con, &grsc->grsc_rom, &grsc->grsc_pc.pc_pr,
      grsc->grsc_parent_id != PDB_ID_NONE ? &grsc->grsc_parent_guid : NULL);
  if (err != 0) {
    if (greq->greq_indent > 0) greq->greq_indent--;
    cl_log(cl, CL_LEVEL_DEBUG,
           "RXN%*s](C) grsc_next: "
           "graphd_match rejects id=%llx",
           2 * greq->greq_indent, "", (unsigned long long)grsc->grsc_pc.pc_id);

    graphd_bad_cache_add(&grsc->grsc_con->con_bad_cache, id);
    grsc_release_id(grsc);
    graphd_stack_resume(stack, stack_context, grsc_are_we_done);

    cl_cover(cl);
    cl_leave(cl, CL_LEVEL_VERBOSE, "graphd_match fails: %s",
             graphd_strerror(err));

    return 0;
  }

  /*  2. Where does it fall within the current sort window, if any?
   *
   *     If we reach the end of this block, "loc" is, for now, where
   *     we'll store the value derived from this primitive - should
   *     we end up wishing to store it, that is.  (We may just
   *     be counting.)
   *
   *     If there is no sort window, the location is simply the
   *     array index in the output page.
   */
  if (grsc->grsc_sort == NULL) {
    if (grsc->grsc_count < grsc->grsc_con->con_start)
      grsc->grsc_page_location = (size_t)-1;
    else
      grsc->grsc_page_location = grsc->grsc_count - grsc->grsc_con->con_start;

    cl_cover(cl);
  } else {
    graphd_constraint const *const con = grsc->grsc_con;

    err = graphd_sort_accept_prefilter(grsc->grsc_sort, grsc->grsc_it,
                                       &grsc->grsc_pc.pc_pr,
                                       &grsc->grsc_page_location);

    if (grsc->grsc_page_location == (size_t)-1) {
      cl_cover(cl);

      if (greq->greq_indent > 0) greq->greq_indent--;
      cl_log(cl, CL_LEVEL_DEBUG,
             "RXN%*s](C) grsc_next: "
             "graphd_sort_accept_prefilter rejects id=%llx",
             2 * greq->greq_indent, "",
             (unsigned long long)grsc->grsc_pc.pc_id);

      /*  We're not returning this one.
       *
       *  But maybe we need to count it.
       *  Reasons to count:
       *  - we're returning a count
       *  - there's a countcon on the constraint
       *    that we may yet exceed
       *  - the item is larger than a sort
       *    cursor boundary, if any.
       */

      if (err != GRAPHD_ERR_TOO_LARGE ||
          (con->con_cursor_offset + grsc->grsc_count >=
               con->con_count.countcon_min &&
           !con->con_count.countcon_max_valid &&
           (!con->con_pframe_want_count ||
            grsc->grsc_count_total != (unsigned long long)-1))) {
        /*  No, we don't need a count.
         */
        grsc_release_id(grsc);

        /*  If the iterator is ordered according to this
         *  constraint's sort root,
         *  and the ID falls outside the sort window,
         *  then we're done reading.
         */
        if (graphd_sort_accept_ended(grsc->grsc_sort)) {
          grsc_deliver(grsc, grsc->grsc_result);

          cl_log(cl, CL_LEVEL_DEBUG,
                 "RXN%*s(C) grsc_next: "
                 "ordered iterator "
                 "has left the sort window",
                 2 * greq->greq_indent, "");
          cl_leave(cl, CL_LEVEL_VERBOSE, "done (sort window)");
          return 0;
        }
        graphd_stack_resume(stack, stack_context, grsc_are_we_done);
        cl_leave(cl, CL_LEVEL_VERBOSE, "no page location");
        return 0;
      }
      cl_log(cl, CL_LEVEL_VERBOSE,
             "grsc_next: still counting, even though"
             " the sort has rejected this record");
    }

    cl_log(cl, CL_LEVEL_VERBOSE, "grsc_next: sort into %zu",
           grsc->grsc_page_location);
  }

  /*  grsc->grsc_page_location may be (size_t)-1 if we have a
   *  start offset and we haven't seen that many results yet.
   *
   *  In that case, grsc_one_deliver must check, but not assign.
   */

  /*
   *  3. Evaluate the primitive subconstraints.  Do they match?
   *  	This may call grsc_one_deliver directly.
   */
  graphd_read_one_push(grsc->grsc_base, grsc, grsc->grsc_pc.pc_id,
                       &grsc->grsc_pc.pc_pr, grsc->grsc_con, grsc_one_deliver,
                       (void *)grsc);
  cl_leave(cl, CL_LEVEL_VERBOSE, "-> grsc_one_deliver");
  return 0;
}

/*  Sample one pattern.
 *
 * @param grsc 	the current context
 * @param pat	the pattern to sample
 * @param out	assign the value to this
 * @param res	we got back <res> from the subconstraint.
 */
static int grsc_sample(graphd_read_set_context *grsc, graphd_pattern const *pat,
                       graphd_value *out, graphd_value *res) {
  graphd_request *greq = grsc->grsc_base->grb_greq;
  cl_handle *cl = graphd_request_cl(greq);
  graphd_value *val_in;
  int err;

  cl_assert(cl, out != NULL);
  cl_assert(cl, pat != NULL);

  if (out->val_type != GRAPHD_VALUE_UNSPECIFIED) {
    char b1[200], b2[200];
    cl_log(cl, CL_LEVEL_VERBOSE,
           "grsc_sample: ignore %s, because "
           "result value exists already (as %s).",
           graphd_pattern_dump(pat, b1, sizeof b1),
           graphd_value_to_string(out, b2, sizeof b2));
    return 0;
  }

  if (grsc->grsc_sort != NULL) {
    char buf[200];
    cl_log(cl, CL_LEVEL_VERBOSE, "grsc_sample: saving %s for after the sort",
           graphd_pattern_dump(pat, buf, sizeof buf));
    return 0;
  }

  if (!pat->pat_sample) {
    char buf[200];
    cl_log(cl, CL_LEVEL_VERBOSE,
           "grsc_sample: ignore %s, because it isn't a sample",
           graphd_pattern_dump(pat, buf, sizeof buf));
    return 0;
  }

  /* Sampling a pick means picking an element and sampling *that*.
   */

  val_in = res + pat->pat_result_offset;

  /* Can't sample a list / that doesn't exist! */
  if (val_in->val_type != GRAPHD_VALUE_LIST) {
    char b1[200], b2[200];
    cl_log(cl, CL_LEVEL_VERBOSE,
           "grsc_sample: ignore input %s for %s (at offset %zu),"
           " because it isn't a list",
           graphd_value_to_string(val_in, b1, sizeof b1),
           graphd_pattern_dump(pat, b2, sizeof b2), pat->pat_result_offset);
    return 0;
  }

  val_in = val_in->val_list_contents + pat->pat_element_offset;
  if (val_in->val_type == GRAPHD_VALUE_UNSPECIFIED) {
    char buf[200];
    /*  We want a value, but this input doesn't get
     *  us one.  We need to continue sampling.
     */
    grsc->grsc_sampling = true;
    cl_log(cl, CL_LEVEL_VERBOSE,
           "grsc_sample: can't field %s, value is unspecified",
           graphd_pattern_dump(pat, buf, sizeof buf));
    return 0;
  }

  /*  Remember that we sampled a deferred value.  We need
   *  to evaluate that value, or else we don't know whether
   *  we actually "got" a sample or not!
   */
  if (val_in->val_type == GRAPHD_VALUE_DEFERRED) {
    char buf[200];
    cl_log(cl, CL_LEVEL_VERBOSE,
           "grsc_sample: sampled a deferred value into %s",
           graphd_pattern_dump(pat, buf, sizeof buf));
    grsc->grsc_deferred_samples = true;
  }

  err = graphd_value_copy(graphd_request_graphd(greq), greq->greq_req.req_cm,
                          cl, out, val_in);
  {
    char b1[200], b2[200];
    cl_log(cl, CL_LEVEL_VERBOSE, "grsc_sample: %s := %s",
           graphd_pattern_dump(pat, b1, sizeof b1),
           graphd_value_to_string(val_in, b2, sizeof b2));
  }
  return err;
}

static int grsc_sample_list(graphd_read_set_context *grsc,
                            graphd_pattern const *pat, graphd_value *out,
                            graphd_value *res) {
  graphd_request *const greq = grsc->grsc_base->grb_greq;
  cl_handle *const cl = graphd_request_cl(greq);
  graphd_value *o;
  graphd_pattern const *p;

  cl_assert(cl, out != NULL);
  cl_assert(cl, out->val_type == GRAPHD_VALUE_LIST);

  cl_assert(cl, pat != NULL);
  cl_assert(cl, pat->pat_type == GRAPHD_PATTERN_LIST);

  o = out->val_list_contents;
  p = pat->pat_list_head;
  cl_assert(cl, pat->pat_list_n == out->val_list_n);

  for (; p != NULL; o++, p = p->pat_next) {
    int err;

    /* Not a sample - this is the result set.
     */
    if (p->pat_type == GRAPHD_PATTERN_LIST) continue;

    err = grsc_sample(grsc, p, o, res);
    if (err != 0) return err;
  }
  return 0;
}

/**
 * @brief Take delivery of a single-ID result (from graphd-read-one.c)
 *
 *	This moves the values corresponding to twice-nested arrays
 *  	into their respective slots.
 *
 * @param grsc	the grsc context
 * @param res	result array (with con->pframe_n elements)
 *  		for that one ID
 */
static int grsc_one_deliver_per_instance_data(graphd_read_set_context *grsc,
                                              graphd_value *res) {
  graphd_read_base *grb = grsc->grsc_base;
  graphd_request *greq = grb->grb_greq;
  graphd_handle *g = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  size_t i;
  graphd_constraint *con = grsc->grsc_con;
  graphd_pattern_frame const *pf = con->con_pframe;

  /*  If our count is before start, there's nothing
   *  to do for us - unless we're sorted.
   */
  if (grsc->grsc_sort == NULL && grsc->grsc_count < con->con_start) return 0;

  for (i = 0; i < con->con_pframe_n; i++, pf++) {
    graphd_value *v, *li;

    /*  Fill unspecified samples.
     */
    if (pf->pf_set == NULL || pf->pf_one == NULL) continue;

    li = grsc->grsc_result + i;
    cl_assert(cl, li->val_type == GRAPHD_VALUE_LIST);

    li = li->val_list_contents + pf->pf_one_offset;
    cl_assert(cl, li->val_type == GRAPHD_VALUE_SEQUENCE);

    /*  Add another record to the sequence, if needed;
     *  free a previous entry, if needed.
     */
    if (grsc->grsc_page_location == (size_t)-1) {
      /*  We have no place for this.  We usede it
       *  to count, but that's it.
       */
      graphd_value_finish(cl, res + i);
    } else {
      if (grsc->grsc_page_location >= li->val_list_n) {
        cl_assert(cl, grsc->grsc_page_location == li->val_list_n);

        v = graphd_value_array_alloc(g, cl, li, 1);
        if (v == NULL) return errno ? errno : ENOMEM;

        *v = res[i];
        graphd_value_array_alloc_commit(cl, li, 1);
      } else {
        v = li->val_list_contents + grsc->grsc_page_location;

        graphd_value_finish(cl, v);
        *v = res[i];
      }
    }

    /*  Clear out the value we just moved from the
     *  return value.
     */
    graphd_value_initialize(res + i);
  }
  return 0;
}

/**
 * @brief End piece for both grsc_one_deliver and grsc_one_redeliver.
 *
 *   The requested item may have dropped out of the sort window, but
 *   it does match the subconstraints, etc.
 */
static int grsc_one_deliver_count_success(graphd_read_set_context *grsc) {
  graphd_read_base *grb = grsc->grsc_base;
  graphd_request *greq = grb->grb_greq;
  cl_handle *cl = graphd_request_cl(greq);
  graphd_constraint *con = grsc->grsc_con;
  int err;

  cl_enter(cl, CL_LEVEL_VERBOSE, "count := %llu", grsc->grsc_count + 1);

  /*  Count it as matched.
   */
  grsc->grsc_count++;

  /*  If we're exactly at our resultpagesize, and we want a
   *  cursor, and this isn't sorted, store a cursor.
   *  We'll still read the next element to find out
   *  whether that cursor is actually worth keeping,
   *  but the one we want is the one we'd get now,
   *  not the one we'd see after reading yet another
   *  element.
   */

  if (grsc->grsc_sort == NULL && con->con_pframe_want_cursor &&
      con->con_resultpagesize_valid &&
      grsc->grsc_count == con->con_start + con->con_resultpagesize) {
    size_t i;

    for (i = 0; i < con->con_pframe_n; i++) {
      err = graphd_read_set_cursor_get(grsc, con->con_pframe + i,
                                       grsc->grsc_result + i);
      if (err != 0) {
        cl_leave(cl, CL_LEVEL_VERBOSE,
                 "error from "
                 "graphd_read_set_cursor_get: %s",
                 graphd_strerror(err));
        return err;
      }
    }
  }

  /*  If, during sampling, we acquired deferred values
   *  in our sampling grid, we need to evaluate those
   *  deferred values *now* - otherwise, we won't know
   *  whether they're actually unspecified (and we need
   *  to go on sampling through the next result) or not.
   */
  if (grsc->grsc_deferred_samples && grsc->grsc_page_location != (size_t)-1) {
    /* Go on to "evaluate deferred samples".
     */
    grsc->grsc_deferred_samples_i = 0;
    graphd_stack_resume(&greq->greq_stack, &grsc->grsc_sc,
                        grsc_evaluate_deferred_samples);
    cl_leave(cl, CL_LEVEL_VERBOSE, "see you in grsc_evaluate_deferred_samples");
  } else {
    /* Go on directly to "are we done yet?".
     */
    graphd_stack_resume(&greq->greq_stack, &grsc->grsc_sc, grsc_are_we_done);
    cl_leave(cl, CL_LEVEL_VERBOSE, "see you in grsc_are_we_done");
  }
  return 0;
}

/**
 * @brief Finish grsc_one_deliver after a deferred value has been evaluated.
 *
 *  We may have more deferred values to evaluate; or we may go on to
 *  grsc_one_deliver_count_success().
 */
static int grsc_one_redeliver(graphd_stack *stack,
                              graphd_stack_context *stack_context) {
  graphd_read_set_context *grsc = (void *)stack_context;
  graphd_request *greq = grsc->grsc_base->grb_greq;
  cl_handle *cl = graphd_request_cl(greq);
  graphd_value *please_evaluate = NULL;
  int err = 0;

  cl_enter(cl, CL_LEVEL_VERBOSE, "grsc=%p, sort=%p", (void *)grsc,
           grsc->grsc_sort);
  cl_assert(cl, grsc->grsc_sort != NULL);

  if (grsc->grsc_err != 0) {
    if (grsc->grsc_err == GRAPHD_ERR_NO) grsc->grsc_err = 0;

    graphd_stack_resume(&greq->greq_stack, &grsc->grsc_sc, grsc_are_we_done);

    cl_leave(cl, CL_LEVEL_VERBOSE, "no; oh well. back to grsc_are_we_done");
    return 0;
  }

  if (grsc->grsc_page_location != (size_t)-1) {
    err = graphd_sort_accept(grsc->grsc_sort, grsc->grsc_it, &please_evaluate);
    if (err == GRAPHD_ERR_NO) {
      /*  Even if it didn't make it into
       *  the sort window, it still counts
       *  as success.
       */
      err = grsc_one_deliver_count_success(grsc);
      cl_leave(cl, CL_LEVEL_VERBOSE, "no; sort didn't like it");
      return err;
    }
  }
  if (err == PDB_ERR_MORE) {
    cl_assert(cl, please_evaluate != NULL);
    err = graphd_value_deferred_push(greq, please_evaluate);
    if (err != 0) return err;

    cl_leave(cl, CL_LEVEL_VERBOSE, "but first ...");
    return 0;
  } else if (err != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "graphd_sort_accept: %s",
             graphd_strerror(err));
    return err;
  }

  cl_leave(cl, CL_LEVEL_VERBOSE, "ok, sort accepts it");
  return grsc_one_deliver_count_success(grsc);
}

/**
 * @brief Take delivery of a single-ID result (from graphd-read-one.c)
 *
 *  If we get here with err==0, then the id was matched.
 *  The only thing that can go wrong now is that it's outside
 *  of the sort range.
 *
 * @param data	the grsc context
 * @param err	result for that one ID
 * @param con	constraint
 * @param id	ID that matched (or didn't)
 * @param res	for err=0, vector of results.
 */
static void grsc_one_deliver(void *data, int err, pdb_id id,
                             graphd_constraint const *con, graphd_value *res) {
  graphd_read_set_context *grsc = (void *)data;
  graphd_read_base *grb = grsc->grsc_base;
  graphd_request *greq = grb->grb_greq;
  cl_handle *cl = graphd_request_cl(greq);
  size_t i;
  graphd_pattern_frame const *pf = con->con_pframe;

  cl_enter(cl, CL_LEVEL_VERBOSE, "id=%llx %s", (unsigned long long)id,
           err ? graphd_strerror(err) : "ok");

  if (greq->greq_indent > 0) greq->greq_indent--;
  cl_log(cl, CL_LEVEL_DEBUG, "RXN%*s](C) grsc_one_deliver: ID %llx; %s",
         2 * greq->greq_indent, "", (unsigned long long)grsc->grsc_pc.pc_id,
         err ? graphd_strerror(err) : "ok");

  if (err != 0) {
  err:
    if (err != GRAPHD_ERR_NO)
      grsc->grsc_err = err;
    else
      graphd_bad_cache_add(&grsc->grsc_con->con_bad_cache, grsc->grsc_pc.pc_id);

    graphd_stack_resume(&greq->greq_stack, &grsc->grsc_sc, grsc_are_we_done);

    cl_leave(cl, CL_LEVEL_VERBOSE, "no; oh well. back to grsc_are_we_done");
    return;
  }

  /*  If we're one past our pagesize and are
   *  looking for a cursor, we may just have evaluated
   *  this to tell whether or not we have a null cursor.
   */

  /*  We sample now unless we're waiting for a sort
   *  (or we're already over our pagesize, or don't need
   *  to sample to begin with).
   */
  if (grsc->grsc_sort == NULL &&
      (!con->con_resultpagesize_valid ||
       grsc->grsc_count < con->con_start + con->con_resultpagesize) &&
      grsc->grsc_count >= con->con_start && grsc->grsc_sampling) {
    /*  This will be reset to true if we
     *  don't find something we're looking for.
     */
    grsc->grsc_sampling = false;

    for (i = 0; i < con->con_pframe_n; i++, pf++) {
      /*  Fill unspecified samples.
       */
      if (pf->pf_set == NULL) continue;

      if (pf->pf_set->pat_type == GRAPHD_PATTERN_LIST)
        err = grsc_sample_list(grsc, pf->pf_set, grsc->grsc_result + i, res);
      else
        err = grsc_sample(grsc, pf->pf_set, grsc->grsc_result + i, res);
      if (err != 0) goto err;
    }
  }

  /*  We take delivery of the instance data if
   *  - we're sorting (in which case we need to read everything!)
   *  - or if we haven't filled up the page yet.
   */
  cl_assert(cl, con->con_resultpagesize_valid);
  if ((grsc->grsc_count < con->con_start + con->con_resultpagesize &&
       grsc->grsc_count >= con->con_start) ||
      grsc->grsc_sort != NULL) {
    err = grsc_one_deliver_per_instance_data(grsc, res);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "grsc_one_deliver_per_instance_data", err,
                   "unexpected error");
      goto err;
    }

    /*  If we're sorting, graphd_sort_accept() tells
     *  us whether or not it accepts the record into
     *  its result set.  In the course of deciding
     *  that, further evaluation of deferred results
     *  may become necessary - be prepared to react
     *  to an PDB_ERR_MORE by scheduling evaluation.
     */
    if (grsc->grsc_sort != NULL) {
      graphd_value *please_evaluate = NULL;

      err =
          graphd_sort_accept(grsc->grsc_sort, grsc->grsc_it, &please_evaluate);

      if (err == GRAPHD_ERR_NO) {
        err = grsc_one_deliver_count_success(grsc);
        if (err != 0) goto err;
        cl_leave(cl, CL_LEVEL_VERBOSE, "no; sort didn't like it");
        return;
      } else if (err == PDB_ERR_MORE) {
        cl_assert(cl, please_evaluate != NULL);
        err = graphd_value_deferred_push(greq, please_evaluate);
        if (err != 0) goto err;

        graphd_stack_resume(&greq->greq_stack, &grsc->grsc_sc,
                            grsc_one_redeliver);

        cl_leave(cl, CL_LEVEL_VERBOSE, "see you in grsc_one_redeliver");
        return;
      } else if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_sort_accept", err,
                     "unexpected error");
        goto err;
      }
    }
  }

  err = grsc_one_deliver_count_success(grsc);
  if (err != 0) goto err;

  cl_leave(cl, CL_LEVEL_VERBOSE, "grsc %p, grsc_link %zu", (void *)grsc,
           (size_t)grsc->grsc_link);
}

/**
 * @brief Utility: create the iterator for a subconstraint
 *
 *  Note that not all subconstraint iterator constructors
 *  go through here - if there's a stored cursor string,
 *  that string is used instead.
 *
 * @param greq		Request whose stack we're pushing on
 * @param con		Constraint caller wants to match
 * @param parent_id	NULL or ID of the parent
 * @param parent_pr	NULL or parent's whole primitive
 * @param it_out	Out: the iterator over child candidates
 *
 * @return 0 on success, a nonzero error code on unexpected error.
 */
static int grsc_subconstraint_iterator(graphd_request *greq,
                                       graphd_constraint *con, pdb_id parent_id,
                                       pdb_primitive const *parent_pr,
                                       pdb_iterator **it_out) {
  graphd_session *const gses = graphd_request_session(greq);
  graphd_handle *const g = gses->gses_graphd;
  cl_handle *const cl = gses->gses_cl;
  pdb_handle *const pdb = g->g_pdb;
  pdb_iterator *sub = NULL;
  pdb_iterator *and_clone = NULL;
  int err = 0;
  char buf[200];
  graph_guid *type_guid = NULL;
  pdb_id type_id = PDB_ID_NONE;
  char const *ordering = NULL;
  graphd_direction direction;

  cl_enter(cl, CL_LEVEL_SPEW, "(%s)", graphd_constraint_to_string(con));

  cl_assert(cl, parent_pr != NULL);
  cl_assert(cl, graphd_linkage_is_i_am(con->con_linkage) ||
                    graphd_linkage_is_my(con->con_linkage));

  if (!GRAPH_GUID_IS_NULL(con->con_linkguid[PDB_LINKAGE_TYPEGUID])) {
    type_guid = con->con_linkguid + PDB_LINKAGE_TYPEGUID;
    err = pdb_id_from_guid(pdb, &type_id, type_guid);
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
      return err;
    }
  }

  if (con->con_cursor_s != NULL) {
    cl_assert(cl, con->con_cursor_e != NULL);

    PDB_IS_ITERATOR(gses->gses_cl, con->con_it);
    err = pdb_iterator_clone(pdb, con->con_it, it_out);
  } else if (graphd_linkage_is_i_am(con->con_linkage)) {
    graph_guid guid;

    /*  We have a parent.  It points to us.
     *  There is at most one GUID that our parent points to.
     *
     *  Find it, and stick it into a fresh subconstraint cursor.
     *  If anything non-drastic goes wrong, create a null iterator
     *  and let the regular iteration code sort it out.
     */
    if (!pdb_primitive_has_linkage(parent_pr,
                                   graphd_linkage_i_am(con->con_linkage)))
      goto null_iterator;

    pdb_primitive_linkage_get(parent_pr, graphd_linkage_i_am(con->con_linkage),
                              guid);
    err = graphd_iterator_fixed_create_guid_array(
        g, &guid, 1, con->con_low, con->con_high, con->con_forward, it_out);
  }

  /*  We point to our parent.  Our cursor is:
   *
   *	AND(  parent's fan-in,
   *	      precomputed subconstraints )
   */
  else {
    err = graphd_iterator_vip_create(
        g, parent_id, graphd_linkage_my(con->con_linkage), type_id, type_guid,
        con->con_low, con->con_high, con->con_forward,
        /* error_if_null */ false, &sub);
    if (err != 0) goto err;

    if (pdb_iterator_all_is_instance(pdb, con->con_it)) {
      *it_out = sub;
      sub = NULL;
    } else {
      pdb_primitive_summary psum;

      err = pdb_iterator_primitive_summary(pdb, sub, &psum);
      if (err == 0) {
        err = pdb_iterator_restrict(pdb, con->con_it, &psum, &and_clone);
        if (err == PDB_ERR_NO) goto null_iterator;

        if (err == PDB_ERR_ALREADY)
          err = pdb_iterator_clone(pdb, con->con_it, &and_clone);
        if (err != 0) goto err;
      } else if (err == PDB_ERR_NO) {
        err = pdb_iterator_clone(pdb, con->con_it, &and_clone);
        if (err != 0) goto err;
      } else
        goto err;

      /* Create an "AND" iterator with 2 elements:
       *    - a linkage iterator (pointing to our parent)
       *    - a restricted clone of the stored constraint
       * 	iterator
       */
      direction = graphd_sort_root_iterator_direction(greq, con, &ordering);
      cl_assert(cl, direction != GRAPHD_DIRECTION_ORDERING || ordering != NULL);

      err = graphd_iterator_and_create(greq, 2, con->con_low, con->con_high,
                                       direction, ordering, it_out);
      if (err != 0) goto err;

      if (con->con_resultpagesize_valid)
        graphd_iterator_and_set_context_pagesize(
            g, *it_out, con->con_resultpagesize + con->con_start);

      graphd_iterator_and_set_context_setsize(g, *it_out, con->con_setsize);
      graphd_constraint_account(greq, con, sub);

      if ((err = graphd_iterator_and_add_subcondition(g, *it_out, &sub)) == 0 &&
          (err = graphd_iterator_and_add_subcondition(g, *it_out,
                                                      &and_clone)) == 0)

        err = graphd_iterator_and_create_commit(g, *it_out);

      pdb_iterator_destroy(pdb, &and_clone);
      pdb_iterator_destroy(pdb, &sub);
    }
  }
  if (err != 0) goto err;

  if (*it_out != NULL) graphd_constraint_account(greq, con, *it_out);

  cl_leave(cl, CL_LEVEL_SPEW, "it=%s",
           pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
  return err;

null_iterator:
  pdb_iterator_destroy(pdb, &sub);
  pdb_iterator_destroy(pdb, &and_clone);
  pdb_iterator_destroy(pdb, it_out);

  cl_leave(cl, CL_LEVEL_SPEW, "null");
  return pdb_iterator_null_create(pdb, it_out);

err:
  pdb_iterator_destroy(pdb, &sub);
  pdb_iterator_destroy(pdb, &and_clone);
  pdb_iterator_destroy(pdb, it_out);

  cl_leave(cl, CL_LEVEL_SPEW, "error: %s", graphd_strerror(err));
  return err;
}

static int grsc_shape_result(graphd_read_set_context *const grsc,
                             graphd_value *const val,
                             graphd_pattern_frame const *const pf) {
  graphd_request *const greq = grsc->grsc_base->grb_greq;
  graphd_handle *const g = graphd_request_graphd(greq);
  cl_handle *const cl = graphd_request_cl(greq);
  cm_handle *const cm = greq->greq_req.req_cm;
  int err;

  cl_assert(cl, val != NULL);
  cl_assert(cl, pf != NULL);

  /*  Allocate a value in the shape of a pattern.
   */
  if (pf->pf_set == NULL) {
    val->val_type = GRAPHD_VALUE_UNSPECIFIED;
    return 0;
  }
  if (pf->pf_set->pat_type != GRAPHD_PATTERN_LIST) return 0;

  err = graphd_value_list_alloc(g, cm, cl, val, pf->pf_set->pat_list_n);
  if (err != 0) return err;

  if (pf->pf_one == NULL) return 0;
  cl_assert(cl, pf->pf_one->pat_type == GRAPHD_PATTERN_LIST);

  /*  Allocate a sequence in pf->pf_one's slot.  The sequence
   *  elements will be per-id list copies of pf->pf_one.
   */
  graphd_value_sequence_set(cm, val->val_list_contents + pf->pf_one_offset);
  return 0;
}

/* @brief Push a producer for the set requested by con.
 *
 * @param grb		request context
 * @param con		constraint we'd like to check
 * @param parent_id	PDB_ID_NONE or parent ID
 * @param parent_pr	NULL or parent primitive (optional)
 * @param callback	deliver results via this
 * @param callback_data	passed in when calling the callback.
 */
void graphd_read_set_push(graphd_read_base *grb, graphd_constraint *con,
                          pdb_id parent_id, pdb_primitive const *parent_pr,
                          graphd_read_set_callback *callback,
                          void *callback_data) {
  graphd_read_set_context *grsc = NULL;

  graphd_request *greq = grb->grb_greq;
  graphd_session *gses = graphd_request_session(greq);
  graphd_handle *g = gses->gses_graphd;
  cl_handle *cl = gses->gses_cl;
  cm_handle *cm = greq->greq_req.req_cm;
  size_t i;
  int err = 0;

  cl_enter(cl, CL_LEVEL_VERBOSE, "%s", graphd_constraint_to_string(con));

  cl_assert(cl, con != NULL);
  cl_assert(cl, callback != NULL);

  /*  If the constraint is implicitly impossible to satisfy,
   *  and we care, let's stop it right here.
   */
  if (con->con_false && GRAPHD_CONSTRAINT_IS_MANDATORY(con)) {
    err = GRAPHD_ERR_NO;
    goto err;
  }

  /*  Allocate space for context and results.
   */
  grsc =
      cm_zalloc(cm, sizeof(*grsc) + sizeof(graphd_value) * con->con_pframe_n);
  if (grsc == NULL) {
    err = errno ? errno : ENOMEM;
    goto err;
  }

  grsc->grsc_result = (graphd_value *)(grsc + 1);
  grsc->grsc_callback = callback;
  grsc->grsc_callback_data = callback_data;

  grsc->grsc_base = grb;
  grsc->grsc_pc.pc_id = PDB_ID_NONE;
  grsc->grsc_pc.pc_pr_valid = false;
  grsc->grsc_sort = NULL;
  grsc->grsc_con = con;
  grsc->grsc_count_total = (unsigned long long)-1;
  grsc->grsc_link = 1;
  grsc->grsc_verify = true;
  grsc->grsc_sampling = true;

  /*  Just so we don't hold a lock on the parent primitive
   *  beyond what we need to -
   *
   *  If this constraint is pointed to by the parent,
   *  just read the constraint GUID.
   *
   *  If this constraint points to the parent, remember
   *  where the parent points.
   */

  /*  Preshape result frames.
   */
  for (i = 0; i < con->con_pframe_n; i++) {
    err = grsc_shape_result(grsc, grsc->grsc_result + i, con->con_pframe + i);
    if (err != 0) goto err;

    char b1[200], b2[200];
    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_read_set_push: shape #%zu pat=%s%s val=%s", i,
           con->con_pframe[i].pf_set ? "" : "(one)",
           graphd_pattern_dump(con->con_pframe[i].pf_set
                                   ? con->con_pframe[i].pf_set
                                   : con->con_pframe[i].pf_one,
                               b1, sizeof b1),
           graphd_value_to_string(grsc->grsc_result + i, b2, sizeof b2));
  }

  /*  Create the per-read-context iterator that will return
   *  candidates for a match.
   */
  if (con->con_parent == NULL) {
    err = pdb_iterator_clone(g->g_pdb, con->con_it, &grsc->grsc_it);
  } else {
    cl_assert(cl, parent_id != PDB_ID_NONE);
    cl_assert(cl, parent_pr != NULL);

    err = grsc_subconstraint_iterator(greq, con, parent_id, parent_pr,
                                      &grsc->grsc_it);
    pdb_primitive_guid_get(parent_pr, grsc->grsc_parent_guid);
    grsc->grsc_parent_id = parent_id;
  }
  if (err != 0) goto err;

  /*  Shortcut: If we have a null iterator, and this has a
   *  minimum count of > 0, throw it out right here - don't
   *  even push it.
   */
  if (con->con_count.countcon_min > 0 &&
      pdb_iterator_null_is_instance(g->g_pdb, grsc->grsc_it)) {
    err = GRAPHD_ERR_NO;
    goto err;
  }

  graphd_stack_push(&greq->greq_stack, (graphd_stack_context *)grsc,
                    &grsc_resource_type, &grsc_stack_type);

  graphd_stack_resume(
      &greq->greq_stack, (graphd_stack_context *)grsc,
      grsc_should_do_statistics(grsc) ? grsc_statistics : grsc_are_we_done);

  cl_leave(cl, CL_LEVEL_VERBOSE, "(grsc=%p) -> grsc_statistics", (void *)grsc);
  return;

err:
  if (grsc != NULL) graphd_read_set_free(grsc);

  (*callback)(callback_data, err, con, NULL);
  cl_leave(cl, CL_LEVEL_SPEW, "error: %s", graphd_strerror(err));
}

/* @brief Push a producer for the set requested by con.
 *
 *  The grsc context in this call was originally created
 *  by graphd_read_set_push().  It was then stored in a
 *  deferred value; now someone has become curious about
 *  the actual (non-deferred) values involved (e.g. for
 *  sorting), and we need to go back to the grsc and actually
 *  evaluate it, producing a set of results.
 *
 * @param grsc		request context allocated by
 *			graphd_read_set_push()
 * @param callback	deliver results via this
 * @param callback_data	passed in when calling the callback.
 */
void graphd_read_set_resume(graphd_read_set_context *grsc,
                            graphd_read_set_callback *callback,
                            void *callback_data) {
  graphd_request *greq = grsc->grsc_base->grb_greq;
  graphd_session *gses = graphd_request_session(greq);
  cl_handle *cl = gses->gses_cl;

  cl_enter(cl, CL_LEVEL_SPEW, "grsc=%p, con=%s", (void *)grsc,
           graphd_constraint_to_string(grsc->grsc_con));

  cl_assert(cl, grsc != NULL);
  cl_assert(cl, callback != NULL);

  grsc->grsc_callback = callback;
  grsc->grsc_callback_data = callback_data;
  grsc->grsc_sampling = true;

  /*  By setting the "verify" flag to false, we make sure
   *  that this time, evaluation runs to completion.
   */
  grsc->grsc_verify = false;

  /*  The grsc at this point is probably also linked to
   *  from the deferred base of a value that points to it -
   *  we need to add a link for pushing it on the stack here.
   */
  grsc->grsc_link++;

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_read_set_resume: "
         "grsc=%p, ++grsc_link=%zu",
         (void *)grsc, (size_t)grsc->grsc_link);

  /*  Push the grsc on the stack.
   */
  graphd_stack_push(&greq->greq_stack, (graphd_stack_context *)grsc,
                    &grsc_resource_type, &grsc_stack_type);

  /* (Re-)start with statistics.  (If we already did
   * statistics, that'll just breeze through.)
   */
  graphd_stack_resume(&greq->greq_stack, (graphd_stack_context *)grsc,
                      grsc_statistics);

  cl_leave(cl, CL_LEVEL_SPEW, "-> grsc_statistics");
}
