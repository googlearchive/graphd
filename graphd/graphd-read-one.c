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

/*  This module is involved with extracting values from a single ID,
 *  given a constraint.
 *
 *  Results are returned via callback.
 *
 *  The values are returned using a result value vector groc->groc_result
 *  whose elements groc->groc_result[i] correspond to instances of
 *  con->con_pframe[i].
 *
 *  Layout in the result vector:
 *
 *		assignment[0]'s value   (con->con_pframe[0].pf_one)
 *		assignment[1]'s value   (con->con_pframe[1].pf_one)
 *		...
 *		assignment[N-1]'s value (con->con_pframe[N-1].pf_one)
 *	     [  result			(con->con_pframe[N].pf_one)   ]
 *	     [  temporary		(con->con_pframe[N+1].pf_one) ]
 *
 *  If an element fails to match, that fact is delivered as an GRAPHD_ERR_NO
 *  error code, and no values are filled in.
 *
 *  If a pf_one is NULL (e.g. because the call just samples), the
 *  corresponding result value remains GRAPHD_VALUE_UNSPECIFIED.
 */

/* If we cached a primitive, release it.  We can always
 * reload the primitive using groc_cache_pr.
 */
static void groc_release_pr(graphd_read_one_context *groc) {
  if (groc->groc_pc.pc_pr_valid) {
    graphd_request *const greq = groc->groc_base->grb_greq;
    graphd_handle *const g = graphd_request_graphd(greq);

    pdb_primitive_finish(g->g_pdb, &groc->groc_pc.pc_pr);
    groc->groc_pc.pc_pr_valid = false;
  }
}

/*  Cache the primitive we're working on.
 */
static int groc_cache_pr(graphd_read_one_context *groc) {
  if (!groc->groc_pc.pc_pr_valid) {
    graphd_request *const greq = groc->groc_base->grb_greq;
    graphd_handle *const g = graphd_request_graphd(greq);
    cl_handle *const cl = graphd_request_cl(greq);
    int err;

    err = pdb_id_read(g->g_pdb, groc->groc_pc.pc_id, &groc->groc_pc.pc_pr);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err, "id=%lld",
                   (long long)groc->groc_pc.pc_id);
      return err;
    }
    groc->groc_pc.pc_pr_valid = true;
  }
  return 0;
}

/*  @brief Free a read context.
 *
 *  Called from the context's resource free
 *  method, and from various places that allocate a context and
 *  have to bail out.
 *
 *  @param groc	The context to free
 */
void graphd_read_one_context_free(graphd_read_one_context *groc) {
  graphd_read_base *grb = groc->groc_base;
  graphd_request *greq = grb->grb_greq;
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  size_t i;
  graphd_value *val;

  if (groc->groc_link-- > 1) return;

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_read_one_context_free %llx %s",
         (unsigned long long)groc->groc_pc.pc_id,
         graphd_constraint_to_string(groc->groc_con));

  if (groc->groc_parent != NULL) {
    graphd_read_set_free(groc->groc_parent);
    groc->groc_parent = NULL;
  }

  /* Free results.
   */
  val = groc->groc_result;
  for (i = groc->groc_con->con_pframe_n; i > 0; i--)
    graphd_value_finish(cl, val++);

  /*  Free the "contents" temporary.
   */
  graphd_value_finish(cl, &groc->groc_contents);

  /*  Free values returned by variable assignments from below.
   */
  val = groc->groc_local;
  for (i = groc->groc_con->con_local_n; i > 0; i--)
    graphd_value_finish(cl, val++);

  /*  Free any remaining primitive record.
   */
  groc_release_pr(groc);

  /*  Free the context itself.
   */
  cm_free(cm, groc);
}

void graphd_read_one_context_link(graphd_read_one_context *groc) {
  if (groc != NULL) groc->groc_link++;
}

/**
 * @brief resource type
 */

static void groc_resource_free(void *resource_manager_data,
                               void *resource_data) {
  graphd_read_one_context_free(resource_data);
}

static void groc_resource_list(void *log_data, void *resource_manager_data,
                               void *resource_data) {
  cl_handle *cl = log_data;
  graphd_read_one_context *groc = resource_data;

  cl_log(cl, CL_LEVEL_VERBOSE, "read one context %llx: %s",
         (unsigned long long)groc->groc_pc.pc_id,
         graphd_constraint_to_string(groc->groc_con));
}

static cm_resource_type groc_resource_type = {
    "constraint read context", groc_resource_free, groc_resource_list};

static int groc_stack_run(graphd_stack *stack,
                          graphd_stack_context *stack_context);

static int groc_stack_suspend(graphd_stack *stack,
                              graphd_stack_context *stack_context) {
  graphd_read_one_context *groc = (void *)stack_context;
  graphd_read_base *grb = groc->groc_base;
  graphd_request *const greq = grb->grb_greq;
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  size_t i;
  int err;
  graphd_value *val;

  if (groc->groc_sc.sc_suspended) return 0;

  groc->groc_sc.sc_suspended = true;

  val = groc->groc_result;
  for (i = groc->groc_con->con_pframe_n; i > 0; i--) {
    err = graphd_value_suspend(cm, cl, val++);
    if (err != 0) return err;
  }

  val = groc->groc_local;
  for (i = groc->groc_con->con_local_n; i > 0; i--) {
    err = graphd_value_suspend(cm, cl, val++);
    if (err != 0) return err;
  }

  err = graphd_value_suspend(cm, cl, &groc->groc_contents);
  if (err != 0) return err;

  groc_release_pr(groc);

  return 0;
}

static int groc_stack_unsuspend(graphd_stack *stack,
                                graphd_stack_context *stack_context) {
  graphd_read_one_context *groc = (void *)stack_context;

  if (!groc->groc_sc.sc_suspended) return 0;
  groc->groc_sc.sc_suspended = false;

  return groc_cache_pr(groc);
}

static const graphd_stack_type groc_stack_type = {
    groc_stack_run, groc_stack_suspend, groc_stack_unsuspend};

/**
 * @brief Retrieve the value for a single per-primitive pattern.
 *
 * @param groc	read context
 * @param pat	pattern to fill
 * @param res	store the result here
 *
 *  Retrieves a value if it can.  It is not an error
 *  to be unable to supply a value because that value
 *  isn't known yet.
 *
 * @result 0 on success, a nonzero error code on unexpected error.
 */
static int groc_sample(graphd_read_one_context *groc, graphd_pattern const *pat,
                       graphd_value *res) {
  graphd_request *greq = groc->groc_base->grb_greq;
  graphd_handle *g = graphd_request_graphd(greq);
  cl_handle *const cl = graphd_request_cl(greq);
  cm_handle *const cm = greq->greq_req.req_cm;
  pdb_primitive const *pr;
  graphd_value const *src = NULL;
  graphd_pattern const *sub_pat;
  graphd_read_or_map const *rom = NULL;
  graphd_variable_declaration *vdecl;
  char buf[200];
  size_t i;
  int err;

again:
  cl_log(cl, CL_LEVEL_VERBOSE, "groc_sample(%s)",
         graphd_pattern_dump(pat, buf, sizeof buf));
  cl_assert(cl, pat != NULL);
  cl_assert(cl, res != NULL);

  /*  Make sure the primitive is cached.
   */
  if ((err = groc_cache_pr(groc)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "groc_cache_pr", err,
                 "can't cache primitive for sample %s?",
                 graphd_pattern_dump(pat, buf, sizeof buf));
    return err;
  }
  pr = &groc->groc_pc.pc_pr;

  /*  In some cases (namely, the default), whether or not a pattern
   *  is included depends on the primitive in question.
   *
   *  Evaluate those conditionals.
   */
  if (pat->pat_link_only) {
    /* A primitive is "a link" if it has a
     * right or left pointer.
     */
    if (!pdb_primitive_has_left(pr) && !pdb_primitive_has_right(pr)) {
      /*  No -> This pattern evaluates to an empty sequence.
       */
      graphd_value_sequence_set(cm, res);
      return 0;
    }
  }

  if (pat->pat_contents_only) {
    /* A primitive "has contents" if the groc's contents
     * field is non-empty.
     */
    if (groc->groc_contents.val_type != GRAPHD_VALUE_SEQUENCE ||
        groc->groc_contents.val_sequence_n == 0) {
      /*  No -> This pattern evaluates to an empty sequence.
       */
      graphd_value_sequence_set(cm, res);
      return 0;
    }
  }

  switch (pat->pat_type) {
    case GRAPHD_PATTERN_COUNT:
    case GRAPHD_PATTERN_CURSOR:
    case GRAPHD_PATTERN_ITERATOR:
    case GRAPHD_PATTERN_ESTIMATE:
    case GRAPHD_PATTERN_ESTIMATE_COUNT:
      return 0;

    case GRAPHD_PATTERN_LIST:

      /*  Make a list, and fill it with the sampled
       *  contents of the pattern list.
       */
      cl_log(cl, CL_LEVEL_VERBOSE,
             "groc_sample: got pattern list at %p; %zu entries; %s",
             (void *)pat, pat->pat_list_n,
             graphd_pattern_dump(pat, buf, sizeof buf));

      err = graphd_value_list_alloc(g, cm, cl, res, pat->pat_list_n);
      if (err != 0) return err;
      for (sub_pat = pat->pat_list_head, i = 0;
           sub_pat != NULL && i < pat->pat_list_n;
           sub_pat = sub_pat->pat_next, i++) {
        err = groc_sample(groc, sub_pat, res->val_list_contents + i);
        if (err != 0) {
          graphd_value_finish(cl, res);
          return err;
        }
      }
      return 0;

    case GRAPHD_PATTERN_CONTENTS:
      src = &groc->groc_contents;
      cl_assert(cl, src != NULL);
      break;

    case GRAPHD_PATTERN_VARIABLE:
      vdecl = pat->pat_variable_declaration;

      cl_assert(cl, groc->groc_local != NULL);
      cl_assert(cl, vdecl != NULL);
      if (vdecl->vdecl_constraint != groc->groc_con) {
        cl_notreached(cl,
                      "vdecl->vdecl_constraint: %p %s; "
                      "groc->groc_con: %p %s",
                      (void *)vdecl->vdecl_constraint,
                      graphd_constraint_to_string(vdecl->vdecl_constraint),
                      (void *)groc->groc_con,
                      graphd_constraint_to_string(groc->groc_con));
      }

      cl_log(cl, CL_LEVEL_VERBOSE, "vdecl %p, vdecl_local %zu, n %zu",
             (void *)vdecl, vdecl->vdecl_local, groc->groc_con->con_local_n);
      cl_assert(cl, vdecl->vdecl_constraint == groc->groc_con);
      cl_assert(cl, vdecl->vdecl_local < groc->groc_con->con_local_n);

      src = groc->groc_local + vdecl->vdecl_local;
      cl_assert(cl, src != NULL);
      if (res->val_type != GRAPHD_VALUE_UNSPECIFIED) {
        cl_log(cl, CL_LEVEL_VERBOSE, "groc_sample: result not empty");
        return 0;
      }
      if (src->val_type == GRAPHD_VALUE_UNSPECIFIED) {
        cl_log(cl, CL_LEVEL_VERBOSE, "groc_sample: source unspecified");
        return 0;
      }
      cl_log(cl, CL_LEVEL_VERBOSE, "groc_sample: copy variable from %s",
             graphd_value_to_string(src, buf, sizeof buf));
      break;

    case GRAPHD_PATTERN_PICK:

      if (groc->groc_parent != NULL)
        rom = &groc->groc_parent->grsc_rom;
      else
        cl_log(cl, CL_LEVEL_VERBOSE,
               "groc_sample: no parent, hence no grsc_rom");

      /*  Pick the first from the list <pat> whose
       *  containing "or" branch is active.
       */
      for (sub_pat = pat->pat_list_head; sub_pat != NULL;
           sub_pat = sub_pat->pat_next)

        if (graphd_read_or_check(greq, sub_pat->pat_or_index, rom)) {
          cl_log(cl, CL_LEVEL_VERBOSE,
                 "groc_sample: picking "
                 "or-branch #%zu",
                 sub_pat->pat_or_index);
          break;
        }

      if (sub_pat == NULL) {
        cl_log(cl, CL_LEVEL_VERBOSE,
               "groc_sample: found no active "
               "or-branch; return NULL");
        graphd_value_null_set(res);
        return 0;
      }
      pat = sub_pat;
      goto again;

    default:
      cl_cover(cl);

      if (pat->pat_or_index != 0 &&
          (groc->groc_parent == NULL ||
           !graphd_read_or_check(greq, pat->pat_or_index,
                                 &groc->groc_parent->grsc_rom))) {
        cl_log(cl, CL_LEVEL_VERBOSE,
               "groc_sample: parent %p/read-or %zu fails?",
               (void *)groc->groc_parent, (size_t)pat->pat_or_index);
        return 0;
      }

      err = graphd_pattern_from_primitive(greq, pat, pr, groc->groc_con, res);
      if (err != 0) {
        char buf[200];

        if (err == GRAPHD_ERR_NO) {
          cl_log(cl, CL_LEVEL_VERBOSE,
                 "groc_sample: graphd_pattern_"
                 "from_primitive fails.");
          return 0;
        }
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_pattern_from_primitive", err,
                     "primitive %s",
                     pdb_primitive_to_string(pr, buf, sizeof buf));
      }
      return err;
  }

  /*  If we arrive here, we'd like to append an existing value
   *  to the result set.
   */
  cl_assert(cl, src != NULL);
  err = graphd_value_copy(g, cm, cl, res, src);
  if (err != 0)
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_value_copy", err,
                 "can't copy value %s",
                 graphd_value_to_string(src, buf, sizeof buf));
  return err;
}

/*  <pat> is the per-primitive pattern, always a list.
 *  <res> is a list that will hold the corresponding value.
 *  Move the correct pieces of primitive data into the slots,
 *  as directed by <pat>.
 */
static int groc_fill_pattern(graphd_read_one_context *groc,
                             graphd_pattern const *pat, graphd_value *res) {
  cl_handle *const cl = graphd_request_cl(groc->groc_base->grb_greq);
  graphd_value *val;
  int err;
  char buf[200];

  cl_log(cl, CL_LEVEL_VERBOSE, "groc_fill_pattern(%s)",
         graphd_pattern_dump(pat, buf, sizeof buf));

  cl_assert(cl, pat != NULL);
  cl_assert(cl, pat->pat_type == GRAPHD_PATTERN_LIST);
  cl_assert(cl, res->val_type == GRAPHD_VALUE_LIST);
  cl_assert(cl, res->val_list_n == pat->pat_list_n);

  pat = pat->pat_list_head;

  for (val = res->val_list_contents; pat != NULL; pat = pat->pat_next, val++) {
    if (pat->pat_type == GRAPHD_PATTERN_LIST) continue;

    err = groc_sample(groc, pat, val);
    if (err != 0) return err;
  }
  return 0;
}

#if 0
static int groc_promote_or_branches(
	graphd_request		* greq,
	graphd_read_one_context	* groc,
	graphd_constraint 	* con)
{
	graphd_constraint_or	* cor;
	graphd_constraint	* proto;
	int			  err;

	/*  Promote results of accepted "or" branches
	 *  to their prototypes.
	 */
	for (cor = con->con_or_head; cor != NULL; cor = cor->cor_next)
	{
		graphd_constraint * accepted;

		accepted = (  graphd_read_or_state(
				greq,
				&cor->or_head,
				&grsc->grsc_rom)
			   == GRAPHD_READ_OR_FALSE)
			   ? cor->or_tail
			   : &cor->or_head;

		err = groc_promote_or_branches(greq, groc, accepted);
		if (err != 0)
			return err;
	}

	if (con->con_or == NULL)
		return 0;

	/*  Proto is not necessarily groc->groc_con - we may be
	 *  more deeply nested, in which case groc->groc_con is the
	 *  ur-prototype, and proto is my direct containing ancestor.
	 */
	proto = con->con_or->or_prototype;

	/*  The numbers in a->a_parent_index are relative to the
	 *  ur-prototype.
	 */
	for (a = con->con_assignment_head;
	     a != *con->con_assignment_tail;
	     a = a->a_next)
	{
		graphd_result	* from, * to;

		from = groc->groc_result + i;
		to   = groc->groc_result + j;
	}
}
#endif

static int groc_fill_results(graphd_read_one_context *const groc) {
  graphd_constraint *con = groc->groc_con;
  int err;
  size_t i;
  graphd_pattern_frame const *pf = con->con_pframe;
  graphd_value *res = groc->groc_result;
  graphd_request *const greq = groc->groc_base->grb_greq;
  cl_handle *const cl = graphd_request_cl(greq);

  cl_enter(cl, CL_LEVEL_VERBOSE, "%s", graphd_constraint_to_string(con));

  if ((err = groc_cache_pr(groc)) != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "error from groc_cache_pr: %s",
             graphd_strerror(err));
    return err;
  }

  /*  Null out those contents whose subclauses did not
   *  match in our "or".
   */
  if (groc->groc_contents.val_type == GRAPHD_VALUE_SEQUENCE &&
      groc->groc_parent != NULL) {
    size_t i;
    graphd_read_or_map const *rom;
    graphd_constraint *sub;

    rom = &groc->groc_parent->grsc_rom;

    for (i = 0, sub = groc->groc_con->con_head;
         i < groc->groc_contents.val_sequence_n; i++, sub = sub->con_next) {
      cl_assert(cl, sub != NULL);

      if (!graphd_read_or_check(greq, sub->con_parent->con_or_index, rom)) {
        graphd_value_finish(cl, groc->groc_contents.val_sequence_contents + i);
        graphd_value_null_set(groc->groc_contents.val_sequence_contents + i);
      }
    }
  }

  /*  Complete branches.
   */
  for (i = 0; i < con->con_pframe_n; i++, pf++, res++) {
    char buf1[200], buf2[200];

    if (pf->pf_one == NULL) continue;

    cl_log(cl, CL_LEVEL_VERBOSE,
           "con %p [pframe %zu]: "
           "set %s, one %s",
           (void *)con, i, graphd_pattern_dump(pf->pf_set, buf1, sizeof buf1),
           graphd_pattern_dump(pf->pf_one, buf2, sizeof buf2));

    if (pf->pf_one->pat_type != GRAPHD_PATTERN_LIST)
      cl_notreached(cl, "unexpected pf_one->pat_type %d (pf=%p)",
                    (int)pf->pf_one->pat_type, (void *)pf);

    if (res->val_type != GRAPHD_VALUE_LIST)
      cl_notreached(cl, "unexpected res->val_type %d (res=%p)",
                    (int)res->val_type, (void *)res);

    if ((err = groc_fill_pattern(groc, pf->pf_one, res)) != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "error from groc_fill_pattern: %s",
               graphd_strerror(err));
      return err;
    }
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "%zu results", con->con_pframe_n);
  return 0;
}

/*  Advance groc_sub to the next subconstraint worth matching.
 *  If it's NULL after a call, we're done.
 *
 *  @return true if there's a subconstraint left to match, false if not.
 */
static bool groc_next_subconstraint(graphd_read_one_context *groc) {
  graphd_request *greq = groc->groc_base->grb_greq;

  /*  "or" processing:
   *
   *   Sequences of alternatives exist for this constraints.
   *  (The branches of an or.)
   *
   *   These alternatives may or may not contain subconstraints.
   *   The subconstraints occur in order of alternative.
   *
   *   If an "or" does *not* have subconstraints at this point,
   *   and hasn't failed the intrinsics match, it is true
   *   (nothing else can go wrong), and its later alternatives
   *   are never evaluated.
   */

  do {
    if (groc->groc_sub == NULL) {
      groc->groc_sub_i = 0;
      groc->groc_sub = groc->groc_con->con_head;
    } else {
      groc->groc_sub_i++;
      groc->groc_sub = groc->groc_sub->con_next;
    }

    /*  Some of the subconstraints we're matching here
     *  may only be active in a particular "OR" branch.
     *  If sub's containing "OR" branch is not in the
     *  running anymore, skip matching "sub".
     */
  } while (groc->groc_sub != NULL &&
           graphd_read_or_state(greq, groc->groc_sub->con_parent,
                                &groc->groc_parent->grsc_rom) ==
               GRAPHD_READ_OR_FALSE);

  return groc->groc_sub != NULL;
}

/*  Get results back from a subconstraint evaluation.
 *
 *  res points to the beginning of an array of con->con_pframe_n
 *  values; each value res[i] corresponds to the con->con_pframe[i]
 *  result.
 */
static void groc_set_deliver(void *callback_data, int err,
                             graphd_constraint const *sub, graphd_value *res) {
  graphd_assignment const *a;
  graphd_read_one_context *groc = callback_data;
  graphd_request *greq = groc->groc_base->grb_greq;
  graphd_constraint_or *cor;
  cl_handle *const cl = graphd_request_cl(greq);
  size_t i;

  cl_assert(cl, sub != NULL);
  cl_enter(cl, CL_LEVEL_VERBOSE, "sub=%s",
           graphd_constraint_to_string((graphd_constraint *)sub));

  /*  GRAPHD_ERR_NO - the subevaluation failed?
   */
  if (err == GRAPHD_ERR_NO) {
    /*  If the subconstraint that failed was the head branch
     *  of its OR, and the tail branch is still active,
     *  keep going.
     */
    graphd_read_or_fail(greq, sub->con_parent, &groc->groc_parent->grsc_rom);

    /*  Is there anything else to try?
     */
    if (groc_next_subconstraint(groc)) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "leave");
      return;
    }

    /*  Did all branches of the "or" fail, including
     *  the top branch?
     */
    if (!graphd_read_or_check(greq, 0, &groc->groc_parent->grsc_rom)) {
      /*  Yeah.
       */
      groc->groc_err = err;
      cl_assert(cl, groc->groc_sub == NULL);
      cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
      return;
    }

    /*  No, there is one branch - one without
     *  subconstraints - that succeeded and is still
     *  valid.
     */
    groc->groc_err = 0;
    cl_leave(cl, CL_LEVEL_VERBOSE,
             "no match for this or-branch, but ok in general.");
    return;
  }

  /* Some other unexpected error.
   */
  if (err != 0) {
    groc->groc_err = err;
    groc->groc_sub = NULL;

    cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
    return;
  }

  /*  Move variable values into their groc->groc_local[] slots.
   */
  a = sub->con_assignment_head;
  for (i = 0; i < sub->con_assignment_n; i++, a = a->a_next) {
    graphd_value *local;
    char const *name_s, *name_e;
    char buf[200];

    if (a->a_declaration->vdecl_constraint != groc->groc_con) {
      graphd_variable_declaration_name(a->a_declaration, &name_s, &name_e);
      cl_log(cl, CL_LEVEL_VERBOSE,
             "groc_set_deliver: ignore assignment [%zu] "
             "%.*s = %s, because "
             "it's not intended for this constraint",
             i, (int)(name_e - name_s), name_s,
             graphd_pattern_to_string(a->a_result, buf, sizeof buf));
      continue;
    }

    local = groc->groc_local + a->a_declaration->vdecl_local;

    /*  If the slot is already occupied, or there is no
     *  value to assign, don't assign.
     */
    if (local->val_type != GRAPHD_VALUE_UNSPECIFIED ||
        res[i].val_type == GRAPHD_VALUE_UNSPECIFIED) {
      graphd_variable_declaration_name(a->a_declaration, &name_s, &name_e);

      cl_log(cl, CL_LEVEL_VERBOSE,
             "groc_set_deliver: ignore assignment [%zu] "
             "%.*s = %s, because %s",
             i, (int)(name_e - name_s), name_s,
             graphd_pattern_to_string(a->a_result, buf, sizeof buf),
             local->val_type != GRAPHD_VALUE_UNSPECIFIED
                 ? "we already have a result"
                 : "there is no incoming result value");
      continue;
    }
    *local = res[i];
    graphd_value_initialize(res + i);

    {
      char buf2[200];
      graphd_variable_declaration_name(a->a_declaration, &name_s, &name_e);

      cl_log(cl, CL_LEVEL_VERBOSE,
             "groc_set_deliver: local[%zu] := res[%zu]: "
             "%.*s = %s = %s",
             i, a->a_declaration->vdecl_local, (int)(name_e - name_s), name_s,
             graphd_pattern_to_string(a->a_result, buf, sizeof buf),
             graphd_value_to_string(local, buf2, sizeof buf2));
    }
  }

  /*  Move the result set for the subframe into its slot
   *  in our content sequence.
   */
  if (groc->groc_contents.val_type == GRAPHD_VALUE_SEQUENCE &&
      i < sub->con_pframe_n && res[i].val_type != GRAPHD_VALUE_UNSPECIFIED) {
    graphd_value *seq = groc->groc_contents.val_sequence_contents;

    cl_assert(cl, seq != NULL);
    cl_assert(cl, groc->groc_sub_i < groc->groc_contents.val_sequence_n);

    if (seq[groc->groc_sub_i].val_type != GRAPHD_VALUE_UNSPECIFIED) {
      char buf[200];
      cl_notreached(
          cl,
          "subconstraint contents "
          "#%zu of %zu: %s already filled?",
          (size_t)groc->groc_sub_i, (size_t)groc->groc_contents.val_sequence_n,
          graphd_value_to_string(seq + groc->groc_sub_i, buf, sizeof buf));
    }

    seq[groc->groc_sub_i] = res[i];
    graphd_value_initialize(res + i);
  } else {
    char buf[200];
    cl_log(
        cl, CL_LEVEL_VERBOSE, "groc_set_deliver: ignoring result %s because %s",
        i < sub->con_pframe_n ? graphd_value_to_string(res + i, buf, sizeof buf)
                              : "*nonexistant*",
        groc->groc_contents.val_type != GRAPHD_VALUE_SEQUENCE
            ? "there is no result sequence"
            : (i < sub->con_pframe_n ? "there is no result"
                                     : "its value is unspecified"));
  }

  /*  If this subconstraint was the last subconstraint of the head
   *  of an "or" branch, mark it as true, its tail as false.
   *
   *  That way, we won't unnecessarily descend into
   *  its sibling branch.
   */
  if (sub->con_parent != NULL && (cor = sub->con_parent->con_or) != NULL) {
    while (cor != NULL) {
      if (cor->or_head.con_tail == &sub->con_next ||
          cor->or_head.con_tail == &cor->or_head.con_head)

        graphd_read_or_match_subconstraints(greq, &cor->or_head,
                                            &groc->groc_parent->grsc_rom);

      /*  If the tail has its own subconstraints
       *  that we haven't seen yet, break out of
       *  the marking loop.  All containing branches
       *  will contain those tail constraints, too.
       */
      if (cor->or_tail != NULL && cor->or_tail->con_tail != &sub->con_next &&
          cor->or_tail->con_tail != &cor->or_tail->con_head)
        break;

      if (cor->or_tail != NULL)
        graphd_read_or_match_subconstraints(greq, cor->or_tail,
                                            &groc->groc_parent->grsc_rom);

      cor = cor->or_prototype->con_or;
    }
  }

  /*  Advance to the next subconstraint.
   */
  (void)groc_next_subconstraint(groc);

  /*  groc_stack_run() will pick up here once the subconstraint
   *  frame method has finished running, and either terminate
   *  or push the next subconstraint evaluation.
   */
  cl_leave(cl, CL_LEVEL_VERBOSE, "leave");
}

/**
 * @brief Deliver the final results of evaluating the subconstraints.
 */
static void groc_deliver_results(graphd_read_one_context *groc) {
  graphd_request *greq = groc->groc_base->grb_greq;
  cl_handle *cl = graphd_request_cl(greq);

  if (groc->groc_err == 0) {
    groc->groc_err = groc_fill_results(groc);
    {
      size_t i;
      char b1[200], b2[200];
      graphd_constraint *con = groc->groc_con;

      for (i = 0; i < con->con_pframe_n; i++) {
        if (con->con_pframe[i].pf_one == NULL) continue;

        cl_log(cl, CL_LEVEL_VERBOSE, "[%zu] %s := %s", i,
               graphd_pattern_dump(con->con_pframe[i].pf_one, b1, sizeof b1),
               graphd_value_to_string(groc->groc_result + i, b2, sizeof b2));
      }
    }
  }

  (*groc->groc_callback)(groc->groc_callback_data, groc->groc_err,
                         groc->groc_pc.pc_id, groc->groc_con,
                         groc->groc_err ? NULL : groc->groc_result);
}

/**
 * @brief Deliver the final results of evaluating the subconstraints.
 */
static void groc_deliver(graphd_read_one_context *groc) {
  graphd_request *greq = groc->groc_base->grb_greq;
  cl_handle *cl = graphd_request_cl(greq);

  cl_enter(cl, CL_LEVEL_VERBOSE, "groc=%p", (void *)groc);

  groc_deliver_results(groc);

  /*  Using remove rather than pop because the callback
   *  may have pushed new processing on the stack - the
   *  thing we want to destroy isn't necessarily on top
   *  of the stack.
   *
   *  We can't pop first because popping will call the
   *  destructor, and the destructor will free the results
   *  we're trying to return!
   */
  graphd_stack_remove(&greq->greq_stack, &groc->groc_sc);
  cl_leave(cl, CL_LEVEL_VERBOSE, "leave");
}

/**
 * @brief Push another subconstraint on the stack for evaluation.
 */
static int groc_stack_run(graphd_stack *stack,
                          graphd_stack_context *stack_context) {
  graphd_read_one_context *groc = (void *)stack_context;
  cl_handle *cl = graphd_request_cl(groc->groc_base->grb_greq);

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");

  /*  There was an error, or we ran out of subconstraints?
   */
  if (groc->groc_err != 0 || groc->groc_sub == NULL) {
    groc_deliver(groc);
    cl_leave(cl, CL_LEVEL_VERBOSE, "(delivered)");

    return 0;
  }

  /*  Generate the results for the subconstraint, given
   *  our constraint.
   */
  graphd_read_set_push(groc->groc_base, groc->groc_sub, groc->groc_pc.pc_id,
                       groc->groc_pc.pc_pr_valid ? &groc->groc_pc.pc_pr : NULL,
                       groc_set_deliver, groc);

  cl_leave(cl, CL_LEVEL_VERBOSE, "(pushed)");
  return 0;
}

/**
 * @brief Preshape a result.
 *
 *  Given a result instruction subtree, allocate a value
 *  of the right shape in the result.
 *
 * @param groc	graphd read constraint context
 * @param con	constraint whose patterns we're modeling.
 * @param res	result structure to initialize
 *
 * @return 0 on success, a nonzero error code on system error.
 */
static int groc_shape_results(graphd_read_one_context *groc) {
  graphd_request *greq = groc->groc_base->grb_greq;
  cl_handle *const cl = graphd_request_cl(greq);
  graphd_handle *const g = graphd_request_graphd(greq);
  graphd_constraint *con = groc->groc_con;
  graphd_value *res = groc->groc_result;
  graphd_pattern_frame *pf = con->con_pframe;
  size_t i;
  int err;

  /*  For all result patterns ..
   */
  for (i = 0; i < con->con_pframe_n; i++, res++, pf++) {
    char b1[200], b2[200];
    cl_log(cl, CL_LEVEL_VERBOSE, "groc_shape_results: set %s, one %s",
           graphd_pattern_dump(pf->pf_set, b1, sizeof b1),
           graphd_pattern_dump(pf->pf_one, b2, sizeof b2));

    /*  If the returned per-element value is a list ...
     */
    if (pf->pf_one != NULL && pf->pf_one->pat_type == GRAPHD_PATTERN_LIST) {
      /*  Make a list with that many elements.
       */
      err = graphd_value_list_alloc(g, greq->greq_req.req_cm, cl, res,
                                    pf->pf_one->pat_list_n);
      if (err != 0) goto err;
    }
  }

  /*  Allocate the sequence that our "contents" will live in.
   */
  cl_assert(cl, groc->groc_contents.val_type == GRAPHD_VALUE_UNSPECIFIED);
  if (con->con_uses_contents) {
    err = graphd_value_list_alloc(g, greq->greq_req.req_cm, cl,
                                  &groc->groc_contents, con->con_subcon_n);
    if (err != 0) goto err;

    /* We need a sequence, not a list. */
    groc->groc_contents.val_type = GRAPHD_VALUE_SEQUENCE;
  }
  return 0;

err:
  for (;; i--) {
    graphd_value_finish(cl, res + i);
    if (i == 0) break;
  }
  return err;
}

/*  Verify and read the result frames for a single GUID.
 *
 *  As far as intrinsic constraints go, the ID has been accepted
 *  and is part of the result set.  Its relation to its parent
 *  has been checked.  Subconstraints may still fail.
 *
 *  Result frames are:
 *	the results ("contents" element/sample of the container)
 *      variables assigned in the body (including implicit
 *		assignments generated by the calling code.)
 *
 *  Result frames are delivered to the callback once we have them.
 */
void graphd_read_one_push(graphd_read_base *grb, graphd_read_set_context *grsc,
                          pdb_id id, pdb_primitive *pr, graphd_constraint *con,
                          graphd_read_one_callback *callback,
                          void *callback_data) {
  graphd_request *greq = grb->grb_greq;
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;

  graphd_read_one_context *groc = NULL;
  int err = 0;

  cl_enter(cl, CL_LEVEL_SPEW, "%llx: %s; pframe_n %zu, local_n %zu",
           (unsigned long long)id, graphd_constraint_to_string(con),
           con->con_pframe_n, con->con_local_n);

  cl_assert(cl, con != NULL);
  cl_assert(cl, grb != NULL);
  cl_assert(cl, id != PDB_ID_NONE);

  /*  Allocate space for
   *
   *	(a) our execution frame
   *	(b) the values we'll return to the caller
   *	(c) values returned by subconstraint evaluations
   * 	    that we may use in evaluating our own result set.
   */
  groc =
      cm_zalloc(cm, sizeof(*groc) + sizeof(graphd_value) * con->con_pframe_n +
                        sizeof(graphd_value) * con->con_local_n);
  if (groc == NULL) {
    err = errno ? errno : ENOMEM;
    goto err;
  }
  groc->groc_base = grb;
  groc->groc_parent = grsc;
  groc->groc_pc.pc_id = id;
  groc->groc_con = con;
  groc->groc_result = (graphd_value *)(groc + 1);
  groc->groc_local = (graphd_value *)(groc + 1) + con->con_pframe_n;
  groc->groc_callback = callback;
  groc->groc_callback_data = callback_data;
  groc->groc_link = 1;

  if (grsc != NULL) graphd_read_set_context_link(grsc);

  if (pr != NULL) {
    /*  Move the primitive from the caller
     *  into our cache.
     */
    pdb_primitive_guid_get(pr, groc->groc_pc.pc_guid);
    groc->groc_pc.pc_pr = *pr;
    groc->groc_pc.pc_pr_valid = true;

    pdb_primitive_initialize(pr);
  } else {
    groc->groc_pc.pc_pr_valid = false;
    pdb_primitive_initialize(&groc->groc_pc.pc_pr);
    err = pdb_id_to_guid(graphd_request_graphd(greq)->g_pdb, id,
                         &groc->groc_pc.pc_guid);
    if (err != 0) goto err;
  }

  /*  Pre-shape the results.  For example, if we're supposed
   *  to return a list of values as $a, allocate that list.
   */
  if ((err = groc_shape_results(groc)) != 0) goto err;

  /*  Match the subconstraints within their first layer.
   *  This may (a) fail (b) return deferred evaluation frames.
   */
  groc->groc_sub = NULL;
  if (groc_next_subconstraint(groc)) {
    graphd_stack_push(&greq->greq_stack, (graphd_stack_context *)groc,
                      &groc_resource_type, &groc_stack_type);
  } else {
    /*  If we have no subconstraint, we're done now.
     */
    cl_assert(cl, groc->groc_sub == NULL);

    groc_deliver_results(groc);
    graphd_read_one_context_free(groc);

    cl_leave(cl, CL_LEVEL_VERBOSE, "(no subconstraints)");
    return;
  }
  cl_leave(cl, CL_LEVEL_SPEW, "-> groc_stack_run");
  return;

err:
  /*  Deliver the error to the caller.
   */
  (*callback)(callback_data, err, id, con, NULL);

  /*  Free anything we allocated.
   */
  if (groc != NULL) graphd_read_one_context_free(groc);

  cl_leave(cl, CL_LEVEL_SPEW, "error: %s", graphd_strerror(err));
}
