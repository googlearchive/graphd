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

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>

/* graphd-iterate.c -- a variant of graphd-read that doesn't
 *  	actually read anything, but instead runs a set of
 * 	trials on the cursor generated for a constraint.
 */
static int iterate_failed_find(graphd_request *greq, pdb_iterator *it,
                               pdb_id id, int err_expected, char const *file,
                               int line) {
  int err;
  graphd_handle *graphd = graphd_request_graphd(greq);
  pdb_handle *pdb = graphd->g_pdb;
  pdb_id id_tmp;
  char ibuf[42], buf[200];

  id_tmp = id;
  err = pdb_iterator_find_nonstep(pdb, it, id, &id_tmp);
  if (err == err_expected) return 0;

  if (err == 0) {
    graphd_request_errprintf(
        greq, false,
        "SYSTEM TESTFAIL [%s:%d] FIND(%s, %llx): expected "
        "error \"%s\", got %s (%s)",
        file, line, pdb_iterator_to_string(pdb, it, buf, sizeof buf),
        (unsigned long long)id, graphd_strerror(err_expected),
        pdb_id_to_string(pdb, id_tmp, ibuf, sizeof ibuf),
        id != id_tmp ? "changed" : "unchanged");
    return GRAPHD_ERR_NO;
  }
  graphd_request_errprintf(greq, false,
                           "SYSTEM TESTFAIL [%s:%d] FIND(%s, %llx): expected "
                           "GRAPHD_ERR_NO, got error: %s",
                           file, line,
                           pdb_iterator_to_string(pdb, it, buf, sizeof buf),
                           (unsigned long long)id, graphd_strerror(err));
  return err;
}

static int iterate_successful_find(graphd_request *greq, pdb_iterator *it,
                                   pdb_id id_in, pdb_id id_out,
                                   char const *file, int line) {
  int err;
  graphd_handle *graphd = graphd_request_graphd(greq);
  pdb_handle *pdb = graphd->g_pdb;
  pdb_id id_tmp;
  char ibuf[42], jbuf[42], buf[200];

  id_tmp = id_in;
  err = pdb_iterator_find_nonstep(pdb, it, id_in, &id_tmp);
  if (err != 0) {
    graphd_request_errprintf(
        greq, false,
        "SYSTEM TESTFAIL [%s:%d] FIND(%s, %llx): expected "
        "%s, got error: %s",
        file, line, pdb_iterator_to_string(pdb, it, buf, sizeof buf),
        (unsigned long long)id_in,
        pdb_id_to_string(pdb, id_out, ibuf, sizeof ibuf), graphd_strerror(err));
    return err;
  }
  if (id_tmp != id_out) {
    graphd_request_errprintf(greq, false,
                             "SYSTEM TESTFAIL [%s:%d] NEXT(%s, %llx): expected "
                             "%s, got %s",
                             file, line,
                             pdb_iterator_to_string(pdb, it, buf, sizeof buf),
                             (unsigned long long)id_in,
                             pdb_id_to_string(pdb, id_out, ibuf, sizeof ibuf),
                             pdb_id_to_string(pdb, id_tmp, jbuf, sizeof jbuf));
    return GRAPHD_ERR_NO;
  }
  return 0;
}

static int iterate_failed_next(graphd_request *greq, pdb_iterator *it,
                               int err_expected, char const *file, int line) {
  int err;
  graphd_handle *graphd = graphd_request_graphd(greq);
  pdb_handle *pdb = graphd->g_pdb;
  pdb_id id_tmp;
  char ibuf[42], buf[200];

  err = pdb_iterator_next_nonstep(pdb, it, &id_tmp);
  if (err == err_expected) return 0;

  if (err == 0) {
    graphd_request_errprintf(greq, false,
                             "SYSTEM TESTFAIL [%s:%d] NEXT(%s): expected "
                             "GRAPHD_ERR_NO, got %s",
                             file, line,
                             pdb_iterator_to_string(pdb, it, buf, sizeof buf),
                             pdb_id_to_string(pdb, id_tmp, ibuf, sizeof ibuf));
    return GRAPHD_ERR_NO;
  }
  graphd_request_errprintf(greq, false,
                           "SYSTEM TESTFAIL [%s:%d] NEXT(%s): expected "
                           "GRAPHD_ERR_NO, got error: %s",
                           file, line,
                           pdb_iterator_to_string(pdb, it, buf, sizeof buf),
                           graphd_strerror(err));
  return 0;
}

static int iterate_successful_next(graphd_request *greq, pdb_iterator *it,
                                   pdb_id id, char const *file, int line) {
  int err;
  graphd_handle *graphd = graphd_request_graphd(greq);
  pdb_handle *pdb = graphd->g_pdb;
  pdb_id id_tmp;
  char ibuf[42], jbuf[42], buf[200];

  err = pdb_iterator_next_nonstep(pdb, it, &id_tmp);
  if (err != 0) {
    graphd_request_errprintf(
        greq, false,
        "SYSTEM TESTFAIL [%s:%d] NEXT(%s): expected "
        "%s, got error: %s",
        file, line, pdb_iterator_to_string(pdb, it, buf, sizeof buf),
        pdb_id_to_string(pdb, id, ibuf, sizeof ibuf), graphd_strerror(err));
    return err;
  }
  if (id_tmp != id) {
    graphd_request_errprintf(greq, false,
                             "SYSTEM TESTFAIL [%s:%d] NEXT(%s): expected "
                             "%s, got %s",
                             file, line,
                             pdb_iterator_to_string(pdb, it, buf, sizeof buf),
                             pdb_id_to_string(pdb, id, ibuf, sizeof ibuf),
                             pdb_id_to_string(pdb, id_tmp, jbuf, sizeof jbuf));
    return GRAPHD_ERR_NO;
  }
  return 0;
}

static int iterate_successful_reset(graphd_request *greq, pdb_iterator *it,
                                    char const *file, int line) {
  graphd_handle *graphd = graphd_request_graphd(greq);
  pdb_handle *pdb = graphd->g_pdb;
  int err;

  err = pdb_iterator_reset(pdb, it);
  if (err != 0) {
    char buf[200];
    graphd_request_errprintf(
        greq, false, "SYSTEM TESTFAIL [%s:%d] RESET(%s): error: %s", file, line,
        pdb_iterator_to_string(pdb, it, buf, sizeof buf), graphd_strerror(err));
  }
  return err;
}

static int iterate_successful_clone(graphd_request *greq, pdb_iterator *it,
                                    pdb_iterator **it_out, char const *file,
                                    int line) {
  graphd_handle *graphd = graphd_request_graphd(greq);
  pdb_handle *pdb = graphd->g_pdb;
  int err;

  err = pdb_iterator_clone(pdb, it, it_out);
  if (err != 0) {
    char buf[200];
    graphd_request_errprintf(
        greq, false, "SYSTEM TESTFAIL [%s:%d] CLONE(%s): error: %s", file, line,
        pdb_iterator_to_string(pdb, it, buf, sizeof buf), graphd_strerror(err));
  }
  return err;
}

/**
 * @brief Run some tests.
 *
 * @param stack		execution stack in the request
 * @param stack_context	current read constraint context
 *
 * @return 0 on successful, possibly partial, execution
 * @return other nonzero errors on system/resource error.
 */
static int iterate(graphd_stack *stack, graphd_stack_context *stack_context) {
  graphd_read_context *grc = (void *)stack_context;
  graphd_request *greq = grc->grc_base->grb_greq;
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  int err = 0;

  pdb_id *id = NULL, id_tmp;
  size_t id_n = 0;
  size_t id_m = 0;

  graphd_handle *graphd = graphd_request_graphd(greq);
  pdb_handle *pdb = graphd->g_pdb;
  pdb_iterator *it = grc->grc_it;
  pdb_iterator *it_clone = NULL;
  pdb_iterator *it_ptr = NULL;

  it = grc->grc_it;

  PDB_IS_ITERATOR(cl, it);

  id_m = /* 32 */ 1 * 1024;
  id = cm_malloc(cm, sizeof(*id) * id_m);
  if (id == NULL) {
    err = errno ? errno : ENOMEM;
    goto err;
  }

  /* Get the first 1000 entries. */

  for (id_n = 0; id_n < id_m; id_n++) {
    err = pdb_iterator_next_nonstep(pdb, it, id + id_n);
    if (err != 0) break;

    if (pdb_iterator_sorted(pdb, it)) {
      if (id_n > 0) {
        if (pdb_iterator_forward(pdb, it) ? id[id_n - 1] >= id[id_n]
                                          : id[id_n - 1] <= id[id_n]) {
          graphd_request_errprintf(greq, false,
                                   "SYSTEM NEXT: "
                                   "[%zu] %lld %s [%zu] %lld",
                                   id_n - 1, (long long)id[id_n - 1],
                                   pdb_iterator_forward(pdb, it) ? ">=" : "<=",
                                   id_n, (long long)id[id_n]);
          goto err;
        }
      }
    } else {
      size_t j;
      for (j = 0; j < id_n; j++)
        if (id[j] == id[id_n]) {
          graphd_request_errprintf(greq, false,
                                   "SYSTEM NEXT: "
                                   "[%zu] %llx == [%zu] %llx",
                                   j, (long long)id[j], id_n,
                                   (long long)id[id_n]);
          goto err;
        }
    }
  }

  if (err == GRAPHD_ERR_NO) err = 0;

  if (pdb_iterator_sorted(pdb, it)) {
    /*  on-or-after trials.
     */

    id_tmp = (pdb_iterator_forward(pdb, it) ? 0 : it->it_high);

    if (id_n == 0)
      err = iterate_failed_find(greq, it, 0, GRAPHD_ERR_NO, __FILE__, __LINE__);
    else
      err = iterate_successful_find(greq, it, 0, id[0], __FILE__, __LINE__);
    if (err) goto err;

    if (id_n > 0) {
      /*  Position on the last we know about.
       */
      err = iterate_successful_find(greq, it, id[id_n - 1], id[id_n - 1],
                                    __FILE__, __LINE__);
      if (err) goto err;

      if (id_n < id_m) {
        err = iterate_failed_next(greq, it, GRAPHD_ERR_NO, __FILE__, __LINE__);
        if (err) goto err;
      }

      /*  Position on the first and last we know about.
       */
      err = iterate_successful_find(greq, it, id[0], id[0], __FILE__, __LINE__);
      if (err) goto err;
    }

    /*  Position too far out, fail;
     *  then do a next that fails.
     */
    if (pdb_iterator_forward(pdb, it)) {
      id_tmp = it->it_high;
      if (id_tmp == PDB_ITERATOR_HIGH_ANY) id_tmp = pdb_primitive_n(pdb);
    } else {
      id_tmp = it->it_low;
      if (id_tmp > 0)
        id_tmp--;
      else
        id_tmp = PDB_ID_NONE;
    }

    if (id_tmp != PDB_ID_NONE) {
      err = iterate_failed_find(greq, it, id_tmp, GRAPHD_ERR_NO, __FILE__,
                                __LINE__);
      if (err) goto err;

      err = iterate_failed_next(greq, it, GRAPHD_ERR_NO, __FILE__, __LINE__);
      if (err) goto err;
    }
  }

  /*  Reset, get out the same first ids as before.
   */
  err = iterate_successful_reset(greq, it, __FILE__, __LINE__);
  if (err) goto err;

  /*  Next.
   */
  if (id_n == 0) goto first_skip;

  /*  ------------------------------------------------------------
   *                        ID #2 REFREEZE TRIAL
   *  ------------------------------------------------------------
   */

  err = iterate_successful_next(greq, it, id[0], __FILE__, __LINE__);
  if (err) goto err;

  /*  Freeze, then thaw.
   */

  err = graphd_iterator_hard_clone(greq, it, &it_clone);
  if (err != 0) {
    char buf[200];
    graphd_request_errprintf(
        greq, false, "SYSTEM TESTFAIL [%s:%d]: CLONE(%s): %s", __FILE__,
        __LINE__, pdb_iterator_to_string(pdb, it, buf, sizeof buf),
        graphd_strerror(err));
    goto err;
  }

  /*  Next after the thaw - should be the second ID, if
   *  there is one.
   */
  if (id_n > 1)
    err = iterate_successful_next(greq, it_clone, id[1], __FILE__, __LINE__);
  else
    err =
        iterate_failed_next(greq, it_clone, GRAPHD_ERR_NO, __FILE__, __LINE__);

  pdb_iterator_destroy(pdb, &it_clone);

first_skip:;

  /*  ------------------------------------------------------------
   *                     END: JUST AFTER THE LAST ID.
   *  ------------------------------------------------------------
   */
  if (id_n == 0) goto end_skip;

  err = pdb_iterator_clone(pdb, it, &it_ptr);
  if (err != 0) {
    graphd_request_errprintf(greq, false, "SYSTEM CLONE(END) error: %s",
                             graphd_strerror(err));
    goto err;
  }

  if (pdb_iterator_sorted(pdb, it)) {
    id_tmp = id[id_n - 1];

    err = iterate_successful_find(greq, it_ptr, id_tmp, id_tmp, __FILE__,
                                  __LINE__);
    if (err) goto err;
  } else {
    pdb_iterator *it_ptr_dup = NULL;
    for (;;) {
      pdb_iterator_destroy(pdb, &it_ptr_dup);
      err = iterate_successful_clone(greq, it_ptr, &it_ptr_dup, __FILE__,
                                     __LINE__);
      if (err != 0) goto err;

      err = pdb_iterator_next_nonstep(pdb, it_ptr, &id_tmp);
      if (err == GRAPHD_ERR_NO) {
        err = 0;
        break;
      }
      if (err != 0) {
        char buf[200];
        graphd_request_errprintf(
            greq, false, "SYSTEM TESTFAIL [%s:%d] NEXT(%s) fails: %s", __FILE__,
            __LINE__, pdb_iterator_to_string(pdb, it_ptr, buf, sizeof buf),
            graphd_strerror(err));
        goto err;
      }
    }
    pdb_iterator_destroy(pdb, &it_ptr);
    it_ptr = it_ptr_dup;
  }

  err = graphd_iterator_hard_clone(greq, it_ptr, &it_clone);
  if (err != 0) {
    graphd_request_errprintf(greq, false, "SYSTEM CLONE(LAST): %s",
                             graphd_strerror(err));
    goto err;
  }

  /*  Next after the hard clone - should fail.
   */
  err = iterate_failed_next(greq, it_clone, GRAPHD_ERR_NO, __FILE__, __LINE__);
  if (err) goto err;
  pdb_iterator_destroy(pdb, &it_clone);

  err = iterate_failed_next(greq, it_ptr, GRAPHD_ERR_NO, __FILE__, __LINE__);
  if (err) goto err;

  err = iterate_successful_clone(greq, it_ptr, &it_clone, __FILE__, __LINE__);
  if (err) goto err;

  /*  Next after the hard clone - should again fail.
   */
  err = iterate_failed_next(greq, it_clone, GRAPHD_ERR_NO, __FILE__, __LINE__);
  if (err) goto err;

  err = iterate_successful_reset(greq, it_ptr, __FILE__, __LINE__);
  if (err) goto err;

  /*  Next after reset returns first.
   */
  err = iterate_successful_next(greq, it_ptr, id[0], __FILE__, __LINE__);
  if (err) goto err;

end_skip:;
err:
  pdb_iterator_destroy(pdb, &it_clone);
  pdb_iterator_destroy(pdb, &it_ptr);

  if (id != NULL) cm_free(cm, id);

  graphd_stack_pop(stack);
  return 0;
}

/**
 * @brief I: Compile statistics for this iterator context
 *
 *  Compiling statistics means that the iterator figures out
 *  internally how to actually get us its values.  Only after
 *  statistics have taken place do we know, e.g., whether the
 *  iterator is sorted, and in what direction (if any).
 *
 * @param stack		execution stack in the request
 * @param stack_context	current read constraint context
 *
 * @return 0 on successful, possibly partial, execution
 * @return other nonzero errors on system/resource error.
 */
static int iterate_constraint_statistics(graphd_stack *stack,
                                         graphd_stack_context *stack_context) {
  graphd_read_context *grc = (void *)stack_context;
  graphd_read_base *grb = grc->grc_base;
  graphd_request *greq = grb->grb_greq;
  cl_handle *cl = graphd_request_cl(greq);
  pdb_budget budget = 10000;
  int err;

  PDB_IS_ITERATOR(cl, grc->grc_it);

  err = pdb_iterator_statistics(graphd_request_graphd(greq)->g_pdb, grc->grc_it,
                                &budget);
  if (err == PDB_ERR_MORE)
    return 0;

  else if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_statistics", err,
                 "unexpected error");
    return err;
  }
  PDB_IS_ITERATOR(cl, grc->grc_it);
  graphd_stack_resume(stack, stack_context, iterate);
  return 0;
}

/**
 * @brief Entry point: read candidates that match the cursor.
 *
 *  Execution resumes with a call to graphd_read_constraint_finish()
 *  on the same stack_context; an error code will be left in grc_err.
 *
 * @param stack		execution stack in the request
 * @param stack_context	current read constraint context
 */
static void graphd_iterate_constraint_alternatives(
    graphd_stack *stack, graphd_stack_context *stack_context) {
  graphd_read_context *grc = (void *)stack_context;
  cl_handle *cl = graphd_request_cl(grc->grc_base->grb_greq);

  PDB_IS_ITERATOR(cl, grc->grc_it);
  graphd_stack_resume(stack, stack_context, iterate_constraint_statistics);
}

/**
 * @brief Read a constraint, part II
 *
 * @param stack
 * @param stack_context
 *
 * @return 0 on successful (partial) execution
 * @return nonzero error codes on unexpected system/resource errors
 */
static int iterate_constraint_resume(graphd_stack *stack,
                                     graphd_stack_context *stack_context) {
  graphd_read_context *grc = (void *)stack_context;
  graphd_request *const greq = grc->grc_base->grb_greq;
  graphd_value *out = grc->grc_contents_out;
  graphd_constraint *con = grc->grc_con;
  cl_handle *cl = graphd_request_cl(greq);

  cl_enter(cl, CL_LEVEL_SPEW, "enter");

  /*  We counted con_start too many entries that we
   *  didn't actually store.
   */
  if (con->con_start > grc->grc_count)
    grc->grc_count = 0;
  else
    grc->grc_count -= con->con_start;

  /* If we didn't find enough alternatives, fail.
   */
  if (grc->grc_err == 0 && grc->grc_count < con->con_count.countcon_min) {
    cl_log(cl, CL_LEVEL_SPEW,
           "iterate_constraint_resume: "
           "count %lu < atleast: %lu",
           (unsigned long)grc->grc_count,
           (unsigned long)con->con_count.countcon_min);
    grc->grc_err = GRAPHD_ERR_NO;
    cl_cover(cl);
  }

  /* If we found too many, fail too!
   */
  if (grc->grc_err == 0 && con->con_count.countcon_max_valid &&
      grc->grc_count > con->con_count.countcon_max_valid) {
    cl_log(cl, CL_LEVEL_SPEW,
           "iterate_constraint_resume: "
           "count %lu > atmost: %lu",
           (unsigned long)grc->grc_count,
           (unsigned long)con->con_count.countcon_max);
    grc->grc_err = GRAPHD_ERR_TOO_MANY_MATCHES;
    cl_cover(cl);
  }

  cl_cover(cl);

  if (grc->grc_err_out != NULL) *grc->grc_err_out = grc->grc_err;

  {
    char buf[200];
    cl_leave(cl, CL_LEVEL_SPEW, "done: %s",
             grc->grc_err ? graphd_strerror(grc->grc_err)
                          : graphd_value_to_string(out, buf, sizeof buf));
  }
  graphd_stack_pop(stack);

  return 0;
}

static int iterate_constraint_freeze(graphd_stack *stack,
                                     graphd_stack_context *stack_context) {
  return PDB_ERR_MORE;
}

static int iterate_constraint_thaw(graphd_stack *stack,
                                   graphd_stack_context *stack_context) {
  return GRAPHD_ERR_SYNTAX;
}

static graphd_stack_type iterate_constraint_type = {iterate_constraint_resume,
                                                    iterate_constraint_freeze,
                                                    iterate_constraint_thaw};

/**
 * @brief Constraint-iterate context resource method: free.
 *
 *  Free all resources associated with a single-constraint
 *  iterate context.
 *
 * @param resource_manager_data	opaque application handle for all
 *		resources in this manager, ignored
 * @param resoure_data	the graphd_stack, cast to void *
 */

static void iterate_constraint_context_resource_free(
    void *resource_manager_data, void *resource_data) {
  graphd_read_context *const grc = resource_data;
  graphd_request *const greq = grc->grc_base->grb_greq;
  graphd_handle *const g = graphd_request_graphd(greq);
  cm_handle *const cm = greq->greq_req.req_cm;

  pdb_iterator_destroy(g->g_pdb, &grc->grc_it);
  cm_free(cm, grc);
}

/**
 * @brief Annotate context resource method: list.
 *
 * @param log_data	a cl_handle, cast to void *
 * @param resource_manager_data	opaque application handle for all
 *		resources in this manager, ignored
 * @param resoure_data	the graphd_stack, cast to void *
 */
static void iterate_constraint_context_resource_list(
    void *log_data, void *resource_manager_data, void *resource_data) {
  cl_handle *cl = log_data;
  graphd_read_context *grc = resource_data;

  cl_log(cl, CL_LEVEL_VERBOSE, "iterate constraint context (%s)",
         graphd_constraint_to_string(grc->grc_con));
}

/**
 * @brief Constraint iterate context resource type
 */
static cm_resource_type iterate_constraint_context_resource_type = {
    "constraint iterate context", iterate_constraint_context_resource_free,
    iterate_constraint_context_resource_list};

/**
 * @brief Push a context on the request stack.
 *	 The context will iterate a constraint subtree.
 *
 *  This is the workhorse used by graphd-iterate.c to recursively
 *  match one constraint of a tree.
 *
 * @param greq		Request whose stack we're pushing on
 * @param con		Constraint caller wants to match
 * @param grb		Context for the whole iterate request
 * @param parent	Parent context
 * @param contents_out	NULL or where to store the result.
 * @param err_out 	Return errors here.
 */
void graphd_iterate_constraint_push(graphd_request *greq,
                                    graphd_constraint *con,
                                    graphd_read_base *grb,
                                    graphd_value *contents_out, int *err_out) {
  graphd_read_context *grc = NULL;
  graphd_handle *g = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  pdb_iterator sub;
  int err = 0;

  cl_enter(cl, CL_LEVEL_SPEW, "%s", graphd_constraint_to_string(con));

  cl_assert(cl, con != NULL);

  /*  If the constraint is implicitly impossible to satisfy,
   *  and we care, let's stop it right here.
   */
  if (con->con_false && GRAPHD_CONSTRAINT_IS_MANDATORY(con)) {
    err = GRAPHD_ERR_NO;
    cl_log(cl, CL_LEVEL_FAIL,
           "graphd_iterate_constraint_push: "
           "constraint is marked as false");
    goto err;
  }

  PDB_IS_ITERATOR(cl, con->con_it);

  memset(&sub, 0, sizeof(sub));
  if ((grc = cm_zalloc(cm, sizeof(*grc))) == NULL) {
    int err = errno;
    cl_leave_err(cl, CL_LEVEL_SPEW, err, "cm_zalloc fails");
    *err_out = err ? err : ENOMEM;

    return;
  }

  /* Initialize the result as unassigned.
   */
  if ((grc->grc_err_out = err_out) != NULL) *err_out = 0;
  grc->grc_contents_out = contents_out;

  grc->grc_sub_assigned = NULL;
  grc->grc_sub_assigned_n = 0;
  grc->grc_base = grb;
  grc->grc_parent = NULL;
  grc->grc_parent_guid = NULL;
  grc->grc_sort = NULL;
  grc->grc_it = NULL;

  grc->grc_con = con;
  grc->grc_count_total = (unsigned long long)-1;
  grc->grc_count_wanted = false;
  grc->grc_data_wanted = false;
  grc->grc_sample_wanted = true;

  PDB_IS_ITERATOR(cl, con->con_it);

  /*  Create the per-iterate-context iterator that will return
   *  candidates for a match.
   */
  err = pdb_iterator_clone(g->g_pdb, con->con_it, &grc->grc_it);
  if (err != 0) goto err;

  /*  Schedule execution of the scanning part; it'll eventually
   *  pass control back to iterate_constraint_resume by calling
   *  graphd_iterate_constraint_finish().
   */
  graphd_stack_push(&greq->greq_stack, (graphd_stack_context *)grc,
                    &iterate_constraint_context_resource_type,
                    &iterate_constraint_type);

  graphd_iterate_constraint_alternatives(&greq->greq_stack,
                                         (graphd_stack_context *)grc);

  cl_leave(cl, CL_LEVEL_SPEW, "-> iterate_constraint_statistics");
  return;

err:
  *err_out = err;

  if (grc != NULL) {
    if (grc->grc_it != NULL) pdb_iterator_destroy(g->g_pdb, &grc->grc_it);

    cm_free(cm, grc);
  }
  cl_leave(cl, CL_LEVEL_SPEW, "error: %s", graphd_strerror(err));
}
