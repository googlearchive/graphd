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
#include "graphd/graphd-write.h"

#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdbool.h>

#define IS_ANCHOR(con)                      \
  ((con)->con_anchor == GRAPHD_FLAG_TRUE || \
   (con)->con_anchor == GRAPHD_FLAG_TRUE_LOCAL)

/*  Annotate "write" constraints with with "key" clauses with
 *  their matching counterparts in the database, if any.
 *
 *  - identify and duplicate selections from the anchor constraints
 *  - match the selection against the database
 *  - map the match results onto the constraint tree
 */

/* Utility: Return next constraint in traversal order: self, children, next.
 */
static graphd_constraint *next_constraint(graphd_constraint *con) {
  /* Children? */
  if (con->con_head != NULL) return con->con_head;

  /* Next? if not, go up (but do not revisit).  */
  while (con->con_next == NULL) {
    if (con->con_parent == NULL) return NULL;
    con = con->con_parent;
  }
  return con->con_next;
}

/*  Utility: Is this constraint the root of a cluster of constraints
 * 	that have keys and are connected by anchor links?
 */
static bool is_anchor_cluster_root(graphd_constraint *con) {
  return con != NULL && IS_ANCHOR(con) &&
         (con->con_parent == NULL || !IS_ANCHOR(con->con_parent));
}

/*  Utility: make result=((guid)) or result=((guid contents))
 */
static int make_result_pattern(graphd_request *greq, graphd_constraint *con) {
  graphd_pattern *l, *pat;

  con->con_result = l = graphd_pattern_alloc(greq, NULL, GRAPHD_PATTERN_LIST);
  if (con->con_result == NULL) return ENOMEM;

  l = graphd_pattern_alloc(greq, l, GRAPHD_PATTERN_LIST);
  if (l == NULL) return ENOMEM;

  pat = graphd_pattern_alloc(greq, l, GRAPHD_PATTERN_GUID);
  if (pat == NULL) return ENOMEM;

  if (con->con_head != NULL) {
    pat = graphd_pattern_alloc(greq, l, GRAPHD_PATTERN_CONTENTS);
    if (pat == NULL) return ENOMEM;
  }

  con->con_uses_contents = graphd_constraint_uses_contents(con);
  return 0;
}

/**
 * @brief Utility: Make a duplicate of <in> (which will be
 * 	used in the same request).
 *
 *  We're allocating on the request heap where needed,
 *  and are otherwise reusing data from the original request.
 *
 * @param greq	request context this is happening for.
 * @param in	bind this constraint.
 *
 * @result a duplicate of in, or NULL on allocation failure
 */
static graphd_constraint *duplicate_anchor_cluster(graphd_request *greq,
                                                   graphd_constraint *in) {
  graphd_constraint *out = NULL;
  cl_handle *cl = graphd_request_cl(greq);
  graphd_constraint *out_sub;
  graphd_constraint *in_sub;
  int i, err;

  if ((out = cm_malloc(greq->greq_req.req_cm, sizeof(*out))) == NULL) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "cm_malloc", errno ? errno : ENOMEM,
                 "out of memory while trying to duplicate %u bytes",
                 (unsigned int)sizeof(*out));
    return NULL;
  }
  graphd_constraint_initialize(graphd_request_graphd(greq), out);

  out->con_name = in->con_name;
  out->con_value = in->con_value;
  out->con_value_comparator = in->con_value_comparator;
  out->con_type = in->con_type;
  out->con_valuetype = in->con_valuetype;

  out->con_guid = in->con_guid;
  for (i = 0; i < PDB_LINKAGE_N; i++) out->con_linkcon[i] = in->con_linkcon[i];

  if (in->con_parent != NULL && IS_ANCHOR(in->con_parent) != 0)
    out->con_linkage = in->con_linkage;

  out->con_timestamp_valid = in->con_timestamp_valid;
  out->con_timestamp_min = in->con_timestamp_min;
  out->con_timestamp_max = in->con_timestamp_max;

  /*  Implicit aspects: must be live, must be the newest version;
   *  must exist exactly once.
   */
  out->con_live = GRAPHD_FLAG_TRUE;
  out->con_newest.gencon_valid = 1;
  out->con_newest.gencon_min = 0;
  out->con_newest.gencon_max = 0;
  out->con_countlimit = 1;
  out->con_countlimit_valid = true;
  out->con_resultpagesize = 1;
  out->con_resultpagesize_valid = true;
  out->con_archival = GRAPHD_FLAG_DONTCARE;

  out->con_count.countcon_min_valid = true;
  out->con_count.countcon_min = 1;

  if (is_anchor_cluster_root(in)) {
    out->con_count.countcon_max_valid = true;
    out->con_count.countcon_max = 1;
  }

  for (in_sub = in->con_head; in_sub != NULL; in_sub = in_sub->con_next) {
    if (!IS_ANCHOR(in_sub)) continue;

    if ((out_sub = duplicate_anchor_cluster(greq, in_sub)) == NULL) {
      /* Children go unfree'd - no big deal, they're
       * on the request heap.
       */
      return NULL;
    }
    graphd_constraint_append(out, out_sub);
  }
  cl_assert(cl, out->con_subcon_n <= in->con_subcon_n);

  /*  Result=((guid)) if there are no children,
   *  otherwise ((guid contents)).
   */
  if ((err = make_result_pattern(greq, out)) != 0 ||
      (err = graphd_pattern_frame_create(greq, out)) != 0) {
    cm_free(greq->greq_req.req_cm, out);
    return NULL;
  }
  return out;
}

/**
 * @brief Utility: Annotate anchor nodes in a cluster with search results.
 *
 * @param greq	request context this is happening for.
 * @param con	constraint to annotate.
 * @param val	value corresponding to the constraint.
 */
static void annotate_anchor_cluster(graphd_request *greq,
                                    graphd_constraint *con,
                                    graphd_value const *val) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_value const *li = 0;
  graphd_constraint *sub;
  size_t i;
  char buf[200];

  cl_enter(cl, CL_LEVEL_SPEW, "(%s)",
           graphd_value_to_string(val, buf, sizeof buf));

  while (val != NULL && (val->val_type == GRAPHD_VALUE_LIST ||
                         val->val_type == GRAPHD_VALUE_SEQUENCE) &&
         val->val_array_n >= 1) {
    li = val;
    val = li->val_list_contents;
  }
  cl_log(cl, CL_LEVEL_SPEW, "annotate_anchor_cluster: result is %s",
         graphd_value_to_string(val, buf, sizeof buf));

  cl_assert(cl, val != NULL);
  cl_assert(cl, li != NULL);

  if (val->val_type != GRAPHD_VALUE_GUID)
    cl_notreached(cl, "expected GUID, got %s",
                  graphd_value_to_string(val, buf, sizeof buf));
  cl_assert(cl, val->val_type == GRAPHD_VALUE_GUID);

  graphd_write_annotate_guid(con, &val->val_guid);
  i = 0;

  for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
    if (!IS_ANCHOR(sub)) continue;

    cl_assert(cl, li->val_list_n >= 2);
    val = li->val_list_contents + 1;
    cl_assert(cl, val->val_type == GRAPHD_VALUE_SEQUENCE);
    cl_assert(cl, val->val_list_n >= i);

    annotate_anchor_cluster(greq, sub, val->val_list_contents + i++);
  }

  cl_leave(cl, CL_LEVEL_SPEW, "%s",
           graphd_value_to_string(val, buf, sizeof buf));
}

typedef struct {
  graphd_stack_context ann_sc;

  graphd_constraint *ann_con;
  graphd_request *ann_greq;

  int ann_err;
  int *ann_err_out;

  graphd_value ann_value;

} annotate_context;

/**
 * @brief Annotate context resource method: free.
 * @param resource_manager_data	opaque application handle for all
 *		resources in this manager, ignored
 * @param resoure_data	the graphd_stack, cast to void *
 */

static void annotate_context_resource_free(void *resource_manager_data,
                                           void *resource_data) {
  annotate_context *ann = resource_data;

  graphd_value_finish(graphd_request_cl(ann->ann_greq), &ann->ann_value);
  cm_free(ann->ann_greq->greq_req.req_cm, ann);
}

/**
 * @brief Annotate context resource method: list.
 *
 * @param log_data	a cl_handle, cast to void *
 * @param resource_manager_data	opaque application handle for all
 *		resources in this manager, ignored
 * @param resoure_data	the graphd_stack, cast to void *
 */
static void annotate_context_resource_list(void *log_data,
                                           void *resource_manager_data,
                                           void *resource_data) {
  cl_handle *cl = log_data;
  annotate_context *ann = resource_data;

  cl_log(cl, CL_LEVEL_VERBOSE, "write anchor annotate context @ %p",
         (void *)ann);
}

/**
 * @brief Annotate context resource type
 */
static cm_resource_type annotate_context_resource_type = {
    "write anchor annotate context", annotate_context_resource_free,
    annotate_context_resource_list};

static int annotate_run_read_results(graphd_stack *stack,
                                     graphd_stack_context *stack_context);

/**
 * @brief Annotate context stack-context method: run (1)
 *
 *   This is called directly after the context has
 *   been pushed on stack.
 *
 * @param stack		Stack we're running on
 * @param stack_context	Specific context
 */
static int annotate_run(graphd_stack *stack,
                        graphd_stack_context *stack_context) {
  annotate_context *ann = (void *)stack_context;
  cl_handle *cl = graphd_request_cl(ann->ann_greq);

  cl_enter(cl, CL_LEVEL_SPEW, "enter");

  /*  Fast forward through the tree until we're
   *  standing on the root of a anchor cluster.
   */
  while (ann->ann_con != NULL && !is_anchor_cluster_root(ann->ann_con))
    ann->ann_con = next_constraint(ann->ann_con);

  if (ann->ann_con == NULL) {
    /*  We're done annotating matches to anchor constraints.
     */
    graphd_stack_pop(stack);
    cl_leave(cl, CL_LEVEL_SPEW, "done");

    return 0;
  }

  /* Duplicate the anchor cluster.
   */
  ann->ann_con->con_anchor_dup =
      duplicate_anchor_cluster(ann->ann_greq, ann->ann_con);
  if (ann->ann_con->con_anchor_dup == NULL) {
    if (ann->ann_err_out != NULL) *ann->ann_err_out = errno ? errno : ENOMEM;
    graphd_stack_pop(stack);

    cl_leave(cl, CL_LEVEL_SPEW, "error");
    return 0;
  }

  /*  Run the anchor cluster as a query.  The response
   *  will be delivered to the next function, below.
   */
  graphd_stack_resume(stack, stack_context, annotate_run_read_results);

  graphd_read_push(ann->ann_greq, ann->ann_con->con_anchor_dup, &ann->ann_value,
                   &ann->ann_err);

  cl_leave(cl, CL_LEVEL_SPEW, "-> read");
  return 0;
}

/**
 * @brief Annotate context stack-context method: run (1)
 *
 *   This is called directly after the context has
 *   been pushed on stack.
 *
 * @param stack		Stack we're running on
 * @param stack_context	Specific context
 */
static int annotate_suspend(graphd_stack *stack,
                            graphd_stack_context *stack_context) {
  annotate_context *ann = (void *)stack_context;
  cl_handle *cl = graphd_request_cl(ann->ann_greq);
  cm_handle *cm = ann->ann_greq->greq_req.req_cm;

  return graphd_value_suspend(cm, cl, &ann->ann_value);
}

static int annotate_thaw(graphd_stack *stack,
                         graphd_stack_context *stack_context) {
  return 0;
}

static const graphd_stack_type annotate_type = {annotate_run, annotate_suspend,
                                                annotate_thaw};

/**
 * @brief Annotate context stack-context method: run (2)
 *
 *   This deals with the results from the read that's tried
 *   to find matches for a anchor cluster.
 *
 * @param stack		Stack we're running on
 * @param stack_context	Specific context
 */
static int annotate_run_read_results(graphd_stack *stack,
                                     graphd_stack_context *stack_context) {
  annotate_context *ann = (void *)stack_context;
  cl_handle *cl = graphd_request_cl(ann->ann_greq);

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");

  if (ann->ann_err != 0 ||
      ann->ann_value.val_type == GRAPHD_VALUE_UNSPECIFIED) {
    int err = *ann->ann_err_out = ann->ann_err;
    graphd_request *greq = ann->ann_greq;

    graphd_stack_pop(stack);

    /*  These calls to errprintf free the write request
     *  and with it the stack - no accesses to <ann> are
     *  valid after this point.
     */
    if (err == GRAPHD_ERR_NO)
      graphd_request_errprintf(greq, false,
                               "EMPTY anchor constraint not found");

    else if (err == GRAPHD_ERR_TOO_MANY_MATCHES)
      graphd_request_errprintf(greq, false,
                               "TOOMANY anchor constraint not unique");
    else
      graphd_request_errprintf(greq, false,
                               "SYSTEM unexpected system error: %s",
                               graphd_strerror(err));

    cl_leave(cl, CL_LEVEL_VERBOSE, "anchoring fails: %s", graphd_strerror(err));
    return 0;
  }

  /*  The anchor read found something,
   *  and returns, in ann->ann_value, the results
   *  of matching the constraint against the existing
   *  database.
   */
  annotate_anchor_cluster(ann->ann_greq, ann->ann_con, &ann->ann_value);

  ann->ann_con = next_constraint(ann->ann_con);
  graphd_stack_resume(stack, stack_context, annotate_run);

  cl_leave(cl, CL_LEVEL_SPEW, "leave");
  return 0;
}

/**
 * @brief Push a context on the stack that will annotate a
 * 	anchor constraint tree.
 *
 *  This module annotates a constraint with information about
 *  primitives that match its key constraint clusters.
 *
 *  (Key constraint clusters are subtrees of constraints
 *  that have "key" clauses in them and are connected by
 *  linkage listed in its owner's "key" clause.)
 *
 *  The GUIDs of primitives corresponding to anchor
 *  constraints are reported by filling in GUID
 *  fields in the constraints.
 *
 * @param greq		Request whose stack we're pushing on
 * @param con		Constraint caller wants to bind
 * @param err_out 	return errors here.
 */
void graphd_write_annotate_anchor_push(graphd_request *greq,
                                       graphd_constraint *con, int *err_out) {
  annotate_context *ann;
  cl_handle *cl = graphd_request_cl(greq);

  cl_enter(cl, CL_LEVEL_SPEW, "enter");

  cl_assert(cl, err_out != NULL);
  *err_out = 0;

  ann = cm_malloc(greq->greq_req.req_cm, sizeof(*ann));
  if (ann == NULL) {
    int err = errno;
    cl_leave(cl, CL_LEVEL_ERROR, "failed to allocate context: %s",
             strerror(err));
    *err_out = err ? err : ENOMEM;
    return;
  }

  memset(ann, 0, sizeof(*ann));
  graphd_value_initialize(&ann->ann_value);

  ann->ann_greq = greq;
  ann->ann_con = con;
  ann->ann_err_out = err_out;

  *err_out = 0;

  graphd_stack_push(&greq->greq_stack, &ann->ann_sc,
                    &annotate_context_resource_type, &annotate_type);

  cl_leave(cl, CL_LEVEL_SPEW, "leave");
}
