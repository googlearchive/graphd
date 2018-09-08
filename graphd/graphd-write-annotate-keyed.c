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

/*  Annotate "write" constraints with with "key" clauses with
 *  their matching counterparts in the database, if any.
 *
 *  - identify and duplicate selections from the keyed constraints
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
 * 	that have keys and are connected by keyed links?
 */
static bool is_keyed_cluster_root(graphd_constraint *con) {
  if (con == NULL || !con->con_key) return false;

  if (con->con_parent == NULL || con->con_parent->con_key == 0) return true;

  /*  Is the connection between con and con's parent
   *  part of the key of the connection holder?
   *
   *  If yes, then this child was already included
   *  in the parent's cluster.
   */
  return !graphd_write_is_keyed_parent_connection(con);
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
 * @brief Utility: Make an empty linked constraint.
 *
 * @param greq	  request context this is happening for.
 * @param linkage what this constraint is to its parent.
 *
 * @result a new constraint with nothing in it, other
 *	than the linkage.
 */
static graphd_constraint *make_empty_linked_constraint(graphd_request *greq,
                                                       int linkage) {
  graphd_constraint *out = NULL;
  int err;

  if ((out = cm_malloc(greq->greq_req.req_cm, sizeof(*out))) == NULL)
    return NULL;

  graphd_constraint_initialize(graphd_request_graphd(greq), out);
  out->con_linkage = graphd_linkage_make_i_am(linkage);

  /*  Implicit aspects: must be live, must be the newest version;
   *  pagesize is 1.
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

  /*  Result=((guid))
   */
  if ((err = make_result_pattern(greq, out)) != 0 ||
      (err = graphd_pattern_frame_create(greq, out)) != 0) {
    cm_free(greq->greq_req.req_cm, out);
    return NULL;
  }
  return out;
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
static graphd_constraint *duplicate_keyed_cluster(graphd_request *greq,
                                                  graphd_constraint *in) {
  graphd_constraint *out = NULL;
  cl_handle *cl = graphd_request_cl(greq);
  graphd_constraint *out_sub;
  graphd_constraint *in_sub;
  int i, err;
  int key;

  if ((out = cm_malloc(greq->greq_req.req_cm, sizeof(*out))) == NULL) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "cm_malloc", errno ? errno : ENOMEM,
                 "out of memory while trying to duplicate %u bytes",
                 (unsigned int)sizeof(*out));
    return NULL;
  }
  graphd_constraint_initialize(graphd_request_graphd(greq), out);

  key = in->con_key;

  if (key & (1 << GRAPHD_PATTERN_NAME)) out->con_name = in->con_name;

  if (key & (1 << GRAPHD_PATTERN_VALUE)) {
    out->con_value = in->con_value;
    out->con_value_comparator = in->con_value_comparator;
  }
  if (key & (1 << GRAPHD_PATTERN_TYPEGUID)) out->con_type = in->con_type;

  if (key & ((1 << GRAPHD_PATTERN_DATATYPE) | (1 << GRAPHD_PATTERN_VALUETYPE)))
    out->con_valuetype = in->con_valuetype;

  for (i = 0; i < PDB_LINKAGE_N; i++)
    if (key & (1 << GRAPHD_PATTERN_LINKAGE(i)))
      out->con_linkcon[i] = in->con_linkcon[i];

  if (in->con_parent != NULL && in->con_parent->con_key != 0 &&
      graphd_write_is_keyed_parent_connection(in))
    out->con_linkage = in->con_linkage;

  if (key & (1 << GRAPHD_PATTERN_TIMESTAMP)) {
    out->con_timestamp_valid = in->con_timestamp_valid;
    out->con_timestamp_min = in->con_timestamp_min;
    out->con_timestamp_max = in->con_timestamp_max;
  }

  /*  Implicit aspects: must be live, must be the newest version;
   *  pagesize is 1.
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

  for (in_sub = in->con_head; in_sub != NULL; in_sub = in_sub->con_next) {
    if (!graphd_write_is_keyed_parent_connection(in_sub)) continue;

    if ((out_sub = duplicate_keyed_cluster(greq, in_sub)) == NULL) {
      /* Children go unfree'd - no big deal, they're
       * on the request heap.
       */
      return NULL;
    }
    graphd_constraint_append(out, out_sub);
  }
  cl_assert(cl, out->con_subcon_n <= in->con_subcon_n);

  /*  If our parent connection is keyed, but the parent
   *  itself isn't, reflect the parent into a subconstraint.
   */
  if (in->con_parent != NULL &&
      in->con_parent->con_key == GRAPHD_PATTERN_UNSPECIFIED &&
      graphd_write_is_keyed_parent_connection(in)) {
    /*  It must be an "is-my" linkage, where I'm pointing
     *  to the parent -- otherwise, the parent would have
     *  to have a key for it to be a keyed linkage.
     */
    cl_assert(cl, graphd_linkage_is_my(in->con_linkage));

    out_sub =
        make_empty_linked_constraint(greq, graphd_linkage_my(in->con_linkage));
    if (out_sub == NULL) {
      cl_log_errno(cl, CL_LEVEL_ERROR, "make_empty_linked_constraint", errno,
                   "out of memory while trying to "
                   "duplicate linked subconstraint");
      return NULL;
    }
    graphd_constraint_append(out, out_sub);
  }

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
 * @brief Utility: Annotate keyed nodes in a cluster with search results.
 *
 * @param greq	request context this is happening for.
 * @param con	constraint to annotate.
 * @param val	value corresponding to the constraint.
 */
static void annotate_keyed_cluster(graphd_request *greq, graphd_constraint *con,
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
  cl_log(cl, CL_LEVEL_SPEW, "annotate_keyed_cluster: result is %s",
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
    if (!graphd_write_is_keyed_parent_connection(sub)) continue;

    cl_assert(cl, li->val_list_n >= 2);
    val = li->val_list_contents + 1;
    cl_assert(cl, val->val_type == GRAPHD_VALUE_SEQUENCE);
    cl_assert(cl, val->val_list_n >= i);

    annotate_keyed_cluster(greq, sub, val->val_list_contents + i++);
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

  cl_log(cl, CL_LEVEL_VERBOSE, "write key=() annotate context @ %p",
         (void *)ann);
}

/**
 * @brief Annotate context resource type
 */
static cm_resource_type annotate_context_resource_type = {
    "write key=() annotate context", annotate_context_resource_free,
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
   *  standing on the root of a keyed cluster.
   */
  while (ann->ann_con != NULL && !is_keyed_cluster_root(ann->ann_con))
    ann->ann_con = next_constraint(ann->ann_con);

  if (ann->ann_con == NULL) {
    /*  We're done annotating matches to key constraints.
     *  Do a final pass over the constraint tree to infer
     *  GUID annotations for pointed-to subconstraint clusters
     *  not linked by keyed links.
     */
    graphd_stack_pop(stack);
    cl_leave(cl, CL_LEVEL_SPEW, "done");
    return 0;
  }

  /* Duplicate the keyed cluster.
   */
  ann->ann_con->con_key_dup =
      duplicate_keyed_cluster(ann->ann_greq, ann->ann_con);
  if (ann->ann_con->con_key_dup == NULL) {
    if (ann->ann_err_out != NULL) *ann->ann_err_out = errno ? errno : ENOMEM;
    graphd_stack_pop(stack);

    cl_leave(cl, CL_LEVEL_SPEW, "error");
    return 0;
  }

  /*  Run the keyed cluster as a query.  The response
   *  will be delivered to the next function, below.
   */
  graphd_stack_resume(stack, stack_context, annotate_run_read_results);

  graphd_read_push(ann->ann_greq, ann->ann_con->con_key_dup, &ann->ann_value,
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
static int annotate_freeze(graphd_stack *stack,
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

static const graphd_stack_type annotate_type = {annotate_run, annotate_freeze,
                                                annotate_thaw};

/**
 * @brief Annotate context stack-context method: run (2)
 *
 *   This deals with the results from the read that's tried
 *   to find matches for a keyed cluster.
 *
 * @param stack		Stack we're running on
 * @param stack_context	Specific context
 */
static int annotate_run_read_results(graphd_stack *stack,
                                     graphd_stack_context *stack_context) {
  annotate_context *ann = (void *)stack_context;
  cl_handle *cl = graphd_request_cl(ann->ann_greq);

  cl_enter(cl, CL_LEVEL_SPEW, "enter");

  if (ann->ann_err == 0 &&
      ann->ann_value.val_type != GRAPHD_VALUE_UNSPECIFIED) {
    /*  The keyed read found something,
     *  and returns, in ann->ann_value, the results
     *  of matching the constraint against the existing
     *  database.
     */
    annotate_keyed_cluster(ann->ann_greq, ann->ann_con, &ann->ann_value);
  } else if (ann->ann_err == GRAPHD_ERR_NO) {
    cl_log(cl, CL_LEVEL_SPEW, "annotate_run_read_results: no match");
    ann->ann_err = 0;
  }
  graphd_value_finish(cl, &ann->ann_value);

  if (ann->ann_err != 0) {
    cl_log(cl, CL_LEVEL_ERROR,
           "annotate_run_read_results: unexpected error: %s",
           graphd_strerror(ann->ann_err));
    *ann->ann_err_out = ann->ann_err;
    graphd_stack_pop(stack);

    cl_leave(cl, CL_LEVEL_SPEW, "aborting");
    return 0;
  }

  ann->ann_con = next_constraint(ann->ann_con);
  graphd_stack_resume(stack, stack_context, annotate_run);

  cl_leave(cl, CL_LEVEL_SPEW, "leave");
  return 0;
}

/**
 * @brief Push a context on the stack that will annotate a
 * 	keyed constraint tree.
 *
 *  This module annotates a constraint with information about
 *  primitives that match its key constraint clusters.
 *
 *  (Key constraint clusters are subtrees of constraints
 *  that have "key" clauses in them and are connected by
 *  linkage listed in its owner's "key" clause.)
 *
 *  The GUIDs of primitives corresponding to keyed
 *  constraints are reported by filling in GUID
 *  fields in the constraints.
 *
 * @param greq		Request whose stack we're pushing on
 * @param con		Constraint caller wants to bind
 * @param err_out 	return errors here.
 */
void graphd_write_annotate_keyed_push(graphd_request *greq,
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
