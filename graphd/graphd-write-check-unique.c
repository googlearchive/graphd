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

/*  Make sure that any unique clusters in a write request
 *  don't already exist.
 *
 *  - identify and duplicate selections from the unique constraints
 *  - match the selection against the database
 *  - if anything _doesn't_ return GRAPHD_ERR_NO,
 *	fail with GRAPHD_ERR_UNIQUE_EXISTS.
 */

#define HAS_GUID(guidcon)                                                    \
  ((guidcon).guidcon_include_valid && (guidcon).guidcon_include.gs_n == 1 && \
   !GRAPH_GUID_IS_NULL((guidcon).guidcon_include.gs_guid[0]))

/*  Utility: Is the connection between parent and child part
 *  of the unique constraint?
 */
static bool connection_is_part_of_unique_constraint(graphd_constraint *con) {
  if (con == NULL || con->con_parent == NULL) return false;

  return !!(graphd_linkage_is_my(con->con_linkage)
                ? (con->con_unique & (1 << GRAPHD_PATTERN_LINKAGE(
                                          graphd_linkage_my(con->con_linkage))))
                : (con->con_parent->con_unique &
                   (1 << GRAPHD_PATTERN_LINKAGE(
                        graphd_linkage_i_am(con->con_linkage)))));
}
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
 * 	that have unique annotations and are connected by unique
 *  	links?
 */
static bool is_unique_cluster_root(graphd_constraint *con) {
  if (con == NULL || !con->con_unique) return false;

  if (con->con_parent == NULL || con->con_parent->con_unique == 0) return true;

  /*  Is the connection between con and con's parent
   *  part of the key of the connection holder?
   *
   *  If yes, then this child was already included
   *  in the parent's cluster.
   */
  return !connection_is_part_of_unique_constraint(con);
}

/*  Utility: make result=()
 */
static int make_result_pattern(graphd_request *greq, graphd_constraint *con) {
  con->con_result = graphd_pattern_alloc(greq, NULL, GRAPHD_PATTERN_LIST);
  if (con->con_result == NULL) return errno ? errno : ENOMEM;

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
  out->con_resultpagesize_valid = true;
  out->con_resultpagesize = 1;
  out->con_countlimit_valid = true;
  out->con_countlimit = 1;
  out->con_archival = GRAPHD_FLAG_DONTCARE;
  out->con_count.countcon_min_valid = true;
  out->con_count.countcon_min = 1;

  /*  Result=()
   */
  if ((err = make_result_pattern(greq, out)) != 0 ||
      (err = graphd_pattern_frame_create(greq, out)) != 0) {
    cm_free(greq->greq_req.req_cm, out);
    return NULL;
  }
  return out;
}

/**
 * @brief Utility: Make a duplicate of <in> into <out> (both used in
 *  	the same request).
 *
 *  We're allocating on the request heap where needed, and are otherwise
 *  reusing data from the original request.
 *
 * @param greq	request context this is happening for.
 * @param in	bind this constraint.
 *
 * @return a duplicate of in
 */
static graphd_constraint *duplicate_unique_cluster(graphd_request *greq,
                                                   graphd_constraint *in) {
  graphd_constraint *out = NULL;
  cl_handle *cl = graphd_request_cl(greq);
  graphd_constraint *out_sub;
  graphd_constraint *in_sub;
  int i, err;
  int unq;

  cl_enter(cl, CL_LEVEL_SPEW, "(in:%s)", graphd_constraint_to_string(in));

  if ((out = cm_malloc(greq->greq_req.req_cm, sizeof(*out))) == NULL) {
    cl_leave(cl, CL_LEVEL_SPEW, "out of memory");
    return NULL;
  }
  graphd_constraint_initialize(graphd_request_graphd(greq), out);

  unq = in->con_unique;

  if (unq & (1 << GRAPHD_PATTERN_NAME)) out->con_name = in->con_name;

  if (unq & (1 << GRAPHD_PATTERN_VALUE)) {
    out->con_value = in->con_value;
    out->con_value_comparator = in->con_value_comparator;
  }

  if (unq & (1 << GRAPHD_PATTERN_TYPEGUID)) out->con_type = in->con_type;

  if (unq & ((1 << GRAPHD_PATTERN_DATATYPE) | (1 << GRAPHD_PATTERN_VALUETYPE)))
    out->con_valuetype = in->con_valuetype;

  for (i = 0; i < PDB_LINKAGE_N; i++)
    if (unq & (1 << GRAPHD_PATTERN_LINKAGE(i)))
      out->con_linkcon[i] = in->con_linkcon[i];

  if (connection_is_part_of_unique_constraint(in))
    out->con_linkage = in->con_linkage;

  if (unq & (1 << GRAPHD_PATTERN_TIMESTAMP)) {
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
  out->con_count.countcon_min_valid = true;
  out->con_count.countcon_min = 1;
  out->con_resultpagesize_valid = true;
  out->con_resultpagesize = 1;
  out->con_countlimit_valid = true;
  out->con_countlimit = 1;
  out->con_archival = GRAPHD_FLAG_DONTCARE;

  /*  If the write has a GUID constraint - that is, if it
   *  versions another GUID or lineage - exclude that GUID
   *  (or that lineage's head) from the match for the purposes
   *  of unique.
   */
  if (HAS_GUID(in->con_guid) && !in->con_guid.guidcon_include_annotated) {
    out->con_guid.guidcon_exclude_valid = true;
    graphd_guid_set_initialize(&out->con_guid.guidcon_exclude);

    err = graphd_guid_set_add(greq, &out->con_guid.guidcon_exclude,
                              in->con_guid.guidcon_include.gs_guid);
    if (err != 0) {
      /* Can't currently happen, actually.
       */
      cl_leave(cl, CL_LEVEL_SPEW, "allocation error");
      return NULL;
    }
    out->con_guid.guidcon_exclude_valid = true;
  }

  for (in_sub = in->con_head; in_sub != NULL; in_sub = in_sub->con_next) {
    if (!connection_is_part_of_unique_constraint(in_sub)) continue;

    if ((out_sub = duplicate_unique_cluster(greq, in_sub)) == NULL) {
      /* Children go unfree'd - no big deal, they're
       * on the request heap.
       */
      cl_leave(cl, CL_LEVEL_SPEW, "recursive error");
      return NULL;
    }
    graphd_constraint_append(out, out_sub);

    /*  If the subconstraint already knows its GUID, include
     *  the GUID itself in the constraint set as a linkage.
     */
    if (graphd_linkage_is_i_am(in_sub->con_linkage) &&
        HAS_GUID(in_sub->con_guid)) {
      int linkage = graphd_linkage_i_am(in_sub->con_linkage);
      err = graphd_guid_constraint_intersect_with_guid(
          greq, out, out->con_linkcon + linkage,
          in_sub->con_guid.guidcon_include.gs_guid);
      if (err != 0) {
        cl_leave(cl, CL_LEVEL_VERBOSE, "GUID intersect fails: %s",
                 graphd_strerror(err));
        return NULL;
      }

      cl_log(cl, CL_LEVEL_VERBOSE,
             "duplicate_unique_cluster: sub con %s "
             "knows its guid",
             graphd_constraint_to_string(in_sub));
    }
  }
  cl_assert(cl, out->con_subcon_n <= in->con_subcon_n);

  /*  If our parent connection is unique, but the parent
   *  itself isn't, reflect the parent into a subconstraint.
   */
  if (in->con_parent != NULL &&
      in->con_parent->con_unique == GRAPHD_PATTERN_UNSPECIFIED &&
      connection_is_part_of_unique_constraint(in)) {
    graphd_constraint *par = in->con_parent;

    /*  It must be an "is-my" linkage, where I'm pointing
     *  to the parent -- otherwise, the parent would have
     *  to have a unique tag for it to be a unique linkage.
     */
    cl_assert(cl, graphd_linkage_is_my(in->con_linkage));

    /*  If the parent constraint already knows its GUID, include
     *  the GUID itself in the constraint set as a linkage.
     */
    if (HAS_GUID(par->con_guid)) {
      int linkage = graphd_linkage_my(in->con_linkage);

      err = graphd_guid_constraint_intersect_with_guid(
          greq, out, out->con_linkcon + linkage,
          par->con_guid.guidcon_include.gs_guid);
      if (err != 0) {
        cl_leave(cl, CL_LEVEL_VERBOSE, "GUID intersect fails: %s",
                 graphd_strerror(err));
        return NULL;
      }
      cl_log(cl, CL_LEVEL_VERBOSE,
             "duplicate_unique_cluster: parent con %s "
             "knows its guid",
             graphd_constraint_to_string(par));
    } else {
      out_sub = make_empty_linked_constraint(
          greq, graphd_linkage_my(in->con_linkage));
      if (out_sub == NULL) {
        cl_leave(cl, CL_LEVEL_SPEW,
                 "failed to "
                 "allocate linked constraint");
        return NULL;
      }
      graphd_constraint_append(out, out_sub);
    }
  }

  /*  Result=()
   */
  if ((err = make_result_pattern(greq, out)) != 0 ||
      (err = graphd_pattern_frame_create(greq, out)) != 0) {
    cl_leave(cl, CL_LEVEL_SPEW, "result pattern error: %s",
             graphd_strerror(err));
    return NULL;
  }

  cl_leave(cl, CL_LEVEL_SPEW, "%s", graphd_constraint_to_string(out));
  return out;
}

typedef struct {
  graphd_stack_context cuc_sc;

  graphd_constraint *cuc_con;
  graphd_request *cuc_greq;

  int cuc_err;
  int *cuc_err_out;

  graphd_value cuc_value;

} check_unique_context;

/**
 * @brief Annotate context resource method: free.
 * @param resource_manager_data	opaque application handle for all
 *		resources in this manager, ignored
 * @param resoure_data	the graphd_stack, cast to void *
 */

static void check_unique_context_resource_free(void *resource_manager_data,
                                               void *resource_data) {
  check_unique_context *cuc = resource_data;

  graphd_value_finish(graphd_request_cl(cuc->cuc_greq), &cuc->cuc_value);

  cm_free(cuc->cuc_greq->greq_req.req_cm, cuc);
}

/**
 * @brief Annotate context resource method: list.
 *
 * @param log_data	a cl_handle, cast to void *
 * @param resource_manager_data	opaque application handle for all
 *		resources in this manager, ignored
 * @param resoure_data	the graphd_stack, cast to void *
 */
static void check_unique_context_resource_list(void *log_data,
                                               void *resource_manager_data,
                                               void *resource_data) {
  cl_handle *cl = log_data;
  check_unique_context *cuc = resource_data;

  cl_log(cl, CL_LEVEL_VERBOSE, "write unique=() checking context @ %p",
         (void *)cuc);
}

/**
 * @brief Check unique context resource type
 */
static cm_resource_type check_unique_context_resource_type = {
    "write unique=() check context", check_unique_context_resource_free,
    check_unique_context_resource_list};

static int check_unique_run_read_results(graphd_stack *stack,
                                         graphd_stack_context *stack_context);

/**
 * @brief Check unique context stack-context method: run (1)
 *
 *   This is called directly after the context has
 *   been pushed on stack.
 *
 * @param stack		Stack we're running on
 * @param stack_context	Specific context
 */
static int check_unique_run(graphd_stack *stack,
                            graphd_stack_context *stack_context) {
  check_unique_context *cuc = (void *)stack_context;
  cl_handle *cl = graphd_request_cl(cuc->cuc_greq);

  cl_enter(cl, CL_LEVEL_SPEW, "enter");

  /*  Fast forward through the tree until we're
   *  standing on the root of a unique cluster.
   */
  while (cuc->cuc_con != NULL && !is_unique_cluster_root(cuc->cuc_con))
    cuc->cuc_con = next_constraint(cuc->cuc_con);

  if (cuc->cuc_con == NULL) {
    /*  Done.
     */
    graphd_stack_pop(stack);
    cl_leave(cl, CL_LEVEL_SPEW, "done");
    return 0;
  }

  /* Duplicate the unique cluster.
   */
  cuc->cuc_con->con_unique_dup =
      duplicate_unique_cluster(cuc->cuc_greq, cuc->cuc_con);
  if (cuc->cuc_con->con_unique_dup == NULL) {
    if (cuc->cuc_err_out != NULL) *cuc->cuc_err_out = errno ? errno : ENOMEM;
    graphd_stack_pop(stack);
    cl_leave(cl, CL_LEVEL_SPEW, "error (stored)");
    return 0;
  }

  /*  Run the unique cluster as a query.  The response
   *  will be delivered to the next function, below.
   */
  graphd_stack_resume(stack, stack_context, check_unique_run_read_results);

  graphd_read_push(cuc->cuc_greq, cuc->cuc_con->con_unique_dup, &cuc->cuc_value,
                   &cuc->cuc_err);

  cl_leave(cl, CL_LEVEL_SPEW, "-> read");
  return 0;
}

static int check_unique_freeze(graphd_stack *stack,
                               graphd_stack_context *stack_context) {
  return PDB_ERR_MORE;
}

static int check_unique_thaw(graphd_stack *stack,
                             graphd_stack_context *stack_context) {
  return GRAPHD_ERR_NO;
}

static graphd_stack_type check_unique_type = {
    check_unique_run, check_unique_freeze, check_unique_thaw};

/**
 * @brief Annotate context stack-context method: run (2)
 *
 *   This deals with the results from the read that's tried
 *   to find matches for a unique cluster.
 *
 * @param stack		Stack we're running on
 * @param stack_context	Specific context
 */
static int check_unique_run_read_results(graphd_stack *stack,
                                         graphd_stack_context *stack_context) {
  check_unique_context *cuc = (void *)stack_context;
  cl_handle *cl = graphd_request_cl(cuc->cuc_greq);

  cl_enter(cl, CL_LEVEL_SPEW, "enter");

  if (cuc->cuc_err == 0)
    cuc->cuc_err = GRAPHD_ERR_UNIQUE_EXISTS;

  else if (cuc->cuc_err == GRAPHD_ERR_NO)
    cuc->cuc_err = 0;

  graphd_value_finish(cl, &cuc->cuc_value);

  if (cuc->cuc_err != 0) {
    *cuc->cuc_err_out = cuc->cuc_err;
    cl_leave(cl, CL_LEVEL_SPEW, "%s", graphd_strerror(cuc->cuc_err));

    graphd_stack_pop(stack);
    return 0;
  }

  cuc->cuc_con = next_constraint(cuc->cuc_con);
  graphd_stack_resume(stack, stack_context, check_unique_run);

  cl_leave(cl, CL_LEVEL_SPEW, "leave");
  return 0;
}

/**
 * @brief Push a context on the stack that will check
 * 	unique clusters in a constraint tree.
 *
 *  This module returns GRAPHD_ERR_UNIQUE_EXISTS if any of the
 *  constraint clusters marked as "unique" already exist in the
 *  database.
 *
 *  (Unique constraint clusters are subtrees of constraints
 *  that have "unique" clauses in them and are connected by
 *  linkage listed in its owner's "unique" clause.)
 *
 * @param greq		Request whose stack we're pushing on
 * @param con		Root of the request's constraint tree
 * @param err_out 	return errors here.
 */
void graphd_write_check_unique_push(graphd_request *greq,
                                    graphd_constraint *con, int *err_out) {
  check_unique_context *cuc;
  cl_handle *cl = graphd_request_cl(greq);

  cl_enter(cl, CL_LEVEL_SPEW, "enter");

  cl_assert(cl, err_out != NULL);
  *err_out = 0;

  cuc = cm_zalloc(greq->greq_req.req_cm, sizeof(*cuc));
  if (cuc == NULL) {
    int err = errno;
    cl_leave(cl, CL_LEVEL_ERROR, "failed to allocate context: %s",
             strerror(err));
    *err_out = err ? err : ENOMEM;
    return;
  }

  graphd_value_initialize(&cuc->cuc_value);

  cuc->cuc_greq = greq;
  cuc->cuc_con = con;
  cuc->cuc_err_out = err_out;

  *err_out = 0;

  graphd_stack_push(&greq->greq_stack, &cuc->cuc_sc,
                    &check_unique_context_resource_type, &check_unique_type);

  cl_leave(cl, CL_LEVEL_SPEW, "leave");
}
