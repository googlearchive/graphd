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
#include <sysexits.h>

typedef struct graphd_write_context {
  graphd_stack_context gwc_sc;

  graphd_request *gwc_greq;
  graphd_constraint *gwc_con;

  int gwc_err;

  int *gwc_err_out;
  graphd_value *gwc_val_out;

} graphd_write_context;

#define GUIDCON_HAS_GUID(x) \
  ((x).guidcon_include_valid && (x).guidcon_include.gs_n == 1)

bool graphd_write_result_ok(graphd_request *greq, graphd_pattern const *pat) {
  char buf[200];

  switch (pat->pat_type) {
    default:
      graphd_request_errprintf(greq, 0,
                               "SEMANTICS cannot use "
                               "%s as a write result, only "
                               "literal=, guid, contents, or none",
                               graphd_pattern_to_string(pat, buf, sizeof buf));
      return false;

    case GRAPHD_PATTERN_LIST:
      for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next)
        if (!graphd_write_result_ok(greq, pat)) return false;

    case GRAPHD_PATTERN_CONTENTS:
    case GRAPHD_PATTERN_GUID:
    case GRAPHD_PATTERN_NONE:
    case GRAPHD_PATTERN_LITERAL:
      break;
  }
  return true;
}

/*  Pruning the result tree - one value, one pattern.
 */
static int prune_pattern(graphd_request *greq, graphd_constraint const *con,
                         graphd_value const *val_in, graphd_value *val_out,
                         graphd_pattern const *pat) {
  graphd_pattern const *p;
  graphd_value *v;
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  graphd_handle *g = graphd_request_graphd(greq);
  size_t i;
  char buf[200], b2[200];
  int err;

  cl_log(cl, CL_LEVEL_VERBOSE, "prune_pattern con=%p pat=%s val=%s",
         (void *)con, graphd_pattern_to_string(pat, buf, sizeof buf),
         graphd_value_to_string(val_in, b2, sizeof b2));

  cl_assert(cl, val_in != NULL);
  cl_assert(cl, val_in->val_type == GRAPHD_VALUE_LIST);
  cl_assert(cl, val_in->val_list_n >= 1);

  switch (pat->pat_type) {
    default:
      cl_notreached(cl, "unexpected pattern %p %s", (void *)pat,
                    graphd_pattern_to_string(pat, buf, sizeof buf));
    /* NOTREACHED */

    case GRAPHD_PATTERN_CONTENTS:

      /*  Add values from val_in 1..N-1 to the
       *  containing list.
       */
      graphd_value_sequence_set(cm, val_out);

      if (val_in->val_list_n <= 1) return 0;

      v = graphd_value_array_alloc(g, cl, val_out, val_in->val_list_n - 1);
      if (v == NULL) return errno ? errno : ENOMEM;

      for (i = 1; i < val_in->val_list_n; i++, v++) {
        err = graphd_value_copy(g, cm, cl, v, val_in->val_list_contents + i);
        if (err != 0) return err;
      }
      graphd_value_array_alloc_commit(cl, val_out, val_in->val_list_n - 1);
      break;

    case GRAPHD_PATTERN_GUID:
      err = graphd_value_copy(g, cm, cl, val_out, val_in->val_list_contents);
      if (err != 0) return err;
      break;

    case GRAPHD_PATTERN_NONE:
      graphd_value_atom_set_constant(val_out, "", 0);
      break;

    case GRAPHD_PATTERN_LIST:
      /*  How many elements will this list have?
       */
      i = 0;
      for (p = pat->pat_list_head; p != NULL; p = p->pat_next) i++;

      /*  Allocate a list of the right size.
       */
      err = graphd_value_list_alloc(g, cm, cl, val_out, i);
      if (err != 0) return err;

      /*  Recursively fill the list with values.
       */
      v = val_out->val_list_contents;
      for (p = pat->pat_list_head; p != NULL; p = p->pat_next, v++) {
        err = prune_pattern(greq, con, val_in, v, p);
        if (err != 0) return err;
      }
      break;

    case GRAPHD_PATTERN_LITERAL:
      graphd_value_text_set(val_out, GRAPHD_VALUE_ATOM, pat->pat_string_s,
                            pat->pat_string_e, NULL);
      break;
  }
  return 0;
}

/*  Prune the full write result tree in accordance with the
 *  result patterns specified by the write request constraint.
 */
static int graphd_write_constraint_prune(graphd_request *greq,
                                         graphd_constraint const *con,
                                         graphd_value *val) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_pattern const *pat;
  graphd_value new_result;
  int err;

  if (greq->greq_request != GRAPHD_REQUEST_WRITE ||
      greq->greq_error_message != NULL)
    return 0;

  cl_assert(cl, val->val_type == GRAPHD_VALUE_LIST);

  /*  We are currently holding LIST( GUID, CONTENTS ).  If that's
   *  what the pattern dictates, stick with it.
   */
  if ((pat = con->con_result) == NULL ||
      (pat->pat_type == GRAPHD_PATTERN_LIST && pat->pat_list_n == 2 &&
       pat->pat_list_head->pat_type == GRAPHD_PATTERN_GUID &&
       pat->pat_list_head->pat_next->pat_type == GRAPHD_PATTERN_CONTENTS))
    return 0;

  /*  Make a result pattern;
   *  free our current pattern;
   *  move the result pattern into the location of our current pattern.
   */
  graphd_value_initialize(&new_result);
  err = prune_pattern(greq, con, val, &new_result, pat);
  if (err != 0) return err;

  graphd_value_finish(cl, val);
  *val = new_result;

  return 0;
}

/**
 * @brief Actually write primitives
 *
 *  This call recurses through a constraint tree, writing the
 *  primitives.
 *
 *  Precondition: If there is a parent constraint that we link
 *  to, it has been written, and its GUID is pointed to by parent_guid.
 *
 *  Postcondition: This primitive and all the primitives in the
 *  constraints below it have been written or identified, and their
 *  GUIDs have been stored in a list created in reply.
 *
 *  Some failures of graphd_write_constraints may turn the
 *  calling request into an error request and free stack
 *  contexts allocated by the caller.
 *
 * @param g		graphd handle
 * @param gses		session
 * @param greq		request
 * @param con		constraint to write.
 * @param parent_guid	NULL or pointer to GUID of parent cosntraint
 * @param reply		Assign the value of the created GUID to here.
 */
int graphd_write_constraint(graphd_request *greq, graphd_constraint *con,
                            graph_guid const *guid_parent,
                            pdb_primitive const *pr_parent,
                            graphd_value *reply) {
  graphd_handle *g = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  graph_guid guid_parent_buf;
  char buf[200];
  int err;

  cl_enter(cl, CL_LEVEL_VERBOSE, "con=%p (%s), result=%s", (void *)con,
           graphd_constraint_to_string(con),
           graphd_pattern_to_string(con->con_result, buf, sizeof buf));

  cl_assert(cl, reply != NULL);
  if (con->con_key != 0) {
    pdb_primitive tmp_parent;

    pdb_primitive_initialize(&tmp_parent);

    /*  If we don't know our parent primitive,
     *  but we do know the parent GUID, read
     *  the parent for use in graphd_key_bind().
     */
    if (pr_parent == NULL && guid_parent != NULL) {
      /* Reread the parent primitive */
      err = pdb_primitive_read(g->g_pdb, guid_parent, &tmp_parent);
      if (err != 0) {
        cl_leave(cl, CL_LEVEL_VERBOSE,
                 "failed to "
                 "read parent primitive: %s",
                 err ? graphd_strerror(err) : "ok");
        return err;
      }
      pr_parent = &tmp_parent;
    }
    err = graphd_key_bind(greq, con, pr_parent, reply);
    pdb_primitive_finish(g->g_pdb, &tmp_parent);

    if (err != 0)
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_key_bind", err,
                   "parent bind fails");

    cl_leave(cl, CL_LEVEL_VERBOSE, "keyed: %s",
             err ? graphd_strerror(err) : "ok");
    return err;
  }

  if (pr_parent != NULL && guid_parent == NULL) {
    pdb_primitive_guid_get(pr_parent, guid_parent_buf);
    guid_parent = &guid_parent_buf;
  }

  cl_assert(cl, reply != NULL);
  err = graphd_key_align(greq, con, guid_parent, NULL, reply);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_key_align", err,
                 "internal error in write");

    graphd_value_finish(graphd_request_cl(greq), reply);
    if (greq->greq_error_message == NULL) {
      if (err == GRAPHD_ERR_PRIMITIVE_TOO_LARGE)
        graphd_request_errprintf(greq, 0, "TOOBIG primitive too big");
      else
        graphd_request_errprintf(greq, 0, "SYSTEM internal error in write: %s",
                                 strerror(err));
    }
    goto err;
  }

  err = graphd_write_constraint_prune(greq, con, reply);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_write_prune", err,
                 "internal error while allocating write result");

    graphd_value_finish(graphd_request_cl(greq), reply);
    if (greq->greq_error_message == NULL)
      graphd_request_errprintf(greq, 0,
                               "SYSTEM internal error while allocating "
                               "write results: %s",
                               strerror(err));
  }
err:
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s",
           err ? graphd_strerror(err)
               : graphd_value_to_string(reply, buf, sizeof buf));
  return err;
}

/**
 * @brief Utility: is this write request semantically correct?
 */

static int graphd_write_check_endpoint(graphd_request *greq,
                                       graph_guid const *guid,
                                       char const *name) {
  graphd_handle *g = graphd_request_graphd(greq);
  int err;
  pdb_primitive pr;
  char errbuf[200];
  char guidbuf[GRAPH_GUID_SIZE];

  /*  If there's an explicit right or left, they must
   *  exist.
   */
  if ((err = pdb_primitive_read(g->g_pdb, guid, &pr)) != 0) {
    /*  Normally, we'll treat a NULL GUID is a category error
     *  rather than a value error - but here, be consistent
     *  and just complain that you can't resolve it.
     */
    snprintf(errbuf, sizeof errbuf, "SEMANTICS %s=%s: %s", name,
             graph_guid_to_string(guid, guidbuf, sizeof guidbuf),
             err == PDB_ERR_NO ? "not found" : graphd_strerror(err));
    graphd_request_error(greq, errbuf);

    return GRAPHD_ERR_SEMANTICS;
  }
  pdb_primitive_finish(g->g_pdb, &pr);
  return 0;
}

static bool single_element_strqeue(graphd_string_constraint_queue const *q) {
  graphd_string_constraint *strcon;

  if (q == NULL || (strcon = q->strqueue_head) == NULL) return true;

  return strcon->strcon_op == GRAPHD_OP_EQ && strcon->strcon_next == NULL &&
         (strcon->strcon_head == NULL ||
          strcon->strcon_head->strcel_next == NULL);
}

/**
 * @brief Utility: is this write request semantically correct?
 */

static int graphd_write_check(graphd_request *greq,
                              graphd_constraint const *con) {
  graphd_handle *g = graphd_request_graphd(greq);
  int err = 0;
  graphd_constraint const *subcon;
  unsigned int guidcon_linkage;
  unsigned int subcon_linkage;
  size_t i;

  /*  The write can have at most one GUID.
   */
  if (con->con_guid.guidcon_include_valid &&
      con->con_guid.guidcon_include.gs_n > 1) {
    graphd_request_error(greq, "SEMANTICS cannot version more than one GUID");
    return GRAPHD_ERR_SEMANTICS;
  }

  if (!graphd_write_result_ok(greq, con->con_result))
    return GRAPHD_ERR_SEMANTICS;

  /* The write can have at most one value, name, or type each.
   */
  if (greq->greq_request == GRAPHD_REQUEST_WRITE) {
    if (!single_element_strqeue(&con->con_type)) {
      graphd_request_errprintf(greq, false,
                               "SYNTAX more than one value for \"type\"");
      return GRAPHD_ERR_SYNTAX;
    }
    if (!single_element_strqeue(&con->con_name)) {
      graphd_request_errprintf(greq, false,
                               "SYNTAX more than one value for \"name\"");
      return GRAPHD_ERR_SYNTAX;
    }
    if (!single_element_strqeue(&con->con_value)) {
      graphd_request_errprintf(greq, false,
                               "SYNTAX more than one value for \"value\"");
      return GRAPHD_ERR_SYNTAX;
    }
  }

  /*  If we have a GUID with =, the corresponding record must
   *  exist and be last in its lineage.
   */
  if (GUIDCON_HAS_GUID(con->con_guid)) {
    pdb_primitive pr;

    err = pdb_primitive_read(g->g_pdb, con->con_guid.guidcon_include.gs_guid,
                             &pr);
    if (err != 0) {
      char buf[GRAPH_GUID_SIZE];

      graphd_request_errprintf(
          greq, 0,
          "SEMANTICS %s: "
          "cannot read predecessor record %s",
          err == PDB_ERR_NO ? "not found" : graphd_strerror(err),
          graph_guid_to_string(con->con_guid.guidcon_include.gs_guid, buf,
                               sizeof buf));
      return GRAPHD_ERR_SEMANTICS;
    }

    err = pdb_generation_check_range(
        g->g_pdb, NULL /* writes are always now */,
        con->con_guid.guidcon_include.gs_guid,
        GRAPH_GUID_SERIAL(con->con_guid.guidcon_include.gs_guid[0]), true, 0,
        0, /* The 0th newest */
        false, 0, 0);
    if (err != 0) {
      char buf[GRAPH_GUID_SIZE];
      char const *gs = graph_guid_to_string(
          con->con_guid.guidcon_include.gs_guid, buf, sizeof buf);

      graphd_request_errprintf(
          greq, 0, (err == GRAPHD_ERR_NO ? "OUTDATED \"%s\" has been versioned"
                                         : "SYSTEM unexpected error while "
                                           "looking up versions of \"%s\""),
          gs);
      pdb_primitive_finish(g->g_pdb, &pr);
      return err;
    }
    pdb_primitive_finish(g->g_pdb, &pr);
  }

  /*  You can't both have a type guid and a type.
   */
  if (GUIDCON_HAS_GUID(con->con_typeguid) &&
      con->con_type.strqueue_head != NULL) {
    graphd_request_error(greq,
                         "SEMANTICS can't have a type and a typeguid in the "
                         "same write request.");
    return GRAPHD_ERR_SEMANTICS;
  }

  if (graphd_linkage_is_my(con->con_linkage) &&
      (con->con_linkcon[graphd_linkage_my(con->con_linkage)]
           .guidcon_include_valid ||
       con->con_linkcon[graphd_linkage_my(con->con_linkage)]
           .guidcon_match_valid)) {
    int l = graphd_linkage_my(con->con_linkage);
    char const *lname = pdb_linkage_to_string(l);

    graphd_request_errprintf(
        greq, 0, "SEMANTICS cannot mix <-%s and %s%s=...", lname, lname,
        con->con_linkcon[l].guidcon_include_valid ? "" : "~");
    return GRAPHD_ERR_SEMANTICS;
  }

  subcon_linkage = guidcon_linkage = 0;
  for (i = 0; i < PDB_LINKAGE_N; i++) {
    if (con->con_linkcon[i].guidcon_include_valid ||
        con->con_linkcon[i].guidcon_match_valid)
      guidcon_linkage |= 1 << i;
  }

  for (subcon = con->con_head; subcon; subcon = subcon->con_next) {
    if ((err = graphd_write_check(greq, subcon)) != 0) {
      return err;
    }

    if (graphd_linkage_is_i_am(subcon->con_linkage)) {
      unsigned int linkage;

      linkage = graphd_linkage_i_am(subcon->con_linkage);
      if (guidcon_linkage & (1 << linkage)) {
        char const *name;

        name = pdb_linkage_to_string(linkage);
        graphd_request_errprintf(greq, 0, "SEMANTICS cannot mix %s->() and %s=",
                                 name, name);
        return GRAPHD_ERR_SEMANTICS;
      }

      if (subcon_linkage & (1 << linkage)) {
        graphd_request_errprintf(greq, 0,
                                 "SEMANTICS %s->() conflicts with "
                                 "sibling subconstraint",
                                 pdb_linkage_to_string(linkage));
        return GRAPHD_ERR_SEMANTICS;
      }
      subcon_linkage |= 1 << linkage;
    }
  }

  /*  Can't claim that both my parent and my subconstraint are pointed
   *  to by the same field.  (Strictly speaking, they might be the same
   *  primitive, but ...)
   */
  if (graphd_linkage_is_my(con->con_linkage) &&
      (subcon_linkage & (1 << graphd_linkage_my(con->con_linkage)))) {
    char const *name;

    name = pdb_linkage_to_string(graphd_linkage_my(con->con_linkage));
    graphd_request_errprintf(greq, 0,
                             "SEMANTICS %s->() subconstraint "
                             "conflicts with <-%s in container",
                             name, name);
    return GRAPHD_ERR_SEMANTICS;
  }

  /*  If there's an explicit right/left/typeguid/scope,
   *  they must exist.
   */
  for (i = 0; i < PDB_LINKAGE_N; i++)
    if (GUIDCON_HAS_GUID(con->con_linkcon[i])) {
      err = graphd_write_check_endpoint(
          greq, con->con_linkcon[i].guidcon_include.gs_guid,
          pdb_linkage_to_string(i));
      if (err != 0) return err;
    }

  /* If you're writing, your constraint must not be self-contradictory.
   */
  if (con->con_false) {
    graphd_request_error(
        greq, con->con_error
                  ? con->con_error
                  : "SEMANTICS self-contradictory constraint in literal");
    return GRAPHD_ERR_SEMANTICS;
  }

  /* Write constraints can't have dateline constraints.
   */
  if (con->con_dateline.dateline_min != NULL ||
      con->con_dateline.dateline_max != NULL) {
    graphd_request_error(greq, "SEMANTICS dateline constraint in literal");
    return GRAPHD_ERR_SEMANTICS;
  }

  return err;
}

/**
 * @brief Write context resource method: free.
 * @param resource_manager_data	opaque application handle for all
 *		resources in this manager, ignored
 * @param resoure_data	the graphd_stack, cast to void *
 */

static void write_context_resource_free(void *resource_manager_data,
                                        void *resource_data) {
  graphd_write_context *gwc = resource_data;

  if (gwc->gwc_val_out != NULL)
    graphd_value_finish(graphd_request_cl(gwc->gwc_greq), gwc->gwc_val_out);

  cm_free(gwc->gwc_greq->greq_req.req_cm, gwc);
}

/**
 * @brief Annotate context resource method: list.
 *
 * @param log_data	a cl_handle, cast to void *
 * @param resource_manager_data	opaque application handle for all
 *		resources in this manager, ignored
 * @param resoure_data	the graphd_stack, cast to void *
 */
static void write_context_resource_list(void *log_data,
                                        void *resource_manager_data,
                                        void *resource_data) {
  cl_handle *cl = log_data;
  graphd_write_context *gwc = resource_data;

  cl_log(cl, CL_LEVEL_VERBOSE, "write context (%s)",
         graphd_constraint_to_string(gwc->gwc_con));
}

/**
 * @brief Write context resource type
 */
static cm_resource_type write_context_resource_type = {
    "write context", write_context_resource_free, write_context_resource_list};

static int write_freeze(graphd_stack *stack,
                        graphd_stack_context *stack_context) {
  return PDB_ERR_MORE;
}

static int write_thaw(graphd_stack *stack,
                      graphd_stack_context *stack_context) {
  return EINVAL;
}

static int write_run(graphd_stack *, graphd_stack_context *);

/**
 * @brief write preparation step 4: post-unique
 *
 * @param stack		Stack we're running on
 * @param stack_context	Specific context
 */
static int write_x4_unique(graphd_stack *stack,
                           graphd_stack_context *stack_context) {
  graphd_write_context *gwc = (void *)stack_context;
  graphd_session *gses = graphd_request_session(gwc->gwc_greq);
  cl_handle *cl = gses->gses_cl;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");

  if (gwc->gwc_err != 0) {
    cl_assert(cl, gwc->gwc_err_out != NULL);
    *gwc->gwc_err_out = gwc->gwc_err;
    cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(gwc->gwc_err));

    if (gwc->gwc_err == GRAPHD_ERR_UNIQUE_EXISTS) {
      /*  This empties the stack as a side effect
       *  when graphd_request_error converts the
       *  request into a "pure" error.
       */
      graphd_request_error(gwc->gwc_greq,
                           "EXISTS primitive tagged as unique "
                           "already exist");
    }
    graphd_stack_pop(stack);
    return 0;
  }
  graphd_stack_resume(stack, stack_context, write_run);
  cl_leave(cl, CL_LEVEL_VERBOSE, "-> write_run");

  return 0;
}

/**
 * @brief write preparation step 3: key to unique.
 *
 * @param stack		Stack we're running on
 * @param stack_context	Specific context
 */
static int write_x3_key_to_unique(graphd_stack *stack,
                                  graphd_stack_context *stack_context) {
  graphd_write_context *gwc = (void *)stack_context;
  graphd_request *greq = gwc->gwc_greq;
  graphd_session *gses = graphd_request_session(gwc->gwc_greq);
  graphd_constraint *gcon = gwc->gwc_con;
  cl_handle *cl = gses->gses_cl;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");

  /*  Key postprocessing.
   */
  if (gwc->gwc_err != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(gwc->gwc_err));
    *gwc->gwc_err_out = gwc->gwc_err;
    graphd_stack_pop(stack);

    return 0;
  }

  /*  The  keys, whenever they matched, have been annotated
   *  with the GUIDs of their primitives.  Extend the matches
   *  across outgoing pointers into non-keyed pointer clusters.
   */
  gwc->gwc_err = graphd_write_annotate_pointed(greq, gcon);
  if (gwc->gwc_err != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(gwc->gwc_err));
    graphd_stack_pop(stack);
    return 0;
  }

  /*  Once the unique check completes, continue here.
   */
  graphd_stack_resume(stack, stack_context, write_x4_unique);

  /*  Push the unique checker.  It'll construct a unique
   *  constraint and match it against the database; if that
   *  fails, it'll set gwc->gwc_err to nonzero.
   */
  graphd_write_check_unique_push(greq, gcon, &gwc->gwc_err);
  if (gwc->gwc_err != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE,
             "unexpected error "
             "from graphd_write_check_unique_push: %s",
             graphd_strerror(gwc->gwc_err));
    graphd_stack_pop(stack);
  } else
    cl_leave(cl, CL_LEVEL_VERBOSE, "pushed");

  return 0;
}

/**
 * @brief write preparation step 2: anchor to key.
 *
 * @param stack		Stack we're running on
 * @param stack_context	Specific context
 */
static int write_x2_anchor_to_key(graphd_stack *stack,
                                  graphd_stack_context *stack_context) {
  graphd_write_context *gwc = (void *)stack_context;
  graphd_session *gses = graphd_request_session(gwc->gwc_greq);
  cl_handle *cl = gses->gses_cl;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");

  /*  Anchor postprocessing.
   */
  if (gwc->gwc_err != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(gwc->gwc_err));
    *gwc->gwc_err_out = gwc->gwc_err;
    graphd_stack_pop(stack);

    return 0;
  }

  graphd_stack_resume(stack, stack_context, write_x3_key_to_unique);

  /*  Push the key annotator.
   *
   *  Once we regain control on this level of the stack,
   *  constraints that have anhor clauses will be annotated
   *  with GUIDs.
   *
   *  (Unless an error occurred, and gwc->gwc_err is set.)
   */
  graphd_write_annotate_keyed_push(gwc->gwc_greq, gwc->gwc_con, &gwc->gwc_err);
  if (gwc->gwc_err != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE,
             "unexpected error "
             "from graphd_write_annotate_keyed_push: %s",
             graphd_strerror(gwc->gwc_err));
    graphd_stack_pop(stack);
    return 0;
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "-> write_x3_key_to_unique");
  return 0;
}

/**
 * @brief write preparation step 1: push the "anchor" evaluation.
 *
 * @param stack		Stack we're running on
 * @param stack_context	Specific context
 */
static int write_x1_anchor(graphd_stack *stack,
                           graphd_stack_context *stack_context) {
  graphd_write_context *gwc = (void *)stack_context;
  graphd_session *gses = graphd_request_session(gwc->gwc_greq);
  cl_handle *cl = gses->gses_cl;

  if (gwc->gwc_err != 0) {
    *gwc->gwc_err_out = gwc->gwc_err;
    graphd_stack_pop(stack);

    return 0;
  }

  /*  Once the anchor annotator completes, we'll continue
   *  with the key- and unique-checks.
   */
  graphd_stack_resume(stack, stack_context, write_x2_anchor_to_key);

  /*  Push the anchor annotator.
   *
   *  Once we regain control on this level of the stack,
   *  constraints that have anhor clauses will be annotated
   *  with GUIDs.
   *
   *  (Unless an error occurred, and gwc->gwc_err is set.)
   */
  graphd_write_annotate_anchor_push(gwc->gwc_greq, gwc->gwc_con, &gwc->gwc_err);
  if (gwc->gwc_err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_write_annotate_anchor_push",
                 gwc->gwc_err, "unexpected error");
    graphd_stack_pop(stack);
  }
  return 0;
}

/**
 * @brief write method: run
 *
 *  This gets executed once the first four steps perform
 *  anchoring, keying, and unique checks.
 *
 * @param stack		Stack we're running on
 * @param stack_context	Specific context
 */
static int write_run(graphd_stack *stack, graphd_stack_context *stack_context) {
  graphd_write_context *gwc = (void *)stack_context;
  graphd_request *greq = gwc->gwc_greq;
  graphd_session *gses = graphd_request_session(gwc->gwc_greq);
  graphd_constraint *gcon = gwc->gwc_con;
  graphd_handle *g = gses->gses_graphd;
  cl_handle *cl = gses->gses_cl;
  unsigned long long horizon;
  int err;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");
  if (gwc->gwc_err != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(gwc->gwc_err));
    *gwc->gwc_err_out = gwc->gwc_err;

    graphd_stack_pop(stack);
    return 0;
  }

  /*  Execute the (time-limited) code that actually
   *  writes constraints.
   */
  horizon = pdb_primitive_n(g->g_pdb);

  cl_assert(cl, gwc->gwc_val_out != NULL);
  err = graphd_write_constraint(greq, gcon, NULL, NULL, gwc->gwc_val_out);
  if (err != 0) {
    int rollback_err = graphd_checkpoint_rollback(g, horizon);
    if (rollback_err != 0) {
      char bigbuf[1024 * 8];
      char const *req_s;
      int req_n;
      bool incomplete;

      graphd_request_as_string(greq, bigbuf, sizeof bigbuf, &req_s, &req_n,
                               &incomplete);

      cl_log_errno(cl, CL_LEVEL_FATAL, "graphd_checkpoint_rollback",
                   rollback_err, "failed to roll back to horizon=%llx",
                   horizon);

      srv_epitaph_print(gses->gses_ses.ses_srv, EX_UNAVAILABLE,
                        "graphd: failed to roll back changes after "
                        "an error: session=%s (SID=%lu, RID=%lu), "
                        "error=\"%s\" (%d), "
                        "rollback error=\"%s\" (%d), "
                        "request: %.*s%s",
                        gses->gses_ses.ses_displayname, gses->gses_ses.ses_id,
                        greq->greq_req.req_id, graphd_strerror(err), err,
                        graphd_strerror(rollback_err), rollback_err, (int)req_n,
                        req_s, incomplete ? "..." : "");
      exit(EX_UNAVAILABLE);
    }

    cl_leave(cl, CL_LEVEL_DEBUG, "graphd_write_constraint fails: %s",
             strerror(err));

    graphd_stack_pop(stack);
    return 0;
  }

  if (!pdb_transactional(g->g_pdb)) {
    /* Call the much-shorter pdb_checkpoint_optional to
     * update the marker file if non-transactional
     */
    pdb_checkpoint_optional(g->g_pdb, 0);
  }

  /* Make sure pdb's indices will get flushed to disk.
   */
  if ((err = graphd_idle_install_checkpoint(g)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_idle_install_checkpoint", err,
                 "unexpected error");
    cl_leave(cl, CL_LEVEL_VERBOSE, "error %s", graphd_strerror(err));
    return err;
  }

  /* Return a result, if someone's waiting for one.
   */
  if (gwc->gwc_err_out) *gwc->gwc_err_out = gwc->gwc_err;
  if (gwc->gwc_val_out != NULL) {
    if (gwc->gwc_err != 0) graphd_value_finish(cl, gwc->gwc_val_out);

    /* Keep the resource delete function from clearing this. */
    gwc->gwc_val_out = NULL;
  }
  graphd_stack_pop(stack);
  cl_leave(cl, CL_LEVEL_VERBOSE, "done");

  return 0;
}

static const graphd_stack_type write_stack_type = {write_x1_anchor,
                                                   write_freeze, write_thaw};

/**
 * @brief Push a context on the stack that will write a constraint tree.
 *
 * @param greq		Request whose stack we're pushing on
 * @param con		Constraint caller wants to match.
 * @param val_out 	return a value tree here.
 * @param err_out 	return errors here.
 */
static void graphd_write_push(graphd_request *greq, graphd_constraint *con,
                              graphd_value *val_out, int *err_out) {
  graphd_handle *g = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  graphd_write_context *gwc;
  int err;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");
  cl_assert(cl, val_out != NULL);
  cl_assert(cl, err_out != NULL);

  *err_out = 0;
  graphd_value_initialize(val_out);

  err = graphd_defer_write(greq);
  if (err) {
    *err_out = err;

    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_defer_write", err,
                 "refusing to write while no disk is available");

    cl_leave(cl, CL_LEVEL_VERBOSE,
             "refusing to write while "
             "no disk is available: %s",
             strerror(err));

    return;
  }

  cl_assert(cl, pdb_disk_is_available(g->g_pdb));

  /*  If we're in the middle of delayed database updates
   *  and we urgently need to get a checkpoint done,
   *  try and get some checkpointing work done.
   */
  if (g->g_checkpoint_state != GRAPHD_CHECKPOINT_CURRENT &&
      pdb_checkpoint_urgent(g->g_pdb)) {
    cl_log(cl, CL_LEVEL_VERBOSE, "graphd_write_push: urgent checkpoint");
    err = graphd_checkpoint_optional(g);
    if (err && PDB_ERR_MORE != err) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_checkpoint_optional", err,
                   "refusing to write while the checkpoint "
                   "system is stalled");

      *err_out = err;
      cl_leave(cl, CL_LEVEL_VERBOSE,
               "refusing to write while "
               "the checkpoint system is stalled: %s",
               strerror(err));
      return;
    }
  }

  /*  Convert generational constraints to constants.
   */
  err = graphd_guid_constraint_convert(greq, greq->greq_constraint,
                                       false /* !is_read */);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_guid_constraint_convert", err,
                 "unexpected error");
    goto err;
  }

  /* Check whether the writes are legitimate.
   */
  if ((err = graphd_write_check(greq, con)) != 0) {
    cl_log_errno(cl, CL_LEVEL_DEBUG, "graphd_write_check", err,
                 "semantics error");

  err:
    *err_out = err;
    cl_leave(cl, CL_LEVEL_VERBOSE, "%s", err ? graphd_strerror(err) : "ok");
    return;
  }

  /*  Allocate a new write context.
   */
  if ((gwc = cm_talloc(cm, graphd_write_context, 1)) == NULL) {
    *err_out = ENOMEM;
    cl_leave(cl, CL_LEVEL_VERBOSE, "malloc fails");
    return;
  }
  memset(gwc, 0, sizeof(*gwc));

  gwc->gwc_greq = greq;
  gwc->gwc_con = con;
  gwc->gwc_err_out = err_out;
  gwc->gwc_val_out = val_out;

  /*  Push the context onto the runtime stack.
   */
  graphd_stack_push(&greq->greq_stack, &gwc->gwc_sc,
                    &write_context_resource_type, &write_stack_type);
  cl_leave(cl, CL_LEVEL_VERBOSE, "-> write_x1_anchor");

  return;
}

/**
 * @brief Run the request stack for a write request.
 *
 * @param greq		Request whose stack we're pushing on
 * @param deadline	If running beyond this, return.
 *
 * @return PDB_ERR_MORE	to continue later
 * @return 0		if the request is done.
 *
 */
static int graphd_write(graphd_request *greq, unsigned long long deadline) {
  graphd_handle *g = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  srv_handle *srv = g->g_srv;
  pdb_handle *pdb = g->g_pdb;
  char buf[200];
  int err;

  cl_enter(cl, CL_LEVEL_VERBOSE, "(%s)",
           greq->greq_req.req_session->ses_displayname);
  graphd_request_diary_log(greq, 0, "RUN");

  if ((err = graphd_smp_pause_for_write(greq))) return err;

  /*  Nothing on the stack?
   */
  if (graphd_stack_top(&greq->greq_stack) == NULL) {
    if (g->g_test_sleep_write || g->g_test_sleep_forever_write) {
      sleep(1);
      if (g->g_test_sleep_forever_write) return GRAPHD_ERR_MORE;
    }
    graphd_write_push(greq, greq->greq_constraint, &greq->greq_reply,
                      &greq->greq_reply_err);
    switch (greq->greq_reply_err) {
      case 0:
        /* in non-transactional mode, a crash while writing
         * will corrupt the database, so we cannot restart
         * safely
         */
        if (!pdb_transactional(pdb)) srv_shared_set_safe(srv, false);
        break;

      case PDB_ERR_MORE:
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_write_push",
                     greq->greq_reply_err, "can't run yet");
        cl_leave(cl, CL_LEVEL_VERBOSE, "can't run yet");
        greq->greq_reply_err = 0;
        return PDB_ERR_MORE;

      default:
        cl_leave(cl, CL_LEVEL_VERBOSE,
                 "error "
                 "from graphd_write_push: %s",
                 graphd_strerror(greq->greq_reply_err));
        return 0;
    }
  }

  err = graphd_stack_run_until_deadline(greq, &greq->greq_stack, deadline);

  if (!pdb_transactional(pdb) && err != PDB_ERR_MORE)
    srv_shared_set_safe(srv, true);

  if (err == 0) err = greq->greq_reply_err;

  cl_leave(cl, CL_LEVEL_VERBOSE, "%s",
           err == 0 ? graphd_value_to_string(&greq->greq_reply, buf, sizeof buf)
                    : (err == PDB_ERR_MORE ? "(to be continued...)"
                                           : graphd_strerror(err)));

  return err;
}

static int graphd_write_run(graphd_request *greq, unsigned long long deadline) {
  graphd_session *gses = graphd_request_session(greq);
  graphd_handle *g = graphd_request_graphd(greq);
  int err = 0;

  if (g->g_access == GRAPHD_ACCESS_REPLICA ||
      g->g_access == GRAPHD_ACCESS_REPLICA_SYNC) {
    gses->gses_last_action = "writethrough";

    /*  We're done running, but we won't be ready for
     *  output until the graphd_writethrough request
     *  says we are.
     */
    err = graphd_writethrough(greq);

    if (err != GRAPHD_ERR_MORE && err != GRAPHD_ERR_SUSPEND)
      srv_request_run_done(&greq->greq_req);

    else if (err == GRAPHD_ERR_SUSPEND) {
      srv_request_suspend(&greq->greq_req);
      err = GRAPHD_ERR_MORE;
    }
  } else {
    gses->gses_last_action = "write";
    err = graphd_write(greq, deadline);

    if (err == GRAPHD_ERR_MORE || err == GRAPHD_ERR_SUSPEND) {
      /* We're not ready yet.
       */
      if (err == GRAPHD_ERR_SUSPEND) srv_request_suspend(&greq->greq_req);

      err = GRAPHD_ERR_MORE;
    } else {
      if (err != 0)
        cl_log_errno(gses->gses_cl, CL_LEVEL_FAIL, "graphd_write", err,
                     "unexpected write error");

      /*  Even in the error case, we're ready to
       *  send a reply now.
       */
      graphd_request_served(greq);
    }
  }
  return err;
}

static graphd_request_type graphd_write_request = {
    "write",
    /* input-arrived */ NULL,
    /* output-sent 	 */ NULL,
    graphd_write_run,
    /* graphd_write_cancel */ NULL,
    /* graphd_set_free */ NULL};

int graphd_write_initialize(graphd_request *greq) {
  greq->greq_request = GRAPHD_REQUEST_WRITE;
  greq->greq_type = &graphd_write_request;

  return 0;
}
