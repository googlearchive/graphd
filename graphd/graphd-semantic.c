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

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>

/**
 * @brief Check a constraint subtree after it has been parsed
 *
 *  This is part of a node-first traversal over all constraints.
 *
 * @param greq	Request we're in the middle of parsing
 * @param con	Root of the subtree to check
 */
static void graphd_semantic_constraint_complete_subtree(
    graphd_request *greq, graphd_constraint *con) {
  graphd_constraint *sub;
  graphd_constraint const *sup;
  int err;
  cl_handle *const cl = graphd_request_cl(greq);
  graphd_assignment *a, *a2;
  unsigned long used_pattern = 0;
  graphd_variable_declaration *decl;
  char const *decl_s, *decl_e;

  cl_assert(cl, con != NULL);

  /*  Assign a unique number to each constraint.
   */
  if (con->con_parent == NULL) con->con_id = 1;

  if (con->con_unique != 0 && greq->greq_request != GRAPHD_REQUEST_WRITE) {
    cl_cover(cl);
    cl_assert(cl, greq->greq_request != 0);
    graphd_request_error(greq, "SYNTAX \"unique=\" only works with \"write\"");
    return;
  }
  if (con->con_key != 0 && greq->greq_request != GRAPHD_REQUEST_WRITE) {
    cl_cover(cl);
    graphd_request_error(greq, "SYNTAX \"key=\" only works with \"write\"");
    return;
  }

  /*  (Re)-number "or" subconstraints.
   */
  graphd_constraint_or_index(greq, con, 0);

  /*  If a variable is used in a result expression,
   *  it must be set by the constraint or a contained constraint.
   */
  decl = NULL;
  while ((decl = graphd_variable_declaration_next(con, decl)) != NULL) {
    if (graphd_assignment_by_declaration(con, decl) != NULL) continue;

    graphd_variable_declaration_name(decl, &decl_s, &decl_e);

    /*  This variable is used, but never directly assigned
     *  to in this context.
     *  We must get its value from below.
     */
    if (!graphd_variable_is_assigned_in_or_below(cl, con, decl_s, decl_e)) {
      cl_cover(cl);

      graphd_request_errprintf(greq, 0,
                               "SYNTAX variable %.*s is returned, but not "
                               "set in the constraint or any subconstraint",
                               (int)(decl_e - decl_s), decl_s);

      greq->greq_error_token.tkn_start = NULL;
      greq->greq_error_token.tkn_end = NULL;

      return;
    }
  }

  /*
   *  - Conversely, if a variable is assigned to,
   *    it must be used elsewhere in the constraint
   *    (then it's an alias) or in a containing constraint.
   */
  for (a = con->con_assignment_head; a != NULL; a = a->a_next) {
    size_t var_i;
    graphd_constraint *c2;
    char const *name_s, *name_e;

    if (a->a_declaration->vdecl_constraint != con) continue;

    graphd_variable_declaration_name(a->a_declaration, &name_s, &name_e);

    for (sup = con; sup != NULL; sup = sup->con_parent) {
      graphd_constraint *or_root;

      or_root = graphd_constraint_or_prototype_root(sup);
      if (graphd_variable_is_used(cl, or_root, name_s, name_e, &var_i)) {
        cl_cover(cl);
        break;
      }
    }

    if (sup == NULL) {
      cl_cover(cl);

      graphd_request_errprintf(greq, 0,
                               "SYNTAX variable %.*s is assigned, but not "
                               "returned in this or any containing "
                               "constraint",
                               (int)(name_e - name_s), name_s);

      greq->greq_error_token.tkn_start = NULL;
      greq->greq_error_token.tkn_end = NULL;

      return;
    }

    /* - This variable must not be set again in the
     *     same constraint.
    */
    for (a2 = a->a_next; a2 != NULL; a2 = a2->a_next) {
      if (a->a_declaration == a2->a_declaration) {
        cl_cover(cl);
        graphd_request_errprintf(greq, 0,
                                 "SYNTAX variable %.*s is assigned "
                                 "to twice",
                                 (int)(name_e - name_s), name_s);

        greq->greq_error_token.tkn_start = NULL;
        greq->greq_error_token.tkn_end = NULL;

        return;
      }
    }

    /* - The variable must not be set again in any
     *    containing constraint, either.
     */
    for (c2 = con->con_parent; c2 != NULL; c2 = c2->con_parent)
      if (graphd_assignment_by_name(c2, name_s, name_e)) {
        cl_cover(cl);
        graphd_request_errprintf(greq, 0,
                                 "SYNTAX variable %.*s is assigned "
                                 "to twice in nested constraints",
                                 (int)(name_e - name_s), name_s);

        greq->greq_error_token.tkn_start = NULL;
        greq->greq_error_token.tkn_end = NULL;

        return;
      }

    cl_cover(cl);
  }

  /*  - A variable must not be assigned to itself, or to another
   *    variable that is assigned to it through some number of
   *    steps.  That is, you can't write:
   *
   *	$a=($b), $b=($a)
   */
  for (a = con->con_assignment_head; a != NULL; a = a->a_next) {
    if (graphd_assignment_is_recursive(cl, con, a)) {
      char const *name_s, *name_e;

      graphd_variable_declaration_name(a->a_declaration, &name_s, &name_e);

      cl_cover(cl);
      graphd_request_errprintf(greq, 0,
                               "SYNTAX circular assignment of %.*s to itself",
                               (int)(name_e - name_s), name_s);

      greq->greq_error_token.tkn_start = NULL;
      greq->greq_error_token.tkn_end = NULL;

      return;
    }
  }

  /* The constraint is resumable if
   *
   *	- there's a soft timeout on the request
   *	- the constraint pattern uses a timeout or a cursor.
   */
  if (greq->greq_soft_timeout &&
      used_pattern &
          ((1 << GRAPHD_PATTERN_TIMEOUT) | (1 << GRAPHD_PATTERN_CURSOR)))
    con->con_resumable = true;

  /* - If we have a parent constraint, we know how we relate to it.
   */
  if (con->con_parent != NULL && !con->con_linkage) {
    cl_cover(cl);
    graphd_request_errprintf(greq, 0,
                             "SEMANTICS don't know"
                             " how to connect these nested constraints");
    return;
  }

  /* - If we have a linkage that talks about our parent,
   *   we must actually have a parent!
   */
  if (graphd_linkage_is_my(con->con_linkage) && con->con_parent == NULL) {
    char const *str =
        pdb_linkage_to_string(graphd_linkage_my(con->con_linkage));
    cl_cover(cl);
    graphd_request_errprintf(greq, 0,
                             "SEMANTICS can't use (<-%s ..) on the outermost "
                             "constraint - do you mean %s=GUID?",
                             str, str);
    return;
  }

  /* - If we have a "unique" constraint, we must have the
   *   corresponding values in the write request.
   */
  if (con->con_unique != 0)
    if (graphd_unique_parse_check(greq, con, con->con_unique)) return;

  /* - If we have a "key" constraint, we must have the
   *   corresponding values in the write request.
   */
  if (con->con_key != 0)
    if (graphd_key_parse_check(greq, con, con->con_key)) return;

  /* - If we have a key constraint, we mustn't have a ~=
   *   constraint.
   */
  if (con->con_key != 0 && con->con_guid.guidcon_match_valid) {
    graphd_request_error(greq, "SEMANTICS cannot mix key and ~= constraints");
    return;
  }

  /* - If "contents" aren't used in the result, in sorting, or in
   *   any assignments, the results of all subconstraints can
   *   be forced to empty.
   */
  if (con->con_result == NULL) {
    if (greq->greq_request == GRAPHD_REQUEST_READ ||
        greq->greq_request == GRAPHD_REQUEST_ITERATE)
      con->con_result = (graphd_pattern *)graphd_pattern_read_default();
    else if (greq->greq_request == GRAPHD_REQUEST_WRITE)
      con->con_result = (graphd_pattern *)graphd_pattern_write_default();
  }

  con->con_uses_contents = graphd_constraint_uses_contents(con);

  /*  If our parent points to us with its linkage, there's
   *  at most one of us per parent.  Reduce the pagesize to 1.
   *  That'll let the iterators terminate instead of deferring
   *  themselves unnecessarily.
   */
  if (graphd_linkage_is_i_am(con->con_linkage)) {
    if (con->con_pagesize_valid) {
      if (con->con_pagesize > 1) con->con_pagesize = 1;
    } else {
      con->con_pagesize_valid = true;
      con->con_pagesize = 1;
    }

    if (con->con_countlimit_valid && con->con_countlimit > 1)
      con->con_countlimit = 1;

    if (con->con_resultpagesize_parsed_valid &&
        con->con_resultpagesize_parsed > 1)
      con->con_resultpagesize_parsed = 1;

    if (con->con_resultpagesize_valid && con->con_resultpagesize > 1)
      con->con_resultpagesize = 1;
  }

  /*  countlimit and resultpagesize default to
   *  the pagesize.
   */
  if (con->con_pagesize_valid) {
    if (!con->con_countlimit_valid) {
      con->con_countlimit = con->con_start + con->con_pagesize;
      con->con_countlimit_valid = true;
    }
    if (!con->con_resultpagesize_parsed_valid) {
      con->con_resultpagesize_parsed = con->con_pagesize;
      con->con_resultpagesize_parsed_valid = true;
    }
  }

  /*  The result page size defaults to a thousand; if explicitly
   *  specified, it maxes out at 64k.
   *  The count page size is unlimited.
   */
  if (!con->con_resultpagesize_parsed_valid) {
    con->con_resultpagesize_parsed_valid = true;
    con->con_resultpagesize_parsed = GRAPHD_RESULT_PAGE_SIZE_DEFAULT;
  }
  if (con->con_resultpagesize_parsed > GRAPHD_RESULT_PAGE_SIZE_MAX)
    con->con_resultpagesize_parsed = GRAPHD_RESULT_PAGE_SIZE_MAX;

  if (!con->con_resultpagesize_valid) {
    con->con_resultpagesize_valid = true;
    con->con_resultpagesize = GRAPHD_RESULT_PAGE_SIZE_DEFAULT;
  }
  if (con->con_resultpagesize > GRAPHD_RESULT_PAGE_SIZE_MAX)
    con->con_resultpagesize = GRAPHD_RESULT_PAGE_SIZE_MAX;

  /*  If our parent points to us with its linkage, there's
   *  at most one of us per parent.  Reduce the pagesize to 1.
   *  That'll let the iterators terminate instead of deferring
   *  themselves unnecessarily.
   */
  if (graphd_linkage_is_i_am(con->con_linkage)) {
    if (con->con_pagesize_valid) {
      if (con->con_pagesize > 1) con->con_pagesize = 1;
    } else {
      con->con_pagesize_valid = true;
      con->con_pagesize = 1;
    }

    if (con->con_countlimit_valid && con->con_countlimit > 1)
      con->con_countlimit = 1;

    if (con->con_resultpagesize_parsed_valid &&
        con->con_resultpagesize_parsed > 1)
      con->con_resultpagesize_parsed = 1;

    if (con->con_resultpagesize_valid) {
      if (con->con_resultpagesize > 1) con->con_resultpagesize = 1;
    } else {
      con->con_resultpagesize = 1;
      con->con_resultpagesize_valid = true;
    }
  }

  /*  Compile the sort query.
   */
  if ((err = graphd_sort_compile(greq, con)) != 0) {
    graphd_request_errprintf(greq, 0, "SYSTEM sort compilation fails: %s",
                             strerror(err));
    return;
  }

  /* Mark a subtree as usable in a cursor.
   */
  graphd_constraint_cursor_mark_usable(greq, con);

  /* Assign a unique number to each constraint, part I
   */
  con->con_id = (con->con_parent != NULL ? con->con_parent->con_id : 1);

  /* Recurse. */
  for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
    /*  Force the subconstraint's results to empty.
     */
    if (!con->con_uses_contents) {
      /*  Force the subconstraint's result to NULL.
       */
      sub->con_result = graphd_pattern_empty();
    }

    graphd_semantic_constraint_complete_subtree(greq, sub);
    if (graphd_request_has_error(greq)) return;

    /* Assign a unique number to each constraint, part II
     */
    con->con_id = sub->con_id + 1;
  }

  con->con_title = NULL;

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_semantic_constraint_complete_subtree: done: %s",
         graphd_constraint_to_string(con));
  graphd_assignment_dump(greq, con);
}

/*  Called after all constraints of an expression are fully parsed.
 */
void graphd_semantic_constraint_complete(graphd_request *greq,
                                         graphd_constraint *con) {
  int err;
  cl_handle *const cl = graphd_request_cl(greq);

  if (graphd_request_has_error(greq)) return;

  cl_assert(cl, con != NULL);

  /* For each subtree, recursively ...
   */
  graphd_semantic_constraint_complete_subtree(greq, con);
  if (graphd_request_has_error(greq)) return;

  /*  Just for the root of a read or iterate request.
   */
  if (con->con_parent == NULL &&
      (greq->greq_request == GRAPHD_REQUEST_READ ||
       greq->greq_request == GRAPHD_REQUEST_ITERATE)) {
    if ((err = graphd_variable_analysis(greq)) != 0) {
      graphd_request_errprintf(greq, 0,
                               "SYSTEM semantic variable analysis fails: %s",
                               strerror(err));
      return;
    }
  }
  con->con_title = NULL;
}

/*
 * Copy comparators from the con_sort_comparators array into the
 * sort pattern
 */
static int annotate_sort_comparators(graphd_request *greq,
                                     graphd_constraint *con) {
  graphd_comparator_list *clist;
  cl_handle *const cl = graphd_request_cl(greq);

  const graphd_comparator *cmp;
  graphd_pattern *srpat;
  int i = 0;

  clist = &con->con_sort_comparators;
  srpat = con->con_sort_valid ? con->con_sort : NULL;

  /*
   * No sort? No sort-comparator!
   */
  if (!srpat) {
    if (clist->gcl_n) {
      /*
       * If you don't have a sort, you can't specify a
       * sort-comparator
       */
      graphd_request_errprintf(greq, 0,
                               "SEMANTICS sortcomparators with no sort");
      return GRAPHD_ERR_SEMANTICS;
    }

    /*
     * Nothing to do
     */
    return 0;
  }

  if (srpat->pat_type == GRAPHD_PATTERN_LIST) srpat = srpat->pat_list_head;

  for (i = 0; i < clist->gcl_n; i++) {
    if (!srpat) {
      graphd_request_errprintf(greq, 0,
                               "SEMANTICS more sort comparators than sorts");
      return GRAPHD_ERR_SEMANTICS;
    }

    cmp = clist->gcl_comp[i];

    cl_assert(cl, cmp);

    cl_log(cl, CL_LEVEL_VERBOSE, "assigned comparator %s to pat %p",
           graphd_comparator_to_string(cmp), srpat);

    srpat->pat_comparator = cmp;
    srpat = srpat->pat_next;
  }

  /*
   * Fill extra patterns with the clause's 'comparator'
   */

  for (; srpat; srpat = srpat->pat_next) {
    cl_log(graphd_request_cl(greq), CL_LEVEL_VERBOSE,
           "annotate_sort_comparators: defaulting %p for %p",
           con->con_comparator, srpat);
    srpat->pat_comparator = con->con_comparator;
  }

  return 0;
}

/* If someone wrote <= ("a" "b"), that's the same as
 * just <= "a".
 *
 * We can't do that transformation at parse-time
 * because its details depend on the value-comparator.
 */
static void truncate_strcon_range_boundaries(
    graphd_request *greq, graphd_constraint *con,
    graphd_string_constraint_queue *q) {
  graphd_string_constraint *strcon;

  for (strcon = q->strqueue_head; strcon != NULL;
       strcon = strcon->strcon_next) {
    int which;

    /* if the set contains more than one element
     * and the operator is `>=' or `>', then keep
     * only the smallest member in the set; if the
     * operator is `<' or `<=', then keep only the
     * largest.
     */
    /* At most one element?
     */
    if (strcon->strcon_head == NULL || strcon->strcon_head->strcel_next == NULL)
      continue;

    switch (strcon->strcon_op) {
      case GRAPHD_OP_LT:
      case GRAPHD_OP_LE:
        which = -1;
        break;
      case GRAPHD_OP_GT:
      case GRAPHD_OP_GE:
        which = 1;
        break;
      default:
        continue;
    }

    strcon->strcon_head =
        graphd_string_constraint_pick(greq, con, strcon, which);

    /* truncate the list to just that element */
    if (strcon->strcon_head != NULL) {
      strcon->strcon_head->strcel_next = NULL;
      strcon->strcon_tail = &strcon->strcon_head->strcel_next;
    }
  }
}

/*  If a node is marked anchored=true, its anchored=unspecified
 *  subtree is also anchored=true.  (This can be interrupted by
 *  anchored=false or anchored=local.)
 */
static void anchor_subtree(graphd_constraint *con) {
  graphd_constraint *sub;
  for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
    if (sub->con_anchor == GRAPHD_FLAG_UNSPECIFIED) {
      sub->con_anchor = GRAPHD_FLAG_TRUE_LOCAL;
      anchor_subtree(sub);
    }
  }
}

/*  If an anchored (true or true-inferred) node points to another,
 *  that other becomes inferred-anchored.
 */
static void anchor_infer(graphd_request *greq, graphd_constraint *con) {
  graphd_constraint *sub, *par;

  if (con->con_anchor == GRAPHD_FLAG_TRUE ||
      con->con_anchor == GRAPHD_FLAG_TRUE_LOCAL) {
    if (graphd_linkage_is_my(con->con_linkage) &&
        (par = con->con_parent) != NULL) {
      /*  I am pointing to my parent.  Since I am
       *  anchored, my parent must be anchor=true
       *  or anchor=true-local.
       */
      switch (par->con_anchor) {
        case GRAPHD_FLAG_TRUE:
        case GRAPHD_FLAG_TRUE_LOCAL:
          break;

        case GRAPHD_FLAG_FALSE:
          graphd_request_errprintf(greq, 0,
                                   "SEMANTICS an anchored "
                                   "constraint cannot point to "
                                   "an unanchored one.");
          return;

        case GRAPHD_FLAG_UNSPECIFIED:
          par->con_anchor = GRAPHD_FLAG_TRUE_LOCAL;
          anchor_infer(greq, par);
          break;

        default:
          cl_notreached(graphd_request_cl(greq),
                        "anchor_infer: unexpected "
                        "flag value %d",
                        par->con_anchor);
      }
    }

    for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
      if (graphd_linkage_is_i_am(sub->con_linkage)) {
        /*  The parent, con, is pointing to sub.
         */
        switch (sub->con_anchor) {
          case GRAPHD_FLAG_TRUE:
          case GRAPHD_FLAG_TRUE_LOCAL:
            break;

          case GRAPHD_FLAG_FALSE:
            graphd_request_errprintf(greq, 0,
                                     "SEMANTICS an anchored "
                                     "constraint cannot point to "
                                     "an unanchored one.");
            return;

          case GRAPHD_FLAG_UNSPECIFIED:
            sub->con_anchor = GRAPHD_FLAG_TRUE_LOCAL;
            anchor_infer(greq, sub);
            break;

          default:
            cl_notreached(graphd_request_cl(greq),
                          "anchor_infer: unexpected "
                          "flag value %d",
                          sub->con_anchor);
        }
      }
    }
  }
  for (sub = con->con_head; sub != NULL; sub = sub->con_next)
    anchor_infer(greq, sub);
}

/*  Called from within the parser after a constraint and all its
 *  subconstraints have completed a parse.
 *
 *  The constraint may be an or-branch or a toplevel.
 */
void graphd_semantic_constraint_complete_parse(graphd_request *greq,
                                               graphd_constraint *con) {
  graphd_constraint_or *cor;
  cl_handle *cl = graphd_request_cl(greq);
  graphd_string_constraint *cur;
  int err;

  /*  Merge the parsed subclauses into the semantic constraint
   */
  err = graphd_constraint_clause_merge_all(greq, con);
  if (err != 0) {
    graphd_request_error(greq, "SEMANTIC error merging subclause");
    return;
  }

  /*  Check the result instruction.
   */
  if (con->con_result &&
      (err = graphd_constraint_use_result_instruction(greq, con,
                                                      con->con_result)) != 0) {
    cl_cover(cl);
    return;
  }

  if ((con->con_guid.guidcon_match_valid ||
       con->con_guid.guidcon_include_valid) &&
      con->con_key != 0) {
    cl_cover(cl);
    graphd_request_error(greq,
                         "SYNTAX cannot mix \"key=\" and \"guid~=\" "
                         "constraints - did you mean \"unique\"?");
    return;
  }
  if (con->con_unique != 0 && con->con_key != 0) {
    cl_cover(cl);
    graphd_request_error(greq,
                         "SYNTAX cannot mix \"key=\" and \"unique=\" "
                         "constraints");
    return;
  }

  /* Infer anchorage of subtrees.
   */
  if (con->con_anchor == GRAPHD_FLAG_TRUE) anchor_subtree(con);

  /* Only for the topmost node...
   */
  if (con->con_parent == NULL) anchor_infer(greq, con);

  if (con->con_meta == GRAPHD_META_LINK_FROM) {
    graphd_constraint *sub;

    /*  "->" without accompanying keyword means:
     *
     *	If I have a parent and no linkage constraint,
     *  	my parent is my left side.
     *
     * 	If my children have no linkage constraint,
     *	they are my right side.
     */

    if (con->con_linkage == 0 && con->con_parent != NULL &&
        !(graphd_constraint_linkage_pattern(con) &
          (1 << GRAPHD_PATTERN_LINKAGE(PDB_LINKAGE_LEFT))))
      con->con_linkage = graphd_linkage_make_my(PDB_LINKAGE_LEFT);

    if (!(graphd_constraint_linkage_pattern(con) &
          (1 << GRAPHD_PATTERN_LINKAGE(PDB_LINKAGE_RIGHT))))

      for (sub = con->con_head; sub != NULL; sub = sub->con_next)

        if (sub->con_linkage == 0) {
          sub->con_linkage = graphd_linkage_make_i_am(PDB_LINKAGE_RIGHT);
          break;
        }
  } else if (con->con_meta == GRAPHD_META_LINK_TO) {
    graphd_constraint *sub;

    /*  "<-" without accompanying keyword means:
     *
     *	If I have no linkage constraint, my parent is
     *	my right side.
     *
     * 	If my children have no linkage constraint,
     *	they are my left side.
     */

    if (con->con_linkage == 0 && con->con_parent != NULL &&
        !(graphd_constraint_linkage_pattern(con) &
          (1 << GRAPHD_PATTERN_LINKAGE(PDB_LINKAGE_RIGHT))))
      con->con_linkage = graphd_linkage_make_my(PDB_LINKAGE_RIGHT);

    if (!(graphd_constraint_linkage_pattern(con) &
          (1 << GRAPHD_PATTERN_LINKAGE(PDB_LINKAGE_LEFT))))

      for (sub = con->con_head; sub != NULL; sub = sub->con_next)

        if (sub->con_linkage == 0) {
          sub->con_linkage = graphd_linkage_make_i_am(PDB_LINKAGE_LEFT);
          break;
        }
  }

  if (con->con_archival == GRAPHD_FLAG_UNSPECIFIED) {
    cl_cover(cl);
    con->con_archival = GRAPHD_FLAG_DONTCARE;
  }
  if (con->con_live == GRAPHD_FLAG_UNSPECIFIED) {
    cl_cover(cl);
    con->con_live = GRAPHD_FLAG_TRUE;
  }

  if (!con->con_count.countcon_min_valid) {
    cl_cover(cl);
    con->con_count.countcon_min = con->con_start + 1;
  }

  /*  If there are guidcons with ~= or = and zero
   *  matching GUIDs, mark the whole constraint as
   *  false (impossible to satisfy).
   */
  if ((con->con_guid.guidcon_include_valid &&
       con->con_guid.guidcon_include.gs_n == 0) ||
      (con->con_guid.guidcon_match_valid &&
       con->con_guid.guidcon_match.gs_n == 0)) {
    cl_log(cl, CL_LEVEL_DEBUG, "FALSE [%s:%d] GUID must be NULL", __FILE__,
           __LINE__);

    con->con_false = true;
    con->con_error =
        "SEMANTICS GUID constraints are impossible "
        "to satisfy";
  }

  /*
   * If you don't have a comparator,
   * default to graphd_comparator_unspecified, this is important because
   * the sort_root code assumes that it can change your comparator
   * to match the sort root if your comparator is unspecified.
   */
  cl_assert(cl, con->con_comparator != NULL);
  if (con->con_value_comparator == NULL)
    con->con_value_comparator = con->con_comparator;

  for (cur = con->con_value.strqueue_head; cur; cur = cur->strcon_next) {
    err = con->con_value_comparator->cmp_syntax(greq, cur);
    if (err != 0) {
      /*
       * If you're going to complaint, you'd better
       * tell us why and mark the query is broken.
       */
      cl_assert(cl, greq->greq_error_message);
      return;
    }
  }

  /*  Now that we have a value comparator,
   *  remove duplicate boundaries from ranges.
   */
  truncate_strcon_range_boundaries(greq, con, &con->con_type);
  truncate_strcon_range_boundaries(greq, con, &con->con_name);
  truncate_strcon_range_boundaries(greq, con, &con->con_value);

  /*
   * Annotate sorts with the comparator for the sort
   */
  err = annotate_sort_comparators(greq, con);
  if (err != 0) cl_assert(cl, greq->greq_error_message);

  /*  Update dependent "or"s.
   */
  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    err = graphd_constraint_or_complete_parse(greq, con, &cor->or_head);
    if (err != 0) {
      cl_assert(cl, greq->greq_error_message != NULL);
      return;
    }
    if (cor->or_tail != NULL) {
      err = graphd_constraint_or_complete_parse(greq, con, cor->or_tail);
      if (err != 0) {
        cl_assert(cl, greq->greq_error_message != NULL);
        return;
      }
    }
  }
}
