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
#include "graphd/graphd-hash.h"

#include <errno.h>
#include <string.h>

/* Find the assignment to [s..e[ in con.
 */
graphd_assignment* graphd_assignment_by_name(graphd_constraint const* con,
                                             char const* s, char const* e) {
  return graphd_assignment_by_declaration(
      con, graphd_variable_declaration_by_name(con, s, e));
}

graphd_assignment* graphd_assignment_by_declaration(
    graphd_constraint const* con, graphd_variable_declaration const* vdecl) {
  graphd_assignment* a;

  if (vdecl == NULL) return NULL;

  for (a = con->con_assignment_head; a; a = a->a_next)
    if (a->a_declaration == vdecl) return a;
  return NULL;
}

/*  Test whether an assignment is recursive - via any number of nested
 *  steps - by recursing more times than we have variables in an attempt
 *  to traverse the full depth of the assignment tree.
 *
 * @param cl 	Log through this
 * @param con	The constraint we're talking about
 * @param var_s	first byte of the variable name
 * @param var_e last byte of the variable name
 * @param depth	Initially, the total number of assignments.  If we
 *		end up traversing more than <depth> links, we know
 *		we've run in circles.
 */
static bool assignment_is_recursive(
    cl_handle* const cl, graphd_constraint const* const con,
    graphd_variable_declaration const* const decl, size_t const depth) {
  graphd_assignment const* a;
  graphd_pattern const* pat;

  if (depth <= 0) return true;

  a = graphd_assignment_by_declaration(con, decl);
  if (a == NULL) return false;

  /*  For all patterns on the right hand side of this
   *  assignments, are they recursive?
   */
  for (pat = a->a_result; pat != NULL;
       pat = graphd_pattern_preorder_next(pat)) {
    if (pat->pat_type == GRAPHD_PATTERN_VARIABLE &&
        pat->pat_variable_declaration->vdecl_constraint == con) {
      if (assignment_is_recursive(cl, con, pat->pat_variable_declaration,
                                  depth - 1))

        return true;
    }
  }
  return false;
}

/**
 * @brief Does this assignment's left hand side refer to itself?
 *
 * @param cl		Log through this
 * @param con		constraint we're asking about
 * @param a		assignment within that constraint we're asking about
 *
 * @return true if the variable is returned, false if not.
 */
bool graphd_assignment_is_recursive(cl_handle* const cl,
                                    graphd_constraint const* const con,
                                    graphd_assignment const* const a) {
  graphd_pattern const* pat;

  if (a == NULL) return false;

  /*  For all variables on this assignment's right hand side
   *  that are set in this constraint...
   */
  for (pat = a->a_result; pat != NULL;
       pat = graphd_pattern_preorder_next(pat)) {
    if (pat->pat_type == GRAPHD_PATTERN_VARIABLE &&
        pat->pat_variable_declaration->vdecl_constraint == con) {
      /*  Do a depth-first traversal across
       *  assignment links.  If that goes deeper
       *  than con->con_assignment_n steps,
       *  we've got a loop.
       */
      if (assignment_is_recursive(cl, con, pat->pat_variable_declaration,
                                  con->con_assignment_n))

        return true;
    }
  }
  return false;
}

/**
 * @brief Hash an assignment chain.
 *
 * @param cl		Log through this
 * @param a		assignment chain to hash
 * @param hash_inout	hash accumulator
 */
void graphd_assignments_hash(cl_handle* const cl, graphd_assignment const* a,
                             unsigned long* const hash_inout) {
  for (; a != NULL; a = a->a_next) {
    char const *s, *e;

    graphd_variable_declaration_name(a->a_declaration, &s, &e);
    GRAPHD_HASH_BYTES(*hash_inout, s, e);
  }
}

/**
 * @brief Are two assignment chains equal?
 *
 *  False negatives are okay.
 *
 * @param cl		Log through this
 * @param a		one assignment chain
 * @param b		another assignment chain
 *
 * @return true if they're equal, false if they may not be equal.
 */
bool graphd_assignments_equal(cl_handle* const cl,
                              graphd_constraint const* a_con,
                              graphd_assignment const* a,
                              graphd_constraint const* b_con,
                              graphd_assignment const* b) {
  for (; a != NULL && b != NULL; a = a->a_next, b = b->a_next) {
    if (!graphd_variable_declaration_equal(cl, a_con, a->a_declaration, b_con,
                                           b->a_declaration))
      return false;

    if (!graphd_pattern_equal(cl, a_con, a->a_result, b_con, b->a_result))
      return false;
  }
  return a == NULL && b == NULL;
}

/**
 * @brief Allocate a variable assignment
 *
 *  The variable is used in a constraint or one of its containing
 *  outer constraints, and assigned within the constraint or
 *  within a contained, inner constraint.
 *
 * @param greq  the read request whose constraints contained the assignment
 * @param con 	the constraint itself
 * @param s 	NULL or a pointer to the beginning of the variable name
 * @param e 	NULL or a pointer to the end of the variable name
 *
 * @return NULL no allocation error, otherwise a pointer to an
 *	assignment structure
 */
graphd_assignment* graphd_assignment_alloc_declaration(
    graphd_request* greq, graphd_constraint* con,
    graphd_variable_declaration* vdecl) {
  cl_handle* cl = graphd_request_cl(greq);
  cm_handle* cm = greq->greq_req.req_cm;
  graphd_assignment* a;

  cl_assert(cl, con != NULL);
  cl_assert(cl, con->con_assignment_tail != NULL);
  cl_assert(cl, greq != NULL);

  /*  Connect the declaration to the result-side.
   */
  a = cm_zalloc(cm, sizeof(*a));
  if (a == NULL) return a;

  a->a_result = NULL;
  a->a_next = NULL;
  a->a_declaration = vdecl;

  *con->con_assignment_tail = a;
  con->con_assignment_tail = &a->a_next;
  con->con_assignment_n++;

  /*  Flush the constraint's string cache.
   */
  con->con_title = NULL;

  return a;
}

/**
 * @brief Allocate a variable assignment
 *
 *  The variable is used in a constraint or one of its containing
 *  outer constraints, and assigned within the constraint or
 *  within a contained, inner constraint.
 *
 * @param greq  the read request whose constraints contained the assignment
 * @param con 	the constraint itself
 * @param s 	NULL or a pointer to the beginning of the variable name
 * @param e 	NULL or a pointer to the end of the variable name
 *
 * @return NULL no allocation error, otherwise a pointer to an
 *	assignment structure
 */
graphd_assignment* graphd_assignment_alloc(graphd_request* greq,
                                           graphd_constraint* con,
                                           char const* s, char const* e) {
  cl_handle* cl = graphd_request_cl(greq);
  cm_handle* cm = greq->greq_req.req_cm;
  graphd_variable_declaration* vdecl;

  cl_assert(cl, con != NULL);
  cl_assert(cl, con->con_assignment_tail != NULL);
  cl_assert(cl, greq != NULL);
  cl_assert(cl, (s == NULL) == (e == NULL));

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_assignment_alloc to %.*s in %s",
         (int)(e - s), s, graphd_constraint_to_string(con));

  /*  Make or access a local declaration for the variable.
   */
  vdecl = graphd_variable_declaration_add(cm, cl, con, s, e);
  if (vdecl == NULL) return NULL;

  return graphd_assignment_alloc_declaration(greq, con, vdecl);
}

static bool variables_are_assigned_to(graphd_pattern const* pat,
                                      graphd_assignment const* a) {
  if (GRAPHD_PATTERN_IS_COMPOUND(pat->pat_type)) {
    for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next)
      if (variables_are_assigned_to(pat, a)) return true;
    return false;
  }

  if (pat->pat_type != GRAPHD_PATTERN_VARIABLE) return false;

  /*  Do any of the trailing assignmnets in <a> assign to <pat>?
   */
  for (; a != NULL; a = a->a_next) {
    if (a->a_declaration == pat->pat_variable_declaration) return true;
  }
  return false;
}

/*  Sort the set of assignments within <con> such that
 *  any assignment that *uses* a variable happens *after*
 *  an assignment *to* the variable.
 */
int graphd_assignment_sort(graphd_request* greq, graphd_constraint* con) {
  graphd_assignment** insertion_point;
  graphd_assignment** a;

  a = insertion_point = &con->con_assignment_head;
  while (*a != NULL) {
    graphd_assignment* tmp;

    if (variables_are_assigned_to((*a)->a_result, *insertion_point)) {
      /* We can't move this one yet. */
      a = &(*a)->a_next;
      continue;
    }

    /*  If none of the variables used by
     *  (*a)->a_result are assigned to after the
     *  insertion point, <a> can be added to
     *  the list of assignable variables before
     *  the insertion point.
     */

    /* Chain out *a.
     */
    tmp = *a;
    *a = tmp->a_next;
    if (*a == NULL) con->con_assignment_tail = a;

    /*  Move the former *a (now "tmp") into
     *  the insertion point.
     */
    tmp->a_next = *insertion_point;
    *insertion_point = tmp;
    insertion_point = &tmp->a_next;
    if (tmp->a_next == NULL) con->con_assignment_tail = &tmp->a_next;

    /*  Start over at the insertion point.
     */
    a = insertion_point;
  }

  /*  We ran out of things to do.  If that's because we
   *  did something to all the assignments, we're good.
   *  Otherwise, we have a loop.
   */
  if (a != insertion_point) {
    char const *name_s, *name_e;

    /* There's a loop.
     */
    graphd_variable_declaration_name((*insertion_point)->a_declaration, &name_s,
                                     &name_e);

    graphd_request_errprintf(greq, false,
                             "SEMANTICS loop in variable assignments "
                             "to/from %.*s",
                             (int)(name_e - name_s), name_s);
    return GRAPHD_ERR_SEMANTICS;
  }

  /*  Reset the constraint string cache.
   */
  con->con_title = NULL;
  return 0;
}

static void clear_marks_below(graphd_pattern* pat) {
  for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next) {
    pat->pat_sample = false;
    pat->pat_collect = false;

    if (GRAPHD_PATTERN_IS_COMPOUND(pat->pat_type)) clear_marks_below(pat);
  }
}

static void mark_pattern(graphd_request* greq, graphd_constraint* con,
                         graphd_pattern* pat, size_t depth) {
  graphd_pattern* sub;
  cl_handle* cl = graphd_request_cl(greq);

  /*  Broken?
   */
  if (depth > 2) return;

  if (depth == 1 && pat->pat_type == GRAPHD_PATTERN_LIST &&
      (pat->pat_parent == NULL ||
       pat->pat_parent->pat_type != GRAPHD_PATTERN_PICK)) {
    pat->pat_collect = true;
    clear_marks_below(pat);
  }

  if (depth <= 1 && pat->pat_type != GRAPHD_PATTERN_LIST) {
    pat->pat_collect = false;
    if (pat->pat_parent == NULL ||
        pat->pat_parent->pat_type != GRAPHD_PATTERN_PICK) {
      pat->pat_sample = !graphd_pattern_is_set_dependent(cl, con, pat);
      if (pat->pat_type == GRAPHD_PATTERN_PICK) clear_marks_below(pat);
    }
  }
  if (GRAPHD_PATTERN_IS_COMPOUND(pat->pat_type))

    for (sub = pat->pat_list_head; sub != NULL; sub = sub->pat_next)

      mark_pattern(greq, con, sub,
                   depth + (pat->pat_type == GRAPHD_PATTERN_LIST));
}

/*  Label each assignment with its depth.  Depth lets
 *  us later decide which assignments are sampling and
 *  which are implicit sequences.
 */
static int parenthesize_pattern(graphd_request* greq, graphd_constraint* con,
                                graphd_pattern* pat, size_t depth) {
  cl_handle* cl = graphd_request_cl(greq);
  int err;
  graphd_assignment* a;
  graphd_pattern* sub;

  /* { char b2[200];
  cl_log(cl, CL_LEVEL_VERBOSE, "parenthesize_pattern con=%s, pat=%s, depth=%zu",
  graphd_constraint_to_string(con),
  graphd_pattern_to_string(pat, b2, sizeof b2),
  depth);
  } */

  if (pat->pat_type == GRAPHD_PATTERN_LIST && ++depth >= 3) {
    char buf[200];
    graphd_request_errprintf(greq, 0,
                             "SEMANTICS result expression \"%s\" "
                             "nests lists more than two levels deep",
                             graphd_pattern_to_string(pat, buf, sizeof buf));
    return GRAPHD_ERR_SEMANTICS;
  }

  else if (GRAPHD_PATTERN_IS_COMPOUND(pat->pat_type)) {
    /*  If the compound is a "pick", we go in
     *  without incrementing <depth>.
     *
     *  If the compound is a list, we incremented
     *  the depth in the first "if" branch.
     */
    for (sub = pat->pat_list_head; sub != NULL; sub = sub->pat_next) {
      err = parenthesize_pattern(greq, con, sub, depth);
      if (err != 0) {
        char buf[200];
        cl_log(cl, CL_LEVEL_FAIL,
               "parenthesize_pattern: error "
               "while recursing into %s",
               graphd_pattern_dump(pat, buf, sizeof buf));
        return err;
      }
    }
    return 0;
  } else if (pat->pat_type != GRAPHD_PATTERN_VARIABLE)
    return 0;

  /*  We are standing on a variable.
   *  Label the declaration of the variable in the current constraint.
   */
  cl_assert(cl, pat->pat_variable_declaration != NULL);

  /* An assignment to that variable.
   */
  a = graphd_assignment_by_declaration(con, pat->pat_variable_declaration);

  if (pat->pat_variable_declaration->vdecl_parentheses < depth) {
    /* Annotate the variable declaration with its
     * new depth.
     */
    pat->pat_variable_declaration->vdecl_parentheses = depth;

    /*  Relabel (with their new depths) the variables used in
     *  the assignment to that pattern.
     *
     *  We can't go into an infinite loop here because our depth
     *  would go beyond 3, and because assignments aren't
     *  recursive (we checked for that earlier).
     */
    if (a != NULL) {
      a->a_depth = depth;
      err = parenthesize_pattern(greq, con, a->a_result, depth);
      if (err != 0) {
        char const *name_s, *name_e;
        char buf[200];

        graphd_variable_declaration_name(a->a_declaration, &name_s, &name_e);

        cl_log(cl, CL_LEVEL_FAIL,
               "parenthesize_pattern: error "
               "while recursing into %.*s=%s",
               (int)(name_e - name_s), name_s,
               graphd_pattern_dump(a->a_result, buf, sizeof buf));
        return err;
      }
    }
  } else if (a != NULL)
    a->a_depth = pat->pat_variable_declaration->vdecl_parentheses;

  return 0;
}

/* Check the parenthesization of assignments in <con>, and
 * mark each declaration with its depth of use.
 */
int graphd_assignment_parenthesize(graphd_request* greq,
                                   graphd_constraint* con) {
  graphd_assignment* a;
  cl_handle* cl = graphd_request_cl(greq);
  char buf[200];
  int err;

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_assignment_parenthesize: %s",
         graphd_constraint_to_string(con));
  graphd_assignment_dump(greq, con);

  if (con->con_result != NULL) {
    err = parenthesize_pattern(greq, con, con->con_result, 0);
    if (err != 0) {
      cl_log(cl, CL_LEVEL_FAIL,
             "parenthesize_pattern: error "
             "from result %s",
             graphd_pattern_dump(con->con_result, buf, sizeof buf));
      return err;
    }
  }

  /* For all assignments...
   */
  for (a = con->con_assignment_head; a != NULL; a = a->a_next) {
    /*  The assignment's depth can still change,
     *  but we start with the declaration depth.
     */
    a->a_depth = (a->a_declaration->vdecl_constraint != con
                      ? 0
                      : a->a_declaration->vdecl_parentheses);
    err = parenthesize_pattern(greq, con, a->a_result, a->a_depth);
    if (err) return err;
  }

  /*  Now that we know how deeply nested everything is,
   *  mark subtree roots as collections or sampled,
   *  according to how they're actually being used.
   */
  if (con->con_result != NULL) mark_pattern(greq, con, con->con_result, 0);

  /* For all assignments...
   */
  for (a = con->con_assignment_head; a != NULL; a = a->a_next)
    mark_pattern(greq, con, a->a_result, a->a_depth);

  return 0;
}

/* Print a list of assignments.
 */
void graphd_assignment_dump(graphd_request* greq, graphd_constraint* con) {
  cl_handle* cl = graphd_request_cl(greq);
  graphd_assignment const* a;
  char buf[200];

  cl_log(cl, CL_LEVEL_VERBOSE, "result = %s",
         con->con_result ? graphd_pattern_dump(con->con_result, buf, sizeof buf)
                         : "(null)");

  for (a = con->con_assignment_head; a != NULL; a = a->a_next) {
    char buf[200];
    char const *name_s, *name_e;

    graphd_variable_declaration_name(a->a_declaration, &name_s, &name_e);

    cl_log(cl, CL_LEVEL_VERBOSE, "(%d) %.*s = %s", (int)a->a_depth,
           (int)(name_e - name_s), name_s,
           a->a_result ? graphd_pattern_dump(a->a_result, buf, sizeof buf)
                       : "(null)");
  }
}
