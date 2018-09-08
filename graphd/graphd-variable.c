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

/**
 * @brief Is a variable assigned to in, or below, a constraint?
 *
 *  It is a syntax error to reference a variable that doesn't
 *  get assigned to at or below its point of use.  This function
 *  is used to check for that.
 *
 * @param cl	log through here
 * @param con	constraint we're looking at
 * @param pat 	variable pattern (must be of type GRAPHD_PATTERN_VARIABLE)
 *
 * @return true if the variable is used, false if not.
 */
bool graphd_variable_is_assigned_in_or_below(cl_handle *cl,
                                             graphd_constraint const *con,
                                             char const *s, char const *e) {
  graphd_constraint_or *cor;

  if (graphd_assignment_by_name(con, s, e) != NULL) return true;

  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next)

    if (graphd_variable_is_assigned_in_or_below(cl, &cor->or_head, s, e) ||
        (cor->or_tail != NULL &&
         graphd_variable_is_assigned_in_or_below(cl, cor->or_tail, s, e)))
      return true;

  for (con = con->con_head; con != NULL; con = con->con_next)
    if (graphd_variable_is_assigned_in_or_below(cl, con, s, e)) return true;

  cl_cover(cl);
  return false;
}

/**
 * @brief Does this pattern use this variable?
 *  	If yes, where do we get its value?
 *
 * @param cl		Log through this
 * @param con		context that contains the pattern somewhere
 * @param pat		pattern that may contain the variable.
 * @param vdecl		variable declaration
 * @param index_out	NULL or its location on the stack
 *
 * @return true if the variable is returned, false if not.
 */
static bool graphd_variable_is_used_in_pattern(
    cl_handle *const cl, graphd_constraint const *con,
    graphd_pattern const *pat, graphd_variable_declaration const *vdecl,
    size_t *const index_out) {
  for (; pat != NULL; pat = graphd_pattern_preorder_next(pat)) {
    if (pat->pat_type == GRAPHD_PATTERN_VARIABLE &&
        pat->pat_variable_declaration == vdecl) {
      if (index_out != NULL)
        *index_out = pat->pat_variable_declaration->vdecl_local;
      cl_cover(cl);
      return true;
    }
    cl_cover(cl);
  }
  return false;
}

/**
 * @brief Does this constraint use this variable anywhere?
 *
 *  This includes uses in sort, in a result, and in other
 *  assignments to variables.
 *
 * @param cl		Log through here
 * @param con		constraint the caller is asking about
 * @param name_s	start of variable name (without the $)
 * @param name_e	just past the end of variable name (without the $)
 * @param index_out	NULL or store the variable stack offset here
 */
bool graphd_variable_is_used(cl_handle *cl, graphd_constraint const *con,
                             char const *name_s, char const *name_e,
                             size_t *index_out) {
  graphd_assignment const *a;
  graphd_constraint_or *cor;
  graphd_variable_declaration const *vdecl;

  vdecl = graphd_variable_declaration_by_name(con, name_s, name_e);
  if (vdecl == NULL) return false;

  if (graphd_variable_is_used_in_pattern(cl, con, con->con_result, vdecl,
                                         index_out) ||
      (con->con_sort != NULL && con->con_sort_valid &&
       graphd_variable_is_used_in_pattern(cl, con, con->con_sort, vdecl,
                                          index_out)))
    return true;

  for (a = con->con_assignment_head; a != NULL; a = a->a_next) {
    cl_assert(cl, a->a_result != NULL);
    if (graphd_variable_is_used_in_pattern(cl, con, a->a_result, vdecl,
                                           index_out))
      return true;
  }

  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    if (graphd_variable_is_used(cl, &cor->or_head, name_s, name_e, index_out) ||
        (cor->or_tail != NULL &&
         graphd_variable_is_used(cl, cor->or_tail, name_s, name_e, index_out)))
      return true;
  }

  return false;
}

/**
 * @brief Is a variable used on the right-hand of a return,
 * 	assignment, or sort above a constraint?
 *
 * @parem cl		Log through this
 * @param con		constraint we're looking above
 * @param name_s	variable name to look for, start
 * @param name_e	variable name to look for, pointer just past the end
 * @param index_out	NULL or where to store the offset within the frame.
 *
 * @return a pointer to the using constraint, or NULL if there is none.
 */
graphd_constraint *graphd_variable_user_in_or_above(
    cl_handle *cl, graphd_constraint const *con, char const *name_s,
    char const *name_e, size_t *index_out) {
  graphd_variable_declaration *vdecl;
  graphd_assignment const *a;

  cl_assert(cl, name_s != NULL && name_e != NULL);
  cl_assert(cl, con != NULL);

  vdecl = graphd_variable_declaration_by_name(con, name_s, name_e);
  if (vdecl != NULL) {
    a = graphd_assignment_by_name(con, name_s, name_e);
    if (a == NULL) return NULL;

    for (; a != NULL; a = a->a_next) {
      if (graphd_variable_is_used_in_pattern(cl, con, a->a_result, vdecl,
                                             index_out))
        return (graphd_constraint *)con;
    }
  }

  while (con->con_or != NULL) {
    con = con->con_or->or_prototype;
    if (graphd_variable_is_used(cl, con, name_s, name_e, index_out))
      return (graphd_constraint *)con;
  }

  for (con = con->con_parent; con != NULL; con = con->con_parent)
    if (graphd_variable_is_used(cl, con, name_s, name_e, index_out))
      return (graphd_constraint *)con;

  return NULL;
}

/**
 * @brief Replace aliased variables with their left hand sides.
 *
 * @param cl		Log through this
 * @param con		constraint we're replacing in
 * @param pat		pattern to replace in.
 */
static int graphd_variable_pattern_replace_aliases(
    graphd_request *greq, graphd_constraint const *const con,
    graphd_pattern *pat) {
  cm_handle *const cm = greq->greq_req.req_cm;

  graphd_assignment const *a;

  for (; pat != NULL; pat = graphd_pattern_preorder_next(pat)) {
    if (pat->pat_type == GRAPHD_PATTERN_VARIABLE &&
        pat->pat_variable_declaration->vdecl_constraint == con &&
        (a = graphd_assignment_by_declaration(
             con, pat->pat_variable_declaration)) != NULL) {
      cl_handle *const cl = graphd_request_cl(greq);
      bool sort_forward = pat->pat_sort_forward;
      size_t or_index = pat->pat_or_index;
      int err;
      const graphd_comparator *cmp;
      char const *name_s, *name_e;
      char buf[200];

      cmp = pat->pat_comparator;
      err = graphd_pattern_dup_in_place(cm, pat, a->a_result);
      if (err != 0) return err;

      /*  Keep the sign: $foo and -$foo are different.
       *  Keep the comparator and or-index.
       */
      pat->pat_sort_forward ^= !sort_forward;
      pat->pat_comparator = cmp;
      pat->pat_or_index = or_index;

      graphd_variable_declaration_name(a->a_declaration, &name_s, &name_e);

      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_variable_pattern_replace_aliases: "
             "replace %.*s with %s",
             (int)(name_e - name_s), name_s,
             graphd_pattern_to_string(pat, buf, sizeof buf));
    }
  }
  return 0;
}

/**
 * @brief Replace aliased variables with their left hand sides.
 *
 * @param cl		Log through this
 * @param con		constraint we're replacing in
 */
int graphd_variable_remove_unused(graphd_request *greq,
                                  graphd_constraint *const con) {
  cl_handle *const cl = graphd_request_cl(greq);
  graphd_assignment *a;
  graphd_assignment **ap;

  /*  After this, any variable that's not used above its
   *  context can be dropped from its chain.
   */
  /* For all variable names in all left hand sides of all assignments...
   */
  for (ap = &con->con_assignment_head; *ap != NULL;) {
    char const *name_s, *name_e;

    a = *ap;

    /*  We're the highest user of this variable?
     */
    graphd_variable_declaration_name(a->a_declaration, &name_s, &name_e);
    if (graphd_variable_user_in_or_above(cl, con, name_s, name_e, NULL) ==
        NULL) {
      /*  Yes.  There's no need to export this value.
       *  Get rid of the slot.
       */

      /*  It's okay to just drop this one; the variable
       *  was allocated either in the context or on the
       *  request heap stack.
       */
      con->con_assignment_n--;
      *ap = a->a_next;

      /* Continue without advancing ap; we just
       * pulled the rest of the list up into it.
       */
      continue;
    }
    ap = &a->a_next;
  }
  con->con_assignment_tail = ap;
  return 0;
}

/**
 * @brief Replace aliased variables with their left hand sides.
 *
 * @param cl		Log through this
 * @param con		constraint we're replacing in
 */
int graphd_variable_replace_aliases(graphd_request *greq,
                                    graphd_constraint *const con) {
  graphd_assignment *a;
  int err;

  /* For all variables used in all right hand sides of all assignments...
   */
  for (a = con->con_assignment_head; a != NULL; a = a->a_next) {
    err = graphd_variable_pattern_replace_aliases(greq, con, a->a_result);
    if (err != 0) return err;
  }
  err = graphd_variable_pattern_replace_aliases(greq, con, con->con_result);
  if (err != 0) return err;

  if (con->con_sort != NULL) {
    err = graphd_variable_pattern_replace_aliases(greq, con, con->con_sort);
    if (err != 0) return err;
  }
  return 0;
}

/**
 * @brief Anchor a variable assignment
 *
 *  Create little assignments that hand the value of a variable
 *  up the constraint chain.
 *
 *  The variable is used in a constraint or one of its containing
 *  outer constraints, and assigned within the constraint or
 *  within a contained, inner constraint.
 *
 * @param greq  the read request whose constraints contained the assignment
 * @param con 	the constraint itself
 * @param a 	assignment
 *
 * @return 0 on success, otherwise an error code.
 */
int graphd_variable_anchor(graphd_request *greq, graphd_constraint *con,
                           char const *name_s, char const *name_e) {
  graphd_assignment *a;
  graphd_constraint *anc, *highest = NULL;
  cl_handle *const cl = graphd_request_cl(greq);

  if (name_s == NULL || name_s == name_e) return 0;

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_variable_anchor \"%.*s\" above %s",
         (int)(name_e - name_s), name_s, graphd_constraint_to_string(con));

  /* Find the highest ancestor that declares <name>'s destination
   * variable.
   *
   * Stop looking if you see another assignment to <name>.
   * (If that happens, we've already done the work for
   * _that_ assignment.)
   */
  for (anc = con->con_parent; anc != NULL; anc = anc->con_parent) {
    graphd_constraint *or_root;

    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_variable_anchor \"%.*s\" "
           "in [%p; parent %p, or %p] %s",
           (int)(name_e - name_s), name_s, (void *)anc, (void *)anc->con_parent,
           (void *)anc->con_or, graphd_constraint_to_string(anc));

    or_root = graphd_constraint_or_prototype_root(anc);

    if (graphd_assignment_by_name(or_root, name_s, name_e)) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_variable_anchor: found "
             "assignment in %s",
             graphd_constraint_to_string(or_root));
      break;
    }

    if (graphd_variable_declaration_by_name(or_root, name_s, name_e) != NULL) {
      cl_log(cl, CL_LEVEL_VERBOSE, "graphd_variable_anchor: found use in %s",
             graphd_constraint_to_string(or_root));
      highest = or_root;
    }
  }

  /* Nothing to do. Probably an alias or an error.
   */
  if (highest == NULL) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_variable_anchor: no highest point found");
    return 0;
  }

  /*  Add assignments wherever they don't exist yet.
   */
  for (anc = con->con_parent; anc != NULL; anc = anc->con_parent) {
    graphd_pattern *pat;

    if (anc == highest) break;

    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_variable_anchor: adding %.*s=%.*s "
           "assignment to %s",
           (int)(name_e - name_s), name_s, (int)(name_e - name_s), name_s,
           graphd_constraint_to_string(anc));

    pat = graphd_variable_declare(greq, anc, NULL, name_s, name_e);
    if (pat == NULL) return errno ? errno : ENOMEM;

    a = graphd_assignment_alloc(greq, anc, name_s, name_e);
    if (a == NULL) return errno ? errno : ENOMEM;

    /*  The right-hand-side of the assignment is the
     *  variable value.
     */
    a->a_result = pat;
    pat->pat_sample = true;
  }
  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_variable_anchor: done.");
  return 0;
}

/**
 * @brief Declare a variable.
 *
 *  A variable is implicitly declared when it is used on the right hand
 *  side of a sort= or result= or $var= statement.  By assigning/returning/
 *  sorting by $foo, a query sets in motion the allocation of a slot for
 *  "foo" inside its read contexts.
 *
 * @param greq  	request
 * @param con 		constraint
 * @param pattern 	NULL or pattern containing the variable
 * @param var_s 	first byte of variable name
 * @param var_e 	pointer just after last byte of variable name
 *
 * @return NULL no allocation error, otherwise a pointer to an
 *	assignment structure
 */
graphd_pattern *graphd_variable_declare(graphd_request *greq,
                                        graphd_constraint *con,
                                        graphd_pattern *pattern,
                                        char const *var_s, char const *var_e) {
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  graphd_variable_declaration *vdecl;

  vdecl = graphd_variable_declaration_add(cm, cl, con, var_s, var_e);
  if (vdecl == NULL) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_variable_declare", errno,
                 "can't allocate variable declaration");
    return NULL;
  }
  cl_cover(cl);
  return graphd_pattern_alloc_variable(greq, pattern, vdecl);
}

static void variable_rename_constraint(graphd_request *greq,
                                       graphd_constraint *con,
                                       graphd_variable_declaration *source,
                                       graphd_variable_declaration *dest) {
  graphd_assignment *a;

  graphd_pattern_variable_rename(con->con_result, source, dest);
  if (con->con_sort != NULL && con->con_sort_valid)
    graphd_pattern_variable_rename(con->con_sort, source, dest);

  for (a = con->con_assignment_head; a != NULL; a = a->a_next) {
    graphd_pattern_variable_rename(a->a_result, source, dest);
    if (a->a_declaration == source) a->a_declaration = dest;
  }
}

/**
 * @brief Rename a variable from one declaration to another.
 *
 *  A variable is implicitly declared when it is used on the right hand
 *  side of a sort= or result= or $var= statement.  By assigning/returning/
 *  sorting by $foo, a query sets in motion the allocation of a slot for
 *  "foo" inside its read contexts.
 *
 * @param greq  	request
 * @param con 		constraint
 * @param source 	old variable declaration
 * @param dest 		new variable declaration
 */
void graphd_variable_rename(graphd_request *greq, graphd_constraint *con,
                            graphd_variable_declaration *source,
                            graphd_variable_declaration *dest) {
  graphd_constraint_or *cor;

  {
    char const *from_s, *from_e, *to_s, *to_e;
    cl_handle *cl = graphd_request_cl(greq);

    graphd_variable_declaration_name(source, &from_s, &from_e);
    graphd_variable_declaration_name(dest, &to_s, &to_e);
    cl_log(cl, CL_LEVEL_VERBOSE, "graphd_variable_rename: %p: %.*s -> %.*s",
           (void *)con, (int)(from_e - from_s), from_s, (int)(to_e - to_s),
           to_s);
  }

  variable_rename_constraint(greq, con, source, dest);

  /*  This touches only the top levels of embedded ORs.  Their
   *  subconstraints are going to get visited in the subconstraint
   *  traversal below.
   */
  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    variable_rename_constraint(greq, &cor->or_head, source, dest);
    if (cor->or_tail != NULL)
      variable_rename_constraint(greq, cor->or_tail, source, dest);
  }

  /*  This is only one level deep because you cannot cross
   *  more than one level with assignments.
   */
  for (con = con->con_head; con != NULL; con = con->con_next)
    variable_rename_constraint(greq, con, source, dest);
}
