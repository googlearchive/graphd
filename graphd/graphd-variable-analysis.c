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

/*  ADDRESSING SAMPLES
 *
 *  Given the slot of the ID, a sample <pat> of a specific ID can
 *  be addressed as a pair of two numbers.
 *
 *  The first number identifies the slot of the assignment or
 *  result value it is returned in.  This is either one of the
 *  assignments (1...N) or the result slot (0).
 *
 *  The second number identifies the position within those
 *  values, if they're lists.  So, a sample address of { 2, 3 }
 *  may mean that our sample is the fourth element of the second
 *  assignment value.
 */

/*  Remove unused result=(..) expressions from constraints, so we
 *  don't end up needlessly computing subconstraint results.
 */
static void gva_remove_unused_results(cl_handle *cl, graphd_constraint *con) {
  bool uses_contents;
  graphd_constraint *sub;

  /*  If there are no "contents" on this level,
   *  set the result assignments of all subconstraints
   *  to NULL, and recurse.
   */
  uses_contents = graphd_constraint_uses_contents(con);

  con->con_title = NULL;
  cl_log(cl, CL_LEVEL_VERBOSE,
         "gva_remove_unused_results: %s %s use sub results.",
         graphd_constraint_to_string(con), uses_contents ? "does" : "doesn't");

  for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
    if (!uses_contents) {
      sub->con_result = NULL;

      /* Flush the display name cache.
       */
      sub->con_title = NULL;
    }
    gva_remove_unused_results(cl, sub);
  }

  con->con_title = NULL;
  cl_log(cl, CL_LEVEL_VERBOSE, "gva_remove_unused_results: done: %s",
         graphd_constraint_to_string(con));
}

/*  Remove unused sort=(..) expressions from constraints.
 *
 *  A sort expression is used
 *	- whenever something is sampled
 *	- whenever a twice-nested list is returned or assigned to a variable.
 */
static int gva_remove_unused_sorts(graphd_request *greq,
                                   graphd_constraint *con) {
  graphd_constraint *sub;
  cl_handle *cl = graphd_request_cl(greq);
  int err;

  if (con->con_sort != NULL && con->con_sort_valid) {
    graphd_assignment *a;

    /* Is there an assignment upwards that is
     *  group or sampling?
     */
    if (con->con_result != NULL &&
        graphd_pattern_is_sort_dependent(cl, con, con->con_result))
      goto use_sort;

    for (a = con->con_assignment_head; a != NULL; a = a->a_next)
      if (graphd_pattern_is_sort_dependent(cl, con, a->a_result)) goto use_sort;

    /*  Just zero it out - it's small, and allocated
     *  on the request heap.
     */
    char buf[200];
    cl_log(cl, CL_LEVEL_DEBUG, "gva_remove_unused_sorts: remove %s from %s",
           graphd_pattern_to_string(con->con_sort, buf, sizeof buf),
           graphd_constraint_to_string(con));

    con->con_sort_valid = false;
  }

use_sort:
  for (sub = con->con_head; sub != NULL; sub = sub->con_next)
    if ((err = gva_remove_unused_sorts(greq, sub)) != 0) return err;

  con->con_title = NULL;
  cl_log(cl, CL_LEVEL_VERBOSE, "gva_remove_unused_sorts: done: %s",
         graphd_constraint_to_string(con));
  return 0;
}

static void gva_remove_unused_pagesizes(graphd_request *greq,
                                        graphd_constraint *con) {
  graphd_constraint *sub;

  con->con_resultpagesize = con->con_resultpagesize_parsed;
  con->con_resultpagesize_valid = con->con_resultpagesize_parsed_valid;

  for (sub = con->con_head; sub != NULL; sub = sub->con_next)
    gva_remove_unused_pagesizes(greq, sub);

  if (!graphd_pattern_frame_uses_per_primitive_data(greq, con)) {
    if (!con->con_resultpagesize_valid || con->con_resultpagesize > 1) {
      con->con_resultpagesize = 1;
      con->con_resultpagesize_valid = true;
    }
  }
}

static void gva_count_variable_uses(cl_handle *const cl, graphd_pattern *pat) {
  if (pat == NULL) return;

  if (GRAPHD_PATTERN_IS_COMPOUND(pat->pat_type)) {
    graphd_pattern *p;

    for (p = pat->pat_list_head; p != NULL; p = p->pat_next) {
      if (GRAPHD_PATTERN_IS_COMPOUND(p->pat_type))
        gva_count_variable_uses(cl, p);

      else if (p->pat_type == GRAPHD_PATTERN_VARIABLE)
        p->pat_variable_declaration->vdecl_linkcount++;
    }
  } else if (pat->pat_type == GRAPHD_PATTERN_VARIABLE)
    pat->pat_variable_declaration->vdecl_linkcount++;
}

static void gva_clear_declarations(graphd_request *greq,
                                   graphd_constraint *con) {
  graphd_variable_declaration *vdecl = NULL;

  /*  Set all declarations' linkcounts to zero.
   */
  while ((vdecl = graphd_variable_declaration_next(con, vdecl)) != NULL)
    vdecl->vdecl_linkcount = 0;

  /*  Do the same for all subconstraints.
   */
  for (con = con->con_head; con != NULL; con = con->con_next)
    gva_clear_declarations(greq, con);
}

static void gva_mark_used_declarations(graphd_request *greq,
                                       graphd_constraint *con) {
  graphd_assignment const *a;
  cl_handle *cl = graphd_request_cl(greq);

  /*  Mark those that actually get used (read).
   */
  for (a = con->con_assignment_head; a != NULL; a = a->a_next)
    gva_count_variable_uses(cl, a->a_result);

  if (con->con_sort != NULL && con->con_sort_valid)
    gva_count_variable_uses(cl, con->con_sort);
  gva_count_variable_uses(cl, con->con_result);

  /*  Do the same for all subconstraints.
   */
  for (con = con->con_head; con != NULL; con = con->con_next)
    gva_mark_used_declarations(greq, con);
}

static bool gva_remove_assignments_to_unmarked_declarations(
    graphd_request *greq, graphd_constraint *con) {
  cl_handle *const cl = graphd_request_cl(greq);
  cm_handle *const cm = greq->greq_req.req_cm;
  graphd_assignment *a, **a_ptr;
  bool any = false;

  /*  Remove assignments to variables with unmarked declarations.
   */
  a_ptr = &con->con_assignment_head;
  while ((a = *a_ptr) != NULL) {
    if (a->a_declaration->vdecl_linkcount > 0)
      a_ptr = &a->a_next;
    else {
      char const *name_s, *name_e;
      graphd_variable_declaration_name(a->a_declaration, &name_s, &name_e);

      cl_log(cl, CL_LEVEL_VERBOSE,
             "gva_remove_assignments_to_unmarked"
             "_declarations: remove %.*s from %p",
             (int)(name_e - name_s), name_s, (void *)con);

      *a_ptr = a->a_next;
      cm_free(cm, a);

      any = true;
      con->con_assignment_n--;
    }
  }
  con->con_assignment_tail = a_ptr;
  *a_ptr = NULL;
  con->con_title = NULL;

  /*  Do the same for all subconstraints.
   */
  for (con = con->con_head; con != NULL; con = con->con_next)
    any |= gva_remove_assignments_to_unmarked_declarations(greq, con);

  return any;
}

static bool gva_remove_unmarked_declarations(graphd_request *greq,
                                             graphd_constraint *con) {
  cl_handle *const cl = graphd_request_cl(greq);
  graphd_variable_declaration *vdecl = NULL;
  bool any = false;

  graphd_variable_declaration *next;

  /*  Remove unmarked declarations.
   */
  next = graphd_variable_declaration_next(con, NULL);
  any = false;
  while ((vdecl = next) != NULL) {
    char const *name_s, *name_e;

    next = graphd_variable_declaration_next(con, vdecl);
    if (vdecl->vdecl_linkcount == 0) {
      graphd_variable_declaration_name(vdecl, &name_s, &name_e);

      cl_log(cl, CL_LEVEL_VERBOSE,
             "gva_remove_unmarked_declarations: "
             "remove %.*s from %p",
             (int)(name_e - name_s), name_s, (void *)con);

      graphd_variable_declaration_delete(vdecl);
      any = true;
    }
  }

  con->con_title = NULL;

  /*  Do the same for all subconstraints.
   */
  for (con = con->con_head; con != NULL; con = con->con_next)
    any |= gva_remove_unmarked_declarations(greq, con);

  return any;
}

static void gva_remove_unused_declarations(graphd_request *greq,
                                           graphd_constraint *con) {
  bool any;

  do {
    any = false;

    gva_clear_declarations(greq, greq->greq_constraint);
    gva_mark_used_declarations(greq, greq->greq_constraint);

    any |= gva_remove_assignments_to_unmarked_declarations(
        greq, greq->greq_constraint);
    any |= gva_remove_unmarked_declarations(greq, greq->greq_constraint);

  } while (any);
}

static int gva_create_pattern_frames(graphd_request *greq,
                                     graphd_constraint *con) {
  int err;

  graphd_variable_declaration_assign_slots(con);

  err = graphd_pattern_frame_create(greq, con);
  if (err != 0) return err;

  /*  Do the same for all subconstraints.
   */
  for (con = con->con_head; con != NULL; con = con->con_next) {
    err = gva_create_pattern_frames(greq, con);
    if (err != 0) return err;
  }
  return 0;
}

/*  Does this "or", or one of its prototypes,
 *  have this index?
 */
static bool constraint_contains_or(graphd_constraint *orcon, size_t or_index) {
  for (;;) {
    if (orcon->con_or_index == or_index) return true;
    if (orcon->con_or == NULL) break;

    orcon = orcon->con_or->or_prototype;
  }
  return false;
}

/*  Do the picked values in this assignment cover <orcon>
 *  or one of its ancestors?
 */
static bool pick_contains_or(graphd_pattern *pat, graphd_constraint *orcon) {
  if (pat->pat_type != GRAPHD_PATTERN_PICK) return true;

  for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next)
    if (constraint_contains_or(orcon, pat->pat_or_index)) return true;
  return false;
}

/*  Return true if, if the <orcon> evaluates to true and ends
 *  up being included in the results, there will be an assignment
 *  to <vdecl> that is active.
 *
 *  This is used to decide whether or not to add assignments from
 *  another vdecl in orcon that implicitly forwards its result
 *  towards vdecl.  If one is already active, no implicit forwarding
 *  happens.
 */
static bool vdecl_has_input_through_or(graphd_variable_declaration *vdecl,
                                       graphd_constraint *orcon) {
  graphd_assignment *a;

  /*  An assignment in vdecl's constraint that is
   *  either unconditional, or known to be active when
   *  <orcon> is active.
   */
  a = graphd_assignment_by_declaration(vdecl->vdecl_constraint, vdecl);
  if (a != NULL) {
    if (a->a_result == NULL || a->a_result->pat_type != GRAPHD_PATTERN_PICK ||
        pick_contains_or(a->a_result, orcon))

      return true;
  }
  return false;
}

/*  Instead of declaring a variable in an "or" branch,
 *  declare it in the prototype, and add a pick assignment
 *  to the prototype variable.
 */

/*  The new variable <vdecl> has just been declared.
 */
static int vdecl_inferences(graphd_request *greq,
                            graphd_variable_declaration *vdecl) {
  int err;
  char const *name_s;
  char const *name_e;
  graphd_constraint *par, *sub, *con;
  graphd_assignment *a_new;
  graphd_variable_declaration *vdecl_new, *vdecl_par;
  cl_handle *cl = graphd_request_cl(greq);

recurse:
  con = vdecl->vdecl_constraint;

  /* We're in an "or"?
   */
  if (con->con_or != NULL) {
    graphd_variable_declaration *vdecl_new;
    graphd_constraint *arch;

    arch = graphd_constraint_or_prototype_root(con);
    err =
        graphd_constraint_or_compile_declaration(greq, arch, vdecl, &vdecl_new);
    if (err != 0) return err;

    cl_log(cl, CL_LEVEL_VERBOSE,
           "vdecl_inferences: %p is an OR; "
           "inferring to arch constraint %p",
           (void *)con, (void *)arch);

    if (vdecl_new != NULL) {
      /*  We made a new assignment!
       *  Draw inferences from *that* one, in turn.
       */
      con = arch;
      vdecl = vdecl_new;
    } else
      return 0;
  }

  /*  OK.  What's that variable called?  We'll use the
   *  name to find variable uses that are meant to connect
   *  to it, but don't yet.
   */
  graphd_variable_declaration_name(vdecl, &name_s, &name_e);
  par = sub = con;

  /*  In the case we want to react to,
   *
   *  - someone is using a variable named like the destination of
   *    our assignment above us. (That created a declaration.)
   *
   *  - There is no assignment to that variable in the
   *    constraints between us and its use.
   */
  vdecl_par = NULL;
  for (;;) {
    /*  Go up or back through an "or".
     */
    if (par->con_or != NULL)
      par = graphd_constraint_or_prototype_root(par);
    else
      par = par->con_parent;

    if (par == NULL) break;

    /*  Is the variable declared here?
     */
    vdecl_par = graphd_variable_declaration_by_name(par, name_s, name_e);
    if (vdecl_par != NULL) break;

    /*  Remember where we came from.
     */
    sub = par;
  }

  /*  The variable isn't used?  Then we don't need to do anything.
   */
  if (vdecl_par == NULL) return 0;

  /*  We just crossed an "or", and there is already an
   *  assignment?
   */
  if (sub->con_or != NULL && vdecl_has_input_through_or(vdecl_par, sub))
    return 0;

  /*  We're not inside an "or", and there's an assignment to vdecl
   *  in the declaring constraint, or in the one below it?
   */
  if ((graphd_assignment_by_declaration(par, vdecl_par) != NULL &&
       sub->con_or == NULL) ||
      graphd_assignment_by_declaration(sub, vdecl_par) != NULL)
    return 0;

  /*  Add declarations all the way up, or until you hit an "or".
   */
  par = sub = con;
  vdecl_new = vdecl;

  for (;;) {
    graphd_variable_declaration *par_decl, *rhs_decl;
    cm_handle *cm = greq->greq_req.req_cm;
    bool existing_declaration = false;
    char buf[200];

    /*  If we hit an "or" boundary, go back to bridging that
     *  for the most recently created vdecl.
     */
    if (par->con_or != NULL) {
      con = par;
      vdecl = vdecl_new;

      goto recurse;
    }

    par = par->con_parent;
    if (par == NULL) break;

    rhs_decl = NULL;

    par_decl = graphd_variable_declaration_by_name(par, name_s, name_e);
    if (par_decl != NULL)
      existing_declaration = true;
    else {
      if (par->con_or != NULL) {
        err = graphd_constraint_or_declare(greq, par, name_s, name_e, &par_decl,
                                           &rhs_decl);
        if (err != 0) return err;

        /*  If a new declaration was created for
         *  this specific purpose, and we'll want
         *  to continue, rhs_decl will have been
         *  set to that new declaration.
         *  It is null if the declaration wasn't new.
         */
        if (rhs_decl == NULL) existing_declaration = true;
      } else {
        /*  Declare the variable in the parent.
         */
        par_decl = graphd_variable_declaration_add(cm, cl, par, name_s, name_e);
        if (par_decl == NULL) return ENOMEM;

        rhs_decl = par_decl;
      }
    }

    /*  Create an assignment into the parent in the
     *  constraint below it.
     */
    if (sub->con_or != NULL) sub = graphd_constraint_or_prototype_root(sub);

    a_new = graphd_assignment_alloc_declaration(greq, sub, par_decl);
    if (a_new == NULL) return ENOMEM;

    cl_assert(cl, vdecl_new != NULL);
    a_new->a_result = graphd_pattern_alloc_variable(greq, NULL, vdecl_new);
    if (a_new->a_result == NULL) return ENOMEM;

    cl_log(cl, CL_LEVEL_VERBOSE,
           "vdecl_inferences: "
           "%p %.*s := %p %s",
           (void *)par_decl->vdecl_constraint, (int)(name_e - name_s), name_s,
           (void *)vdecl_new->vdecl_constraint,
           graphd_pattern_to_string(a_new->a_result, buf, sizeof buf));

    if (existing_declaration) break;

    vdecl_new = rhs_decl;
    sub = par = vdecl_new->vdecl_constraint;
  }

  return 0;
}

/**
 * @brief Create inferred assignments.
 *
 * @param greq	Parsed request.
 * @param con	Root of the subtree to check.
 */
static int gva_infer_constraint(graphd_request *greq, graphd_constraint *con) {
  int err;
  graphd_variable_declaration *vdecl, *vdecl_next;

  vdecl_next = graphd_variable_declaration_next(con, NULL);
  while ((vdecl = vdecl_next) != NULL) {
    vdecl_next = graphd_variable_declaration_next(con, vdecl);
    if ((err = vdecl_inferences(greq, vdecl)) != 0) return err;
  }

  if (con->con_or != NULL)
    graphd_constraint_or_move_assignments(
        greq, graphd_constraint_or_prototype_root(con), con);
  return 0;
}

static int gva_infer_or_constraint(graphd_request *greq,
                                   graphd_constraint *con) {
  int err;
  graphd_constraint_or *cor;
  cl_handle *cl = graphd_request_cl(greq);

  cl_enter(cl, CL_LEVEL_VERBOSE, "looking at (con=%p, con_or=%p, or_head=%p)",
           (void *)con, con->con_or, con->con_or_head);

  /*  Recurse into the "or"s inside <con>.
   */
  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    err = gva_infer_or_constraint(greq, &cor->or_head);
    if (err != 0) return err;

    if (cor->or_tail != NULL) {
      err = gva_infer_or_constraint(greq, cor->or_tail);
      if (err != 0) return err;
    }
  }

  /*  Now this one.
   */
  err = gva_infer_constraint(greq, con);

  cl_leave(cl, CL_LEVEL_VERBOSE, "%s", err ? graphd_strerror(err) : "ok");

  return err;
}

/**
 * @brief Create inferred assignments.
 *
 * @param greq	Parsed request.
 * @param con	Root of the subtree to check.
 */
static int gva_infer(graphd_request *greq, graphd_constraint *con) {
  int err;
  graphd_constraint *sub;
  cl_handle *cl = graphd_request_cl(greq);

  cl_enter(cl, CL_LEVEL_VERBOSE, "looking at (con=%p, con_or=%p, or_head=%p)",
           (void *)con, con->con_or, con->con_or_head);

  /*  Recurse into the constraints below us...
   */
  for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
    err = gva_infer(greq, sub);
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
      return err;
    }
  }

  err = gva_infer_or_constraint(greq, con);
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s", err ? graphd_strerror(err) : "ok");

  return 0;
}

static int gva_resolve_aliases(graphd_request *greq, graphd_constraint *con) {
  graphd_constraint *sub;
  int err;

  err = graphd_variable_replace_aliases(greq, con);
  if (err != 0) return err;

  for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
    err = gva_resolve_aliases(greq, sub);
    if (err != 0) return err;
  }
  return 0;
}

static void gva_dump_constraint(graphd_request *greq, int indent,
                                graphd_constraint *con) {
  cl_handle *cl = graphd_request_cl(greq);
  char buf[200];
  graphd_constraint_or *cor;
  graphd_assignment *a;
  size_t i;
  graphd_variable_declaration *vdecl = NULL;

  if (!con->con_variable_declaration_valid &&
      con->con_assignment_head == NULL &&
      (con->con_or != NULL || con->con_head == NULL)) {
    cl_log(cl, CL_LEVEL_VERBOSE, "%*s constraint %p: null", indent, "",
           (void *)con);
    return;
  }

  cl_log(cl, CL_LEVEL_VERBOSE, "%*s constraint %p: {", indent, "", (void *)con);

  vdecl = NULL;
  while ((vdecl = graphd_variable_declaration_next(con, vdecl)) != NULL) {
    char const *name_s, *name_e;

    graphd_variable_declaration_name(vdecl, &name_s, &name_e);
    cl_log(cl, CL_LEVEL_VERBOSE, "%*s vdecl \"%.*s\"", indent + 2, "",
           (int)(name_e - name_s), name_s);
    cl_assert(cl, vdecl->vdecl_constraint == con);
  }
  for (a = con->con_assignment_head; a != NULL; a = a->a_next) {
    char pbuf[200];
    graphd_variable_declaration *vdecl = NULL;
    char const *name_s, *name_e;
    char const *label;

    vdecl = a->a_declaration;
    graphd_variable_declaration_name(vdecl, &name_s, &name_e);

    if (vdecl->vdecl_constraint == con)
      label = "    .";
    else if (vdecl->vdecl_constraint == con->con_parent)
      label = "  ..";
    else if (vdecl->vdecl_constraint ==
             graphd_constraint_or_prototype_root(con))
      label = "{..}";
    else {
      snprintf(buf, sizeof buf, "%p", (void *)vdecl->vdecl_constraint);
      label = buf;
    }

    cl_log(cl, CL_LEVEL_VERBOSE, "%*s assignment %s \"%.*s\" = %s", indent + 2,
           "", label, (int)(name_e - name_s), name_s,
           graphd_pattern_to_string(a->a_result, pbuf, sizeof pbuf));
  }

  for (i = 0; i < con->con_pframe_n; i++) {
    graphd_pattern_frame *pf = con->con_pframe + i;
    char b1[200], b3[200];

    *b3 = '\0';
    *b1 = '\0';

    if (pf->pf_one != NULL) {
      char b2[200];
      snprintf(b3, sizeof b3, " ONE[%zu]=%s", pf->pf_one_offset,
               graphd_pattern_dump(pf->pf_one, b2, sizeof b2));
    }
    if (pf->pf_set != NULL) {
      char b2[200];
      snprintf(b1, sizeof b1, " SET=%s",
               graphd_pattern_dump(pf->pf_set, b2, sizeof b2));
    }

    cl_log(cl, CL_LEVEL_VERBOSE, "%*s pframe%s%s", indent + 2, "", b1, b3);
  }

  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    cl_log(cl, CL_LEVEL_VERBOSE, "%*s or(%p) {", indent + 2, "", (void *)cor);

    gva_dump_constraint(greq, indent + 4, &cor->or_head);
    if (cor->or_tail != NULL)
      gva_dump_constraint(greq, indent + 4, cor->or_tail);

    cl_log(cl, CL_LEVEL_VERBOSE, "%*s }", indent + 2, "");
  }
  if (con->con_or == NULL) {
    graphd_constraint *sub;
    for (sub = con->con_head; sub != NULL; sub = sub->con_next)
      gva_dump_constraint(greq, indent + 2, sub);
  }

  cl_log(cl, CL_LEVEL_VERBOSE, "%*s }", indent, "");
}

static void gva_dump(graphd_request *greq, char const *title) {
  cl_handle *cl = graphd_request_cl(greq);
  cl_log(cl, CL_LEVEL_VERBOSE, "%s -- BEGIN VARIABLE DUMP --", title);

  gva_dump_constraint(greq, 0, greq->greq_constraint);

  cl_log(cl, CL_LEVEL_VERBOSE, "%s -- END VARIABLE DUMP --", title);
}

static int gva_sort_assignments(graphd_request *greq, graphd_constraint *con) {
  int err;

  err = graphd_assignment_sort(greq, con);
  if (err != 0) {
    graphd_request_errprintf(greq, false,
                             "SEMANTICS unspecified error while sorting "
                             "variable assignments: %s",
                             graphd_strerror(err));
    return err;
  }

  for (con = con->con_head; con != NULL; con = con->con_next) {
    err = gva_sort_assignments(greq, con);
    if (err != 0) return err;
  }

  return 0;
}

/*  Is <pat> a proper possible sort pattern?
 */
static int gva_check_sort_pattern(graphd_request *greq,
                                  graphd_pattern const *pat) {
  cl_handle *const cl = graphd_request_cl(greq);

  if (pat == NULL) return 0;

  if (pat->pat_type != GRAPHD_PATTERN_LIST) {
    cl_cover(cl);
    return 0;
  }

  for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next) {
    if (pat->pat_type == GRAPHD_PATTERN_LIST) {
      graphd_request_error(greq, "SYNTAX cannot sort by nested lists.");
      return GRAPHD_ERR_SYNTAX;
    }
  }
  cl_cover(cl);
  return 0;
}

/*  Is <pat> a proper pattern?
 */
static int gva_check_pattern(graphd_request *greq, graphd_constraint *con,
                             graphd_pattern const *pat, unsigned long *used,
                             unsigned long *maybe_used, size_t depth) {
  size_t n_lists = 0;
  cl_handle *const cl = graphd_request_cl(greq);
  unsigned long sub_used = 0;
  int err;
  graphd_assignment const *a;

  if (pat == NULL) return 0;

  /* { char b1[200];
  cl_log(cl, CL_LEVEL_VERBOSE,
          "gva_check_pattern con=%s pat=%s depth=%zu",
          graphd_constraint_to_string(con),
          graphd_pattern_to_string(pat, b1, sizeof b1),
          depth);
  } */

  *used |= 1 << pat->pat_type;
  *maybe_used |= 1 << pat->pat_type;

  switch (pat->pat_type) {
    case GRAPHD_PATTERN_VARIABLE:
      a = graphd_assignment_by_declaration(con, pat->pat_variable_declaration);
      if (a == NULL) return 0;

      return gva_check_pattern(greq, con, a->a_result, used, maybe_used, depth);

    case GRAPHD_PATTERN_PICK:
      sub_used = (unsigned long)-1;
      for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next) {
        unsigned long pick_used = 0;
        err = gva_check_pattern(greq, con, pat, &pick_used, maybe_used, depth);
        if (err != 0) return err;
        sub_used &= pick_used;
      }
      *used |= sub_used;
      cl_cover(cl);
      return 0;

    case GRAPHD_PATTERN_CURSOR:
    case GRAPHD_PATTERN_ESTIMATE:
    case GRAPHD_PATTERN_ITERATOR:
    case GRAPHD_PATTERN_TIMEOUT:
    case GRAPHD_PATTERN_COUNT:
    case GRAPHD_PATTERN_ESTIMATE_COUNT:
      if (depth == 2) {
        cl_cover(cl);
        graphd_request_error(greq,
                             "SYNTAX 'count', 'cursor', 'estimate', "
                             "'estimate-count', 'iterator', or 'timeout' can "
                             "only appear inside at most one set of "
                             "parentheses");
        return GRAPHD_ERR_SYNTAX;
      }
    /* FALL THROUGH */
    default:
      cl_cover(cl);
      return 0;

    case GRAPHD_PATTERN_LIST:
      if (depth >= 2) {
        graphd_request_error(greq,
                             "SYNTAX result lists can only "
                             "nest two lists deep");
        return GRAPHD_ERR_SYNTAX;
      }
      break;
  }

  depth++;
  for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next) {
    unsigned long sub_maybe_used = 0;

    err = gva_check_pattern(greq, con, pat, used, &sub_maybe_used, depth);
    if (err != 0) return err;

    if ((sub_maybe_used & (1 << GRAPHD_PATTERN_LIST)) && ++n_lists > 1) {
      cl_cover(cl);
      graphd_request_error(greq,
                           "SYNTAX can only have one nested list "
                           "per result list - (x (y)) and ((x y)) "
                           "work, ((x) (y)) doesn't.");
      return GRAPHD_ERR_SYNTAX;
    }
    *maybe_used |= sub_maybe_used;
  }
  cl_cover(cl);
  return 0;
}

static int gva_parenthesize_assignments(graphd_request *greq,
                                        graphd_constraint *con) {
  int err;
  cl_handle *cl = graphd_request_cl(greq);
  unsigned long used = 0, maybe_used = 0;
  graphd_assignment *a;

  err = graphd_assignment_parenthesize(greq, con);
  if (err != 0) {
    graphd_request_errprintf(greq, false,
                             "SEMANTICS unspecified error while parenthesizing "
                             "variable assignments: %s",
                             graphd_strerror(err));
    return err;
  }

  /* - Result instructions are at most two lists deep,
   *   and contain at most a single sublist.
   */
  used = 0;
  if (con->con_result != NULL &&
      (err = gva_check_pattern(greq, con, con->con_result, &used, &maybe_used,
                               0)) != 0) {
    return err;
  }
  for (a = con->con_assignment_head; a != NULL; a = a->a_next) {
    cl_assert(cl, a->a_result != NULL);

    if ((err = gva_check_pattern(greq, con, a->a_result, &used, &maybe_used,
                                 a->a_declaration->vdecl_constraint == con
                                     ? a->a_declaration->vdecl_parentheses
                                     : 0)) != 0) {
      return err;
    }
  }

  /* Sort instructions are at most one list deep.
   *
   * (We catch this at the parse, normally, but resolving
   * aliases can introduce extra nesting levels.)
   *
   * Don't check con_sort_valid before this check - we complain
   * even about unused malformed patterns.
   */
  if (con->con_sort != NULL &&
      (err = gva_check_sort_pattern(greq, con->con_sort)) != 0) {
    return err;
  }

  /*  Recurse.
   */
  for (con = con->con_head; con != NULL; con = con->con_next) {
    err = gva_parenthesize_assignments(greq, con);
    if (err != 0) return err;
  }
  return 0;
}

/*  @brief Prepare a read or iterate request's variable assignments
 *	for execution.
 *
 *   	At the end, declarations are annotated with their local
 *	frame offsets, duplicates are removed, aliases expanded,
 *	and samples and sorts know where to get their values.
 *
 *  @param greq	 request to prepare
 *  @return 0 on success, a nonzero error code on error.
 */
int graphd_variable_analysis(graphd_request *greq) {
  cl_handle *cl = graphd_request_cl(greq);
  int err;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");

  gva_dump(greq, "graphd_variable_analysis: incoming");

  /*  Create picks and implied samples.
   */
  err = gva_infer(greq, greq->greq_constraint);
  if (err != 0) return err;

  gva_dump(greq, "graphd_variable_analysis: after gva_infer");

  /*  Remove unused result= assignments.
   */
  gva_remove_unused_results(cl, greq->greq_constraint);

  /*  Remove unused sort= assignments.
   */
  gva_remove_unused_sorts(greq, greq->greq_constraint);

  /*  Resolve aliases (1 of 2).
   */
  err = gva_resolve_aliases(greq, greq->greq_constraint);
  if (err != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "error from gva_resolve_aliases: %s",
             graphd_strerror(err));
    return err;
  }

  /*  Mark up sort roots.  This may add sort=() constraints
   *  to the tree - consequently, it must happen before pframes
   *  are created.
   */
  err = graphd_sort_root_mark(greq, greq->greq_constraint);
  if (err != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "error from graphd_sort_root_mark: %s",
             graphd_strerror(err));
    return err;
  }

  /*  Remove "obvious" (same-constraint) sort roots.
   */
  graphd_sort_root_unmark(greq, greq->greq_constraint);

  err = graphd_sort_root_promote(greq, greq->greq_constraint);
  if (err != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "error from graphd_sort_root_promote: %s",
             graphd_strerror(err));
    return err;
  }

  /*  Resolve aliases (2 of 2)
   */
  err = gva_resolve_aliases(greq, greq->greq_constraint);
  if (err != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "error from gva_resolve_aliases: %s",
             graphd_strerror(err));
    return err;
  }

  /*  Check validity of the sort expression
   */
  err = graphd_sort_check(greq, greq->greq_constraint);
  if (err != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "error from gva_resolve_aliases: %s",
             graphd_strerror(err));
    return err;
  }

  /*  Check the depths of assignments.
   */
  err = gva_parenthesize_assignments(greq, greq->greq_constraint);
  if (err != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE,
             "error from gva_parenthesize_assignments: %s",
             graphd_strerror(err));
    return err;
  }

  /*  Count the number of times each variable is used;
   *  remove unused declarations.  (They became unused
   *  in the course of alias resolution.)
   */
  gva_remove_unused_declarations(greq, greq->greq_constraint);

  /*  Sort assignments: variable assignment before
   *  variable use.
   */
  err = gva_sort_assignments(greq, greq->greq_constraint);
  if (err != 0) {
    cl_cover(cl);
    graphd_request_errprintf(greq, false,
                             "SEMANTICS unspecified error while sorting "
                             "variable assignments: %s",
                             graphd_strerror(err));
    return err;
  }

  /*  Create pattern frames.
   */
  err = gva_create_pattern_frames(greq, greq->greq_constraint);
  if (err != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "error from gva_create_pattern_frames: %s",
             graphd_strerror(err));
    return err;
  }

  /*  Remove unused pagesize=... parameters.
   */
  gva_remove_unused_pagesizes(greq, greq->greq_constraint);

  gva_dump(greq, "graphd_variable_analysis: done");
  cl_leave(cl, CL_LEVEL_VERBOSE, "leave");
  return 0;
}
