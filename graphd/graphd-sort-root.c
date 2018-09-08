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
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

/*  A SORT ROOT is a combination of
 *
 *	a pattern, including the order in it
 *	a constraint
 *
 *  If a constraint has a sort root, then its records are sorted
 *  by the value of the given pattern element of the records
 *  matching the given constraint.  For example, nodes might be
 *  sorted by the timestamp of their links.
 */

char const *graphd_sort_root_to_string(graphd_sort_root const *sr, char *buf,
                                       size_t bufsize) {
  char patbuf[200];

  if (sr == NULL || sr->sr_con == NULL) return "(null)";
  if (bufsize <= 20) return "(sortroot)";
  snprintf(buf, bufsize, "(con=%.*s, pat=%.*s)", (int)(bufsize - 20) / 2,
           graphd_constraint_to_string(sr->sr_con), (int)(bufsize - 20) / 2,
           graphd_pattern_to_string(&sr->sr_pat, patbuf, sizeof patbuf));
  return buf;
}

/**
 * @brief What's the sort root variable whose assignment is routed through
 * <con>?
 */
static void sort_root_intermediary(graphd_request *greq, graphd_constraint *top,
                                   graphd_constraint *bottom,
                                   graphd_pattern *out) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_pattern pat;
  graphd_constraint *sub, *con;
  bool ok;
  int sign;
  graphd_assignment const *a = NULL;
  const graphd_comparator *scmp;

  cl_enter(cl, CL_LEVEL_VERBOSE, "top=%s bottom=%s",
           graphd_constraint_to_string(top),
           graphd_constraint_to_string(bottom));

  memset(out, 0, sizeof *out);

  /*  Start with the variable that <top> is actually sorted by.
   */
  cl_assert(cl, top != NULL);
  cl_assert(cl, bottom != NULL);
  cl_assert(cl, top != bottom);
  cl_assert(cl, top->con_sort != NULL);
  cl_assert(cl, top->con_sort_valid);
  cl_assert(cl, bottom->con_sort == NULL || !bottom->con_sort_valid);

  ok = graphd_pattern_head(top->con_sort, &pat);
  cl_assert(cl, ok);
  cl_assert(cl, pat.pat_type == GRAPHD_PATTERN_VARIABLE);
  sign = pat.pat_sort_forward;
  scmp = pat.pat_comparator;

  cl_assert(cl, scmp);

  for (con = top;; con = sub) {
    char buf[200];

    /* Find the assignment from a subconstraint to our variable.
     */
    for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
      a = graphd_assignment_by_declaration(sub, pat.pat_variable_declaration);
      if (a != NULL) break;
    }
    cl_assert(cl, a != NULL);

    /* The first part of what's assigned to the variable
     */
    ok = graphd_pattern_head(a->a_result, &pat);
    cl_assert(cl, ok);
    sign ^= !pat.pat_sort_forward;

    cl_log(cl, CL_LEVEL_VERBOSE,
           "sort_root_intermediary: looking at pattern %s in constraint %s",
           graphd_pattern_dump(&pat, buf, sizeof buf),
           graphd_constraint_to_string(sub));

    if (sub == bottom) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "%s",
               graphd_pattern_dump(&pat, buf, sizeof buf));

      *out = pat;
      out->pat_comparator = scmp;
      out->pat_sort_forward = sign;

      return;
    }
    cl_assert(cl, pat.pat_type == GRAPHD_PATTERN_VARIABLE);
  }
}

/**
 * @brief Fill in sort=() constraints between sort root and sort user.
 *
 *  Somewhere up in the tree, a variable is used to sort.
 *  Somewhere below, the variable is assigned to.  (That's the "sort root").
 *
 *  All the way up from the root to the user, if there is no sort=(),
 *  assume the same sort as on the top.
 *
 * @param cl	log through here
 * @param con	constraint that is sorted
 * @param var	variable it is sorted through
 * @param out	sort root to set.
 *
 * @return GRAPHD_ERR_NO if this variable has no proper sort root below con.
 * @return 0 if the variable has a root below or in con,
 *	and its value is assigned to out.
 * @return other nonzero errors on serious error, e.g. allocation failure
 */
int graphd_sort_root_promote(graphd_request *greq, graphd_constraint *con) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_constraint *sub;
  int err;

  cl_enter(cl, CL_LEVEL_VERBOSE, "con=%s", graphd_constraint_to_string(con));

  /*  Do I have a non-trivial sort root?
   */
  if (con->con_sort && con->con_sort_valid &&
      con->con_sort_root.sr_con != NULL && con->con_sort_root.sr_con != con) {
    /* Going upwards from that sort root...
     */
    for (sub = con->con_sort_root.sr_con; sub != NULL && sub != con;
         sub = sub->con_parent) {
      char buf[200];
      graphd_pattern pat;

      /*  Make sub sorted like con,
       *  if it isn't already.
       */
      if (sub->con_sort != NULL && sub->con_sort_valid) continue;

      sort_root_intermediary(greq, con, sub, &pat);

      sub->con_sort = graphd_pattern_dup(greq, NULL, &pat);
      if (sub->con_sort == NULL) {
        cl_leave(cl, CL_LEVEL_VERBOSE,
                 "error - "
                 "graphd_pattern_dup() returns NULL");
        return ENOMEM;
      }
      sub->con_sort_valid = true;

      sub->con_sort_root.sr_con = con->con_sort_root.sr_con;
      sub->con_sort_root.sr_pat = con->con_sort_root.sr_pat;

      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_sort_root_promote: tagged %s with %s",
             graphd_constraint_to_string(sub),
             graphd_sort_root_to_string(&sub->con_sort_root, buf, sizeof buf));
      cl_log(cl, CL_LEVEL_VERBOSE, "graphd_sort_root_promote: new sort %s",
             graphd_pattern_dump(sub->con_sort, buf, sizeof buf));
    }
  }

  /*  Do the same for the subconstraints.
   */
  for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
    err = graphd_sort_root_promote(greq, sub);
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "recursive error: %s: %s",
               graphd_constraint_to_string(sub), graphd_strerror(err));
      return err;
    }
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "ok");
  return 0;
}

/**
 * @brief Constraint <con> is sorted primarily by <var>.
 *
 *  Upon successful return, there is a sort-root chain from the
 *  assignment to <var> up to including <out> in <con>.
 *
 *  The sort root assignment fails if there is more than one
 *  possible source for the variable values, or if the subconstraints
 *  that the variable value comes from are sorted by something other
 *  than the sorting criterion.
 *
 * @param cl	log through here
 * @param con	constraint that is sorted
 * @param var	variable it is sorted through
 * @param out	sort root to set.
 *
 * @return GRAPHD_ERR_NO if this variable has no proper sort root below con.
 * @return 0 if the variable has a root below or in con,
 *	and its value is assigned to out.
 * @return other nonzero errors on serious error, e.g. allocation failure
 */
static bool sort_root_for_variable(graphd_request *greq, graphd_constraint *con,
                                   graphd_pattern *var, graphd_sort_root *out) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_pattern pat;
  graphd_constraint *sub;
  char buf[200];

  cl_enter(cl, CL_LEVEL_VERBOSE, "%s below %s",
           graphd_pattern_dump(var, buf, sizeof buf),
           graphd_constraint_to_string(con));

  memset(out, 0, sizeof *out);
  out->sr_con = NULL;

  cl_assert(cl, var->pat_type == GRAPHD_PATTERN_VARIABLE);

  for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
    graphd_assignment const *a;

    a = graphd_assignment_by_declaration(sub, var->pat_variable_declaration);

    if (a == NULL) {
      /*  This particular subconstraint of <con> doesn't
       *  assign to the variable whose assigners we're
       *  looking for.  That's fine.
       */
      continue;
    }

    /* We already found something else on this level?
     */
    if (out->sr_con != NULL) {
      out->sr_con = NULL;
      cl_leave(cl, CL_LEVEL_VERBOSE, "no: more than one sort root");
      return false;
    }

    /*  This is an optional subconstraint, or a subconstraint
     *  in an "or" ?
     */
    if (!GRAPHD_CONSTRAINT_IS_MANDATORY(sub) || sub->con_parent != con) {
      cl_leave(cl, CL_LEVEL_VERBOSE,
               "no: assignment in optional subconstraint");
      return false;
    }

    /* The first part of what's assigned to the variable
     */
    if (!graphd_pattern_head(a->a_result, &pat)) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "no: no head in %s",
               graphd_pattern_dump(a->a_result, buf, sizeof buf));
      return false;
    }

    /*  If it's assigned *from* a variable, in turn,
     *  we need to follow it further down.
     *
     *  (sort=$bar
     *	(<- $bar=$foo
     *		($foo=value)))
     */
    if (pat.pat_type == GRAPHD_PATTERN_VARIABLE) {
      graphd_sort_root sub_root;

      /*  As we recurse, we're communicating the
       *  comparator and sort direction of the tree
       *  above downward.
       */
      pat.pat_sort_forward ^= !var->pat_sort_forward;
      if (pat.pat_comparator == NULL) pat.pat_comparator = var->pat_comparator;

      if (!sort_root_for_variable(greq, sub, &pat, &sub_root)) {
        cl_leave(cl, CL_LEVEL_VERBOSE, "no: can't resolve variable %s",
                 graphd_pattern_dump(&pat, buf, sizeof buf));
        return false;
      }
      *out = sub_root;
    } else {
      /* Leaf case.
       */
      out->sr_con = sub;
      out->sr_pat = pat;

      out->sr_pat.pat_sort_forward ^= !var->pat_sort_forward;
      if (var->pat_comparator != NULL)
        out->sr_pat.pat_comparator = var->pat_comparator;
    }

    /*  It's not a per-instance pattern (but something more
     *  like COUNT or CURSOR)?
     */
    if (GRAPHD_PATTERN_IS_SET_VALUE(out->sr_pat.pat_type)) {
      out->sr_con = NULL;
      out->sr_pat.pat_type = GRAPHD_PATTERN_UNSPECIFIED;

      cl_leave(cl, CL_LEVEL_VERBOSE, "no: not a sampling pattern");
      return false;
    }

    /*  If we're reaching below this subconstraint's
     *  results, the subconstraint must be sorted by the
     *  same criterion, or not sorted at all.
     *  (We'll infer their sort.)
     *
     *  Otherwise, the sort root stops here.
     */
    if (sub->con_sort != NULL && sub->con_sort_valid) {
      if (sub->con_sort_root.sr_con != out->sr_con ||
          sub->con_sort_root.sr_pat.pat_type != out->sr_pat.pat_type ||
          sub->con_sort_root.sr_pat.pat_comparator !=
              out->sr_pat.pat_comparator ||
          sub->con_sort_root.sr_pat.pat_sort_forward !=
              out->sr_pat.pat_sort_forward) {
        char b2[200];
        cl_leave(cl, CL_LEVEL_VERBOSE,
                 "no: different sort: %p sorted "
                 "by %s, not %s",
                 (void *)sub, graphd_pattern_dump(&sub->con_sort_root.sr_pat,
                                                  buf, sizeof buf),
                 graphd_pattern_dump(&out->sr_pat, b2, sizeof b2));
        return false;
      }
    }
  }

  if (out->sr_con == NULL)
    cl_leave(cl, CL_LEVEL_VERBOSE, "no sort root found");
  else {
    char buf[200];
    cl_leave(cl, CL_LEVEL_VERBOSE, "%p: %s", (void *)out->sr_con,
             graphd_pattern_to_string(&out->sr_pat, buf, sizeof buf));
  }
  return !!out->sr_con;
}

/* @brief Annotate a constraint with its sort root, if there is one.
 */
int graphd_sort_root_mark(graphd_request *greq, graphd_constraint *con) {
  graphd_pattern pat;
  char buf[200];
  graphd_constraint *sub;
  cl_handle *cl = graphd_request_cl(greq);

  memset(&con->con_sort_root, 0, sizeof con->con_sort_root);
  con->con_sort_root.sr_con = NULL;

  /* Recurse into subconstraints; we'll use that later.
   */
  for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
    int err = graphd_sort_root_mark(greq, sub);
    if (err != 0) return err;
  }

  if (!con->con_sort_valid || !graphd_pattern_head(con->con_sort, &pat))
    return 0;

  if (pat.pat_type == GRAPHD_PATTERN_VARIABLE) {
    (void)sort_root_for_variable(greq, con, &pat, &con->con_sort_root);
  } else {
    con->con_sort_root.sr_pat = pat;
    con->con_sort_root.sr_con = con;
  }

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_sort_root_mark: con=%p sortroot=%s",
         (void *)con,
         graphd_sort_root_to_string(&con->con_sort_root, buf, sizeof buf));
  return 0;
}

/* @brief Remove _obvious_ sort roots.
 *
 *  If we don't remove them, we just end up with a bunch
 *  of sorted iterators that save their ordering in their
 *  cursors - we don't really need that.
 */
void graphd_sort_root_unmark(graphd_request *greq, graphd_constraint *con) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_pattern pat;
  char buf[200];
  graphd_constraint *sub;

  cl_enter(cl, CL_LEVEL_VERBOSE, "%s", graphd_constraint_to_string(con));

  /* Recurse into subconstraints.
   */
  for (sub = con->con_head; sub != NULL; sub = sub->con_next)
    graphd_sort_root_unmark(greq, sub);

  if (!con->con_sort_valid || !graphd_pattern_head(con->con_sort, &pat)) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "no sort clause in the constraint");
    return;
  }

  /*  We're local, our parent doesn't reference us,
   *  and our actual sort order is trivial.
   */
  if (con->con_sort_root.sr_con == con &&
      (con->con_parent == NULL ||
       con->con_parent->con_sort_root.sr_con != con) &&
      (con->con_sort_root.sr_pat.pat_type == GRAPHD_PATTERN_GUID ||
       con->con_sort_root.sr_pat.pat_type == GRAPHD_PATTERN_TIMESTAMP)) {
    con->con_sort_root.sr_pat.pat_type = GRAPHD_PATTERN_UNSPECIFIED;
    con->con_sort_root.sr_con = NULL;
  }

  /*  Our sort root is a value, and somewhere along a
   *  non-1-element set along the way there's a disagreement
   *  about comparators?
   */

  cl_assert(cl, con->con_value_comparator);
  if (con->con_sort_root.sr_con != NULL &&
      con->con_sort_root.sr_pat.pat_type == GRAPHD_PATTERN_VALUE &&
      con->con_sort_root.sr_con != con) {
    for (sub = con->con_sort_root.sr_con; sub != NULL && sub != con;
         sub = sub->con_parent) {
      graphd_pattern spat;

      if (graphd_linkage_is_i_am(sub->con_linkage)) continue;

      cl_assert(cl, sub->con_value_comparator);

      if (!sub->con_sort_valid || !graphd_pattern_head(sub->con_sort, &spat)) {
        /*
         * This constraint doesn't even have a sort.
         * That's fine.
         */
        continue;
      }

      /*
       * If we don't share comparators (two different
       * value sorts on the same value in the same tree)
       * give up.
       */
      if (spat.pat_comparator == pat.pat_comparator) continue;

      cl_log(cl, CL_LEVEL_INFO,
             "graphd_sort_root_unmark: "
             "comparator disagreement between root %s "
             "(con %s) and %s (con %s)",
             pat.pat_comparator ? pat.pat_comparator->cmp_name : "(null)",
             graphd_constraint_to_string(con),
             spat.pat_comparator ? spat.pat_comparator->cmp_name : "(null)",
             graphd_constraint_to_string(sub));

      /*  Sorry, but this doesn't work.  Break the
       *  link between this and the sort top.
       */
      for (;;) {
        sub->con_sort_root.sr_con = NULL;
        sub->con_sort_root.sr_pat.pat_type = GRAPHD_PATTERN_UNSPECIFIED;

        if (sub == con) break;
        sub = sub->con_parent;
        if (sub == NULL) break;
      }
    }
  }

  cl_leave(cl, CL_LEVEL_VERBOSE, "%s",
           graphd_sort_root_to_string(&con->con_sort_root, buf, sizeof buf));
}

/*  Translate a /-separated path relative to a constraint
 *  into a pattern.
 *
 *  The path has the form of
 *
 *	0/3/1.0
 *
 *  where numbers in the /-separted path lead to
 *  a subconstraint (0 is the 0th subconstraint, etc.)
 *  and the number after the . leads to a pframe;
 *  the sort root is the first actual pattern in that
 *  pframe.
 */
int graphd_sort_root_from_string(graphd_request *greq, char const *s,
                                 char const *e, graphd_sort_root *sr) {
  graphd_constraint *con = greq->greq_constraint;
  size_t n;

  while (s < e) {
    /*  Skip leading /
     */
    while (s < e && *s == '/') s++;
    if (s >= e) return GRAPHD_ERR_LEXICAL;

    if (!isascii(*s) || !isdigit(*s)) break;

    /*  Scan a number.
     */
    n = 0;
    while (s < e && isascii(*s) && isdigit(*s)) {
      n *= 10;
      n += *s - '0';
      s++;
    }

    /*  Go to the child of con that that number addresses.
     */
    con = con->con_head;
    while (con != NULL && n > 0) {
      n--;
      con = con->con_next;
    }
    if (con == NULL) return GRAPHD_ERR_LEXICAL;
  }
  if (con == NULL) return GRAPHD_ERR_LEXICAL;

  /*  Scan a .
   */
  while (s < e && *s == '.') s++;

  if (s >= e || !isascii(*s) || !isdigit(*s)) return GRAPHD_ERR_LEXICAL;

  /*  Scan a number.
   */
  n = 0;
  while (isascii(*s) && isdigit(*s)) {
    n *= 10;
    n += *s - '0';
    s++;
  }
  if (s != e) return GRAPHD_ERR_LEXICAL;

  sr->sr_con = con;
  sr->sr_ordering = NULL;

  /*  Go to the variable or result addressed by that.
   */
  if (n >= con->con_pframe_n) return GRAPHD_ERR_SEMANTICS;

  /*  The single-element pattern frame addressed by that.
   */
  if (con->con_pframe[n].pf_one == NULL) return GRAPHD_ERR_SEMANTICS;

  /*  It must be a list. Take its first pattern element.
   */
  if (con->con_pframe[n].pf_one->pat_type == GRAPHD_PATTERN_LIST &&
      con->con_pframe[n].pf_one->pat_list_n > 0)

    return graphd_pattern_dup_in_place(
        greq->greq_req.req_cm, &sr->sr_pat,
        con->con_pframe[n].pf_one->pat_list_head);

  return GRAPHD_ERR_SEMANTICS;
}

/*  Translate a pattern into a path relative to a constraint.
 */
static int graphd_sort_root_constraint_path(graphd_constraint const *root,
                                            graphd_constraint const *pat_con,
                                            cm_buffer *buf) {
  graphd_constraint const *sub, *par;
  size_t i;
  int err;

  if (pat_con == NULL || root == NULL) return GRAPHD_ERR_NO;

  if (pat_con == root) {
    cm_buffer_truncate(buf);
    return 0;
  }

  par = pat_con->con_parent;
  if (par != root) {
    err = graphd_sort_root_constraint_path(root, par, buf);
    if (err != 0) return err;
  }

  i = 0;
  for (sub = par->con_head; sub != NULL; sub = sub->con_next) {
    if (sub == pat_con) break;
    i++;
  }
  return cm_buffer_sprintf(buf, "/%zu", i);
}

/**
 * @brief Translate a sort root into a path relative to a root constraint.
 */
static int sort_root_to_buffer(cl_handle *cl, graphd_constraint const *root,
                               graphd_sort_root const *sr, cm_buffer *buf) {
  graphd_pattern_frame *pf;
  size_t i, j;
  int err;

  /* Where is the pattern within the constraint?
   */
  for (i = 0, pf = sr->sr_con->con_pframe; i < sr->sr_con->con_pframe_n;
       i++, pf++) {
    if (pf->pf_one == NULL) continue;

    if (pf->pf_one->pat_type != GRAPHD_PATTERN_LIST) {
      if (graphd_pattern_equal_value(cl, sr->sr_con, pf->pf_one, sr->sr_con,
                                     &sr->sr_pat))
        break;
    } else if (pf->pf_one->pat_list_n >= 1) {
      graphd_pattern *p;

      for (j = 0, p = pf->pf_one->pat_list_head; j < pf->pf_one->pat_list_n;
           j++, p = p->pat_next) {
        if (graphd_pattern_equal_value(cl, sr->sr_con, p, sr->sr_con,
                                       &sr->sr_pat))
          break;
      }
      if (j < pf->pf_one->pat_list_n) break;
    }
  }

  if (i >= sr->sr_con->con_pframe_n) {
    char buf[200];

    cl_log(cl, CL_LEVEL_FAIL,
           "sort_root_to_buffer: cannot "
           "find pattern \"%s\" in constraint!",
           graphd_pattern_dump(&sr->sr_pat, buf, sizeof buf));
    return GRAPHD_ERR_NO;
  }

  /*  Assemble the path through the constraint tree.
   */
  err = graphd_sort_root_constraint_path(root, sr->sr_con, buf);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_sort_root_constraint_path", err,
                 "cannot convert constraint path for %s",
                 graphd_constraint_to_string(sr->sr_con));
    return err;
  }

  return cm_buffer_sprintf(buf, ".%zu", i);
}

bool graphd_sort_root_equal(cl_handle *cl, graphd_sort_root const *a,
                            graphd_sort_root const *b) {
  return a->sr_con == b->sr_con &&
         graphd_pattern_equal(cl, a->sr_con, &a->sr_pat, b->sr_con, &b->sr_pat);
}

/**
 * @brief Return the ordering (a string pathame) for a given sort root.
 */
char const *graphd_sort_root_ordering(graphd_request *greq,
                                      graphd_sort_root *sr) {
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  cm_buffer buf;

  cl_assert(cl, sr != NULL);

  if (sr->sr_con == NULL) return NULL;

  if (sr->sr_ordering == NULL) {
    int err;
    graphd_constraint const *top;

    for (top = sr->sr_con; top->con_parent != NULL; top = top->con_parent)
      ;

    cm_buffer_initialize(&buf, cm);
    err = sort_root_to_buffer(cl, top, sr, &buf);
    if (err != 0) {
      char buf[200];
      cl_log_errno(cl, CL_LEVEL_FAIL, "sort_root_to_buffer", err,
                   "cannot convert %s to a buffer",
                   graphd_sort_root_to_string(sr, buf, sizeof buf));
      return NULL;
    }
    sr->sr_ordering = buf.buf_s;
  }
  cl_assert(cl, sr->sr_ordering != NULL);
  return sr->sr_ordering;
}

/**
 * @brief compare an ordering to a sort root.  Does the
 *  	sort root correspond to this ordering?
 */
bool graphd_sort_root_has_ordering(graphd_sort_root const *sr,
                                   char const *ordering) {
  if (sr == NULL || sr->sr_con == NULL || ordering == NULL) return false;

  /*  If the iterator had been made from this sort root,
   *  the sort root's ordering parameter would have been
   *  set in the course.
   */
  if (sr->sr_ordering == NULL) return false;

  return strcasecmp(sr->sr_ordering, ordering) == 0;
}

/**
 * @brief Return the direction the iterator for this constraint
 * 	should go in, along with an ordering.
 */
graphd_direction graphd_sort_root_iterator_direction(
    graphd_request *greq, graphd_constraint *con, char const **ordering_out) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_direction dir;

  cl_assert(cl, con != NULL);

  if (con->con_or)
    return graphd_sort_root_iterator_direction(
        greq, graphd_constraint_or_prototype_root(con), ordering_out);

  *ordering_out = graphd_sort_root_ordering(greq, &con->con_sort_root);

  if (*ordering_out == NULL) con->con_sort_root.sr_con = NULL;

  dir = graphd_sort_iterator_direction(con->con_sort_valid ? con->con_sort
                                                           : NULL);
  if (con->con_sort_root.sr_con == NULL) return dir;

  if (dir == GRAPHD_DIRECTION_ANY) return GRAPHD_DIRECTION_ORDERING;

  cl_assert(cl, *ordering_out != NULL);
  return dir;
}
