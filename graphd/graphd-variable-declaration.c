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

#include <errno.h>
#include <string.h>

/*  The variable declaration hashtable lives in the constraint,
 *  holds a record for each variable used in the constraint.
 *  (Used means: appearing on the RIGHT hand side of a constraint
 *  or in a result expression.)
 *
 *  Variable patterns point to the variable declaration.  Variable
 *  declarations eventually track where in the per-constraint id
 *  read record the actual variable value resides.
 */

graphd_variable_declaration *graphd_variable_declaration_by_name(
    graphd_constraint const *con, char const *s, char const *e) {
  if (!con->con_variable_declaration_valid) return NULL;

  return cm_haccess((cm_hashtable *)&con->con_variable_declaration,
                    graphd_variable_declaration, s ? s : "", s ? e - s : 0);
}

graphd_variable_declaration *graphd_variable_declaration_add(
    cm_handle *cm, cl_handle *cl, graphd_constraint *con, char const *s,
    char const *e) {
  graphd_variable_declaration *vdecl;

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_variable_declaration_add \"%.*s\" to %p",
         (int)(e - s), s, (void *)con);

  if (!con->con_variable_declaration_valid) {
    cm_hashinit(cm, &con->con_variable_declaration,
                sizeof(graphd_variable_declaration), 8);
    con->con_variable_declaration_valid = true;
  }
  vdecl = cm_hnew(&con->con_variable_declaration, graphd_variable_declaration,
                  s ? s : "", s ? e - s : 0);
  if (vdecl == NULL) {
    int err = errno;
    cl_log_errno(cl, CL_LEVEL_ERROR, "cm_hnew", err,
                 "failed to "
                 "allocate variable declaration slot");
    return NULL;
  }
  vdecl->vdecl_constraint = con;
  return vdecl;
}

void graphd_variable_declaration_delete(graphd_variable_declaration *vdecl) {
  if (vdecl == NULL) return;

  (void)cm_hdelete(&vdecl->vdecl_constraint->con_variable_declaration,
                   graphd_variable_declaration, vdecl);
}

void graphd_variable_declaration_assign_slots(graphd_constraint *con) {
  size_t i = 0;
  graphd_variable_declaration *vdecl;

  if (!con->con_variable_declaration_valid) {
    con->con_local_n = 0;
    return;
  }

  vdecl = NULL;
  while ((vdecl = cm_hnext(&con->con_variable_declaration,
                           graphd_variable_declaration, vdecl)) != NULL)
    vdecl->vdecl_local = i++;
  con->con_local_n = i;
}

void graphd_variable_declaration_destroy(graphd_constraint *con) {
  if (con->con_variable_declaration_valid) {
    con->con_variable_declaration_valid = false;
    cm_hashfinish(&con->con_variable_declaration);
  }
}

graphd_variable_declaration *graphd_variable_declaration_next(
    graphd_constraint *con, graphd_variable_declaration *ptr) {
  if (!con->con_variable_declaration_valid) return NULL;

  return cm_hnext(&con->con_variable_declaration, graphd_variable_declaration,
                  ptr);
}

/* Two variable declarations are equal if they've
 * both got the same name and the same position relative
 * to some parent or child constraint.
 */
bool graphd_variable_declaration_equal(cl_handle *cl,
                                       graphd_constraint const *a_con,
                                       graphd_variable_declaration const *a,
                                       graphd_constraint const *b_con,
                                       graphd_variable_declaration const *b) {
  char const *a_name;
  size_t a_size;

  char const *b_name;

  if (!a || !b) return !a && !b;

  cl_assert(cl, a_con == a->vdecl_constraint ||
                    a_con->con_parent == a->vdecl_constraint ||
                    a_con == a->vdecl_constraint->con_parent);

  cl_assert(cl, b_con == b->vdecl_constraint ||
                    b_con->con_parent == b->vdecl_constraint ||
                    b_con == b->vdecl_constraint->con_parent);

  a_size = cm_hsize(&a->vdecl_constraint->con_variable_declaration,
                    graphd_variable_declaration const, a);

  if (a_size != cm_hsize(&b->vdecl_constraint->con_variable_declaration,
                         graphd_variable_declaration const, b))
    return false;

  a_name = cm_hmem(&a->vdecl_constraint->con_variable_declaration,
                   graphd_variable_declaration const, a);

  b_name = cm_hmem(&b->vdecl_constraint->con_variable_declaration,
                   graphd_variable_declaration const, b);

  if (memcmp(a_name, b_name, a_size)) return false;

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_variable_declaration_equal %d %.*s %.*s",
         (int)a_size, (int)a_size, a_name, (int)a_size, b_name);

  return (a_con == a->vdecl_constraint
              ? b_con == b->vdecl_constraint
              : (a_con->con_parent == a->vdecl_constraint
                     ? b_con->con_parent == b->vdecl_constraint
                     : a->vdecl_constraint->con_parent == a_con &&
                           b->vdecl_constraint->con_parent == b_con));
}

void graphd_variable_declaration_name(graphd_variable_declaration const *vdecl,
                                      char const **s_out, char const **e_out) {
  *s_out = cm_hmem(&vdecl->vdecl_constraint->con_variable_declaration,
                   graphd_variable_declaration const, vdecl);

  *e_out = *s_out + cm_hsize(&vdecl->vdecl_constraint->con_variable_declaration,
                             graphd_variable_declaration const, vdecl);
}

size_t graphd_variable_declaration_n(graphd_constraint *con) {
  if (con->con_variable_declaration_valid)
    return cm_hashnelems(&con->con_variable_declaration);

  return 0;
}

char const *graphd_variable_declaration_to_string(
    graphd_variable_declaration const *vdecl, char *buf, size_t size) {
  char const *s, *e;
  char const *brackets;

  if (vdecl == NULL) return "null";

  switch (vdecl->vdecl_parentheses) {
    case 0:
      brackets = "\"";
      break;
    case 1:
      brackets = "()";
      break;
    case 2:
      brackets = "(())";
      break;
    case 3:
      brackets = "((()))";
      break;
    default:
      return "????";
  }
  graphd_variable_declaration_name(vdecl, &s, &e);
  snprintf(buf, size, "%.*s%.*s%s [%zu @ %p]",
           (int)(vdecl->vdecl_parentheses + !vdecl->vdecl_parentheses),
           brackets, (int)(e - s), s, brackets + vdecl->vdecl_parentheses,
           vdecl->vdecl_local, (void *)vdecl->vdecl_constraint);
  return buf;
}
