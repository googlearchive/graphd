/*
Copyright 2018 Google Inc. All rights reserved.
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

#include <ctype.h>
#include <limits.h>
#include "gld.h"

gld_primitive* gld_var_create(gld_handle* gld, char const* name_s,
                              char const* name_e) {
  gld_primitive* pr;

  if (gld->gld_var == NULL) {
    gld->gld_var = cm_hcreate(gld->gld_cm, gld_primitive, 256);
    if (gld->gld_var == NULL) return NULL;
  }

  pr = cm_hnew(gld->gld_var, gld_primitive, name_s, name_e - name_s);

  if (pr->pr_tail == NULL)
    gld_primitive_set_nil(gld, pr);
  else
    gld_primitive_free_contents(gld, pr);

  return pr;
}

/* Scan a dot, followed by a number, out of [*s..e).  Advance *s.
 */
static int dot_number(char const** s, char const* e, int* num) {
  char const* p;
  int sign = 1;

  if ((p = *s) >= e) return ENOENT;

  if (p < e && *p == '.') p++;
  if (p >= e) return EILSEQ;
  if (*p == '-')
    sign = -1, p++;
  else if (*p == '+')
    p++;

  *num = 0;
  if (p >= e || !isascii(*p) || !isdigit(*p)) return EILSEQ;
  while (p < e && isascii(*p) && isdigit(*p)) {
    if (*num > (INT_MAX - (*p - '0')) / 10) return ERANGE;

    *num = *num * 10 + (*p - '0');
    p++;
  }
  *num *= sign;
  *s = p;

  return 0;
}

/*  Look up a scalar, given a name with a trailing path expression.
 *
 */
graph_guid const* gld_var_lookup(gld_handle* gld, char const* name_s,
                                 char const* name_e) {
  char const* square;
  gld_primitive* pr;

  cl_log(gld->gld_cl, CL_LEVEL_VERBOSE, "lookup %.*s", (int)(name_e - name_s),
         name_s);

  for (square = name_s; square < name_e; square++)
    if ((isascii(*square) && isspace(*square)) || *square == '[') break;

  /*  If we have a request outstanding for that name,
   *  wait for it to complete.
   */
  gld_request_wait(gld, name_s, square);

  if (gld->gld_var == NULL) {
    cl_log(gld->gld_cl, CL_LEVEL_FAIL, "%.*s: no variables registered",
           (int)(name_e - name_s), name_s);
    return NULL;
  }
  pr = cm_haccess(gld->gld_var, gld_primitive, name_s, square - name_s);
  if (pr == NULL) {
    cl_log(gld->gld_cl, CL_LEVEL_FAIL, "%.*s: no such variable",
           (int)(name_e - name_s), name_s);
    return NULL;
  }

  if (square < name_e && *square == '[') {
    int num, err;

    square++;
    while (!(err = dot_number(&square, name_e - 1, &num))) {
      if (!gld_primitive_is_list(gld, pr)) {
        cl_log(gld->gld_cl, CL_LEVEL_FAIL,
               "gld_var_lookup: trying to"
               " index something that isn't "
               "a list (in %.*s)",
               (int)(name_e - name_s), name_s);
        return NULL;
      }

      if (num < 0) {
        num += gld_primitive_n(gld, pr);
        if (num < 0) {
          cl_log(gld->gld_cl, CL_LEVEL_FAIL,
                 "gld_var_lookup: negative"
                 " index out of bounds "
                 "(in %.*s)",
                 (int)(name_e - name_s), name_s);
          return NULL;
        }
      }
      pr = gld_primitive_nth(gld, pr, num);
      if (pr == NULL) {
        cl_log(gld->gld_cl, CL_LEVEL_FAIL,
               "gld_var_lookup: no %d'th node "
               "(in %.*s)",
               num, (int)(name_e - name_s), name_s);
        return NULL;
      }
    }
    if (err != ENOENT) {
      cl_log(gld->gld_cl, CL_LEVEL_FAIL,
             "gld_var_lookup: error looking up element"
             " (in %.*s): %s",
             (int)(name_e - name_s), name_s, strerror(err));
      return NULL;
    }
    if (square < name_e && *square == ']') square++;
  }

  /* Descend into lists until we hit a single GUID element.
   */
  while (pr->pr_head != NULL && pr->pr_tail != NULL) pr = pr->pr_head;
  if (pr->pr_tail != NULL) return NULL;

  return &pr->pr_guid;
}
