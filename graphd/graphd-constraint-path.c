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

/*  Given a request, we can reference a specific constraint
 *  inside that request with a pathname of dot-separated numbers.
 *
 *  Pathname has the form	NUMBER . NUMBER . NUMBER [etc.] . NUMBER
 *
 *  Given a tree of
 *			A
 *		     ,--`--.
 *                   B     C
 *		,----|---,
 *		D    E   F
 *			 |
 *                       G
 *
 *  The following pathnames resolve to the following constraints:
 *
 *  ""		- A
 *  "1"		- B
 *  "2"		- C
 *  "1.1"	- D
 *  "1.2"	- E
 *  "1.3"	- F
 *  "1.3.1"	- G
 */

/*  Append a constraint path name to the given buffer.
 */
int graphd_constraint_path(cl_handle* cl, graphd_constraint const* con,
                           cm_buffer* buf) {
  int err;
  size_t i = 0;
  graphd_constraint const* sub;

  if (con->con_parent) {
    err = graphd_constraint_path(cl, con->con_parent, buf);
    if (err != 0) return err;
  }
  if (con->con_parent && con->con_parent->con_parent) {
    err = cm_buffer_add_string(buf, ".");
    if (err != 0) return err;
  }

  i = 1;
  for (sub = con->con_parent->con_head; sub != NULL && sub != con;
       sub = sub->con_next)
    i++;
  cl_assert(cl, sub != NULL);

  return cm_buffer_sprintf(buf, "%zu", i);
}

int graphd_constraint_path_lookup(graphd_request* greq, char const* name_s,
                                  char const* name_e,
                                  graphd_constraint** con_out) {
  char const* r;

  *con_out = greq->greq_constraint;

  if (name_s == NULL || name_s == name_e) return 0;

  /*  Skip a prefix, if any.
   */
  r = memchr(name_s, ':', name_e - name_s);
  if (r)
    r++;
  else
    r = name_s;

  /*  Go down the chain.
   */
  while (r < name_e) {
    size_t i, n = 0;
    graphd_constraint* con;

    while (r < name_e && *r == '.') r++;
    if (r >= name_e || !isascii(*r) || !isdigit(*r)) break;

    while (r < name_e && isdigit(*r)) {
      n *= 10;
      n += *r - '0';
      r++;
    }
    for (i = 1, con = (*con_out)->con_head; i < n && con != NULL; i++)
      con = con->con_next;
    *con_out = con;
  }
  return *con_out == NULL ? GRAPHD_ERR_SEMANTICS : 0;
}
