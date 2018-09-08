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

#define IS_SPACE(ch) (isascii(ch) && isspace(ch))

#define IS_LIT(s, e, lit)                     \
  ((s) != NULL && e - s == sizeof(lit) - 1 && \
   !strncasecmp(s, lit, sizeof(lit) - 1))

static int cost_next_pair(graphd_request* greq, char const** s, char const* e,
                          char const** name_s, char const** name_e,
                          char const** value_s, char const** value_e) {
  char const* r = *s;

  while (r < e && IS_SPACE(*r)) r++;
  if (r >= e) return GRAPHD_ERR_NO;

  *name_s = r;
  while (r < e && !IS_SPACE(*r) && *r != '=') r++;
  *name_e = r;
  while (r < e && IS_SPACE(*r)) r++;

  if (*name_s == *name_e) {
    graphd_request_errprintf(greq, false,
                             "SYNTAX error "
                             "parsing cost element at \"%.*s\" -- "
                             "expected \"name=value\"",
                             e - *name_s > 100 ? 100 : (int)(e - *name_s),
                             *name_s);
    return GRAPHD_ERR_SYNTAX;
  }

  if (r >= e) {
    graphd_request_errprintf(
        greq, false,
        "SYNTAX error "
        "parsing cost element at \"%.*s\" -- expected '=', "
        "got end-of-string",
        e - *name_s > 100 ? 100 : (int)(e - *name_s), *name_s);
    return GRAPHD_ERR_SYNTAX;
  }
  if (*r != '=') {
    graphd_request_errprintf(
        greq, false,
        "SYNTAX error "
        "parsing cost element at \"%.*s\" -- expected '=', "
        "got '%c'",
        e - *name_s > 100 ? 100 : (int)(e - *name_s), *name_s, *r);
    return GRAPHD_ERR_SYNTAX;
  }
  r++;
  while (r < e && IS_SPACE(*r)) r++;
  *value_s = r;
  while (r < e && !IS_SPACE(*r)) r++;
  *value_e = r;

  if (*value_s >= *value_e) {
    graphd_request_errprintf(greq, false,
                             "SYNTAX error "
                             "parsing cost element at \"%.*s\" -- "
                             "expected \"name=value\"",
                             e - *name_s > 100 ? 100 : (int)(e - *name_s),
                             *name_s);
    return GRAPHD_ERR_SYNTAX;
  }
  *s = r;
  return 0;
}

static void cost_scan(graphd_request* greq, unsigned long long* var_out,
                      char const* meaning, char const* s, char const* e) {
  unsigned long ull;
  char const* s0 = s;

  ull = 0;

  for (; s < e && isascii(*s) && isdigit(*s); s++) {
    unsigned long long tmp;
    tmp = ull * 10 + (*s - '0');
    if (tmp < ull) {
      graphd_request_errprintf(greq, false,
                               "SYNTAX overflow error while parsing cost "
                               "element \"%.*s\" -- expected %s",
                               e - s0 > 100 ? 100 : (int)(e - s0), s0, meaning);
      return;
    }
    ull = tmp;
  }
  if (s < e) {
    graphd_request_errprintf(greq, false,
                             "SYNTAX unexpected %scost element \"%.*s\" -- "
                             "expected %s",
                             s == s0 ? "" : "trailing data in ",
                             e - s0 > 100 ? 100 : (int)(e - s0), s0, meaning);
    return;
  }
  *var_out = ull;
}

//
// FIXME:
//
//	Does the function below duplicate the functionality of
//	graphd_cost_from_string() ?
//

/*  The caller is sending in a budget as cost="...".
 *
 *  Parse the contents of the budget, and modify the request
 *  allowance accordingly.
 */
void graphd_cost_parse(graphd_request* greq, gdp_token const* tok,
                       graphd_runtime_statistics* a) {
  char const* s;
  char const *name_s, *name_e;
  char const *val_s, *val_e;
  cl_handle* cl = graphd_request_cl(greq);

  s = tok->tkn_start;
  if (s == NULL) return;

  while (greq->greq_error_message == NULL &&
         cost_next_pair(greq, &s, tok->tkn_end, &name_s, &name_e, &val_s,
                        &val_e) == 0) {
    if (greq->greq_error_message != NULL) {
      cl_cover(cl);
      return;
    }

    if (IS_LIT(name_s, name_e, "ts")) {
      cost_scan(greq, &a->grts_system_millis, "milliseconds", val_s, val_e);
      a->grts_system_micros = a->grts_system_millis * 1000;
    } else if (IS_LIT(name_s, name_e, "tu")) {
      cost_scan(greq, &a->grts_user_millis, "milliseconds", val_s, val_e);
      a->grts_user_micros = a->grts_user_millis * 1000;
    } else if (IS_LIT(name_s, name_e, "tr")) {
      cost_scan(greq, &a->grts_wall_millis, "milliseconds", val_s, val_e);
      a->grts_wall_micros = a->grts_wall_millis * 1000;
    } else if (IS_LIT(name_s, name_e, "te")) {
      cost_scan(greq, &a->grts_endtoend_millis, "milliseconds", val_s, val_e);
      a->grts_endtoend_micros = a->grts_endtoend_millis * 1000;
    } else if (IS_LIT(name_s, name_e, "pr"))
      cost_scan(greq, &a->grts_minflt, "page reclaims", val_s, val_e);
    else if (IS_LIT(name_s, name_e, "pf"))
      cost_scan(greq, &a->grts_majflt, "page faults", val_s, val_e);
    else if (IS_LIT(name_s, name_e, "va"))
      cost_scan(greq, &a->grts_values_allocated, "values allocated", val_s,
                val_e);
    else if (IS_LIT(name_s, name_e, "dr"))
      cost_scan(greq, &a->grts_pdb.rts_primitives_read, "data primitives read",
                val_s, val_e);
    else if (IS_LIT(name_s, name_e, "dw"))
      cost_scan(greq, &a->grts_pdb.rts_primitives_written,
                "data primitives written", val_s, val_e);
    else if (IS_LIT(name_s, name_e, "ir"))
      cost_scan(greq, &a->grts_pdb.rts_index_elements_read,
                "index entries read", val_s, val_e);
    else if (IS_LIT(name_s, name_e, "iw"))
      cost_scan(greq, &a->grts_pdb.rts_index_elements_written,
                "index entries written", val_s, val_e);
    else if (IS_LIT(name_s, name_e, "in"))
      cost_scan(greq, &a->grts_pdb.rts_index_extents_read, "indices sized",
                val_s, val_e);
    else {
      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_cost_parse: unknown cost \"%.*s\" -- "
             "ignored (known: ts tu tr te pr pf va dr dw ir iw in)",
             (int)(val_e - name_s), name_s);
    }
  }
  cl_cover(cl);
}
