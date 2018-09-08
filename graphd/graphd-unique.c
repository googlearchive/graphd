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

#define SEMANTICS_ERROR(req, msg)                  \
  do {                                             \
    graphd_request_error((req), "SEMANTICS " msg); \
    return GRAPHD_ERR_SEMANTICS;                   \
  } while (0)

#define GUIDCON_HAS_GUID(x)                                     \
  ((x).guidcon_op == GRAPHD_OP_EQ && (x).guidcon_guid_n == 1 && \
   !GRAPH_GUID_IS_NULL((x).guidcon_guid[0]))

static int const graphd_unique_constraint_mask =
    (1 << GRAPHD_PATTERN_VALUETYPE) | (1 << GRAPHD_PATTERN_LEFT) |
    (1 << GRAPHD_PATTERN_NAME) | (1 << GRAPHD_PATTERN_RIGHT) |
    (1 << GRAPHD_PATTERN_SCOPE) | (1 << GRAPHD_PATTERN_TIMESTAMP) |
    (1 << GRAPHD_PATTERN_TYPEGUID) | (1 << GRAPHD_PATTERN_VALUE);

static int const graphd_unique_constraints[] = {

    1 << GRAPHD_PATTERN_VALUETYPE,
    1 << GRAPHD_PATTERN_LEFT,
    1 << GRAPHD_PATTERN_NAME,
    1 << GRAPHD_PATTERN_RIGHT,
    1 << GRAPHD_PATTERN_SCOPE,
    1 << GRAPHD_PATTERN_TIMESTAMP,
    1 << GRAPHD_PATTERN_TYPEGUID,
    1 << GRAPHD_PATTERN_VALUE,
    0};

/**
 * @brief Return a human-readable (partial) string representation of a
 *	"unique" instruction.
 * @param u 	NULL or the unique instruction to format
 * @param buf 	use this buffer to format
 * @param size	number of  usable bytes pointed ot by buf
 * @return a pointer to the beginning of the NUL-terminated string.
 */
char const* graphd_unique_to_string(int u, char* buf, size_t size) {
  char *w, *e;
  char const* sep = "";
  char const* b0 = buf;
  int i;

  u &= ~graphd_unique_constraint_mask;

  switch (u) {
    case 0:
      return "null";
    case 1 << GRAPHD_PATTERN_VALUETYPE:
      return "valuetype";
    case 1 << GRAPHD_PATTERN_LEFT:
      return "left";
    case 1 << GRAPHD_PATTERN_NAME:
      return "name";
    case 1 << GRAPHD_PATTERN_RIGHT:
      return "right";
    case 1 << GRAPHD_PATTERN_SCOPE:
      return "scope";
    case 1 << GRAPHD_PATTERN_TIMESTAMP:
      return "timestamp";
    case 1 << GRAPHD_PATTERN_TYPEGUID:
      return "typeguid";
    case 1 << GRAPHD_PATTERN_VALUE:
      return "value";
    default:
      break;
  }

  if (size <= 10) return "(...)";

  e = buf + size - 5;
  w = buf;
  *w++ = '(';

  for (i = 0; graphd_unique_constraints[i]; i++) {
    char const* ptr;
    char b[200];
    int mask = graphd_unique_constraints[i];

    if (!(u & mask)) continue;

    ptr = graphd_unique_to_string(mask, b, sizeof b);
    snprintf(w, (int)(e - w), "%s%s", sep, ptr);
    sep = " ";
    if (w < e) w += strlen(w);

    if (e - w <= 5) break;
    u &= ~mask;
  }
  *w++ = ')';
  *w = '\0';

  return b0;
}

/**
 * @brief Did the application specify values for the criteria
 *  	whose uniqueness it wants us to check?
 * @param greq	request that's asking
 * @param con	constraint that we're asking about
 * @param u	parsed list of criteria
 */
int graphd_unique_parse_check(graphd_request* greq,
                              graphd_constraint const* con, int u) {
  int linkage;
  int pat = graphd_constraint_linkage_pattern(con);
  cl_handle* cl = graphd_request_cl(greq);

  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++) {
    if ((u & (1 << GRAPHD_PATTERN_LINKAGE(linkage))) &&
        !(pat & (1 << GRAPHD_PATTERN_LINKAGE(linkage)))) {
      char const* linkage_name;

      if (linkage == PDB_LINKAGE_TYPEGUID &&
          con->con_type.strqueue_head != NULL)
        continue;

      /* No, we can't find it - complain.
       */
      linkage_name = pdb_linkage_to_string(linkage);
      graphd_request_errprintf(greq, 0,
                               "SEMANTICS request for %s uniqueness "
                               "without specifying a %s?",
                               linkage_name, linkage_name);
      return GRAPHD_ERR_SEMANTICS;
    }
  }

  if (u & ((1 << GRAPHD_PATTERN_DATATYPE) | (1 << GRAPHD_PATTERN_VALUETYPE))) {
    if (con->con_valuetype == GRAPH_DATA_UNSPECIFIED) {
      cl_cover(cl);
      SEMANTICS_ERROR(greq,
                      "request for data- or valuetype "
                      "uniqueness without specifying a "
                      "data- or valuetype?");
    }
    cl_cover(cl);
  }

  if (u & (1 << GRAPHD_PATTERN_TIMESTAMP)) {
    if (!con->con_timestamp_valid) {
      cl_cover(cl);
      SEMANTICS_ERROR(greq,
                      "request for timestamp "
                      "uniqueness without specifying a timestamp?");
    }
    cl_cover(cl);
  }
  if (u & (1 << GRAPHD_PATTERN_NAME)) {
    if (con->con_name.strqueue_head == NULL) {
      cl_cover(cl);
      SEMANTICS_ERROR(greq,
                      "request for name "
                      "uniqueness without specifying a name?");
    }
  }
  if (u & (1 << GRAPHD_PATTERN_VALUE)) {
    if (con->con_value.strqueue_head == NULL) {
      cl_cover(cl);
      SEMANTICS_ERROR(greq,
                      "request for value "
                      "uniqueness without specifying a value?");
    }
    cl_cover(cl);
  }
  return 0;
}
