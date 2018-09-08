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

#include <stdio.h>
#include <string.h>

static char const *pdb_primitive_to_string(graph_guid const *guid, char *buf,
                                           size_t size) {
  if (GRAPH_GUID_IS_NULL(*guid)) return "*";
  snprintf(buf, size, "%llx", GRAPH_GUID_SERIAL(*guid));
  return buf;
}

char const *pdb_primitive_to_string(pdb_primitive const *pr, char *buf,
                                    size_t size) {
  int type_n, name_n, value_n;
  char link_buf[200];
  char val_buf[200];
  char loc_buf[200];
  char b1[200], b2[200];

  graph_guid guid, left, right;

  if (pr == NULL) return "(null)";

  pdb_primitive_guid_get(pr, guid);
  if (pdb_primitive_is_node(pr)) {
  }

  switch (pdb_primitive_nlinks_get(pr)) {
    case 0:
      *link_buf = '\0';
      break;

    case 1:
      pdb_primitive_left_get(pr, left);
      snprintf(link_buf, sizeof link_buf, ": %s ->",
               pdb_primitive_to_string(&left, b1, sizeof b1));
      break;

    case 2:
      pdb_primitive_right_get(pr, right);
      pdb_primitive_left_get(pr, left);
      snprintf(link_buf, sizeof link_buf, ": %s -> %s",
               pdb_primitive_to_string(&left, b1, sizeof b1),
               pdb_primitive_to_string(&right, b2, sizeof b2));
      break;
    default:
      snprintf(link_buf, sizeof link_buf,
               "ERROR pdb_primitive_to_string: %p with %d links?", (void *)pr,
               pdb_primitive_nlinks_get(pr));
  }

  type_n = pdb_primitive_type_get_size(pr);
  name_n = pdb_primitive_name_get_size(pr);
  value_n = pdb_primitive_value_get_size(pr);

  *val_buf = '\0';
  *loc_buf = '\0';

  if (type_n > 0) {
    if (name_n > 0)
      snprintf(loc_buf, sizeof loc_buf, "%.*s.%.*s", type_n,
               pdb_primitive_type_get_memory(pr), name_n,
               pdb_primitive_name_get_memory(pr));
    else
      snprintf(loc_buf, sizeof loc_buf, "%.*s", type_n,
               pdb_primitive_type_get_memory(pr));
  } else if (name_n > 0)
    snprintf(loc_buf, sizeof loc_buf, ".%.*s", name_n,
             pdb_primitive_name_get_memory(pr));
  if (value_n)
    snprintf(val_buf, sizeof val_buf, ": %s=%.*s", loc_buf, value_n,
             pdb_primitive_value_get_memory(pr));
  else if (loc_buf[0])
    snprintf(val_buf, sizeof val_buf, ": %s", loc_buf);

  snprintf(buf, size, "{%llx%s%s}", (unsigned long long)GRAPH_GUID_SERIAL(guid),
           val_buf, link_buf);
  return buf;
}
