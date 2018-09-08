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
#include "libpdb/pdbp.h"

#include <stdio.h>

void pdb_primitive_dump_loc(cl_handle *cl, pdb_primitive const *pr,
                            char const *file, int line) {
  if (pr == NULL)
    cl_log(cl, CL_LEVEL_DEBUG, "%s:%d  null primitive", file, line);
  else
    cl_log(cl, CL_LEVEL_DEBUG,
           "%s:%d  %p: addb_data { memory=%p, size=%lu; type %d }", file, line,
           (void *)pr, (void *)pr->pr_data.data_memory,
           (unsigned long)pr->pr_data.data_size, (int)pr->pr_data.data_type);
}

/*  {%llu: blue.name=\"%s\": %llu -> %llu}
 */
static char const *pdb_primitive_to_string_guid(graph_guid const *guid,
                                                char *buf, size_t size) {
  if (guid == NULL) return "null";
  if (GRAPH_GUID_IS_NULL(*guid)) return "*";
  snprintf(buf, size, "%llx", GRAPH_GUID_SERIAL(*guid));
  return buf;
}

char const *pdb_primitive_to_string(pdb_primitive const *pr, char *buf,
                                    size_t size) {
  int name_n, value_n;
  char link_buf[200];
  char val_buf[200];
  char type_buf[GRAPH_GUID_SIZE];
  char loc_buf[200];
  char b1[200], b2[200];

  graph_guid guid, left, right;
  char const *errmsg;

  if (pr == NULL) return "(null)";

  if ((errmsg = pdb_primitive_check(pr, buf, size)) != NULL) return errmsg;

  pdb_primitive_guid_get(pr, guid);
  if (pdb_primitive_has_left(pr)) {
    if (pdb_primitive_has_right(pr)) {
      pdb_primitive_right_get(pr, right);
      pdb_primitive_left_get(pr, left);
      snprintf(link_buf, sizeof link_buf, ": %s -> %s",
               pdb_primitive_to_string_guid(&left, b1, sizeof b1),
               pdb_primitive_to_string_guid(&right, b2, sizeof b2));
    } else {
      pdb_primitive_left_get(pr, left);
      snprintf(link_buf, sizeof link_buf, ": %s ->",
               pdb_primitive_to_string_guid(&left, b1, sizeof b1));
    }
  } else {
    if (pdb_primitive_has_right(pr)) {
      pdb_primitive_right_get(pr, right);
      snprintf(link_buf, sizeof link_buf, ": -> %s",
               pdb_primitive_to_string_guid(&right, b2, sizeof b2));
    } else
      *link_buf = '\0';
  }

  name_n = pdb_primitive_name_get_size(pr);
  value_n = pdb_primitive_value_get_size(pr);

  *val_buf = '\0';
  *loc_buf = '\0';

  if (pdb_primitive_has_typeguid(pr)) {
    graph_guid tg;

    pdb_primitive_typeguid_get(pr, tg);

    if (name_n > 0)
      snprintf(loc_buf, sizeof loc_buf, "%s.%.*s",
               graph_guid_to_string(&tg, type_buf, sizeof type_buf), name_n,
               pdb_primitive_name_get_memory(pr));
    else
      snprintf(loc_buf, sizeof loc_buf, "%s",
               graph_guid_to_string(&tg, type_buf, sizeof type_buf));
  } else {
    if (name_n > 0)
      snprintf(loc_buf, sizeof loc_buf, ".%.*s", name_n,
               pdb_primitive_name_get_memory(pr));
  }
  if (value_n)
    snprintf(val_buf, sizeof val_buf, ": %s=%.*s", loc_buf, value_n,
             pdb_primitive_value_get_memory(pr));
  else if (loc_buf[0])
    snprintf(val_buf, sizeof val_buf, ": %s", loc_buf);

  snprintf(buf, size, "{%llx%s%s}", (unsigned long long)GRAPH_GUID_SERIAL(guid),
           val_buf, link_buf);
  return buf;
}

/**
 * @brief Check that a primitive is internally consistent.
 *
 *  For example, no length inside the primitive must
 *  point outside of the primitive data.
 *
 * @param pr	the primitive data pointer
 * @param buf	data buffer for use in composing an error message
 * @param size	number of bytes pointed to by buf
 *
 * @return NULL if everything's okay, otherwise an
 * 	English error message detailing the problem.
 */
char const *pdb_primitive_check(pdb_primitive const *pr, char *buf,
                                size_t size) {
  unsigned long u;
  char guid_buf[GRAPH_GUID_SIZE];
  graph_guid guid;

  if (pr->pr_data.data_size < PDB_PRIMITIVE_NAME_OFFSET) {
    snprintf(buf, size, "primitive size %lu below minimum %u",
             (unsigned long)pr->pr_data.data_size, PDB_PRIMITIVE_NAME_OFFSET);
    return buf;
  }

  if (PDB_PRIMITIVE_HAS_VALUE(pr)) {
    u = PDB_PRIMITIVE_VALUE_OFFSET(pr);
    if (u > pr->pr_data.data_size) {
      pdb_primitive_guid_get(pr, guid);
      snprintf(buf, size,
               "%s: value offset %lu points outside "
               "the %zu-byte primitive",
               graph_guid_to_string(&guid, guid_buf, sizeof guid_buf), u,
               pr->pr_data.data_size);
      return buf;
    }
  }

  if (pdb_primitive_len(pr) > pr->pr_data.data_size) {
    snprintf(buf, size,
             "Primitive size: %lu is greater than"
             "allocated size: %zu.",
             (unsigned long)pdb_primitive_len(pr), pr->pr_data.data_size);
    return buf;
  }

  return NULL;
}
