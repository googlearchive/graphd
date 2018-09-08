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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include "libcl/cl.h"
#include "libcm/cm.h"

int pdb_status(pdb_handle* pdb, pdb_status_callback* cb, void* cb_data) {
  int err;
  char num_buf[42], name_buf[200];
  int i;
  cm_prefix prefix;

  if (pdb == NULL) return EINVAL;

  prefix = cm_prefix_initialize(name_buf, sizeof name_buf);

  err = (*cb)(cb_data, "pdb.path", pdb->pdb_path);
  if (err != 0) return err;

  snprintf(num_buf, sizeof num_buf, "%d", pdb->pdb_predictable);
  err = (*cb)(cb_data, "pdb.predictable", num_buf);
  if (err) return err;

  snprintf(num_buf, sizeof num_buf, "%llu", pdb->pdb_database_id);
  err = (*cb)(cb_data, "pdb.database-id", num_buf);
  if (err) return err;

  snprintf(num_buf, sizeof num_buf, "%llu", pdb_checkpoint_deficit(pdb));
  err = (*cb)(cb_data, "pdb.checkpoint-deficit", num_buf);
  if (err) return err;

  err = addb_istore_status(pdb->pdb_primitive, &prefix, cb, cb_data);
  if (err) return err;

  err = addb_status(pdb->pdb_addb, &prefix, cb, cb_data);

  for (i = 0; i < PDB_INDEX_N; i++) {
    pdb_index_instance* const ii = &pdb->pdb_indices[i];
    char const* name;
    cm_prefix index_prefix;

    name = pdb_index_name(i);
    if (name != NULL)
      index_prefix = cm_prefix_push(&prefix, name);
    else
      index_prefix = prefix;

    err = (*ii->ii_type->ixt_status)(pdb, ii, &index_prefix, cb, cb_data);
    if (err) return err;
  }
  return 0;
}

int pdb_status_tiles(pdb_handle* pdb, pdb_status_callback* cb, void* cb_data) {
  int err;
  char name_buf[200];
  int i;
  cm_prefix prefix;

  if (pdb == NULL) return EINVAL;

  prefix = cm_prefix_initialize(name_buf, sizeof name_buf);

  err = addb_istore_status_tiles(pdb->pdb_primitive, &prefix, cb, cb_data);
  if (err) return err;

  for (i = 0; i < PDB_INDEX_N; i++) {
    pdb_index_instance* const ii = &pdb->pdb_indices[i];
    char const* name;
    cm_prefix index_prefix;

    name = pdb_index_name(i);
    if (name != NULL)
      index_prefix = cm_prefix_push(&prefix, name);
    else
      index_prefix = prefix;

    err = (*ii->ii_type->ixt_status_tiles)(pdb, ii, &index_prefix, cb, cb_data);
    if (err) return err;
  }
  return 0;
}
