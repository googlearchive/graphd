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

#include "libaddb/addb-bmap.h"

/* GMAP index types
 */

static int pdb_bmi_close(pdb_handle* pdb, pdb_index_instance* ii) {
  return addb_bmap_close(ii->ii_impl.bm);
}

/* FIXME */
/* Thisis going to be wierd */
static int pdb_bmi_truncate(pdb_handle* pdb, pdb_index_instance* ii) {
  return addb_bmap_truncate(ii->ii_impl.bm);
}

static int pdb_bmi_status(pdb_handle* pdb, pdb_index_instance* ii,
                          cm_prefix const* prefix,
                          pdb_status_callback* callback, void* callback_data) {
  return addb_bmap_status(ii->ii_impl.bm, prefix, callback, callback_data);
}

static int pdb_bmi_status_tiles(pdb_handle* pdb, pdb_index_instance* ii,
                                cm_prefix const* prefix,
                                pdb_status_callback* callback,
                                void* callback_data) {
  return addb_bmap_status_tiles(ii->ii_impl.bm, prefix, callback,
                                callback_data);
}

static unsigned long long pdb_bmi_horizon(pdb_handle* pdb,
                                          pdb_index_instance* ii) {
  cl_assert(pdb->pdb_cl, ii->ii_impl.bm);

  return addb_bmap_horizon(ii->ii_impl.bm);
}

static void pdb_bmi_advance_horizon(pdb_handle* pdb, pdb_index_instance* ii,
                                    unsigned long long horizon) {
  cl_assert(pdb->pdb_cl, ii->ii_impl.bm);
  cl_assert(pdb->pdb_cl, horizon >= addb_bmap_horizon(ii->ii_impl.bm));

  addb_bmap_horizon_set(ii->ii_impl.bm, horizon);
}

static int pdb_bmi_rollback(pdb_handle* pdb, pdb_index_instance* ii) {
  return addb_bmap_checkpoint_rollback(ii->ii_impl.bm);
}

static int pdb_bmi_finish_backup(pdb_index_instance* ii, bool hard_sync,
                                 bool block) {
  return addb_bmap_checkpoint_finish_backup(ii->ii_impl.bm, hard_sync, block);
}

static int pdb_bmi_sync_backup(pdb_index_instance* ii, bool hard_sync,
                               bool block) {
  return addb_bmap_checkpoint_sync_backup(ii->ii_impl.bm, hard_sync, block);
}

static int pdb_bmi_start_writes(pdb_index_instance* ii, bool hard_sync,
                                bool block) {
  return addb_bmap_checkpoint_start_writes(ii->ii_impl.bm, hard_sync, block);
}

static int pdb_bmi_finish_writes(pdb_index_instance* ii, bool hard_sync,
                                 bool block) {
  return addb_bmap_checkpoint_finish_writes(ii->ii_impl.bm, hard_sync, block);
}

static int pdb_bmi_remove_backup(pdb_index_instance* ii, bool hard_sync,
                                 bool block) {
  return addb_bmap_checkpoint_remove_backup(ii->ii_impl.bm, hard_sync, block);
}

static int pdb_bmi_refresh(pdb_handle* pdb, pdb_index_instance* ii,
                           unsigned long long pdb_n) {
  return addb_bmap_refresh(ii->ii_impl.bm, pdb_n);
}

pdb_index_type pdb_index_bmap = {
    "bmap",
    pdb_bmi_close,
    pdb_bmi_truncate,
    pdb_bmi_status,
    pdb_bmi_status_tiles,
    pdb_bmi_horizon,
    pdb_bmi_advance_horizon,
    pdb_bmi_rollback,
    pdb_bmi_refresh,
    {pdb_bmi_finish_backup, pdb_bmi_sync_backup, NULL, pdb_bmi_start_writes,
     pdb_bmi_finish_writes, NULL, NULL, pdb_bmi_remove_backup}};
