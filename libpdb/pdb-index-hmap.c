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

static int pdb_hmi_close(pdb_handle* pdb, pdb_index_instance* ii) {
  return addb_hmap_close(ii->ii_impl.hm);
}

static int pdb_hmi_truncate(pdb_handle* pdb, pdb_index_instance* ii) {
  return addb_hmap_truncate(ii->ii_impl.hm, ii->ii_path);
}

static int pdb_hmi_status(pdb_handle* pdb, pdb_index_instance* ii,
                          cm_prefix const* prefix,
                          pdb_status_callback* callback, void* callback_data) {
  return addb_hmap_status(ii->ii_impl.hm, prefix, callback, callback_data);
}

static int pdb_hmi_status_tiles(pdb_handle* pdb, pdb_index_instance* ii,
                                cm_prefix const* prefix,
                                pdb_status_callback* callback,
                                void* callback_data) {
  return addb_hmap_status_tiles(ii->ii_impl.hm, prefix, callback,
                                callback_data);
}

static unsigned long long pdb_hmi_horizon(pdb_handle* pdb,
                                          pdb_index_instance* ii) {
  cl_assert(pdb->pdb_cl, ii->ii_impl.hm);

  return addb_hmap_horizon(ii->ii_impl.hm);
}

static void pdb_hmi_advance_horizon(pdb_handle* pdb, pdb_index_instance* ii,
                                    unsigned long long horizon) {
  cl_assert(pdb->pdb_cl, ii->ii_impl.hm);
  cl_assert(pdb->pdb_cl, horizon >= addb_hmap_horizon(ii->ii_impl.hm));

  addb_hmap_horizon_set(ii->ii_impl.hm, horizon);
}

static int pdb_hmi_rollback(pdb_handle* pdb, pdb_index_instance* ii) {
  return addb_hmap_checkpoint_rollback(ii->ii_impl.hm);
}

static int pdb_hmi_finish_backup(pdb_index_instance* ii, bool hard_sync,
                                 bool block) {
  addb_hmap* const hm = ii->ii_impl.hm;

  return addb_hmap_checkpoint_finish_backup(hm, hard_sync, block);
}

static int pdb_hmi_sync_backup(pdb_index_instance* ii, bool hard_sync,
                               bool block) {
  addb_hmap* const hm = ii->ii_impl.hm;

  return addb_hmap_checkpoint_sync_backup(hm, hard_sync, block);
}

static int pdb_hmi_start_writes(pdb_index_instance* ii, bool hard_sync,
                                bool block) {
  addb_hmap* const hm = ii->ii_impl.hm;

  return addb_hmap_checkpoint_start_writes(hm, hard_sync, block);
}

static int pdb_hmi_finish_writes(pdb_index_instance* ii, bool hard_sync,
                                 bool block) {
  addb_hmap* const hm = ii->ii_impl.hm;

  return addb_hmap_checkpoint_finish_writes(hm, hard_sync, block);
}

static int pdb_hmi_remove_backup(pdb_index_instance* ii, bool hard_sync,
                                 bool block) {
  addb_hmap* const hm = ii->ii_impl.hm;

  return addb_hmap_checkpoint_remove_backup(hm, hard_sync, block);
}

static int pdb_hmi_sync_directory(pdb_index_instance* ii, bool hard_sync,
                                  bool block) {
  addb_hmap* const hm = ii->ii_impl.hm;
  return addb_hmap_checkpoint_sync_directory(hm, hard_sync, block);
}

static int pdb_hmi_refresh(pdb_handle* pdb, pdb_index_instance* ii,
                           unsigned long long pdb_n) {
  return addb_hmap_refresh(ii->ii_impl.hm, pdb_n);
}

pdb_index_type pdb_index_hmap = {
    "hmap",
    pdb_hmi_close,
    pdb_hmi_truncate,
    pdb_hmi_status,
    pdb_hmi_status_tiles,
    pdb_hmi_horizon,
    pdb_hmi_advance_horizon,
    pdb_hmi_rollback,
    pdb_hmi_refresh,
    {pdb_hmi_finish_backup, pdb_hmi_sync_backup, pdb_hmi_sync_directory,
     pdb_hmi_start_writes, pdb_hmi_finish_writes, NULL, NULL,
     pdb_hmi_remove_backup}};
