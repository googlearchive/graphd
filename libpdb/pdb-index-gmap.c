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

/* GMAP index types
 */

static int pdb_gmi_close(pdb_handle* pdb, pdb_index_instance* ii) {
  return addb_gmap_close(ii->ii_impl.gm);
}

static int pdb_gmi_truncate(pdb_handle* pdb, pdb_index_instance* ii) {
  return addb_gmap_truncate(ii->ii_impl.gm, ii->ii_path);
}

static int pdb_gmi_status(pdb_handle* pdb, pdb_index_instance* ii,
                          cm_prefix const* prefix,
                          pdb_status_callback* callback, void* callback_data) {
  return addb_gmap_status(ii->ii_impl.gm, prefix, callback, callback_data);
}

static int pdb_gmi_status_tiles(pdb_handle* pdb, pdb_index_instance* ii,
                                cm_prefix const* prefix,
                                pdb_status_callback* callback,
                                void* callback_data) {
  return addb_gmap_status_tiles(ii->ii_impl.gm, prefix, callback,
                                callback_data);
}

static unsigned long long pdb_gmi_horizon(pdb_handle* pdb,
                                          pdb_index_instance* ii) {
  cl_assert(pdb->pdb_cl, ii->ii_impl.gm);

  return addb_gmap_horizon(ii->ii_impl.gm);
}

static void pdb_gmi_advance_horizon(pdb_handle* pdb, pdb_index_instance* ii,
                                    unsigned long long horizon) {
  cl_assert(pdb->pdb_cl, ii->ii_impl.gm);
  cl_assert(pdb->pdb_cl, horizon >= addb_gmap_horizon(ii->ii_impl.gm));

  addb_gmap_horizon_set(ii->ii_impl.gm, horizon);
}

static int pdb_gmi_rollback(pdb_handle* pdb, pdb_index_instance* ii) {
  return addb_gmap_checkpoint_rollback(ii->ii_impl.gm);
}

static int pdb_gmi_finish_backup(pdb_index_instance* ii, bool hard_sync,
                                 bool block) {
  addb_gmap* const gm = ii->ii_impl.gm;

  return addb_gmap_checkpoint_finish_backup(gm, hard_sync, block);
}

static int pdb_gmi_sync_backup(pdb_index_instance* ii, bool hard_sync,
                               bool block) {
  addb_gmap* const gm = ii->ii_impl.gm;

  return addb_gmap_checkpoint_sync_backup(gm, hard_sync, block);
}

static int pdb_gmi_start_writes(pdb_index_instance* ii, bool hard_sync,
                                bool block) {
  addb_gmap* const gm = ii->ii_impl.gm;

  return addb_gmap_checkpoint_start_writes(gm, hard_sync, block);
}

static int pdb_gmi_finish_writes(pdb_index_instance* ii, bool hard_sync,
                                 bool block) {
  addb_gmap* const gm = ii->ii_impl.gm;

  return addb_gmap_checkpoint_finish_writes(gm, hard_sync, block);
}

static int pdb_gmi_remove_backup(pdb_index_instance* ii, bool hard_sync,
                                 bool block) {
  addb_gmap* const gm = ii->ii_impl.gm;

  return addb_gmap_checkpoint_remove_backup(gm, hard_sync, block);
}

static int pdb_gmi_sync_directory(pdb_index_instance* ii, bool hard_sync,
                                  bool block) {
  addb_gmap* const gm = ii->ii_impl.gm;

  return addb_gmap_checkpoint_sync_directory(gm, hard_sync, block);
}

static int pdb_gmi_refresh(pdb_handle* pdb, pdb_index_instance* ii,
                           unsigned long long pdb_n) {
  addb_gmap* gm = ii->ii_impl.gm;

  return addb_gmap_refresh(gm, pdb_n);
}

pdb_index_type pdb_index_gmap = {
    "gmap",
    pdb_gmi_close,
    pdb_gmi_truncate,
    pdb_gmi_status,
    pdb_gmi_status_tiles,
    pdb_gmi_horizon,
    pdb_gmi_advance_horizon,
    pdb_gmi_rollback,
    pdb_gmi_refresh,
    {pdb_gmi_finish_backup, pdb_gmi_sync_backup, pdb_gmi_sync_directory,
     pdb_gmi_start_writes, pdb_gmi_finish_writes, NULL, NULL,
     pdb_gmi_remove_backup}};
