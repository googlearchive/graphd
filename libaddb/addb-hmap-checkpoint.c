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
#include "libaddb/addb.h"
#include "libaddb/addb-hmap.h"
#include "libaddb/addbp.h"

#include <errno.h>
#include <time.h>

int addb_hmap_checkpoint_rollback(addb_hmap* hm) {
  int err = 0;

  err = addb_tiled_read_backup(hm->hmap_td, hm->hmap_horizon);
  if (err && err != ADDB_ERR_ALREADY && err != ENOENT) {
    cl_log_errno(hm->hmap_addb->addb_cl, CL_LEVEL_FAIL,
                 "addb_tiled_read_backup", err, "Unable to read backup");
    return err;
  }
  err = addb_gmap_checkpoint_rollback(hm->hmap_gm);
  if (err && err != ADDB_ERR_ALREADY && err != ENOENT) {
    cl_log_errno(hm->hmap_addb->addb_cl, CL_LEVEL_FAIL,
                 "addb_gmap_checkpoint_rollback", err, "Unable to read backup");
    return err;
  }

  if (!err)
    cl_log(hm->hmap_addb->addb_cl, CL_LEVEL_SPEW, "%s: rolled back to %llu.",
           hm->hmap_dir_path, hm->hmap_horizon);

  return err;
}

int addb_hmap_backup(addb_hmap* hm, unsigned long long horizon) {
  int err;

  err = addb_gmap_backup(hm->hmap_gm, horizon);
  if (err) return err;
  err = addb_tiled_backup(hm->hmap_td, 1);
  if (err) return err;

  hm->hmap_horizon = horizon;
  hm->hmap_backup = true;

  return 0;
}

/* HMAP checkpoint stages...
 */

int addb_hmap_checkpoint_finish_backup(addb_hmap* hm, bool hard_sync,
                                       bool block) {
  int err;

  err = addb_gmap_checkpoint_finish_backup(hm->hmap_gm, hard_sync, block);
  if (err) return err;

  err = addb_tiled_checkpoint_finish_backup(hm->hmap_td, hm->hmap_horizon,
                                            hard_sync, block);
  if (err) return err;

  return 0;
}

int addb_hmap_checkpoint_sync_backup(addb_hmap* hm, bool hard_sync,
                                     bool block) {
  int err;

  err = addb_gmap_checkpoint_sync_backup(hm->hmap_gm, hard_sync, block);
  if (err) return err;

  err = addb_tiled_checkpoint_sync_backup(hm->hmap_td, hm->hmap_horizon,
                                          hard_sync, block);
  if (err) return err;

  if (hard_sync) {
    err =
        addb_file_sync_start(hm->hmap_addb->addb_cl, hm->hmap_dir_fd,
                             &hm->hmap_dir_fsync_ctx, hm->hmap_dir_path, true);

    cl_assert(hm->hmap_addb->addb_cl, err != ADDB_ERR_MORE);
  }

  return err;
}

int addb_hmap_checkpoint_sync_directory(addb_hmap* hm, bool hard_sync,
                                        bool block) {
  int err;

#if ADDB_FSYNC_DIRECTORY

  err = addb_gmap_checkpoint_sync_directory(hm->hmap_gm, hard_sync, block);

  if (err) return err;

  err = addb_file_sync_finish(hm->hmap_addb->addb_cl, &hm->hmap_dir_fsync_ctx,
                              block, hm->hmap_dir_path);
#else
  err = 0;
#endif
  return err;
}

int addb_hmap_checkpoint_start_writes(addb_hmap* hm, bool hard_sync,
                                      bool block) {
  int err;

  err = addb_gmap_checkpoint_start_writes(hm->hmap_gm, hard_sync, block);
  if (err) return err;

  err = addb_tiled_checkpoint_start_writes(hm->hmap_td, hm->hmap_horizon,
                                           hard_sync, block);
  if (err) return err;

  return 0;
}

int addb_hmap_checkpoint_finish_writes(addb_hmap* hm, bool hard_sync,
                                       bool block) {
  int err;

  err = addb_gmap_checkpoint_finish_writes(hm->hmap_gm, hard_sync, block);
  if (err) return err;

  err = addb_tiled_checkpoint_finish_writes(hm->hmap_td, hm->hmap_horizon,
                                            hard_sync, block);
  if (err) return err;

  return 0;
}

int addb_hmap_checkpoint_remove_backup(addb_hmap* hm, bool hard_sync,
                                       bool block) {
  int err;

  err = addb_gmap_checkpoint_remove_backup(hm->hmap_gm, hard_sync, block);
  if (err) return err;

  err = addb_tiled_checkpoint_remove_backup(hm->hmap_td, hm->hmap_horizon,
                                            hard_sync, block);
  if (err) return err;

  return 0;
}
