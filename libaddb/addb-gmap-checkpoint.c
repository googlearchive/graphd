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
#include "libaddb/addb-bgmap.h"
#include "libaddb/addb-gmap.h"
#include "libaddb/addbp.h"

#include <errno.h>
#include <time.h>

/* Iterate over partitions applying a checkpoint function
 */
static int addb_gmap_checkpoint_partitions(addb_gmap* gm,
                                           unsigned long long horizon,
                                           bool hard_sync, bool block,
                                           addb_tiled_checkpoint_fn* cpfn) {
  addb_gmap_partition* part;
  addb_gmap_partition* part_end;
  bool wouldblock = false;
  int err = 0;

  if (!gm) return 0;

  part_end = gm->gm_partition +
             (sizeof(gm->gm_partition) / sizeof(gm->gm_partition[0]));

  for (part = gm->gm_partition; part < part_end; part++) {
    if (!part->part_td) continue;

    err = (*cpfn)(part->part_td, horizon, hard_sync, block);
    if (err) {
      if (ADDB_ERR_MORE == err) {
        wouldblock = true;
        err = 0;
      } else if (ADDB_ERR_ALREADY != err)
        return err;
    }
  }

  if (!err && wouldblock) err = ADDB_ERR_MORE;

  return err;
}

/* Apply the same tiled checkpoint function to both the partitions
 * and the large files.
 */
static int addb_gmap_checkpoint_stage(addb_gmap* gm, bool hard_sync, bool block,
                                      addb_tiled_checkpoint_fn* cpfn) {
  bool wouldblock = false;
  int err;

  err = addb_gmap_checkpoint_partitions(gm, gm->gm_horizon, hard_sync, block,
                                        cpfn);
  if (err) {
    if (ADDB_ERR_MORE == err) {
      wouldblock = true;
      err = 0;
    } else if (ADDB_ERR_ALREADY != err)
      return err;
  }

  err = addb_bgmap_checkpoint(gm, gm->gm_horizon, hard_sync, block, cpfn);
  if (err) {
    if (ADDB_ERR_MORE == err) {
      wouldblock = true;
      err = 0;
    } else if (ADDB_ERR_ALREADY != err)
      return err;
  }

  err = addb_largefile_checkpoint(gm->gm_lfhandle, gm->gm_horizon, hard_sync,
                                  block, cpfn);
  if (err) {
    if (ADDB_ERR_MORE == err) {
      wouldblock = true;
      err = 0;
    } else if (ADDB_ERR_ALREADY != err)
      return err;
  }

  if (wouldblock) return ADDB_ERR_MORE;

  return 0;
}

/* GMAP checkpoint stages...
 */

int addb_gmap_checkpoint_finish_backup(addb_gmap* gm, bool hard_sync,
                                       bool block) {
  return addb_gmap_checkpoint_stage(gm, hard_sync, block,
                                    addb_tiled_checkpoint_finish_backup);
}

int addb_gmap_checkpoint_sync_backup(addb_gmap* gm, bool hard_sync,
                                     bool block) {
  int err;

  err = addb_gmap_checkpoint_stage(gm, hard_sync, block,
                                   addb_tiled_checkpoint_sync_backup);

  if (hard_sync && err == 0) {
    err = addb_file_sync_start(gm->gm_addb->addb_cl, gm->gm_dir_fd,
                               &gm->gm_dir_fsync_ctx, gm->gm_path, true);
    cl_assert(gm->gm_addb->addb_cl, err != ADDB_ERR_MORE);
  }
  return err;
}

int addb_gmap_checkpoint_sync_directory(addb_gmap* gm, bool hard_sync,
                                        bool block) {
#if ADDB_FSYNC_DIRECTORY
  int err;
  cl_handle* cl;

  cl = gm->gm_addb->addb_cl;

  err = addb_file_sync_finish(cl, &gm->gm_dir_fsync_ctx, block, gm->gm_path);
  return err;
#else
  return 0;
#endif
}

int addb_gmap_checkpoint_start_writes(addb_gmap* gm, bool hard_sync,
                                      bool block) {
  int err;
  err = addb_gmap_checkpoint_stage(gm, hard_sync, block,
                                   addb_tiled_checkpoint_start_writes);
  return err;
}

int addb_gmap_checkpoint_finish_writes(addb_gmap* gm, bool hard_sync,
                                       bool block) {
  return addb_gmap_checkpoint_stage(gm, hard_sync, block,
                                    addb_tiled_checkpoint_finish_writes);
}

int addb_gmap_checkpoint_remove_backup(addb_gmap* gm, bool hard_sync,
                                       bool block) {
  return addb_gmap_checkpoint_stage(gm, hard_sync, block,
                                    addb_tiled_checkpoint_remove_backup);
}

/**
 * @brief Roll back to a well-defined previous state.
 *
 * @return 0 on completion
 * @return ADDB_ERR_MORE if work didn't complete because it ran out of time
 * @return other nonzero errors in other error cases.
 */
int addb_gmap_checkpoint_rollback(addb_gmap* gm) {
  addb_handle* addb = gm->gm_addb;
  addb_gmap_partition* part;
  addb_gmap_partition* part_end;
  size_t i;
  int any = 0;
  int err = 0;
  int e;

  /* Ordering here it important. The GMAPS need to be rolled back
   * and then the largefiles need to be rolled back.  The largefiles
   * will update in-memory metadata based on the rolled-back
   * GMAP state.
   */
  cl_assert(addb->addb_cl, gm->gm_backup);
  cl_enter(addb->addb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_RECOVERY,
           "horizon: %llu", gm->gm_horizon);

  part_end =
      gm->gm_partition + (sizeof gm->gm_partition / sizeof gm->gm_partition[0]);

  for (part = gm->gm_partition, i = 0; part < part_end; i++, part++)
    if (part->part_td) {
      e = addb_tiled_read_backup(part->part_td, gm->gm_horizon);
      if (e && e != ADDB_ERR_ALREADY && e != ENOENT && e != ADDB_ERR_NO) {
        if (!err) err = e;
        cl_log_errno(addb->addb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_RECOVERY,
                     "addb_tiled_read_backup", e, "Unable to rollback %s",
                     part->part_path);
      }
      if (!e) any++;
    }

  e = addb_largefile_rollback(gm->gm_lfhandle, gm->gm_horizon);
  if (e && e != ADDB_ERR_ALREADY && e != ENOENT && e != ADDB_ERR_NO) {
    if (!err) err = e;
    cl_log_errno(addb->addb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_RECOVERY,
                 "addb_largefile_rollback", e,
                 "Unable to rollback %s largefiles", part->part_path);
  }

  if (any)
    cl_log(addb->addb_cl, CL_LEVEL_DEBUG, "%s: rolled back to %llu.",
           gm->gm_path, gm->gm_horizon);

  cl_leave(addb->addb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_RECOVERY, "leave");

  return err;
}
