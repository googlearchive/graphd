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
#include "libaddb/addbp.h"
#include "libaddb/addb-smap.h"

#include <errno.h>
#include <time.h>

/* Iterate over partitions applying a checkpoint function
 */
static int addb_smap_checkpoint_partitions(addb_smap* sm,
                                           unsigned long long horizon,
                                           bool hard_sync, bool block,
                                           addb_tiled_checkpoint_fn* cpfn) {
  addb_smap_partition* part;
  addb_smap_partition* part_end;
  bool wouldblock = false;
  int err = 0;

  if (!sm) return 0;

  part_end = sm->sm_partition +
             (sizeof(sm->sm_partition) / sizeof(sm->sm_partition[0]));

  for (part = sm->sm_partition; part < part_end; part++) {
    if (!part->part_td) continue;

    err = (*cpfn)(part->part_td, horizon, hard_sync, block);
    if (err) {
      if (EWOULDBLOCK == err) {
        wouldblock = true;
        err = 0;
      } else if (EALREADY != err)
        return err;
    }
  }

  if (!err && wouldblock) err = EWOULDBLOCK;

  return err;
}

/* Apply the tiled checkpoint function to the partitions
 */
static int addb_smap_checkpoint_stage(addb_smap* sm, bool hard_sync, bool block,
                                      addb_tiled_checkpoint_fn* cpfn) {
  bool wouldblock = false;
  int err;

  err = addb_smap_checkpoint_partitions(sm, sm->sm_horizon, hard_sync, block,
                                        cpfn);
  if (err) {
    if (EWOULDBLOCK == err) {
      wouldblock = true;
      err = 0;
    } else if (EALREADY != err)
      return err;
  }

  if (wouldblock) return EWOULDBLOCK;

  return 0;
}

/* GMAP checkpoint stages...
 */

int addb_smap_checkpoint_finish_backup(addb_smap* sm, bool hard_sync,
                                       bool block) {
  return addb_smap_checkpoint_stage(sm, hard_sync, block,
                                    addb_tiled_checkpoint_finish_backup);
}

int addb_smap_checkpoint_sync_backup(addb_smap* sm, bool hard_sync,
                                     bool block) {
  return addb_smap_checkpoint_stage(sm, hard_sync, block,
                                    addb_tiled_checkpoint_sync_backup);
}

int addb_smap_checkpoint_start_writes(addb_smap* sm, bool hard_sync,
                                      bool block) {
  return addb_smap_checkpoint_stage(sm, hard_sync, block,
                                    addb_tiled_checkpoint_start_writes);
}

int addb_smap_checkpoint_finish_writes(addb_smap* sm, bool hard_sync,
                                       bool block) {
  return addb_smap_checkpoint_stage(sm, hard_sync, block,
                                    addb_tiled_checkpoint_finish_writes);
}

int addb_smap_checkpoint_remove_backup(addb_smap* sm, bool hard_sync,
                                       bool block) {
  return addb_smap_checkpoint_stage(sm, hard_sync, block,
                                    addb_tiled_checkpoint_remove_backup);
}

/**
 * @brief Roll back to a well-defined previous state.
 *
 * @return 0 on completion
 * @return EWOULDBLOCK if work didn't complete because it ran out of time
 * @return other nonzero errors in other error cases.
 */
int addb_smap_checkpoint_rollback(addb_smap* sm) {
  addb_handle* addb = sm->sm_addb;
  addb_smap_partition* part;
  addb_smap_partition* part_end;
  size_t i;
  int any = 0;
  int err = 0;
  int e;

  cl_assert(addb->addb_cl, sm->sm_backup);
  cl_enter(addb->addb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_RECOVERY,
           "horizon: %llu", sm->sm_horizon);

  part_end =
      sm->sm_partition + (sizeof sm->sm_partition / sizeof sm->sm_partition[0]);

  for (part = sm->sm_partition, i = 0; part < part_end; i++, part++)
    if (part->part_td) {
      e = addb_tiled_read_backup(part->part_td, sm->sm_horizon);
      if (e && e != EALREADY && e != ENOENT) {
        if (!err) err = e;
        cl_log_errno(addb->addb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_RECOVERY,
                     "addb_tiled_read_backup", e, "Unable to rollback %s",
                     part->part_path);
      }
      if (!e) any++;
    }

  if (any)
    cl_log(addb->addb_cl, CL_LEVEL_DEBUG, "%s: rolled back to %llu.",
           sm->sm_path, sm->sm_horizon);

  cl_leave(addb->addb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_RECOVERY, "");

  return err;
}
