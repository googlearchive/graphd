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

/**
 * @brief Turn on production of a "rollback" file in a database.
 *
 * @param gm		database handle
 * @param horizon	reset to this point.
 *
 * @return 0 on success, a nonzero error number on error.
 */

int addb_gmap_backup(addb_gmap* gm, unsigned long long horizon) {
  addb_handle* addb = gm->gm_addb;
  addb_gmap_partition *part, *part_end;
  int err;

  part_end = gm->gm_partition +
             (sizeof(gm->gm_partition) / sizeof(gm->gm_partition[0]));

  for (part = gm->gm_partition; part < part_end; part++)
    if (part->part_td) {
      /* TODO: only need this to set the horizon, backup already set in
       * addb_gmap_open */
      err = addb_tiled_backup(part->part_td, true);
      if (err) return err;
    }

  gm->gm_horizon = horizon;
  gm->gm_backup = true;

  cl_log(addb->addb_cl, CL_LEVEL_VERBOSE, "%s: backup enabled.", gm->gm_path);
  return 0;
}
