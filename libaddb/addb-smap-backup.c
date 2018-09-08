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

/**
 * @brief Turn on production of a "rollback" file in a database.
 *
 * @param sm		database handle
 * @param horizon	reset to this point.
 *
 * @return 0 on success, a nonzero error number on error.
 */

int addb_smap_backup(addb_smap* sm, unsigned long long horizon) {
  addb_handle* addb = sm->sm_addb;
  addb_smap_partition *part, *part_end;
  int err;

  part_end = sm->sm_partition +
             (sizeof(sm->sm_partition) / sizeof(sm->sm_partition[0]));

  for (part = sm->sm_partition; part < part_end; part++)
    if (part->part_td) {
      /* TODO: only need this to set the horizon, backup already set in
       * addb_smap_open */
      err = addb_tiled_backup(part->part_td, true);
      if (err) return err;
    }

  sm->sm_horizon = horizon;
  sm->sm_backup = true;

  cl_log(addb->addb_cl, CL_LEVEL_SPEW, "%s: backup enabled.", sm->sm_path);
  return 0;
}
