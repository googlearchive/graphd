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

#include "libcl/cl.h"
#include "libcm/cm.h"


/* How many seconds to wait between repeated warnings
 * about a full disk.
 */
#define PDB_DISK_SECONDS_BETWEEN_WARNINGS 60

/**
 * @brief Does the system currently expect writes to work?
 *
 *  If this function returns false, the system wants to see
 *  a pdb_checkpoint_optional run to completion before
 *  accepting any more write calls.
 *
 *  (I.e., a previous call failed because it lacked disk
 *  space; the database state is consistent, but falling
 *  behind; we need more space before we can accept new
 *  writes.)
 *
 * @param pdb	opaque module handle
 *
 * @return true if the system currently expects writes to work
 * @return false if the system is in an emergency state
 */
bool pdb_disk_is_available(pdb_handle const* pdb) {
  return pdb->pdb_disk_available;
}

/**
 * @brief Set the disk-available status
 *
 *  If this is news, a warning is logged.
 *
 * @param pdb	opaque module handle
 * @param avail	set the status to this.
 */
void pdb_disk_set_available(pdb_handle* pdb, bool avail) {
  if (!avail && pdb->pdb_disk_available) {
    time_t now;
    time(&now);

    if (pdb->pdb_disk_warning == 0 ||
        difftime(now, pdb->pdb_disk_warning) >=
            PDB_DISK_SECONDS_BETWEEN_WARNINGS) {
      cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
             "\"%s\": failed to flush written "
             "data to disk",
             pdb->pdb_path);
      pdb->pdb_disk_warning = now;
    }
  }
  pdb->pdb_disk_available = avail;
}
