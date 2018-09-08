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
#include <unistd.h>

/*
 * Update internal data structures for a gmap that may have been
 * modified on disk. 'destructive' modifications are not allowed, nor are
 * asynchronous modifications that may occur while we read data.
 *
 * Last id is the highest ID that might be in this gmap now.
 *
 */
int addb_gmap_refresh(addb_gmap* gm, unsigned long long last_id) {
  int last_part = last_id / ADDB_GMAP_SINGLE_ENTRY_N;
  cl_handle* cl = gm->gm_addb->addb_cl;
  int i;
  int err;
  addb_gmap_partition* gp;

  cl_log(cl, CL_LEVEL_DEBUG, "addb_gmap_refresh: refreshing gmap %s",
         gm->gm_base);

  for (i = 0; i <= last_part; i++) {
    gp = gm->gm_partition + i;

    /*
     * If the partition exists, increase its file if needbe.
     * If it doesn't exist, check to see if it exists now.
     */
    if (gp->part_td) {
      cl_log(cl, CL_LEVEL_VERBOSE, "addb_gmap_refresh: stretching partition %i",
             i);
      err = addb_tiled_stretch(gp->part_td);

      if (err) {
        cl_log_errno(cl, CL_LEVEL_ERROR, "addb_tiled_stretch", err,
                     "Can not stretch tile for gmap %s", gp->part_path);
        return err;
      }
    } else {
      int rv;
      addb_gmap_partition_name(gp, i);
      rv = access(gp->part_path, F_OK);
      err = errno;
      if (rv == 0) {
        cl_log(cl, CL_LEVEL_VERBOSE, "addb_gmap_refresh: trying to open %s",
               gp->part_path);
        err = addb_gmap_partition_open(gp, ADDB_MODE_READ_ONLY);

        if (err) {
          /* LOG */
          return err;
        }

        if (last_part > gm->gm_partition_n) gm->gm_partition_n = last_part;
      } else if (err == ENOENT) {
        /* This is okay. There may be no links
         * to anything that would be in this
         * partition yet
         */
        cl_log(cl, CL_LEVEL_VERBOSE,
               "addb_gmap_refresh: %s does not exist"
               "(yet)",
               gp->part_path);
      } else {
        cl_notreached(cl,
                      "Wierd error %s while trying"
                      " to access(2) %s",
                      strerror(err), gp->part_path);
      }
    }
  }

  err = addb_largefile_refresh(gm->gm_lfhandle);
  if (err) return err;

  err = addb_bgmap_refresh(gm, last_id);
  if (err) return err;

  return 0;
}
