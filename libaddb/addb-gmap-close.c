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
#include "libaddb/addbp.h"

#include <stdio.h>

#include "libcm/cm.h"

/**
 * @brief Free resources associated with a gmap.
 *
 *  It is up to the caller to ensure that the GMAP has been
 *  flushed to disk prior to closing it.  (An unflushed GMAP
 *  will likely roll back to a previously consistent horizon,
 *  taking the rest of the database with it.)
 *
 * @param gm NULL (in which case the call does nothing) or an
 *	addb_gmap object created with addb_gmap_open().
 * @return 0 on success, a nonzero error code on error.
 */
int addb_gmap_close(addb_gmap* gm) {
  int e, err = 0;

  if (gm != NULL) {
    addb_handle* addb = gm->gm_addb;
    addb_gmap_partition *part, *part_end;

    cl_log(addb->addb_cl, CL_LEVEL_SPEW, "addb_gmap_close(%s)", gm->gm_path);

    part = gm->gm_partition;
    part_end = gm->gm_partition + gm->gm_partition_n;

    for (; part < part_end; part++) {
      e = addb_gmap_partition_finish(part);
      if (e != 0 && err == 0) err = e;
    }
    gm->gm_partition_n = 0;

    addb_largefile_close(gm->gm_lfhandle);
    addb_bgmap_handle_destroy(gm->gm_bgmap_handle);
    gm->gm_bgmap_handle = NULL;
    gm->gm_lfhandle = NULL;
    gm->gm_tiled_pool = NULL;
    close(gm->gm_dir_fd);

    cm_free(addb->addb_cm, gm);
  }

  return err;
}
