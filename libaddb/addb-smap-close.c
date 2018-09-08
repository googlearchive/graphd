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

#include <stdio.h>

#include "libcm/cm.h"

/**
 * @brief Free resources associated with a smap.
 *
 *  It is up to the caller to ensure that the SMAP has been
 *  flushed to disk prior to closing it.  (An unflushed SMAP
 *  will likely roll back to a previously consistent horizon,
 *  taking the rest of the database with it.)
 *
 * @param sm NULL (in which case the call does nothing) or an
 *	addb_smap object created with addb_smap_open().
 * @return 0 on success, a nonzero ersmror code on error.
 */
int addb_smap_close(addb_smap* sm) {
  int e, err = 0;

  if (sm != NULL) {
    addb_handle* addb = sm->sm_addb;
    addb_smap_partition *part, *part_end;

    cl_log(addb->addb_cl, CL_LEVEL_SPEW, "addb_smap_close(%s)", sm->sm_path);

    part = sm->sm_partition;
    part_end = sm->sm_partition + sm->sm_partition_n;

    for (; part < part_end; part++) {
      e = addb_smap_partition_finish(part);
      if (e != 0 && err == 0) err = e;
    }
    sm->sm_partition_n = 0;

    sm->sm_tiled_pool = NULL;

    cm_free(addb->addb_cm, sm);
  }

  return err;
}
