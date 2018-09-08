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

#include <string.h>
#include <errno.h>

#include "libcl/cl.h"
#include "libcm/cm.h"

const cl_facility addb_facilities[] = {
    {"tile", ADDB_FACILITY_TILE}, {"recovery", ADDB_FACILITY_RECOVERY}, {0}};

/* Initialize libaddb, create tiled pool, etc.
 */

addb_handle* addb_create(cm_handle* cm, cl_handle* cl,
                         unsigned long long total_memory, bool transactional) {
  int err;
  addb_handle* addb = cm_talloc(cm, addb_handle, 1);

  if (!addb) return NULL;

  memset(addb, 0, sizeof *addb);
  addb->addb_cm = cm;
  addb->addb_cl = cl;
  addb->addb_opcount = 1;
  addb->addb_bytes_locked = -1; /* no memory locking */
  addb->addb_mlock_max = 0;
  addb->addb_transactional = transactional;

  addb->addb_master_tiled_pool = addb_tiled_pool_create(addb);
  if (addb->addb_master_tiled_pool == NULL) {
    err = errno;
    cm_free(cm, addb);
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_tiled_pool_create", err,
                 "Unable to create a tiled pool");
    return NULL;
  }

  addb_tiled_pool_set_max(addb->addb_master_tiled_pool, total_memory);

  cl_log(cl, CL_LEVEL_DEBUG, "Addb database with %llu bytes of mmap buffers.",
         total_memory);

  return addb;
}
