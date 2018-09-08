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

#include "libcm/cm.h"

/**
 * @brief Free an addb_istore object created with addb_istore_open().
 * @param is opaque istore database handle, created with addb_istore_open().
 * @result 0 on success, a nonzero error code on unexpected error.
 */
int addb_istore_close(addb_istore* is) {
  int err, result = 0;

  if (is != NULL) {
    addb_handle* addb = is->is_addb;
    addb_istore_partition *part, *part_end;

    cl_enter(addb->addb_cl, CL_LEVEL_SPEW, "(%d partitions)",
             (int)is->is_partition_n);

    part = is->is_partition;
    part_end = is->is_partition + is->is_partition_n;

    for (; part < part_end; part++) {
      err = addb_istore_partition_finish(is, part);
      if (err != 0 && result == 0) result = err;

      cl_cover(addb->addb_cl);
    }

    is->is_partition_n = 0;
    cm_free(addb->addb_cm, is);
    cl_cover(addb->addb_cl);

    cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "leave");
  }

  return result;
}
