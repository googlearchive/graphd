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
#include "libaddb/addb-istore.h"
#include "libaddb/addbp.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/**
 * @brief read-only access to a primitive
 *
 *  After a successful call, the primitive resources must be
 *  released with a call to addb_istore_free() or
 *  addb_istore_reference_free().
 *
 * @param is	istore object, returned by addb_istore_open()
 * @param id 	istore-relative 34-bit identifier of the data,
 * 		returned by addb_istore_alloc().
 * @param data_out data to be filled in by the call.
 * @param file	calling source code file, filled in by
 *		addb_istore_read() macro
 * @param line	calling source code line, filled in by
 *		addb_istore_read() macro
 *
 * @returns 0 on success, a nonzero error code on error.
 */
int addb_istore_read_loc(addb_istore* is, addb_istore_id id,
                         addb_data* data_out, char const* file, int line) {
  addb_handle* addb = is->is_addb;
  addb_istore_partition* part;

  data_out->data_type = ADDB_DATA_NONE;
  if (id >= 1ull << 34) return EINVAL;

  cl_assert(addb->addb_cl, id < (1ull << 34));

  /* Which partition is it on?
   */
  part = is->is_partition + (id >> 24);
  if (!part->ipart_td) {
    cl_cover(addb->addb_cl);
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: istore read: id %llu would be in "
           "partition %llu, which doesn't exist [%s:%d]",
           (unsigned long long)id, (unsigned long long)(id >> 24), __FILE__,
           __LINE__);

    return ADDB_ERR_NO;
  }

  cl_cover(addb->addb_cl);

  return addb_istore_partition_data_loc(is, part, id % ADDB_ISTORE_INDEX_N,
                                        data_out, file, line);
}
