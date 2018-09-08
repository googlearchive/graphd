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

/**
 * @brief write a new record
 *
 *  The record is written to the mapped database.
 *
 *  If successful, a later call to addb_istore_read on the same
 *  database will return data whose first size bytes will match those
 *  written here.  (In other words, a returned chunk can be larger
 *  than what was written - the data must be self-delimiting.)
 *
 * @param is 	the database to write to
 * @param data 	bytes to write
 * @param size 	number of bytes to write
 * @param id_out on success, the index for the new record.
 *
 * @result 0 on success, otherwise a nonzero error number.
 */
int addb_istore_write(addb_istore* is, char const* data, size_t size,
                      addb_istore_id* id_out) {
  addb_data d;
  int err;

  if (is == NULL) return EINVAL;

  cl_assert(is->is_addb->addb_cl, data != NULL);
  cl_assert(is->is_addb->addb_cl, id_out != NULL);

  err = addb_istore_alloc(is, size, &d, id_out);
  if (err != 0) return err;

  cl_assert(is->is_addb->addb_cl, size <= d.data_size);
  memcpy(d.data_memory, data, size);
  if (size < d.data_size) {
    cl_cover(is->is_addb->addb_cl);
    memset(d.data_memory + size, 0, d.data_size - size);
  }

  /*  Done.  Unlock the tile we wrote.
   */
  cl_cover(is->is_addb->addb_cl);
  addb_istore_free(is, &d);

  return 0;
}
