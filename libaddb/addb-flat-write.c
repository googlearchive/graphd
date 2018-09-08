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
#include "libaddb/addb-flat.h"
#include "libaddb/addbp.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>

/**
 * @brief Update a flat database.
 *
 *  The data passed into this call is only the payload; the
 *  header  (with the magic number) is not changed by this call.
 *
 *  Once the call returns, the flat database is fully written
 *  and updated.
 *
 * @param fl	the database, created with addb_flat_open()
 * @param data	data to write.
 * @param size	number of bytes pointed to by #data.  Must be
 *  	less than or equal to the size of the existing database.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int addb_flat_write(addb_flat* fl, char const* data, size_t size) {
  if (fl == NULL || data == NULL) return EINVAL;
  if (!fl->fl_memory) return ENOMEM;
  if (fl->fl_memory_size < size + ADDB_FLAT_HEADER_SIZE) return EINVAL;

  memcpy(fl->fl_memory + ADDB_FLAT_HEADER_SIZE, data, size);

  cl_cover(fl->fl_addb->addb_cl);

  return msync(fl->fl_memory, fl->fl_memory_size, MS_SYNC);
}
