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
#include "libaddb/addb-flat.h"

#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/**
 * @brief Read the data in a flat database.
 *
 * @param fl	opaque database handle
 * @param data	uninitialized data pointer; set by the call.
 *
 * @return 0 on success
 * @return ENOMEM if the file contents couldn't be mapped into memory.
 * @return other nonzero error codes on system errors.
 */
int addb_flat_read(addb_flat* fl, addb_data* data) {
  if (fl == NULL || data == NULL) return EINVAL;
  if (!fl->fl_memory) return ENOMEM;

  data->data_memory = fl->fl_memory + ADDB_FLAT_HEADER_SIZE;
  data->data_size = fl->fl_memory_size - ADDB_FLAT_HEADER_SIZE;

  data->data_type = ADDB_DATA_FLAT;
  data->data_ref.ref_flat = fl;

  cl_cover(fl->fl_addb->addb_cl);

  return 0;
}
