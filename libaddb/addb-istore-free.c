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
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/**
 * @brief Free a chunk of data.
 *
 *  The data was mapped in from the file underlying the istore database.
 *
 * @param is 	opaque istore handle, obtained with addb_istore_open()
 * @param data 	data returned by addb_istore_alloc() or addb_istore_read().
 * @param file	__FILE__ of the calling code, set by addb_istore_free macro.
 * @param line	__LINE__ of the calling code, set by addb_istore_free macro.
 */
void addb_istore_free_loc(addb_istore* is, addb_data* data, char const* file,
                          int line) {
  if (data != NULL) {
    if (data->data_type == ADDB_DATA_ISTORE) {
      cl_cover(is->is_addb->addb_cl);
      addb_istore_reference_free_loc(&data->data_iref, file, line);
    } else if (data->data_type == ADDB_DATA_CM) {
      cl_cover(is->is_addb->addb_cl);
      cm_free(data->data_ref.ref_cm, data->data_memory);
    }
    data->data_type = ADDB_DATA_NONE;
  }
}
