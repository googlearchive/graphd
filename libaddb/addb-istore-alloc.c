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
 * @brief Allocate a new variable-sized chunk of data.
 *
 *  This returns (in @em data) a pointer to the fresh, uninitialized
 *  storage, ready for writing (via an immediately following memcpy).
 *
 *  Once the caller is done with the data, it must be released with
 *  a call to addb_istore_free() or addb_istore_reference_free().
 *
 * @param is 	istore database pointer, allocated with addb_istore_open()
 * @param size		number of bytes to allocate
 * @param data_out 	assign the memory location and tile reference to this.
 * @param id_out	assign the id of the new fragment to this.
 *
 * @return 0 on success, otherwise a nonzero error number.
 * @return ERANGE if this database is full.
 */
int addb_istore_alloc(addb_istore* is, size_t size, addb_data* data_out,
                      addb_istore_id* id_out) {
  addb_handle* addb;
  addb_istore_partition* part;
  addb_istore_id id, part_local_id;
  off_t b_start, b_end, /* bytes */
      b_start_original;
  int err = 0;

  if (is == NULL) return EINVAL;

  addb = is->is_addb;
  cl_enter(addb->addb_cl, CL_LEVEL_SPEW, "(%s, %llu)", is->is_path,
           (unsigned long long)size);

  cl_assert(addb->addb_cl, data_out != NULL);
  cl_assert(addb->addb_cl, id_out != NULL);

  data_out->data_type = ADDB_DATA_NONE;

  size = addb_round_up(size, 8);
  if (size > ADDB_TILE_SIZE) {
    cl_log(addb->addb_cl, CL_LEVEL_FAIL,
           "addb: rounded size %zu exceeds tile size %zu. [%s:%d]", size,
           (size_t)ADDB_TILE_SIZE, __FILE__, __LINE__);
    cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "too large");
    return ADDB_ERR_PRIMITIVE_TOO_LARGE;
  }

  id = is->is_next.ism_memory_value;

  if (id > ADDB_ISTORE_INDEX_MAX) {
    cl_cover(addb->addb_cl);
    cl_log(addb->addb_cl, CL_LEVEL_FAIL, "addb: istore \"%s\" is full. [%s:%d]",
           is->is_path, __FILE__, __LINE__);
    cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "full");
    return ERANGE;
  }

  part_local_id = id % ADDB_ISTORE_INDEX_N;

  /* Which partition would it be on?
   */
  part = is->is_partition + id / ADDB_ISTORE_INDEX_N;
  if (!part->ipart_td) {
    /* Create the new partition. */
    err = addb_istore_partition_name(is, part, id / ADDB_ISTORE_INDEX_N);
    if (err != 0) {
      cl_leave(addb->addb_cl, CL_LEVEL_SPEW,
               "addb_istore_partition_name fails: %s", addb_xstrerror(err));
      return err;
    }

    err = addb_istore_partition_open(is, part, ADDB_MODE_READ_WRITE);
    if (err != 0) {
      cl_leave(addb->addb_cl, CL_LEVEL_SPEW,
               "addb_istore_partition_open fails: %s", addb_xstrerror(err));
      return err;
    }
    cl_cover(addb->addb_cl);
  }

  b_start_original = b_start = part->ipart_size;
  b_end = b_start + size;

  (void)addb_tiled_align(part->ipart_td, &b_start, &b_end);

  data_out->data_memory =
      addb_tiled_alloc(data_out->data_iref.iref_td = part->ipart_td, b_start,
                       b_end, &data_out->data_iref.iref_tref);
  if (data_out->data_memory == NULL) {
    cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "addb_tiled_alloc fails: %s",
             addb_xstrerror(err));
    return errno ? errno : ENOMEM;
  }

  data_out->data_type = ADDB_DATA_ISTORE;
  data_out->data_size = b_end - b_start;

  /*  From this point on, <data_out> *must* be free'ed in case
   *  of error -- otherwise, we'd lock the pointed-to primitive
   *  in the tile cache memory.
   *
   *  Hence "goto err" instead of "return".
   */

  /*  If this is the first index entry, or the previous index
   *  doesn't match the actual start of this record, update its
   *  end to be the beginning of this one.
   */
  if (b_start_original != b_start) {
    /*  If this were the very first entry in its partition,
     *  it would have started on a tile boundary,
     *  and b_start_original and b_start would be at the same
     *  offset.
     */
    cl_assert(addb->addb_cl, part_local_id > 0);

    err = addb_istore_index_set(is, part, part_local_id - 1, b_start);
    if (err) goto err;
  }

  /*  Add an index entry for the end of this record.
   */
  err = addb_istore_index_set(is, part, part_local_id, b_end);
  if (err) goto err;

  /*  Update the virtual file size.
   */
  part->ipart_size = b_end;

  /*  Update the high ID in the partition.
   */
  err = addb_istore_partition_next_id_set(is, part, part_local_id + 1);
  if (err) goto err;

  is->is_next.ism_memory_value = id + 1;
  *id_out = id;

  cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "got id %llu", (unsigned long long)id);
  return 0;

err:
  cl_assert(is->is_addb->addb_cl, err != 0);
  cl_cover(addb->addb_cl);

  addb_istore_free(is, data_out);
  cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "%s", addb_xstrerror(err));
  return err;
}
