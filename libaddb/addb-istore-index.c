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
 * @brief Get the first and just-after-last byte offset, given an id
 *
 *  The offsets are in bytes, counting from the beginning
 *  of the partition file.
 *
 * @param is opaque istore handle, obtained with addb_istore_open()
 * @param part partition the id is relative to.
 * @param id partition-local ID, in [0...ADDB_ISTORE_INDEX_N).
 * @param start_out assign the start offset to this.
 * @param end_out assign the end offset to this.
 * @return 0 on success.
 * @return ADDB_ERR_NO if the entry doesn't exist.
 */
int addb_istore_index_boundaries_get(addb_istore* is,
                                     addb_istore_partition* part,
                                     addb_istore_id id, off_t* start_out,
                                     off_t* end_out) {
  addb_handle* const addb = is->is_addb;
  cl_handle* const cl = addb->addb_cl;
  int err;

  cl_assert(cl, part != NULL);
  cl_assert(cl, id < ADDB_ISTORE_INDEX_N);

  /* the start offset */
  if (id == 0)
    *start_out = ADDB_ISTORE_DATA_OFFSET_0;
  else if ((err = addb_istore_index_get(is, part, id - 1, start_out)))
    return err;

  /* the end offset */
  if ((err = addb_istore_index_get(is, part, id, end_out))) return err;

  cl_assert(cl, *start_out <= *end_out);

  return 0;
}

/**
 * @brief get the end-offset for the object with a given index
 *
 *  The offset is in bytes, counting from the beginning
 *  of the partition file.
 *
 * @param is an opaque istore handle opened with addb_istore_open()
 * @param part the partition to look in
 * @param id the partition-local id.
 *
 * @return 0 on success
 * @return ADDB_ERR_NO if the entry doesn't exist
 * @return ENOMEM on allocation error
 */
int addb_istore_index_get(addb_istore* is, addb_istore_partition* part,
                          addb_istore_id id, off_t* out) {
  addb_handle* addb = is->is_addb;
  unsigned char const* ptr;
  unsigned long ul;
  addb_tiled* td;
  unsigned long long offset;

  cl_assert(addb->addb_cl, part != NULL);

  /* Why <= ? */
  cl_assert(addb->addb_cl, id <= ADDB_ISTORE_INDEX_N);

  td = part->ipart_td;
  offset = ADDB_ISTORE_INDEX_OFFSET(id);

  /* try a fast peek first, in case the table is mapped to memory */
  ptr = addb_tiled_peek(td, offset, ADDB_ISTORE_INDEX_SIZE);
  if (ptr != NULL) {
    /* good, we can just use the initial mmap
     */

    ul = ADDB_GET_U4(ptr);
  } else {
    /* no luck.. perform a tile lookup (slow!)
     */

    const unsigned long long s = offset;
    const unsigned long long e = s + ADDB_ISTORE_INDEX_SIZE;
    addb_tiled_reference tref;

    /*  This access can't cross tile boundaries, because its size
     *  (ADDB_ISTORE_INDEX_SIZE) divides both its offset and the
     *  tile size.
     */
    ptr = addb_tiled_get(td, s, e, ADDB_MODE_READ, &tref);
    if (ptr == NULL) return ENOMEM;
    ul = ADDB_GET_U4(ptr);
    addb_tiled_free(td, &tref);
  }

  if (0 == ul) {
    cl_log(addb->addb_cl, CL_LEVEL_FAIL,
           "addb_istore_index_get: zero index -> "
           "id=%llu not found.",
           (unsigned long long)id);
    return ADDB_ERR_NO;
  }

  /* convert offset relative to the start of the data segment to an
   * absolute byte offset */
  *out = ADDB_ISTORE_IXOFFSET_TO_BYTES(ul);

  return 0;
}

/**
 * @brief Assign an offset to an index slot (private utility).
 *
 *  The offset is in bytes, counting from the beginning
 *  of the partition file.  (We convert it into an IXOFFSET.)
 *
 *  This happens as part of an addb_istore_alloc().
 *
 * @param is an opaque istore handle opened with addb_istore_open()
 * @param part the partition to look in
 * @param id the partition-local id.
 * @param val the byte count to assign to it.
 *
 * @return 0 on success
 * @return ADDB_ERR_NO if the entry doesn't exist
 * @return ENOMEM on allocation error
 */
int addb_istore_index_set(addb_istore* is, addb_istore_partition* part,
                          addb_istore_id id, off_t val) {
  addb_handle* addb = is->is_addb;
  unsigned char* ptr;
  addb_tiled_reference tref;
  unsigned long long s, e;
  unsigned long ul;

  cl_assert(addb->addb_cl, part != NULL);
  cl_assert(addb->addb_cl, id <= ADDB_ISTORE_INDEX_N);

  /*  This access can't cross tile boundaries, because its size
   *  - ADDB_ISTORE_INDEX_SIZE - divides both its offset and the
   *  tile size.
   */
  cl_assert(addb->addb_cl,
            ADDB_ISTORE_INDEX_OFFSET_BASE % ADDB_ISTORE_INDEX_SIZE == 0);
  cl_assert(addb->addb_cl, ADDB_ISTORE_TILE_SIZE % ADDB_ISTORE_INDEX_SIZE == 0);

  s = ADDB_ISTORE_INDEX_OFFSET(id);
  e = s + ADDB_ISTORE_INDEX_SIZE;

  ptr = addb_tiled_get(part->ipart_td, s, e, ADDB_MODE_WRITE, &tref);
  if (ptr == NULL) return ENOMEM;

  ul = ADDB_ISTORE_IXOFFSET_FROM_BYTES(val);
  cl_assert(addb->addb_cl, ul > 0);
  ADDB_PUT_U4(ptr, ul);

  cl_cover(addb->addb_cl);
  addb_tiled_free(part->ipart_td, &tref);

  return 0;
}
