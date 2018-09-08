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
#include "libaddb/addb-gmap.h"
#include "libaddb/addbp.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/*  Get an array with 2^ex slots from the free list.
 *
 *  If one is available, the call unchains it and returns
 *  its base with a reference in tref.
 *
 *  Otherwise the call returns NULL, and no reference is
 *  established.
 */

int addb_gmap_freelist_alloc(addb_gmap_partition* part, size_t ex,
                             unsigned long long* offset_out) {
  cl_handle* cl = part->part_gm->gm_addb->addb_cl;
  unsigned long long slot_offset;
  unsigned long long slot_val, off, pointer;
  int err;

  cl_assert(cl, ex > 0 && ex <= ADDB_GMAP_FREE_ENTRY_N);

  slot_offset = ADDB_GMAP_FREE_OFFSET(ex);

  err = addb_gmap_partition_get(part, slot_offset, &slot_val);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "addb_gmap_partition_get", err,
                 "slot_offset=%llu", slot_offset);
    return err;
  }

  if (ADDB_GMAP_IVAL_IS_EMPTY(slot_val)) {
    cl_cover(cl);
    return ADDB_ERR_NO;
  }

  off = ADDB_GMAP_MULTI_ENTRY_OFFSET(slot_val);
  *offset_out = off;

  /* Advance the freelist head to point just after the block we
   * allocated.
   *
   *  pointer := slot->next;
   *  slot := pointer;
   */
  err = addb_gmap_partition_get(part, off, &pointer);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "addb_gmap_partition_get", err, "off=%llu",
                 off);
    return err;
  }

  err = addb_gmap_partition_put(part, slot_offset, pointer);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "addb_gmap_partition_put", err,
                 "slot_offset=%llu", slot_offset);
    return err;
  }

  return 0;
}

/**
 * @brief Chain an array of 2^ex elements into the freelist.
 *
 * @param part 	partition this is happening in
 * @param off	base of the array as an offset from start of the file
 * @param ex	exponent (base 2) of the maximum array size
 */
int addb_gmap_freelist_free(addb_gmap_partition* part, unsigned long long off,
                            size_t ex) {
  int err;
  cl_handle* cl = part->part_gm->gm_addb->addb_cl;
  unsigned long long head_offset, head_val;
  unsigned long long ull;

  /*
  cl_log(cl, CL_LEVEL_SPEW, "gmap part %s: freelist(%d) += %llu[%llu] "
          "(at %p)",
          part->part_path, (int)ex, off,
          (1ull << ex) * ADDB_GMAP_FREE_ENTRY_N, (void *)base);
  */

  cl_assert(cl, ex >= 1 && ex <= ADDB_GMAP_FREE_ENTRY_N);
  cl_assert(cl, off >= ADDB_GMAP_MULTI_OFFSET);
  cl_assert(cl, (off - ADDB_GMAP_MULTI_OFFSET) % ADDB_GMAP_MULTI_FACTOR == 0);

  head_offset = ADDB_GMAP_FREE_OFFSET(ex);

  /* Save the old head pointer in the first bytes of
   * the array that will be inserted.
   *
   *  obj->next = head;
   */
  err = addb_gmap_partition_get(part, head_offset, &head_val);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "addb_gmap_partition_get", err,
                 "head_offset=%llu", head_offset);
    return err;
  }

  err = addb_gmap_partition_put(part, off, head_val);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "addb_gmap_partition_put", err, "off=%llu",
                 off);
    return err;
  }

  /* Set the head index to point to this entry.
   *
   *  head = obj;
   */
  ull = ADDB_GMAP_IVAL_MAKE_MULTI_OFFSET_EXP(off, ex);
  cl_assert(part->part_gm->gm_addb->addb_cl, ull != 0);

  err = addb_gmap_partition_put(part, head_offset, ull);
  if (err != 0) return err;

  cl_cover(cl);
  return 0;
}
