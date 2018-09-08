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
#include "libaddb/addb-smap.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/**
 * @brief Get the index pointer and partition for a source ID.
 *
 *   If the source ID doesn't exist yet, its index entry is
 *   allocated -- unless it's larger than the possible maximum
 *   for local database IDs ((2 << 34) - 1).
 *
 * @param sm 		database to index
 * @param id 		source ID we're looking for
 * @param part_out	assign the partition to this
 * @param offset_out	assign the offset within the partition to this
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int addb_smap_index_offset(addb_smap* sm, addb_smap_id id,
                                  addb_smap_partition** part_out,
                                  unsigned long long* offset_out) {
  cl_handle* cl = sm->sm_addb->addb_cl;
  addb_smap_partition* part;
  int err;
  size_t part_i;

  if (id > ADDB_GMAP_ID_MAX) {
    cl_cover(cl);
    cl_log(cl, CL_LEVEL_FAIL,
           "addb: cannot translate %llu "
           "into an index pointer (for %s) [%s:%d]",
           (unsigned long long)id, sm->sm_path, __FILE__, __LINE__);
    return ADDB_ERR_NO;
  }

  /*  Get id's partition; create a new one if needed.
   */
  part_i = id / ADDB_GMAP_SINGLE_ENTRY_N;
  part = sm->sm_partition + part_i;
  if (part->part_td == NULL) {
    /* Open the new partition. */
    if ((err = addb_smap_partition_name(part, part_i)) != 0) {
      cl_log(cl, CL_LEVEL_ERROR, "%s: can't set partition name? [%s:%d]",
             sm->sm_path, __FILE__, __LINE__);
      return err;
    }

    err = addb_smap_partition_open(part, ADDB_MODE_READ_WRITE);
    if (err != 0) {
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_ERROR, "%s: can't open partition: %s [%s:%d]",
             sm->sm_path, addb_xstrerror(err), __FILE__, __LINE__);
      return err;
    }
    if (part_i >= sm->sm_partition_n) {
      cl_cover(cl);
      sm->sm_partition_n = part_i + 1;
    }
  }

  id %= ADDB_GMAP_SINGLE_ENTRY_N;
  ;

  *offset_out = ADDB_GMAP_SINGLE_ENTRY_OFFSET(id);
  *part_out = part;

  return 0;
}

/**
 * @brief Allocate a tiled area of a given size.
 *
 *  The tiled area is always allocated at the end of the current file.
 *  It may map to multiple tiles.
 *
 * @param part	partition to allocate in
 * @param size	number of bytes in the area
 * @param offset_out assign the start offset to here
 *
 * @result 0 on success
 * @result ENOMEM if we couldn't allocate the specified slab
 */
static int addb_smap_partition_alloc(addb_smap_partition* part, size_t size,
                                     unsigned long long* offset_out) {
  unsigned long long s, e, s0;
  char* ptr;
  addb_tiled_reference tref;
  int err = 0;

  s = s0 = part->part_size;
  e = s + size;

  /*  If we'd always allocate the whole piece at once, and it crossed
   *  tile boundaries, we'd automatically create a double-sized tile.
   *
   *  We don't want that to happen, because we'll later access other
   *  parts of the same tile area without crossing boundaries;
   *  single tiles would be created that overlap with the area of
   *  the double tile, keeping versions of one piece of memory
   *  in two places.
   *
   *  So we make sure that we only ever allocate pieces that are in
   *  one tile only; if we cross tiles, we break the allocation into
   *  parts.
   *
   *  (Normally, one obviously would change the tile manager
   *  to always allocate single-tile fragments only; but smap
   *  and istore share the tile manager, and istore *does* want
   *  the multi-sized tiles.)
   */

  while (s / ADDB_TILE_SIZE < (e - 1) / ADDB_TILE_SIZE) {
    unsigned long long boundary;

    /* Allocate a partial tile.
     */
    boundary = ((s / ADDB_TILE_SIZE) + 1) * ADDB_TILE_SIZE;

    cl_assert(part->part_sm->sm_addb->addb_cl, boundary < e);

    ptr = addb_tiled_alloc(part->part_td, s, boundary, &tref);
    if (ptr == NULL) return errno ? errno : ENOMEM;

    addb_tiled_free(part->part_td, &tref);
    s = boundary;
  }

  /* Allocate the final fragment */

  ptr = addb_tiled_alloc(part->part_td, s, e, &tref);
  if (ptr == NULL) return errno ? errno : ENOMEM;

  addb_tiled_free(part->part_td, &tref);

  err = addb_smap_partition_grow(part, e);
  if (err != 0) return err;

  part->part_size = e;
  *offset_out = s0;

  return 0;
}

/**
 * @brief Add an id to the list of ids associated with a source.
 *
 *  It is up to the caller to make sure that the id isn't yet
 *  associated with the source.
 *
 * @param sm 	 opaque smap module handle
 * @param source the left-hand-side of the 1:N relationship
 * @param id 	 new element added to the right-hand-side of the relationship,
 *			0...2^34-1
 *
 * @return 0 on success, an error number on error.
 */

int addb_smap_add(addb_smap* sm, addb_smap_id source, addb_smap_id id,
                  bool duplicates_okay) {
  cl_handle* cl;
  addb_smap_partition* part;
  int err;

  unsigned long long i_val, i_offset;
  unsigned long long old_offset;
  unsigned long long new_size_exp, new_val_sentinel;
  unsigned long long new_offset, new_size;

  cl = sm->sm_addb->addb_cl;

  cl_log(cl, CL_LEVEL_SPEW, "%s: smap add %llu %llu", sm->sm_path,
         (unsigned long long)source, (unsigned long long)id);

  cl_assert(cl, id < (1ull << 34));

  err = addb_smap_index_offset(sm, source, &part, &i_offset);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "addb_smap_index_offset", err,
                 "%s: can't get index offset for %llu", sm->sm_path,
                 (unsigned long long)source);
    return err;
  }

  err = addb_smap_partition_get(part, i_offset, &i_val);
  if (err != 0) {
    if (err == ADDB_ERR_NO) {
      i_val = ADDB_GMAP_IVAL_MAKE_SINGLE(id);
      return addb_smap_partition_put(part, i_offset, i_val);
    }
    cl_log_errno(cl, CL_LEVEL_FAIL, "addb_smap_partition_get", err,
                 "%s: can't get partition for index offset %llu", sm->sm_path,
                 (unsigned long long)i_offset);
    return err;
  }

  if (ADDB_GMAP_IVAL_IS_EMPTY(i_val)) {
    /*  Case 1: There is no entry.  Create one in place
     *  	of the "empty" placeholder.
     */

    cl_cover(cl);

    i_val = ADDB_GMAP_IVAL_MAKE_SINGLE(id);
    return addb_smap_partition_put(part, i_offset, i_val);
  }

  /*  If execution reaches the end of the following block, new_size_exp
   *  is the desired exponent of the new array's size, base 2.
   */

  if (ADDB_GMAP_IVAL_IS_SINGLE(i_val)) {
    if (ADDB_GMAP_IVAL_SINGLE(i_val) >= id) {
      if (duplicates_okay)
        return ADDB_ERR_EXISTS;
      else
        cl_notreached(cl,
                      "Tried to add value %llu to smap %llu"
                      " twice",
                      (unsigned long long)source, (unsigned long long)id);
    }
    new_size_exp = 1;
    cl_cover(cl);
    old_offset = i_offset;
  } else {
    unsigned long long s_offset, s_val;

    old_offset = ADDB_GMAP_MULTI_ENTRY_OFFSET(i_val);
    s_offset = old_offset + ADDB_GMAP_IVAL_M_SIZE(i_val) - ADDB_GMAP_ENTRY_SIZE;

    err = addb_smap_partition_get(part, s_offset, &s_val);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "addb_smap_partition_get", err,
                   "addb_smap_add %s: can't "
                   "get partition value for %llu",
                   sm->sm_path, (unsigned long long)s_offset);
      return err;
    }

    if (!ADDB_GMAP_MVAL_S_IS_FULL(s_val)) {
      unsigned long long nel;
      unsigned long long last;

      /*  Case 2: An entry there points to an array that's
       *      not yet full.  Add to the array and update
       *  	its sentinel element.
       */
      nel = ADDB_GMAP_MVAL_S_NELEMS(s_val);
      cl_assert(part->part_sm->sm_addb->addb_cl, nel > 0);

      /*  Get the last element.  Check if it
       *  is >= what we're trying to append.
       */
      err = addb_smap_partition_get(
          part, old_offset + (nel - 1) * ADDB_GMAP_ENTRY_SIZE, &last);
      if (err) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "addb_smap_partition_get", err,
                     "addb_smap_add %s: can't "
                     "get partition value for "
                     "%llu: ",
                     sm->sm_path,
                     (unsigned long long)(old_offset +
                                          (nel - 1) * ADDB_GMAP_ENTRY_SIZE));
        return err;
      }
      if (ADDB_GMAP_MVAL_INDEX(last) >= id) {
        cl_log(cl, CL_LEVEL_SPEW,
               "addb_smap_add: "
               "mval index of %llx >= id %llx",
               (unsigned long long)last, (unsigned long long)id);

        if (duplicates_okay)
          return ADDB_ERR_EXISTS;
        else
          cl_notreached(cl,
                        "Tried to add value %llu"
                        " to %llu twice",
                        (unsigned long long)id, (unsigned long long)source);
      }

      if (nel + 1 < ADDB_GMAP_IVAL_M_NELEMS(i_val)) {
        /*  Write the element somewhere in the
         *  second half of the array.
         */
        err = addb_smap_partition_put(
            part, old_offset + nel * ADDB_GMAP_ENTRY_SIZE, id);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "addb_smap_partition_put", err,
                       "%s[%llu] += %llu", sm->sm_path,
                       (unsigned long long)source, (unsigned long long)id);
          return err;
        }

        /*  Increment the lower-end "fill level"
         *  aspect of the sentinel.
         */
        s_val++;
        cl_cover(cl);
      } else {
        /*  The array will be full once we add to it.
         *  By rewriting the last element, we both
         *  add the ID and rewrite the fill level
         *  indicator.
         */
        s_val = ADDB_GMAP_MVAL_S_MAKE_LAST(id);
        cl_cover(cl);
      }

      /* Rewrite the sentinel.
       */
      return addb_smap_partition_put(part, s_offset, s_val);
    }

    if (ADDB_GMAP_MVAL_INDEX(s_val) >= id) {
      cl_log(cl, CL_LEVEL_SPEW,
             "addb_smap_add: "
             "mval index of sentinel %llx >= id %llx",
             (unsigned long long)s_val, (unsigned long long)id);
      if (duplicates_okay)
        return ADDB_ERR_EXISTS;
      else
        cl_notreached(cl, "tried to add id %llu to %llu twice",
                      (unsigned long long)id, (unsigned long long)source);
    }
    new_size_exp = 1 + ADDB_GMAP_IVAL_M_EXP(i_val);

    cl_assert(cl, new_size_exp > 1);
  }

  /*  Case 3: We need a new array, moving either from a
   *  	smaller array we've outgrown, or from an ival slot.
   *  	The index and both arrays are modified.
   */

  /*  Try allocating an array from the freelist.
   *  If that doesn't work, grow the partition.
   */
  new_size = ADDB_GMAP_IVAL_M_EXP_TO_SIZE(new_size_exp);

  /* Try to expand this map into a new file */

  // ???

  err = addb_smap_freelist_alloc(part, new_size_exp, &new_offset);
  if (err) {
    err = addb_smap_partition_alloc(part, new_size, &new_offset);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "addb_smap_partition_alloc", err,
                   "%s: can't allocate partition for %llu", sm->sm_path,
                   (unsigned long long)new_size);
      return err;
    }
  }

  /* <new_offset> points to <new_size> bytes we can use.
   */

  /*  Fill the first half of the new array from what we had previously.
   */
  if (ADDB_GMAP_IVAL_IS_SINGLE(i_val)) {
    err =
        addb_smap_partition_put(part, new_offset, ADDB_GMAP_IVAL_SINGLE(i_val));
    if (err != 0) return err;

    new_val_sentinel = ADDB_GMAP_MVAL_S_MAKE_LAST(id);
    cl_cover(cl);
  } else {
    size_t have_nel = 1ull << (new_size_exp - 1);

    err = addb_smap_partition_copy(part, new_offset, old_offset, new_size / 2);
    if (err != 0) return err;

    /*  Add the new element.  The new element isn't itself the
     *  sentinel; if it were, our old array would have had
     *  length 1, and ADDB_GMAP_IVAL_IS_SINGLE(i_val) would have
     *  been true, above.
     */
    err = addb_smap_partition_put(part, new_offset + new_size / 2, id);
    if (err != 0) return err;

    new_val_sentinel = ADDB_GMAP_MVAL_S_MAKE_NELEMS(have_nel + 1);
    cl_cover(cl);
  }

  /* Place the new sentinel at the end of the new array.
   */
  err = addb_smap_partition_put(
      part, new_offset + new_size - ADDB_GMAP_ENTRY_SIZE, new_val_sentinel);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_smap_partition_put", err,
                 "%s: failed to write new sentinel for adding %llu "
                 "to %llu at %llu",
                 sm->sm_path, (unsigned long long)id,
                 (unsigned long long)source, new_offset);
    return err;
  }

  /*  OK, we have a new array.
   *  Repointer the index entry to the new array.
   */
  err = addb_smap_partition_put(
      part, i_offset,
      ADDB_GMAP_IVAL_MAKE_MULTI_OFFSET_EXP(new_offset, new_size_exp));
  if (err) return err;

  return err;
}
