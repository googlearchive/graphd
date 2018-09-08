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
#include "libaddb/addb-bgmap.h"
#include "libaddb/addb-gmap-access.h"
#include "libaddb/addb-largefile-file.h"
#include "libaddb/addbp.h"

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
 * @param gm 		database to index
 * @param id 		source ID we're looking for
 * @param part_out	assign the partition to this
 * @param offset_out	assign the offset within the partition to this
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int addb_gmap_index_offset(addb_gmap* gm, addb_gmap_id id,
                                  addb_gmap_partition** part_out,
                                  unsigned long long* offset_out) {
  cl_handle* cl = gm->gm_addb->addb_cl;
  addb_gmap_partition* part;
  int err;
  size_t part_i;

  if (id > ADDB_GMAP_ID_MAX) {
    cl_cover(cl);
    cl_log(cl, CL_LEVEL_FAIL,
           "addb: cannot translate %llu "
           "into an index pointer (for %s) [%s:%d]",
           (unsigned long long)id, gm->gm_path, __FILE__, __LINE__);
    return ADDB_ERR_NO;
  }

  /*  Get id's partition; create a new one if needed.
   */
  part_i = id / ADDB_GMAP_SINGLE_ENTRY_N;
  part = gm->gm_partition + part_i;
  if (part->part_td == NULL) {
    /* Open the new partition. */
    if ((err = addb_gmap_partition_name(part, part_i)) != 0) {
      cl_log(cl, CL_LEVEL_ERROR, "%s: can't set partition name? [%s:%d]",
             gm->gm_path, __FILE__, __LINE__);
      return err;
    }

    err = addb_gmap_partition_open(part, ADDB_MODE_READ_WRITE);
    if (err != 0) {
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_ERROR, "%s: can't open partition: %s [%s:%d]",
             gm->gm_path, addb_xstrerror(err), __FILE__, __LINE__);
      return err;
    }
    if (part_i >= gm->gm_partition_n) {
      cl_cover(cl);
      gm->gm_partition_n = part_i + 1;
    }
  }

  id %= ADDB_GMAP_SINGLE_ENTRY_N;
  ;

  *offset_out = ADDB_GMAP_SINGLE_ENTRY_OFFSET(id);
  *part_out = part;

  return 0;
}

/*
 * These functions form a pair of callbacks that the largefile code
 * uses to set the real size of largefiles that are backing gmaps.
 * This is because it is more efficient to store the sizes in the gmaps
 * themselves but the largefile subsystem doesn't know anything about gmaps.
 */

int addb_gmap_largefile_size_get(void* cookie, unsigned long long id,
                                 size_t* size) {
  addb_gmap* gm = cookie;
  addb_gmap_partition* part;
  unsigned long long i_offset;
  unsigned long long i_val;
  cl_handle* const cl = gm->gm_addb->addb_cl;

  int err;

  err = addb_gmap_index_offset(gm, id, &part, &i_offset);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_index_offset", err,
                 "can't calculate index offset for id %llu", id);

    return err;
  }

  err = addb_gmap_partition_get(part, i_offset, &i_val);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_partition_get", err,
                 "Can't read gmap index"
                 "for id: %llu",
                 id);
    return err;
  }

  if (!ADDB_GMAP_IVAL_IS_FILE(i_val)) {
    /* This probably happens in the case
     * where we create a largefile and then decide to rollback
     * to before we made it.
     */

    cl_log(cl, CL_LEVEL_INFO,
           "addb_gmap_largefile_size_get: tried to get the "
           "size of something not a largefile. Assuming we're "
           "in the middle of a rollback.");

    *size = 0;
    return 0;
  }

  *size = ADDB_GMAP_IVAL_FILE_LENGTH(i_val);
  return 0;
}

int addb_gmap_largefile_size_set(void* cookie, unsigned long long id,
                                 size_t size) {
  addb_gmap* gm = cookie;
  addb_gmap_partition* part;
  unsigned long long i_offset;
  unsigned long long i_val;
  cl_handle* const cl = gm->gm_addb->addb_cl;

  int err;

  err = addb_gmap_index_offset(gm, id, &part, &i_offset);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_index_offset", err,
                 "can't calculate index offset for id %llu", id);

    return err;
  }

  i_val = ADDB_GMAP_IVAL_MAKE_FILE(ADDB_GMAP_LOW_34(size));

  err = addb_gmap_partition_put(part, i_offset, i_val);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_partition_put", err,
                 "Can't write gmap index for id: %llu", id);
    return err;
  }

  return 0;
}

/**
 * @brief append one entery to a largefile backed a gmap
 */
static int addb_gmap_largefile_append(addb_gmap* gm,
                                      addb_largefile_handle* handle,
                                      unsigned long long id,
                                      unsigned long long val,
                                      bool duplicates_okay) {
  addb_u5 data;
  int err;
  unsigned long long last_id;
  addb_gmap_accessor ac;

  ADDB_PUT_U5(data, val);

  err = addb_gmap_accessor_set(gm, id, &ac);
  if (err) {
    cl_log_errno(gm->gm_addb->addb_cl, CL_LEVEL_ERROR, "addb_gmap_accessor_set",
                 err, "Can't get large file accessor for %llu", id);
    return err;
  }

  err = addb_gmap_accessor_get(
      &ac, (addb_gmap_accessor_n(&ac) - 1) * ADDB_GMAP_ENTRY_SIZE, &last_id);
  if (err) {
    cl_log_errno(gm->gm_addb->addb_cl, CL_LEVEL_ERROR, "addb_gmap_accessor_get",
                 err, "Can't read largefile slot %llu",
                 addb_gmap_accessor_n(&ac) - 1);
    return err;
  }

  if (last_id >= val) {
    if (duplicates_okay)
      return ADDB_ERR_EXISTS;
    else
      cl_notreached(gm->gm_addb->addb_cl,
                    "Tried to add duplicate value %llu to %llu", val, id);
  }

  return addb_largefile_append(handle, id, (char const*)data, sizeof data);
}

int addb_gmap_bgmap_read_size(addb_gmap* gm, addb_gmap_id s,
                              unsigned long long* n) {
  addb_gmap_partition* part;
  unsigned long long i_offset, i_val;
  int err;
  cl_handle* cl = gm->gm_addb->addb_cl;

  err = addb_gmap_index_offset(gm, s, &part, &i_offset);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_index_offset", err,
                 "can't get gmap partition for %llu", (unsigned long long)s);
    return err;
  }

  err = addb_gmap_partition_get(part, i_offset, &i_val);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "addb_gmap_partition_get", err,
                 "i_offset=%llu", i_offset);
    return err;
  }

  cl_assert(cl, ADDB_GMAP_IVAL_IS_BGMAP(i_val));

  *n = ADDB_GMAP_IVAL_FILE_LENGTH(i_val);

  cl_log(cl, CL_LEVEL_SPEW, "bgmap %llu size: %llu", (unsigned long long)s, *n);
  return 0;
}

static int addb_gmap_bgmap_write_size(addb_gmap* gm, addb_gmap_id s,

                                      unsigned long long n) {
  addb_gmap_partition* part;
  unsigned long long i_offset, i_val;
  int err;
  cl_handle* cl = gm->gm_addb->addb_cl;

  err = addb_gmap_index_offset(gm, s, &part, &i_offset);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_index_offset", err,
                 "can't get gmap part of offset for: %llu",
                 (unsigned long long)s);
    return err;
  }
  i_val = ADDB_GMAP_IVAL_MAKE_BGMAP(n);

  err = addb_gmap_partition_put(part, i_offset, i_val);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_partition_put", err,
                 "can't write %llu to %llu (s=%llu)", i_val, i_offset,
                 (unsigned long long)s);
    return err;
  }
  return 0;
}

/* If an id array could profitably be represented by
 * a bitmap, return the size of the id array.  If not,
 * return 0;
 */
static unsigned long long addb_gmap_bgmap_decide(addb_gmap* gm,
                                                 addb_gmap_id source,
                                                 addb_gmap_id high) {
  addb_gmap_accessor ac;
  int err = addb_gmap_accessor_set(gm, source, &ac);
  if (err) {
    cl_log_errno(gm->gm_addb->addb_cl, CL_LEVEL_FAIL, "addb_gmap_accessor_set",
                 err, "Can't get number of elements in %llu",
                 (unsigned long long)source);

    return 0;
  }

  unsigned long long gm_size = addb_gmap_accessor_n(&ac);
  if ((gm_size * 40 > high) && (gm_size > 128 * 1024)) return gm_size;

  return 0;
}

static int addb_gmap_bgmap_create(addb_gmap* gm, addb_gmap_id s) {
  int err;
  int n, i;
  cl_handle* cl = gm->gm_addb->addb_cl;
  addb_bgmap* bg;
  addb_gmap_accessor ac;
  unsigned long long val;

  /*
   * XXX we should pass a flag here that forced the file to
   * be truncated if it already exists. Otherwise, a well timed
   * crash might put in bogus data when the file is re-created.
   */
  err = addb_bgmap_lookup(gm, s, &bg);

  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_bgmap_lookup:", err,
                 "can't make bgmap for id: %llu", (unsigned long long)s);
    return err;
  }

  err = addb_gmap_accessor_set(gm, s, &ac);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_accessor_set", err,
                 "Can't read gmap for id %llu", (unsigned long long)s);
    return err;
  }

  n = addb_gmap_accessor_n(&ac);

  for (i = 0; i < n; i++) {
    addb_gmap_accessor_get(&ac, i * 5, &val);
    val = ADDB_GMAP_LOW_34(val);
    err = addb_bgmap_append(gm, bg, val);

    if (err) {
      cl_log_errno(cl, CL_LEVEL_ERROR, "addb_bgmap_append", err,
                   "can't append %llu to %llu", val, (unsigned long long)s);
      return err;
    }
  }

  /*
   * Now that we've copied the bmap, lets write the new index
   * record
   */

  err = addb_gmap_bgmap_write_size(gm, s, n);
  if (err) return err;

  ac.gac_lf->lf_delete = true;
  ac.gac_lf->lf_delete_count = 2;
  return 0;
}

/*
 * Append val to the bgmap for s
 */
static int addb_gmap_bgmap_append(addb_gmap* gm, addb_gmap_id s,
                                  addb_gmap_id val) {
  int err;
  cl_handle* cl = gm->gm_addb->addb_cl;
  addb_bgmap* bg;
  unsigned long long bitcount;

  err = addb_bgmap_lookup(gm, s, &bg);

  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_bgmap_lookup", err,
                 "Can't get bgmap for id: %llu", (unsigned long long)s);
    return err;
  }

  err = addb_bgmap_append(gm, bg, val);

  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_bgmap_append", err,
                 "Can't append to %llu", (unsigned long long)s);
    return err;
  }

  err = addb_gmap_bgmap_read_size(gm, s, &bitcount);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_bgmap_append", err,
                 "Can't read size for bmap: %llu", (unsigned long long)s);
    return err;
  }

  bitcount++;

  err = addb_gmap_bgmap_write_size(gm, s, bitcount);

  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_bgmap_append", err,
                 "can't set size for bgmap: %llu", (unsigned long long)s);
    return err;
  }

  return 0;
}

/**
 * @brief  create a largefile from a gmap array.
 *
 *  This create a largefile from a gmap:id pair and copies the contents
 *  of the gmap into the new file.
 *
 */

static int addb_gmap_largefile_create(addb_gmap* gm, addb_gmap_id s) {
  cl_handle* const cl = gm->gm_addb->addb_cl;
  unsigned long long ival;
  unsigned long long sval;
  unsigned long long offset;   /* offset in the gmap partition */
  unsigned long long ival_pos; /* offset of the ival in the gmap */
  unsigned long long len;      /* how much data to copy */
  addb_largefile* lf = NULL;
  addb_gmap_partition* part = NULL; /* partition to copy from */
  int err;

  cl_log(cl, CL_LEVEL_DEBUG,
         "Promoting gmap array "
         "%s[%llu] to its own file.",
         gm->gm_path, (unsigned long long)s);

  /* Step 1: create the file.
   * The callbacks to keep track of the data size in the gmap index slot
   * are specifically disabled until we turn them on at the end of
   * this function. This makes it easy to bail out of anything goes wrong.
   * */
  err = addb_largefile_new(gm->gm_lfhandle, s, gm->gm_cf.gcf_lf_init_map, &lf);
  if (err) return err;

  /* Step 2: Get everything we need to copy the data over */
  err = addb_gmap_index_offset(gm, s, &part, &ival_pos);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_index_offset", err,
                 "Unable to get partition and offset");
    goto bail;
  }
  err = addb_gmap_partition_get(part, ival_pos, &ival);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_partition_get", err,
                 "unable to get index from partition for "
                 "ival_pos=%llu",
                 ival_pos);
    goto bail;
  }
  cl_assert(cl, !ADDB_GMAP_IVAL_IS_SINGLE(ival));

  offset = ADDB_GMAP_MULTI_ENTRY_OFFSET(ival);
  err = addb_gmap_partition_get(
      part, offset + ADDB_GMAP_IVAL_M_SIZE(ival) - ADDB_GMAP_ENTRY_SIZE, &sval);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_partition_get", err,
                 "offset %llu + ADDB_GMAP_IVAL_M_SIZE %llu - %d", offset,
                 (unsigned long long)ADDB_GMAP_IVAL_M_SIZE(ival),
                 (int)ADDB_GMAP_ENTRY_SIZE);

    goto bail;
  }
  if (ADDB_GMAP_MVAL_S_IS_FULL(sval))
    len = ADDB_GMAP_IVAL_M_NELEMS(ival) * ADDB_GMAP_ENTRY_SIZE;
  else
    len = ADDB_GMAP_MVAL_S_NELEMS(sval) * ADDB_GMAP_ENTRY_SIZE;

  /* Step 3: Copy the data.
   * At this point:
   * len is the number of bytes left to copy
   * offset is the current source copy pointer
   * old_offset is the location of the index pointer, used later.
   */
  while (len > 0) {
    addb_tiled_reference tref;
    const char* data_start;
    const char* data_end;
    size_t size;

    err = addb_gmap_partition_get_chunk(part, offset, &data_start, &data_end,
                                        &tref);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_partition_get_chunk", err,
                   "Unable to get chunk at offset: %llu", offset);

      goto bail;
    }

    size = data_end - data_start;
    if (size > len) size = len;

    cl_log(cl, CL_LEVEL_SPEW,
           "addb_gmap_largefile_create:"
           " Copying %zu bytes from %llu[%p]",
           size, offset, data_start);

    err = addb_largefile_append(gm->gm_lfhandle, s, data_start, size);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_ERROR, "addb_largefile_append", err,
                   "Unable to %zu bytes to new gmap largefile", size);
      addb_tiled_free(part->part_td, &tref);

      goto bail;
    }
    addb_tiled_free(part->part_td, &tref);

    offset += size;
    len -= size;
  }

  err = addb_largefile_new_done(gm->gm_lfhandle, s);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "addb_largefile_new_commit", err,
                 "Can't store new index pointer for gmap id"
                 " %llu",
                 (unsigned long long)s);
    return err;
  }

  return 0;

bail:
  cl_assert(cl, err);
  cl_log(cl, CL_LEVEL_ERROR,
         "Failed to create a largefile for index %llu, map: %s."
         " Falling back to in-line storage",
         (unsigned long long)s, gm->gm_path);

  return err;
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
static int addb_gmap_partition_alloc(addb_gmap_partition* part, size_t size,
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
   *  to always allocate single-tile fragments only; but gmap
   *  and istore share the tile manager, and istore *does* want
   *  the multi-sized tiles.)
   */

  while (s / ADDB_TILE_SIZE < (e - 1) / ADDB_TILE_SIZE) {
    unsigned long long boundary;

    /* Allocate a partial tile.
     */
    boundary = ((s / ADDB_TILE_SIZE) + 1) * ADDB_TILE_SIZE;

    cl_assert(part->part_gm->gm_addb->addb_cl, boundary < e);

    ptr = addb_tiled_alloc(part->part_td, s, boundary, &tref);
    if (ptr == NULL) return errno ? errno : ENOMEM;

    addb_tiled_free(part->part_td, &tref);
    s = boundary;
  }

  /* Allocate the final fragment */

  ptr = addb_tiled_alloc(part->part_td, s, e, &tref);
  if (ptr == NULL) return errno ? errno : ENOMEM;

  addb_tiled_free(part->part_td, &tref);

  err = addb_gmap_partition_grow(part, e);
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
 * @param gm 	 opaque gmap module handle
 * @param source the left-hand-side of the 1:N relationship
 * @param id 	 new element added to the right-hand-side of the relationship,
 *			0...2^34-1
 *
 * @return 0 on success, an error number on error.
 */

int addb_gmap_add(addb_gmap* gm, addb_gmap_id source, addb_gmap_id id,
                  bool duplicates_okay) {
  cl_handle* cl;
  addb_gmap_partition* part;
  int err;

  unsigned long long i_val, i_offset;
  unsigned long long old_offset;
  unsigned long long new_size_exp, new_val_sentinel;
  unsigned long long new_offset, new_size;

  cl = gm->gm_addb->addb_cl;

  cl_log(cl, CL_LEVEL_SPEW, "%s: gmap add %llu %llu", gm->gm_path,
         (unsigned long long)source, (unsigned long long)id);

  cl_assert(cl, id < (1ull << 34));

  err = addb_gmap_index_offset(gm, source, &part, &i_offset);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "addb_gmap_index_offset", err,
                 "%s: can't get index offset for %llu", gm->gm_path,
                 (unsigned long long)source);
    return err;
  }

  err = addb_gmap_partition_get(part, i_offset, &i_val);
  if (err != 0) {
    if (err == ADDB_ERR_NO) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "addb_gmap_partition_get", err,
                   "i_offset=%llu", i_offset);

      i_val = ADDB_GMAP_IVAL_MAKE_SINGLE(id);
      return addb_gmap_partition_put(part, i_offset, i_val);
    }
    cl_log_errno(cl, CL_LEVEL_FAIL, "addb_gmap_partition_get", err,
                 "%s: can't get partition for index offset %llu", gm->gm_path,
                 (unsigned long long)i_offset);
    return err;
  }

  if (ADDB_GMAP_IVAL_IS_EMPTY(i_val)) {
    /*  Case 1: There is no entry.  Create one in place
     *  	of the "empty" placeholder.
     */

    cl_cover(cl);

    i_val = ADDB_GMAP_IVAL_MAKE_SINGLE(id);
    return addb_gmap_partition_put(part, i_offset, i_val);
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
                      "Tried to add value %llu to gmap %llu"
                      " twice",
                      (unsigned long long)source, (unsigned long long)id);
    }
    new_size_exp = 1;
    cl_cover(cl);
    old_offset = i_offset;
  } else if (ADDB_GMAP_IVAL_IS_FILE(i_val)) {
    unsigned long long new_bitmap_length =
        gm->gm_bitmap ? addb_gmap_bgmap_decide(gm, source, id) : 0;

    if (new_bitmap_length) {
      cl_log(cl, CL_LEVEL_INFO,
             "addb_gmap_add: gmap %llu, %llu entries -> bmap",
             (unsigned long long)source, new_bitmap_length);

      err = addb_gmap_bgmap_create(gm, source);
      if (err)
        cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_bgmap_create", err,
                     "Promotion of %llu failed "
                     "Using largefile append instead",
                     (unsigned long long)source);
      else
        return addb_gmap_bgmap_append(gm, source, id);
    }

    return addb_gmap_largefile_append(gm, gm->gm_lfhandle, source, id,
                                      duplicates_okay);
  } else if (ADDB_GMAP_IVAL_IS_BGMAP(i_val)) {
    return addb_gmap_bgmap_append(gm, source, id);
  } else {
    unsigned long long s_offset, s_val;

    old_offset = ADDB_GMAP_MULTI_ENTRY_OFFSET(i_val);
    s_offset = old_offset + ADDB_GMAP_IVAL_M_SIZE(i_val) - ADDB_GMAP_ENTRY_SIZE;

    err = addb_gmap_partition_get(part, s_offset, &s_val);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "addb_gmap_partition_get", err,
                   "addb_gmap_add %s: can't "
                   "get partition value for %llu",
                   gm->gm_path, (unsigned long long)s_offset);
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
      cl_assert(part->part_gm->gm_addb->addb_cl, nel > 0);

      /*  Get the last element.  Check if it
       *  is >= what we're trying to append.
       */
      err = addb_gmap_partition_get(
          part, old_offset + (nel - 1) * ADDB_GMAP_ENTRY_SIZE, &last);
      if (err) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "addb_gmap_partition_get", err,
                     "addb_gmap_add %s: can't "
                     "get partition value for "
                     "%llu: ",
                     gm->gm_path,
                     (unsigned long long)(old_offset +
                                          (nel - 1) * ADDB_GMAP_ENTRY_SIZE));
        return err;
      }
      if (ADDB_GMAP_MVAL_INDEX(last) >= id) {
        cl_log(cl, CL_LEVEL_SPEW,
               "addb_gmap_add: "
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
        err = addb_gmap_partition_put(
            part, old_offset + nel * ADDB_GMAP_ENTRY_SIZE, id);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "addb_gmap_partition_put", err,
                       "%s[%llu] += %llu", gm->gm_path,
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
      return addb_gmap_partition_put(part, s_offset, s_val);
    }

    if (ADDB_GMAP_MVAL_INDEX(s_val) >= id) {
      cl_log(cl, CL_LEVEL_SPEW,
             "addb_gmap_add: "
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
  if (!gm->gm_cf.gcf_split_thr) {
    gm->gm_cf.gcf_split_thr = 14;
    cl_log(cl, CL_LEVEL_INFO,
           "gcf_split_thr is zero."
           "Changing to 14 for now.");
  }
  cl_assert(cl, gm->gm_cf.gcf_split_thr);
  if (new_size_exp > gm->gm_cf.gcf_split_thr) {
    cl_assert(cl, !ADDB_GMAP_IVAL_IS_FILE(i_val));
    err = addb_gmap_largefile_create(gm, source);
    if (err)
      cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_largefile_create", err,
                   "%s: Unable to create largefile for %llu, "
                   "continuing with gmap allocation",
                   gm->gm_path, (unsigned long long)source);
    /* fall through to in-gmap case */
    else {
      err = addb_gmap_largefile_append(gm, gm->gm_lfhandle, source, id,
                                       duplicates_okay);
      if (err && err != ADDB_ERR_EXISTS)
        cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_largefile_append", err,
                     "%s: Unable to append to new "
                     "largefile for %llu",
                     gm->gm_path, (unsigned long long)source);
      else if (err == ADDB_ERR_EXISTS)
        return ADDB_ERR_EXISTS;

      /* If the append call fails, we've moved this map into
       * its own file but were then unable to append to it.
       * we need to return the error but still free the old
       * map
       */
      goto free_old_array;
    }
  }
  err = addb_gmap_freelist_alloc(part, new_size_exp, &new_offset);
  if (err) {
    err = addb_gmap_partition_alloc(part, new_size, &new_offset);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "addb_gmap_partition_alloc", err,
                   "%s: can't allocate partition for %llu", gm->gm_path,
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
        addb_gmap_partition_put(part, new_offset, ADDB_GMAP_IVAL_SINGLE(i_val));
    if (err != 0) return err;

    new_val_sentinel = ADDB_GMAP_MVAL_S_MAKE_LAST(id);
    cl_cover(cl);
  } else {
    size_t have_nel = 1ull << (new_size_exp - 1);

    err = addb_gmap_partition_copy(part, new_offset, old_offset, new_size / 2);
    if (err != 0) return err;

    /*  Add the new element.  The new element isn't itself the
     *  sentinel; if it were, our old array would have had
     *  length 1, and ADDB_GMAP_IVAL_IS_SINGLE(i_val) would have
     *  been true, above.
     */
    err = addb_gmap_partition_put(part, new_offset + new_size / 2, id);
    if (err != 0) return err;

    new_val_sentinel = ADDB_GMAP_MVAL_S_MAKE_NELEMS(have_nel + 1);
    cl_cover(cl);
  }

  /* Place the new sentinel at the end of the new array.
   */
  err = addb_gmap_partition_put(
      part, new_offset + new_size - ADDB_GMAP_ENTRY_SIZE, new_val_sentinel);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_partition_put", err,
                 "%s: failed to write new sentinel for adding %llu "
                 "to %llu at %llu",
                 gm->gm_path, (unsigned long long)id,
                 (unsigned long long)source, new_offset);
    return err;
  }

  /*  OK, we have a new array.
   *  Repointer the index entry to the new array.
   */
  err = addb_gmap_partition_put(
      part, i_offset,
      ADDB_GMAP_IVAL_MAKE_MULTI_OFFSET_EXP(new_offset, new_size_exp));
  if (err) return err;

free_old_array:

  /*  If we had a previous array, insert it at the head of
   *  the free list for its size.
   */
  if (new_size_exp > 1) {
    int new_err = addb_gmap_freelist_free(part, old_offset, new_size_exp - 1);

    if (new_err) {
      cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_freelist_free", new_err,
                   "%s: couldn't add array to freelist. Leaking.", gm->gm_path);
      if (!err) err = new_err;
    }
  }

  return err;
}
