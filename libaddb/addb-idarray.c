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
#include "libaddb/addb-gmap-access.h"
#include "libaddb/addbp.h"

#include <errno.h>
#include <stdio.h>

/*  An IDARRAY is a big piece of vitual storage that contains
 *  IDs.
 *
 *  IDARRAYs come out of largefiles or gmaps; possibly via hmaps.
 *  They are opened and closed; in between, they can be read
 *  as index arrays (no need to worry about the 5-byte stuff)
 *  or as sequences of memory fragments.
 *
 *  In the memory fragment case, each ID is represented by the
 *  lower 34 bits in a 5-byte sequence.  (The caller has to mask
 *  out those 34 bits.)
 *
 *  The byte memory has byte addresses from 0 through 5(n-1); the
 *  id memory copies out into IDs addressed from 0 through n-1.
 *
 *  To create an idarray, use
 *
 *	addb_gmap_idarray()	[addb-gmap-array.c]
 *	addb_hmap_idarray()	[addb-hmap.c]
 *
 *  Once it is created, it must be destroyed with addb_idarray_finish().
 */

/**
 * @brief Destroy an id array handle
 *
 *  Specifically, there may be a link-counted tile reference
 *  inside the array that may need freeing.
 *
 * @param ida 		the id array
 */
void addb_idarray_finish(addb_idarray *ida) {
  if (ida != NULL && !ida->ida_is_single) {
    addb_tiled *td;

    /* If the addb accessor is pointing to a largefile, use that
     * td; otherwise it's a gmap partition, so use that one.
     */
    td = (ida->ida_gac.gac_lf != NULL ? ida->ida_gac.gac_lf->lf_td
                                      : ida->ida_gac.gac_part->part_td);

    /* Largefiles can be closed. If this is the case, td is already
     * freed and this does not need to happen again. Likewise, a
     * partition might not be open yet.
     */
    if (td != NULL) {
      addb_tiled_free(td, &ida->ida_tref);
    } else {
      /* Suspiciously, td is null. We better not have a valid reference to it.
       */
      cl_assert(ida->ida_cl, ida->ida_tref == ADDB_TILED_REFERENCE_EMPTY);

      cl_log(ida->ida_cl, CL_LEVEL_INFO,
             "We tried to free something that's "
             "already gone. Strange bookkeeping?");
    }

    ida->ida_tref = ADDB_TILED_REFERENCE_EMPTY;
  }
}

/**
 * @brief Set an idarray to a well-defined value.
 *
 *  Finishing an addb_idarray that has been
 *  initialized is safe (and does nothing).
 *
 * @param ida 		the id array
 */
void addb_idarray_initialize(addb_idarray *ida) { ida->ida_is_single = true; }

/**
 * @brief Get a pointer to bytes starting at an offset
 *
 *  The bytes are part of an index-array where the lowest 34 bits
 *  of a ADDB_GMAP_ENTRY_SIZE-byte sequence encode an ID.
 *  (The high bits may or may not be used for other things.)
 *
 *  The pointers and offsets returned may not be multiples
 *  of ADDB_GMAP_ENTRY_SIZE or aligned on ADDB_GMAP_ENTRY_SIZE-byte
 *  boundaries.
 *
 *  Blocks of memory returned by calls with successive offsets
 *  may not be physically adjacent, although they often will be.
 *
 *  Offsets start at 0 and continue through n*ADDB_GMAP_ENTRY_SIZE-1, inclusive.
 *  A call with a start offset greater or equal to an end offset
 *  returns ADDB_ERR_NO.
 *
 *  A reference to the most recently returned tile is carried
 *  within the idarray,  That means that no more than one value
 *  returned by an idarray at a time does not stay valid;
 *  use the addb_idarray_read() interface to head off that kind
 *  of problem.
 *
 * @param ida			array we're doing this with
 * @param start_offset		offset of first byte to read
 * @param end_offset		offset of first byte we're not interested in
 * @param ptr_out		out: pointer to the first byte
 * @param end_offset_out	out: offset of first byte we didn't get
 *
 * @return 0 on success, a nonzero error code for unexpected errors.
 */
int addb_idarray_read_raw(addb_idarray *ida, unsigned long long start_offset,
                          unsigned long long end_offset,
                          unsigned char const **ptr_out,
                          unsigned long long *end_offset_out) {
  int err = 0;
  addb_tiled_reference tref;
  addb_tiled *td;

  /*
  cl_log(ida->ida_cl, CL_LEVEL_VERBOSE, "addb_idarray_read_raw: ida %p; start
  %llu, end %llu; n %llu",
  (void *)ida, start_offset, end_offset, (unsigned long
  long)ida->ida_gac.gac_length);
  */

  if (ida->ida_is_single) {
    if (end_offset > ADDB_GMAP_ENTRY_SIZE) end_offset = ADDB_GMAP_ENTRY_SIZE;

    if (start_offset >= end_offset) return ADDB_ERR_NO;

    *ptr_out = ida->ida_single_bytes + start_offset;
    *end_offset_out = end_offset;
  } else {
    unsigned long long accessor_end_offset;
    unsigned long long accessor_n_bytes =
        ida->ida_gac.gac_length * ADDB_GMAP_ENTRY_SIZE;

    if (end_offset > accessor_n_bytes) end_offset = accessor_n_bytes;

    if (start_offset >= end_offset) {
      cl_log(ida->ida_cl, CL_LEVEL_FAIL,
             "addb_idarray_read_raw: "
             "start %llu >= adjusted end %llu",
             start_offset, end_offset);
      return ADDB_ERR_NO;
    }

    if (ida->ida_gac.gac_lf != NULL) {
      err = addb_largefile_read_raw(ida->ida_gac.gac_lf,
                                    ida->ida_gac.gac_offset + start_offset,
                                    ida->ida_gac.gac_offset + end_offset,
                                    ptr_out, &accessor_end_offset, &tref);
      if (err != 0) {
        cl_log_errno(ida->ida_cl, CL_LEVEL_FAIL, "addb_largefile_read_raw", err,
                     "%llu..%llu", ida->ida_gac.gac_offset + start_offset,
                     ida->ida_gac.gac_offset + end_offset - 1);
        return err;
      }
      td = ida->ida_gac.gac_lf->lf_td;
    } else {
      err = addb_gmap_partition_read_raw(ida->ida_gac.gac_part,
                                         ida->ida_gac.gac_offset + start_offset,
                                         ida->ida_gac.gac_offset + end_offset,
                                         ptr_out, &accessor_end_offset, &tref);
      if (err != 0) {
        cl_log_errno(ida->ida_cl, CL_LEVEL_FAIL, "addb_gmap_partition_read_raw",
                     err, "%llu..%llu", ida->ida_gac.gac_offset + start_offset,
                     ida->ida_gac.gac_offset + end_offset - 1);
        return err;
      }
      td = ida->ida_gac.gac_part->part_td;
    }

    /*  The offset that the user deals with don't include
     *  the header; ours may.
     */
    *end_offset_out = accessor_end_offset - ida->ida_gac.gac_offset;

    /* Free the previous reference (if any); store the new one.
     */
    addb_tiled_free(td, &ida->ida_tref);
    ida->ida_tref = tref;
  }
  return 0;
}

/**
 * @brief Read one index starting at an offset.
 *
 *  Unlike addb_idarray_read_raw(), above, this call does not
 *  terminate early if it's convenient; it converts ids from
 *  the native representation to ID representation until it runs
 *  out of bytes to convert.
 *
 * @param ida		Array we're reading from
 * @param offset 	the offset we want
 * @param id_out	assign id to here.
 *
 * @return 0 on success
 * @return ADDB_ERR_NO if the idarray is shorter than {offset}.
 * @return other nonzero error codes on unexpected system error
 */
int addb_idarray_read1(addb_idarray const *ida, unsigned long long offset,
                       addb_id *id_out) {
  unsigned long long val;
  int err;

  if (offset >= ida->ida_gac.gac_length) return ADDB_ERR_NO;

  if (ida->ida_is_single) {
    *id_out = ida->ida_single_id;
    return 0;
  }

  offset *= ADDB_GMAP_ENTRY_SIZE;

  err = addb_gmap_accessor_get(&ida->ida_gac, offset, &val);
  if (err != 0) {
    cl_log(ida->ida_cl, CL_LEVEL_ERROR,
           "addb_idarray_read1(%s): cannot access gmap"
           "data for %llu: %s",
           addb_gmap_accessor_display_name(&ida->ida_gac),
           (unsigned long long)offset, addb_xstrerror(err));
    return err;
  }

  *id_out = ADDB_GMAP_LOW_34(val);
  return 0;
}

/**
 * @brief Read some indices starting at an index offset.
 *
 *  Unlike addb_idarray_read_raw(), above, this call does not
 *  terminate early if it's convenient; it converts ids from
 *  the native representation to ID representation until it runs
 *  out of bytes to convert.
 *
 * @param ida	Array we're reading from
 * @param start	the first index we want
 * @param end	first index we don't want
 * @param id_buf  caller's buffer to write indices into
 * @param end_out end at which we stopped copying (because
 *	we ran out of indices in the array).
 *
 * @return 0 on success
 * @return ADDB_ERR_NO if the idarray is shorter than {start}.
 * @return other nonzero error codes on unexpected system error
 */
int addb_idarray_read(addb_idarray *ida, unsigned long long start,
                      unsigned long long end, addb_id *id_buf,
                      unsigned long long *end_out) {
  int err = 0;

  unsigned char const *ptr;
  unsigned long long raw_start, raw_end, raw_next;
  addb_id *id = id_buf;
  addb_id acc = 0;
  int missing = 0;

  /*  Translate the id offsets into byte offsets.
   */
  raw_start = start * ADDB_GMAP_ENTRY_SIZE;
  raw_end = end * ADDB_GMAP_ENTRY_SIZE;

  cl_log(ida->ida_cl, CL_LEVEL_VERBOSE,
         "addb_idarray_read: ida %p; start %llu, end %llu", (void *)ida, start,
         end);

  if (ida->ida_is_single) {
    if (start > 0) return ADDB_ERR_NO;

    *id_buf = ida->ida_single_id;
    *end_out = 1;

    cl_log(ida->ida_cl, CL_LEVEL_VERBOSE,
           "addb_idarray_read(%llu..%llu): single %llu", start, end,
           (unsigned long long)*id_buf);
    return 0;
  }

  while (raw_start < raw_end) {
    err = addb_idarray_read_raw(ida, raw_start, raw_end, &ptr, &raw_next);
    if (err != 0) {
      if (err == ADDB_ERR_NO) {
        *end_out = start + (id - id_buf);
        return *end_out == start ? ADDB_ERR_NO : 0;
      }
      cl_log_errno(ida->ida_cl, CL_LEVEL_FAIL, "addb_idarray_read_raw", err,
                   "(%llu..%llu)", raw_start, raw_end - 1);
      return err;
    }

    cl_log(ida->ida_cl, CL_LEVEL_VERBOSE,
           "addb_idarray_read: got %p %llu..%llu", (void *)ptr, raw_start,
           raw_next);

    /*  Complete previous bytes.
     */
    if (missing > 0) {
      while (raw_start < raw_next && missing > 0) {
        acc <<= 8;
        acc |= *ptr++;

        raw_start++;
        missing--;
      }
      if (missing == 0) {
        *id++ = ADDB_GMAP_LOW_34(acc);
        acc = 0;
      }
    }

    while (raw_start + ADDB_GMAP_ENTRY_SIZE <= raw_next) {
      acc = *ptr++;
      acc <<= 8;
      acc |= *ptr++;
      acc <<= 8;
      acc |= *ptr++;
      acc <<= 8;
      acc |= *ptr++;
      acc <<= 8;
      acc |= *ptr++;

      raw_start += ADDB_GMAP_ENTRY_SIZE;
      *id++ = ADDB_GMAP_LOW_34(acc);
    }

    if (raw_start < raw_next) {
      missing = ADDB_GMAP_ENTRY_SIZE;
      acc = 0;

      while (raw_start < raw_next) {
        acc <<= 8;
        acc |= *ptr++;

        raw_start++;
        missing--;
      }
    }
  }
  *end_out = start + (id - id_buf);
  return err;
}

/**
 * @brief Find the offset of an id in an idarray.
 *
 *  If the id exists between the listed constraints
 *
 *  - *off_out is set to its offset, and *id_out is set to the id.
 *
 *  If the id isn't found:
 *
 *  - if it is smaller than the last item, the index
 *    of the smallest larger item is returned in *off_out,
 *    and *id_out is set to that smallest larger item.
 *
 *  - otherwise, e is returned in *off_out, and *id_out
 *	is set to the id.
 *
 * @param ida	array to search in
 * @param s	the start of the offset range in which to search
 * @param e	end of the offset range in which to search;
 *		that is, the first excluded offset
 * @param id	id to search for
 * @param off_out assign the offset of id, or of the next larger id, here
 * @param id_out  assign the id at offset here
 *
 * @return 0 on success (even if the ID itself wasn't found)
 * @return nonzero system error codes for unexpected error.
 */
int addb_idarray_search(addb_idarray *ida, unsigned long long s,
                        unsigned long long e, addb_id id,
                        unsigned long long *off_out, addb_id *id_out) {
  unsigned long long s_orig = s;
  unsigned long long e_orig = e;
  int err = 0;
  unsigned long long endval = id;

  if (ida->ida_is_single) {
    if (id <= ida->ida_single_id && s <= 0 && e >= 1) {
      *off_out = 0;
      *id_out = ida->ida_single_id;
    } else {
      *off_out = e;
      *id_out = id;
    }

  done:
    cl_log(ida->ida_cl, CL_LEVEL_VERBOSE,
           "addb_idarray_search for id=%llu in %p between "
           "s=%llu and e=%llu: %llu at %llu",
           (unsigned long long)id, (void *)ida, s_orig, e_orig,
           (unsigned long long)*id_out, *off_out);

    return 0;
  }

  while (s < e) {
    unsigned long long val, middle, e_new;
    unsigned char const *ptr;

    /*  Can we do this whole subsearch in the byte domain?
     */
    err = addb_idarray_read_raw(ida, s * ADDB_GMAP_ENTRY_SIZE,
                                e * ADDB_GMAP_ENTRY_SIZE, &ptr, &e_new);
    if (err != 0) return err;

    if (e_new == e * ADDB_GMAP_ENTRY_SIZE) {
      unsigned char const *s_ptr, *e_ptr, *m_ptr, *s0_ptr;

      /*  Yes!  That was the last roundtrip to the tile
       *  manager for the rest of this bsearch.  Yay.
       */
      s0_ptr = s_ptr = ptr;
      e_ptr = ptr + (e - s) * ADDB_GMAP_ENTRY_SIZE;

      while (s_ptr < e_ptr) {
        m_ptr = s_ptr + (((e_ptr - s_ptr) / 5) / 2) * 5;
        cl_assert(ida->ida_cl, m_ptr >= s_ptr && m_ptr < e_ptr);

        val = 0x3 & *m_ptr++;
        val = (val << 8) | *m_ptr++;
        val = (val << 8) | *m_ptr++;
        val = (val << 8) | *m_ptr++;
        val = (val << 8) | *m_ptr++;

        /* We've advanced m_ptr by one entry
         * reading val.
         */
        if (val == id) {
          *off_out = s + ((m_ptr - 5) - s0_ptr) / 5;
          *id_out = id;
          goto done;
        }
        if (val > id) {
          e_ptr = m_ptr - 5;
          endval = val;
        } else
          s_ptr = m_ptr;
      }
      e = s + (e_ptr - s0_ptr) / 5;
      break;
    }

    /* No. Well, let's just do it the slow way.
     */

    middle = s + (e - s) / 2;

    err = addb_gmap_accessor_get(&ida->ida_gac, middle * ADDB_GMAP_ENTRY_SIZE,
                                 &val);
    if (err != 0) {
      cl_log_errno(ida->ida_cl, CL_LEVEL_FAIL, "addb_gmap_accessor_get", err,
                   "for [%llu]", (unsigned long long)middle);
      return err;
    }

    val = ADDB_GMAP_LOW_34(val);
    if (val > id) {
      e = middle;
      endval = val;
    } else if (val < id)
      s = middle + 1;
    else {
      *off_out = middle;
      *id_out = id;

      goto done;
    }
  }
  *off_out = e;
  *id_out = endval;

  goto done;
}

/**
 * @brief Return the number of elements in an idarray.
 * @param ida an idarray, created with addb_idarray_open()
 * @return the number of elements in an idarray.
 */
unsigned long long addb_idarray_n(addb_idarray const *ida) {
  return addb_gmap_accessor_n(&ida->ida_gac);
}

/**
 * @brief Create an idarray with a single ID in it.
 *
 * @param ida 	idarray
 * @param id	the single ID
 *
 * @return 0 on success, a nonzero error code on error.
 * @return ADDB_ERR_NO if the source doesn't have a mapping
 */
void addb_idarray_single(cl_handle *cl, addb_idarray *ida, addb_id id) {
  id = ADDB_GMAP_LOW_34(id);

  ADDB_TILED_REFERENCE_INITIALIZE(ida->ida_tref);
  ida->ida_gac.gac_part = NULL;
  ida->ida_gac.gac_lf = NULL;
  ida->ida_gac.gac_length = 1;
  ida->ida_gac.gac_offset = 0;
  ida->ida_cl = cl;
  ida->ida_is_single = true;
  ida->ida_single_id = id;

  ida->ida_single_bytes[0] = 0x03 & (id >> 32);
  ida->ida_single_bytes[1] = 0xFF & (id >> 24);
  ida->ida_single_bytes[2] = 0xFF & (id >> 16);
  ida->ida_single_bytes[3] = 0xFF & (id >> 8);
  ida->ida_single_bytes[4] = 0xFF & id;

  cl_log(cl, CL_LEVEL_VERBOSE, "addb_idarray_single: %p", (void *)ida);
}

/**
 * @brief Create an idarray with multiple ids in it.
 *
 * @param ida 	idarray
 * @param id	the single ID
 *
 * @return 0 on success, a nonzero error code on error.
 * @return ADDB_ERR_NO if the source doesn't have a mapping
 */
void addb_idarray_multiple(cl_handle *cl, addb_idarray *ida) {
  ida->ida_cl = cl;
  ADDB_TILED_REFERENCE_INITIALIZE(ida->ida_tref);
  ida->ida_is_single = false;
}
