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
#include "libaddb/addb-gmap-access.h"

#include <errno.h>
#include <stdio.h>

/*  A GMAP is a collection of collections of addb_gmap_ids
 *  in the range 1...2^34-1, indexed by addb_gmap_ids also
 *  in the range 1...2^34-1.
 *
 *  In other words, it is a function that maps one index,
 *  (called the "source"), to a sorted list of indexes.
 */

/**
 * @brief Get the number of elements of an array, if it's less
 * 	than some given number.
 *
 * @param gm 		opaque GMAP database handle
 * @param source	the source of the iteration
 * @param upper_bound	the upper bound
 * @param n_out		out: the upper bound
 *
 * @return 0 on success, a nonzero error code on error.
 * @return ADDB_ERR_NO if the source doesn't have a mapping
 */
int addb_gmap_array_n_bounded(addb_gmap* gm, addb_gmap_id source,
                              unsigned long long upper_bound,
                              unsigned long long* n_out) {
  int err;
  addb_gmap_accessor ac;

  if ((err = addb_gmap_accessor_set(gm, source, &ac)) != 0) return err;

  *n_out = addb_gmap_accessor_n(&ac);
  return 0;
}

/**
 * @brief Get the number of elements of an array.
 *
 * @param gm 		opaque GMAP database handle
 * @param source	the source of the iteration
 * @param n_out		out: how many elements in the array?
 *
 * @return the number of elements in the array.
 */
int addb_gmap_array_n(addb_gmap* gm, addb_gmap_id source,
                      unsigned long long* n_out) {
  int err;
  addb_gmap_accessor ac;

  err = addb_gmap_accessor_set(gm, source, &ac);

  cl_cover(gm->gm_addb->addb_cl);
  if (err == ADDB_ERR_NO) {
    *n_out = 0;
    return 0;
  } else if (err != 0)
    return err;

  *n_out = addb_gmap_accessor_n(&ac);
  return 0;
}

/**
 * @brief Get the last element of an array.
 *
 * @param gm 		opaque GMAP database handle
 * @param source	the source of the iteration
 * @param id_out	assign the last element to this
 *
 * @return 0 on success, a nonzero error number on error.
 * @return ADDB_ERR_NO if there is no mapping from source.
 */
int addb_gmap_array_last(addb_gmap* gm, addb_gmap_id source,
                         addb_gmap_id* id_out) {
  int err;
  unsigned long long val;
  addb_gmap_accessor ac;

  err = addb_gmap_accessor_set(gm, source, &ac);
  if (err != 0) return err;

  if (addb_gmap_accessor_n(&ac) == 1)
    *id_out = ADDB_GMAP_LOW_34(ac.gac_index);
  else {
    err = addb_gmap_accessor_get(
        &ac, (addb_gmap_accessor_n(&ac) - 1) * ADDB_GMAP_ENTRY_SIZE, &val);
    if (err) return err;

    *id_out = ADDB_GMAP_LOW_34(val);
  }
  return 0;
}

/**
 * @brief Get the Nth element of a GMAP result.
 *
 * @param gm 		opaque GMAP database handle
 * @param source	the source of the result
 * @param i 		offset (in elements, not bytes) we want to read at.
 * @param id_out 	NULL or a place to store the read ID.
 *
 * @return 0 on success, a nonzero error number on rror.
 * @return ADDB_ERR_NO if running out of array elements
 */
int addb_gmap_array_nth(addb_gmap* gm, addb_gmap_id source,
                        unsigned long long i, addb_gmap_id* id_out) {
  unsigned long long offset;
  int err;
  unsigned long long val;
  addb_gmap_accessor ac;

  err = addb_gmap_accessor_set(gm, source, &ac);
  if (err != 0) {
    cl_cover(gm->gm_addb->addb_cl);
    return ADDB_ERR_NO;
  }
  if (i >= addb_gmap_accessor_n(&ac)) {
    cl_cover(gm->gm_addb->addb_cl);
    return ADDB_ERR_NO;
  }
  if (addb_gmap_accessor_n(&ac) == 1) {
    cl_cover(gm->gm_addb->addb_cl);
    *id_out = ADDB_GMAP_LOW_34(ac.gac_index);
    return 0;
  }

  offset = ADDB_GMAP_ENTRY_SIZE * i;
  if ((err = addb_gmap_accessor_get(&ac, offset, &val))) {
    cl_cover(gm->gm_addb->addb_cl);
    cl_log_errno(gm->gm_addb->addb_cl, CL_LEVEL_ERROR, "addb_gmap_accessor_get",
                 err, "%s: cannot access gmap data for %llu[%llu]: %s",
                 addb_gmap_accessor_display_name(&ac),
                 (unsigned long long)source, (unsigned long long)i,
                 addb_xstrerror(err));
    return err;
  }
  *id_out = ADDB_GMAP_LOW_34(val);
  cl_cover(gm->gm_addb->addb_cl);
  return 0;
}

/**
 * @brief Create an accessor based on a GMAP.
 *
 *  The accessor will carry tile references for some kinds
 *  of accessor, and therefore MUST be CLOSED once it has
 *  been opened.  Don't just let these variables go out of scope,
 *  or you'll reap tile reference count assertion failures!
 *
 * @param gm 		opaque GMAP database handle
 * @param source	the source of the results
 * @param upper_bound	the upper bound
 *
 * @return 0 on success, a nonzero error code on error.
 * @return ADDB_ERR_NO if the source doesn't have a mapping
 * @return ADDB_ERR_BITMAP if the mapping is a bitmap
 */
int addb_gmap_idarray(addb_gmap* gm, addb_gmap_id source, addb_idarray* ida) {
  int err;

  err = addb_gmap_accessor_set(gm, source, &ida->ida_gac);
  if (err != 0) return err;

  /*
   * If this gmap is stored by a bgmap, return ADDB_ERR_BITMAP without
   * providing an idarray. Calling code aught be smart enough
   * to recognize this and behave like a bgmap.
   */
  if (ida->ida_gac.gac_bgmap) return ADDB_ERR_BITMAP;

  if (addb_gmap_accessor_n(&ida->ida_gac) == 1)
    addb_idarray_single(gm->gm_addb->addb_cl, ida, ida->ida_gac.gac_index);
  else
    addb_idarray_multiple(gm->gm_addb->addb_cl, ida);
  return 0;
}
