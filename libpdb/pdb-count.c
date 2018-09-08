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
#include "libpdb/pdbp.h"

#include <errno.h>

/**
 * @brief How many entries between low and high in this idarray?
 *
 *  This is a helper function for the gmap and hmap versions below.
 *  The idarray is destroyed in the course of running this function.
 *
 * @param pdb 		opaque database pointer, created with pdb_create()
 * @param ida 		array to look in, destroyed by this call.
 * @param low	 	lower value bound or PDB_ITERATOR_LOW_ANY
 * @param high	 	higher value bound or PDB_ITERATOR_HIGH_ANY
 * @param n_out	 	out: the number of values in the idarray
 *			between low and high.
 *
 * @return 0 on success, a nonzero error code on error
 */
static int pdb_count_idarray(pdb_handle* pdb, addb_idarray* ida, pdb_id low,
                             pdb_id high, unsigned long long* n_out) {
  int err;
  unsigned long long offset;
  addb_id id;

  pdb->pdb_runtime_statistics.rts_index_extents_read++;
  *n_out = addb_idarray_n(ida);
  offset = 0;

  /*  Find <low> in the idarray, and remember its position
   *  as <offset>.
   */
  if (low != PDB_ITERATOR_LOW_ANY) {
    pdb->pdb_runtime_statistics.rts_index_extents_read++;
    pdb->pdb_runtime_statistics.rts_index_elements_read++;

    cl_assert(ida->ida_cl, low < (1ull << 34));
    err = addb_idarray_search(ida, 0, *n_out, low, &offset, &id);
    if (err != 0) goto err;

    if (high <= id) {
      /*  No elements -- the first ID that we
       *  just found is >= our high.
       */
      *n_out = 0;
      addb_idarray_finish(ida);

      return 0;
    }
    *n_out -= offset;
  }

  /*  Find <high> in the idarray, and adjust <*n_out> to
   *  not include it in the count.
   */
  if (*n_out > 0 && high != PDB_ITERATOR_HIGH_ANY) {
    pdb->pdb_runtime_statistics.rts_index_extents_read++;
    pdb->pdb_runtime_statistics.rts_index_elements_read++;

    cl_assert(ida->ida_cl, high < (1ull << 34));
    err =
        addb_idarray_search(ida,
                            /* first index included */ offset,
                            /* first index not included */ addb_idarray_n(ida),
                            /* look for this id */ high, &offset, &id);
    if (err != 0) goto err;

    *n_out -= addb_idarray_n(ida) - offset;
  }
  addb_idarray_finish(ida);
  return 0;

err:
  cl_assert(pdb->pdb_cl, err != 0);
  addb_idarray_finish(ida);

  if (err == ADDB_ERR_NO) {
    *n_out = 0;
    return 0;
  }
  return err;
}

int pdb_count_gmap_est(pdb_handle* pdb, addb_gmap* gm, pdb_id source,
                       pdb_id low, pdb_id high, unsigned long long upper_bound,
                       unsigned long long* n_out) {
  addb_idarray ida;
  int err;

  if (low == PDB_ITERATOR_LOW_ANY && high == PDB_ITERATOR_HIGH_ANY) {
    pdb->pdb_runtime_statistics.rts_index_extents_read++;
    err = addb_gmap_array_n_bounded(gm, source, upper_bound, n_out);

    if (err == ADDB_ERR_NO) {
      *n_out = 0;
      return 0;
    }
    return 0;
  }

  err = addb_gmap_idarray(gm, source, &ida);
  if (err == 0) {
    err = pdb_count_idarray(pdb, &ida, low, high, n_out);
  } else if (err == ADDB_ERR_NO) {
    *n_out = 0;
    return 0;
  } else if (err == ADDB_ERR_BITMAP) {
    /*
     * We know this thing is a bitmap. Use random sampling
     * to estimate the number of bits set.
     */
    if (high == PDB_ITERATOR_HIGH_ANY)
      high = addb_istore_next_id(pdb->pdb_primitive);

    return addb_bgmap_estimate(gm, source, low, high, n_out);
  }

  return err;
}
/**
 * @brief How many links emerge from this node (ID version)?
 *
 * @param pdb 		opaque database pointer, created with pdb_create()
 * @param gm 		opaque gmap poitner
 * @param source 	source of the GMAP iteration
 * @param low	 	lower value bound or PDB_ITERATOR_LOW_ANY
 * @param high	 	higher value bound or PDB_ITERATOR_HIGH_ANY
 * @param upper_bound 	higher number of results bound, or PDB_COUNT_UNBOUNDED
 * @param nout	 	out: number of entries in this GMAP.
 *
 * @return 0 on success, a nonzero error code on error
 */
int pdb_count_gmap(pdb_handle* pdb, addb_gmap* gm, pdb_id source, pdb_id low,
                   pdb_id high, unsigned long long upper_bound,
                   unsigned long long* n_out) {
  int err;

  if (low == PDB_ITERATOR_LOW_ANY && high == PDB_ITERATOR_HIGH_ANY) {
    pdb->pdb_runtime_statistics.rts_index_extents_read++;
    err = addb_gmap_array_n_bounded(gm, source, upper_bound, n_out);
  } else {
    addb_idarray ida;

    err = addb_gmap_idarray(gm, source, &ida);
    if (err == 0)
      err = pdb_count_idarray(pdb, &ida, low, high, n_out);
    else if (err == ADDB_ERR_BITMAP) {
      /* XXX
       * This wants to be...
       * err = addb_gmap_bgmap_count(gm, source, &n_out);
       *
       * as soon as addb_gmap_bgmap_count works.
       */
      cl_notreached(pdb->pdb_cl, "Tried to count a bgmap as a gmap");
    }
  }
  if (err == ADDB_ERR_NO) {
    *n_out = 0;
    err = 0;
  }

  return err;
}

int pdb_count_hmap(pdb_handle* pdb, addb_hmap* hm, addb_hmap_id hash_of_key,
                   char const* key, size_t key_len, addb_hmap_type type,
                   pdb_id low, pdb_id high, unsigned long long upper_bound,
                   unsigned long long* n_out) {
  int err;

  if (PDB_ITERATOR_LOW_ANY == low && PDB_ITERATOR_HIGH_ANY == high) {
    pdb->pdb_runtime_statistics.rts_index_extents_read++;
    err = addb_hmap_array_n_bounded(hm, hash_of_key, key, key_len, type,
                                    upper_bound, n_out);
  } else {
    addb_idarray ida;

    err = addb_hmap_idarray(hm, hash_of_key, key, key_len, type, &ida);
    if (err == 0) err = pdb_count_idarray(pdb, &ida, low, high, n_out);
  }

  if (err == ADDB_ERR_NO) {
    *n_out = 0;
    err = 0;
  }
  return err;
}
