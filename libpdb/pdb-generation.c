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

#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>

/*
 * There is a gmap for generations.  It is indexed with the
 * local 34-bit pdb_id of the first object.
 *
 * - If you don't find it (the most common case), the item exists
 *   only once, and you're holding that instance.
 *
 * - Otherwise, the result is the list of PDB IDs for objects of
 *   that generation, beginning to end.
 */

/**
 * @brief How many generations were there back then?
 *
 *  There are *n_total ids in total in *guid_key's value array.
 *  They're in ascending order.
 *  We're only interested in those whose value is less than end.
 *  Adjust the value of *n_total downwards to match the state
 *  when we only had end primitives (the largest one having a value
 *  of end - 1) in the system overall.
 *
 * @param pdb		Database to do this for
 * @param lineage_id 	original id of the lineage
 * @param end		cut-off point from from the "as-of" matrix
 * @param n_total	in/out: the number of elements in the array to look at.
 *
 * @return 0 unless a system error happened; even if the resulting
 *	array has 0 elements, the call returns 0 (and sets *n_total to 0),
 *  	not PDB_ERR_NO.
 */
static int pdb_generation_reduce(pdb_handle* pdb, pdb_id lineage_id,
                                 unsigned long long end,
                                 unsigned long long* n_total) {
  cl_handle* cl = pdb->pdb_cl;
  pdb_id n = *n_total, base, hs, found, nelem;
  int err;

  /* Easy cases. */

  if (*n_total == 0) return 0;

  if (lineage_id >= end) {
    *n_total = 0;
    return 0;
  }
  if (*n_total == 1) return 0;

  /* Do a binary search in 0...*n_total for the primitive
   * whose timestamp is closest to end.
   */
  nelem = *n_total;
  base = 0;

  for (;;) {
    addb_gmap_id val;

    hs = nelem / 2;
    found = base + hs;

    cl_assert(cl, nelem > 0);
    cl_assert(cl, base < n);
    cl_assert(cl, found < n);

    pdb->pdb_runtime_statistics.rts_index_elements_read++;

    /*  Get the element at position "found".
     */
    err = addb_hmap_sparse_array_nth(pdb->pdb_hmap, lineage_id, addb_hmt_gen,
                                     found, &val);
    if (err != 0) return err;

    if (val == end)
      break;

    else if (val > end) {
      /*  We're too far into the future.
       *  Reduce the table size to exclude the item we're on.
       */
      if ((nelem = hs) == 0)
        /*  value(found) > end;
         *  value(found - 1), if it exists, < end.
         */
        break;
    } else {
      /*  We're still too far into the past.
       *  Reduce the table size to start behind
       *  the item we're on.
       */
      base = found + 1;
      if ((nelem -= hs + 1) == 0) {
        /*  value(found) < end;
         *  value(found + 1), if exists, > end.
         */
        found++;
        break;
      }
    }
    cl_assert(cl, nelem > 0);
    cl_assert(cl, found < n);
    cl_assert(cl, base < n);
  }

  *n_total = found;
  return 0;
}

/**
 * @brief Check whether an index occurs in a set of specified generations.
 *
 *  We know that the record with PDB ID id exists.  We don't know
 *  which generation it is.  Check whether it is within the specified
 *  generational range.
 *
 * @param pdb		database in which this is happening
 * @param asof		NULL or the dateline we pretend we're at, for the
 *			purposes of this operation.
 * @param guid_key 	GUID or application key of the generational
 *			chain we're interested in.
 * @param id		check that this exists in the specified range
 * @param new_valid	If true, the next two parameters matter.
 * @param new_min	Minimum allowed generation counting from the
 *			newest one backwards, 0-based.  (The previous-to-last
 *			has index 1.)
 * @param new_max	Maximum allowed generation counting from the
 *			newest one backwards, 0-based.  (The previous-to-last
 *			has index 1.)
 * @param old_valid	If true, the next two parameters matter.
 * @param old_min	Minimum allowed generation counting from the
 *			oldest one forwards, 0-based. (The second has index 1.)
 * @param old_max	Maximum allowed generation counting from the
 *			oldest one forwards, 0-based.  (The second has index 1.)
 *
 * @return 0 if the id's generation is in guid_key's range;
 * @return PDB_ERR_NO if the id's generation is not in the indicated range.
 * @return other nonzero error codes on system error.
 */

int pdb_generation_check_range(pdb_handle* pdb, graph_dateline const* asof,
                               graph_guid const* guid, pdb_id id, int new_valid,
                               unsigned long long new_min,
                               unsigned long long new_max, int old_valid,
                               unsigned long long old_min,
                               unsigned long long old_max) {
  unsigned long long my_min, my_max;
  unsigned long long my_gen, n_total;
  pdb_id lineage_id;
  addb_hmap_iterator iter_key;
  int err;
  bool res = false;
  bool is_old;

  bool default_gencon =
      new_valid && (0 == new_min) && (0 == new_max) && (!old_valid);

  pdb_primitive pr;

  cl_assert(pdb->pdb_cl, guid != NULL);

  if ((err = pdb_is_versioned(pdb, id, &is_old))) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_is_versioned", err,
                 "Can't check versioned bitmap for %llx",
                 (unsigned long long)id);
    return err;
  }

  if (default_gencon) {
    if (!is_old)
      return 0;
    else if (!asof)
      return PDB_ERR_NO;
  }

  addb_hmap_iterator_initialize(&iter_key);
  err = pdb_primitive_read(pdb, guid, &pr);

  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_primitive_read", err,
                 "Can't read primitive for %llx", (unsigned long long)id);
  }

  /*
   * If the primitive
   * hasn't been versioned and the primitive hasn't versioned
   * any other primitives, we can fast-path our way out of
   * looking it up in the generation hmap
   */

  if (!is_old && !(pdb_primitive_has_generation(&pr))) {
    n_total = 1;
    pdb_primitive_finish(pdb, &pr);
    err = pdb_generation_guid_to_lineage(pdb, guid, &lineage_id, NULL);
    if (err) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR,
                   "pdb_generation_guid_to_lineage", err,
                   "Can't get lineage for id %llx", (unsigned long long)id);
      return err;
    }
  } else {
    pdb_primitive_finish(pdb, &pr);

    /*
     * A side effect sets lineage_id even if err is PDB_ERR_NO
     */
    err = pdb_generation_guid_to_iterator(pdb, guid, &lineage_id, NULL,
                                          &iter_key);

    if (err == PDB_ERR_NO) {
      cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
             "version bitmap claims that %llx was versioned "
             "but there is no hash generation for it",
             (unsigned long long)id);
      n_total = 1;
    } else if (err != 0) {
      return err;
    } else {
      pdb->pdb_runtime_statistics.rts_index_extents_read++;

      err = pdb_generation_lineage_n(pdb, lineage_id, &n_total);
      if (err != 0) {
        addb_hmap_iterator_finish(&iter_key);
        return err;
      }

      cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
             "pdb_generation_check_range: have generations; "
             "n_total=%lu",
             (unsigned long)n_total);
    }
  }

  if (asof != NULL) {
    unsigned long long end;

    err = graph_dateline_get(asof, pdb->pdb_database_id, &end);
    if (err == 0) {
      /*  Bsearch for end or higher in the dateline space.
       */
      err = pdb_generation_reduce(pdb, lineage_id, end, &n_total);
      if (err != 0) {
        addb_hmap_iterator_finish(&iter_key);
        return err;
      }
    } else if (err != GRAPH_ERR_NO) {
      addb_hmap_iterator_finish(&iter_key);
      return err;
    }

    /*  Otherwise, err is GRAPH_ERR_NO, and there are no limits
     *  placed on the (unmentioned) server.
     */
    err = 0;
  }

  /*  Keep the error around for later - but first, translate the
   *  start-relative and end-relative constraints into absolutes.
   */
  my_min = 0;
  my_max = n_total - 1;

  if (old_valid) {
    if (old_min > my_min) my_min = old_min;
    if (old_max < my_max) my_max = old_max;
  }

  if (new_valid) {
    if (new_min + 1 > n_total) {
      cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
             "pdb_generation_in_range: no; minimum %llu "
             ">= n_total %llu",
             new_min, n_total);
      goto done;
    }

    if (n_total - (new_min + 1) < my_max) my_max = n_total - (new_min + 1);

    if (new_max < n_total)
      if (n_total - (new_max + 1) > my_min) my_min = n_total - (new_max + 1);

    /*
                    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
                            "new_min %llu, new_max %llu, "
                            "n_total %llu -> my_min %llu, my_max %llu",
                            (unsigned long long)new_min,
                            (unsigned long long)new_max,
                            (unsigned long long)n_total,
                            (unsigned long long)my_min,
                            (unsigned long long)my_max);
    */
  }

  if (my_min > my_max) {
    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
           "pdb_generation_in_range: no; absolute constraints "
           "%llu..%llu; id %llx, lineage_id %llx",
           my_min, my_max, (unsigned long long)id,
           (unsigned long long)lineage_id);
    goto done;
  }

  if (err != 0 || id == lineage_id) {
    if (my_min > 0)
      cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
             "pdb_generation_in_range: %s; err %d, id %llx, "
             "lineage_id %llx, my_min %llu",
             my_min <= 0 ? "yes" : "no", err, (unsigned long long)id,
             (unsigned long long)lineage_id, (unsigned long long)my_min);
    res = my_min <= 0;
    goto done;
  }

  /*  We have a choice of either finding the ID in the stretch
   *  of IDs in my_min..my_max or testing for its absence in
   *  the outer parts.  Which one is more efficient?
   */
  if ((my_max + 1) - my_min < n_total / 2) {
    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
           "pdb_generation_in_range: testing for presence");

    if (my_min == 0)
      my_min = 1;
    else if (my_min > 1) {
      /* Test for presence. */
      err = addb_hmap_sparse_iterator_set_offset(
          pdb->pdb_hmap, lineage_id, addb_hmt_gen, &iter_key, my_min);
      if (err != 0) {
        cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
               "pdb_generation_in_range: "
               "failed to set offset of "
               "addb/gmap iterator to %llu: %s",
               (unsigned long long)my_min, strerror(err));
        addb_hmap_iterator_finish(&iter_key);
        return err;
      }
    }

    /* If we don't find it, the result will be negative. */
    for (my_gen = my_min; my_gen <= my_max; my_gen++) {
      addb_gmap_id gen_id;

      pdb->pdb_runtime_statistics.rts_index_elements_read++;
      err = addb_hmap_sparse_iterator_next(pdb->pdb_hmap, lineage_id,
                                           addb_hmt_gen, &iter_key, &gen_id);
      if (err != 0) {
        cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
               "%s:%d: unexpected failure of "
               "addb_hmap_sparse_iterator_next: %s",
               __FILE__, __LINE__, strerror(err));
        addb_hmap_iterator_finish(&iter_key);
        return err;
      }
      if (id == gen_id) {
        res = true;
        break;
      }
    }
  } else {
    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
           "pdb_generation_in_range: testing for absence");

    /*  Test for absence.  If the result isn't in the
     *  excluded section, the overall result will be positive.
     */
    res = true;

    for (my_gen = 1; my_gen < my_min; my_gen++) {
      addb_gmap_id gen_id;

      pdb->pdb_runtime_statistics.rts_index_elements_read++;
      err = addb_hmap_sparse_iterator_next(pdb->pdb_hmap, lineage_id,
                                           addb_hmt_gen, &iter_key, &gen_id);
      if (err != 0) {
        cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR,
                     "addb_hmap_sparse_iterator_next", err, "lineage_id=%llx",
                     (unsigned long long)lineage_id);
        addb_hmap_iterator_finish(&iter_key);
        return err;
      }
      if (id == gen_id) {
        res = false;
        goto done;
      }
    }

    if (my_max + 1 != my_gen) {
      /* Skip the middle part */
      err = addb_hmap_sparse_iterator_set_offset(
          pdb->pdb_hmap, lineage_id, addb_hmt_gen, &iter_key, my_max + 1);
      if (err != 0) {
        if (err != ADDB_ERR_NO) {
          cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR,
                       "addb_hmap_sparse_iterator_set_offset", err,
                       "lineage_id=%llx", (unsigned long long)lineage_id);
          addb_hmap_iterator_finish(&iter_key);
          return err;
        }

        goto done;
      }
      my_gen = my_max + 1;
    }

    for (;; my_gen++) {
      addb_gmap_id gen_id;

      pdb->pdb_runtime_statistics.rts_index_elements_read++;
      err = addb_hmap_sparse_iterator_next(pdb->pdb_hmap, lineage_id,
                                           addb_hmt_gen, &iter_key, &gen_id);
      if (err != 0) {
        if (err == ADDB_ERR_NO) break;

        cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
               "%s:%d: unexpected "
               "failure of addb_gmap"
               "_iterator_next: %s",
               __FILE__, __LINE__, strerror(err));
        addb_hmap_iterator_finish(&iter_key);
        return err;
      }
      if (id == gen_id) {
        res = false;
        break;
      }
    }
  }
done:
  addb_hmap_iterator_finish(&iter_key);
  return res ? 0 : PDB_ERR_NO;
}

/**
 * @brief Look up a GUID's lineage
 *
 *  There's a primitive that, at some generation, has had the GUID
 *  guid_key.  We want to know how many versions of it exist, and what
 *  the last element is.
 *
 * @param pdb		database in which this is happening
 * @param asof		NULL or assumed current dateline
 * @param guid_key 	GUID or application key of the generational
 *			chain we're interested in.
 * @param last_out	NULL or where to store the ID of the last generation.
 * @param n_out		NULL or where to store the number of generations.
 *
 * @return PDB_ERR_NO if the GUID has no lineage (and is the newest element).
 */
int pdb_generation_last_n(pdb_handle* pdb, graph_dateline const* asof,
                          graph_guid const* guid, pdb_id* last_out,
                          pdb_id* n_out) {
  pdb_id lineage_id;
  int err;

  cl_assert(pdb->pdb_cl, guid != NULL);

  if (n_out != NULL) *n_out = 0;
  if (last_out != NULL) *last_out = PDB_ID_NONE;

  err = pdb_generation_guid_to_lineage(pdb, guid, &lineage_id, NULL);
  if (err != 0) return err;

  if (asof != NULL) {
    unsigned long long end;
    unsigned long long n_total;

    err = graph_dateline_get(asof, pdb->pdb_database_id, &end);
    if (err == 0) {
      /*  Bsearch for end or higher in the dateline space.
       */
      err = pdb_generation_reduce(pdb, lineage_id, end, &n_total);
      if (err != 0) return err;

      if (n_out != NULL) *n_out = n_total;
      if (last_out != NULL)
        return addb_hmap_sparse_array_nth(pdb->pdb_hmap, lineage_id,
                                          addb_hmt_gen, n_total, last_out);
      return 0;
    }

    /*  Otherwise, if we didn't get a value from
     *  the "asof" dateline, it doesn't limit access,
     *  and we treat it as if asof had been null.
     */
  }

  if (n_out) {
    unsigned long long ull;

    pdb->pdb_runtime_statistics.rts_index_extents_read++;
    err =
        addb_hmap_sparse_array_n(pdb->pdb_hmap, lineage_id, addb_hmt_gen, &ull);

    if (err == PDB_ERR_NO) {
      ull = 1;
      if (last_out != NULL) return pdb_id_from_guid(pdb, last_out, guid);
    } else if (err != 0)
      return err;

    *n_out = ull;
  }
  if (last_out) {
    pdb->pdb_runtime_statistics.rts_index_extents_read++;
    err = addb_hmap_sparse_last(pdb->pdb_hmap, lineage_id, addb_hmt_gen,
                                last_out);
    if (err == PDB_ERR_NO)
      return pdb_id_from_guid(pdb, last_out, guid);
    else if (err != 0)
      return err;
  }
  return 0;
}

/**
 * @brief Look up a GUID generation
 *
 *  There's a primitive that, at some generation, has had the GUID
 *  <guid>.  We want to know what GUID it had at generation <off>.
 *
 * @param pdb		database in which this is happening
 * @param asof 		NULL or pretend-current dateline
 * @param guid	 	GUID or application key of the generational
 *			chain we're interested in.
 * @param is_newest	If true, we're counting backwards from today
 * @param off		Offset from end (is_newest=true) or
 *			start (is_newest=false), 0-based.
 *			(The second element has index 1.)
 * @param id_out	NULL or where to store the ID of the nth generation.
 * @param guid_out	NULL or where to store the GUID of the nth generation.
 */
int pdb_generation_nth(pdb_handle* pdb, graph_dateline const* asof,
                       graph_guid const* guid, bool is_newest,
                       unsigned long long off, pdb_id* id_out,
                       graph_guid* guid_out) {
  pdb_id lineage_id, id_tmp;
  int err;

  cl_assert(pdb->pdb_cl, guid != NULL);
  cl_log(pdb->pdb_cl, CL_LEVEL_SPEW, "pdb_generation_nth(%s, off %llu)",
         is_newest ? "newest" : "oldest", off);

  /* Special case: "nth from newest" with n == 0 --
   * that's called "last", and we can do it in one.
   */
  if (off == 0 && is_newest) {
    if (id_out == NULL) id_out = &id_tmp;
    err = pdb_generation_last_n(pdb, asof, guid, id_out, NULL);
    if (err != 0) {
      char buf[GRAPH_GUID_SIZE];
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_generation_last_n", err,
                   "failed to get last generation for %s",
                   graph_guid_to_string(guid, buf, sizeof buf));
      return err;
    }
    return (guid_out != NULL) ? pdb_id_to_guid(pdb, *id_out, guid_out) : 0;
  }

  /* Look up the lineage in the primitive.
   */
  err = pdb_generation_guid_to_lineage(pdb, guid, &lineage_id, NULL);
  if (err != 0) return err;

  /* Use the lineage to translate the "newest" notation into
   * the native "oldest" notation.
   */
  if (is_newest) {
    unsigned long long n_total;
    pdb_id n_total_pdb;

    pdb->pdb_runtime_statistics.rts_index_extents_read++;

    err = pdb_generation_last_n(pdb, asof, guid, NULL, &n_total_pdb);

    /* Explicit cast for old compilers */
    n_total = (unsigned long long)n_total_pdb;

    /*  If there is no sparse entry, and we're looking for the
     *  0th generation (from start or end, doesn't matter),
     *  that generation is the one the caller is holding.
     */
    if (err == PDB_ERR_NO && off == 0) {
      if (id_out != NULL) *id_out = lineage_id;
      if (guid_out != NULL) *guid_out = *guid;

      return 0;
    }
    if (err != 0) return err;

    if (off >= n_total) {
      cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
             "pdb_generation_nth: have %llu generations; "
             "caller asks for #%llu -- PDB_ERR_NO",
             n_total, off);
      return PDB_ERR_NO;
    }
    off = n_total - (1 + off);

    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
           "pdb_generation_nth: have generations; "
           "n_total=%llu",
           n_total);
  }
  cl_log(pdb->pdb_cl, CL_LEVEL_SPEW, "pdb_generation_nth: off=%llu", off);

  pdb->pdb_runtime_statistics.rts_index_elements_read++;
  err = addb_hmap_sparse_array_nth(pdb->pdb_hmap, lineage_id, addb_hmt_gen, off,
                                   &id_tmp);
  if (err != 0) return err;

  if (asof != NULL) {
    unsigned long long end;

    err = graph_dateline_get(asof, pdb->pdb_database_id, &end);
    if (!err) {
      /* Final check -- if the id about to be returned is past
       * the asof, return PDB_ERR_NO
       */
      if (id_tmp >= end) return PDB_ERR_NO;
    }
    /* Errors reading datelines fallthrough as if asof == NULL */
  }

  if (id_out != NULL) *id_out = id_tmp;

  if (guid_out != NULL) return pdb_id_to_guid(pdb, id_tmp, guid_out);

  return 0;
}

/**
 * @brief Synchronize - update the internal index to accomodate a record.
 *
 * @param pdb 	opaque pdb module handle
 * @param id	local ID of the passed-in record
 * @param pr 	passed-in record
 *
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_generation_synchronize(pdb_handle* pdb, pdb_id id,
                               pdb_primitive const* pr) {
  int err;
  pdb_id lineage_id;

  if (!pdb_primitive_has_previous(pr)) return 0;

  lineage_id = pdb_primitive_lineage_get(pr);

  /*  This may fail.
  *
  *   If the lineage already exists, this will fail harmlessly,
   *  because the "exclusive" flag is set and the mapping from
   *  the original to itself already exists.
   *  If the lineage doesn't already exist, it'll be created.
   */
  err =
      addb_hmap_sparse_add(pdb->pdb_hmap, lineage_id, addb_hmt_gen, lineage_id);
  if (err == 0)
    pdb->pdb_runtime_statistics.rts_index_elements_written++;
  else if (err != ADDB_ERR_EXISTS) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_hmap_sparse_add", err,
                 "couldn't add %llx -> %llx to lineage table",
                 (unsigned long long)lineage_id,
                 (unsigned long long)lineage_id);
    return err;
  } else
    pdb->pdb_runtime_statistics.rts_index_elements_read++;

  /*  This, in contrast, should never fail.
   */
  pdb->pdb_runtime_statistics.rts_index_elements_written++;
  err = addb_hmap_sparse_add(pdb->pdb_hmap, lineage_id, addb_hmt_gen, id);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_hmap_sparse_add", err,
                 "couldn't add %llx -> %llx to lineage table",
                 (unsigned long long)lineage_id, (unsigned long long)id);
    return err;
  }
  return 0;
}

/**
 * @brief Given a GUID, get its lineage and generation.
 *
 *  This function succeeds whether or not the object actually has
 *  a lineage attached to it.
 *
 * @param pdb 			opaque module handle
 * @param guid 			GUID of the primitive we're asking about
 * @param lineage_id_out 	out: primitive id of the lineage
 * @param gen_iter_out 		out: generation
 *
 * @return 0 success (and *id contains the local ID)
 * @return PDB_ERR_NO if the GUID was neither local nor known
 * @return other nonzero error codes on system error.
 */
int pdb_generation_guid_to_lineage(pdb_handle* pdb, graph_guid const* guid,
                                   pdb_id* lineage_id_out, pdb_id* gen_out) {
  int err;
  pdb_primitive pr;

  if ((err = pdb_primitive_read(pdb, guid, &pr)) != 0) {
    char buf[GRAPH_GUID_SIZE];
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_primitive_read", err,
                 "in pdb_generation_guid_to_lineage(guid=%s)",
                 graph_guid_to_string(guid, buf, sizeof buf));
    return err;
  }

  if (!pdb_primitive_has_previous(&pr)) {
    if (lineage_id_out != NULL) *lineage_id_out = GRAPH_GUID_SERIAL(*guid);
    if (gen_out != NULL) *gen_out = 0;
  } else {
    if (lineage_id_out != NULL)
      *lineage_id_out = pdb_primitive_lineage_get(&pr);
    if (gen_out != NULL) *gen_out = pdb_primitive_generation_get(&pr);
  }

  pdb_primitive_finish(pdb, &pr);

  return 0;
}

/**
 * @brief Given a GUID, get an iterator of its versions and the source ID.
 *
 *  This function succeeds (and returns 0) only if the underlying
 *  application object actually has been versioned.
 *
 * @param pdb 			opaque module handle
 * @param guid 			GUID of one of the primitives in this line
 * @param lineage_id_out 	out: primitive id, passed to iterator
 * @param gen_out 		out: generation (starting at 0.)
 * @param lineage_iter_out 	out: iterator for later versions
 *
 * @return 0 success
 * @return PDB_ERR_NO if the GUID has no lineage attached to it.
 * @return other nonzero error codes on system error.
 */
int pdb_generation_guid_to_iterator(pdb_handle* pdb, pdb_guid const* guid,
                                    pdb_id* lineage_id_out, pdb_id* gen_out,
                                    addb_hmap_iterator* lineage_iter_out) {
  pdb_id lineage_id;
  pdb_id g_dummy = 0;
  pdb_id* g_out = gen_out ? gen_out : &g_dummy;
  int err;

  if (lineage_id_out == NULL) lineage_id_out = &lineage_id;

  /*
   * generation_guid_to_iterator  returns PDB_ERR_NO if I don't have any lineage
   * information.
   * i.e. I am not versioned and I have not versioned anyone else.
   */
  err = pdb_generation_guid_to_lineage(pdb, guid, lineage_id_out, g_out);
  if (err != 0) return err;

  if (*g_out && lineage_iter_out)
    err = addb_hmap_sparse_iterator_next(pdb->pdb_hmap, *lineage_id_out,
                                         addb_hmt_gen, lineage_iter_out,
                                         &lineage_id);

  return err;
}

/**
 * @brief Given a GUID, get an iterator of its versions and the source ID.
 *
 *  This function succeeds (and returns 0) only if the underlying
 *  application object actually has been versioned.
 *
 * @param pdb 			opaque module handle
 * @param guid 			GUID of one of the primitives in this line
 * @param lineage_id_out 	out: primitive id, passed to iterator
 * @param gen_out 		out: generation (starting at 0.)
 * @param ida_out 		out: iterator for versions
 *
 * @return 0 success
 * @return PDB_ERR_NO if the GUID has no lineage attached to it.
 * @return other nonzero error codes on system error.
 */
int pdb_generation_guid_idarray(pdb_handle* pdb, pdb_guid const* guid,
                                pdb_id* lineage_id_out, pdb_id* gen_out,
                                addb_idarray* ida_out) {
  pdb_id lineage_id;
  int err;

  if (lineage_id_out == NULL) lineage_id_out = &lineage_id;

  err = pdb_generation_guid_to_lineage(pdb, guid, lineage_id_out, gen_out);
  if (err != 0) return err;

  if (ida_out != NULL)
    err = addb_hmap_sparse_idarray(pdb->pdb_hmap, *lineage_id_out, addb_hmt_gen,
                                   ida_out);
  return err;
}

/**
 * @brief Given a lineage ID, get the number of entries.
 *
 * @param pdb 			opaque module handle
 * @param lineage_id		ID of the lineage
 * @param lineage_n_out 	out: number of elements in the lineage
 *
 * @return 0 success
 * @return nonzero error codes on system error.
 */
int pdb_generation_lineage_n(pdb_handle* pdb, pdb_id id,
                             unsigned long long* n_out) {
  int err;

  err = addb_hmap_sparse_array_n(pdb->pdb_hmap, id, addb_hmt_gen, n_out);
  if (err == ADDB_ERR_NO) {
    *n_out = 1;
    err = 0;
  }
  return err;
}
