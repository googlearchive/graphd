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


/*  GMAP iterator
 *
 *  The array may be added to while it's being iterated over.
 *
 *  The iterator is robust against the underlying array moving around,
 *  as long as the array is only being added to.
 */

/**
 * @brief Remember the current iterator position.
 *
 *  Fill pos_out with a position that can be passed to
 *  addb_gmap_iterator_set_position to restore to an iterator
 *  over the same source the state it has now.
 *
 * @param gm 		opaque GMAP database handle
 * @param source	the source of the iteration
 * @param iter		an iterator over this source.
 * @param pos_out	assign the position to this.
 */
void addb_gmap_iterator_get_position(addb_gmap* gm, addb_gmap_id source,
                                     addb_gmap_iterator* iter,
                                     addb_gmap_iterator_position* pos_out) {
  cl_assert(gm->gm_addb->addb_cl, iter != NULL);
  cl_assert(gm->gm_addb->addb_cl, pos_out != NULL);

  if (!addb_gmap_accessor_is_set(&iter->iter_ac) || iter->iter_i == 0) {
    *pos_out = ADDB_GMAP_POSITION_START;
    cl_cover(gm->gm_addb->addb_cl);
  }
  if (iter->iter_i >= iter->iter_n) {
    *pos_out = ADDB_GMAP_POSITION_END;
    cl_cover(gm->gm_addb->addb_cl);
  } else {
    *pos_out = iter->iter_i;
    cl_cover(gm->gm_addb->addb_cl);
  }
}

/**
 * @brief Restore a previously remembered cursor position.
 *
 *  The source of the iteration must be the same for set- and
 *  get-position, but the data may have been added to.
 *
 *  The data may have been desequentialized from outside
 *  sources and must not be trusted.
 *
 * @param gm 		opaque GMAP database handle
 * @param source	the source of the iteration
 * @param iter		an iterator over this source.
 * @param pos		position previously written to by
 *			addb_gmap_iterator_get_position().
 * @return 0 on success, a nonzero error number on error.
 */
int addb_gmap_iterator_set_position(addb_gmap* gm, addb_gmap_id source,
                                    addb_gmap_iterator* iter,
                                    addb_gmap_iterator_position const* pos) {
  cl_handle* cl;
  int err;

  cl = gm->gm_addb->addb_cl;
  cl_assert(cl, pos != NULL);
  if (!addb_gmap_accessor_is_set(&iter->iter_ac)) {
    iter->iter_i = 0;
    iter->iter_n = 0;

    err = addb_gmap_accessor_set(gm, source, &iter->iter_ac);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_VERBOSE, "addb_gmap_accessor_set", err,
                   "source=%llu, pos %llu", (unsigned long long)source,
                   (unsigned long long)*pos);
      return err;
    }
    iter->iter_n = addb_gmap_accessor_n(&iter->iter_ac);
  }
  if (*pos == ADDB_GMAP_POSITION_START)
    iter->iter_i = 0;
  else if (*pos == ADDB_GMAP_POSITION_END)
    iter->iter_i = iter->iter_n;
  else if (*pos > iter->iter_n) {
    cl_leave(cl, CL_LEVEL_FAIL,
             "(%s): "
             "encoded offset %llu is > array size %llu",
             addb_gmap_accessor_display_name(&iter->iter_ac),
             (unsigned long long)*pos, (unsigned long long)iter->iter_n);
    iter->iter_i = iter->iter_n;
    cl_cover(cl);
    return EINVAL;
  } else
    iter->iter_i = *pos;

  cl_cover(gm->gm_addb->addb_cl);
  cl_log(cl, CL_LEVEL_VERBOSE,
         "addb_gmap_iterator_set_position: "
         "source=%llu, pos=%llu; index=%llu (of %llu)",
         (unsigned long long)source, (unsigned long long)*pos,
         (unsigned long long)iter->iter_i, (unsigned long long)iter->iter_n);

  return 0;
}

/**
 * @brief Undo the effects of the most recent "next" call
 * 	to an iterator.
 *
 * @param gm		opaque gmap pointer
 * @param source	array we're iterating over
 * @param iter		iterator
 * @param id		id to "put back" (ignored)
 */
void addb_gmap_iterator_unget(addb_gmap* gm, addb_gmap_id source,
                              addb_gmap_iterator* iter, addb_gmap_id id) {
  cl_assert(gm->gm_addb->addb_cl, iter != NULL);

  if (!addb_gmap_accessor_is_set(&iter->iter_ac)) return;

  if (iter->iter_i > 0) iter->iter_i--;
}

/**
 * @brief Get the next element from an iterator.
 *
 * @param gm		opaque gmap pointer
 * @param source	array we're iterating over
 * @param iter		iterator
 * @param out		out: the current id
 * @param file		__FILE__ of calling code, usually inserted by
 *				addb_gmap_iterator_next macro.
 * @param line		__LINE__ of calling code
 *
 * @return 0 on success, otherwise a nonzero error code.
 * @return ADDB_ERR_NO if we've hit the end of the id list.
 */
int addb_gmap_iterator_next_loc(addb_gmap* gm, addb_gmap_id source,
                                addb_gmap_iterator* iter, addb_gmap_id* out,
                                char const* file, int line) {
  unsigned long long val;
  int err;
  addb_gmap_id i;

  cl_assert(gm->gm_addb->addb_cl, out != NULL);
  cl_assert(gm->gm_addb->addb_cl, iter != NULL);

  if (!addb_gmap_accessor_is_set(&iter->iter_ac)) {
    unsigned long long n;

    iter->iter_i = 0;

    err = addb_gmap_accessor_set(gm, source, &(iter->iter_ac));
    if (err) return err;
    n = addb_gmap_accessor_n(&iter->iter_ac);
    if ((iter->iter_n = n) == 0) {
      cl_cover(gm->gm_addb->addb_cl);
      cl_log(gm->gm_addb->addb_cl, CL_LEVEL_SPEW,
             "addb_gmap_iterator_next: no elements "
             "for %s:%llu [from %s:%d]",
             gm->gm_path, (unsigned long long)source, file, line);
      return ADDB_ERR_NO;
    }
  } else {
    if (iter->iter_i >= iter->iter_n) {
      cl_log(gm->gm_addb->addb_cl, CL_LEVEL_SPEW,
             "addb_gmap_iterator_next: %s:%llu: END "
             "[from %s:%d]",
             addb_gmap_accessor_display_name(&iter->iter_ac),
             (unsigned long long)source, file, line);

      cl_cover(gm->gm_addb->addb_cl);
      return ADDB_ERR_NO;
    }
  }

  cl_cover(gm->gm_addb->addb_cl);

  i = iter->iter_i++;
  if (!iter->iter_forward) i = iter->iter_n - (i + 1);

  err = addb_gmap_accessor_get(&iter->iter_ac, i * ADDB_GMAP_ENTRY_SIZE, &val);

  if (err == 0) {
    *out = ADDB_GMAP_LOW_34(val);

    cl_log(gm->gm_addb->addb_cl, CL_LEVEL_SPEW,
           "addb_gmap_iterator_next: "
           "%s%s:%llu[%llu] = %llu [from %s:%d]",
           iter->iter_forward ? "" : "~",
           addb_gmap_accessor_display_name(&iter->iter_ac),
           (unsigned long long)source, (unsigned long long)iter->iter_i - 1,
           (unsigned long long)*out,

           file, line);
  } else {
    iter->iter_i--;

    cl_log(gm->gm_addb->addb_cl, CL_LEVEL_SPEW,
           "addb_gmap_iterator_next: %s:%llu: %s [from %s:%d]",
           addb_gmap_accessor_display_name(&iter->iter_ac),
           (unsigned long long)source, addb_xstrerror(err), file, line);
  }

  return err;
}

/**
 * @brief Reposition an iterator on or after a location.
 *
 *  Given the value of an element, find that element
 *  (or the next larger) element in an array, return it,
 *  and position as if it had just been returned  with a
 *  call to addb_gmap_iterator_next (including the potential
 *  for a call to unget).
 *
 *  The next call to "next" on this iterator will return
 *  the element following the one returned in this call.
 *  To return the one the iterator is positioned on right now,
 *  call iterator_unget on it immediately after on-or-after.
 *
 * @param gm		opaque gmap pointer
 * @param source	which array are we're iterating over
 * @param iter		iterator
 * @param id_in_out	in: the element value we want to
 *			position on.  (Note: this is a value,
 *			not an index.)
 *			out: the element value we're actually
 *			positioned on -- the smallest in the
 *			array that's &gt;= the passed-in value.
 * @param changed_out	out: did we change the id on its way through?
 * @param changed_out	out: offset,
 * @param file		__FILE__ of calling code, usually inserted by
 *				addb_gmap_iterator_next macro.
 * @param line		__LINE__ of calling code
 */
int addb_gmap_iterator_find_loc(addb_gmap* gm, addb_gmap_id source,
                                addb_gmap_iterator* iter,
                                addb_gmap_id* id_in_out, bool* changed_out,
                                char const* file, int line) {
  unsigned long long nelem, base, middle, found, i;
  unsigned long long val;
  int err;

  cl_assert(gm->gm_addb->addb_cl, id_in_out != NULL);
  cl_assert(gm->gm_addb->addb_cl, iter != NULL);

  if (!addb_gmap_accessor_is_set(&iter->iter_ac)) {
    unsigned long long n;

    iter->iter_i = 0;

    err = addb_gmap_accessor_set(gm, source, &(iter->iter_ac));
    if (err) return err;
    n = addb_gmap_accessor_n(&iter->iter_ac);

    if ((iter->iter_n = n) == 0) {
      cl_cover(gm->gm_addb->addb_cl);
      cl_log(gm->gm_addb->addb_cl, CL_LEVEL_SPEW,
             "addb_gmap_iterator_find: no elements "
             "for %s:%llu [from %s:%d]",
             gm->gm_path, (unsigned long long)source, file, line);
      return ADDB_ERR_NO;
    }
  }

  cl_cover(gm->gm_addb->addb_cl);

  base = iter->iter_i = 0;
  nelem = iter->iter_n;

  /*  Binary search for <*id_in_out>
   */
  if (nelem == 0) {
    cl_log(gm->gm_addb->addb_cl, CL_LEVEL_SPEW,
           "addb_gmap_iterator_find: no elements "
           "for %s:%llu [from %s:%d]",
           gm->gm_path, (unsigned long long)source, file, line);
    return ADDB_ERR_NO;
  }

  if (iter->iter_forward) {
    do {
      middle = nelem / 2;
      found = base + middle;

      err = addb_gmap_accessor_get(&iter->iter_ac, found * ADDB_GMAP_ENTRY_SIZE,
                                   &val);
      if (err != 0) {
        cl_log_errno(
            gm->gm_addb->addb_cl, CL_LEVEL_FAIL, "addb_gmap_accessor_get", err,
            "for %s:%llu[%llu] [from %s:%d]", gm->gm_path,
            (unsigned long long)source, (unsigned long long)found, file, line);

        iter->iter_i = iter->iter_n;
        return err;
      }
      val = ADDB_GMAP_LOW_34(val);
      if (val == *id_in_out) {
        iter->iter_i = found + 1;
        *changed_out = false;

        return 0;
      } else if (val > *id_in_out)
        nelem = middle;
      else {
        base = found + 1;
        nelem -= middle + 1;
      }

    } while (nelem > 0);

    for (; found < iter->iter_n; found++) {
      err = addb_gmap_accessor_get(&iter->iter_ac, found * ADDB_GMAP_ENTRY_SIZE,
                                   &val);
      if (err != 0) {
        iter->iter_i = iter->iter_n;
        return err;
      }
      val = ADDB_GMAP_LOW_34(val);
      if (val >= *id_in_out) {
        iter->iter_i = iter->iter_n;
        *changed_out = (val != *id_in_out);
        *id_in_out = val;
        iter->iter_i = found + 1;

        return 0;
      }
    }
  } else {
    do {
      middle = nelem / 2;
      found = base + middle;

      i = iter->iter_n - (found + 1);
      err = addb_gmap_accessor_get(&iter->iter_ac, i * ADDB_GMAP_ENTRY_SIZE,
                                   &val);

      if (err != 0) {
        cl_log_errno(
            gm->gm_addb->addb_cl, CL_LEVEL_SPEW, "addb_gmap_accessor_get", err,
            "for %s:%llu[%llu] [from %s:%d]", gm->gm_path,
            (unsigned long long)source, (unsigned long long)i, file, line);

        iter->iter_i = iter->iter_n;
        return err;
      }
      val = ADDB_GMAP_LOW_34(val);
      if (val == *id_in_out) {
        iter->iter_i = found + 1;
        *changed_out = false;

        return 0;
      } else if (val > *id_in_out) {
        /* Our indexes run backwards.
         *  	3 2 1 0
         *
         *  The values run forwards:
         *      2 7 9 15
         *
         *  We want smaller values.  Smaller
         *  values are the ones with larger
         *  indexes.
         */
        base = found + 1;
        nelem -= middle + 1;
      } else {
        nelem = middle;
      }

    } while (nelem > 0);

    for (; found < iter->iter_n; found++) {
      i = iter->iter_n - (found + 1);
      err = addb_gmap_accessor_get(&iter->iter_ac, i * ADDB_GMAP_ENTRY_SIZE,
                                   &val);
      if (err != 0) {
        iter->iter_i = iter->iter_n;
        return err;
      }

      val = ADDB_GMAP_LOW_34(val);
      if (val <= *id_in_out) {
        *changed_out = (val != *id_in_out);
        *id_in_out = val;

        iter->iter_i = found + 1;
        return 0;
      }
    }
  }
  return ADDB_ERR_NO;
}

/**
 * @brief How many elements are left in an iterator,
 *  	given its current position?
 *
 * 	That is, how many times can I call addb_gmap_iterator_next()
 *	on this thing and not have it return ADDB_ERR_NO ?
 *
 * @param gm		opaque gmap pointer
 * @param source	array we're iterating over
 * @param iter		iterator
 */
int addb_gmap_iterator_n(addb_gmap* gm, addb_gmap_id source,
                         addb_gmap_iterator* iter, unsigned long long* n_out) {
  int err;

  cl_assert(gm->gm_addb->addb_cl, iter != NULL);

  if (!addb_gmap_accessor_is_set(&iter->iter_ac)) {
    iter->iter_i = 0;

    err = addb_gmap_accessor_set(gm, source, &iter->iter_ac);
    if (err != 0) return err;

    iter->iter_n = addb_gmap_accessor_n(&iter->iter_ac);
  }
  *n_out = iter->iter_n - iter->iter_i;
  return 0;
}

/**
 * @brief Free resources associated with this GMAP iterator.
 *
 *  (In reality, there are no resources associated with a
 *  GMAP iterator - but this might one day change.)
 *
 * @param iter	iterator to free
 */
void addb_gmap_iterator_finish(addb_gmap_iterator* iter) {
  addb_gmap_iterator_initialize(iter);
}

/**
 * @brief Initialize an iterator.
 *
 *  After a call to addb_gmap_iterator_initialize, it is safe
 *  (but not necessary) to call addb_gmap_iterator_finish(),
 *  even without an intervening call to other iterator functions.
 *
 * @param iter iterator to initialize
 */
void addb_gmap_iterator_initialize(addb_gmap_iterator* iter) {
  addb_gmap_accessor_clear(&iter->iter_ac);
  iter->iter_n = 0;
  iter->iter_i = 0;
  iter->iter_forward = true;
}

/**
 * @brief Go to the i'th element of an iteration.
 *
 *  The next call to addb_gmap_iterator_next will return
 *  the i'th element, or an error if that element doesn't exist.
 *
 * @param gm 		opaque GMAP database handle
 * @param source	the source of the iteration
 * @param iter		in/out: an iterator over this source.
 * @param i	 	offset we want to go to.
 *
 * @return 0 on success, a nonzero error number on error.
 * @return ADDB_ERR_NO if the source doesn't exist, or if the
 *	iterator can already tell that the offset is out of range.
 */
int addb_gmap_iterator_set_offset(addb_gmap* gm, addb_gmap_id source,
                                  addb_gmap_iterator* iter,
                                  unsigned long long i) {
  int err = 0;

  if (!addb_gmap_accessor_is_set(&iter->iter_ac)) {
    unsigned long long n;

    if (i == 0) return 0;

    err = addb_gmap_accessor_set(gm, source, &(iter->iter_ac));

    if (err) return err;
    n = addb_gmap_accessor_n(&iter->iter_ac);

    if ((iter->iter_n = n) == 0) {
      cl_cover(gm->gm_addb->addb_cl);
      cl_log(gm->gm_addb->addb_cl, CL_LEVEL_SPEW,
             "addb_gmap_iterator_set_offset: no elements "
             "for %s:%llu",
             gm->gm_path, (unsigned long long)source);
      return ADDB_ERR_NO;
    }
  } else {
    if (i == 0) {
      cl_cover(gm->gm_addb->addb_cl);

      addb_gmap_accessor_clear(&(iter->iter_ac));
      iter->iter_i = 0;
      iter->iter_n = 0;

      return 0;
    }
    cl_cover(gm->gm_addb->addb_cl);
  }

  if (i >= iter->iter_n) {
    i = iter->iter_n;
    err = ADDB_ERR_NO;
  }
  iter->iter_i = i;
  return err;
}

/**
 * @brief Return a string representation of an iterator.
 *
 * @param gm 		opaque GMAP database handle
 * @param source	the source of the iteration
 * @param iter		in/out: an iterator over this source.
 * @param buf	 	buffer to optionally use
 * @param size		number of bytes pointed to by buf.
 *
 * @return a pointer to a string representation of the iterator.
 */
char const* addb_gmap_iterator_to_string(addb_gmap* gm, addb_gmap_id source,
                                         addb_gmap_iterator* iter, char* buf,
                                         size_t size) {
  if (iter == NULL) return "null";

  if (!addb_gmap_accessor_is_set(&iter->iter_ac))
    snprintf(buf, size, "%s%s.%llu[unopened]", iter->iter_forward ? "" : "~",
             gm->gm_path, (unsigned long long)source);
  else
    snprintf(buf, size, "%s%s.%llu[%llu of %llu]",
             iter->iter_forward ? "" : "~",
             addb_gmap_accessor_display_name(&iter->iter_ac),
             (unsigned long long)source, (unsigned long long)iter->iter_i,
             (unsigned long long)iter->iter_n);
  return buf;
}

/**
 * @brief Set the iterator's direction.
 *
 *   This should be called before any accesses to the iterator.
 *   Effects of changing direction in mid-flow are not defined.
 *
 * @param gm 		opaque GMAP database handle
 * @param iter		an iterator
 * @param forward 	true: go from low to high; false: high to low.
 */
void addb_gmap_iterator_set_forward(addb_gmap* gm, addb_gmap_iterator* iter,
                                    bool forward) {
  cl_assert(gm->gm_addb->addb_cl, iter != NULL);
  iter->iter_forward = forward;
}
