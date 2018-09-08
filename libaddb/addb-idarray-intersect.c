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

#include <errno.h>

/*  Recursive intersect for relatively small sets.
 *
 *  Both sets are given as an idarray, a source, and
 *  start and end boundaries.  The boundaries change
 *  through the course of the recursion.
 *
 * @return ADDB_ERR_MORE	more results than we have
 *				space for.
 */
int addb_idarray_intersect(addb_handle *addb,

                           addb_idarray *a, unsigned long long a_s,
                           unsigned long long a_e,

                           addb_idarray *b, unsigned long long b_s,
                           unsigned long long b_e,

                           addb_id *id_inout, size_t *n_inout, size_t m) {
  int err;
  cl_handle *cl = addb->addb_cl;

  cl_log(cl, CL_LEVEL_VERBOSE,
         "addb_idarray_intersect "
         "%p, %llu..%llu vs. %p, %llu...%llu",
         (void *)a, a_s, a_e - 1, (void *)b, b_s, b_e - 1);

  for (;;) /* Tail recursion at the end of this loop. */
  {
    unsigned long long b_off, a_off;
    addb_id a_id, b_id;

    if (b_e - b_s < a_e - a_s) {
      /* Swap a and b, so a's always the smaller one.. */

      addb_idarray *tmp = a;
      unsigned long long tmp_s = a_s;
      unsigned long long tmp_e = a_e;

      a = b;
      a_s = b_s;
      a_e = b_e;

      b = tmp;
      b_s = tmp_s;
      b_e = tmp_e;
    }

    /*  Are we out of things to intersect?
     */
    if (a_s >= a_e) break;

    /*  The middle value in a's range.
     */
    a_off = a_s + (a_e - a_s) / 2;
    cl_assert(cl, a_off < a_e);
    cl_assert(cl, a_off >= a_s);

    err = addb_idarray_read1(a, a_off, &a_id);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "addb_idarray_read1", err,
                   "ar=%p, off %llu", (void *)a, a_off);
      return err;
    }
    cl_log(cl, CL_LEVEL_VERBOSE, "a[%llu] = %llu", a_off,
           (unsigned long long)a_id);

    /*  Project the middle value into b.
     */
    cl_assert(cl, a_id < (1ull << 34));
    err = addb_idarray_search(b, b_s, b_e, a_id, &b_off, &b_id);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "addb_idarray_search", err,
                   "ar=%p, id=%llu, range %llu..%llu", b,
                   (unsigned long long)a_id, b_s, b_e);
      return err;
    }

    /*  Recursion: (1) The entries before a_off.
     */
    if (a_off > a_s) {
      err = addb_idarray_intersect(addb, a, a_s, a_off, b, b_s, b_off, id_inout,
                                   n_inout, m);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "addb_idarray_intersect", err,
                     "%p %llu..%llu and %p %llu..%llu", (void *)a, a_s, a_e,
                     (void *)b, b_s, b_e);
        return err;
      }
    }

    /*  The middle element
     */
    if (b_off < b_e && b_id == a_id) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "addb_idarray_intersect "
             "found %llu at a=%llu, b=%llu",
             (unsigned long long)a_id, a_off, b_off);

      if (*n_inout >= m) return ADDB_ERR_MORE;

      id_inout[(*n_inout)++] = a_id;
      b_off++;
    } else
      cl_log(cl, CL_LEVEL_VERBOSE,
             "addb_idarray_intersect: "
             "middle for a_id %llu is a=%llu, b=%llu",
             (unsigned long long)a_id, a_off, b_off);

    /* Recursion: (2) The entries after a_off (tail ~)
     */
    b_s = b_off;
    a_s = a_off + 1;
  }
  return 0;
}

/**
 * @brief Intersect between an idarray and a fixed set of ids.
 *
 *  The first set is an idarray and start/end boundaries.
 *  The second set is a fixed set of ids with start and end point.
 *  The boundaries change through the course of the recursion.
 *
 * @param addb 		opaque addb module handle
 *
 * @param a		one idarray
 * @param a_s		start index
 * @param a_e		end index
 *
 * @param b_base	a fixed set of indices
 * @param b_n		number of indices pointed to by b_base.
 *
 * @param id_out	out: elements shared by a and b.
 * @param n_out		out: number of occupied slots in id_out.
 * @param m		maximum number of slots available.
 *
 * @return ADDB_ERR_MORE	ran out of slots
 */
int addb_idarray_fixed_intersect(addb_handle *addb,

                                 addb_idarray *a, unsigned long long a_s,
                                 unsigned long long a_e,

                                 addb_id *b_base, size_t b_n,

                                 addb_id *id_out, size_t *n_out, size_t m) {
  int err;

  cl_log(addb->addb_cl, CL_LEVEL_VERBOSE,
         "addb_idarray_fixed_intersect "
         "%p, %llu..%llu vs. fixed[%llu]",
         (void *)a, a_s, a_e - 1, (unsigned long long)b_n);

  for (;;) /* Tail recursion at the end of this loop. */
  {
    unsigned long long b_off, a_off;
    addb_id a_id, b_id;

    if (b_n < a_e - a_s) {
      /* B is smaller.  Look up b's center in a. */

      /*  Are we out of things to intersect?
       */
      if (b_n <= 0) break;

      /*  The middle value in b's range.
       */
      b_off = b_n / 2;
      cl_assert(addb->addb_cl, b_off < b_n);

      b_id = b_base[b_off];

      cl_log(addb->addb_cl, CL_LEVEL_VERBOSE, "b[%llu] = %llu", b_off,
             (unsigned long long)b_id);

      /*  Project the middle value, b_id, into a.
       */
      err = addb_idarray_search(a, a_s, a_e, b_id, &a_off, &a_id);
      if (err != 0) {
        cl_log_errno(addb->addb_cl, CL_LEVEL_FAIL, "addb_idarray_search", err,
                     "a=%p id=%llu, fixed[%zu]", a, (unsigned long long)b_id,
                     b_n);
        return err;
      }

      /*  Recursion: (1) The entries before b_off.
       */
      if (b_off > 0) {
        err = addb_idarray_fixed_intersect(addb, a, a_s, a_off, b_base, b_off,
                                           id_out, n_out, m);
        if (err != 0) {
          cl_log_errno(addb->addb_cl, CL_LEVEL_FAIL,
                       "addb_idarray_fixed_intersect", err,
                       "fixed[..%llu] and %p %llu..%llu",
                       (unsigned long long)b_off, (void *)a, a_s, a_e);
          return err;
        }
      }

      /*  The middle element
       */
      if (a_id == b_id && a_off < a_e) {
        cl_log(addb->addb_cl, CL_LEVEL_VERBOSE,
               "addb_idarray_fixed_intersect "
               "found %llu at a=%llu, b=%llu",
               (unsigned long long)b_id, a_off, b_off);

        if (*n_out >= m) return ADDB_ERR_MORE;

        id_out[(*n_out)++] = a_id;
        a_off++;
      } else
        cl_log(addb->addb_cl, CL_LEVEL_VERBOSE,
               "addb_idarray_fixed_intersect: "
               "middle for b_id %llu is a=%llu/b=%llu",
               (unsigned long long)b_id, a_off, b_off);

      /* Recursion: (2) The entries after b_off (tail ~)
       */
      a_s = a_off;

      b_n -= b_off + 1;
      b_base += b_off + 1;
    } else {
      /* A is smaller.  Look up a's median in b. */
      if (a_s >= a_e) break;

      /*  The middle value in a's range.
       */
      a_off = a_s + (a_e - a_s) / 2;
      cl_assert(addb->addb_cl, a_off < a_e);
      cl_assert(addb->addb_cl, a_off >= a_s);

      err = addb_idarray_read1(a, a_off, &a_id);
      if (err != 0) {
        cl_log_errno(addb->addb_cl, CL_LEVEL_FAIL, "addb_idarray_read1", err,
                     "ida=%p, off %llu", (void *)a, a_off);
        return err;
      }

      cl_log(addb->addb_cl, CL_LEVEL_VERBOSE, "a[%llu] = %llu", a_off,
             (unsigned long long)a_id);

      /*  Project the middle value into b.
       */
      {
        size_t b_end = b_n;
        size_t b_start = 0;
        unsigned long long endval = a_id;

        for (;;) {
          b_off = b_start + (b_end - b_start) / 2;
          b_id = b_base[b_off];

          if (b_id < a_id)
            b_start = ++b_off;

          else if (b_id > a_id) {
            b_end = b_off;
            endval = b_id;
          } else {
            break;
          }

          if (b_start >= b_end) {
            b_id = endval;
            break;
          }
        }
      }

      /*  if b_off is < b_n,
       *  b_id is the value at b_off, and is
       *  the smallest value >= a_id in b.
       */

      /*  Recursion: (1) The entries before b_off.
       */
      if (b_off > 0) {
        cl_assert(addb->addb_cl, b_base[b_off - 1] < b_id);
        cl_assert(addb->addb_cl, b_off == b_n || b_base[b_off] >= b_id);

        err = addb_idarray_fixed_intersect(addb, a, a_s, a_off, b_base, b_off,
                                           id_out, n_out, m);
        if (err != 0) {
          cl_log_errno(
              addb->addb_cl, CL_LEVEL_FAIL, "addb_idarray_fixed_intersect", err,
              "%p %llu..%llu and fixed[%zd]", (void *)a, a_s, a_e, b_n);
          return err;
        }
      }

      /*  The middle element
       */
      if (b_off < b_n && b_id == a_id) {
        cl_assert(addb->addb_cl, b_base[b_off] == b_id);
        cl_log(addb->addb_cl, CL_LEVEL_VERBOSE,
               "addb_idarray_fixed_intersect "
               "found %llu at a=%llu, b=%llu",
               (unsigned long long)a_id, a_off, b_off);

        if (*n_out >= m) return ADDB_ERR_MORE;
        id_out[(*n_out)++] = a_id;
        b_off++;
      } else {
        cl_assert(addb->addb_cl, b_off == b_n || b_base[b_off] > a_id);
        cl_log(addb->addb_cl, CL_LEVEL_VERBOSE,
               "addb_idarray_fixed_intersect: "
               "middle for a_id %llu is a=%llu, "
               "b=%llu",
               (unsigned long long)a_id, a_off, b_off);
      }

      /* Recursion: (2) The entries after a_off (tail ~)
       */
      b_base += b_off;
      b_n -= b_off;

      a_s = a_off + 1;
    }
  }
  return 0;
}
