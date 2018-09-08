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
#include "graphd/graphd.h"

#include <errno.h>
#include <stdio.h>

/**
 * @brief Given a timestamp, get the nearest local primitive.
 *
 *  Does a binary search on all primitives, which must be in
 *  timestamp order.  Whether or not that is actually true
 *  depends on the inserting party.
 *
 *  If we ever grow a timestamp-order index, this code should
 *  be changed to use it.
 *
 *  Used by asof:timestamp to translate a timestamp into local state.
 *
 * @param pdb		The database to read from
 * @param timestamp	Timestamp we're shooting for
 * @param time_op	What exactly are we looking for?
 *			GRAPHD_OP_LT	the largest that's smaller
 *			GRAPHD_OP_LE	the largest that's smaller or equal
 *			GRAPHD_OP_EQ	the exact timestamp
 *			GRAPHD_OP_GE	the smallest that's larger or equal
 *			GRAPHD_OP_GT	the smallest that's larger
 * @param id_out	NULL or where to store the resulting ID
 * @param guid_out	NULL or where to store the resulting GUID.
 *
 * @return 0 on success.
 * @return ENOENT if the request cannot be fulfilled
 * @return other nonzero errors on system error.
 */

int graphd_timestamp_to_id(pdb_handle* pdb, graph_timestamp_t const* timestamp,
                           graphd_operator op, pdb_id* id_out,
                           graph_guid* guid_out) {
  cl_handle* cl = pdb_log(pdb);
  graph_timestamp_t const ts = *timestamp;
  pdb_primitive pr;
  pdb_id n, base, hs, found, nelem;
  int err;
  bool found_loaded = false;

  pdb_primitive_initialize(&pr);
  n = pdb_primitive_n(pdb);

  /* Do a binary search in 0...n for the primitive
   * whose timestamp is closest to *timestamp according to op.
   */
  nelem = n;
  base = 0;

  if (nelem == 0) return GRAPHD_ERR_NO;

  for (;;) {
    graph_timestamp_t val;

    hs = nelem / 2;
    found = base + hs;

    cl_assert(cl, nelem > 0);
    cl_assert(cl, base < n);
    cl_assert(cl, found < n);

    pdb_primitive_finish(pdb, &pr);
    if ((err = pdb_id_read(pdb, found, &pr)) != 0) {
      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_primitive_from_timestamp: unexpected "
             "error while reading local primitive %llx: %s",
             (unsigned long long)found, strerror(err));
      return err;
    }
    found_loaded = true;
    val = pdb_primitive_timestamp_get(&pr);

    if ((val = pdb_primitive_timestamp_get(&pr)) == ts) {
      switch (op) {
        case GRAPHD_OP_LT:
          if (found-- == 0) {
            pdb_primitive_finish(pdb, &pr);
            return GRAPHD_ERR_NO;
          }
          found_loaded = false;
          break;

        case GRAPHD_OP_LE:
        case GRAPHD_OP_EQ:
        case GRAPHD_OP_GE:
          break;

        case GRAPHD_OP_GT:
          if (++found >= n) {
            pdb_primitive_finish(pdb, &pr);
            return GRAPHD_ERR_NO;
          }
          found_loaded = false;
          break;

        default:
          cl_notreached(cl, "unexpected operator %d", (int)op);
      }
      goto have_found;
    } else if (val > ts) {
      /*  We're too far into the future.
       *  Reduce the table size to exclude the
       *  item we're on.
       */
      if ((nelem = hs) == 0) {
        /*  found > ts; found - 1, if exists, < ts.
         */
        switch (op) {
          case GRAPHD_OP_LT:
          case GRAPHD_OP_LE:
            if (found-- == 0) {
              pdb_primitive_finish(pdb, &pr);
              return GRAPHD_ERR_NO;
            }
            found_loaded = false;
            break;

          case GRAPHD_OP_EQ:
            pdb_primitive_finish(pdb, &pr);
            return GRAPHD_ERR_NO;

          case GRAPHD_OP_GE:
          case GRAPHD_OP_GT:
          case GRAPHD_OP_NE:
            break;

          default:
            cl_notreached(cl, "unexpected operator %d", (int)op);
        }
        goto have_found;
      }
    } else {
      /*  We're still too far into the past.
       *  Reduce the table size to start behind
       *  the item we're on.
       */
      base = found + 1;
      if ((nelem -= hs + 1) == 0) {
        /*  found < ts; found + 1, if exists, > ts.
         */
        switch (op) {
          case GRAPHD_OP_NE:
          case GRAPHD_OP_LT:
          case GRAPHD_OP_LE:
            break;

          case GRAPHD_OP_EQ:
            pdb_primitive_finish(pdb, &pr);
            return GRAPHD_ERR_NO;

          case GRAPHD_OP_GE:
          case GRAPHD_OP_GT:
            if (++found >= n) {
              pdb_primitive_finish(pdb, &pr);
              return GRAPHD_ERR_NO;
            }
            found_loaded = false;
            break;

          default:
            cl_notreached(cl, "unexpected operator %d", (int)op);
        }
        goto have_found;
      }
    }
    cl_assert(cl, nelem > 0);
    cl_assert(cl, found < n);
    cl_assert(cl, base < n);
  }

have_found:
  cl_assert(cl, found < n);
  if (guid_out != NULL) {
    if (!found_loaded) {
      pdb_primitive_finish(pdb, &pr);
      if ((err = pdb_id_read(pdb, found, &pr)) != 0) {
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_primitive_from_timestamp: "
               "unexpected error while reading local "
               "primitive %llx: %s",
               (unsigned long long)found, strerror(err));
        return err;
      }
      found_loaded = true;
    }
    pdb_primitive_guid_get(&pr, *guid_out);
  }
  if (id_out != NULL) *id_out = found;
  pdb_primitive_finish(pdb, &pr);

  return 0;
}
