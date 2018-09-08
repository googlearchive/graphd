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
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*  All GMAP calls are instantaneous (there is no call state).
 *  The idarray is in the original only.
 */

/*  GMAP Measures            uppercase: pdb iterator concepts
 *                           lowercase: idarray concepts
 *
 *  |
 *  |<-- START -->|                                                     |
 *  |<----------------------------------- END ------------------------->|
 *  |             |                                                     |
 *  |             |                                                     |
 *  |             |<-------------- PDB ITERATOR N - 1 ------------>|    |
 *  |             |<-------------- PDB ITERATOR N --------------------->|
 *  |             |<-- FORWARDS OFFSET -->|<-- BACKWARDS OFFSET -->|    |
 *  |             |                       |                        |    |
 *  |0  data data |LOW DATA DATA DATA DATA|DATA DATA DATA DATA DATA|LAST|HIGH
 *  |             |                       |                        |    |
 *  |<------------idarray offset -------->|                        |    |
 *  |<-------------------------- idarray n ------------------------------->
 *  |             |                       |                        |    |
 */

#define OFFSET_PDB_TO_IDARRAY(pdb, it, off)                 \
  ((it)->it_gmap_start + (pdb_iterator_forward((pdb), (it)) \
                              ? (off)                       \
                              : (pdb_iterator_n((pdb), (it)) - 1) - (off)))

#define OFFSET_IDARRAY_TO_PDB(pdb, it, off) \
  (pdb_iterator_forward((pdb), (it))        \
       ? (off) - (it)->it_gmap_start        \
       : (pdb_iterator_n((pdb), (it)) - 1) - ((off) - (it)->it_gmap_start))

#define gmap_ida(it) (it->it_original->it_gmap_ida)

/*  Translate a name (like "left") to a GMAP pointer.
 */
addb_gmap *pdb_gmap_by_name(pdb_handle *pdb, char const *s, char const *e) {
  int l;

  for (l = 0; l < PDB_LINKAGE_N; l++) {
    char const *ls = pdb_linkage_to_string(l);
    size_t ln;

    if (ls != NULL && (ln = strlen(ls)) == e - s && strncasecmp(s, ls, ln) == 0)

      return pdb_linkage_to_gmap(pdb, l);
  }
  return NULL;
}

/*  Translate GMAP pointer to a name (like "left").
 */
char const *pdb_gmap_to_name(pdb_handle *pdb, addb_gmap *gmap) {
  int l;
  for (l = 0; l < PDB_LINKAGE_N; l++)
    if (gmap == pdb_linkage_to_gmap(pdb, l)) return pdb_linkage_to_string(l);
  return NULL;
}

/**
 * @brief access the next primitive in an iteration
 *
 * @param pdb		opaque module handle, created with pdb_create()
 * @param it		iteration to set through
 * @param pdb_id_out	assign the pdb_id to this
 * @param cost_inout	decrement this budget
 * @param file		filename of calling code
 * @param line		line number of calling code
 *
 * @return 0 on success, a nonzero error code on error
 */
static int pdb_iterator_gmap_next_loc(pdb_handle *pdb, pdb_iterator *it,
                                      pdb_id *pdb_id_out,
                                      pdb_budget *cost_inout, char const *file,
                                      int line) {
  int err;
  unsigned long long off;
  addb_id id;

  PDB_IS_ITERATOR(pdb->pdb_cl, it);
  cl_assert(pdb->pdb_cl, it->it_gmap != NULL);
  cl_assert(pdb->pdb_cl, pdb_id_out != NULL);

  *cost_inout -= PDB_COST_FUNCTION_CALL + PDB_COST_GMAP_ELEMENT;
  pdb_iterator_account_charge(pdb, it, next, 1,
                              PDB_COST_FUNCTION_CALL + PDB_COST_GMAP_ELEMENT);

  /*  Read the item at the current offset.
   *
   *  If we're backwards, the physical offset is
   *  (n - 1) - the virtual offset.  (We count from
   *  the end of the array backwards.)
   */
  if (it->it_gmap_offset >= pdb_iterator_n(pdb, it)) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_gmap_next: offset %llu >= n %llu",
           (unsigned long long)it->it_gmap_offset,
           (unsigned long long)pdb_iterator_n(pdb, it));

    pdb_rxs_log(pdb, "NEXT %p gmap done ($%lld)", (void *)it,
                (long long)(PDB_COST_FUNCTION_CALL + PDB_COST_GMAP_ELEMENT));
    return PDB_ERR_NO;
  }

  off = OFFSET_PDB_TO_IDARRAY(pdb, it, it->it_gmap_offset);
  err = addb_idarray_read1(&gmap_ida(it), off, &id);
  if (err != 0) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_gmap_next %s%s(%llx) [%llu]: %s",
           pdb_iterator_forward(pdb, it) ? "" : "~",
           pdb_linkage_to_string(it->it_gmap_linkage),
           (unsigned long long)it->it_gmap_source,
           (unsigned long long)it->it_gmap_offset, strerror(err));
    return err;
  }

  it->it_gmap_offset++;
  *pdb_id_out = id;

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "pdb_iterator_gmap_next %s%s(%llx) [%llu]: %llx",
         pdb_iterator_forward(pdb, it) ? "" : "~",
         pdb_linkage_to_string(it->it_gmap_linkage),
         (unsigned long long)it->it_gmap_source,
         (unsigned long long)it->it_gmap_offset - 1, (unsigned long long)id);

  pdb_rxs_log(pdb, "NEXT %p gmap %llx ($%lld)", (void *)it,
              (unsigned long long)*pdb_id_out,
              (long long)(PDB_COST_FUNCTION_CALL + PDB_COST_GMAP_ELEMENT));

  return 0;
}

/**
 * @brief access the first primitive in an iteration near a point
 *
 * @param pdb		opaque module handle, created with pdb_create()
 * @param it		iteration to set through
 * @param id_in		in: first one we'd be okay with
 * @param id_out	out: closest on or after id_in
 * @param budget_inout	decrement this budget
 * @param file		filename of calling code
 * @param line		line number of calling code
 *
 * @return 0 on success, a nonzero error code on error
 */
static int pdb_iterator_gmap_find_loc(pdb_handle *pdb, pdb_iterator *it,
                                      pdb_id id_in, pdb_id *id_out,
                                      pdb_budget *budget_inout,
                                      char const *file, int line) {
  int err;
  unsigned long long off;
  pdb_budget budget_in = *budget_inout;
  pdb_id id, id_found;

  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  cl_assert(pdb->pdb_cl, it->it_gmap != NULL);
  cl_assert(pdb->pdb_cl, pdb_iterator_statistics_done(pdb, it));
  cl_assert(pdb->pdb_cl, id_in < (1ull << 34));

  *budget_inout -= pdb_iterator_find_cost(pdb, it);
  pdb_iterator_account_charge(pdb, it, find, 1,
                              pdb_iterator_find_cost(pdb, it));

  /*  Move the ID pointer into the low...high range
   *  from the side that the iterator direction
   *  indicates.
   */
  id = id_in;
  if (pdb_iterator_forward(pdb, it)) {
    if (id < it->it_low) id = it->it_low;
  } else {
    if (it->it_high <= id) {
      cl_assert(pdb->pdb_cl, it->it_low < it->it_high);

      /*  We *do* know the highest
       *  element in the actual iterator;
       *  that's it->it_high - 1;
       */
      id = it->it_high - 1;
      it->it_gmap_offset = 0;

      goto done;
    }
  }

  pdb->pdb_runtime_statistics.rts_index_extents_read++;
  pdb->pdb_runtime_statistics.rts_index_elements_read++;

  /*  Find id_in or larger in the array.
   */
  cl_assert(pdb->pdb_cl, id < (1ull << 34));
  err = addb_idarray_search(&gmap_ida(it), it->it_gmap_start, it->it_gmap_end,
                            id, &off, &id_found);
  if (err != 0) {
    /*  This error is a system error,
     *  not a "we ran out of data" error.
     */
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
           "pdb_iterator_gmap_find %llx -> %llx: %s ($%d)",
           (unsigned long long)id_in, (unsigned long long)id, strerror(err),
           (int)pdb_iterator_check_cost(pdb, it));
    return err;
  }

  /*  Running off the high end?
   */
  if (off >= it->it_gmap_end) {
    cl_assert(pdb->pdb_cl, off == it->it_gmap_end);

    if (pdb_iterator_forward(pdb, it)) {
      it->it_gmap_offset = pdb_iterator_n(pdb, it);

      cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
             "pdb_iterator_gmap_find_loc: "
             "too high ($%d)",
             (int)pdb_iterator_check_cost(pdb, it));
      pdb_rxs_log(pdb, "FIND %p gmap %llx done ($%lld)", (void *)it,
                  (unsigned long long)id_in,
                  (long long)(budget_in - *budget_inout));
      return PDB_ERR_NO;
    }

    /*  Backwards.  Odd, we should have cought that
     *  when we turned out >= it->it_high.
     */
    it->it_gmap_offset = 0;
    *id_out = it->it_high - 1;

    goto done;
  }

  it->it_gmap_offset = OFFSET_IDARRAY_TO_PDB(pdb, it, off);

  /*  Found it?
   */
  if (id == id_found) goto done;

  /*  Didn't find it; we slipped forward in idarray order.
   */
  if (pdb_iterator_forward(pdb, it)) {
    /*  We slipped forward; that's what we're supposed to do.
     */
    id = id_found;
    goto done;
  }

  /*  We slipped in the wrong direction.  (idarray search slips
   *  forward; backwards on-or-after slips backwards.)
   *  Go back one more; that will give us the correct result.
   *
   *  In a backwards iterator, going back means incrementing the offset.
   */
  it->it_gmap_offset++;
  if (it->it_gmap_offset >= pdb_iterator_n(pdb, it)) {
    /*  We were already at the upper end of the scale.
     */
    it->it_gmap_offset = pdb_iterator_n(pdb, it);

    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_gmap_find_loc: "
           "slipped backwards out of range ($%d)",
           (int)pdb_iterator_check_cost(pdb, it));
    pdb_rxs_log(pdb, "FIND %p gmap %llx done ($%lld)", (void *)it,
                (unsigned long long)id_in,
                (long long)(budget_in - *budget_inout));
    return PDB_ERR_NO;
  }

  /*  Read the item just before what addb_idarray_search returned.
   */
  off = OFFSET_PDB_TO_IDARRAY(pdb, it, it->it_gmap_offset);
  err = addb_idarray_read1(&gmap_ida(it), off, &id);
  if (err != 0) {
    /*  This error is a system error,
     *  not a "we ran out of data" error.
     */
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_idarray_read1", err,
                 "id=%llx", (unsigned long long)id_in);
    return err;
  }

done:
  it->it_gmap_offset++;
  *id_out = id;
  cl_assert(pdb->pdb_cl, *id_out >= it->it_low);
  cl_assert(pdb->pdb_cl, *id_out < it->it_high);

  pdb_rxs_log(pdb, "FIND %p gmap %llx -> %llx ($%lld)", (void *)it,
              (unsigned long long)id_in, (unsigned long long)*id_out,
              (long long)(budget_in - *budget_inout));

  return 0;
}

/**
 * @brief Turn the GMAP iterator into a string, e.g. for use in a cursor.
 *
 *  Syntax:	gmap:[~]LOW[-HIGH]:LINKAGE->SOURCE/OFFSET/
 *
 *	~	-- ~ for reverse, nothing for forward direction
 *	LOW	-- result ID at low end of the range (first included)
 *	HIGH	-- result ID at high end of the range (first not included),
 *		   missing if PDB_ITERATOR_HIGH_ANY.
 * 	LINKAGE	-- one of "typeguid" "right" "left" "scope"
 *	SOURCE	-- ID we're indexing the map with
 *	OFFSET	-- current offset, first is 0.
 *
 * @param pdb		opaque module handle, created with pdb_create()
 * @param it		iteration to set through
 * @param flags		which parts do we want?
 * @param buf		append position string to this.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int pdb_iterator_gmap_freeze(pdb_handle *pdb, pdb_iterator *it,
                                    unsigned int flags, cm_buffer *buf) {
  int err;
  char const *sep = "";

  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  cl_assert(pdb->pdb_cl, buf != NULL);
  cl_cover(pdb->pdb_cl);

  if (flags & PDB_ITERATOR_FREEZE_SET) {
    err = pdb_iterator_freeze_intro(buf, it, "gmap");
    if (err != 0) return err;

    err = cm_buffer_sprintf(buf, ":%.1s->%llu",
                            pdb_linkage_to_string(it->it_gmap_linkage),
                            (unsigned long long)it->it_gmap_source);
    if (err != 0) return err;

    err = pdb_iterator_freeze_ordering(pdb, buf, it);
    if (err) return err;

    err = pdb_iterator_freeze_account(pdb, buf, it);
    if (err) return err;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    err = cm_buffer_sprintf(buf, "%s%llu", sep,
                            (unsigned long long)it->it_gmap_offset);
    if (err) return err;
    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_STATE) {
    err = cm_buffer_sprintf(buf, "%s", sep);
    if (err) return err;
  }
  return 0;
}

/**
 * @brief Reset the current position in an iteration to the beginning
 *
 * @param pdb		opaque module handle, created with pdb_create()
 * @param it		iteration to set
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int pdb_iterator_gmap_reset(pdb_handle *pdb, pdb_iterator *it) {
  it->it_has_position = true;
  it->it_gmap_offset = 0;

  return 0;
}

static int pdb_iterator_gmap_clone(pdb_handle *pdb, pdb_iterator *it,
                                   pdb_iterator **it_out) {
  int err;
  pdb_iterator *it_orig = it->it_original;

  PDB_IS_ITERATOR(pdb->pdb_cl, it);
  PDB_IS_ORIGINAL_ITERATOR(pdb->pdb_cl, it_orig);
  cl_assert(pdb->pdb_cl, !it_orig->it_suspended);

  cl_cover(pdb->pdb_cl);
  {
    char buf[200];
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_gmap_clone(%s), offset=%lld%s",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf),
           (long long)it->it_gmap_offset,
           pdb_iterator_has_position(pdb, it) ? "" : " (inactive)");
  }

  if ((err = pdb_iterator_make_clone(pdb, it_orig, it_out)) != 0) return err;

  /*  Only the original's gmap_ida is live.  Zero out this one.
   */
  memset(&(*it_out)->it_gmap_ida, 0, sizeof((*it_out)->it_gmap_ida));

  if (!pdb_iterator_has_position(pdb, it)) {
    err = pdb_iterator_gmap_reset(pdb, *it_out);
    cl_assert(pdb->pdb_cl, err == 0);
  } else {
    (*it_out)->it_gmap_offset = it->it_gmap_offset;
    (*it_out)->it_has_position = true;
  }
  cl_assert(pdb->pdb_cl, pdb_iterator_has_position(pdb, *it_out));

  return 0;
}

/*  Free the iterator.
 */
static void pdb_iterator_gmap_finish(pdb_handle *pdb, pdb_iterator *it) {
  PDB_IS_FINISHING_ITERATOR(pdb->pdb_cl, it);

  cl_assert(pdb->pdb_cl, it->it_gmap != NULL);
  cl_cover(pdb->pdb_cl);

  /* Only in the original ...
   */
  if (it->it_original == it) {
    if (!it->it_suspended) addb_idarray_finish(&it->it_gmap_ida);
  }

  if (it->it_displayname != NULL) {
    cm_free(pdb->pdb_cm, it->it_displayname);
    it->it_displayname = NULL;
  }
  it->it_type = NULL;
  it->it_magic = 0;
}

static char const *pdb_iterator_gmap_to_string(pdb_handle *pdb,
                                               pdb_iterator *it, char *buf,
                                               size_t size) {
  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  cl_assert(pdb->pdb_cl, it->it_displayname == NULL);

  snprintf(buf, size, "%sgmap:%.1s(%llx):[%llx@%llu..%llx@%llu]",
           it->it_forward ? "" : "~",
           pdb_linkage_to_string(it->it_gmap_linkage),
           (unsigned long long)it->it_gmap_source, it->it_low,
           it->it_gmap_start, it->it_high - 1, it->it_gmap_end - 1);
  it->it_displayname = cm_strmalcpy(pdb->pdb_cm, buf);

  return buf;
}

static int pdb_iterator_gmap_check(pdb_handle *pdb, pdb_iterator *it, pdb_id id,
                                   pdb_budget *budget_inout) {
  int err;
  addb_id found_id;
  unsigned long long off;
  cl_handle *cl = pdb->pdb_cl;

  if (id < it->it_low || id >= it->it_high) {
    *budget_inout -= PDB_COST_FUNCTION_CALL;

    pdb_iterator_account_charge(pdb, it, check, 1, PDB_COST_FUNCTION_CALL);

    pdb_rxs_log(pdb, "CHECK %p gmap %llx no ($%lld; boundaries)", (void *)it,
                (unsigned long long)id, (long long)PDB_COST_FUNCTION_CALL);
    return PDB_ERR_NO;
  }
  if (id == it->it_gmap_cached_check_id) {
    *budget_inout -= PDB_COST_FUNCTION_CALL;
    pdb_iterator_account_charge(pdb, it, check, 1, PDB_COST_FUNCTION_CALL);

    pdb_rxs_log(pdb, "CHECK %p gmap %llx %s ($%lld; cached)", (void *)it,
                (unsigned long long)id,
                it->it_gmap_cached_check_result ? "no" : "ok",
                (long long)PDB_COST_FUNCTION_CALL);
    return it->it_gmap_cached_check_result ? PDB_ERR_NO : 0;
  }

  /*  If it's cheaper to just read the primitive
   *  and look, do that, rather than checking against the
   *  gmap.
   */
  *budget_inout -= pdb_iterator_check_cost(pdb, it);
  pdb_iterator_account_charge(pdb, it, check, 1,
                              pdb_iterator_check_cost(pdb, it));

  if (pdb_iterator_check_cost(pdb, it) > PDB_COST_PRIMITIVE) {
    pdb_primitive pr;

    err = pdb_id_read(pdb, id, &pr);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err, "id=%llx",
                   (unsigned long long)id);
      return err;
    }

    if (!pdb_primitive_has_linkage(&pr, it->it_gmap_linkage))
      err = PDB_ERR_NO;
    else {
      graph_guid guid;

      pdb_primitive_linkage_get(&pr, it->it_gmap_linkage, guid);
      err = pdb_id_from_guid(pdb, &found_id, &guid);
      if (err == 0) err = found_id == it->it_gmap_source ? 0 : PDB_ERR_NO;
    }
    pdb_primitive_finish(pdb, &pr);
    cl_assert(pdb->pdb_cl, err != PDB_ERR_MORE);
    goto have_result;
  } else {
    cl_assert(pdb->pdb_cl, id < (1ull << 34));

    err = addb_idarray_search(&gmap_ida(it), it->it_gmap_start, it->it_gmap_end,
                              id, &off, &found_id);
    if (err == PDB_ERR_NO) goto have_result;
  }
  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_idarray_search", err,
                 "can't search %s(%llx) for %llx",
                 pdb_linkage_to_string(it->it_gmap_linkage),
                 (unsigned long long)it->it_gmap_source,
                 (unsigned long long)id);
    return err;
  }
  err = (id == found_id && off < it->it_gmap_end) ? 0 : PDB_ERR_NO;

have_result:
  cl_assert(pdb->pdb_cl, err != PDB_ERR_MORE);

  it->it_gmap_cached_check_id = id;
  it->it_gmap_cached_check_result = !!err;

  pdb_rxs_log(pdb, "CHECK %p gmap %llx %s ($%lld)", (void *)it,
              (unsigned long long)id, err == 0 ? "yes" : "no",
              (long long)pdb_iterator_check_cost(pdb, it));
  return err;
}

/**
 * @brief Return the idarray for a GMAP iterator.
 *
 * @param pdb		module handle
 * @param it		a gmap iterator
 * @param id_out	out: idarray
 * @param s_out		out: first index
 * @param e_out		out: first index not included
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int pdb_iterator_gmap_idarray(pdb_handle *pdb, pdb_iterator *it,
                                     addb_idarray **ida_out,
                                     unsigned long long *s_out,
                                     unsigned long long *e_out) {
  cl_assert(pdb->pdb_cl, !it->it_suspended);

  *ida_out = &gmap_ida(it);
  *s_out = it->it_gmap_start;
  *e_out = it->it_gmap_end;

  return 0;
}

/**
 * @brief Return the summary for a GMAP iterator.
 *
 * @param pdb		module handle
 * @param it		a gmap iterator
 * @param psum_out	out: summary
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int pdb_iterator_gmap_primitive_summary(
    pdb_handle *pdb, pdb_iterator *it, pdb_primitive_summary *psum_out) {
  int err;

  if (it->it_gmap_linkage >= PDB_LINKAGE_N) return PDB_ERR_NO;

  if (!it->it_gmap_source_guid_valid) {
    err = pdb_id_to_guid(pdb, it->it_gmap_source, &it->it_gmap_source_guid);
    if (err != 0) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_id_to_guid", err,
                   "it->it_gmap_source=%lld", (long long)it->it_gmap_source);
      return err;
    }
    it->it_gmap_source_guid_valid = true;
  }

  psum_out->psum_guid[it->it_gmap_linkage] = it->it_gmap_source_guid;
  psum_out->psum_locked = 1 << it->it_gmap_linkage;
  psum_out->psum_result = PDB_LINKAGE_N;
  psum_out->psum_complete = true;

  return 0;
}

static int pdb_iterator_gmap_restrict(pdb_handle *pdb, pdb_iterator *it,
                                      pdb_primitive_summary const *psum,
                                      pdb_iterator **it_out) {
  int err;
  int linkage;

  /*  We can only do this for gmap iterators with a single linkage,
   *  and psums whose result is the primitive GUID.
   */
  if (it->it_gmap_linkage >= PDB_LINKAGE_N ||
      psum->psum_result != PDB_LINKAGE_N)
    return PDB_ERR_ALREADY;

  /*  Do we conflict with the restriction?
   */
  if (psum->psum_locked & (1 << it->it_gmap_linkage)) {
    pdb_id id;
    err = pdb_id_from_guid(pdb, &id, psum->psum_guid + it->it_gmap_linkage);
    if (err != 0) return err;

    if (id != it->it_gmap_source) return PDB_ERR_NO;
  }

  /*  Turn to VIP?
   */

  /*  Case 1, I'm a type, you're a left or right.
   */
  if ((it->it_gmap_linkage == PDB_LINKAGE_TYPEGUID) &&
      (((psum->psum_locked &
         ((1 << PDB_LINKAGE_RIGHT) | (1 << PDB_LINKAGE_LEFT))) ==
        (1 << (linkage = PDB_LINKAGE_RIGHT))) ||
       ((psum->psum_locked &
         ((1 << PDB_LINKAGE_RIGHT) | (1 << PDB_LINKAGE_LEFT))) ==
        (1 << (linkage = PDB_LINKAGE_LEFT))))) {
    graph_guid guid;

    err = pdb_id_to_guid(pdb, it->it_gmap_source, &guid);
    if (err != 0) return err;

    return pdb_vip_linkage_iterator(
        pdb, psum->psum_guid + linkage, linkage, &guid, it->it_low, it->it_high,
        pdb_iterator_forward(pdb, it), true, it_out, NULL);
  }

  /*  Case 2, I'm a left or right, you're a type.
   */
  if ((it->it_gmap_linkage == PDB_LINKAGE_RIGHT ||
       it->it_gmap_linkage == PDB_LINKAGE_LEFT) &&
      (psum->psum_locked & (1 << PDB_LINKAGE_TYPEGUID)))

    /*  Turn into our VIP.
     */
    return pdb_iterator_gmap_to_vip(pdb, it, it->it_gmap_linkage,
                                    psum->psum_guid + PDB_LINKAGE_TYPEGUID,
                                    it_out);

  return PDB_ERR_ALREADY;
}

/**
 * @brief Has this iterator progressed beyond this value?
 *
 * @param pdb		module handle
 * @param it		iterator we're asking about
 * @param s		start of comparison value
 * @param e		end of comparison value
 * @param beyond_out	out: true if we're safely beyond this value.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int pdb_iterator_gmap_beyond(pdb_handle *pdb, pdb_iterator *it,
                                    char const *s, char const *e,
                                    bool *beyond_out) {
  int err;
  char buf[200];
  unsigned long long off;
  pdb_id id, last_id;

  if (e - s != sizeof(id)) {
    *beyond_out = false;
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb_iterator_gmap_beyond: unexpected "
           "value size (%zu bytes; expected %zu)",
           (size_t)(e - s), sizeof(id));
    return EINVAL;
  }
  memcpy(&id, s, sizeof(id));

  if (it->it_gmap_offset == 0) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_gmap_beyond: "
           "still at the beginning");
    *beyond_out = false;
    return 0;
  }

  cl_assert(pdb->pdb_cl, !it->it_suspended);

  off = OFFSET_PDB_TO_IDARRAY(pdb, it, it->it_gmap_offset - 1);
  err = addb_idarray_read1(&gmap_ida(it), off, &last_id);
  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "addb_idarray_read1", err,
                 "off=%llu", off);
    return err;
  }

  *beyond_out = (pdb_iterator_forward(pdb, it) ? id < last_id : id > last_id);

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "pdb_iterator_gmap_beyond: %llx vs. last_id %llx in %s: %s",
         (unsigned long long)id, (unsigned long long)last_id,
         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         *beyond_out ? "yes" : "no");
  return 0;
}

static int pdb_iterator_gmap_range_estimate(pdb_handle *pdb, pdb_iterator *it,
                                            pdb_range_estimate *range) {
  pdb_id id, off;
  int err;

  pdb_iterator_range_estimate_default(pdb, it, range);

  if (it->it_gmap_offset == 0) {
    range->range_n_max = range->range_n_exact = pdb_iterator_n(pdb, it);

    return 0;
  }

  if (it->it_gmap_offset >= pdb_iterator_n(pdb, it)) {
    range->range_low = range->range_high = 0;
    range->range_n_max = range->range_n_exact = 0;

    return 0;
  }

  off = OFFSET_PDB_TO_IDARRAY(pdb, it, it->it_gmap_offset);
  err = addb_idarray_read1(&gmap_ida(it), off, &id);
  if (err != 0) {
    char buf[200];
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_idarray_read1", err,
                 "off=%llu, it=%s", (unsigned long long)off,
                 pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    return err;
  }
  if (pdb_iterator_forward(pdb, it))
    range->range_low = id;
  else
    range->range_high = id + 1;

  range->range_n_exact = range->range_n_max =
      pdb_iterator_n(pdb, it) - (it->it_gmap_start + it->it_gmap_offset);
  return 0;
}

/*
 * Suspend access to the database.
 */
static int pdb_iterator_gmap_suspend(pdb_handle *pdb, pdb_iterator *it) {
  if (it->it_original == it) addb_idarray_finish(&it->it_gmap_ida);
  return 0;
}

/* Resume access to the database.
 */
static int pdb_iterator_gmap_unsuspend(pdb_handle *pdb, pdb_iterator *it) {
  int err;
  cl_handle *const cl = pdb->pdb_cl;
  addb_gmap *gmap;
  pdb_iterator *new_it;

  if (it->it_original != it) {
    int err;

    /*  If our original now has a different type,
     *  become that type.
     */
    err = pdb_iterator_refresh(pdb, it);
    return err == PDB_ERR_ALREADY ? 0 : err;
  }

  /*  We're the original.  Reopen.
   */
  gmap = pdb_linkage_to_gmap(pdb, it->it_gmap_linkage);
  cl_assert(cl, gmap != NULL);

  err = addb_gmap_idarray(gmap, it->it_gmap_source, &it->it_gmap_ida);
  switch (err) {
    case 0:
      break;

    case ADDB_ERR_NO:
      return pdb_iterator_null_become(pdb, it);

    case ADDB_ERR_BITMAP:

      /*  Recreate the iterator as a bitmap.
       */
      err = pdb_iterator_bgmap_create(
          pdb, gmap, it->it_gmap_source, it->it_gmap_linkage, it->it_high,
          it->it_low, pdb_iterator_forward(pdb, it), &new_it);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_bgmap_create", err,
                     "%s(%llx)", pdb_linkage_to_string(it->it_gmap_linkage),
                     (unsigned long long)it->it_gmap_source);
        return err;
      }
      return pdb_iterator_substitute(pdb, it, new_it);

    default:
      cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_idarray", err, "%s(%llx)",
                   pdb_linkage_to_string(it->it_gmap_linkage),
                   (unsigned long long)it->it_gmap_source);
      return err;
  }
  return 0;
}

static const pdb_iterator_type pdb_iterator_gmap = {
    "gmap",

    pdb_iterator_gmap_finish,
    pdb_iterator_gmap_reset,
    pdb_iterator_gmap_clone,
    pdb_iterator_gmap_freeze,
    pdb_iterator_gmap_to_string,

    pdb_iterator_gmap_next_loc,
    pdb_iterator_gmap_find_loc,
    pdb_iterator_gmap_check,
    pdb_iterator_util_statistics_none,

    pdb_iterator_gmap_idarray,
    pdb_iterator_gmap_primitive_summary,
    pdb_iterator_gmap_beyond,
    pdb_iterator_gmap_range_estimate,
    pdb_iterator_gmap_restrict,

    pdb_iterator_gmap_suspend,
    pdb_iterator_gmap_unsuspend};

/**
 * @brief initialize a GMAP iterator.
 *
 *  If the GMAP iterator would be empty, a null iterator is
 *  created instead.
 *
 * @param pdb		module handle
 * @param gmap		GMAP to index with source
 * @param source	index whose result we're iterating over
 * @param low		lowest included value
 * @param high		highest value that isn't included
 * @param forward	true if we're iterating from low to high.
 * @param error_if_null	if true, don't bother creating a null iterator
 *			 for an empty case; just return PDB_ERR_NO.
 * @param it_out	the iterator to initialize
 *
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_iterator_gmap_create(pdb_handle *pdb, addb_gmap *gmap, int linkage,
                             pdb_id source, pdb_id low, pdb_id high,
                             bool forward, bool error_if_null,
                             pdb_iterator **it_out) {
  int err;
  unsigned long long n, end, start;
  addb_id last;
  pdb_iterator *it;
  cl_handle *const cl = pdb->pdb_cl;
  addb_idarray ida;
  char buf[200];
  pdb_budget bsearch_cost;

  *it_out = NULL;
  cl_cover(cl);
  if (pdb->pdb_primitive == NULL &&
      ((err = pdb_initialize(pdb)) != 0 ||
       (err = pdb_initialize_checkpoint(pdb)) != 0)) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_initialize", err, "can't initialize?");
    return err;
  }

  if (low <= source) low = source + 1;

  if (low >= high) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_gmap_create: low=%llx >= high=%llx, "
           "returning null iterator",
           (unsigned long long)low, (unsigned long long)high);

    return error_if_null ? PDB_ERR_NO : pdb_iterator_null_create(pdb, it_out);
  }

  err = addb_gmap_idarray(gmap, source, &ida);
  if (err != 0) {
    if (err == ADDB_ERR_NO)
      return error_if_null ? PDB_ERR_NO : pdb_iterator_null_create(pdb, it_out);

    if (err == ADDB_ERR_BITMAP)
      return pdb_iterator_bgmap_create(pdb, gmap, source, linkage, high, low,
                                       forward, it_out);

    cl_log_errno(cl, CL_LEVEL_FAIL, "addb_gmap_idarray", err, "%s(%llx)",
                 pdb_linkage_to_string(linkage), (unsigned long long)source);
    return err;
  }

  /*  Determine start offset and true low.
   */
  if (low <= source + 1) {
    /* The start is 0; the true low is the zero'th element.
     */
    err = addb_idarray_read1(&ida, 0, &low);
    if (err != 0) {
      addb_idarray_finish(&ida);
      if (err == PDB_ERR_NO) {
        cl_log(cl, CL_LEVEL_VERBOSE,
               "pdb_iterator_gmap_create: "
               "failed to read 0th element "
               "from %s(%llx); returning null "
               "iterator",
               pdb_linkage_to_string(linkage), (unsigned long long)source);
        return error_if_null ? PDB_ERR_NO
                             : pdb_iterator_null_create(pdb, it_out);
      }

      cl_log_errno(cl, CL_LEVEL_FAIL, "addb_idarray_read1", err, "%s(%llx)[0]",
                   pdb_linkage_to_string(linkage), (unsigned long long)source);
      return err;
    }
    start = 0;
  } else {
    /*  Find the lowest element we've been given (or the
     *  first higher one that actually exists), and remember
     *  that offset.
     */
    cl_assert(pdb->pdb_cl, low < (1ull << 34));
    err = addb_idarray_search(&ida, 0, addb_idarray_n(&ida), low, &start, &low);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "addb_idarray_search", err,
                   "%llx in %s(%llx)", (unsigned long long)low,
                   pdb_linkage_to_string(linkage), (unsigned long long)source);

      addb_idarray_finish(&ida);
      return err;
    }

    /* There was no existing element >= low?
     */
    if (start >= addb_idarray_n(&ida)) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "pdb_iterator_gmap_create: adjusted start=%llu "
             ">= n=%llu, returning null iterator",
             (unsigned long long)start,
             (unsigned long long)addb_idarray_n(&ida));
      goto null;
    }
  }

  /*  Do we have enough information to throw this out yet?
   *  If yes, stop wasting our time with measurements and just
   *  return a null iterator.
   */
  if (low >= high) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_gmap_create: adjusted low=%llx "
           ">= high=%llx, returning null iterator",
           (unsigned long long)low, (unsigned long long)high);

  null:
    addb_idarray_finish(&ida);
    if (error_if_null) return PDB_ERR_NO;
    return pdb_iterator_null_create(pdb, it_out);
  }

  /*  Determine end offset, last, and with it the true high (last + 1)
   */
  if (high == PDB_ITERATOR_HIGH_ANY) {
    /* Find the last element. */

    end = addb_idarray_n(&ida);
    if (end == 0) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "pdb_iterator_gmap_create: 0 elements in "
             "%s(%llx); returning null iterator",
             pdb_linkage_to_string(linkage), (unsigned long long)source);
      goto null;
    }
    err = addb_idarray_read1(&ida, end - 1, &last);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "addb_idarray_read1", err,
                   "%s(%llx)[%llu]", pdb_linkage_to_string(linkage),
                   (unsigned long long)source, end - 1);

    err:
      addb_idarray_finish(&ida);
      return err;
    }
  } else {
    cl_assert(cl, high > 0);

    /*  Find the end element we've been given, and remember
     *  that offset.
     */
    cl_assert(pdb->pdb_cl, high - 1 < (1ull << 34));
    err = addb_idarray_search(&ida, start, addb_idarray_n(&ida), high - 1, &end,
                              &last);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "addb_idarray_search", err,
                   "%llx in %s(%llx)", (unsigned long long)high - 1,
                   pdb_linkage_to_string(linkage), (unsigned long long)source);
      goto err;
    }

    /*  At the end of the next "if", "end" is the end offset --
     *  the first one *not* included.
     */
    if (last == high - 1 && end < addb_idarray_n(&ida)) {
      end++;
    } else {
      /* We slipped forwards.  The last included element
       * is the one *before* this one.  Read that value.
       */
      if (end == 0) {
        cl_log(cl, CL_LEVEL_VERBOSE,
               "pdb_iterator_gmap_create: no "
               "elements between %llu and @%llx; "
               "returning null iterator",
               (unsigned long long)start, (unsigned long long)high - 1);
        goto null;
      }
      err = addb_idarray_read1(&ida, end - 1, &last);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "addb_idarray_read1", err,
                     "%s(%llx)[%llu]", pdb_linkage_to_string(linkage),
                     (unsigned long long)source, end);
        goto err;
      }
    }
  }

  /*  Do we still think there are elements in this collection?
   */
  if (low >= last + 1) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_gmap_create: adjusted low %llx "
           ">= adjusted high %llx: returning null iterator",
           (unsigned long long)low, (unsigned long long)last + 1);
    goto null;
  }

  if (start >= end)
    cl_log(cl, CL_LEVEL_ERROR,
           "start=%llu, end=%llu, low=%llx, last=%llx, what gives?",
           (unsigned long long)start, (unsigned long long)end,
           (unsigned long long)low, (unsigned long long)last);
  cl_assert(pdb->pdb_cl, start < end);

  if ((*it_out = it = cm_malloc(pdb->pdb_cm, sizeof(*it))) == NULL) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "cm_malloc", errno,
                 "can't allocate iterator?");
    addb_idarray_finish(&ida);
    return ENOMEM;
  }

  pdb_iterator_make(pdb, it, low, last + 1, forward);

  it->it_type = &pdb_iterator_gmap;
  it->it_gmap = gmap;
  it->it_gmap_source = source;

  it->it_gmap_ida = ida;
  it->it_gmap_end = end;
  it->it_gmap_start = start;
  it->it_gmap_linkage = linkage;
  it->it_gmap_cached_check_id = PDB_ID_NONE;

  cl_assert(pdb->pdb_cl, start < end);

  pdb->pdb_runtime_statistics.rts_index_extents_read++;
  n = end - start;
  cl_assert(pdb->pdb_cl, n > 0);

  pdb_iterator_sorted_set(pdb, it, true);
  pdb_iterator_n_set(pdb, it, n);
  pdb_iterator_suspend_chain_in(pdb, it);

  cl_log(cl, CL_LEVEL_VERBOSE,
         "pdb_iterator_gmap_create: %p lo=%llx hi=%llx n=%llu", (void *)*it_out,
         (unsigned long long)low, (unsigned long long)high,
         (unsigned long long)pdb_iterator_n(pdb, *it_out));

  /*  If it costs us more to check in the table
   *  than in the primitive, use the primitive!
   */
  bsearch_cost =
      PDB_COST_FUNCTION_CALL + pdb_iterator_bsearch_cost(n, 32 * 1024 / 5,
                                                         PDB_COST_GMAP_ARRAY,
                                                         PDB_COST_GMAP_ELEMENT);

  pdb_iterator_check_cost_set(
      pdb, it,
      bsearch_cost > PDB_COST_PRIMITIVE + PDB_COST_FUNCTION_CALL
          ? PDB_COST_PRIMITIVE + PDB_COST_FUNCTION_CALL
          : bsearch_cost);
  pdb_iterator_find_cost_set(pdb, it, bsearch_cost);
  pdb_iterator_next_cost_set(pdb, it,
                             PDB_COST_FUNCTION_CALL + PDB_COST_GMAP_ELEMENT);
  pdb_iterator_statistics_done_set(pdb, it);

  pdb_rxs_log(pdb, "CREATE %p gmap %s(%llx) %llx %llx %s", (void *)it,
              pdb_linkage_to_string(linkage), (unsigned long long)source,
              (unsigned long long)low, (unsigned long long)high,
              forward ? "forward" : "backward");

  cl_log(cl, CL_LEVEL_VERBOSE | PDB_FACILITY_ITERATOR,
         "PDB STAT for %s: n=%llu cc=%llu "
         "nc=%llu fc=%llu; sorted %lld..%lld (incl)",

         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         (unsigned long long)pdb_iterator_n(pdb, it),
         (unsigned long long)pdb_iterator_check_cost(pdb, it),
         (unsigned long long)pdb_iterator_next_cost(pdb, it),
         (unsigned long long)pdb_iterator_find_cost(pdb, it), it->it_low,
         it->it_high - 1);

  return 0;
}

/**
 * @brief Desequentialize a GMAP iterator.
 *
 * @param pdb		module handle
 * @param pit		pointer to text representation
 * @param pib		reconcilement base (unused)
 * @param it_out	the iterator to initialize
 *
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_iterator_gmap_thaw(pdb_handle *pdb, pdb_iterator_text const *pit,
                           pdb_iterator_base *pib, pdb_iterator **it_out) {
  int err, linkage;
  addb_gmap *gmap;
  pdb_id source;
  unsigned long long low, high, off = 0;
  bool forward;
  char const *s = pit->pit_set_s;
  char const *e = pit->pit_set_e;
  char const *ordering = NULL;
  pdb_iterator_account *acc = NULL;

  cl_cover(pdb->pdb_cl);

  /*  :[~]LOW[-HIGH]:LRTS->id[o:..]/OFF/
   */
  err = pdb_iterator_util_thaw(pdb, &s, pit->pit_set_e,
                               "%{forward}%{low[-high]}:%{linkage}->%{id}%{"
                               "ordering}%{account}%{extensions}%{end}",
                               &forward, &low, &high, &linkage, &source, pib,
                               &ordering, pib, &acc,
                               (pdb_iterator_property *)NULL);
  if (err != 0) return err;

  off = 0;
  if ((s = pit->pit_position_s) != NULL && s < (e = pit->pit_position_e)) {
    e = pit->pit_position_e;
    if ((err = pdb_scan_ull(&s, e, &off)) != 0) {
      cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
             "pdb_iterator_gmap_thaw: expected "
             "offset, got \"%.*s\": %s",
             (int)(e - s), s, strerror(err));
      return err;
    }
    err = pdb_iterator_util_thaw(pdb, &s, e, "%{extensions}%{end}",
                                 (pdb_iterator_property *)NULL);
    if (err != 0) return err;
  }
  if ((s = pit->pit_state_s) != NULL && s < (e = pit->pit_state_e)) {
    err = pdb_iterator_util_thaw(pdb, &s, e, "%{extensions}%{end}",
                                 (pdb_iterator_property *)NULL);
    if (err != 0) return err;
  }

  gmap = pdb_linkage_to_gmap(pdb, linkage);
  err = pdb_iterator_gmap_create(pdb, gmap, linkage, source, low, high, forward,
                                 false, it_out);
  if (err != 0) return err;

  pdb_iterator_account_set(pdb, *it_out, acc);

  /*
   * Check if we ended up with a bgmap. If we did,
   * do the conversions to get the right offset
   */
  if (pdb_iterator_bgmap_is_instance(pdb, *it_out, PDB_LINKAGE_ANY)) {
    err = pdb_iterator_bgmap_position_recover_init(pdb, *it_out, off);
    if (err) {
      pdb_iterator_destroy(pdb, it_out);
      return err;
    }
  } else {
    (*it_out)->it_gmap_offset = off;
  }

  if (ordering != NULL) {
    pdb_iterator_ordering_set(pdb, *it_out, ordering);
    pdb_iterator_ordered_set(pdb, *it_out, true);
  } else {
    pdb_iterator_ordered_set(pdb, *it_out, false);
  }
  return 0;
}

/**
 * @brief Is this a gmap iterator?
 *
 * @param pdb		module handle
 * @param it		the iterator we're asking about
 * @param linkage	linkage we're asking about
 *
 * @return true if this iterator is an instance
 * 	of the GMAP for the specific linkage, false if not.
 */
bool pdb_iterator_gmap_is_instance(pdb_handle *pdb, pdb_iterator const *it,
                                   int linkage) {
  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  return it != NULL && it->it_type == &pdb_iterator_gmap &&
         (linkage == PDB_LINKAGE_ANY ||
          it->it_gmap == pdb_linkage_to_gmap(pdb, linkage));
}

/**
 * @brief What is this iterator's linkage?
 *
 * @param pdb		module handle
   @param it		the iterator we're asking about
 * @param linkage	out: the local ID
 *
 * @return 0 on success, a nonzero error code on error.
 * @return PDB_ERR_NO if the iterator doesn't support a linkage.
 */
int pdb_iterator_gmap_linkage(pdb_handle *pdb, pdb_iterator *it, int *linkage) {
  /*
   * XXX This should probably be split into a pdb_iterator_xgmap
   * function like wd did with pdb_iterator_gmap_instance.
   */

  /*  Calls to this should probably use
   *  pdb_iterator_primitive_summary() instead.
   */
  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  if (pdb_iterator_gmap_is_instance(pdb, it, PDB_LINKAGE_ANY))
    *linkage = it->it_gmap_linkage;

  else if (pdb_iterator_bgmap_is_instance(pdb, it, PDB_LINKAGE_ANY)) {
    *linkage = it->it_bgmap_linkage;
  } else {
    return PDB_ERR_NO;
  }

  return 0;
}

/**
 * @brief What is this iterator's source, as a local ID?
 *
 * @param pdb		module handle
   @param it		the iterator we're asking about
 * @param source_id	out: the local ID
 *
 * @return 0 on success, a nonzero error code on error.
 * @return PDB_ERR_NO if the iterator doesn't support a source ID.
 */
int pdb_iterator_gmap_source_id(pdb_handle *pdb, pdb_iterator *it,
                                pdb_id *source_id) {
  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  if (pdb_iterator_gmap_is_instance(pdb, it, PDB_LINKAGE_ANY))
    *source_id = it->it_gmap_source;

  else if (pdb_iterator_bgmap_is_instance(pdb, it, PDB_LINKAGE_ANY))
    *source_id = it->it_bgmap_source;

  else
    return PDB_ERR_NO;

  return 0;
}

int pdb_iterator_gmap_to_vip(pdb_handle *pdb, pdb_iterator *it, int linkage,
                             graph_guid const *qualifier,
                             pdb_iterator **it_out) {
  int err = 0;
  unsigned long long n, n2;
  char buf[GRAPH_GUID_SIZE];

  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  if (it == NULL || it->it_type != &pdb_iterator_gmap || qualifier == NULL ||
      GRAPH_GUID_IS_NULL(*qualifier))
    return PDB_ERR_ALREADY;

  if (it->it_suspended) {
    err = pdb_iterator_unsuspend(pdb, it);
    if (err != 0) return err;
  }

  pdb->pdb_runtime_statistics.rts_index_extents_read++;
  err = addb_gmap_array_n(it->it_gmap, it->it_gmap_source, &n);
  if (err != 0) return err;

  if (!pdb_vip_is_endpoint_id(pdb, it->it_gmap_source, linkage, qualifier)) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_gmap_to_vip: "
           "pdb_vip_is_endpoint says no");
    return PDB_ERR_ALREADY;
  }

  err = pdb_vip_id_count(pdb, it->it_gmap_source, linkage, qualifier,
                         it->it_low, it->it_high, PDB_COUNT_UNBOUNDED, &n2);
  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_vip_id_count", err,
                 "can't count vip map for %s(%llx)+%s",
                 pdb_linkage_to_string(linkage),
                 (unsigned long long)it->it_gmap_source,
                 graph_guid_to_string(qualifier, buf, sizeof buf));
    return err;
  }

  if (n2 >= n) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_gmap_to_vip: "
           "vip count %llu >= plain count %llu",
           (unsigned long long)n2, (unsigned long long)n);
    return PDB_ERR_ALREADY;
  }

  err = pdb_vip_id_iterator(pdb, it->it_gmap_source, linkage, qualifier,
                            it->it_low, it->it_high, it->it_forward,
                            /* error-if-null */ false, it_out);
  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_vip_id_iterator", err,
                 "can't create vip map");
    return err;
  }

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "pdb_iterator_gmap_to_vip: shrunk "
         "gmap:%s(%llx) to smaller vip map "
         "%s(%llx)+%s",
         pdb_linkage_to_string(linkage), (unsigned long long)it->it_gmap_source,
         pdb_linkage_to_string(linkage), (unsigned long long)it->it_gmap_source,
         graph_guid_to_string(qualifier, buf, sizeof buf));

  return 0;
}

/*  A GMAP check for the purposes of verifying
 *  that an ID exists in the GMAP tables.
 */
int pdb_iterator_gmap_verify_check(pdb_handle *pdb, pdb_iterator *it, pdb_id id,
                                   pdb_budget *budget_inout) {
  int err;
  addb_id found_id;
  unsigned long long off;

  if (it->it_type != &pdb_iterator_gmap)
    return pdb_iterator_check(pdb, it, id, budget_inout);

  if (id < it->it_low || id >= it->it_high) {
    *budget_inout -= PDB_COST_FUNCTION_CALL;
    return PDB_ERR_NO;
  }

  cl_assert(pdb->pdb_cl, id < (1ull << 34));
  *budget_inout -= pdb_iterator_check_cost(pdb, it);

  err = addb_idarray_search(&gmap_ida(it), it->it_gmap_start, it->it_gmap_end,
                            id, &off, &found_id);
  if (err == PDB_ERR_NO) return err;

  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_idarray_search", err,
                 "can't search %s(%llx) for %llx",
                 pdb_linkage_to_string(it->it_gmap_linkage),
                 (unsigned long long)it->it_gmap_source,
                 (unsigned long long)id);
    return err;
  }

  return id == found_id && off < it->it_gmap_end ? 0 : PDB_ERR_NO;
}
