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

#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

#define IS_LIT(s, e, lit)          \
  ((e) - (s) == sizeof(lit) - 1 && \
   strncasecmp((s), (lit), sizeof(lit) - 1) == 0)

/* use these, don't duplicate the key. */
#define hmap_key(it) ((it)->it_original->it_hmap_key)
#define hmap_key_len(it) ((it)->it_original->it_hmap_key_len)
#define hmap_ida(it) ((it)->it_original->it_hmap_ida)

#define OFFSET_PDB_TO_IDARRAY(pdb, it, off)                 \
  ((it)->it_hmap_start + (pdb_iterator_forward((pdb), (it)) \
                              ? (off)                       \
                              : (pdb_iterator_n((pdb), (it)) - 1) - (off)))

#define OFFSET_IDARRAY_TO_PDB(pdb, it, off) \
  (pdb_iterator_forward((pdb), (it))        \
       ? (off) - (it)->it_hmap_start        \
       : (pdb_iterator_n((pdb), (it)) - 1) - ((off) - (it)->it_hmap_start))

static addb_hmap *pdb_hmap_by_name(pdb_handle *pdb, char const *s,
                                   char const *e) {
  if (IS_LIT(s, e, "pool")) return pdb->pdb_hmap;

  return 0;
}

static char const *pdb_hmap_to_name(pdb_handle *pdb, addb_hmap *hmap) {
  if (hmap == pdb->pdb_hmap) return "pool";
  return NULL;
}

static pdb_hash_type pdb_hmap_type_by_name(pdb_handle *pdb, char const *s,
                                           char const *e) {
  if (IS_LIT(s, e, "name")) return PDB_HASH_NAME;

  if (IS_LIT(s, e, "value")) return PDB_HASH_VALUE;

  if (IS_LIT(s, e, "word")) return PDB_HASH_WORD;

  if (IS_LIT(s, e, "typeguid")) return PDB_HASH_TYPEGUID;

  if (IS_LIT(s, e, "scope")) return PDB_HASH_SCOPE;

  if (IS_LIT(s, e, "vip")) return PDB_HASH_VIP;

  if (IS_LIT(s, e, "key")) return PDB_HASH_KEY;

  if (IS_LIT(s, e, "gen")) return PDB_HASH_GEN;

  if (IS_LIT(s, e, "prefix")) return PDB_HASH_PREFIX;
  if (IS_LIT(s, e, "bin")) return PDB_HASH_BIN;

  return PDB_HASH_LAST;
}

static char const *pdb_hmap_type_to_name(pdb_handle *pdb, pdb_hash_type type) {
  return pdb_hash_type_to_string(type);
}

static void pdb_iterator_hmap_finish(pdb_handle *pdb, pdb_iterator *it) {
  PDB_IS_FINISHING_ITERATOR(pdb->pdb_cl, it);
  cl_assert(pdb->pdb_cl, it->it_hmap);

  /*  Only in the original
   */
  if (it->it_original == it && !it->it_suspended)
    addb_idarray_finish(&it->it_hmap_ida);

  if (it->it_hmap_key != NULL) {
    cm_free(pdb->pdb_cm, it->it_hmap_key);
    it->it_hmap_key = NULL;
  }
  if (it->it_displayname != NULL) {
    cm_free(pdb->pdb_cm, it->it_displayname);
    it->it_displayname = NULL;
  }
  it->it_type = 0;
  it->it_magic = 0;
}

static int pdb_iterator_hmap_reset(pdb_handle *pdb, pdb_iterator *it) {
  PDB_IS_ITERATOR(pdb->pdb_cl, it);
  cl_assert(pdb->pdb_cl, it->it_hmap != NULL);

  it->it_hmap_offset = 0;
  it->it_has_position = true;

  return 0;
}

static int pdb_iterator_hmap_clone(pdb_handle *pdb, pdb_iterator *it,
                                   pdb_iterator **it_out) {
  pdb_iterator *it_orig = it->it_original;
  int err;

  PDB_IS_ITERATOR(pdb->pdb_cl, it);
  PDB_IS_ORIGINAL_ITERATOR(pdb->pdb_cl, it_orig);

  cl_assert(pdb->pdb_cl, !it_orig->it_suspended);

  if ((err = pdb_iterator_make_clone(pdb, it_orig, it_out)) != 0) return err;

  /*  Only the original's hmap_ida is live.  Zero out this one.
   *  Similarly, only the original has a live key.
   */
  memset(&(*it_out)->it_hmap_ida, 0, sizeof((*it_out)->it_hmap_ida));
  (*it_out)->it_hmap_key = NULL;
  (*it_out)->it_hmap_key_len = 0;

  if (!pdb_iterator_has_position(pdb, it)) {
    err = pdb_iterator_hmap_reset(pdb, *it_out);
    cl_assert(pdb->pdb_cl, err == 0);
  } else {
    (*it_out)->it_hmap_offset = it->it_hmap_offset;
    (*it_out)->it_has_position = true;
  }
  cl_assert(pdb->pdb_cl, pdb_iterator_has_position(pdb, *it_out));

  pdb_rxs_log(pdb, "CLONE %p hmap %p", (void *)it, (void *)*it_out);

  return 0;
}

/* Print hmap:mapname:typename:key:hash:low-high:offset
 *  	hmap: [~] LOW[-HIGH] :
 */
static int pdb_iterator_hmap_freeze(pdb_handle *pdb, pdb_iterator *it,
                                    unsigned int flags, cm_buffer *buf) {
  int err;
  char const *sep = "";

  if (flags & PDB_ITERATOR_FREEZE_SET) {
    /*  HMAP: [~] LOW[-HIGH] : map : type : hash : key
     */
    err = pdb_iterator_freeze_intro(buf, it, "hmap");
    if (err != 0) return err;

    err = cm_buffer_sprintf(buf, ":%s:%s:%llu:",
                            pdb_hmap_to_name(pdb, it->it_hmap),
                            pdb_hmap_type_to_name(pdb, it->it_hmap_type),
                            (unsigned long long)it->it_hmap_hash_of_key);
    if (err != 0) return err;

    err = pdb_iterator_freeze_ordering(pdb, buf, it);
    if (err) return err;

    err = pdb_iterator_freeze_account(pdb, buf, it);
    if (err) return err;

    err = pdb_xx_encode(pdb, hmap_key(it), hmap_key_len(it), buf);
    if (err != 0) return err;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    err = cm_buffer_sprintf(buf, "%s%llu", sep,
                            (unsigned long long)it->it_hmap_offset);
    if (err != 0) return err;
    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_STATE) {
    err = cm_buffer_add_string(buf, sep);
    if (err != 0) return err;
  }
  return 0;
}

static int pdb_iterator_hmap_next_loc(pdb_handle *pdb, pdb_iterator *it,
                                      pdb_id *id_out, pdb_budget *budget_inout,
                                      char const *file, int line) {
  int err;
  unsigned long long off;

  PDB_IS_ITERATOR(pdb->pdb_cl, it);
  cl_assert(pdb->pdb_cl, it->it_hmap != NULL);
  cl_assert(pdb->pdb_cl, id_out != NULL);

  *budget_inout -= PDB_COST_FUNCTION_CALL + PDB_COST_GMAP_ELEMENT;
  pdb_iterator_account_charge(pdb, it, next, 1,
                              pdb_iterator_next_cost(pdb, it));

  /*  Read the item at the current offset.
   *
   *  If we're backwards, the actual offset is
   *  the inverse of the real offset.
   */
  if (it->it_hmap_offset >= pdb_iterator_n(pdb, it)) {
    pdb_rxs_log(pdb, "NEXT %p hmap done", (void *)it);
    return PDB_ERR_NO;
  }

  off = OFFSET_PDB_TO_IDARRAY(pdb, it, it->it_hmap_offset);

  /*  We pulled low and high end of the IDARRAY
   *  at create time, and can answer questions about
   *  them without going to the database.
   */
  if (off == it->it_hmap_start)
    *id_out = it->it_low;
  else if (off + 1 == it->it_hmap_end)
    *id_out = it->it_high - 1;
  else {
    cl_assert(pdb->pdb_cl, !it->it_suspended);
    err = addb_idarray_read1(&hmap_ida(it), off, id_out);
    if (err != 0) {
      cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "pdb_iterator_hmap_next [%llu]: %s",
             (unsigned long long)it->it_hmap_offset, strerror(err));
      return err;
    }
  }
  it->it_hmap_offset++;

  pdb_rxs_log(pdb, "NEXT %p hmap %llx ($%lld)", (void *)it,
              (unsigned long long)*id_out,
              (long long)(PDB_COST_FUNCTION_CALL + PDB_COST_GMAP_ELEMENT));
  return 0;
}

static int pdb_iterator_hmap_find_loc(pdb_handle *pdb, pdb_iterator *it,
                                      pdb_id id_in, pdb_id *id_out,
                                      pdb_budget *budget_inout,
                                      char const *file, int line) {
  int err;
  unsigned long long off;
  pdb_budget budget_in = *budget_inout;
  pdb_id id = id_in, id_found;

  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  cl_assert(pdb->pdb_cl, it->it_hmap != NULL);
  cl_assert(pdb->pdb_cl, pdb_iterator_statistics_done(pdb, it));
  cl_assert(pdb->pdb_cl, id_in < (1ull << 34));

  *budget_inout -= pdb_iterator_find_cost(pdb, it);
  pdb_iterator_account_charge(pdb, it, find, 1,
                              pdb_iterator_find_cost(pdb, it));

  /*  Move the ID pointer into the low...high range
   *  from the side that the iterator direction
   *  indicates.
   */
  if (pdb_iterator_forward(pdb, it)) {
    if (id < it->it_low) {
      it->it_hmap_offset = 0;
      id = it->it_low;

      goto done;
    }
  } else {
    if (it->it_high != PDB_ITERATOR_HIGH_ANY && it->it_high <= id_in) {
      cl_assert(pdb->pdb_cl, it->it_low < it->it_high);

      /*  We *do* know the highest
       *  element in the actual iterator;
       *  that's it->it_hmap_last.
       */

      id = it->it_hmap_last;
      it->it_hmap_offset = 0;

      cl_assert(pdb->pdb_cl, id >= it->it_low);

      cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
             "pdb_iterator_hmap_find_loc: "
             "%llx, slipped back from >= high",
             (unsigned long long)*id_out);
      goto done;
    }
  }

  /* XXX this should be O(log N), not 1 */
  pdb->pdb_runtime_statistics.rts_index_extents_read++;
  pdb->pdb_runtime_statistics.rts_index_elements_read++;

  /*  Find id_in or larger in the array.
   */
  cl_assert(pdb->pdb_cl, id < (1ull << 34));
  cl_assert(pdb->pdb_cl, !it->it_original->it_suspended);

  err = addb_idarray_search(&hmap_ida(it), it->it_hmap_start, it->it_hmap_end,
                            id, &off, &id_found);
  if (err != 0) {
    /*  This error is a system error,
     *  not a "we ran out of data" error.
     */
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
           "pdb_iterator_hmap_find %llx -> %llx: %s", (unsigned long long)id_in,
           (unsigned long long)id, strerror(err));
    return err;
  }

  /*  Running off the high end?
   */
  if (off >= it->it_hmap_end) {
    cl_assert(pdb->pdb_cl, off == it->it_hmap_end);

    if (pdb_iterator_forward(pdb, it)) {
      it->it_hmap_offset = pdb_iterator_n(pdb, it);
      pdb_rxs_log(pdb, "FIND %p hmap %llx done ($%lld)", (void *)it,
                  (unsigned long long)id_in,
                  (long long)(budget_in - *budget_inout));
      return PDB_ERR_NO;
    }

    it->it_hmap_offset = 0;
    id = it->it_hmap_last;
    goto done;
  }

  it->it_hmap_offset = OFFSET_IDARRAY_TO_PDB(pdb, it, off);

  /*  Found it?
   */
  if (id_found == id) goto done;

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
  it->it_hmap_offset++;
  if (it->it_hmap_offset >= pdb_iterator_n(pdb, it)) {
    /*  We were already at the upper end of the scale.
     */
    it->it_hmap_offset = pdb_iterator_n(pdb, it);

    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_hmap_find_loc: "
           "slipped backwards out of range");
    pdb_rxs_log(pdb, "FIND %p hmap %llx done ($%lld)", (void *)it,
                (unsigned long long)id_in,
                (long long)(budget_in - *budget_inout));

    return PDB_ERR_NO;
  }

  /*  Read the item just before what addb_idarray_search returned.
   */
  off = OFFSET_PDB_TO_IDARRAY(pdb, it, it->it_hmap_offset);
  cl_assert(pdb->pdb_cl, !it->it_original->it_suspended);
  err = addb_idarray_read1(&hmap_ida(it), off, &id);
  if (err != 0) {
    /*  This error is a system error,
     *  not a "we ran out of data" error.
     */
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
           "pdb_iterator_hmap_find %llx -> %llx: %s", (unsigned long long)id_in,
           (unsigned long long)id, strerror(err));

    return err;
  }

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "pdb_iterator_hmap_find_loc: "
         "slipped backwards to %llx at offset %llu",
         (unsigned long long)id_in, it->it_hmap_offset - 1);

done:
  *id_out = id;
  it->it_hmap_offset++;
  cl_assert(pdb->pdb_cl, *id_out >= it->it_low);
  cl_assert(pdb->pdb_cl, *id_out < it->it_high);

  pdb_rxs_log(pdb, "FIND %p hmap %llx -> %llx ($%lld)", (void *)it,
              (unsigned long long)id_in, (unsigned long long)*id_out,
              (long long)(budget_in - *budget_inout));
  return 0;
}

static char const *pdb_iterator_hmap_to_string(pdb_handle *pdb,
                                               pdb_iterator *it, char *buf,
                                               size_t size) {
  if (it->it_original == NULL) {
    snprintf(buf, size, "[unlinked hmap clone %p]", (void *)it);
    return buf;
  }
  snprintf(buf, size, "%shmap:%s(%lx:%.*s)", it->it_forward ? "" : "~",
           pdb_hmap_type_to_name(pdb, it->it_hmap_type),
           (unsigned long)it->it_hmap_hash_of_key, (int)hmap_key_len(it),
           hmap_key(it));

  it->it_displayname = cm_strmalcpy(pdb->pdb_cm, buf);
  return buf;
}

static int pdb_iterator_hmap_check(pdb_handle *pdb, pdb_iterator *it, pdb_id id,
                                   pdb_budget *budget_inout) {
  int err;
  addb_id found_id;
  unsigned long long off;

  if (id < it->it_low || id >= it->it_high) {
    *budget_inout -= PDB_COST_FUNCTION_CALL;
    pdb_iterator_account_charge(pdb, it, check, 1, PDB_COST_FUNCTION_CALL);
    pdb_rxs_log(pdb, "CHECK %p hmap %llx no ($%lld)", (void *)it,
                (unsigned long long)id, (long long)PDB_COST_FUNCTION_CALL);
    return PDB_ERR_NO;
  }

  pdb_iterator_account_charge(pdb, it, check, 1,
                              pdb_iterator_check_cost(pdb, it));
  *budget_inout -= pdb_iterator_check_cost(pdb, it);

  cl_assert(pdb->pdb_cl, id <= (1ull << 34));
  cl_assert(pdb->pdb_cl, it->it_hmap_start <= it->it_hmap_end);
  cl_assert(pdb->pdb_cl, !it->it_original->it_suspended);
  err = addb_idarray_search(&hmap_ida(it), it->it_hmap_start, it->it_hmap_end,
                            id, &off, &found_id);
  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_idarray_search", err,
                 "can't search for %llx", (unsigned long long)id);
    return err;
  }

  err = (id == found_id && off < it->it_hmap_end ? 0 : PDB_ERR_NO);
  pdb_rxs_log(pdb, "CHECK %p hmap %llx %s ($%lld)", (void *)it,
              (unsigned long long)id, err == 0 ? "yes" : "no",
              (long long)(PDB_COST_GMAP_ARRAY + PDB_COST_GMAP_ELEMENT));
  return err;
}

/**
 * @brief Return the idarray for an HMAP iterator.
 *
 * @param pdb		module handle
 * @param it		a gmap iterator
 * @param id_out	out: idarray
 * @param s_out		out: first index
 * @param e_out		out: first index not included
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int pdb_iterator_hmap_idarray(pdb_handle *pdb, pdb_iterator *it,
                                     addb_idarray **ida_out,
                                     unsigned long long *s_out,
                                     unsigned long long *e_out) {
  cl_assert(pdb->pdb_cl, !it->it_original->it_suspended);
  *ida_out = &hmap_ida(it);
  *s_out = it->it_hmap_start;
  *e_out = it->it_hmap_end;

  return 0;
}

/**
 * @brief Return the primitive summary for an
 *	  HMAP iterator.  (The summary being
 *	  "I don't summarize".)
 *
 * @param pdb		module handle
 * @param it		an hmap iterator
 * @param psum_out	out: summary
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int pdb_iterator_hmap_primitive_summary(
    pdb_handle *pdb, pdb_iterator *it, pdb_primitive_summary *psum_out) {
  if (it->it_hmap_type == addb_hmt_vip)
    return pdb_vip_hmap_primitive_summary(pdb, it->it_hmap_key,
                                          it->it_hmap_key_len, psum_out);

  if (it->it_hmap_type != addb_hmt_value) return PDB_ERR_NO;

  psum_out->psum_locked = 0;
  psum_out->psum_complete = false;
  psum_out->psum_result = PDB_LINKAGE_N;

  return 0;
}

/**
 * @brief Has this iterator gone beyond this value?
 *
 * @param pdb		module handle
 * @param it		iterator we're asking about
 * @param s		start of comparison value
 * @param e		end of comparison value
 * @param beyond_out	out: true if we're safely beyond this value.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int pdb_iterator_hmap_beyond(pdb_handle *pdb, pdb_iterator *it,
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

  if (it->it_hmap_offset == 0) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_hmap_beyond: "
           "still at the beginning; it=%p, id=%llx",
           (void *)it, (unsigned long long)id);
    *beyond_out = false;
    return 0;
  }
  off = OFFSET_PDB_TO_IDARRAY(pdb, it, it->it_hmap_offset - 1);

  /*  We pulled low and high end of the IDARRAY
   *  at create time, and can answer questions about
   *  them without going to the database.
   */
  if (off == it->it_hmap_start)
    last_id = it->it_low;
  else if (off + 1 == it->it_hmap_end)
    last_id = it->it_high - 1;
  else {
    cl_assert(pdb->pdb_cl, !it->it_original->it_suspended);
    err = addb_idarray_read1(&hmap_ida(it), off, &last_id);
    if (err != 0) {
      cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "pdb_iterator_beyond [%llu]: %s",
             (unsigned long long)it->it_hmap_offset, strerror(err));
      return err;
    }
  }

  *beyond_out = (pdb_iterator_forward(pdb, it) ? id < last_id : id > last_id);

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "pdb_iterator_hmap_beyond: %llx vs. last_id %llx in %s: %s",
         (unsigned long long)id, (unsigned long long)last_id,
         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         *beyond_out ? "yes" : "no");
  return 0;
}

static int pdb_iterator_hmap_range_estimate(pdb_handle *pdb, pdb_iterator *it,
                                            pdb_range_estimate *range) {
  pdb_id id, off;
  int err;

  pdb_iterator_range_estimate_default(pdb, it, range);

  if (it->it_hmap_offset == 0) {
    range->range_n_max = range->range_n_exact = pdb_iterator_n(pdb, it);

    return 0;
  }

  if (it->it_hmap_offset >= pdb_iterator_n(pdb, it)) {
    range->range_n_max = range->range_n_exact = 0;
    if (pdb_iterator_forward(pdb, it))
      range->range_low = range->range_high;
    else
      range->range_high = range->range_low;
    return 0;
  }

  off = OFFSET_PDB_TO_IDARRAY(pdb, it, it->it_hmap_offset);
  if (off == it->it_hmap_start)
    id = it->it_low;
  else if (off + 1 == it->it_hmap_end)
    id = it->it_high - 1;
  else {
    cl_assert(pdb->pdb_cl, !it->it_original->it_suspended);
    err = addb_idarray_read1(&hmap_ida(it), off, &id);
    if (err != 0) {
      char buf[200];
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_idarray_read1", err,
                   "off=%llu, it=%s", (unsigned long long)off,
                   pdb_iterator_to_string(pdb, it, buf, sizeof buf));
      return err;
    }
  }
  if (pdb_iterator_forward(pdb, it))
    range->range_low = id;
  else
    range->range_high = id + 1;

  range->range_n_exact = range->range_n_max =
      pdb_iterator_n(pdb, it) - (it->it_hmap_start + it->it_hmap_offset);

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "pdb_iterator_hmap_range_estimate: %llx%s..%llx%s", range->range_low,
         range->range_low_rising ? " and rising" : "", range->range_high,
         range->range_high_falling ? " and falling" : "");

  return 0;
}

/* Suspend access to the database.
 */
static int pdb_iterator_hmap_suspend(pdb_handle *pdb, pdb_iterator *it) {
  if (it->it_original == it) addb_idarray_finish(&it->it_hmap_ida);
  return 0;
}

/* Resume access to the database.
 */
static int pdb_iterator_hmap_unsuspend(pdb_handle *pdb, pdb_iterator *it) {
  int err;

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
  cl_assert(pdb->pdb_cl, it->it_hmap_ida.ida_tref == (size_t)-1);

  err = addb_hmap_idarray(it->it_hmap, it->it_hmap_hash_of_key, it->it_hmap_key,
                          it->it_hmap_key_len, it->it_hmap_type,
                          &it->it_hmap_ida);
  if (err != 0) {
    if (err == PDB_ERR_NO) return pdb_iterator_null_become(pdb, it);

    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_hmap_idarray", err,
                 "type=%d", it->it_hmap_type);
    return err;
  }
  return 0;
}

const pdb_iterator_type pdb_iterator_hmap = {
    "hmap",
    pdb_iterator_hmap_finish,
    pdb_iterator_hmap_reset,
    pdb_iterator_hmap_clone,
    pdb_iterator_hmap_freeze,
    pdb_iterator_hmap_to_string,

    pdb_iterator_hmap_next_loc,
    pdb_iterator_hmap_find_loc,
    pdb_iterator_hmap_check,
    pdb_iterator_util_statistics_none,

    pdb_iterator_hmap_idarray,
    pdb_iterator_hmap_primitive_summary,
    pdb_iterator_hmap_beyond,
    pdb_iterator_hmap_range_estimate,

    /* restrict */ NULL,

    pdb_iterator_hmap_suspend,
    pdb_iterator_hmap_unsuspend};

/*
 * @brief Create an iterator over the values in an hmap entry.
 *
 * @param pdb		module handle
 * @param hmap		HMAP the value is in
 * @param hash_of_key	hash to index it with
 * @param key 		key the hash comes from
 * @param key_len	number of bytes pointed to by key
 * @param type		type of the key
 * @param low		first included value
 * @param high		first value that isn't included
 * @param forward	true if we're iterating from low to high.
 * @param error_if_null	don't bother creating null iterators
 * @param it_out	return the new iterator here.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_iterator_hmap_create(pdb_handle *pdb, addb_hmap *hmap,
                             addb_hmap_id hash_of_key, char const *const key,
                             size_t key_len, addb_hmap_type type, pdb_id low,
                             pdb_id high, bool forward, bool error_if_null,
                             pdb_iterator **it_out) {
  addb_idarray ida;
  int err = 0;
  unsigned long long n, upper_bounds;
  unsigned long long start, end;
  pdb_iterator *it;
  cm_handle *cm = pdb->pdb_cm;
  addb_id last;

  *it_out = 0;

  if (pdb->pdb_primitive == NULL &&
      ((err = pdb_initialize(pdb)) != 0 ||
       (err = pdb_initialize_checkpoint(pdb)) != 0)) {
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_initialize fails: %s",
           strerror(err));
    return err;
  }
  upper_bounds = pdb_primitive_n(pdb);
  if (low >= (high != PDB_ITERATOR_HIGH_ANY ? high : upper_bounds)) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_hmap_create: low=%llx >= high=%llx/%llx, "
           "returning null iterator",
           (long long)low, (long long)high, (unsigned long long)upper_bounds);
    return error_if_null ? PDB_ERR_NO : pdb_iterator_null_create(pdb, it_out);
  }

  err = addb_hmap_idarray(hmap, hash_of_key, key, key_len, type, &ida);
  if (err != 0) {
    if (err == PDB_ERR_NO)
      return error_if_null ? PDB_ERR_NO : pdb_iterator_null_create(pdb, it_out);

    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_hmap_idarray", err,
                 "type=%d", type);
    return err;
  }

  /*  Determine start offset and true low.
   */

  /*  Find the lowest element we've been given (or the
   *  first higher one that actually exists), and remember
   *  that offset.
   */
  if (low == 0) {
    err = addb_idarray_read1(&ida, 0, &low);
    if (err != 0) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_idarray_read1", err,
                   "off=0");

      addb_idarray_finish(&ida);
      return err;
    }
    start = 0;
  } else {
    cl_assert(pdb->pdb_cl, low <= (1ull << 34));
    cl_assert(pdb->pdb_cl, 0 <= addb_idarray_n(&ida));
    err = addb_idarray_search(&ida, 0, addb_idarray_n(&ida), low, &start, &low);
    if (err != 0) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_idarray_search", err,
                   "%llx", (unsigned long long)low);

      addb_idarray_finish(&ida);
      return err;
    }
  }

  /*  Do we have enough information to throw this out yet?
   *  If yes, stop wasting our time with measurements and just
   *  return a null iterator.
   */
  if (high != PDB_ITERATOR_HIGH_ANY && low >= high) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_hmap_create: adjusted low=%llx "
           ">= high=%llx, returning null iterator",
           (unsigned long long)low, (unsigned long long)high);

  null:
    addb_idarray_finish(&ida);
    return error_if_null ? PDB_ERR_NO : pdb_iterator_null_create(pdb, it_out);
  }

  /*  Determine end offset, last, and with it the true high (last + 1)
   */
  if (high == PDB_ITERATOR_HIGH_ANY) {
    /* Find the last element. */

    end = addb_idarray_n(&ida);
    if (end <= start) {
      cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
             "pdb_iterator_hmap_create: 0 elements; "
             "returning null iterator");
      goto null;
    }
    err = addb_idarray_read1(&ida, end - 1, &last);
    if (err != 0) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_idarray_read1", err,
                   "[%llu]", end - 1);

    err:
      addb_idarray_finish(&ida);
      return err;
    }
  } else {
    cl_assert(pdb->pdb_cl, high > 0);
    cl_assert(pdb->pdb_cl, high <= (1ull << 34));

    /*  Find the end element we've been given, and remember
     *  that offset.
     */
    cl_assert(pdb->pdb_cl, start <= addb_idarray_n(&ida));
    err = addb_idarray_search(&ida, start, addb_idarray_n(&ida), high - 1, &end,
                              &last);
    if (err != 0) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_idarray_search", err,
                   "%llx", (unsigned long long)high - 1);
      goto err;
    }

    /*  At the end of the next "if", "end" is the end offset --
     *  the first one *not* included.
     */
    if (last == high - 1 && end < addb_idarray_n(&ida))
      end++;
    else {
      /* We slipped forwards.  The last included element
       * is the one *before* this one.  Read that value.
       */
      if (end == 0) {
        cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
               "pdb_iterator_hmap_create: no "
               "elements between %llx and @%llu; "
               "returning null iterator",
               (unsigned long long)start, (unsigned long long)high - 1);
        goto null;
      }

      err = addb_idarray_read1(&ida, end - 1, &last);
      if (err != 0) {
        cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_idarray_read1", err,
                     "[%llu]", end);
        goto err;
      }
    }
  }
  high = last + 1; /* First not included */

  /*  Do we still think there are elements in this collection?
   */
  if (low >= high) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_hmap_create: adjusted low %llx "
           ">= adjusted high %llx: returning null iterator",
           (unsigned long long)low, (unsigned long long)high);
    goto null;
  }

  cl_assert(pdb->pdb_cl, start < end);

  pdb->pdb_runtime_statistics.rts_index_extents_read++;
  n = end - start;
  cl_assert(pdb->pdb_cl, n > 0);

  if ((it = *it_out = cm_malloc(cm, sizeof(*it))) == NULL) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "malloc", errno,
                 "can't malloc hmap iterator?");
    addb_idarray_finish(&ida);
    return ENOMEM;
  }
  memset(it, 0, sizeof(*it));
  pdb_iterator_make(pdb, it, low, high, forward);

  it->it_type = &pdb_iterator_hmap;
  it->it_hmap = hmap;
  it->it_hmap_hash_of_key = hash_of_key;

  it->it_hmap_key_len = key_len;
  it->it_hmap_type = type;

  it->it_hmap_ida = ida;
  it->it_hmap_last = last;
  it->it_hmap_end = end;
  it->it_hmap_start = start;
  it->it_low = low;
  it->it_high = high;

  if ((it->it_hmap_key = cm_malcpy(cm, key, key_len)) == NULL) {
    err = errno ? errno : ENOMEM;
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "cm_malcpy", err,
                 "can't copy key");

    addb_idarray_finish(&ida);
    addb_idarray_initialize(&it->it_hmap_ida);
    pdb_iterator_destroy(pdb, it_out);
    return err;
  }

  pdb->pdb_runtime_statistics.rts_index_extents_read++;

  pdb_iterator_sorted_set(pdb, it, true);
  pdb_iterator_n_set(pdb, it, n);
  pdb_iterator_check_cost_set(
      pdb, it, PDB_COST_FUNCTION_CALL +
                   pdb_iterator_bsearch_cost(it->it_n, 32 * 1024 / 5,
                                             PDB_COST_HMAP_ARRAY,
                                             PDB_COST_HMAP_ELEMENT));

  pdb_iterator_next_cost_set(pdb, it,
                             PDB_COST_FUNCTION_CALL + PDB_COST_HMAP_ELEMENT);
  pdb_iterator_find_cost_set(pdb, it, pdb_iterator_check_cost(pdb, it));
  pdb_iterator_statistics_done_set(pdb, it);

  cl_assert(pdb->pdb_cl, !it->it_suspended);
  pdb_iterator_suspend_chain_in(pdb, it);

  pdb_rxs_log(pdb, "CREATE %p hmap %llx %llx %s", (void *)it,
              (unsigned long long)low, (unsigned long long)high,
              forward ? "forward" : "backward");

  {
    char buf[200];
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_hmap_create: %p/%s %llx..%llx[%llu]", (void *)it,
           pdb_iterator_to_string(pdb, it, buf, sizeof buf),
           (long long)it->it_low, (long long)it->it_high, n);
  }
  return 0;
}

int pdb_iterator_hmap_thaw(pdb_handle *pdb, pdb_iterator_text const *pit,
                           pdb_iterator_base *pib, pdb_iterator **it_out) {
  int err;
  addb_hmap *hmap;
  unsigned long long hash_of_key, low, high, off = 0;
  char const *key_s, *key_e, *type_s, *type_e, *map_s, *map_e;
  cm_buffer key_buf;
  pdb_hash_type type;
  bool forward;
  char const *s, *e;
  char const *ordering = NULL;
  pdb_iterator_account *acc = NULL;

  cl_cover(pdb->pdb_cl);
  cm_buffer_initialize(&key_buf, pdb->pdb_cm);

  s = pit->pit_set_s;
  e = pit->pit_set_e;

  /* Note placement of optional section before the
   * encoded VIP text.
   */
  err = pdb_iterator_util_thaw(
      pdb, &s, e,
      "%{forward}%{low[-high]}:%{bytes}:%{bytes}:"
      "%llu:%{ordering}%{account}%{extensions}%{bytes}",
      &forward, &low, &high, &map_s, &map_e, &type_s, &type_e, &hash_of_key,
      pib, &ordering, pib, &acc, (pdb_iterator_property *)NULL, &key_s, &key_e);
  if (err != 0) return err;

  if ((hmap = pdb_hmap_by_name(pdb, map_s, map_e)) == NULL) {
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
           "pdb_iterator_hmap_thaw: expected hmap name, "
           "got \"%.*s\"",
           (int)(map_e - map_s), map_s);
    return PDB_ERR_SYNTAX;
  }
  type = pdb_hmap_type_by_name(pdb, type_s, type_e);
  if (type == PDB_HASH_LAST) {
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
           "pdb_iterator_hmap_thaw: expected hmap type, "
           "got \"%.*s\"",
           (int)(type_e - type_s), type_s);
    return PDB_ERR_SYNTAX;
  }
  if ((err = pdb_xx_decode(pdb, key_s, key_e, &key_buf)) != 0) {
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
           "pdb_iterator_hmap_thaw: expected hash key, got "
           "\"%.*s\"",
           (int)(key_e - key_s), key_s);
    return PDB_ERR_SYNTAX;
  }

  if (s != e) {
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
           "pdb_iterator_hmap_thaw: trailing text \"%.*s\"", (int)(e - s), s);
    cm_buffer_finish(&key_buf);
    return PDB_ERR_SYNTAX;
  }

  if ((s = pit->pit_position_s) != NULL) {
    e = pit->pit_position_e;
    if ((err = pdb_scan_ull(&s, e, &off)) != 0) {
      cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
             "pdb_iterator_hmap_thaw: "
             "expected offset, got "
             "\"%.*s\"",
             (int)(e - s), s);
      cm_buffer_finish(&key_buf);
      return PDB_ERR_SYNTAX;
    }
    err = pdb_iterator_util_thaw(pdb, &s, e, "%{extensions}%{end}",
                                 (pdb_iterator_property *)NULL);
    if (err != 0) {
      cm_buffer_finish(&key_buf);
      return err;
    }
  }
  if ((s = pit->pit_state_s) != NULL) {
    e = pit->pit_state_e;
    err = pdb_iterator_util_thaw(pdb, &s, e, "%{extensions}%{end}",
                                 (pdb_iterator_property *)NULL);
    if (err != 0) {
      cm_buffer_finish(&key_buf);
      return err;
    }
  }

  err = pdb_iterator_hmap_create(
      pdb, hmap, hash_of_key, cm_buffer_memory(&key_buf),
      cm_buffer_length(&key_buf), type, low, high, forward,
      /* error-if-null */ false, it_out);
  cm_buffer_finish(&key_buf);

  if (err == 0) {
    (*it_out)->it_hmap_offset = off;

    pdb_iterator_account_set(pdb, *it_out, acc);

    if (ordering != NULL) {
      pdb_iterator_ordering_set(pdb, *it_out, ordering);
      pdb_iterator_ordered_set(pdb, *it_out, true);
    } else {
      pdb_iterator_ordered_set(pdb, *it_out, false);
    }
    pdb_rxs_log(pdb, "THAW %p hmap %llx %llx %s", (void *)*it_out,
                (unsigned long long)low, (unsigned long long)high,
                forward ? "forward" : "backward");
  } else {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_iterator_hmap_create", err,
                 "thawed from \"%.*s\"", (int)(pit->pit_set_e - pit->pit_set_s),
                 pit->pit_set_s);
  }
  return err;
}

bool pdb_iterator_hmap_is_instance(pdb_handle *pdb, pdb_iterator *it,
                                   char const **name_out,
                                   unsigned long long *hash_out,
                                   char const **s_out, char const **e_out) {
  if (it->it_type != &pdb_iterator_hmap) return false;

  *name_out = pdb_hmap_type_to_name(pdb, it->it_hmap_type);
  *hash_out = it->it_hmap_hash_of_key;
  *s_out = hmap_key(it);
  *e_out = *s_out + hmap_key_len(it);

  return true;
}
