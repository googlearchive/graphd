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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*  An "all iterator".
 *
 *  Returns all primitive IDs between low and high,
 *  simply by counting them out.
 *
 *  If it_forward is false, IDs are published as
 *  (it_high + it_low - 1) - id, and thus appear to
 *  count down from it_high - 1 through it_low.
 */

/**
 * @brief access the next primitive in an iteration
 *
 * @param pdb		opaque module handle, created with pdb_create()
 * @param it		iteration to set through
 * @param id_out	assign the pdb_id to this
 * @param budget_inout	budget to use (dinged by PDB_COST_FUNCTION_CALL)
 * @param state_inout	resumable state, unused
 * @param file		added by macro, calling code's source file, unused
 * @param line		added by macro, calling code's source line, unused
 *
 * @return 0 on success, a nonzero error code on error
 * @return PDB_ERR_NO after running out of primitives.
 */
static int pdb_iterator_all_next_loc(pdb_handle *pdb, pdb_iterator *it,
                                     pdb_id *id_out, pdb_budget *budget_inout,
                                     char const *file, int line) {
  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  *budget_inout -= PDB_COST_FUNCTION_CALL;
  pdb_iterator_account_charge(pdb, it, next, 1, PDB_COST_FUNCTION_CALL);

  if (it->it_all_i >= it->it_high) {
    pdb_rxs_log(pdb, "NEXT %p all done ($%lld)", (void *)it,
                (unsigned long long)PDB_COST_FUNCTION_CALL);
    return PDB_ERR_NO;
  }

  *id_out = it->it_forward ? it->it_all_i
                           : ((it->it_high + it->it_low) - 1) - it->it_all_i;
  it->it_all_i++;

  pdb_rxs_log(pdb, "NEXT %p all %llx ($%lld)", (void *)it,
              (unsigned long long)*id_out,
              (unsigned long long)PDB_COST_FUNCTION_CALL);
  return 0;
}

/**
 * @brief Move on or after a specific id in an iteration
 *
 *  Could pdb_iterator_all_next() have returned this primitive?
 *  If yes, pretend it just did; otherwise, advance to the first
 *  primitive this iterator could have returned and set *changed_out.
 *
 * @param pdb		opaque module handle, created with pdb_create()
 * @param it		iteration to set through
 * @param id_in_out	if non-NULL, assign the pdb_id to this
 * @param changed_out	set if changed
 * @param budget_inout	budget to use (dinged by PDB_COST_FUNCTION_CALL)
 * @param file		added by macro, calling code's source file, unused
 * @param line		added by macro, calling code's source line, unused
 *
 * @return 0 on success, a nonzero error code on error
 * @return PDB_ERR_NO after running out of primitives.
 */
static int pdb_iterator_all_find_loc(pdb_handle *pdb, pdb_iterator *it,
                                     pdb_id id_in, pdb_id *id_out,
                                     pdb_budget *budget_inout, char const *file,
                                     int line) {
  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  *budget_inout -= PDB_COST_FUNCTION_CALL;
  pdb_iterator_account_charge(pdb, it, find, 1, PDB_COST_FUNCTION_CALL);

  if (it->it_forward) {
    if (id_in < it->it_low)
      *id_out = it->it_low;

    else if (id_in >= it->it_high) {
      it->it_all_i = it->it_high;
      pdb_rxs_log(pdb, "FIND %p all %llx done ($%lld)", (void *)it,
                  (unsigned long long)id_in, (long long)PDB_COST_FUNCTION_CALL);
      return PDB_ERR_NO;
    } else
      *id_out = id_in;

    it->it_all_i = *id_out;
  } else {
    if (id_in >= it->it_high) {
      if (it->it_high == 0) {
        pdb_rxs_log(pdb, "FIND %p all %llx done ($%lld)", (void *)it,
                    (unsigned long long)id_in,
                    (long long)PDB_COST_FUNCTION_CALL);
        return PDB_ERR_NO;
      }
      *id_out = it->it_high - 1;
    } else if (id_in < it->it_low) {
      it->it_all_i = it->it_high;
      pdb_rxs_log(pdb, "FIND %p all %llx done ($%lld)", (void *)it,
                  (unsigned long long)id_in, (long long)PDB_COST_FUNCTION_CALL);
      return PDB_ERR_NO;
    } else
      *id_out = id_in;

    it->it_all_i = ((it->it_high + it->it_low) - 1) - *id_out;
  }

  it->it_all_i++;
  pdb_rxs_log(pdb, "FIND %p all %llx -> %llx ($%lld)", (void *)it,
              (unsigned long long)id_in, (unsigned long long)*id_out,
              (long long)PDB_COST_FUNCTION_CALL);
  return 0;
}

/*  [~] ALL : LOW [-HIGH] / IT_ALL_I /
 */
static int pdb_iterator_all_freeze(pdb_handle *pdb, pdb_iterator *it,
                                   unsigned int flags, cm_buffer *buf) {
  int err;
  char const *sep = "";
  ptrdiff_t o0 = cm_buffer_length(buf);

  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  cl_assert(pdb->pdb_cl, buf != NULL);
  cl_cover(pdb->pdb_cl);

  if (flags & PDB_ITERATOR_FREEZE_SET) {
    err = pdb_iterator_freeze_intro(buf, it, "all");
    if (err) return err;

    err = pdb_iterator_freeze_ordering(pdb, buf, it);
    if (err) return err;

    err = pdb_iterator_freeze_account(pdb, buf, it);
    if (err) return err;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    err =
        cm_buffer_sprintf(buf, "%s%llu", sep, (unsigned long long)it->it_all_i);
    if (err) return err;
    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_STATE) {
    err = cm_buffer_sprintf(buf, "%s", sep);
    if (err) return err;
  }

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "pdb_iterator_all_freeze: \"%.*s\"",
         (int)(cm_buffer_length(buf) - o0), cm_buffer_memory(buf) + o0);
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
static int pdb_iterator_all_reset(pdb_handle *pdb, pdb_iterator *it) {
  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  cl_cover(pdb->pdb_cl);
  it->it_all_i = it->it_low;

  return 0;
}

static int pdb_iterator_all_clone(pdb_handle *pdb, pdb_iterator *it,
                                  pdb_iterator **it_out) {
  pdb_iterator *it_orig = it->it_original;
  int err;

  PDB_IS_ITERATOR(pdb->pdb_cl, it);
  PDB_IS_ORIGINAL_ITERATOR(pdb->pdb_cl, it_orig);

  if ((err = pdb_iterator_make_clone(pdb, it_orig, it_out)) != 0) return err;

  if (!pdb_iterator_has_position(pdb, it)) {
    err = pdb_iterator_reset(pdb, *it_out);
    cl_assert(pdb->pdb_cl, err == 0);
  } else {
    (*it_out)->it_all_i = it->it_all_i;
    (*it_out)->it_has_position = true;
  }

  pdb_rxs_log(pdb, "CLONE %p all %p", (void *)it, (void *)*it_out);
  cl_assert(pdb->pdb_cl, pdb_iterator_has_position(pdb, *it_out));
  return 0;
}

static char const *pdb_iterator_all_to_string(pdb_handle *pdb, pdb_iterator *it,
                                              char *buf, size_t size) {
  if (pdb_iterator_has_position(pdb, it))
    snprintf(buf, size, "%sall[%llx...%llx: %llx]", it->it_forward ? "" : "~",
             (unsigned long long)it->it_low, (unsigned long long)it->it_high,
             (unsigned long long)it->it_all_i);
  else
    snprintf(buf, size, "%sall[%llx...%llx]", it->it_forward ? "" : "~",
             (unsigned long long)it->it_low, (unsigned long long)it->it_high);

  it->it_displayname = cm_strmalcpy(pdb->pdb_cm, buf);
  return buf;
}

static int pdb_iterator_all_check(pdb_handle *pdb, pdb_iterator *it, pdb_id id,
                                  pdb_budget *budget_inout) {
  *budget_inout -= PDB_COST_FUNCTION_CALL;
  pdb_iterator_account_charge(pdb, it, check, 1, PDB_COST_FUNCTION_CALL);

  pdb_rxs_log(pdb, "CHECK %p all %llx %s ($%lld)", (void *)it,
              (unsigned long long)id,
              (id < it->it_low || id >= it->it_high) ? "no" : "yes",
              (long long)PDB_COST_FUNCTION_CALL);

  return (id < it->it_low || id >= it->it_high) ? PDB_ERR_NO : 0;
}

/**
 * @brief Return the summary for an ALL iterator.
 *
 * @param pdb		module handle
 * @param it		an all iterator
 * @param psum_out	out: summary
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int pdb_iterator_all_primitive_summary(pdb_handle *pdb, pdb_iterator *it,
                                              pdb_primitive_summary *psum_out) {
  /* I do nothing! */

  psum_out->psum_locked = 0;
  psum_out->psum_result = PDB_LINKAGE_N;
  psum_out->psum_complete = true;

  return 0;
}

/**
 * @brief Has this iterator progressed beyond this value?
 *
 * @param pdb		module handle
 * @param it		iterator we're asking about
 * @param s		start of comparison value
 * @param e		end of comparison value
 * @param beyond_out	out: true if the most recently returned
 *			ID from this iterator was greater than
 *			(or, if it runs backward, smaller than)
 *			the parameter ID.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int pdb_iterator_all_beyond(pdb_handle *pdb, pdb_iterator *it,
                                   char const *s, char const *e,
                                   bool *beyond_out) {
  char buf[200];
  pdb_id id, last_id;

  if (e - s != sizeof(id)) {
    *beyond_out = false;
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb_iterator_all_beyond: unexpected "
           "value size (%zu bytes; expected %zu)",
           (size_t)(e - s), sizeof(id));
    return EINVAL;
  }
  memcpy(&id, s, sizeof(id));

  if (it->it_all_i == 0) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_all_beyond: "
           "still at the beginning");
    *beyond_out = false;
    return 0;
  }

  if (pdb_iterator_forward(pdb, it)) {
    last_id = it->it_all_i - 1;
    *beyond_out = id < last_id;
  } else {
    last_id = ((it->it_high + it->it_low) - 1) - (it->it_all_i - 1);
    *beyond_out = id > last_id;
  }

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "pdb_iterator_all_beyond: %llx vs. last_id %llx in %s: %s",
         (unsigned long long)id, (unsigned long long)last_id,
         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         *beyond_out ? "yes" : "no");
  return 0;
}

static int pdb_iterator_all_range_estimate(pdb_handle *pdb, pdb_iterator *it,
                                           pdb_range_estimate *range) {
  pdb_iterator_range_estimate_default(pdb, it, range);

  if (pdb_iterator_forward(pdb, it)) {
    range->range_low = it->it_all_i;
  } else {
    range->range_high = ((it->it_high + it->it_low) - 1) - (it->it_all_i - 1);
  }
  range->range_n_exact = range->range_n_max =
      range->range_high - range->range_low;

  return 0;
}

static const pdb_iterator_type pdb_iterator_all = {
    "all",

    pdb_iterator_util_finish,
    pdb_iterator_all_reset,
    pdb_iterator_all_clone,
    pdb_iterator_all_freeze,
    pdb_iterator_all_to_string,

    pdb_iterator_all_next_loc,
    pdb_iterator_all_find_loc,
    pdb_iterator_all_check,
    pdb_iterator_util_statistics_none,

    /* idarray */ NULL,

    pdb_iterator_all_primitive_summary,
    pdb_iterator_all_beyond,
    pdb_iterator_all_range_estimate,
    /* restrict */ NULL,

    /* suspend */ NULL,
    /* unsuspend  */ NULL};

/**
 * @brief initialize an iterator that returns all records.
 *
 * @param pdb		module handle
 * @param low		lowest included value
 * @param high		highest value that isn't included
 * @param forward	true if we're iterating from low to high.
 * @param it_out	the iterator to initialize
 *
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_iterator_all_create(pdb_handle *pdb, pdb_id low, pdb_id high,
                            bool forward, pdb_iterator **it_out) {
  pdb_iterator *it;
  int err;
  unsigned long long upper_bound;
  char buf[200];

  if (pdb->pdb_primitive == NULL &&
      ((err = pdb_initialize(pdb)) != 0 ||
       (err = pdb_initialize_checkpoint(pdb)) != 0)) {
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_initialize fails: %s",
           strerror(err));
    return err;
  }

  upper_bound = addb_istore_next_id(pdb->pdb_primitive);
  if (high == PDB_ITERATOR_HIGH_ANY || high > upper_bound) high = upper_bound;
  if (low >= high) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_all_initialize: "
           "low %llx >= high %llx -- returning "
           "null iterator",
           (unsigned long long)low, (unsigned long long)high);
    return pdb_iterator_null_create(pdb, it_out);
  }
  cl_assert(pdb->pdb_cl, pdb->pdb_primitive != NULL);

  *it_out = it = cm_malloc(pdb->pdb_cm, sizeof(*it));
  if (it == NULL) return ENOMEM;

  pdb_iterator_make(pdb, it, low, high, forward);

  it->it_type = &pdb_iterator_all;
  it->it_all_i = low;

  pdb_iterator_n_set(pdb, it, high - low);
  pdb_iterator_next_cost_set(pdb, it, PDB_COST_FUNCTION_CALL);
  pdb_iterator_check_cost_set(pdb, it, PDB_COST_FUNCTION_CALL);
  pdb_iterator_find_cost_set(pdb, it, 0);
  pdb_iterator_sorted_set(pdb, it, true);
  pdb_iterator_statistics_done_set(pdb, it);

  pdb_rxs_log(pdb, "CREATE %p all %llx %llx %s", (void *)it,
              (unsigned long long)low, (unsigned long long)high,
              forward ? "forward" : "backward");

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "pdb_iterator_all_create %p = %s",
         (void *)*it_out, pdb_iterator_to_string(pdb, it, buf, sizeof buf));

  return 0;
}

int pdb_iterator_all_thaw(pdb_handle *pdb, pdb_iterator_text const *pit,
                          pdb_iterator_base *pib, pdb_iterator **it_out) {
  unsigned long long low, high, i = 0;
  bool forward;
  int err;
  char const *ordering = NULL;
  pdb_iterator_account *acc = NULL;
  char const *s = pit->pit_set_s, *e = pit->pit_set_e;

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "pdb_iterator_all_thaw: %.*s/%.*s/%.*s",
         (int)(pit->pit_set_e - pit->pit_set_s), pit->pit_set_s,
         pit->pit_position_s ? (int)(pit->pit_position_e - pit->pit_position_s)
                             : 4,
         pit->pit_position_s ? pit->pit_position_s : "null",
         pit->pit_state_s ? (int)(pit->pit_state_e - pit->pit_state_s) : 4,
         pit->pit_state_s ? pit->pit_state_s : "null");

  err = pdb_iterator_util_thaw(
      pdb, &s, e,
      "%{forward}%{low[-high]}%{ordering}%{account}%{extensions}%{end}",
      &forward, &low, &high, pib, &ordering, pib, &acc,
      (pdb_iterator_property *)NULL);
  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_iterator_util_thaw", err,
                 "set \"%.*s\" (expected: ~LOW-HIGH)",
                 (int)(pit->pit_set_e - pit->pit_set_s), pit->pit_set_s);
    return err;
  }

  if ((s = pit->pit_position_s) != NULL && s < (e = pit->pit_position_e)) {
    err = pdb_scan_ull(&s, pit->pit_position_e, &i);
    if (err != 0) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_scan_ull", err,
                   "cannot parse position \"%.*s\" (expected: N)",
                   (int)(pit->pit_position_e - pit->pit_position_s),
                   pit->pit_position_s);
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

  err = pdb_iterator_all_create(pdb, low, high, forward, it_out);
  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_iterator_all_initialize", err,
                 "thawed from \"%.*s\"", (int)(pit->pit_set_e - pit->pit_set_s),
                 pit->pit_set_s);
    return err;
  }
  pdb_iterator_account_set(pdb, *it_out, acc);

  if (i < low) i = low;
  if (i > high) i = high;
  (*it_out)->it_all_i = i;

  if (ordering != NULL) {
    pdb_iterator_ordering_set(pdb, *it_out, ordering);
    pdb_iterator_ordered_set(pdb, *it_out, true);
  } else {
    pdb_iterator_ordered_set(pdb, *it_out, false);
  }

  return 0;
}

/**
 * @brief Is this an all-iterator?
 *
 * @param pdb	module handle
 * @param it	iterator that's being asked about
 *
 * @return true if this is an "all" iterator,
 *	false if not.
 */
bool pdb_iterator_all_is_instance(pdb_handle *pdb, pdb_iterator const *it) {
  return it != NULL && it->it_type == &pdb_iterator_all;
}
