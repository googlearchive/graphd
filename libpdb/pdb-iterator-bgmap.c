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
#include <string.h>


/*
 *  Be done with a bgmap iterator.  The pointer <it> itself will be
 *  freed by the caller; we only need to remove any internal structures.
 */
static void pdb_iterator_bgmap_finish(pdb_handle *pdb, pdb_iterator *it) {
  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "bgmap_finish[%p]", it);

  if (it->it_displayname != NULL) {
    cm_free(pdb->pdb_cm, it->it_displayname);
    it->it_displayname = NULL;
  }
  it->it_type = NULL;
  it->it_magic = 0;
}

/*
 * Convert this iterator to a pretty string. The caller checks
 * for the cached it->it_displayname value before calling this.
 */
static const char *pdb_iterator_bgmap_to_string(pdb_handle *pdb,
                                                pdb_iterator *it, char *buf,
                                                size_t size) {
  snprintf(buf, size, "%sbgmap(%llx):%llx..%llx", it->it_forward ? "" : "~",
           (unsigned long long)it->it_bgmap_source,
           (unsigned long long)it->it_low, (unsigned long long)it->it_high);

  it->it_displayname = cm_strmalcpy(pdb->pdb_cm, buf);
  return buf;
}

const char *pdb_iterator_bgmap_name(pdb_handle *pdb, pdb_iterator *it) {
  PDB_IS_ITERATOR(pdb->pdb_cl, it);
  return pdb_gmap_to_name(pdb, it->it_bgmap_gmap);
}

/*
 * Marshal this iterator for a cursor
 */
static int pdb_iterator_bgmap_freeze(pdb_handle *pdb, pdb_iterator *it,
                                     unsigned int flags, cm_buffer *buf) {
  int err;
  char const *sep = "";

  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  cl_assert(pdb->pdb_cl, buf != NULL);
  cl_cover(pdb->pdb_cl);

  if (flags & PDB_ITERATOR_FREEZE_SET) {
    err = pdb_iterator_freeze_intro(buf, it, "bgmap");
    if (err != 0) return err;

    err = cm_buffer_sprintf(buf, ":%s->%llu",
                            pdb_linkage_to_string(it->it_bgmap_linkage),
                            (unsigned long long)it->it_bgmap_source);
    if (err != 0) return err;

    err = pdb_iterator_freeze_ordering(pdb, buf, it);
    if (err) return err;

    err = pdb_iterator_freeze_account(pdb, buf, it);
    if (err) return err;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    err = cm_buffer_sprintf(buf, "%s%llu", sep,
                            (unsigned long long)it->it_bgmap_offset);
    if (err) return err;
    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_STATE) {
    err = cm_buffer_sprintf(buf, "%s", sep);
    if (err) return err;
  }
  return 0;
}

/*
 * Thaw a bgmap iterator. bgmap iterators look like:
 * [~]bgmap:<left/right/scope/typeguid>:source:low-high:offset
 * we are called with s pointing to the first :.
 *
 * This is cribbed from addb_terator_gmap_thaw
 */

int pdb_iterator_bgmap_thaw(pdb_handle *pdb, pdb_iterator_text const *pit,
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

  /*  :[~]LOW[-HIGH]:LRTS->id/OFF/
   */
  err = pdb_iterator_util_thaw(pdb, &s, pit->pit_set_e,
                               "%{forward}%{low[-high]}:%{linkage}->%{id}%{"
                               "ordering}%{account}%{extensions}%{end}",
                               &forward, &low, &high, &linkage, &source, pib,
                               &ordering, pib, &acc,
                               (pdb_iterator_property *)NULL);
  if (err != 0) return err;

  if (s < e) {
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
           "pdb_iterator_bgmap_thaw: trailing text after "
           "source: \"%.*s\"",
           (int)(e - s), s);
    return PDB_ERR_SYNTAX;
  }

  if ((s = pit->pit_position_s) != NULL && s < (e = pit->pit_position_e)) {
    e = pit->pit_position_e;
    if ((err = pdb_scan_ull(&s, e, &off)) != 0) {
      cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
             "pdb_iterator_bgmap_thaw: expected "
             "offset, got \"%.*s\": %s",
             (int)(e - s), s, strerror(err));
      return err;
    }
    err = pdb_iterator_util_thaw(pdb, &s, e, "%{extensions}%{end}",
                                 (pdb_iterator_property *)NULL);
    if (err != 0) return err;
  } else {
    off = forward ? low : high;
  }

  if ((s = pit->pit_state_s) != NULL && s < (e = pit->pit_state_e)) {
    err = pdb_iterator_util_thaw(pdb, &s, e, "%{extensions}%{end}",
                                 (pdb_iterator_property *)NULL);
    if (err != 0) return err;
  }

  /*
   * Check that the offset is within range
   */
  if (off < low || off > high) {
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb_iterator_bgmap_thaw: offset %llu is outside"
           " of range %llx-%llx for bgmap:%s:%llx",
           off, low, high, pdb_linkage_to_string(linkage),
           (unsigned long long)source);
    return PDB_ERR_SYNTAX;
  }

  gmap = pdb_linkage_to_gmap(pdb, linkage);
  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "Thawed bgmap: %s:%llx@%llu",
         pdb_gmap_to_name(pdb, gmap), (unsigned long long)source, off);

  err = pdb_iterator_bgmap_create(pdb, gmap, source, linkage, high, low,
                                  forward, it_out);

  if (err) return err;

  pdb_iterator_account_set(pdb, *it_out, acc);

  if (ordering != NULL) {
    pdb_iterator_ordering_set(pdb, *it_out, ordering);
    pdb_iterator_ordered_set(pdb, *it_out, true);
  } else {
    pdb_iterator_ordered_set(pdb, *it_out, false);
  }
  (*it_out)->it_bgmap_offset = off;

  return 0;
}

/*
 * Reset an iterator to its initial state
 */
static int pdb_iterator_bgmap_reset(pdb_handle *pdb, pdb_iterator *it) {
  it->it_has_position = true;

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "bgmap_reset[%p]", it);
  if (it->it_forward)
    it->it_bgmap_offset = it->it_low;
  else
    it->it_bgmap_offset = it->it_high;

  it->it_bgmap_need_recover = false;
  it->it_call_state = 0;

  return 0;
}

/*
 * Return the next ID for this iterator and fast-forward it
 * to the next position.
 */
static int pdb_iterator_bgmap_next_loc(pdb_handle *pdb, pdb_iterator *it,
                                       pdb_id *pdb_id_out,
                                       pdb_budget *budget_inout,
                                       const char *file, int line) {
  pdb_budget budget_in = *budget_inout;
  int err;
  addb_gmap_id s;
  bool o;

  cl_assert(pdb->pdb_cl, it->it_high < PDB_ITERATOR_HIGH_ANY);
  cl_assert(pdb->pdb_cl, it->it_has_position);

  /*
   * If this was a thawed gmap, we might need to do some work
   * to recover the bgmap iterator position.  Do that work.
   */
  if (it->it_bgmap_need_recover) {
    err = pdb_iterator_bgmap_position_recover_work(pdb, it, budget_inout);

    /* position_recover_work returns PDB_ERR_NO if the gmap index
     * puts us beyond it_high.  This can legally happen in a
     * cursor and should be translated as PDB_ERR_NO. i.e. a valid
     * cursor returning no more IDs.
     */
    if (err != 0) {
      if (err != PDB_ERR_MORE && err != PDB_ERR_NO)
        cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR,
                     "pdb_iterator_bgmap_recover_work", err,
                     "can not reposition bgmap after thaw "
                     "from something that was a gmap");
      goto err;
    }

    it->it_bgmap_need_recover = false;
  }

  /*  Going backwards: pre-decrement.
   */
  if (!it->it_forward) {
    if (it->it_bgmap_offset <= it->it_low) {
      pdb_rxs_log(pdb, "NEXT %p bgmap done ($%lld)", (void *)it,
                  (long long)(budget_in - *budget_inout));
      err = PDB_ERR_NO;
      goto err;
    }
    it->it_bgmap_offset--;
  }

  /*
   * Call addb_bgmap_next until we run out of budget or
   * something other than PDB_ERR_MORE happens.
   * addb_bgmap_next always returns the NEXT id to look at so
   * if it finds something (err==0) we will need to
   * add or subtract 1 from s to get *pdb_id_out.
   */
  s = it->it_bgmap_offset;

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "pdb_iterator_bgmap_next[%p] budget: %i, source: %llx", it,
         (int)(*budget_inout), (unsigned long long)(it->it_bgmap_source));

  *budget_inout -= pdb_iterator_next_cost(pdb, it);
  for (; *budget_inout >= 0; *budget_inout -= 20) {
    err = addb_bgmap_next(it->it_bgmap_gmap, it->it_bgmap, &s, it->it_low,
                          it->it_high, it->it_forward);

    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "bgmap_next[%p][%s:%i]: %llx->%llx [%s] ($%lld)", it, file, line,
           (unsigned long long)it->it_bgmap_offset, (unsigned long long)s,
           strerror(err), budget_in - *budget_inout);

    it->it_bgmap_offset = s;
    if (err != ADDB_ERR_MORE) goto have_result;
  }

  /*
   * Ran out of budget looking for the thing
   */
  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "pdb_iterator_bgmap_next suspending");

  pdb_rxs_log(pdb, "NEXT %p bgmap suspend ($%lld)", (void *)it,
              (long long)(budget_in - *budget_inout));

  /* Going backwards: we're parked one above of where we
   * actually are.
   */
  if (!it->it_forward) it->it_bgmap_offset++;
  err = PDB_ERR_MORE;
  goto err;

have_result:
  if (err == PDB_ERR_NO) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_bgmap_next: No more data");
    pdb_rxs_log(pdb, "NEXT %p bgmap done ($%lld)", (void *)it,
                (long long)(budget_in - *budget_inout));
    goto err;
  } else if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_bgmap_next", err,
                 "unexpected error");
    goto err;
  }

  *pdb_id_out = it->it_bgmap_offset;

  /*  Going forward: post-increment.
   */
  if (it->it_forward) {
    it->it_bgmap_offset++;
    if (it->it_bgmap_offset > it->it_high) {
      pdb_rxs_log(pdb, "NEXT %p bgmap done ($%lld)", (void *)it,
                  (long long)(budget_in - *budget_inout));
      err = PDB_ERR_NO;
      goto err;
    }
  }

  /*
   * Sanity check: if we get this far, *pdb_id_out should correspond
   * to a set bit
   */
  err = addb_bgmap_check(it->it_bgmap_gmap, it->it_bgmap, *pdb_id_out, &o);
  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_bgmap_next", err,
                 "final check for %llx fails?",
                 (unsigned long long)*pdb_id_out);
    goto err;
  }

  cl_assert(pdb->pdb_cl, o);

  pdb_rxs_log(pdb, "NEXT %p bgmap %llx ($%lld)", (void *)it,
              (unsigned long long)*pdb_id_out,
              (long long)(budget_in - *budget_inout));
  cl_assert(pdb->pdb_cl, err == 0);

/* FALL THROUGH */
err:
  pdb_iterator_account_charge_budget(pdb, it, next);
  return err;
}

/*
 * Check if a single ID is present in a bgmap
 */
static int pdb_iterator_bgmap_check(pdb_handle *pdb, pdb_iterator *it,
                                    pdb_id id, pdb_budget *budget_inout) {
  pdb_budget budget_in = *budget_inout;
  bool o;
  int err = 0;

  *budget_inout -= pdb_iterator_check_cost(pdb, it);

  if (id >= it->it_high || id < it->it_low) {
    pdb_rxs_log(pdb, "CHECK %p bgmap %llx no ($%lld)", (void *)it,
                (unsigned long long)id,
                (unsigned long long)pdb_iterator_check_cost(pdb, it));
    err = PDB_ERR_NO;
    goto err;
  }

  err = addb_bgmap_check(it->it_bgmap_gmap, it->it_bgmap, id, &o);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_iterator_bgmap_check", err,
                 "%s[%llx]: %llx: %i ($%lld)", addb_bgmap_name(it->it_bgmap),
                 (unsigned long long)it->it_bgmap_source,
                 (unsigned long long)id, (int)o,
                 (long long)pdb_iterator_check_cost(pdb, it));
    goto err;
  }

  pdb_rxs_log(pdb, "CHECK %p bgmap %llx %s ($%lld)", (void *)it,
              (unsigned long long)id, o ? "yes" : "no",
              (long long)pdb_iterator_check_cost(pdb, it));

  err = o ? 0 : PDB_ERR_NO;
/* FALL THROUGH */
err:
  pdb_iterator_account_charge_budget(pdb, it, check);
  return err;
}

/*
 * Find the first bit set on or after id_in
 */
static int pdb_iterator_bgmap_find_loc(pdb_handle *pdb, pdb_iterator *it,
                                       pdb_id id_in, pdb_id *id_out,
                                       pdb_budget *budget_inout,
                                       const char *file, int line) {
  int err;
  pdb_budget budget_in = *budget_inout;
  pdb_id id;

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "bgmap_find[%p][%s:%i] start: %llx state:%i ($%lld)", it, file, line,
         (unsigned long long)id_in, it->it_call_state, *budget_inout);

  switch (it->it_call_state) {
    case 0:
      it->it_bgmap_offset = (pdb_iterator_forward(pdb, it) ? id_in : id_in + 1);
      it->it_has_position = true;
      it->it_bgmap_find_hold = id_in;
      it->it_bgmap_need_recover = false;

      if (it->it_bgmap_offset < it->it_low) {
        it->it_bgmap_offset = it->it_low;
        if (!it->it_forward) {
          pdb_rxs_log(pdb, "FIND %p bgmap %llx done ($%lld)", (void *)it,
                      (unsigned long long)id_in,
                      (long long)(budget_in - *budget_inout));
          err = PDB_ERR_NO;
          goto err;
        }
      }

      if (it->it_bgmap_offset >= it->it_high) {
        it->it_bgmap_offset = it->it_high;
        if (it->it_forward) {
          pdb_rxs_log(pdb, "FIND %p bgmap %llx done ($%lld)", (void *)it,
                      (unsigned long long)id_in,
                      (long long)(budget_in - *budget_inout));
          err = PDB_ERR_NO;
          goto err;
        }
      }
      it->it_call_state = 1;

    case 1:
      id = id_in;
      err = pdb_iterator_bgmap_next(pdb, it, &id, budget_inout);
      if (err != 0) {
        pdb_rxs_log(pdb, "FIND %p bgmap %llx %s ($%lld)", (void *)it,
                    (unsigned long long)it->it_bgmap_find_hold,
                    err == PDB_ERR_NO ? "done" : pdb_xstrerror(err),
                    (long long)(budget_in - *budget_inout));
        goto err;
      }

      it->it_call_state = 0;
      *id_out = id;
      pdb_rxs_log(pdb, "FIND %p bgmap %llx -> %llx ($%lld)", (void *)it,
                  (unsigned long long)id_in, (unsigned long long)*id_out,
                  (long long)(budget_in - *budget_inout));
      err = 0;
      goto err;

    default:
      cl_notreached(pdb->pdb_cl, "it->it_call_state is %i. Should be 0 or 1",
                    it->it_call_state);
  }

  err = 0;
err:
  pdb_iterator_account_charge_budget(pdb, it, check);
  return err;
}

/*
 * Suspend access to the database.
 */
static int pdb_iterator_bgmap_suspend(pdb_handle *pdb, pdb_iterator *it) {
  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "pdb_iterator_bgmap_suspend it=%p", it);
  return 0;
}

/* Resume access to the database.
 */
static int pdb_iterator_bgmap_unsuspend(pdb_handle *pdb, pdb_iterator *it) {
  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "pdb_iterator_bgmap_unsuspend it=%p",
         it);

  return 0;
}

/*
 * Duplicate a bgmap. The new bgmap starts with the same position
 * but gets to move independently.
 */
static int pdb_iterator_bgmap_clone(pdb_handle *pdb, pdb_iterator *it,
                                    pdb_iterator **it_out) {
  int err;
  err = pdb_iterator_make_clone(pdb, it->it_original, it_out);

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "bgmap_clone: %p -> %p",
         it->it_original, *it_out);

  if (err) {
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR, "can't clone iterator: %s",
           strerror(err));
    return err;
  }

  /*
   * Note: cloned iterators are expected to inherent their
   * parents position if it has one. Or to be reset if their
   * parent doesn't have one.
   */
  if (!pdb_iterator_has_position(pdb, it)) {
    err = pdb_iterator_bgmap_reset(pdb, *it_out);
    if (err) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_iterator_bgmap_reset", err,
                   "pdb_iterator_bgmap_clone: can't clone");

      return err;
    }
  } else {
    (*it_out)->it_bgmap_offset = it->it_bgmap_offset;
    (*it_out)->it_has_position = true;
  }
  pdb_rxs_log(pdb, "CLONE %p bgmap %p", (void *)it, (void *)*it_out);
  return 0;
}

/**
 * @brief Return the summary for a BGMAP iterator.
 *
 * @param pdb		module handle
 * @param it		a gmap iterator
 * @param psum_out	out: summary
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int pdb_iterator_bgmap_primitive_summary(
    pdb_handle *pdb, pdb_iterator *it, pdb_primitive_summary *psum_out) {
  int l, err;

  for (l = 0; l < PDB_LINKAGE_N; l++)
    if (it->it_gmap == pdb_linkage_to_gmap(pdb, l)) break;
  if (l == PDB_LINKAGE_N) return PDB_ERR_NO;

  err = pdb_id_to_guid(pdb, it->it_bgmap_source, psum_out->psum_guid + l);
  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_id_to_guid", err,
                 "it->it_bgmap_source=%lld", (long long)it->it_bgmap_source);
    return err;
  }

  psum_out->psum_locked = 1 << l;
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
 * @param beyond_out	out: true if we're safely beyond this value.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int pdb_iterator_bgmap_beyond(pdb_handle *pdb, pdb_iterator *it,
                                     char const *s, char const *e,
                                     bool *beyond_out) {
  char buf[200];
  pdb_id id, last_id;

  if (e - s != sizeof(id)) {
    *beyond_out = false;
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb_iterator_bgmap_beyond: unexpected "
           "value size (%zu bytes; expected %zu)",
           (size_t)(e - s), sizeof(id));
    return EINVAL;
  }
  memcpy(&id, s, sizeof(id));

  if (pdb_iterator_forward(pdb, it)) {
    if (it->it_bgmap_offset == 0) {
      cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
             "pdb_iterator_bgmap_beyond: "
             "still at the beginning");
      *beyond_out = false;
      return 0;
    }
    last_id = it->it_bgmap_offset - 1;
  } else {
    last_id = it->it_bgmap_offset;
  }
  *beyond_out = (pdb_iterator_forward(pdb, it) ? id < last_id : id > last_id);

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "pdb_iterator_bgmap_beyond: %llx vs. last_id %llx in %s: %s",
         (unsigned long long)id, (unsigned long long)last_id,
         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         *beyond_out ? "yes" : "no");
  return 0;
}

static int pdb_iterator_bgmap_range_estimate(pdb_handle *pdb, pdb_iterator *it,
                                             pdb_range_estimate *range) {
  pdb_iterator_range_estimate_default(pdb, it, range);

  if (pdb_iterator_forward(pdb, it)) {
    range->range_low = it->it_bgmap_offset;
  } else {
    range->range_high = it->it_bgmap_offset;
  }
  range->range_n_exact = PDB_COUNT_UNBOUNDED;
  range->range_n_max = range->range_high - range->range_low;

  return 0;
}

static int pdb_iterator_bgmap_restrict(pdb_handle *pdb, pdb_iterator *it,
                                       pdb_primitive_summary const *psum,
                                       pdb_iterator **it_out) {
  int err;
  int linkage;

  /*  We can only do this for gmap iterators with a single linkage,
   *  and psums whose result is the primitive GUID.
   */
  if (it->it_bgmap_linkage >= PDB_LINKAGE_N ||
      psum->psum_result != PDB_LINKAGE_N)
    return PDB_ERR_ALREADY;

  /*  Do we conflict with the restriction?
   */
  if (psum->psum_locked & (1 << it->it_bgmap_linkage)) {
    pdb_id id;
    err = pdb_id_from_guid(pdb, &id, psum->psum_guid + it->it_bgmap_linkage);
    if (err != 0) return err;

    if (id != it->it_bgmap_source) return PDB_ERR_NO;
  }

  /*  Turn to VIP?
   */

  /*  Case 1, I'm a type, you're a left or right.
   */
  if ((it->it_bgmap_linkage == PDB_LINKAGE_TYPEGUID) &&
      (((psum->psum_locked &
         ((1 << PDB_LINKAGE_RIGHT) | (1 << PDB_LINKAGE_LEFT))) ==
        (1 << (linkage = PDB_LINKAGE_RIGHT))) ||
       ((psum->psum_locked &
         ((1 << PDB_LINKAGE_RIGHT) | (1 << PDB_LINKAGE_LEFT))) ==
        (1 << (linkage = PDB_LINKAGE_LEFT))))) {
    graph_guid guid;

    err = pdb_id_to_guid(pdb, it->it_bgmap_source, &guid);
    if (err != 0) return err;

    return pdb_vip_linkage_iterator(
        pdb, psum->psum_guid + linkage, linkage, &guid, it->it_low, it->it_high,
        pdb_iterator_forward(pdb, it), true, it_out, NULL);
  }

  /*  Case 2, I'm a left or right, you're a type.
   */
  else if ((it->it_bgmap_linkage == PDB_LINKAGE_RIGHT ||
            it->it_bgmap_linkage == PDB_LINKAGE_LEFT) &&
           (psum->psum_locked & (1 << PDB_LINKAGE_TYPEGUID))) {
    /*  Turn into our VIP.
     */
    graph_guid guid;

    err = pdb_id_to_guid(pdb, it->it_bgmap_source, &guid);
    if (err != 0) return err;

    return pdb_vip_linkage_iterator(
        pdb, &guid, it->it_bgmap_linkage,
        psum->psum_guid + PDB_LINKAGE_TYPEGUID, it->it_low, it->it_high,
        pdb_iterator_forward(pdb, it), true, it_out, NULL);
  } else
    return PDB_ERR_ALREADY;

  return 0;
}

static const pdb_iterator_type pdb_iterator_bgmap = {
    "bgmap",
    pdb_iterator_bgmap_finish,
    pdb_iterator_bgmap_reset,  /* */
    pdb_iterator_bgmap_clone,  /* */
    pdb_iterator_bgmap_freeze, /* */
    pdb_iterator_bgmap_to_string,
    pdb_iterator_bgmap_next_loc,          /* */
    pdb_iterator_bgmap_find_loc,          /* */
    pdb_iterator_bgmap_check,             /* */
    pdb_iterator_util_statistics_none,    /* statistics */
    NULL,                                 /* idarray */
    pdb_iterator_bgmap_primitive_summary, /* psum */
    pdb_iterator_bgmap_beyond,
    pdb_iterator_bgmap_range_estimate,
    pdb_iterator_bgmap_restrict,

    pdb_iterator_bgmap_suspend,
    pdb_iterator_bgmap_unsuspend};

/*
 * This is the bgmap analogy to pdb_iterator_gmap_is_instance
 */
bool pdb_iterator_bgmap_is_instance(pdb_handle *pdb, pdb_iterator *it,
                                    int linkage) {
  if (!it) return false;

  if ((it->it_type == &pdb_iterator_bgmap) &&
      ((linkage == PDB_LINKAGE_ANY) ||
       (it->it_bgmap_gmap == pdb_linkage_to_gmap(pdb, linkage))))
    return true;

  return false;
}

/*
 * Prepare an bgmap for cursor recovery: In the event that a cursor is
 * made when source is a gmap, and that gmap is turned into a bgmap when
 * before we thaw the cursor, the gmap offset will be incorrect.
 *
 * This function sets up parameters such that iterator_bgmap_next will spend
 * its time calculating the correct bgmap offset before returning
 * any values.
 */
int pdb_iterator_bgmap_position_recover_init(pdb_handle *pdb, pdb_iterator *it,
                                             pdb_id gmap_position) {
  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "Converting gmap index %llu to bgmap index",
         (unsigned long long)gmap_position);
  if (gmap_position == 0) {
    pdb_iterator_bgmap_reset(pdb, it);
    return 0;
  }

  it->it_bgmap_recover_n = gmap_position;
  it->it_bgmap_recover_count = 0;
  it->it_bgmap_recover_pos = it->it_low;
  it->it_has_position = true;

  /* State for next to know it needs to work on this */
  it->it_bgmap_need_recover = true;

  return 0;
}

/*
 * Work until we run out of budget on recovering a bgmap cursor offset
 * from something that used to be a gmap.
 */
int pdb_iterator_bgmap_position_recover_work(pdb_handle *pdb, pdb_iterator *it,
                                             pdb_budget *budget_inout_real) {
  int err;

  pdb_budget *budget_inout = budget_inout_real;

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "continuing gmap->bgmap cursor conversion: %llu %llu %llu",
         (unsigned long long)it->it_bgmap_recover_pos,
         (unsigned long long)it->it_bgmap_recover_count,
         (unsigned long long)it->it_bgmap_recover_n);

  /*
  * Start at the lowest possible set ID and count bits
  */
  do {
    if (*budget_inout < 0) return PDB_ERR_MORE;

    err = pdb_iterator_bgmap_check(pdb, it, it->it_bgmap_recover_pos,
                                   budget_inout);
    if (!err)
      it->it_bgmap_recover_count++;

    else if (err != PDB_ERR_NO) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_iterator_bgmap_check", err,
                   "Can't check bit at: %llu",
                   (unsigned long long)it->it_bgmap_recover_pos);
      return err;
    }

    it->it_bgmap_recover_pos++;
    if (it->it_bgmap_recover_count >= it->it_bgmap_recover_n)
      goto have_bitcount;

  } while (it->it_bgmap_recover_pos < it->it_high);

  /*
   * This might happen because something is horribly wrong.
   * More likely, the gmap was frozen in a state where the next
   * thing to return was PDB_ERR_NO so the gmap offset is beyond
   * the high iterator bound.
   */
  cl_log(pdb->pdb_cl, CL_LEVEL_DEBUG,
         "pdb_iterator_bgmap_position_recover_work: "
         "position: %llu is past the end of high: %llx (bc: %llu)",
         (unsigned long long)it->it_bgmap_recover_pos,
         (unsigned long long)it->it_high,
         (unsigned long long)it->it_bgmap_recover_count);

  return PDB_ERR_NO;

have_bitcount:

  it->it_bgmap_offset = it->it_bgmap_recover_pos;
  cl_log(pdb->pdb_cl, CL_LEVEL_DEBUG,
         "Restore and old gmap cursor to a bgmap %llu -> %llu",
         (unsigned long long)it->it_bgmap_recover_n,
         (unsigned long long)it->it_bgmap_recover_pos);

  it->it_bgmap_need_recover = false;
  return 0;
}

/*
 * Create a bgmap iterator from a gmap:source pair.
 */
int pdb_iterator_bgmap_create(pdb_handle *pdb, addb_gmap *gm, pdb_id source,
                              int linkage, pdb_id high, pdb_id low,
                              bool forward, pdb_iterator **it_out) {
  cl_handle *cl = pdb->pdb_cl;
  pdb_iterator *it; /* The new iterator */
  addb_bgmap *bgm;  /* The bgmap for this iterator */
  int err;
  unsigned long long guess_n; /* how many bits are set from low .. high */
  unsigned long long total_n; /* How many total bits are set */
  unsigned long long adjhigh; /* High boundry after adjustment */
  unsigned long long adjlow;  /* Low boundry after adjustment */
  char buf[200];

  cl_assert(cl, high > low);

  cl_log(cl, CL_LEVEL_VERBOSE,
         "pdb_iterator_bgmap_create: %s:%llx %llx to %llx",
         pdb_linkage_to_string(linkage), (unsigned long long)source,
         (unsigned long long)low, (unsigned long long)high);
  err = addb_bgmap_lookup(gm, source, &bgm);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_bgmap_lookup", err,
                 "Can't get bgm for %s:%llx", pdb_gmap_to_name(pdb, gm),
                 (unsigned long long)source);
    return err;
  }

  err = addb_gmap_bgmap_read_size(gm, source, &total_n);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_bgmap_read_size", err,
                 "Can't get size for %s:%llx", pdb_gmap_to_name(pdb, gm),
                 (unsigned long long)source);
    return err;
  }

  /*
   * Guess the lowest possible n that is still >= the actual n.
   * If the range covers all possible IDs in this bgmap, return the total.
   * If it covers a partial range, return the minimum of the total
   * or the size of that range.
   *
   * This is important because various components trust that if
   * we say we'll return 100 IDs that we return <= 100 IDs. Failure
   * to do so overloads a static buffer (that is protected by an assert)
   *
   */
  adjlow = low <= source + 1 ? source + 1 : low;
  adjhigh = high == PDB_ITERATOR_HIGH_ANY
                ? addb_istore_next_id(pdb->pdb_primitive)
                : high;

  if (adjlow >= adjhigh) return pdb_iterator_null_create(pdb, it_out);

  /*  Shrink the two sides to match what actually exists.
   */
  {
    addb_gmap_id start = adjlow;
    do {
      err = addb_bgmap_next(gm, bgm, &start, adjlow, adjhigh, true);
    } while (err == ADDB_ERR_MORE);
    if (err == PDB_ERR_NO) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "pdb_iterator_bgmap_create: no bits set from"
             " %llx to %llx. returning NULL iterator",
             (unsigned long long)adjlow, adjhigh);
      return pdb_iterator_null_create(pdb, it_out);
    } else if (err != 0)
      return err;
    adjlow = start;
  }

  {
    addb_gmap_id start = adjhigh - 1;
    do {
      err = addb_bgmap_next(gm, bgm, &start, adjlow, adjhigh + 1, false);
    } while (err == ADDB_ERR_MORE);

    if (err == ADDB_ERR_NO) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "pdb_iterator_bgmap_create: no bits set from"
             " %llx to %llx. returning NULL iterator",
             (unsigned long long)adjlow, adjhigh);
      return pdb_iterator_null_create(pdb, it_out);
    } else if (err != 0)
      return err;
    adjhigh = start + 1;
  }

  guess_n = adjhigh - adjlow;

  cl_assert(cl, guess_n > 0);
  cl_assert(cl, total_n > 0);
  if (guess_n > total_n) guess_n = total_n;

  cl_assert(cl, guess_n > 0);

  *it_out = it = cm_malloc(pdb->pdb_cm, sizeof *it);
  if (*it_out == NULL) return ENOMEM;

  pdb_iterator_make(pdb, it, adjlow, adjhigh, forward);

  it->it_type = &pdb_iterator_bgmap;
  it->it_bgmap_gmap = gm;
  it->it_bgmap = bgm;
  it->it_bgmap_source = source;
  it->it_has_position = true;
  it->it_forward = forward;
  it->it_bgmap_need_recover = false;
  it->it_bgmap_linkage = linkage;

  if (forward)
    it->it_bgmap_offset = adjlow;
  else
    it->it_bgmap_offset = adjhigh;

  pdb_iterator_n_set(pdb, it, guess_n);

  pdb_iterator_check_cost_set(pdb, it, 1);
  pdb_iterator_find_cost_set(pdb, it, 3);
  pdb_iterator_next_cost_set(pdb, it, 3);
  pdb_iterator_sorted_set(pdb, it, true);
  pdb_iterator_statistics_done_set(pdb, it);

  pdb_iterator_suspend_chain_in(pdb, it);

  pdb_rxs_log(pdb, "CREATE %p bgmap %s(%llx) %llx %llx %s", (void *)it,
              pdb_linkage_to_string(linkage), (unsigned long long)source,
              (unsigned long long)low, (unsigned long long)high,
              forward ? "forward" : "backward");

  cl_log(cl, CL_LEVEL_VERBOSE | PDB_FACILITY_ITERATOR,
         "PDB STAT for %s: n=%llu cc=%llu "
         "nc=%llu fc=%llu; sorted %llx..%llx (incl)",

         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         (unsigned long long)pdb_iterator_n(pdb, it),
         (unsigned long long)pdb_iterator_check_cost(pdb, it),
         (unsigned long long)pdb_iterator_next_cost(pdb, it),
         (unsigned long long)pdb_iterator_find_cost(pdb, it),
         (unsigned long long)it->it_low, (unsigned long long)it->it_high - 1);

  cl_assert(cl, it->it_high < PDB_ITERATOR_HIGH_ANY);
  return 0;
}

/*
 * Quickly intersect a bgmap and a gmap in when the size of the intersection
 * is known ahead of time.
 * The strategy here is to always do a find on the gmap and a check
 * on the bgmap.
 */
int pdb_iterator_bgmap_idarray_intersect(pdb_handle *pdb, pdb_iterator *bgmap,
                                         addb_idarray *ida, pdb_id low,
                                         pdb_id high, pdb_id *id_out,
                                         size_t *id_n, size_t id_m)

{
  pdb_id id;
  unsigned long long ida_offset = 0;
  unsigned long long ida_max;

  int err;
  char s2[200];
  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "intersecting idarray:%p with %s [%llu,%llu]", ida,
         pdb_iterator_to_string(pdb, bgmap, s2, 200), (unsigned long long)*id_n,
         (unsigned long long)id_m);

  ida_max = addb_idarray_n(ida);

  /*  Start at the lower end of the bgmap
   *  iterator.
   */
  if (bgmap->it_low != 0) {
    pdb_id dummy;

    /* The start offset of the lower end is the
     * start of the intersection iteration.
     */
    err = addb_idarray_search(ida, 0, ida_max, bgmap->it_low, &ida_offset,
                              &dummy);
    if (err != 0) return err;
  }

  *id_n = 0;
  cl_assert(pdb->pdb_cl, bgmap->it_type == &pdb_iterator_bgmap);
  for (; ida_offset < ida_max; ida_offset++) {
    bool bit;

    err = addb_idarray_read1(ida, ida_offset, &id);
    if (err == PDB_ERR_NO)
      goto done;
    else if (err != 0) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "addb_idarray_read1 (gmap)",
                   err, "id=%llx", (unsigned long long)id);
      return err;
    }

    /*  Stop checking at the upper end of
     *  the bgmap iterator.
     */
    else if (id >= bgmap->it_high) {
      err = PDB_ERR_NO;
      goto done;
    }

    err = addb_bgmap_check(bgmap->it_bgmap_gmap, bgmap->it_bgmap, id, &bit);
    if (err == 0) {
      if (bit) {
        if (*id_n >= id_m) {
          cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
                 "overflow after %llu "
                 "results",
                 (unsigned long long)(*id_n));
          return PDB_ERR_TOO_MANY;
        }
        id_out[(*id_n)++] = id;
      }
    } else if (err != PDB_ERR_NO) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "addb_bgmap_check", err,
                   "id=%llx", (unsigned long long)id);
      return err;
    }
  }
done:
  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "%llu results",
         (unsigned long long)(*id_n));
  return 0;
}

/* @brief  		Quickly intersect a fixed array of indices and a gmap.
 *
 * @param pdb		the handle of the surrounding module
 * @param it		the iterator; hopefully, a bgmap
 * @param id_in		array of fixed values
 * @param n_in		number of values in id_in
 * @param id_out	values whose bits are set are copied into here.
 * @param n_out		on output, number of values that were copied to id_out
 * @param id_m		number of values allocated in id_out.
 *
 * @return 0 on success
 * @return PDB_ERR_NOT_SUPPORTED if this wasn't a bgmap
 * @return PDB_ERR_MORE if there are more than id_m values in the
 *	intersection.
 * @return other nonzero error codes on unexpected system errors
 */
int pdb_iterator_bgmap_fixed_intersect(pdb_handle *pdb, pdb_iterator *it,
                                       pdb_id const *id_in, size_t n_in,
                                       pdb_id *id_out, size_t *n_out,
                                       size_t id_m) {
  if (it->it_type != &pdb_iterator_bgmap) return PDB_ERR_NOT_SUPPORTED;

  return addb_bgmap_fixed_intersect(pdb->pdb_addb, it->it_bgmap, id_in, n_in,
                                    id_out, n_out, id_m);
}
