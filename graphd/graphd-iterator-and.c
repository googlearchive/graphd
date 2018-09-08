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
#include "graphd/graphd-iterator-and.h"

#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

GRAPHD_SABOTAGE_DECL;

extern const pdb_iterator_type graphd_iterator_and_type;

static bool graphd_iterator_and_cache_synced(
    graphd_iterator_and const *const ogia) {
  size_t i;

  cl_assert(ogia->gia_cl, ogia->gia_cache != NULL);

  /* At the beginning? */
  if (ogia->gia_cache_ps.ps_id == PDB_ID_NONE)
    return ogia->gia_cache->gic_n == 0;

  /* Empty cache. */
  if (ogia->gia_cache->gic_n == 0) return false;

  /* Is the most recently produced ID the last one in the cache?
   */
  if (ogia->gia_cache->gic_id[ogia->gia_cache->gic_n - 1] ==
      ogia->gia_cache_ps.ps_id)
    return true;

  /*  Are we ahead of ourselves?
   */
  for (i = ogia->gia_cache->gic_n; i > 0;) {
    i--;
    if (ogia->gia_cache->gic_id[i] == ogia->gia_cache_ps.ps_id) {
      cl_log(ogia->gia_cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_and_cache_synced: ID %llu "
             "is at cache offset %zu of %zu",
             (unsigned long long)ogia->gia_cache_ps.ps_id, i,
             ogia->gia_cache->gic_n);
      return false;
    }
  }

  /* We're working on the next cache element.
   */
  return true;
}

static char const *graphd_iterator_and_position_string(pdb_handle *pdb,
                                                       pdb_iterator *it,
                                                       char *buf, size_t size) {
  graphd_iterator_and *gia = it->it_theory;

  if (!pdb_iterator_has_position(pdb, it)) return "?";

  GRAPHD_IS_AND(pdb_log(pdb), gia);

  if (gia->gia_resume_id != PDB_ID_NONE) {
    snprintf(buf, size, "[RESUME %llx]",
             (unsigned long long)gia->gia_resume_id);
    size -= strlen(buf);
    buf += strlen(buf);
  }

  if (gia->gia_cache_offset_valid) {
    if (gia->gia_cache_offset == 0)
      snprintf(buf, size, "0");
    else
      snprintf(buf, size, "(cache)%lld", (long long)gia->gia_cache_offset);
    return buf;
  }
  if (gia->gia_ps.ps_id == PDB_ID_NONE) {
    snprintf(buf, size, "[BAD POSITION in %p]", (void *)it);
    return buf;
  }
  snprintf(buf, size, "@%lld", (long long)gia->gia_ps.ps_id);
  return buf;
}

static void and_subcondition_finish(graphd_iterator_and *const ogia,
                                    graphd_subcondition *const sc) {
  cl_handle *cl = ogia->gia_cl;
  cl_enter(cl, CL_LEVEL_VERBOSE, "sc=%p", (void *)sc);

  graphd_iterator_and_process_state_finish(ogia, &sc->sc_contest_ps);
  pdb_iterator_destroy(ogia->gia_pdb, &sc->sc_it);

  cl_leave(cl, CL_LEVEL_VERBOSE, "sc=%p", (void *)sc);
}

/*
 *  If there's more than one "fixed" subiterator, intersect
 *  them, and replace the two subiterators with the intersection.
 *
 *  This happens in graphd_iterator_and_add_subcondition,
 *  before the iterator is fully built.
 *
 *  @return GRAPHD_ERR_NO if there was nobody to merge with,
 *	or if the iterator wasn't a fixed iterator.
 *  @return 0 if the subiterator was merged and destroyed
 */
static int and_merge_fixed(pdb_handle *pdb, pdb_iterator *it,
                           pdb_iterator **fix_ptr) {
  size_t i;
  graphd_iterator_and *ogia = it->it_theory;
  int err = 0;
  cl_handle *cl = pdb_log(pdb);

  pdb_id *acc_id, *fix_id;
  size_t acc_n, fix_n;
  pdb_budget fixed_merge_budget = 10000;

  pdb_iterator *acc_new;

  PDB_IS_ITERATOR(cl, it);

  cl_assert(cl, it->it_type == &graphd_iterator_and_type);
  cl_assert(cl, it->it_original == it);
  cl = ogia->gia_cl;

  if (!graphd_iterator_fixed_is_instance(pdb, *fix_ptr, &fix_id, &fix_n))
    return GRAPHD_ERR_NO;

  for (i = 0; i < ogia->gia_n; i++) {
    if (!graphd_iterator_fixed_is_instance(pdb, ogia->gia_sc[i].sc_it, &acc_id,
                                           &acc_n))
      continue;

    /*  Intersect sc_it with *fix_ptr,
     */
    err = graphd_iterator_intersect(ogia->gia_graphd, ogia->gia_sc[i].sc_it,
                                    *fix_ptr, it->it_low, it->it_high,
                                    pdb_iterator_forward(pdb, *fix_ptr), false,
                                    &fixed_merge_budget, &acc_new);
    if (err != 0) {
      char b1[200], b2[200];
      cl_log_errno(
          cl, CL_LEVEL_FAIL, "graphd_iterator_intersect", err, "%s and %s",
          pdb_iterator_to_string(pdb, ogia->gia_sc[i].sc_it, b1, sizeof b1),
          pdb_iterator_to_string(pdb, *fix_ptr, b2, sizeof b2));
      return err;
    }

    {
      char b1[200], b2[200], b3[200];
      cl_log(cl, CL_LEVEL_VERBOSE,
             "and_merge_fixed: intersect %s and %s, yielding %s",
             pdb_iterator_to_string(pdb, ogia->gia_sc[i].sc_it, b1, sizeof b1),
             pdb_iterator_to_string(pdb, *fix_ptr, b2, sizeof b2),
             pdb_iterator_to_string(pdb, acc_new, b3, sizeof b3));
    }

    /*  Replace sc_it with the new fix.
     */
    pdb_iterator_destroy(pdb, &ogia->gia_sc[i].sc_it);
    ogia->gia_sc[i].sc_it = acc_new;

    /*   Destroy the new subarray.
     */
    pdb_iterator_destroy(pdb, fix_ptr);
    return 0;
  }
  return GRAPHD_ERR_NO;
}

static bool and_is_null(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_and *ogia = it->it_theory;
  size_t i;

  /*  If low >= high,
   *  the total is a NULL.
   */
  if (it->it_low >= it->it_high) return true;

  /*  If any of the subconditions is a NULL,
   *  the total is a NULL.
   */
  for (i = 0; i < ogia->gia_n; i++) {
    if (pdb_iterator_null_is_instance(pdb, ogia->gia_sc[i].sc_it)) return true;
  }
  return false;
}

/*  Direct budget towards cache expansion.
 *
 *  The cache expansion may have started in another
 *  coroutine.  This just adds some time to it.
 *
 *  The thread under whose control the cache expansion
 *  finishes picks up the result and moves it into the
 *  cache buffer.
 */
static int and_iterator_cache_expand(pdb_handle *pdb, pdb_iterator *it,
                                     pdb_budget *budget_inout,
                                     size_t desired_offset) {
  graphd_iterator_and *ogia = ogia(it);
  graphd_iterator_cache *const gic = ogia->gia_cache;
  and_process_state *const ps = &ogia->gia_cache_ps;
  cl_handle *const cl = ogia->gia_cl;
  int err = 0;

  if (gic->gic_eof) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "and_iterator_cache_expand: already at EOF. ($0)");
    return GRAPH_ERR_NO;
  }
  if (gic->gic_n > desired_offset) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "and_iterator_cache_expand: size %zu >  desired offset %zu. ($0)",
           gic->gic_n, desired_offset);
    return 0;
  }

  cl_enter(cl, CL_LEVEL_VERBOSE,
           "it %p, oit %p, cache %p, cache n: %zu, "
           "last %llx, ps_id %llx; call_state %d, "
           "desired offset %zu, cache offset %zu, nfri=%llx",

           (void *)it, (void *)it->it_original, (void *)gic, gic->gic_n,
           gic->gic_n > 0 ? (long long)gic->gic_id[gic->gic_n - 1] : -1ll,
           (unsigned long long)ps->ps_id, ps->ps_run_call_state, desired_offset,
           gic->gic_n, (long long)ps->ps_next_find_resume_id);

  while (gic->gic_n <= desired_offset) {
    size_t i;

    if (gic->gic_n > 0 && ps->ps_next_find_resume_id != PDB_ID_NONE &&
        gic->gic_id[gic->gic_n - 1] != ps->ps_next_find_resume_id) {
      /* Sometimes, if an iterator is working toward a new result, it will store
       * an intermediate result in ps_next_find_resume_id (see the assignment
       * under
       * run-state 6) -- if this is the case, then it will be greater than (or
       * less than,
       * if reversed) the last produced ID. Let it keep going, and don't smash
       * it.
       */

      if (!(pdb_iterator_forward(pdb, it)
                ? ps->ps_next_find_resume_id > gic->gic_id[gic->gic_n - 1]
                : ps->ps_next_find_resume_id < gic->gic_id[gic->gic_n - 1]))

      {
        /*  We need to catch up the producer - run it until it
         *  produces the highest cached value before writing
         *  more.
         */
        ps->ps_run_call_state = GRAPHD_ITERATOR_AND_RUN_NEXT_CATCH_UP_START;
        ps->ps_next_find_resume_id = gic->gic_id[gic->gic_n - 1];
      }
    }

    /*  Turn the crank  -- catch up with ps_next_find_resume_id,
     *  if we have one, and then produce another value.
     */
    err = graphd_iterator_and_run(it, ogia->gia_producer, ps, budget_inout);
    if (err != GRAPHD_ERR_MORE) ps->ps_next_find_resume_id = PDB_ID_NONE;

    if (err != 0) {
      if (err == GRAPHD_ERR_NO) graphd_iterator_cache_eof(gic);
      break;
    }

    for (i = ogia->gia_cache_ps_offset; i < gic->gic_n; i++)
      if (gic->gic_id[i] == ps->ps_id) break;

    ogia->gia_cache_ps_offset = i + 1;
    if (i >= gic->gic_n) {
      err = graphd_iterator_cache_add(gic, ps->ps_id, ps->ps_run_cost);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_cache_add", err,
                     "id=%llx, cache=%p[%zu]", (unsigned long long)ps->ps_id,
                     (void *)gic, gic->gic_n);
        break;
      }
    }
  }

  if (err == 0)
    cl_leave(cl, CL_LEVEL_VERBOSE, "ok [%zu of %zu] := %llx", desired_offset,
             gic->gic_n, (unsigned long long)gic->gic_id[desired_offset]);
  else if (err == GRAPHD_ERR_NO)
    cl_leave(cl, CL_LEVEL_VERBOSE, "eof at %zu", gic->gic_n);
  else
    cl_leave(cl, CL_LEVEL_VERBOSE, "ps_nfri=%llx %s",
             (unsigned long long)ps->ps_next_find_resume_id,
             graphd_strerror(err));

  return err;
}

/**
 * @brief Will this iterator ever return a value beyond this one?
 *
 * @param graphd	module handle
 * @param it		iterator we're asking about
 * @param s		start of comparison value
 * @param e		end of comparison value
 * @param beyond_out	out: true if we're safely beyond this value.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int and_beyond(pdb_handle *pdb, pdb_iterator *it, char const *s,
                      char const *e, bool *beyond_out) {
  graphd_iterator_and *gia = it->it_theory;
  graphd_iterator_and *ogia;
  int err;
  char buf[200];
  pdb_iterator *p_it;

  /*  Something is out of sync?
   */
  if (!pdb_iterator_statistics_done(pdb, it) ||
      it->it_id != it->it_original->it_id || !pdb_iterator_ordered(pdb, it)) {
    cl_log(gia->gia_cl, CL_LEVEL_VERBOSE, "and_beyond: %s - returning false",
           !pdb_iterator_statistics_done(pdb, it)
               ? "no statistics yet"
               : (it->it_id != it->it_original->it_id
                      ? "original and instance ids don't match"
                      : "iterator isn't ordered"));

    *beyond_out = false;
    return 0;
  }

  ogia = it->it_original->it_theory;

  if (gia->gia_cache_offset_valid) {
    /* XXX  mix cache lookups and guarantees from
     *  below.
     */
    if (gia->gia_cache_offset < ogia->gia_cache->gic_n) {
      *beyond_out = false;

      cl_log(gia->gia_cl, CL_LEVEL_VERBOSE,
             "and_beyond: current offset "
             "isn't at end of cache - can't "
             "ask subiterators - returning false");
      return 0;
    }

    /*  If the last ID returned by the original's
     *  cache producer context is not equal to the
     *  highest id in the cache, we can't use the
     *  producer to do our beyond testing for us.
     */
    if (!graphd_iterator_and_cache_synced(ogia)) {
      cl_log(gia->gia_cl, CL_LEVEL_VERBOSE,
             "and_beyond: current ogia isn't "
             "in sync with cache - returning "
             "false");
      *beyond_out = false;
      return 0;
    }

    /*  Pass the request to the producer.  Since we're
     *  positioned at the end of the cache, the relevant
     *  producer is in the cache-maker state, not in our
     *  local clone state.
     */
    p_it = ogia->gia_cache_ps.ps_it[ogia->gia_producer];
  } else {
    /*  Pass the request to our local clone producer.
     */
    p_it = gia->gia_ps.ps_it[ogia->gia_producer];
  }
  cl_assert(gia->gia_cl, s != NULL);
  cl_assert(gia->gia_cl, e != NULL);
  cl_assert(gia->gia_cl, p_it != NULL);

  /*  Since I'm ordered, and since I picked that guy as my producer,
   *  I know that it'll be ordered, too, no matter whether it knows
   *  that.
   */
  pdb_iterator_ordered_set(pdb, p_it, true);
  pdb_iterator_ordering_set(pdb, p_it, pdb_iterator_ordering(pdb, it));

  err = pdb_iterator_beyond(pdb, p_it, s, e, beyond_out);

  cl_log(gia->gia_cl, CL_LEVEL_VERBOSE, "and_beyond: %s: %s",
         pdb_iterator_to_string(pdb, p_it, buf, sizeof buf),
         err ? graphd_strerror(err)
             : (*beyond_out ? "we're done" : "no, we can still go below that"));
  return err;
}

static int and_range_estimate(pdb_handle *pdb, pdb_iterator *it,
                              pdb_range_estimate *range) {
  graphd_iterator_and *gia = it->it_theory;
  graphd_iterator_and *ogia;
  and_process_state *ps;
  pdb_range_estimate sub_range;
  int err;
  char buf[200];
  size_t i;

  /*  Initialize with maximum permissive values.
   */
  range->range_low = it->it_low;
  range->range_high = it->it_high;
  range->range_n_exact = range->range_n_max = PDB_COUNT_UNBOUNDED;

  /*  Something is out of sync?
   */
  if (!pdb_iterator_statistics_done(pdb, it) ||
      it->it_id != it->it_original->it_id)
    return 0;

  ogia = it->it_original->it_theory;
  if (gia->gia_cache_offset_valid) {
    if (gia->gia_cache_offset < ogia->gia_cache->gic_n) return 0;

    /*  If the last ID returned by the original's
     *  cache producer context is not equal to the
     *  highest id in the cache, we need to resync
     *  with the cache before expanding it.
     */
    if (!graphd_iterator_and_cache_synced(ogia)) return 0;

    ps = &ogia->gia_cache_ps;
  } else {
    ps = &gia->gia_ps;
  }

  for (i = 0; i < ps->ps_n; i++) {
    err = pdb_iterator_range_estimate(pdb, ps->ps_it[i], &sub_range);
    if (err != 0) {
      if (err != PDB_ERR_NO) {
        cl_log_errno(gia->gia_cl, CL_LEVEL_FAIL, "pdb_iterator_range_estimate",
                     err, "it=%s", pdb_iterator_to_string(pdb, ps->ps_it[i],
                                                          buf, sizeof buf));
        return err;
      }
      continue;
    }

    if (range->range_low < sub_range.range_low)
      range->range_low = sub_range.range_low;

    if (range->range_high > sub_range.range_high)
      range->range_high = sub_range.range_high;

    if (sub_range.range_n_max != PDB_COUNT_UNBOUNDED &&
        sub_range.range_n_max < range->range_n_max)
      range->range_n_max = sub_range.range_n_max;

    /*  Our subiterator's exact count bounds our maximum,
     *  but doesn't become our exact count - we'd need to
     *  intersect it first!
     */
    if (sub_range.range_n_exact != PDB_COUNT_UNBOUNDED &&
        sub_range.range_n_exact < range->range_n_max)
      range->range_n_max = sub_range.range_n_exact;
  }
  return 0;
}

static int and_next_loc(pdb_handle *pdb, pdb_iterator *it, pdb_id *id_out,
                        pdb_budget *budget_inout, char const *file, int line) {
  graphd_iterator_and *gia = it->it_theory;
  int err;
  pdb_budget budget_in = *budget_inout;
  cl_handle *cl = gia->gia_cl;

  /* Come back when there's budget!
   */
  if (GRAPHD_SABOTAGE(gia->gia_graphd, *budget_inout < 0)) return PDB_ERR_MORE;

  pdb_rxs_push(pdb, "NEXT %p and", (void *)it);

retry:
  if (gia->gia_ps.ps_eof) {
    err = GRAPHD_ERR_NO;
    goto done;
  }
  err = graphd_iterator_and_access(pdb, it, budget_inout, 1.0);

  if (err != GRAPHD_ERR_ALREADY) {
    if (err == 0) {
      pdb_rxs_pop(pdb, "NEXT %p and redirect ($%lld)", (void *)it,
                  (long long)(budget_in - *budget_inout));
      return pdb_iterator_next_loc(pdb, it, id_out, budget_inout, file, line);
    }
    goto done;
  }

  if (gia->gia_resume_id != PDB_ID_NONE) {
    if (pdb_iterator_sorted(pdb, it)) {
      pdb_id id = gia->gia_resume_id;

      cl_log(cl, CL_LEVEL_DEBUG, "and_iterator_next: resuming to %llx",
             (unsigned long long)gia->gia_resume_id);

      if (pdb_iterator_forward(pdb, it))
        id++;
      else {
        if (id > 0)
          id--;
        else {
          gia->gia_ps.ps_eof = true;
          err = GRAPHD_ERR_NO;
          goto done;
        }
      }

      err = pdb_iterator_find_loc(pdb, it, id, &id, budget_inout, file, line);
      if (err == 0 || err == GRAPHD_ERR_NO) {
        gia->gia_resume_id = PDB_ID_NONE;
        if (err == 0)
          gia->gia_id = *id_out = id;
        else
          gia->gia_ps.ps_eof = true;
      }
      goto done;
    } else
      cl_log(cl, CL_LEVEL_VERBOSE,
             "resume ID %lld for an unsorted iterator; "
             "now at %lld",
             (long long)gia->gia_resume_id, (long long)gia->gia_id);
  }

  gia = it->it_theory;
  if (it->it_call_state == 0) {
    gia->gia_ps.ps_run_call_state = 0;

    /* Grow the cache. */
    if (gia->gia_cache_offset_valid) {
      cl_assert(cl, gia->gia_cache_offset <= ogia(it)->gia_cache->gic_n);
      err = graphd_iterator_cache_index(
          ogia(it)->gia_cache, gia->gia_cache_offset, id_out, budget_inout);

      if (err == 0)
        cl_log(cl, CL_LEVEL_VERBOSE, "and_next: cache[%zu of %zu] := %llu",
               gia->gia_cache_offset,
               ogia(it)->gia_cache ? ogia(it)->gia_cache->gic_n : 0,
               (unsigned long long)*id_out);

      if (err == GRAPHD_ERR_MORE) {
        err = and_iterator_cache_expand(pdb, it, budget_inout,
                                        gia->gia_cache_offset);
        if (err == PDB_ERR_MORE)
          goto done;

        else if (err != 0 && err != GRAPHD_ERR_NO) {
          /* Most likely, we ran out
           * of cache memory.
           */
          gia->gia_cache_offset_valid = false;
        } else {
          /*  We already paid for expanding
           *  the cache - no need to pay again
           *  for retrieving the thing we
           *  helped put in there in the first
           *  place.
           */
          pdb_budget dummy = 100000;

          err = graphd_iterator_cache_index(
              ogia(it)->gia_cache, gia->gia_cache_offset, id_out, &dummy);
          cl_assert(cl, err != PDB_ERR_MORE);
        }
      }

      if (err == 0 || err == GRAPHD_ERR_NO) {
        if (err == 0) {
          gia->gia_ps.ps_id = gia->gia_id = *id_out;
          gia->gia_cache_offset++;

          cl_assert(cl, gia->gia_cache_offset <= ogia(it)->gia_cache->gic_n);

          if (gia->gia_resume_id != PDB_ID_NONE) {
            /*  If we're resuming an unsorted iterator, we need
             *  to wait for the resume-id to actually float past.
             *  This is where it is recognized, and we now start
             *  what hopefully is our final approach into the
             *  results.
                                     */
            if (*id_out == gia->gia_resume_id) gia->gia_resume_id = PDB_ID_NONE;
            cl_log(cl, CL_LEVEL_VERBOSE, "resume: pass %lld",
                   (long long)*id_out);
            goto retry;
          }

        } else {
          gia->gia_ps.ps_id = PDB_ID_NONE;
          gia->gia_id = PDB_ID_NONE;
          gia->gia_ps.ps_eof = true;
        }
        goto done;
      }

      /*  For whatever reason, we've lost traction
       *  in the cache.  To recover, we need to
       *  position the iterator state at the end
       *  of the cache.
       *
       *  The easiest way to do that is to clone
       *  the cache iterator from ogia(it).
       */
      cl_log(cl, CL_LEVEL_FAIL,
             "*** and_next_loc: lost "
             "cache - recovering ***");

      gia->gia_cache_offset_valid = false;
      if (gia->gia_ps.ps_it != NULL)
        graphd_iterator_and_process_state_finish(ogia(it), &gia->gia_ps);
      cl_assert(cl, &ogia(it)->gia_cache_ps.ps_it != NULL);
      err = graphd_iterator_and_process_state_clone(
          pdb, it, &ogia(it)->gia_cache_ps, &gia->gia_ps);
      if (err != 0) goto done;
    }

    /*  We tried this and it didn't
     *  help.  Don't try again next
     *  time.
     */
    it->it_call_state = 1;
  }
  err = graphd_iterator_and_run(it, ogia(it)->gia_producer, &gia->gia_ps,
                                budget_inout);
  if (err == PDB_ERR_MORE) goto done;

  it->it_call_state = 0;
  if (err != 0) {
    if (err == GRAPHD_ERR_NO) gia->gia_ps.ps_eof = true;
    gia->gia_id = gia->gia_ps.ps_id = PDB_ID_NONE;
    goto done;
  }

  gia->gia_id = *id_out = gia->gia_ps.ps_id;
  if (gia->gia_resume_id != PDB_ID_NONE) {
    /*  If we're resuming an unsorted iterator, we need
     *  to wait for the resume-id to actually float past.
     *  This is where it is recognized, and we now start
     *  what hopefully is our final approach into the
     *  results.
     */
    if (*id_out == gia->gia_resume_id) gia->gia_resume_id = PDB_ID_NONE;
    goto retry;
  }
done:
  if (err == 0) {
    ogia(it)->gia_n_produced++;
    pdb_rxs_pop(pdb, "NEXT %p and %llx ($%lld)", (void *)it,
                (unsigned long long)*id_out,
                (long long)(budget_in - *budget_inout));
  } else if (err == GRAPHD_ERR_NO)
    pdb_rxs_pop(pdb, "NEXT %p and EOF ($%lld)", (void *)it,
                (long long)(budget_in - *budget_inout));

  else if (err == PDB_ERR_MORE)
    pdb_rxs_pop(pdb, "NEXT %p and suspended; state=%d ($%lld)", (void *)it,
                it->it_call_state, (long long)(budget_in - *budget_inout));
  else
    pdb_rxs_pop(pdb, "NEXT %p and %s ($%lld)", (void *)it,
                err == GRAPHD_ERR_NO ? "done" : graphd_strerror(err),
                (long long)(budget_in - *budget_inout));

  ogia(it)->gia_total_cost_produce += budget_in - *budget_inout;
  pdb_iterator_account_charge_budget(pdb, it, next);

  return err;
}

static int and_reset(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_and *gia = it->it_theory;

  pdb_rxs_log(pdb, "RESET %p and", (void *)it);

  pdb_iterator_call_reset(pdb, it);

  gia->gia_resume_id = PDB_ID_NONE;
  gia->gia_id = PDB_ID_NONE;
  gia->gia_ps.ps_eof = false;
  gia->gia_cache_offset = 0;
  gia->gia_cache_offset_valid = true;

  return 0;
}

static int and_clone(pdb_handle *pdb, pdb_iterator *it, pdb_iterator **it_out) {
  pdb_iterator *it_orig = it->it_original;
  graphd_iterator_and *gia = it->it_theory;
  graphd_iterator_and *gia2;

  cm_handle *cm = gia->gia_cm;
  cl_handle *cl = gia->gia_cl;
  int err;
  char buf[200], ibuf[200];

  PDB_IS_ITERATOR(cl, it);
  PDB_IS_ORIGINAL_ITERATOR(cl, it_orig);

  /*  If the original iterator has evolved into something
   *  other than an "and" iterator, clone that iterator
   *  directly and reset it.  If we had a position to save,
   *  we would have already evolved.
   */
  if (it_orig->it_type != it->it_type || it_orig->it_id != it->it_id) {
    err = pdb_iterator_clone(pdb, it_orig, it_out);
    if (err != 0) return err;

    err = pdb_iterator_reset(pdb, *it_out);
    if (err != 0) pdb_iterator_destroy(pdb, it_out);
    return err;
  }

  cl_assert(cl, gia->gia_n > 0);
  cl_enter(cl, CL_LEVEL_VERBOSE, "it=%s",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf));

  pdb_rxs_push(pdb, "CLONE %p and", (void *)it);

  if ((gia2 = cm_malcpy(cm, gia, sizeof(*gia))) == NULL) {
    err = errno ? errno : ENOMEM;

    pdb_rxs_pop(pdb, "CLONE %p and - %s", (void *)it, graphd_strerror(err));
    cl_leave(cl, CL_LEVEL_VERBOSE, "error in cm_malcpy: %s",
             graphd_strerror(err));
    return err;
  }
  if ((err = pdb_iterator_make_clone(pdb, it_orig, it_out)) != 0) {
    pdb_rxs_pop(pdb, "CLONE %p and - %s", (void *)it, graphd_strerror(err));
    cl_leave(cl, CL_LEVEL_VERBOSE, "error in pdb_iterator_make_clone: %s",
             graphd_strerror(err));
    cm_free(cm, gia2);
    return err;
  }
  (*it_out)->it_theory = gia2;

  /* Slow check state.  Clone will make its own.
   */
  gia2->gia_scs = NULL;

  /*  Iterators - not used in the clone.
   */
  gia2->gia_sc = NULL;
  gia2->gia_m = 0;

  /*  Check order - not used in the clone.
   */
  gia2->gia_check_order = NULL;

  /*  Contest order - not used in the clone.
   */
  gia2->gia_contest_order = NULL;
  gia2->gia_contest_max_turn = 0;

  /*  Cache - not used in the clone.
   */
  gia2->gia_cache = NULL;
  graphd_iterator_and_process_state_clear(&gia2->gia_cache_ps);

  gia2->gia_ps.ps_check_i = 0;
  (*it_out)->it_has_position = true;

  /*  Clear the process states of the clone; if we want
   *  to use those, we need to explicitly clone them.
   */
  graphd_iterator_and_process_state_clear(&gia2->gia_ps);

  /*  Preserve the "EOF" flag - if all the caller does is
   *  go forward, it'll help us avoid doing statistics again.
   */
  gia2->gia_ps.ps_eof = gia->gia_ps.ps_eof;

  /*  If the original has statistics, clone its iterators
   *  and with that its position (if it's outside the cache).
   *
   *  Otherwise, wait with that.
   */
  if (pdb_iterator_statistics_done(pdb, it)) {
    cl_assert(cl, &ogia(it)->gia_cache_ps.ps_it != NULL);
    if (gia->gia_ps.ps_it != NULL) {
      err = graphd_iterator_and_process_state_clone(pdb, it, &gia->gia_ps,
                                                    &gia2->gia_ps);
      if (err != 0) {
        pdb_rxs_pop(pdb, "CLONE %p and - %s", (void *)it, graphd_strerror(err));
        pdb_iterator_destroy(pdb, it_out);
        cl_leave(cl, CL_LEVEL_VERBOSE,
                 "error in "
                 "graphd_iterator_and_process_state_clone: %s",
                 graphd_strerror(err));
        return err;
      }
    }
  }

  /*  If the original has no position, reset the clone to the
   *  beginning.  Otherwise, keep the position as copied from
   *  <it>.
   */
  if (!pdb_iterator_has_position(pdb, it)) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "and_clone: original has no "
           "position - resetting to the start");

    pdb_iterator_reset(pdb, *it_out);
  }

  pdb_rxs_pop(pdb, "CLONE %p and %p", (void *)it, (void *)*it_out);

  cl_leave(cl, CL_LEVEL_VERBOSE, "%p -> %p: %s [ref=%d, pos=%s]", (void *)it,
           (void *)*it_out,
           pdb_iterator_to_string(pdb, *it_out, ibuf, sizeof ibuf),
           (int)(*it_out)->it_original->it_refcount,
           graphd_iterator_and_position_string(pdb, it, buf, sizeof buf));
  return 0;
}

static void and_finish(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_and *gia = it->it_theory;
  size_t i;

  if (gia != NULL) {
    cl_handle *cl = gia->gia_cl;

    cl_enter(cl, CL_LEVEL_VERBOSE, "it=%p (o=%p)", (void *)it,
             (void *)it->it_original);

    graphd_iterator_and_slow_check_finish(pdb, it);
    graphd_iterator_and_process_state_finish(gia, &gia->gia_ps);

    if (it->it_original == it) {
      graphd_iterator_cache_destroy(gia->gia_cache);
      gia->gia_cache = NULL;

      graphd_iterator_and_process_state_finish(gia, &gia->gia_cache_ps);

      if (gia->gia_sc != NULL) {
        graphd_subcondition *sc;

        for (i = 0, sc = gia->gia_sc; i < gia->gia_n; i++, sc++)
          and_subcondition_finish(gia, sc);

        cm_free(gia->gia_cm, gia->gia_sc);
        gia->gia_sc = NULL;
      }
      cm_free(gia->gia_cm, gia->gia_check_order);
      cm_free(gia->gia_cm, gia->gia_contest_order);
    }
    cm_free(gia->gia_cm, it->it_displayname);
    it->it_displayname = NULL;

    cm_free(gia->gia_cm, gia);
    it->it_theory = NULL;

    cl_leave(cl, CL_LEVEL_VERBOSE, "it=%p", (void *)it);
  }
  it->it_type = NULL;
  it->it_magic = 0;
  it->it_original = NULL;
}

static char const *and_to_string(pdb_handle *pdb, pdb_iterator *it, char *buf,
                                 size_t size) {
  graphd_iterator_and *gia;
  graphd_iterator_and *ogia;
  char producer_buf[200];
  char b2[200], b3[200];
  int i;
  graphd_subcondition *sc;
  char const *sub_string;

  /* Cached? */
  if (it->it_displayname != NULL)
  /*
          return it->it_displayname;
  */
  {
    gia = it->it_theory;
    cm_free(gia->gia_cm, it->it_displayname);
    it->it_displayname = NULL;
  }

  /* Underneath has changed?  */
  if (it->it_original != it) {
    if (it->it_original == NULL) {
      snprintf(buf, size, "[and clone %p]", (void *)it);
      return buf;
    }
    if (it->it_original->it_type != &graphd_iterator_and_type ||
        it->it_id != it->it_original->it_id) {
      snprintf(buf, size, "and**%.*s", (int)size - 6,
               pdb_iterator_to_string(pdb, it->it_original, b2, sizeof b2));
      return buf;
    }
  }

  ogia = it->it_original->it_theory;
  gia = it->it_theory;

  if (gia == NULL || ogia->gia_n == 0) return "and:(no contents)";

  sc = ogia->gia_sc + ogia->gia_producer;
  b2[0] = b3[0] = '\0';
  if ((i = (ogia->gia_producer == 1) ? 0 : 1) < gia->gia_n)
    snprintf(b2, sizeof b2, " + %s",
             pdb_iterator_to_string(pdb, ogia->gia_sc[i].sc_it, producer_buf,
                                    sizeof producer_buf));

  if ((i = (ogia->gia_producer == 2) ? 1 : 2) < ogia->gia_n)
    snprintf(b3, sizeof b3, " + %s",
             pdb_iterator_to_string(pdb, ogia->gia_sc[i].sc_it, producer_buf,
                                    sizeof producer_buf));

  sub_string =
      pdb_iterator_to_string(pdb, ogia->gia_sc[ogia->gia_producer].sc_it,
                             producer_buf, sizeof producer_buf);

  snprintf(buf, size, "%s%sand[%d: %s%s%s%s]", it->it_forward ? "" : "~",
           pdb_iterator_statistics_done(pdb, it) ? "" : "*",
           (int)ogia->gia_producer, sub_string, b2, b3,
           gia->gia_n > 3 ? " + ..." : "");
  /*
          it->it_displayname = cm_strmalcpy(gia->gia_cm, buf);
  */

  return buf;
}

/**
 * @brief Return the primitive summary for an AND iterator.
 *
 * @param pdb		module handle
 * @param it		an and iterator
 * @param psum_out	out: summary
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int and_primitive_summary(pdb_handle *pdb, pdb_iterator *it,
                                 pdb_primitive_summary *psum_out) {
  int err;
  size_t i;
  graphd_iterator_and *gia;
  bool have_off_result = false, have_result = false;
  pdb_primitive_summary off_result;

  /* silence gcc 4.2 warnings */
  memset(&off_result, 0, sizeof off_result);

  /*  Defer to the original.  It may have a different type.
   */
  if (it->it_original != it)
    return pdb_iterator_primitive_summary(pdb, it->it_original, psum_out);

  gia = it->it_theory;

  psum_out->psum_locked = 0;
  psum_out->psum_result = PDB_LINKAGE_N;
  psum_out->psum_complete = true;

  for (i = 0; i < gia->gia_n; i++) {
    pdb_primitive_summary sub;
    int l;

    err = pdb_iterator_primitive_summary(pdb, gia->gia_sc[i].sc_it, &sub);
    if (err != 0) {
      if (err == GRAPHD_ERR_NO) {
        psum_out->psum_complete = false;
        continue;
      }
      return err;
    }

    psum_out->psum_complete &= !!sub.psum_complete;
    if (sub.psum_result != PDB_LINKAGE_N) {
      off_result = sub;
      have_off_result = true;

      continue;
    }
    have_result = true;

    /*  If linkages contradict each other, this may lead
     *  to too-large results -- doesn't matter.
     */
    for (l = 0; l < PDB_LINKAGE_N; l++)
      if (sub.psum_locked & (1 << l)) psum_out->psum_guid[l] = sub.psum_guid[l];

    psum_out->psum_locked |= sub.psum_locked;
  }
  if (!have_result) {
    if (!have_off_result)
      return GRAPHD_ERR_NO;

    else {
      bool tmp = psum_out->psum_complete;
      *psum_out = off_result;
      psum_out->psum_complete = tmp;
    }
  }
  return 0;
}

static int and_iterator_partial_dup(pdb_iterator const *and_in, size_t first_n,
                                    pdb_iterator **it_out) {
  size_t i;
  int err;
  pdb_handle *pdb = ogia(and_in)->gia_pdb;

  err = graphd_iterator_and_create(ogia(and_in)->gia_greq, ogia(and_in)->gia_n,
                                   and_in->it_low, and_in->it_high,
                                   ogia(and_in)->gia_direction,
                                   pdb_iterator_ordering(pdb, and_in), it_out);
  if (err != 0) return err;

  for (i = 0; i < first_n; i++) {
    pdb_iterator *it_clone;

    err = pdb_iterator_clone(pdb, ogia(and_in)->gia_sc[i].sc_it, &it_clone);
    if (err != 0) return err;

    err = graphd_iterator_and_add_subcondition(ogia(and_in)->gia_graphd,
                                               *it_out, &it_clone);
    pdb_iterator_destroy(pdb, &it_clone);
    if (err != 0) return err;
  }
  return 0;
}

static int and_iterator_restrict(pdb_handle *pdb, pdb_iterator *it,
                                 pdb_primitive_summary const *psum,
                                 pdb_iterator **it_out) {
  size_t i;
  int err = 0;
  pdb_iterator *pending_it = NULL;
  pdb_iterator *res_it = NULL;
  cl_handle *cl = ogia(it)->gia_cl;
  char buf[200];

  /*  If one of our subconstraints has a primitive
   *  summary that directly contradicts {psum}, make
   *  a new "or" iterator that doesn't contain that
   *  subconstraint.  Or any of the others that contradict
   *  it.
   */

  *it_out = NULL;
  for (i = 0; i < ogia(it)->gia_n; i++) {
    res_it = NULL;
    err = pdb_iterator_restrict(pdb, ogia(it)->gia_sc[i].sc_it, psum, &res_it);

    if (err == PDB_ERR_NO) {
      /*  If one of the subiterators conflicts,
       *  everything conflicts.  End of story!
       */
      pdb_iterator_destroy(pdb, it_out);
      return err;
    }

    /*  Most common case: they're all included unchanged?
     */
    if (*it_out == NULL && pending_it == NULL && err == PDB_ERR_ALREADY)
      continue;

    if (err == PDB_ERR_ALREADY) {
      /*  Make the accepted iterator a clone.
       */
      err = pdb_iterator_clone(pdb, ogia(it)->gia_sc[i].sc_it, &res_it);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "it=%s",
                     pdb_iterator_to_string(pdb, ogia(it)->gia_sc[i].sc_it, buf,
                                            sizeof buf));
        goto err;
      }
    }

    if (err != 0) goto err;

    cl_assert(cl, res_it != NULL);

    /*  This is the first modified one
     *  after a series of plain accepted ones?
     */
    if (*it_out == NULL && pending_it == NULL && i > 0) {
      if (i == 1) {
        err = pdb_iterator_clone(pdb, ogia(it)->gia_sc->sc_it, &pending_it);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "it=%s",
                       pdb_iterator_to_string(pdb, ogia(it)->gia_sc->sc_it, buf,
                                              sizeof buf));
          goto err;
        }
      } else {
        /*  Make an AND with accepted subiterators
         *  up to, excluding, i.
         */
        err = and_iterator_partial_dup(it, i, it_out);
        if (err != 0) goto err;
      }
    }
    cl_assert(cl, err == 0);

    /*  If we will have to keep track of two iterators,
     *  create an "and" to hold them.  If there was a pending
     *  single iterator, fold it into the "and".
     */
    if (pending_it != NULL && *it_out == NULL) {
      err = and_iterator_partial_dup(it, 0, it_out);
      if (err != 0) goto err;

      err = graphd_iterator_and_add_subcondition(ogia(it)->gia_graphd, *it_out,
                                                 &pending_it);
      pdb_iterator_destroy(pdb, &pending_it);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_and_add_subcondition",
                     err, "it=%s",
                     pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
        goto err;
      }
    }

    /*  We've transferred all the information we were
     *  keeping track of into <*it_out>.
     *  <pending_it>, if needed, has been moved.
     */
    cl_assert(cl, pending_it == NULL);
    cl_assert(cl, res_it != NULL);

    if (*it_out) {
      err = graphd_iterator_and_add_subcondition(ogia(it)->gia_graphd, *it_out,
                                                 &res_it);
      pdb_iterator_destroy(pdb, &res_it);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_and_add_subcondition",
                     err, "it=%s",
                     pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
        goto err;
      }
    } else {
      pending_it = res_it;
      res_it = NULL;
    }

    cl_assert(cl, res_it == NULL);
  }
  if (*it_out == NULL) {
    if (pending_it != NULL) {
      *it_out = pending_it;
      return 0;
    }
    return PDB_ERR_ALREADY;
  }
  graphd_iterator_and_create_commit(ogia(it)->gia_graphd, *it_out);
  return 0;

err:
  pdb_iterator_destroy(pdb, &pending_it);
  pdb_iterator_destroy(pdb, &res_it);
  pdb_iterator_destroy(pdb, it_out);

  return err;
}

const pdb_iterator_type graphd_iterator_and_type = {
    "and",

    and_finish,
    and_reset,
    and_clone,
    graphd_iterator_and_freeze,
    and_to_string,

    and_next_loc,
    graphd_iterator_and_find_loc,
    graphd_iterator_and_check,
    graphd_iterator_and_statistics,

    NULL, /* idarray  */
    and_primitive_summary,
    and_beyond,
    and_range_estimate,
    and_iterator_restrict,

    NULL, /* suspend */
    NULL  /* unsuspend */
};

/**
 * @brief Create an "and" structure.
 *
 *  The caller will use graphd_iterator_and_add_subcondition() calls
 *  to populate it, and finish off with a single call
 *  to graphd_iterator_and_create_commit().
 *
 * @param greq		request for whom we're doing this
 * @param n		guessed number of subconditions we expect to manage
 * @param low		low end of the value space (first included)
 * @param high		high end of the value space (first not included)
 * @param direction	do we prefer a particular direction?  Which?
 * @param ordering	if non-NULL, desired sortroot ordering
 * @param it_out	assign result to this.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_and_create_loc(graphd_request *greq, size_t n,
                                   unsigned long long low,
                                   unsigned long long high,
                                   graphd_direction direction,
                                   char const *ordering, pdb_iterator **it_out,
                                   char const *file, int line) {
  graphd_handle *graphd = graphd_request_graphd(greq);
  pdb_iterator *it = NULL;
  graphd_iterator_and *ogia = NULL;
  cm_handle *cm = pdb_mem(graphd->g_pdb);
  cl_handle *cl = pdb_log(graphd->g_pdb);
  int err;

  cl_assert(cl, n > 0);
  cl_assert(cl, (int)n > 0);
  cl_assert(cl, GRAPHD_DIRECTION_VALID(direction));

  if ((it = cm_malloc(cm, sizeof(*it))) == NULL ||
      (ogia = cm_zalloc(cm, sizeof(*ogia))) == NULL ||
      (ogia->gia_sc = cm_zalloc(cm, sizeof(*ogia->gia_sc) * n)) == NULL) {
    err = errno ? errno : ENOMEM;

    cm_free(cm, it);
    cm_free(cm, ogia);

    return err;
  }
  ogia->gia_cache = graphd_iterator_cache_create(graphd, 8);
  if (ogia->gia_cache == NULL) {
    cm_free(cm, ogia->gia_sc);
    cm_free(cm, ogia);
    cm_free(cm, it);

    return ENOMEM;
  }

  ogia->gia_magic = GRAPHD_AND_MAGIC;
  ogia->gia_graphd = graphd;
  ogia->gia_pdb = graphd->g_pdb;
  ogia->gia_greq = greq;
  ogia->gia_cl = cl;
  ogia->gia_cm = cm;
  ogia->gia_n = 0;
  ogia->gia_m = n;
  ogia->gia_direction = direction;
  ogia->gia_cache_offset = 0;
  ogia->gia_cache_offset_valid = true;
  ogia->gia_contest_max_turn = 10;
  ogia->gia_resume_id = PDB_ID_NONE;
  ogia->gia_id = PDB_ID_NONE;
  ogia->gia_producer_hint = -1;
  ogia->gia_original_version = 0;

  graphd_iterator_and_process_state_clear(&ogia->gia_ps);
  graphd_iterator_and_process_state_clear(&ogia->gia_cache_ps);

  pdb_iterator_make_loc(graphd->g_pdb, it, low, high,
                        direction != GRAPHD_DIRECTION_BACKWARD, file, line);

  if (ordering != NULL) {
    ordering = graphd_iterator_ordering_internalize_request(
        greq, ordering, ordering + strlen(ordering));
    pdb_iterator_ordering_set(pdb, it, ordering);
  }
  it->it_theory = ogia;
  it->it_type = &graphd_iterator_and_type;

  PDB_IS_ITERATOR(cl, it);
  GRAPHD_IS_AND(cl, ogia);

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_and_create (up to %d slots "
         "between %llx and %llx): %p; ordering %d/%s [from %s:%d]",
         (int)ogia->gia_m, low, high, (void *)it, ogia->gia_direction,
         ordering ? ordering : "null", file, line);
  *it_out = it;

  return 0;
}

/*
 * Combine all value bin iterators in an AND into a single value-bin
 * iterators that iterates over the intersection.
 *
 * This may turn the AND iterator into something else (either a null or binned
 * iterator)
 */

/**
 * @brief Annotate an "and" with its setsize.
 *
 *  The setsize is the number of and iterator values we expect
 *  to successfully _use_ once it's combined with a parent hint
 *  ("child of #GUID"), if we read all the way to the end
 *  (disregarding a pagesize).
 *
 *  The relative ratio of pagesize and setsize is important for
 *  deciding how much to invest to remain sorted.
 *
 * @param graphd	system handle
 * @param it		the iterator
 * @param setsize	the context set size
 */
void graphd_iterator_and_set_context_setsize(graphd_handle *graphd,
                                             pdb_iterator *it,
                                             unsigned long long setsize) {
  graphd_iterator_and *ogia;
  cl_handle *cl;

  /* The "and" iterator may turn into a "null" iterator
   * if one adds a null iterator to it.  In that case,
   * there are no results, and we don't care about
   * sizing and sorting.
   */
  it = it->it_original;
  if (it->it_type != &graphd_iterator_and_type) return;

  ogia = it->it_theory;
  cl = ogia->gia_cl;
  PDB_IS_ITERATOR(cl, it);

  ogia->gia_context_setsize = setsize;
}

/**
 * @brief Annotate an "and" with its pagesize.
 *
 *  The pagesize is the number of and iterator values we expect
 *  to successfully _use_ once it's combined with a parent hint
 *  ("child of #GUID"), if the iterator is sorted.
 *
 *  The relative ratio of pagesize and setsize is important for
 *  deciding how much to invest to remain sorted.
 *
 * @param graphd	system handle
 * @param it		the iterator
 * @param setsize	the context set size
 */
void graphd_iterator_and_set_context_pagesize(graphd_handle *graphd,
                                              pdb_iterator *it,
                                              unsigned long long pagesize) {
  graphd_iterator_and *ogia;
  cl_handle *cl;

  /* The "and" iterator may turn into a "null" iterator
   * if one adds a null iterator to it.  In that case,
   * there are no results, and we don't care about
   * sizing and sorting.
   */
  if (it->it_type != &graphd_iterator_and_type) return;

  ogia = it->it_theory;
  cl = ogia->gia_cl;
  PDB_IS_ITERATOR(cl, it);

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_iterator_and_set_context_pagesize %llu",
         pagesize);

  ogia->gia_context_pagesize = pagesize;
  ogia->gia_context_pagesize_valid = true;
}

/**
 * @brief Finish creating an "and" structure.
 *
 * @param it	the completed iterator
 * @return 0 on success, a nonzero error code on error
 */
int graphd_iterator_and_create_commit(graphd_handle *graphd, pdb_iterator *it) {
  graphd_iterator_and *ogia;
  cl_handle *cl;
  pdb_handle *pdb = graphd->g_pdb;
  pdb_iterator *new_it;
  char buf[200];
  int err = 0;

  /* The "and" iterator may turn into a "null" iterator
   * if one adds a null iterator to it.
   */
  if (it->it_type != &graphd_iterator_and_type) return 0;

  ogia = it->it_theory;
  cl = ogia->gia_cl;
  PDB_IS_ITERATOR(cl, it);

  cl_enter(cl, CL_LEVEL_VERBOSE, "(%p:%s) in %llx..%llx", it,
           pdb_iterator_to_string(pdb, it, buf, sizeof buf), it->it_low,
           it->it_high);
  cl_assert(cl, it->it_type == &graphd_iterator_and_type);
  cl_assert(cl, !ogia->gia_committed);

  if (ogia->gia_n == 0) {
    /* No conditions - return everything. */

    err = pdb_iterator_all_create(pdb, it->it_low, it->it_high, it->it_forward,
                                  &new_it);
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE,
               "unexpected "
               "error from pdb_iterator_all_create: %s",
               graphd_strerror(err));
      return err;
    }
    graphd_iterator_set_direction_ordering(pdb, new_it, ogia->gia_direction,
                                           pdb_iterator_ordering(pdb, it));

    err = graphd_iterator_substitute(ogia->gia_greq, it, new_it);
    cl_assert(cl, err == 0);

    cl_leave(cl, CL_LEVEL_VERBOSE, "everything");
    return 0;
  }

  /*  If any of the subconditions is a NULL,
   *  the total is a NULL.
   */
  if (and_is_null(pdb, it)) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "became null");
    return pdb_iterator_null_become(pdb, it);
  }

  if (!ogia->gia_thaw) {
    err = graphd_iterator_and_optimize(graphd, it);
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE,
               "unexpected "
               "error from graphd_iterator_and_optimize: %s",
               graphd_strerror(err));
      return err;
    }
  }

  /*  At this point, we have *something*.  It may be a null iterator,
   *  or a fixed iterator, but it's an iterator.
   */
  cl_assert(cl, it->it_type != NULL);
  PDB_IS_ITERATOR(cl, it);

  if (it->it_type == &graphd_iterator_and_type && it->it_theory == ogia) {
    ogia->gia_committed = true;
    pdb_rxs_log(pdb, "CREATE %p and[%zu]", (void *)it, ogia->gia_n);
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s",
           err ? graphd_strerror(err)
               : pdb_iterator_to_string(pdb, it, buf, sizeof buf));
  return err;
}

/**
 * @brief Add a condition to an AND.
 *
 * @param gia_it structure to add to
 * @param sub_it pdb iterator for the subcondition.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_and_add_subcondition(graphd_handle *graphd,
                                         pdb_iterator *gia_it,
                                         pdb_iterator **sub_it) {
  graphd_iterator_and *ogia;
  pdb_handle *pdb = graphd->g_pdb;
  cl_handle *cl;
  cm_handle *cm;
  graphd_subcondition *sc;
  char buf[200];
  int err = 0;
  pdb_id *fix_id;
  size_t fix_n;

  /* If the and-iterator is not an and-iterator, it
   * is probably a NULL.  Leave it alone, just delete
   * the subiterator.
   */
  if (gia_it->it_type != &graphd_iterator_and_type) {
    pdb_iterator_destroy(graphd->g_pdb, sub_it);
    return 0;
  }

  ogia = gia_it->it_theory;
  cl = ogia->gia_cl;
  cm = ogia->gia_cm;

  cl_assert(cl, *sub_it != NULL);
  cl_assert(cl, (*sub_it)->it_type != NULL);
  cl_assert(cl, (*sub_it)->it_type->itt_name != NULL);

  cl_assert(cl, !ogia->gia_committed);
  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_and_add_subcondition %p:%s (%s%s) to %p",
         (void *)*sub_it, pdb_iterator_to_string(pdb, *sub_it, buf, sizeof buf),
         pdb_iterator_ordered_valid(pdb, (*sub_it)) &&
                 pdb_iterator_ordered(pdb, (*sub_it))
             ? "ordering:"
             : "unordered",
         (pdb_iterator_ordered_valid(pdb, (*sub_it)) &&
                  pdb_iterator_ordered(pdb, (*sub_it))
              ? (pdb_iterator_ordering(pdb, *sub_it)
                     ? pdb_iterator_ordering(pdb, *sub_it)
                     : "(null)")
              : ""),
         (void *)gia_it);

  /*  Adding a null to an and iterator turns it into a null.
   */
  if (pdb_iterator_null_is_instance(pdb, *sub_it)) {
    err = pdb_iterator_substitute(pdb, gia_it, *sub_it);
    cl_assert(cl, err == 0);
    *sub_it = NULL;

    cl_log(cl, CL_LEVEL_VERBOSE, "turned to null");
    return 0;
  }

  PDB_IS_ITERATOR(cl, gia_it);
  cl_assert(cl, *sub_it != NULL);
  cl_assert(cl, gia_it->it_type == &graphd_iterator_and_type);
  cl_assert(cl, gia_it->it_original == gia_it);

  /*  The caller must be holding the only reference to this original.
   *  Otherwise, we can't move it!
   */
  /* Huh?  We're not moving it!
  cl_assert(cl, (*sub_it)->it_refcount == 1);
  */

  /*  The added iterator must have a valid position.
   */
  cl_assert(cl, pdb_iterator_has_position(cl, *sub_it));

  /*  If the incoming iterator has tighter range constraints
   *  than the "and" as a whole, adopt those constraints.
   */
  if ((*sub_it)->it_high < gia_it->it_high)
    gia_it->it_high = (*sub_it)->it_high;

  if ((*sub_it)->it_low > gia_it->it_low) gia_it->it_low = (*sub_it)->it_low;

  if ((*sub_it)->it_original != *sub_it) {
    err = pdb_iterator_refresh_pointer(pdb, sub_it);
    if (err != 0 && err != PDB_ERR_ALREADY) return err;
    err = 0;
  }

  /*  The subiterator direction is the same as that
   *  of the "and".  This is true even if the subiterator
   *  isn't sorted.  (It may get replaced by something
   *  sorted later.)
   */
  cl_assert(cl, (*sub_it)->it_forward == gia_it->it_forward);

  /*  If the incoming iterator is an AND,
   *  merge its subconstraints into this one.
   */

  /*
   * Don't do any of this code for freshly thawed iterators.
   * Instead of re-optimizing, just reload the iterator exactly
   * the way we left it.
   */
  if (!ogia->gia_thaw) {
    if ((*sub_it)->it_type == &graphd_iterator_and_type) {
      graphd_iterator_and *sub_gia = (*sub_it)->it_theory;
      graphd_iterator_and *sub_ogia = ogia(*sub_it);
      size_t i;
      int err;
      pdb_iterator **subsub_ptr, *subsub;

      /*  There are three different situations here.
       *  (a) sub_it is a fully developed clone:
       * 	move its positioning state.
       *  (b) sub_it is an undeveloped clone.
       * 	clone the sc_its of its original.
       *  (c) sub_it is an original of any kind: move
       * 	its sc_it.
       */
      for (i = 0; i < sub_gia->gia_n; i++) {
        /*  If we own the reference to the original,
         *  we can steal from the original.
         */
        if (*sub_it == (*sub_it)->it_original && (*sub_it)->it_refcount == 1) {
          subsub_ptr = &sub_ogia->gia_sc[i].sc_it;
        }

        /*  If we own the reference to this clone,
         *  and the clone has state, we can
         *  steal from the state.
         */
        else if (*sub_it != (*sub_it)->it_original &&
                 sub_gia->gia_ps.ps_it != NULL)
          subsub_ptr = sub_gia->gia_ps.ps_it + i;

        /*  Oh well, looks like we need to clone.
         */
        else {
          err = pdb_iterator_clone(pdb, sub_ogia->gia_sc[i].sc_it,
                                   subsub_ptr = &subsub);
          if (err != 0) return err;
        }

        PDB_IS_ITERATOR(cl, *subsub_ptr);
        err = graphd_iterator_and_add_subcondition(graphd, gia_it, subsub_ptr);
        if (err != 0) {
          pdb_iterator_destroy(pdb, subsub_ptr);
          return err;
        }

        /*  The successful
         * graphd_iterator_and_add_subcondition
         *  zeroed out the subcondition pointer.
         */
        cl_assert(cl, *subsub_ptr == NULL);
      }
      pdb_iterator_destroy(pdb, sub_it);
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_and_add_subcondition: "
             "pulled in the whole subconstraint for %p.",
             gia_it);
      return 0;
    }

    /*  If the incoming iterator is an ALL, and we already have
     *  an ALL, just merge the two.
     */
    else if (pdb_iterator_all_is_instance(pdb, *sub_it)) {
      size_t i;
      graphd_subcondition *sc;

      /*  If we already have an "all", just adjust the
       *  boundaries of the existing "all".
       */
      for (i = ogia->gia_n, sc = ogia->gia_sc; i--; sc++) {
        if (pdb_iterator_all_is_instance(pdb, sc->sc_it)) {
          if ((*sub_it)->it_low > sc->sc_it->it_low)
            sc->sc_it->it_low = (*sub_it)->it_low;

          if ((*sub_it)->it_high < sc->sc_it->it_high)
            sc->sc_it->it_high = (*sub_it)->it_high;

          pdb_iterator_destroy(pdb, sub_it);
          cl_log(cl, CL_LEVEL_VERBOSE,
                 "graphd_iterator_and_add_subcondition: "
                 "merged \"all\" into an existing one.");
          return 0;
        }
      }
    }

    /*  If the incoming iterator is a FIXED, try merging that into
     *  an existing FIXED, too.
     */
    else if (graphd_iterator_fixed_is_instance(pdb, *sub_it, &fix_id, &fix_n)) {
      err = and_merge_fixed(pdb, gia_it, sub_it);
      if (err == 0)
        return 0;
      else if (err != GRAPHD_ERR_NO && err != GRAPHD_ERR_ALREADY) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "and_merge_fixed", err,
                     "subiterator=%s",
                     pdb_iterator_to_string(pdb, *sub_it, buf, sizeof buf));
        return err;
      }
    }
  }
  if (ogia->gia_n >= ogia->gia_m) {
    size_t need = ogia->gia_n + 16;

    /*  Make space for a few more subiterator elements.
     */
    sc = cm_realloc(cm, ogia->gia_sc, need * sizeof(*ogia->gia_sc));
    if (sc == NULL) {
      err = errno ? errno : ENOMEM;
      cl_log_errno(cl, CL_LEVEL_FAIL, "cm_realloc", errno,
                   "failed to reallocate space for %zu "
                   "subconditions",
                   need);
      return err;
    }
    ogia->gia_sc = sc;
    ogia->gia_m = need;
  }

  cl_assert(cl, ogia->gia_n < ogia->gia_m);
  sc = ogia->gia_sc + ogia->gia_n++;

  memset(sc, 0, sizeof(*sc));
  graphd_iterator_and_process_state_clear(&sc->sc_contest_ps);

  /* Move the iterator into the subcondition;
   * then zero out the incoming iterator.
   */
  sc->sc_it = *sub_it;
  *sub_it = NULL;

  PDB_IS_ITERATOR(cl, sc->sc_it);
  cl_assert(cl, pdb_iterator_has_position(pdb, sc->sc_it));
  PDB_IS_ITERATOR(cl, gia_it);

  return 0;
}

/**
 * @brief Get a specific subiterator.
 *
 *   Used when printing the and iterator for the "iterator"
 *   result pattern.
 *
 * @param graphd	module handle
 * @param it		iterator we're asking about -
 *			not necessarily an AND iterator, but could be.
 * @param i		index of subiterator we want.
 * @param sub_out	out: the subiterator iterator at that position
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_and_get_subconstraint(pdb_handle *pdb, pdb_iterator *it,
                                          size_t i, pdb_iterator **sub_out) {
  graphd_iterator_and *ogia;

  /* Forward structural inquiries to the original. */
  if (it->it_original != it) it = it->it_original;

  if (it->it_type != &graphd_iterator_and_type) return GRAPHD_ERR_NO;

  ogia = it->it_theory;
  if (i >= ogia->gia_n) return GRAPHD_ERR_NO;

  *sub_out = ogia->gia_sc[i].sc_it;
  return 0;
}

/**
 * @brief Is this an and iterator?
 *
 *   This is used when printing the and iterator for
 *   the "iterator" result pattern.
 *
 * @param graphd	module handle
 * @param it		iterator we're asking about -
 *			not necessarily an AND iterator, but could be.
 * @param n_out		out: number of subiterators
 * @param producer_out	out: index of the producer
 *
 * @return 0 on success, a nonzero error code on error.
 */
bool graphd_iterator_and_is_instance(pdb_handle *pdb, pdb_iterator *it,
                                     size_t *n_out, size_t *producer_out) {
  graphd_iterator_and *ogia;

  /* Forward structural inquiries to the original. */
  if (it->it_original != it) it = it->it_original;

  if (it->it_type != &graphd_iterator_and_type) return false;

  ogia = it->it_theory;

  if (n_out != NULL) *n_out = ogia->gia_n;
  if (producer_out != NULL) *producer_out = ogia->gia_producer;

  return true;
}

/*
 * Find the smallest sorted set within an AND.
 */
int graphd_iterator_and_cheapest_subiterator(graphd_request *greq,
                                             pdb_iterator *it_and,
                                             unsigned long min_size,
                                             pdb_iterator **it_out,
                                             int *gia_i) {
  size_t i = 0;
  int err = 0;
  unsigned long long best_so_far = PDB_ITERATOR_HIGH_ANY;
  char buf[200];

  pdb_iterator *it;
  pdb_iterator *best = NULL;
  pdb_handle *pdb = graphd_request_graphd(greq)->g_pdb;

  *it_out = NULL;

  /*
   * Get out now if you aren't an AND iterator
   *
   * TODO if it_and isn't an AND, but it is a sorted set, perhaps we
   * should return a clone of it_and?
   */
  if (!graphd_iterator_and_is_instance(pdb, it_and, NULL, NULL)) {
    if (pdb_iterator_statistics_done(pdb, it_and) &&
        pdb_iterator_sorted(pdb, it_and) &&
        (!pdb_iterator_n_valid(pdb, it_and) ||
         pdb_iterator_n(pdb, it_and) >= min_size))

      return pdb_iterator_clone(pdb, it_and, it_out);

    *it_out = NULL;
    return 0;
  }

  for (; !(err = graphd_iterator_and_get_subconstraint(pdb, it_and, i, &it));
       i++) {
    if (!pdb_iterator_statistics_done(pdb, it)) continue;

    if (!pdb_iterator_sorted(pdb, it)) continue;

    if (pdb_iterator_n(pdb, it) < best_so_far) {
      best_so_far = pdb_iterator_n(pdb, it);
      best = it;
      *gia_i = i;
    }
  }

  if (err == GRAPHD_ERR_NO && best) {
    if (pdb_iterator_n_valid(pdb, best) &&
        pdb_iterator_n(pdb, best) < min_size) {
      *it_out = NULL;
      return 0;
    }

    err = pdb_iterator_clone(pdb, best, it_out);
    if (err) {
      cl_log_errno(graphd_request_cl(greq), CL_LEVEL_ERROR,
                   "pdb_iterator_clone", err, "Can't clone %s",
                   pdb_iterator_to_string(pdb, best, buf, sizeof buf));
      return err;
    }

    cl_log(graphd_request_cl(greq), CL_LEVEL_SPEW,
           "cheapest_so_far: selected %s",
           pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
    return 0;
  } else if (err == GRAPHD_ERR_NO) {
    cl_log(graphd_request_cl(greq), CL_LEVEL_SPEW,
           "graphd_iterator_and_cheapest_subiterator: "
           "No cheap iterators");
    return 0;
  } else {
    cl_log_errno(graphd_request_cl(greq), CL_LEVEL_ERROR,
                 "graphd_iterator_and_get_subconstraint", err,
                 "Can't check subconstraints of %s",
                 pdb_iterator_to_string(pdb, it_and, buf, sizeof buf));
    return err;
  }
  return 0;
}
