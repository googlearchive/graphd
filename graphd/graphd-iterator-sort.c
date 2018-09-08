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
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

GRAPHD_SABOTAGE_DECL;

/*  Sort -- take iterator results, sort them, return them in order.
 */

#define GRAPHD_SORT_MAGIC 0x01234567
#define GRAPHD_IS_SORT(cl, sort) \
  cl_assert(cl, (sort)->sort_magic == GRAPHD_SORT_MAGIC)

static const pdb_iterator_type sort_type;

/**
 * @brief Internal state for a sort operator.
 */
typedef struct graphd_iterator_sort_storable {
  graphd_storable sos_storable;

  /*  Context for allocation of base and its referers.
   */
  cm_handle *sos_cm;

  /*  Logging context
   */
  cl_handle *sos_cl;

  /*  The actual contents of the cache.
   */
  graph_idset *sos_idset;

  /*  The most recent add to that cache.
   */
  pdb_id sos_idset_last_added;

} graphd_iterator_sort_storable;

typedef struct graphd_iterator_sort {
  unsigned long sort_magic;

  /**
   * @brief Containing graphd.
   */
  graphd_handle *sort_graphd;

  /**
   * @brief pdb's cm_handle.  Allocate and free through this.
   */
  cm_handle *sort_cm;

  /**
   * @brief pdb's cl_handle.  Log through this.
   */
  cl_handle *sort_cl;

  /**
   * @brief Only in the original - subiterator whose
   *  	results we're sorting here.
   */
  pdb_iterator *sort_sub;

  /**
   * @brief Only in the original - IDs.
   */
  graph_idset *sort_idset;
  graphd_iterator_sort_storable *sort_idset_storable;

  /**
   * @brief The ID most recently added to the idset.
   */
  pdb_id sort_idset_last_added;

  /**
   * @brief Only in the original - the most recently
   *	returned ID.
   *
   *	If it is not PDB_ID_NONE, we need to reposition on that ID
   *	(need to draw values out of the subiterator until we're sure
   *	we've got all the ones up to that ID) before continuing.
   */
  pdb_id sort_idset_resume;

  /**
   *  @brief Only in the original, boundary
   * 	before which the idset is complete.
   */
  unsigned long long sort_horizon;

  /*  The current position in the idset.
   */
  graph_idset_position sort_idset_pos;

  /*
   *  What was the idset pointer when the sort_idset_pos was valid?
   */
  graph_idset *sort_idset_pos_ptr;

  /*  How many elements did the idset have when sort_idset_pos
   *  was valid?
   */
  size_t sort_idset_pos_n;

  /* What was the ID just before (or after, if we're backwards)
   * that position?  PDB_ID_NONE means we're at the very beginning.
   */
  pdb_id sort_idset_pos_id;

  /**
   * Temporary for checking.
   */
  pdb_iterator *sort_sub_check;

} graphd_iterator_sort;

#define osort(it) ((graphd_iterator_sort *)(it)->it_original->it_theory)

/*  We're standing on <id>.  Remember our position in a form that
 *  survives additions to the idset.
 *
 *  <id> may be PDB_ID_NONE; in that case, we're standing on the
 *  set's first element.
 */
static void sort_position_save(pdb_iterator *it, pdb_id id) {
  graphd_iterator_sort *sort = it->it_theory;

  if (osort(it)->sort_idset != NULL) {
    sort->sort_idset_pos_ptr = osort(it)->sort_idset;
    sort->sort_idset_pos_n = osort(it)->sort_idset->gi_n;
  } else {
    sort->sort_idset_pos_ptr = NULL;
    sort->sort_idset_pos_n = 0;
  }
  sort->sort_idset_pos_id = id;
}

/*  Bring our sort_idset_pos in line with where we remember being.
 */
static int sort_position_load(pdb_iterator *it) {
  graphd_iterator_sort *sort = it->it_theory;

  /*  Offset positions in an idset aren't valid after the
   *  idset has been added to or is being restored.
   *  If the idset has changed, restore our position
   *  by positioning on the place we were at last.
   */

  /*  We're done restoring, right?
   */
  cl_assert(sort->sort_cl, osort(it)->sort_idset_resume == PDB_ID_NONE);

  /* Nothing to position in?
   */
  if (osort(it)->sort_idset == NULL) return 0;

  /*  It hasn't changed size?
   */
  if (osort(it)->sort_idset == sort->sort_idset_pos_ptr &&
      osort(it)->sort_idset->gi_n == sort->sort_idset_pos_n)
    return 0;

  /* Position where we remember being.
   */
  if (sort->sort_idset_pos_id == PDB_ID_NONE) {
    if (pdb_iterator_forward(sort->sort_graphd->g_pdb, it))
      graph_idset_next_reset(osort(it)->sort_idset, &sort->sort_idset_pos);
    else
      graph_idset_prev_reset(osort(it)->sort_idset, &sort->sort_idset_pos);
  } else {
    bool found;

    found = graph_idset_locate(osort(it)->sort_idset, sort->sort_idset_pos_id,
                               &sort->sort_idset_pos);
    if (!found) {
      cl_log(sort->sort_cl, CL_LEVEL_FAIL,
             "sort_position_load: failed to find %llu at "
             "%llu.%llu in idset with %llu elements",
             (unsigned long long)sort->sort_idset_pos_id,
             (unsigned long long)sort->sort_idset_pos.gip_ull,
             (unsigned long long)sort->sort_idset_pos.gip_size,
             (unsigned long long)sort->sort_idset->gi_n);
      return GRAPHD_ERR_BADCURSOR;
    }

    cl_assert(sort->sort_cl, found);
  }

  /*  Remember the set we know our position in.
   */
  sort->sort_idset_pos_ptr = osort(it)->sort_idset;
  sort->sort_idset_pos_n = osort(it)->sort_idset->gi_n;

  return 0;
}

static unsigned long sort_storable_hash(void const *data) {
  return (unsigned long)(intptr_t)data;
}

static void sort_storable_destroy(void *data) {
  graphd_iterator_sort_storable *sos = data;
  cm_handle *cm = sos->sos_cm;

  graph_idset_free(sos->sos_idset);
  cm_free(cm, sos);
}

static bool sort_storable_equal(void const *A, void const *B) { return A == B; }

static const graphd_storable_type sort_storable_type = {
    "sort cache", sort_storable_destroy, sort_storable_equal,
    sort_storable_hash};

/*  Create a fresh sort-storable
 *  You will allocate in CM and log through CL.
 *
 *  A successful call transfers one reference to the caller.
 */
static graphd_iterator_sort_storable *graphd_iterator_sort_storable_alloc(
    cm_handle *cm, cl_handle *cl, graph_idset *idset, pdb_id idset_last_added) {
  graphd_iterator_sort_storable *sos;

  sos = cm_malloc(cm, sizeof(*sos));
  if (sos == NULL) return NULL;

  memset(sos, 0, sizeof(*sos));

  sos->sos_storable.gs_linkcount = 1;
  sos->sos_storable.gs_type = &sort_storable_type;
  sos->sos_storable.gs_size = sizeof(*sos);

  sos->sos_cm = cm;
  sos->sos_cl = cl;
  sos->sos_idset = idset;
  sos->sos_idset_last_added = idset_last_added;

  return sos;
}

static int store_idset(pdb_iterator *it) {
  graphd_iterator_sort *sort = osort(it);

  if (sort->sort_idset_storable == NULL) {
    graphd_handle *g = sort->sort_graphd;

    sort->sort_idset_storable = graphd_iterator_sort_storable_alloc(
        g->g_cm, g->g_cl, sort->sort_idset, sort->sort_idset_last_added);
    if (sort->sort_idset_storable == NULL) return ENOMEM;

    sort->sort_idset = sort->sort_idset_storable->sos_idset;
    sort->sort_idset_last_added =
        sort->sort_idset_storable->sos_idset_last_added;
  }
  return 0;
}

static void update_horizon_start(pdb_iterator *it) {
  graphd_iterator_sort *sort = osort(it);
  sort->sort_horizon =
      pdb_iterator_forward(sort->sort_pdb, it) ? it->it_low : it->it_high;

  cl_log(sort->sort_cl, CL_LEVEL_VERBOSE,
         "update_horizon_start: "
         "set horizon to %llu",
         (unsigned long long)sort->sort_horizon);
}

static void update_horizon_eof(pdb_iterator *it) {
  graphd_iterator_sort *sort = osort(it);

  sort->sort_horizon = pdb_iterator_forward(sort->sort_pdb, it)
                           ? PDB_ITERATOR_HIGH_ANY
                           : PDB_ITERATOR_LOW_ANY;
}

static int update_horizon(pdb_iterator *it) {
  graphd_iterator_sort *sort = osort(it);
  cl_handle *cl = sort->sort_cl;
  int err;
  pdb_range_estimate range;
  unsigned long long horizon_in = sort->sort_horizon;

  cl_assert(cl, it);
  cl_assert(cl, sort);
  cl_assert(cl, sort->sort_sub);

  /*  We already know everything?
   */
  if (pdb_iterator_forward(sort->sort_graphd->g_pdb, it)
          ? sort->sort_horizon >= it->it_high
          : sort->sort_horizon < it->it_low)
    return 0;

  PDB_IS_ITERATOR(cl, sort->sort_sub);
  err = pdb_iterator_range_estimate(sort->sort_graphd->g_pdb, sort->sort_sub,
                                    &range);
  if (err != 0) {
    char buf[200];
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_range_estimate", err, "it=%s",
                 pdb_iterator_to_string(sort->sort_graphd->g_pdb,
                                        sort->sort_sub, buf, sizeof buf));
    return err;
  }

  if (range.range_high <= range.range_low || range.range_n_exact == 0)

    /*  The published range is empty.
     */
    update_horizon_eof(it);
  else {
    /*  Adjust the horizon we're interested in.
     */
    if (pdb_iterator_forward(sort->sort_graphd->g_pdb, it)) {
      if (range.range_low > sort->sort_horizon)
        range.range_low = sort->sort_horizon;
    } else {
      if (range.range_high < sort->sort_horizon)
        range.range_high = sort->sort_horizon;
    }
  }

  if (horizon_in != sort->sort_horizon)
    cl_log(cl, CL_LEVEL_VERBOSE,
           "update_horizon: "
           "changed horizon %llu->%llu",
           horizon_in, (unsigned long long)sort->sort_horizon);

  return 0;
}

static int expand_cache(pdb_handle *pdb, pdb_iterator *it,
                        pdb_budget *budget_inout) {
  cl_handle *cl = osort(it)->sort_cl;
  int err;
  pdb_id id;

  cl_assert(cl, osort(it)->sort_idset != NULL);

  err = pdb_iterator_next(pdb, osort(it)->sort_sub, &id, budget_inout);
  if (err != 0) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "expand_cache: "
           "pdb_iterator_next: error: %s",
           graphd_strerror(err));

    /*  We're completely done?  Move the horizon all the way to
     *  the end - we're now knowing everything there is to know
     *  about this iterator.
     */
    if (err == PDB_ERR_NO)
      update_horizon_eof(it);

    else if (err != PDB_ERR_MORE)
      cl_log(cl, CL_LEVEL_VERBOSE,
             "expand_cache: not updating "
             "horizon, since error %d != NO %d",
             err, PDB_ERR_NO);

    return err;
  }

  err = graph_idset_insert(osort(it)->sort_idset, id);
  if (err != 0) return err;

  if (id == osort(it)->sort_idset_resume)
    osort(it)->sort_idset_resume = PDB_ID_NONE;
  osort(it)->sort_idset_last_added = id;

  return 0;
}

static int sort_resume(pdb_handle *pdb, pdb_iterator *it,
                       pdb_budget *budget_inout) {
  graphd_iterator_sort *sort = osort(it);
  graphd_handle *g = sort->sort_graphd;
  int err = 0;

  if (sort->sort_idset_resume == PDB_ID_NONE) return 0;

  cl_log(sort->sort_cl, CL_LEVEL_VERBOSE, "sort_resume: find %llx with $%lld",
         (unsigned long long)sort->sort_idset_resume, (long long)*budget_inout);

  while (*budget_inout >= 0) {
    /*  Get the next ID out of the subiterator and add
     *  it into the idset.
     */
    if (sort->sort_idset == NULL) {
      if (sort->sort_idset_storable == NULL) {
        graph_idset *idset = graph_idset_tile_create(g->g_graph);
        if (idset == NULL) return ENOMEM;

        sort->sort_idset_storable = graphd_iterator_sort_storable_alloc(
            g->g_cm, g->g_cl, idset, PDB_ID_NONE);
        if (sort->sort_idset_storable == NULL) {
          graph_idset_free(idset);
          return ENOMEM;
        }
      }
      sort->sort_idset = sort->sort_idset_storable->sos_idset;
      sort->sort_idset_last_added =
          sort->sort_idset_storable->sos_idset_last_added;
    }

    err = expand_cache(pdb, it, budget_inout);
    if (err != 0) {
      if (err != PDB_ERR_NO) return err;

      /*  We're completely done.
       */
      if (sort->sort_idset_resume != PDB_ID_NONE) {
        /*  We were trying to resume a position that
         *  isn't in the set.  That can happen if
         *  a cursor gets reused across update
         *  boundaries - we remembered a landmark
         *  X that no longer exists - but shouldn't.
         */
        cl_log(sort->sort_cl, CL_LEVEL_FAIL,
               "sort_resume: resumption point "
               "%llu isn't in the result set anymore "
               "- outdated cursor?",
               (unsigned long long)sort->sort_idset_resume);
        return GRAPHD_ERR_BADCURSOR;
      }
      cl_assert(sort->sort_cl, sort->sort_idset_resume == PDB_ID_NONE);
      return 0;
    }

    if (sort->sort_idset_resume == PDB_ID_NONE) return update_horizon(it);
  }

  /* We ran out of budget.
   */
  err = update_horizon(it);
  if (err != 0) return err;

  return PDB_ERR_MORE;
}

static bool id_would_be_cached(pdb_handle *pdb, pdb_iterator *it, pdb_id id) {
  graphd_iterator_sort *sort = osort(it);

  return pdb_iterator_forward(pdb, it) ? sort->sort_horizon > id
                                       : sort->sort_horizon <= id;
}

static int sort_find_loc(pdb_handle *pdb, pdb_iterator *it, pdb_id id_in,
                         pdb_id *id_out, pdb_budget *budget_inout,
                         char const *file, int line) {
  pdb_budget budget_in = *budget_inout;
  graph_idset *idset;
  graph_idset_position *pos, tmp_pos;
  graphd_iterator_sort *sort = it->it_theory;
  int err = 0;
  pdb_id id;
  unsigned long long ull;

  id = id_in;
  if (pdb_iterator_forward(pdb, it)) {
    if (id_in < it->it_low) id = it->it_low;
  } else {
    if (id_in >= it->it_high) id = it->it_high - 1;
  }

  /*  If this id happens to already be in the cache,
   *  we can just position on it without being caught up.
   */
  if (graph_idset_locate(osort(it)->sort_idset, id, &tmp_pos)) {
    *id_out = id;
    sort->sort_idset_pos = tmp_pos;

    *budget_inout -= 3;
    sort_position_save(it, id);

    pdb_rxs_log(pdb, "FIND %p sort %llx -> %llx ($%lld)", (void *)it,
                (unsigned long long)id_in, (unsigned long long)*id_out,
                (long long)(budget_in - *budget_inout));
    pdb_iterator_account_charge_budget(pdb, it, find);
    return 0;
  }

  /*  If we've already gone beyond the iterator, we
   *  can just seek back to that in the idset.
   *
   *  Otherwise, we'll have to go forward until we
   *  move past the destination.
   */
  while (!id_would_be_cached(pdb, it, id)) {
    err = expand_cache(pdb, it, budget_inout);
    if (err == PDB_ERR_NO)
      break;
    else if (err != 0)
      return err;

    update_horizon(it);
  }

  /* Find exactly what we're looking for?
   */
  idset = osort(it)->sort_idset;
  pos = &sort->sort_idset_pos;

  if (graph_idset_locate(idset, id, pos)) {
    *id_out = id;
    sort_position_save(it, id);

    pdb_rxs_log(pdb, "FIND %p sort %llx -> %llx ($%lld)", (void *)it,
                (unsigned long long)id_in, (unsigned long long)*id_out,
                (long long)(budget_in - *budget_inout));
    pdb_iterator_account_charge_budget(pdb, it, find);
    return 0;
  }

  /* No?  Go to the next.
   */
  if (pdb_iterator_forward(pdb, it)) {
    if (graph_idset_next(idset, &ull, pos)) {
      if (ull >= it->it_high)
        err = GRAPHD_ERR_NO;
      else
        *id_out = ull;
      goto err;
    }
  } else {
    if (graph_idset_prev(idset, &ull, pos)) {
      if (ull < it->it_low)
        err = GRAPHD_ERR_NO;
      else
        *id_out = ull;
      goto err;
    }
  }
  err = GRAPHD_ERR_NO;

err:
  if (err == 0) {
    sort_position_save(it, *id_out);
    pdb_rxs_log(pdb, "FIND %p sort %llx %llx ($%lld)", (void *)it,
                (unsigned long long)id_in, (unsigned long long)*id_out,
                (long long)(budget_in - *budget_inout));
  } else
    pdb_rxs_log(pdb, "FIND %p sort %llx %s ($%lld)", (void *)it,
                (unsigned long long)id_in,
                (err == GRAPHD_ERR_NO ? "eof" : err == GRAPHD_ERR_MORE
                                                    ? "suspended"
                                                    : graphd_strerror(err)),
                (long long)(budget_in - *budget_inout));

  pdb_iterator_account_charge_budget(pdb, it, find);
  return err;
}

static int sort_reset(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_sort *sort = it->it_theory;
  graph_idset *idset = osort(it)->sort_idset;
  graph_idset_position *pos = &sort->sort_idset_pos;

  pdb_rxs_log(pdb, "RESET %p sort", (void *)it);

  if (idset == NULL) return 0;

  if (pdb_iterator_forward(pdb, it))
    graph_idset_next_reset(idset, pos);
  else
    graph_idset_prev_reset(idset, pos);

  sort_position_save(it, PDB_ID_NONE);

  return 0;
}

static int sort_statistics(pdb_handle *pdb, pdb_iterator *it,
                           pdb_budget *budget_inout) {
  graphd_iterator_sort *sort = it->it_theory;
  cl_handle *cl = sort->sort_cl;
  int err;
  pdb_budget budget_in = *budget_inout;
  char buf[200];
  bool forward;

  err = pdb_iterator_statistics(pdb, osort(it)->sort_sub, budget_inout);
  if (err != 0) return err;

  /*  If the subiterator turned sorted itself, remove us.
   */
  if (pdb_iterator_sorted(pdb, osort(it)->sort_sub) &&
      pdb_iterator_forward(pdb, it) ==
          pdb_iterator_forward(pdb, osort(it)->sort_sub))
    return pdb_iterator_substitute(pdb, it, osort(it)->sort_sub);

  /*  Inherit results from the subiterator, except for our
   *  sortedness.
   */
  forward = pdb_iterator_forward(pdb, it);
  pdb_iterator_statistics_copy(pdb, it, osort(it)->sort_sub);
  pdb_iterator_forward_set(pdb, it, forward);

  cl_log(cl, CL_LEVEL_VERBOSE | PDB_FACILITY_ITERATOR,
         "PDB STAT for SORT[#%llu] %s: n=%llu cc=%llu "
         "nc=%llu fc=%llu ($%lld)",
         (unsigned long long)it->it_id,
         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         (unsigned long long)pdb_iterator_n(pdb, it),
         (unsigned long long)pdb_iterator_check_cost(pdb, it),
         (unsigned long long)pdb_iterator_next_cost(pdb, it),
         (unsigned long long)pdb_iterator_find_cost(pdb, it),
         (long long)(budget_in - *budget_inout));

  return 0;
}

static int sort_check(pdb_handle *pdb, pdb_iterator *it, pdb_id check_id,
                      pdb_budget *budget_inout) {
  graphd_iterator_sort *const sort = it->it_theory;
  pdb_budget const budget_in = *budget_inout;
  int err = 0;

  if (id_would_be_cached(pdb, it, check_id)) {
    *budget_inout -= PDB_COST_FUNCTION_CALL;
    err = graph_idset_check(osort(it)->sort_idset, check_id) ? 0 : PDB_ERR_NO;
  } else {
    /*  We can do either of two things:
     *
     * 	- call the check function on the subiterator
     *
     *	- pull entries into our cache until the thing
     * 	  the caller is asking about is in there, and
     *	  then just consult the cache.
     *
     *  For now, let's just call the check function.
     */
    if (sort->sort_sub_check == NULL) {
      err = pdb_iterator_clone(pdb, osort(it)->sort_sub, &sort->sort_sub_check);
      if (err != 0) return err;
      cl_assert(sort->sort_cl, sort->sort_sub_check->it_call_state == 0);
    }
    err = pdb_iterator_check(pdb, sort->sort_sub_check, check_id, budget_inout);
  }
  pdb_rxs_log(pdb, "CHECK %p sort %llx: %s [$%llu]", (void *)it,
              (unsigned long long)check_id,
              err == PDB_ERR_NO ? "no" : (err ? graphd_strerror(err) : "yes"),
              budget_in - *budget_inout);
  return err;
}

static int sort_next_loc(pdb_handle *pdb, pdb_iterator *it, pdb_id *id_out,
                         pdb_budget *budget_inout, char const *file, int line) {
  pdb_budget budget_in = *budget_inout;
  graphd_iterator_sort *sort = it->it_theory;
  graph_idset_position pos;
  int err = 0;

  *budget_inout -= PDB_COST_FUNCTION_CALL;

  pdb_rxs_push(pdb, "NEXT %p sort (hor=%llu, pos-id=%llu)", (void *)it,
               (unsigned long long)osort(it)->sort_horizon,
               (unsigned long long)sort->sort_idset_pos_id);

  if ((err = sort_resume(pdb, it, budget_inout)) != 0) {
    pdb_rxs_pop(pdb, "NEXT %p sort resume: %s ($%lld)", (void *)it,
                graphd_strerror(err), (long long)(budget_in - *budget_inout));

    cl_log_errno(sort->sort_cl, CL_LEVEL_FAIL, "sort_resume", err,
                 "resume_id=%llx",
                 (unsigned long long)osort(it)->sort_idset_resume);
    return err;
  }
  cl_assert(sort->sort_cl, osort(it)->sort_idset_resume == PDB_ID_NONE);

  cl_assert(sort->sort_cl, osort(it)->sort_idset != NULL);
  for (;;) {
    unsigned long long ull;

    if (GRAPHD_SABOTAGE(sort->sort_graphd, *budget_inout < 0)) {
      err = PDB_ERR_MORE;
      break;
    }

    /*  We're done restoring, right?
     */
    cl_assert(sort->sort_cl, osort(it)->sort_idset_resume == PDB_ID_NONE);

    if ((err = sort_position_load(it)) != 0) goto err;

    pos = sort->sort_idset_pos;

    /*  What's the next value in our cached current location?
     */
    ull = 0;
    if (pdb_iterator_forward(pdb, it)
            ? graph_idset_next(osort(it)->sort_idset, &ull, &pos)
            : graph_idset_prev(osort(it)->sort_idset, &ull, &pos)) {
      cl_log(sort->sort_cl, CL_LEVEL_VERBOSE, "sort_next: got %llu from idset",
             ull);

      /*  If we know that we have everything up to
       *  (or down to) this point in the cache, the
       *  result is usable.
       */
      if (id_would_be_cached(pdb, it, ull)) {
        cl_log(sort->sort_cl, CL_LEVEL_VERBOSE,
               "sort_next: that would be cached");

        *id_out = ull;
        sort->sort_idset_pos = pos;

        /* Found something we can safely return. */

        err = 0;
        goto err;
      }
    } else {
      /*  Do we already know everything there
       *  is to know?
       */
      ull = pdb_iterator_forward(pdb, it) ? it->it_high - 1 : it->it_low;

      cl_log(sort->sort_cl, CL_LEVEL_VERBOSE,
             "sort_next: got NO from idset; try %llu", ull);

      if (id_would_be_cached(pdb, it, ull)) {
        cl_log(sort->sort_cl, CL_LEVEL_VERBOSE,
               "sort_next: we're completely cached.");
        err = PDB_ERR_NO;
        goto err;
      }
    }

    /*  Add more knowledge to the cache.
     */
    err = expand_cache(pdb, it, budget_inout);
    if (err != 0 && err != PDB_ERR_NO) goto err;

    update_horizon(it);
  }

err:
  if (err == 0) {
    sort_position_save(it, *id_out);
    pdb_rxs_pop(pdb, "NEXT %p sort %llx ($%lld)", (void *)it,
                (unsigned long long)*id_out,
                (long long)(budget_in - *budget_inout));
  } else if (err == PDB_ERR_NO)
    pdb_rxs_pop(pdb, "NEXT %p sort EOF ($%lld)", (void *)it,
                (long long)(budget_in - *budget_inout));

  else if (err == PDB_ERR_MORE)
    pdb_rxs_pop(pdb, "NEXT %p sort suspend ($%lld)", (void *)it,
                (long long)(budget_in - *budget_inout));

  else
    pdb_rxs_pop(pdb, "NEXT %p sort %s ($%lld)", (void *)it,
                graphd_strerror(err), (long long)(budget_in - *budget_inout));

  pdb_iterator_account_charge_budget(pdb, it, next);

  return err;
}

/*
 * sort:[~](SUBIT) / IDSET-POSITION / (SUBPOS/STATE)(SUBPOS/STATE)
 */
static int sort_freeze(pdb_handle *pdb, pdb_iterator *it, unsigned int flags,
                       cm_buffer *buf) {
  graphd_iterator_sort *sort = it->it_theory;
  int err = 0;
  char const *sep = "";

  /*  If the sort_sub iterator has evolved to be structurally
   *  different from the check iterator, don't save the check
   *  iterator.
   */

  if (flags & PDB_ITERATOR_FREEZE_SET) {
    err = cm_buffer_sprintf(buf, "sort:%s(",
                            pdb_iterator_forward(pdb, it) ? "" : "~");
    if (err != 0) return err;

    err = pdb_iterator_freeze(pdb, osort(it)->sort_sub, PDB_ITERATOR_FREEZE_SET,
                              buf);
    if (err != 0) return err;

    err = cm_buffer_add_string(buf, ")");
    if (err != 0) return err;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    char b2[200], b3[200];

    err = cm_buffer_sprintf(
        buf, "%s%s:%llu:%zu:%s:%llu", sep,
        pdb_iterator_has_position(pdb, it)
            ? pdb_id_to_string(pdb, osort(it)->sort_idset_last_added, b2,
                               sizeof b2)
            : "-",
        sort->sort_idset_pos.gip_ull, sort->sort_idset_pos.gip_size,
        pdb_id_to_string(pdb, sort->sort_idset_pos_id, b3, sizeof b3),
        osort(it)->sort_idset ? (unsigned long long)osort(it)->sort_idset->gi_n
                              : 0);
    if (err != 0) return err;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_STATE) {
    char sb[GRAPHD_ITERATOR_RESOURCE_STAMP_SIZE];

    /*  [ids:...] (subpos/substate) (checksubpos/substate)
     */
    if (osort(it)->sort_idset_storable == NULL &&
        osort(it)->sort_idset != NULL) {
      err = store_idset(it);
      if (err != 0) return err;
    }

    if (osort(it)->sort_idset_storable) {
      if ((err = graphd_iterator_resource_store(
               sort->sort_graphd,
               (graphd_storable *)osort(it)->sort_idset_storable, sb,
               sizeof sb)) != 0 ||
          (err = cm_buffer_sprintf(buf, "%s[ids:@%s]", sep, sb)) != 0)
        return err;
    } else {
      err = cm_buffer_sprintf(buf, "%s", sep);
      if (err) return err;
    }

    err = graphd_iterator_util_freeze_subiterator(
        pdb, osort(it)->sort_sub,
        PDB_ITERATOR_FREEZE_POSITION | PDB_ITERATOR_FREEZE_STATE, buf);
    if (err != 0) goto err;

    /*  Are sort_sub and sort_sub_check sibling clones?
     *  If yes, freeze sort_sub_check's state.  Otherwise,
     *  freeze a null state; we'll reclone sort_sub_check
     *  from the (more advanced) sort_sub during thaw.
     */
    err = graphd_iterator_util_freeze_subiterator(
        pdb, (sort->sort_sub_check != NULL && sort->sort_sub != NULL &&
              sort->sort_sub_check->it_original == sort->sort_sub->it_original)
                 ? sort->sort_sub_check
                 : NULL,
        PDB_ITERATOR_FREEZE_POSITION | PDB_ITERATOR_FREEZE_STATE, buf);
    if (err != 0) goto err;
  }
  return 0;
err:
  return err;
}

static int sort_clone(pdb_handle *pdb, pdb_iterator *it,
                      pdb_iterator **it_out) {
  pdb_iterator *it_orig = it->it_original;
  graphd_iterator_sort *sort = it->it_theory;
  cm_handle *cm = sort->sort_cm;
  graphd_iterator_sort *sort_out;
  int err;

  PDB_IS_ITERATOR(sort->sort_cl, it);
  GRAPHD_IS_SORT(pdb_log(pdb), sort);

  /*  If the original iterator has evolved into something
   *  other than an "sort" iterator, clone that iterator
   *  directly and reset it.  If we had a position to save,
   *  we would have already evolved.
   */
  if (it_orig->it_type != it->it_type || it->it_id != it_orig->it_id)
    return pdb_iterator_clone(pdb, it_orig, it_out);

  *it_out = NULL;
  if ((sort_out = cm_malcpy(cm, sort, sizeof(*sort))) == NULL) {
    return errno ? errno : ENOMEM;
  }

  /* Since we're a clone, all these pointers can be NULL.
   */
  sort_out->sort_idset_storable = NULL;
  sort_out->sort_idset = NULL;
  sort_out->sort_sub = NULL;
  sort_out->sort_sub_check = NULL;

  if ((err = pdb_iterator_make_clone(pdb, it_orig, it_out)) != 0) {
    cm_free(sort->sort_cm, sort_out);
    return err;
  }
  (*it_out)->it_theory = sort_out;

  if (!pdb_iterator_has_position(pdb, it) &&
      (err = pdb_iterator_reset(pdb, *it_out)) != 0) {
    pdb_iterator_destroy(pdb, it_out);
    return err;
  }
  return 0;
}

static void sort_finish(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_sort *sort = it->it_theory;

  if (sort != NULL) {
    cl_cover(sort->sort_cl);

    if (sort->sort_sub_check != NULL)
      pdb_iterator_destroy(pdb, &sort->sort_sub_check);

    if (it->it_original == it) {
      pdb_iterator_destroy(pdb, &sort->sort_sub);
      if (sort->sort_idset_storable != NULL) {
        cl_log(
            sort->sort_cl, CL_LEVEL_VERBOSE,
            "sort_finish: %d links from theidset  storable (before taking one)",
            ((graphd_storable *)sort->sort_idset_storable)->gs_linkcount);

        graphd_storable_unlink(sort->sort_idset_storable);
        sort->sort_idset = NULL;
      } else {
        if (sort->sort_idset != NULL) graph_idset_free(sort->sort_idset);
      }
    }

    cm_free(sort->sort_cm, it->it_displayname);
    it->it_displayname = NULL;

    cm_free(sort->sort_cm, sort);
    it->it_theory = NULL;
  }
  it->it_type = NULL;
  it->it_magic = 0;
}

static char const *sort_to_string(pdb_handle *pdb, pdb_iterator *it, char *buf,
                                  size_t size) {
  char sub[200];

  if (it->it_original->it_id != it->it_id) {
    snprintf(buf, sizeof buf, "sort**%s",
             pdb_iterator_to_string(pdb, it->it_original, sub, sizeof sub));
    return buf;
  }

  snprintf(buf, size, "%ssort:%s", it->it_forward ? "" : "~",
           pdb_iterator_to_string(pdb, osort(it)->sort_sub, sub, sizeof sub));
  return buf;
}

/**
 * @brief Will this iterator ever return a value beyond (in sort order)
 * 	the given value?
 *
 * @param pdb		module handle
 * @param it		iterator we're asking about
 * @param s		start of comparison value
 * @param e		end of comparison value
 * @param beyond_out	out: true if we're safely beyond this value.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int sort_beyond(pdb_handle *pdb, pdb_iterator *it, char const *s,
                       char const *e, bool *beyond_out) {
  pdb_range_estimate range;
  pdb_id id;
  int err;

  if (e - s != sizeof id) return PDB_ERR_NO;
  memcpy(&id, s, e - s);

  err = pdb_iterator_range_estimate(pdb, it, &range);
  if (err != 0) return err;

  *beyond_out = pdb_iterator_forward(pdb, it) ? id < range.range_low
                                              : id >= range.range_high;
  return 0;
}

static int sort_range_estimate(pdb_handle *pdb, pdb_iterator *it,
                               pdb_range_estimate *range) {
  graphd_iterator_sort *sort = it->it_theory;
  int err;
  graph_idset_position pos;
  unsigned long long next_id;
  unsigned long long n_cache_unreturned = 0;

  /*  If we're still resuming, we don't know yet.
   */
  if (osort(it)->sort_idset_resume != PDB_ID_NONE)
    return pdb_iterator_range_estimate_default(pdb, it, range);

  err = pdb_iterator_range_estimate(pdb, osort(it)->sort_sub, range);
  if (err != 0) return err;

  cl_assert(sort->sort_cl, osort(it)->sort_idset_resume == PDB_ID_NONE);
  if ((err = sort_position_load(it)) != 0) return err;

  pos = sort->sort_idset_pos;
  if (pdb_iterator_forward(pdb, it)
          ? graph_idset_next(osort(it)->sort_idset, &next_id, &pos)
          : graph_idset_prev(osort(it)->sort_idset, &next_id, &pos)) {
    /*  Expand the subiterator range to include at least <next_id>.
     */
    if (next_id < range->range_low) range->range_low = next_id;
    if (next_id >= range->range_high) range->range_high = next_id + 1;

    n_cache_unreturned =
        1 + graph_idset_offset(
                osort(it)->sort_idset, &pos,
                pdb_iterator_forward(pdb, it) ? it->it_high : it->it_low);
  }

  /*  Unless we're at the end of our range --end == GRAPH_ERR_NO--
   *  we need to add the elements in the cache, but no longer in the
   *  subiterator, to the subiterator's count.
   */
  if (range->range_n_exact != PDB_COUNT_UNBOUNDED)
    range->range_n_exact += n_cache_unreturned;
  if (range->range_n_max != PDB_COUNT_UNBOUNDED)
    range->range_n_max += n_cache_unreturned;

  return 0;
}

static const pdb_iterator_type sort_type = {
    "sort",        sort_finish,         sort_reset, sort_clone,
    sort_freeze,   sort_to_string,

    sort_next_loc, sort_find_loc,       sort_check, sort_statistics,

    NULL, /* idarray */
    NULL, /* primitive-summary */

    sort_beyond,   sort_range_estimate, NULL, /* restrict */

    NULL, /* suspend */
    NULL  /* unsuspend */
};

/**
 * @brief Create a "sort" iterator structure.
 *
 * @param greq		request for whom we're doing this
 * @param forward	sort how?  Forward (true) or backwards (false) ?
 * @param sub		pointer to subiterator.  A successful call
 *			zeroes out the pointer and takes possession of
 *			the pointed-to iterator.
 * @param it_out	assign the new iterator to this
 * @param file		filename of calling code
 * @param line		line number of calling code
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
int graphd_iterator_sort_create_loc(graphd_request *greq, bool forward,
                                    pdb_iterator **sub, pdb_iterator **it_out,
                                    char const *file, int line) {
  graphd_handle *g = graphd_request_graphd(greq);
  pdb_handle *pdb = g->g_pdb;
  cl_handle *cl = pdb_log(pdb);
  cm_handle *cm = pdb_mem(pdb);
  graphd_iterator_sort *sort;

  *it_out = cm->cm_realloc_loc(cm, NULL, sizeof(**it_out), file, line);
  if (*it_out == NULL) return errno ? errno : ENOMEM;

  if ((sort = cm_zalloc(cm, sizeof(*sort))) == NULL) {
    int err = errno ? errno : ENOMEM;

    cm_free(cm, *it_out);
    *it_out = NULL;

    return err;
  }

  sort->sort_idset = graph_idset_tile_create(g->g_graph);
  if (sort->sort_idset == NULL) {
    int err = errno ? errno : ENOMEM;

    cm_free(cm, sort);
    cm_free(cm, *it_out);
    *it_out = NULL;

    return err;
  }

  sort->sort_magic = GRAPHD_SORT_MAGIC;
  sort->sort_idset_resume = PDB_ID_NONE;
  sort->sort_graphd = g;
  sort->sort_cl = cl;
  sort->sort_cm = cm;
  sort->sort_sub_check = NULL;

  pdb_iterator_make_loc(g->g_pdb, *it_out, (*sub)->it_low, (*sub)->it_high,
                        forward, file, line);
  pdb_iterator_forward_set(g->g_pdb, *it_out, forward);

  sort->sort_sub = *sub;
  *sub = NULL;

  (*it_out)->it_theory = sort;
  (*it_out)->it_type = &sort_type;

  GRAPHD_IS_SORT(cl, sort);
  (void)sort_reset(pdb, *it_out);
  update_horizon_start(*it_out);

  pdb_rxs_log(pdb, "CREATE %p sort", (void *)*it_out);

  return 0;
}

/**
 * @brief Reconstitute a frozen sort-iterator
 *
 * [~](SUB) / IDSET-POS / (SUBPOS/STATE)(SUBPOS/STATE)
 *
 * @param graphd	module handle
 * @param s		beginning of stored form
 * @param e		pointer just past the end of stored form
 * @param forward	no ~ before the name?
 * @param it_out	rebuild the iterator here.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_sort_thaw_loc(graphd_handle *graphd,
                                  pdb_iterator_text const *pit,
                                  pdb_iterator_base *pib, cl_loglevel loglevel,
                                  pdb_iterator **it_out, char const *file,
                                  int line) {
  pdb_handle *pdb = graphd->g_pdb;
  pdb_iterator *sub_it = NULL, *sub_check_it = NULL;
  cl_handle *cl = pdb_log(pdb);
  graphd_iterator_sort *sort;
  int err;
  bool forward;
  char const *e, *s;
  char const *state_s, *state_e;
  pdb_iterator_text subpit;
  graphd_request *greq;
  graph_idset_position pos;
  graphd_iterator_sort_storable *store = NULL;
  pdb_id resume_id = PDB_ID_NONE, idset_pos_id = PDB_ID_NONE;
  unsigned long long idset_pos_n = 0;

  /*
   * SET      := [~](SUBSET)
   * POSITION := <idset position>:<last returned>:<n in idset>
   * STATE    := [ids:@...] (SUBPOS/STATE) (SUBPOS/STATE)
   */

  /* Initialize it with zero to make a pdb_iterator_destroy() on
   * error harmless.
   */
  *it_out = NULL;

  greq = pdb_iterator_base_lookup(graphd->g_pdb, pib, "graphd.request");
  if (greq == NULL) {
    err = errno ? errno : EINVAL;
    cl_log_errno(cl, loglevel, "graphd_iterator_sort_thaw", err,
                 "failed to look up request context");
    goto err;
  }

  /*  SET
   */
  s = pit->pit_set_s;
  e = pit->pit_set_e;
  cl_assert(cl, s != NULL && e != NULL);

  err = pdb_iterator_util_thaw(
      pdb, &s, e, "%{forward}%{(bytes)}%{extensions}%{end}", &forward,
      &subpit.pit_set_s, &subpit.pit_set_e, (pdb_iterator_property *)NULL);
  if (err != 0) {
    cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                 "could not thaw set");
    return err;
  }

  memset(&pos, 0, sizeof pos);

  /* POSITION
   */
  if ((s = pit->pit_position_s) != NULL && s < (e = pit->pit_position_e)) {
    unsigned long long llu1, llu2;

    err = pdb_iterator_util_thaw(pdb, &s, e, "%{id}:%llu:%llu:%{id}:%llu",
                                 &resume_id, &llu1, &llu2, &idset_pos_id,
                                 &idset_pos_n);
    if (err != 0) {
      cl_log(cl, loglevel,
             "graphd_iterator_sort_thaw_loc: "
             "can't thaw position \"%.*s\" [from %s:%d]",
             (int)(pit->pit_position_e - pit->pit_position_s),
             pit->pit_position_s, file, line);
      return err;
    }
    pos.gip_ull = llu1;
    pos.gip_size = llu2;
  }

  /* STATE (1) - SUBITERATOR
   */
  state_s = pit->pit_state_s;
  state_e = pit->pit_state_e;
  if (state_s != NULL && state_s < state_e) {
    char const *ids_s, *ids_e;
    char const *state_s0 = state_s;

    /*  [ids:@storage] (SUBPOS/SUBSTATE)
     */
    err = pdb_iterator_util_thaw(pdb, &state_s, state_e, "[ids:@%{bytes}]",
                                 &ids_s, &ids_e);
    if (err != 0) {
      /* [ids:..] can be omitted; in that case,
       *  set ids_s and ids_e to NULL.
       */
      state_s = state_s0;
      ids_s = NULL;
      ids_e = NULL;
    }
    err = pdb_iterator_util_thaw(pdb, &state_s, state_e,
                                 "%{extensions}%{(position/state)}",
                                 (pdb_iterator_property *)NULL, &subpit);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                   "could not thaw state: %.*s",
                   (int)(state_e - pit->pit_state_s), pit->pit_state_s);
      return err;
    }
    err =
        graphd_iterator_thaw(graphd, &subpit, pib, 0, loglevel, &sub_it, NULL);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "graphd_iterator_thaw", err, "%.*s",
                   (int)(subpit.pit_set_e - subpit.pit_set_s),
                   subpit.pit_set_s);
      goto err;
    }

    if (ids_s != NULL && ids_s < ids_e)
      store = graphd_iterator_resource_thaw(graphd, &ids_s, ids_e,
                                            &sort_storable_type);

    if (resume_id == PDB_ID_NONE)
      ;
    else if (store != NULL && store->sos_idset != NULL &&
             graph_idset_check(store->sos_idset, resume_id)) {
      /*  Only clear resume_id if it's actually in the set.
       *  Otherwise, we may have recovered an idset that
       *  was halfway through a restore in its own right.
       */
      resume_id = PDB_ID_NONE;
    } else {
      /*  Rewind the subiterator; we'll
       *  have to recover the idset by pulling
       *  ids out of it.
       */
      err = pdb_iterator_reset(pdb, sub_it);
      if (err != 0) goto err;
    }
  } else {
    subpit.pit_position_s = subpit.pit_position_e = NULL;
    subpit.pit_state_s = subpit.pit_state_e = NULL;

    err =
        graphd_iterator_thaw(graphd, &subpit, pib, 0, loglevel, &sub_it, NULL);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "graphd_iterator_thaw", err, "%.*s",
                   (int)(subpit.pit_set_e - subpit.pit_set_s),
                   subpit.pit_set_s);
      goto err;
    }
  }

  /* STATE (2) - CHECK
   */
  if (state_s != NULL && state_s < state_e) {
    /*  [OPT] (SUBPOS/SUBSTATE)
     */
    err = pdb_iterator_util_thaw(pdb, &state_s, state_e, "%{extensions}",
                                 (pdb_iterator_property *)NULL);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                   "could not thaw extensions");
      return err;
    }
    err = graphd_iterator_util_thaw_partial_subiterator(
        graphd, &state_s, state_e,
        PDB_ITERATOR_FREEZE_POSITION | PDB_ITERATOR_FREEZE_STATE, &subpit, pib,
        loglevel, &sub_check_it);
    if (err != 0) {
      cl_log_errno(
          cl, loglevel, "graphd_iterator_util_thaw_partial_subiterator", err,
          "%.*s", (int)(subpit.pit_set_e - subpit.pit_set_s), subpit.pit_set_s);
      goto err;
    }
  } else {
    sub_check_it = NULL;
  }

  err = graphd_iterator_sort_create_loc(greq, forward, &sub_it, it_out, file,
                                        line);
  pdb_iterator_destroy(graphd->g_pdb, &sub_it);

  if (err != 0) {
    cl_log_errno(cl, loglevel, "graphd_iterator_sort_create_loc", err,
                 "unexpected error");
    goto err;
  }

  /* Still a sort iterator? */
  if ((*it_out)->it_type != &sort_type) {
    if (store != NULL) graphd_storable_unlink(store);

    pdb_iterator_destroy(pdb, &sub_check_it);
    return 0;
  }

  sort = (*it_out)->it_theory;
  sort->sort_sub_check = sub_check_it;
  sub_check_it = NULL;

  /* Recover our horizon
   */
  err = update_horizon(*it_out);
  if (err != 0) goto err;

  sort->sort_idset_resume = resume_id;
  sort->sort_idset_storable = store;
  if (store != NULL) {
    /*  Replace the idset with our recovered one.
     */
    graph_idset_free(sort->sort_idset);
    sort->sort_idset = store->sos_idset;
    sort->sort_idset_last_added = store->sos_idset_last_added;
  }

  sort->sort_idset_pos_n = idset_pos_n;
  sort->sort_idset_pos_id = idset_pos_id;
  sort->sort_idset_pos = pos;

  /*  Recover our statistics informaiton, if it's cheap.
   */
  if (pdb_iterator_statistics_done(pdb, osort(*it_out)->sort_sub)) {
    bool forward;

    /*  Inherit results from the subiterator, except for our
     *  sortedness.
     */
    forward = pdb_iterator_forward(pdb, *it_out);
    pdb_iterator_statistics_copy(pdb, *it_out, osort(*it_out)->sort_sub);
    pdb_iterator_forward_set(pdb, *it_out, forward);
  }
  return 0;

err:
  pdb_iterator_destroy(pdb, it_out);
  pdb_iterator_destroy(pdb, &sub_check_it);
  pdb_iterator_destroy(pdb, &sub_it);
  if (store != NULL) graphd_storable_unlink(store);

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_iterator_sort_thaw: error %s",
         graphd_strerror(err));
  return err;
}
