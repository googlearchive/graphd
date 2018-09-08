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
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

/*  Spend this much cost on intersecting two tractable iterators
 */
#define GRAPHD_LINKSTO_INTERSECT_ESTIMATE_BUDGET (1024 * 10)

/* XXX	remember where you are in lto_id
 * XXX  reposition on lto_id when thawed
 */

GRAPHD_SABOTAGE_DECL;

/*  Linksto  -- an iterator over things that point to values
 *  	from another iterator.
 *
 *			hint,linkage
 *	  [ our results ]-----> [ subiterator ]
 *
 * 	The results are usually not sorted.
 */

/*  How many samples do we test to figure out the average fan-out?
 */
#define GRAPHD_LINKSTO_N_SAMPLES 5

/*  How many empty fanins should we skip until we give up?
 */
#define GRAPHD_LINKSTO_EMPTY_MAX 1024

/*  If a linksto-target has more than that many elements, we don't
 *  bother pre-evaluating it.
 */
#define GRAPHD_LINKSTO_PREEVALUATE_N 1024

/*  If preevaluation of a linksto iterator yields more than that
 *  many elements, give up on the pre-evaluation.
 */
#define GRAPHD_LINKSTO_PREEVALUATE_ID_N 1024

/* Invest this much before sticking with an unevaluated linksto.
 */
#define GRAPHD_LINKSTO_PREEVALUATE_BUDGET (1024 * 100)

/*  If our fan-in is up to this small, we'll turn into
 *  a fixed iterator.
 */
#define GRAPHD_LINKSTO_FANIN_FIXED_MAX 25

typedef enum {
  LTO_TYPECHECK_INITIAL = 0,
  LTO_TYPECHECK_USE_ID = 1,
  LTO_TYPECHECK_CHECK_MORE = 2,
  LTO_TYPECHECK_FIND_MORE = 3,
  LTO_TYPECHECK_NEXT_MORE = 4

} lto_stat_tc_state;

#define LTO_NEXT_SUBFANIN 0
#define LTO_NEXT_TYPECHECK 1
#define LTO_NEXT_UNSPECIFIED (-1)

typedef struct graphd_iterator_linksto {
  graphd_handle *lto_graphd;
  pdb_handle *lto_pdb;
  cm_handle *lto_cm;
  cl_handle *lto_cl;
  graphd_request *lto_greq;

  int lto_linkage;

  /*  Original only:
   */
  int lto_next_method;

  pdb_id lto_source;
  pdb_iterator *lto_sub;
  pdb_iterator *lto_fanin;

  int lto_statistics_state;
  pdb_iterator *lto_statistics_sub;

  /*  During statistics, up to GRAPHD_LINKSTO_N_SAMPLES
   *  values we pulled out of the subiterators that had
   *  at least _some_ valid fanin.
   */

  pdb_id lto_statistics_id[GRAPHD_LINKSTO_N_SAMPLES];
  size_t lto_statistics_id_n;
  pdb_budget lto_stat_sf_cost;

  pdb_id lto_check_cached_id;
  unsigned int lto_check_cached_result : 1;

  /*  If we have a hint, try using it to generate IDs that
   *  we test on the subiterator - that may be faster.
   */
  pdb_id lto_stat_tc_id[GRAPHD_LINKSTO_N_SAMPLES];

  size_t lto_stat_tc_trial_n;
  size_t lto_stat_tc_id_n;

  int lto_stat_tc_state;
  pdb_iterator *lto_stat_tc_sub;
  pdb_iterator *lto_stat_tc_hint;
  pdb_id lto_stat_tc_endpoint_id;
  pdb_budget lto_stat_tc_cost;

  pdb_budget lto_stat_budget_max;

  /*  The direction that the caller wants.  Influences how we
   *  assign the budget during statistics.
   */
  graphd_direction lto_direction;

  /*  During statistics, the total cumulative valid
   *  fan-in from the values we pulled out.  May be 0.
   */
  size_t lto_statistics_fanin_n;

  /*  During statistics, how many times we pull a value out
   *  of the subiterator.
   */
  size_t lto_statistics_sub_n;

  /*  PDB_ID_NONE or the most recently returned ID.
   */
  pdb_id lto_id;

  /*  PDB_ID_NONE or the position we need to go to
   *  before resuming, and our method is TYPECHECK.
   */
  pdb_id lto_resume_id;

  /*  PDB_ID_NONE or the position we need to go to
   *  before resuming in the subiterator, and our
   *  method is LTO_NEXT_SUBFANIN.
   */
  pdb_id lto_sub_id;

  /*  Either NULL (as from GRAPH_MAKE_NULL), or
   *  a hint-GUID that the links to the output
   *  of lto_sub have in common.
   */
  graph_guid lto_hint_guid;
  pdb_id lto_hint_id;
  int lto_hint_linkage;
  unsigned int lto_hint_vip : 1;
  unsigned int lto_hint_vip_compiled : 1;

  /* NULL or an iterator over the type instances.
   *  We use it for intersections with fanins.
   */
  pdb_iterator *lto_hint_it;

  unsigned int lto_thawed : 1;

} graphd_iterator_linksto;

/*  The code below features four restartable functions.
 *
 *  If a restartable function returns PDB_ERR_MORE, the iterator
 *  call state will be set to values that causes the call to resume
 *  where it left off, if called again.
 *
 *  The RESUME_STATE(..) macro is a "case:" target to the
 *  initial switch.
 */
#undef RESUME_STATE
#define RESUME_STATE(it, st)   \
  while (0) {                  \
    case st:                   \
      (it)->it_call_state = 0; \
  }

/*  The SAVE_STATE(..) macro prepares a resume, always to a
 *  RESUME_STATE(..) macro a few lines above the SAVE_STATE.
 */
#undef SAVE_STATE
#define SAVE_STATE(it, st) return ((it)->it_call_state = (st)), PDB_ERR_MORE

/*  Like SAVE_STATE(..), but with LEAVE.
 */
#undef LEAVE_SAVE_STATE
#define LEAVE_SAVE_STATE(it, st)                                      \
  do {                                                                \
    cl_leave(cl, CL_LEVEL_VERBOSE, "still thinking (%d)", (int)(st)); \
    SAVE_STATE(it, st);                                               \
  } while (0)

static int linksto_hint_it(pdb_handle *pdb, pdb_iterator *it,
                           pdb_iterator **it_out) {
  graphd_iterator_linksto *lto = it->it_theory;
  char buf[200];
  int err;

  /*  If we don't yet have a hint iterator, make one.
   */
  if (*it_out != NULL) return 0;

  if (GRAPH_GUID_IS_NULL(lto->lto_hint_guid) ||
      lto->lto_hint_linkage >= PDB_LINKAGE_N)
    err = pdb_iterator_all_create(pdb, it->it_low, it->it_high, it->it_forward,
                                  it_out);
  else
    err = pdb_linkage_iterator(pdb, lto->lto_hint_linkage, &lto->lto_hint_guid,
                               it->it_low, it->it_high, it->it_forward,
                               /* error-if-null */ true, it_out);
  if (err != 0) {
    if (err == GRAPHD_ERR_NO) return err;

    cl_log_errno(lto->lto_cl, CL_LEVEL_FAIL, "pdb_linkage_iterator", err,
                 "%s(%s)", pdb_linkage_to_string(lto->lto_hint_linkage),
                 graph_guid_to_string(&lto->lto_hint_guid, buf, sizeof buf));
    return err;
  }
  return 0;
}

/*  Count the number of incoming VIP links for <id>.
 */
static int linksto_vip_count(pdb_handle *pdb, pdb_iterator *it, pdb_id id,
                             unsigned long long upper_bound,
                             pdb_budget *budget_inout,
                             unsigned long long *n_out) {
  graphd_iterator_linksto *lto = it->it_theory;
  cl_handle *cl = lto->lto_cl;
  pdb_iterator *fanin_it = NULL;
  char b1[200], b2[200];
  pdb_id intersect_id[PDB_VIP_MIN];
  size_t intersect_id_n;
  int err;
  bool is_vip = false;
  pdb_budget budget;

  /*  There is a hint.
   */
  cl_assert(cl, lto->lto_hint_linkage != PDB_LINKAGE_N);

  if (lto->lto_linkage == PDB_LINKAGE_TYPEGUID &&
      (lto->lto_hint_linkage == PDB_LINKAGE_LEFT ||
       lto->lto_hint_linkage == PDB_LINKAGE_RIGHT)) {
    /* Results are types; hint is left or right.
     */
    if (!lto->lto_hint_vip_compiled) {
      err = pdb_id_from_guid(pdb, &lto->lto_hint_id, &lto->lto_hint_guid);
      if (err != 0) return err;

      err = pdb_vip_id(pdb, lto->lto_hint_id, lto->lto_hint_linkage, &is_vip);
      if (err != 0) return err;

      lto->lto_hint_vip = is_vip;
      lto->lto_hint_vip_compiled = true;
    }

    /*  If we know the hint ID has vip size,
     *  we can count that array's size.
     */
    if (lto->lto_hint_vip) {
      graph_guid source_guid;

      err = pdb_id_to_guid(pdb, id, &source_guid);
      if (err != 0) return err;

      *budget_inout -= PDB_COST_HMAP_ARRAY;
      err = pdb_vip_id_count(pdb, lto->lto_hint_id, lto->lto_hint_linkage,
                             &source_guid, it->it_low, it->it_high, upper_bound,
                             n_out);
      return err;
    }
  } else if (lto->lto_hint_linkage == PDB_LINKAGE_TYPEGUID &&
             (lto->lto_linkage == PDB_LINKAGE_LEFT ||
              lto->lto_linkage == PDB_LINKAGE_RIGHT)) {
    err = pdb_vip_id(pdb, id, lto->lto_linkage, &is_vip);
    if (err != 0) return err;

    /*  We have a precompiled VIP array.
     */
    if (is_vip) {
      *budget_inout -= PDB_COST_HMAP_ARRAY;
      err = pdb_vip_id_count(pdb, id, lto->lto_linkage, &lto->lto_hint_guid,
                             it->it_low, it->it_high, upper_bound, n_out);

      /*  Unless it was too complicated, ...
       */
      if (err != PDB_ERR_MORE) return err;
    }
  }

  if (lto->lto_hint_it == NULL) {
    /*  Create an iterator over the type instances.  We'll
     *  intersect it with id's fan-out.
     */
    err = linksto_hint_it(pdb, it, &lto->lto_hint_it);
    if (err != 0) return err;
  }
  err = pdb_linkage_id_iterator(pdb, lto->lto_linkage, id, it->it_low,
                                it->it_high, it->it_forward,
                                /* error-if-null */ true, &fanin_it);
  if (err != 0) {
    return err;
  }

  /* Intersect lto->lto_hint_it with fanin(vip).
   */
  err = pdb_iterator_intersect(pdb, fanin_it, lto->lto_hint_it, it->it_low,
                               it->it_high, budget_inout, intersect_id,
                               &intersect_id_n,
                               sizeof(intersect_id) / sizeof(*intersect_id));
  if (err == 0) {
    pdb_iterator_destroy(pdb, &fanin_it);
    *n_out = intersect_id_n;
    return 0;
  }

  if (err != PDB_ERR_MORE) {
    pdb_iterator_destroy(pdb, &fanin_it);

    /*  Unexpected error.
     */
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_intersect", err, "%s and %s",
                 pdb_iterator_to_string(pdb, fanin_it, b1, sizeof b1),
                 pdb_iterator_to_string(pdb, lto->lto_hint_it, b2, sizeof b2));
    return err;
  }

  /*  Both sets are larger than a VIP array, but
   *  they're not left-or-right + typeguid.
   *
   *  The good news is they're tractable and easy
   *  to intersect.  Bad news, they may be quite large.
   */
  budget = GRAPHD_LINKSTO_INTERSECT_ESTIMATE_BUDGET;
  err = graphd_iterator_quick_intersect_estimate(
      lto->lto_graphd, fanin_it, lto->lto_hint_it, &budget, n_out);
  if (err != 0)
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_quick_intersect_estimate",
                 err, "intersecting %s and %s",
                 pdb_iterator_to_string(pdb, fanin_it, b1, sizeof b1),
                 pdb_iterator_to_string(pdb, lto->lto_hint_it, b2, sizeof b2));

  pdb_iterator_destroy(pdb, &fanin_it);
  return err;
}

/**
 * @brief Make an iterator that iterates over links to a single given ID
 *
 * @param graphd	module handle
 * @param linkage	along this pointer
 * @param hint_linkage	linkage of the hint GUID
 * @param hint_guid	NULL or a hint GUID of the link that we happen to know
 * @param sub_id	the ID of the linkage end point
 * @param low		low end of result ID range (included)
 * @param high		high end of result ID range (not included)
 * @param forward	true: sort low to high; false: high to low
 * @param it_out	assign result to this
 *
 * @return GRAPHD_ERR_NO if the intersection is empty.
 * @return 0 on success, a nonzero error code on error
 */
static int linksto_fanin(graphd_handle *graphd, int linkage, int hint_linkage,
                         graph_guid const *hint_guid, pdb_id sub_id, pdb_id low,
                         pdb_id high, bool forward, pdb_iterator **it_out) {
  pdb_handle *const pdb = graphd->g_pdb;
  cl_handle *const cl = pdb_log(pdb);
  char buf[200], gbuf[GRAPH_GUID_SIZE];
  int err;

  *it_out = NULL;

  cl_enter(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO, "%s(%llx)",
           pdb_linkage_to_string(linkage), (unsigned long long)sub_id);

  if ((linkage == PDB_LINKAGE_RIGHT || linkage == PDB_LINKAGE_LEFT) &&
      hint_linkage == PDB_LINKAGE_TYPEGUID && hint_guid != NULL &&
      !GRAPH_GUID_IS_NULL(*hint_guid)) {
    pdb_id hint_id;

    if ((err = pdb_id_from_guid(pdb, &hint_id, hint_guid)) != 0) {
      if (err != GRAPHD_ERR_NO)
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "%s",
                     graph_guid_to_string(hint_guid, buf, sizeof buf));
    } else {
      err = graphd_iterator_vip_create(graphd, sub_id, linkage, hint_id,
                                       hint_guid, low, high, forward,
                                       /* error-if-null */ true, it_out);
      if (err != 0 && err != GRAPHD_ERR_NO)
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_vip_create", err,
                     "%s(%llx; type=%s)", pdb_linkage_to_string(linkage),
                     (unsigned long long)sub_id,
                     graph_guid_to_string(hint_guid, buf, sizeof buf));
    }
  } else if ((hint_linkage == PDB_LINKAGE_RIGHT ||
              hint_linkage == PDB_LINKAGE_LEFT) &&
             linkage == PDB_LINKAGE_TYPEGUID && hint_guid != NULL &&
             !GRAPH_GUID_IS_NULL(*hint_guid)) {
    pdb_id hint_id;
    graph_guid sub_guid;

    if ((err = pdb_id_from_guid(pdb, &hint_id, hint_guid)) != 0) {
      if (err != GRAPHD_ERR_NO)
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "%s",
                     graph_guid_to_string(hint_guid, buf, sizeof buf));
    } else if ((err = pdb_id_to_guid(pdb, sub_id, &sub_guid)) != 0) {
      if (err != GRAPHD_ERR_NO)
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "%s",
                     graph_guid_to_string(hint_guid, buf, sizeof buf));
    } else {
      err = graphd_iterator_vip_create(graphd, hint_id, hint_linkage, sub_id,
                                       &sub_guid, low, high, forward,
                                       /* error-if-null */ true, it_out);
      if (err != 0 && err != GRAPHD_ERR_NO)
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_vip_create", err,
                     "%s(%llx; type=%s)", pdb_linkage_to_string(linkage),
                     (unsigned long long)sub_id,
                     graph_guid_to_string(&sub_guid, buf, sizeof buf));
    }
  } else {
    err = pdb_linkage_id_iterator(pdb, linkage, sub_id, low, high, forward,
                                  true, it_out);
    if (err != 0 && err != GRAPHD_ERR_NO)
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_linkage_id_iterator", err,
                   "%s(%llx)", pdb_linkage_to_string(linkage),
                   (unsigned long long)sub_id);
  }

  cl_leave(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO, "%s(%llx; %s) -> %s",
           pdb_linkage_to_string(linkage), (unsigned long long)sub_id,
           graph_guid_to_string(hint_guid, gbuf, sizeof gbuf),
           err ? graphd_strerror(err)
               : pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
  return err;
}

/**
 * @brief Preevaluate a "linksto" that turns into a small fixed set.
 *
 * @return PDB_ERR_MORE if that would take too long
 * @return 0 on success
 * @return other errors on unexpected system error.
 */
static int linksto_become_small_set(graphd_handle *graphd, pdb_iterator *it,
                                    pdb_iterator **it_out) {
  graphd_iterator_linksto *lto = it->it_theory;
  cl_handle *cl = lto->lto_cl;
  pdb_handle *pdb = lto->lto_pdb;
  int err = 0;
  size_t i;
  char buf[200];

  pdb_id short_id[GRAPHD_LINKSTO_FANIN_FIXED_MAX];
  size_t short_n = 0, short_n_last_fanin;

  /*  We've pulled a small set of samples from the subiterator,
   *  and counted the number of incoming links along our linkage
   *  to those samples.
   *
   *  That count came out small.
   *
   *  Given the links we pulled out of the subiterator,
   *  collect the actual samples in a fixed iterator.
   *  Turn into that fixed iterator.
   */
  short_n_last_fanin = short_n = 0;

  for (i = 0; i < lto->lto_statistics_id_n; i++) {
    pdb_iterator *it_fanin;
    pdb_id const source_id = lto->lto_statistics_id[i];

    /*  Load the set of primitives that point to that one
     *  particular id into the fan-in iterator.
     */
    err = linksto_fanin(lto->lto_graphd, lto->lto_linkage,
                        lto->lto_hint_linkage, &lto->lto_hint_guid, source_id,
                        it->it_low, it->it_high, it->it_forward, &it_fanin);
    if (err != 0) {
      if (err == GRAPHD_ERR_NO) {
        cl_log(cl, CL_LEVEL_VERBOSE,
               "pdb_iterator_next: no "
               "fan-in from source %llx",
               (unsigned long long)source_id);
        continue;
      }
      cl_log_errno(cl, CL_LEVEL_FAIL, "linksto_fanin", err,
                   "fan-in from source %llx", (unsigned long long)source_id);
      break;
    }

    /*  We need to check up to short_n_last_fanin
     *  to tell whether something already occurs.
     *
     *  (No ids are duplicate in *this* round's fanin,
     *  but there may be dupes in the *last* round.
     *
     */
    short_n_last_fanin = short_n;

    /*  Pull the IDs from <it_fanin>, and append them to our
     *  result array, if they're new.
     */
    for (;;) {
      pdb_budget budget = 999999;
      pdb_id fanin_id;
      size_t j;

      /* Get the next id from the fanin iterator.
       */
      err = pdb_iterator_next(pdb, it_fanin, &fanin_id, &budget);
      if (err != 0) {
        /* Done? */
        if (err == PDB_ERR_NO) break;

        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "it_fanin=%s",
                     pdb_iterator_to_string(pdb, it_fanin, buf, sizeof buf));
        pdb_iterator_destroy(pdb, &it_fanin);
        return err;
      }

      /* Have we seen it before?
       */
      for (j = 0; j < short_n_last_fanin; j++)
        if (short_id[j] == fanin_id) goto already_there;

      /* Overflow?
       */
      if (short_n >= sizeof(short_id) / sizeof(*short_id)) {
        cl_log(cl, CL_LEVEL_FAIL,
               "linksto_become_small_set: unexpected "
               "overflow while pulling fan-in from %s",
               pdb_iterator_to_string(pdb, it_fanin, buf, sizeof buf));
        pdb_iterator_destroy(pdb, &it_fanin);
        return GRAPHD_ERR_TOO_MANY_MATCHES;
      }
      short_id[short_n++] = fanin_id;

    already_there:;
    }
    pdb_iterator_destroy(pdb, &it_fanin);
  }

  /*  Become that fixed iterator.
   */
  err = graphd_iterator_fixed_create_array(
      lto->lto_graphd, short_id, short_n, 0, PDB_ITERATOR_HIGH_ANY,
      pdb_iterator_forward(pdb, it), it_out);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_fixed_create_array", err,
                 "short_n=%zu", short_n);
    return err;
  }

  return 0;
}

/**
 * @brief Preevaluate a "linksto" that turns into an "or" of
 *	a relatively small number of fan-ins.
 *
 * @return 0 on success
 * @return other errors on unexpected system error.
 */
static int linksto_become_small_or(pdb_iterator *it, pdb_iterator **it_out) {
  graphd_iterator_linksto *lto = it->it_theory;
  graphd_request *greq = lto->lto_greq;
  cl_handle *cl = lto->lto_cl;
  pdb_handle *pdb = lto->lto_pdb;
  int err = 0;
  size_t i;

  pdb_iterator *or_it = NULL;

  /*  Make an "or" of fanins.
   */
  err = graphd_iterator_or_create(greq, lto->lto_statistics_id_n,
                                  it->it_forward, &or_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_create", err,
                 "can't create or for %zu fan-ins!\n",
                 lto->lto_statistics_id_n);
    return err;
  }

  for (i = 0; i < lto->lto_statistics_id_n; i++) {
    pdb_iterator *it_fanin;
    pdb_id const source_id = lto->lto_statistics_id[i];

    /*  Load the set of primitives that point to that one
     *  particular id into the fan-in iterator.
     */
    err = linksto_fanin(lto->lto_graphd, lto->lto_linkage,
                        lto->lto_hint_linkage, &lto->lto_hint_guid, source_id,
                        it->it_low, it->it_high, it->it_forward, &it_fanin);
    if (err != 0) {
      if (err == GRAPHD_ERR_NO) continue;
      cl_log_errno(cl, CL_LEVEL_FAIL, "linksto_fanin", err,
                   "fan-in from source %llx", (unsigned long long)source_id);
      goto err;
    }

    err = graphd_iterator_or_add_subcondition(or_it, &it_fanin);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "linksto_fanin", err,
                   "fan-in from source %llx", (unsigned long long)source_id);
      goto err;
    }
    pdb_iterator_destroy(pdb, &it_fanin);
  }

  err = graphd_iterator_or_create_commit(or_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_create_commit", err,
                 "unexpected error");
    goto err;
  }

  *it_out = or_it;
  return 0;

err:
  pdb_iterator_destroy(pdb, &or_it);
  *it_out = NULL;

  return err;
}

static int linksto_next_resume(pdb_iterator *it, pdb_budget *budget_inout) {
  graphd_iterator_linksto *lto = it->it_theory;
  pdb_handle *pdb = lto->lto_pdb;
  cl_handle *cl = lto->lto_cl;
  pdb_id source = lto->lto_source;
  pdb_iterator *sub = lto->lto_sub;
  char buf[200];
  pdb_id id = 0;
  int err;

  cl_log(cl, CL_LEVEL_VERBOSE, "linksto_next_resume: catch up %s to %llx",
         pdb_iterator_to_string(pdb, lto->lto_sub, buf, sizeof buf),
         (unsigned long long)id);

  switch (it->it_call_state) {
    default:
      RESUME_STATE(it, 0)
      if (source == PDB_ID_NONE) {
        err = pdb_iterator_reset(pdb, sub);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_reset", err,
                       "couldn't reset %s",
                       pdb_iterator_to_string(pdb, sub, buf, sizeof buf));
          return err;
        }
      } else if (pdb_iterator_sorted(pdb, sub)) {
        pdb_id id_found;

        pdb_iterator_call_reset(pdb, sub);
        RESUME_STATE(it, 1)
        err = pdb_iterator_find(pdb, sub, source, &id_found, budget_inout);
        if (err == PDB_ERR_MORE)
          SAVE_STATE(it, 1);

        else if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_find", err,
                       "id=%llx, iterator=%s", (unsigned long long)source,
                       pdb_iterator_to_string(pdb, sub, buf, sizeof buf));
          return err;
        }
        cl_assert(cl, id_found == source);
      } else {
        err = pdb_iterator_reset(pdb, sub);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_reset", err, "sub=%s",
                       pdb_iterator_to_string(pdb, sub, buf, sizeof buf));
          return err;
        }
        do {
          pdb_iterator_call_reset(pdb, sub);
          RESUME_STATE(it, 2)
          err = pdb_iterator_next(pdb, sub, &id, budget_inout);
          if (err == PDB_ERR_MORE) {
            SAVE_STATE(it, 2);
          }
          if (err != 0) {
            cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next", err,
                         "looking for id=%llx in %s",
                         (unsigned long long)source,
                         pdb_iterator_to_string(pdb, sub, buf, sizeof buf));
            return err;
          }
          if (GRAPHD_SABOTAGE(lto->lto_graphd, *budget_inout <= 0)) {
            SAVE_STATE(it, 2);
          }
        } while (id != source);
      }
  }
  return 0;
}

static int linksto_find_loc(pdb_handle *pdb, pdb_iterator *it, pdb_id id_in,
                            pdb_id *id_out, pdb_budget *budget_inout,
                            char const *file, int line) {
  graphd_iterator_linksto *lto = it->it_theory;
  graphd_iterator_linksto *olto = it->it_original->it_theory;
  cl_handle *cl = lto->lto_cl;
  int err;
  pdb_budget budget_in = *budget_inout;
  char buf[200];

  PDB_IS_ITERATOR(cl, it);

  cl_log(cl, CL_LEVEL_DEBUG,
         "linksto_find_loc: %p linksto %llx (state=%d; in=%llx; lto_id=%llx) "
         "[%s:%d]",
         (void *)it, (unsigned long long)id_in, it->it_call_state,
         (unsigned long long)id_in, (unsigned long long)lto->lto_id, file,
         line);

  pdb_rxs_push(pdb, "FIND %p linksto %llx (state=%d; lto_id=%llx)", (void *)it,
               (unsigned long long)id_in, it->it_call_state,
               (unsigned long long)lto->lto_id);

  /*  Only sorted iterators can be called with "find".
   *  If we're getting called, we must either be someone
   *  else or have a LTO_NEXT_TYPECHECK method.
   */
  if (it->it_original != it && it->it_type != it->it_original->it_type) {
    /* We're really someone else. */

    err = pdb_iterator_refresh(pdb, it);
    cl_assert(cl, err != PDB_ERR_ALREADY);

    if (err == 0) {
      pdb_iterator_account_charge_budget(pdb, it, find);
      pdb_rxs_pop(pdb, "FIND %p linksto %llx: redirect", (void *)it,
                  (unsigned long long)id_in);
      cl_log(cl, CL_LEVEL_DEBUG, "linksto: redirect");
      return pdb_iterator_find_loc(pdb, it, id_in, id_out, budget_inout, file,
                                   line);
    }
    goto unexpected_error;
  }

  /* We're a TYPECHECK.
   */
  cl_assert(cl, olto->lto_next_method == LTO_NEXT_TYPECHECK);
  cl_assert(cl, pdb_iterator_sorted(pdb, it));

  /*  If we don't yet have a hint iterator, make one, and
   *  position it on the id that we're on.
   */
  if (lto->lto_hint_it == NULL) {
    cl_log(cl, CL_LEVEL_DEBUG,
           "linksto: updating hint iterator; positioning on %llx",
           (unsigned long long)lto->lto_id);
    err = linksto_hint_it(pdb, it, &lto->lto_hint_it);
    if (err != 0) goto unexpected_error;

    if (it->it_call_state != 0 && lto->lto_id != PDB_ID_NONE) {
      pdb_id id_found;
      char b1[200], b2[500];
      pdb_budget big_budget = 99999;

      /*  lto_hint_it is a tractable vip.  It shouldn't
       *  take long to position in it.
       */
      err = pdb_iterator_find(pdb, lto->lto_hint_it, lto->lto_id, &id_found,
                              &big_budget);

      if (err == PDB_ERR_MORE)
        cl_log(cl, CL_LEVEL_ERROR,
               "pdb_iterator_find: attempt to "
               "reposition on %llx in %s as "
               "part of a \"find\" in %s "
               "costs more than $99999 ?!",
               (unsigned long long)lto->lto_id,
               pdb_iterator_to_string(pdb, lto->lto_hint_it, b1, sizeof b1),
               pdb_iterator_to_string(pdb, it, b2, sizeof b2));

      if (err != 0) {
        cl_log(cl, CL_LEVEL_ERROR,
               "pdb_iterator_find: attempt to "
               "reposition on %llx in %s as part "
               "of a \"find\" in %s fails: %s",
               (unsigned long long)lto->lto_id,
               pdb_iterator_to_string(pdb, lto->lto_hint_it, b1, sizeof b1),
               pdb_iterator_to_string(pdb, it, b2, sizeof b2),
               graphd_strerror(err));
        return GRAPHD_ERR_BADCURSOR;
      }
      if (id_found != lto->lto_id) {
        cl_log(cl, CL_LEVEL_ERROR,
               "pdb_iterator_find: attempt to "
               "reposition on %llx in %s as part of a "
               "\"find\" in %s finds %llx instead.",
               (unsigned long long)lto->lto_id,
               pdb_iterator_to_string(pdb, lto->lto_hint_it, b1, sizeof b1),
               pdb_iterator_to_string(pdb, it, b2, sizeof b2),
               (unsigned long long)id_found);
        return GRAPHD_ERR_BADCURSOR;
      }
      cl_assert(cl, pdb_iterator_forward(pdb, it) ? lto->lto_id >= id_in
                                                  : lto->lto_id <= id_in);
    }
  }
  cl_assert(cl, lto->lto_hint_it != NULL);

  switch (it->it_call_state) {
    case 0:
      /*  Reset a resume ID, if any.
       */
      lto->lto_resume_id = PDB_ID_NONE;

      /*  Position the producer on, or beyond,
       *  our starting point.
       */
      lto->lto_id = id_in;
      err = pdb_iterator_find(pdb, lto->lto_hint_it, id_in, &lto->lto_id,
                              budget_inout);
      if (err != 0) {
        if (err == PDB_ERR_MORE) goto suspended;

        it->it_call_state = 0;
        goto done;
      }
      cl_assert(cl, pdb_iterator_forward(pdb, lto->lto_hint_it)
                        ? lto->lto_id >= id_in
                        : lto->lto_id <= id_in);

    /* Fall through */

    case LTO_TYPECHECK_USE_ID:
      it->it_call_state = 0;
      goto use_the_id;

    case LTO_TYPECHECK_CHECK_MORE:
      cl_assert(cl, pdb_iterator_forward(pdb, it) ? lto->lto_id >= id_in
                                                  : lto->lto_id <= id_in);
      it->it_call_state = 0;
      goto check_some_more;

    case LTO_TYPECHECK_NEXT_MORE:
      it->it_call_state = 0;
      goto next_some_more;

    default:
      cl_notreached(cl, "linksto_find: unexpected call_state %d",
                    it->it_call_state);
  }

  do {
    pdb_primitive pr;
    graph_guid endpoint_guid;

  /*  Pull another ID out of the type iterator.
   */
  next_some_more:
    err = pdb_iterator_next(pdb, lto->lto_hint_it, &lto->lto_id, budget_inout);
    if (err != 0) {
      if (err == PDB_ERR_MORE) it->it_call_state = LTO_TYPECHECK_NEXT_MORE;

      if (err != GRAPHD_ERR_NO)
        cl_log_errno(
            cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "it=%s",
            pdb_iterator_to_string(pdb, lto->lto_hint_it, buf, sizeof buf));
      goto done;
    }
    cl_assert(cl, pdb_iterator_forward(pdb, it) ? lto->lto_id >= id_in
                                                : lto->lto_id <= id_in);

  use_the_id:
    /*  Read the primitive associated with that ID.
     */
    *budget_inout -= PDB_COST_PRIMITIVE;
    err = pdb_id_read(pdb, lto->lto_id, &pr);
    if (err != 0) {
      cl_log_errno(
          cl, CL_LEVEL_FAIL, "pdb_id_read", err, "id=%s",
          pdb_id_to_string(pdb, lto->lto_stat_tc_id[lto->lto_stat_tc_id_n], buf,
                           sizeof buf));

      if (err == GRAPHD_ERR_NO) goto next;

      goto unexpected_error;
    }

    /*  Follow the linkage relationship from the ID.
     */
    if (!pdb_primitive_has_linkage(&pr, lto->lto_linkage)) {
      pdb_primitive_finish(pdb, &pr);
      goto next;
    }
    pdb_primitive_linkage_get(&pr, lto->lto_linkage, endpoint_guid);
    pdb_primitive_finish(pdb, &pr);

    /* Translate that GUID back into an ID .
     */
    err = pdb_id_from_guid(pdb, &lto->lto_sub_id, &endpoint_guid);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "guid=%s",
                   graph_guid_to_string(&endpoint_guid, buf, sizeof buf));
      if (err == GRAPHD_ERR_NO) goto next;

      goto unexpected_error;
    }

    /*  Check that ID against the subiterator.
     */
    cl_assert(cl, lto->lto_sub != NULL);

  /* We may have to resume this - there's no
   * telling how long these subiterator checks take!
   */
  check_some_more:
    cl_assert(cl, it->it_call_state == 0);
    cl_assert(cl, pdb_iterator_forward(pdb, it) ? lto->lto_id >= id_in
                                                : lto->lto_id <= id_in);

    err = pdb_iterator_check(pdb, lto->lto_sub, lto->lto_sub_id, budget_inout);
    if (err != 0) {
      if (err == GRAPHD_ERR_NO) goto next;

      if (err == PDB_ERR_MORE) {
        it->it_call_state = LTO_TYPECHECK_CHECK_MORE;
        cl_assert(cl, pdb_iterator_forward(pdb, it) ? lto->lto_id >= id_in
                                                    : lto->lto_id <= id_in);
        goto suspended;
      }

      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_check", err,
                   "checking %lld against %s", (long long)lto->lto_sub_id,
                   pdb_iterator_to_string(pdb, lto->lto_sub, buf, sizeof buf));
      goto unexpected_error;
    }

    /*  The subiterator accepted the sub_id.  So, its generator
     *  must be part of our result set. Yay.
     */
    *id_out = lto->lto_id;
    cl_assert(cl, pdb_iterator_forward(pdb, it) ? *id_out >= id_in
                                                : *id_out <= id_in);
    goto done;

  next:;
  } while (!GRAPHD_SABOTAGE(lto->lto_graphd, *budget_inout <= 0));

  /*  When re-entering, start at the top of the
   *  previous "while" loop.
   */
  it->it_call_state = LTO_TYPECHECK_NEXT_MORE;
  err = PDB_ERR_MORE;
  goto suspended;

done:
  if (err == 0) {
    pdb_rxs_pop(pdb, "FIND %p linksto %llx -> %llx ($%lld)", (void *)it,
                (unsigned long long)id_in, (unsigned long long)*id_out,
                (long long)(budget_in - *budget_inout));

    cl_assert(cl, pdb_iterator_forward(pdb, it) ? *id_out >= id_in
                                                : *id_out <= id_in);

    goto err;
  }

  if (err == PDB_ERR_MORE) {
  suspended:
    pdb_rxs_pop(pdb,
                "FIND %p linksto %llx suspended; state=%d, lto_id=%llx ($%lld)",
                (void *)it, (unsigned long long)id_in, it->it_call_state,
                (unsigned long long)lto->lto_id,
                (long long)(budget_in - *budget_inout));
    goto err;
  }
  if (err == GRAPHD_ERR_NO) {
    pdb_rxs_pop(pdb, "FIND %p linksto %llx EOF ($%lld)", (void *)it,
                (unsigned long long)id_in,
                (long long)(budget_in - *budget_inout));
    goto err;
  } else {
  unexpected_error:
    pdb_rxs_pop(pdb, "FIND %p linksto %llx error: %s ($%lld)", (void *)it,
                (unsigned long long)id_in, graphd_strerror(err),
                (long long)(budget_in - *budget_inout));
  }

err:
  pdb_iterator_account_charge_budget(pdb, it, find);
  cl_log(cl, CL_LEVEL_DEBUG, "linksto: find %p %llx: %s", (void *)it,
         (unsigned long long)id_in, err ? graphd_strerror(err) : "ok");
  return err;
}

/**
 * @brief Would this iterator eventually produce check_id ?
 *
 * @param pdb		module handle
 * @param it		linksto-iterator to check against
 * @param check_id	id to check
 * @param budget_inout	budget, subtracted from
 *
 * @return 0 for yes, GRAPHD_ERR_NO for no, other nonzero error codes on system
 * error.
 *
 */
static int linksto_check(pdb_handle *pdb, pdb_iterator *it, pdb_id check_id,
                         pdb_budget *budget_inout) {
  graphd_iterator_linksto *lto = it->it_theory;
  int err = 0;
  cl_handle *cl = lto->lto_cl;
  pdb_primitive pr;
  graph_guid guid;
  pdb_budget budget_in = *budget_inout;
  char buf[200];

#undef func
#define func "linksto_check"

  if (GRAPHD_SABOTAGE(lto->lto_graphd, *budget_inout < 0)) return PDB_ERR_MORE;

  pdb_rxs_push(pdb, "CHECK %p linksto %llx (state=%d)", (void *)it,
               (unsigned long long)check_id, it->it_call_state);

  switch (it->it_call_state) {
    default:
      RESUME_STATE(it, 0)
      if ((err = pdb_iterator_refresh(pdb, it)) != PDB_ERR_ALREADY) {
        if (err == 0) {
          pdb_rxs_pop(pdb,
                      "CHECK %p linksto %llx redirect "
                      "($%lld)",
                      (void *)it, (unsigned long long)check_id,
                      (long long)(budget_in - *budget_inout));
          pdb_iterator_account_charge_budget(pdb, it, check);
          return pdb_iterator_check(pdb, it, check_id, budget_inout);
        }
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_refresh", err, "it=%s",
                     pdb_iterator_to_string(pdb, it, buf, sizeof buf));
        pdb_rxs_pop(pdb,
                    "CHECK %p linksto %llx unexpected "
                    "error from refresh: %s"
                    "($%lld)",
                    (void *)it, (unsigned long long)check_id,
                    graphd_strerror(err),
                    (long long)(budget_in - *budget_inout));
        goto err;
      }

      if (lto->lto_check_cached_id == check_id) {
        *budget_inout -= PDB_COST_FUNCTION_CALL;
        pdb_iterator_account_charge_budget(pdb, it, check);
        pdb_rxs_pop(pdb,
                    "CHECK %p linksto %llx cached: %s "
                    "($%lld)",
                    (void *)it, (unsigned long long)check_id,
                    lto->lto_check_cached_result ? "no" : "ok",
                    (long long)(budget_in - *budget_inout));
        return lto->lto_check_cached_result ? PDB_ERR_NO : 0;
      }

      *budget_inout -= PDB_COST_PRIMITIVE;

      if ((err = pdb_id_read(pdb, check_id, &pr)) != 0) {
        cl_log_errno(lto->lto_cl, CL_LEVEL_FAIL, "pdb_id_read", err,
                     "couldn't read id %llx", (unsigned long long)check_id);
        pdb_rxs_pop(pdb, "CHECK %p linksto %llx: %s ($%lld)", (void *)it,
                    (unsigned long long)check_id, graphd_strerror(err),
                    (long long)(budget_in - *budget_inout));
        goto err;
      }

      if (!pdb_primitive_has_linkage(&pr, lto->lto_linkage)) {
        pdb_primitive_finish(pdb, &pr);
        pdb_rxs_pop(pdb, "CHECK %p linksto %llx no ($%lld)", (void *)it,
                    (unsigned long long)check_id,
                    (long long)(budget_in - *budget_inout));
        err = GRAPHD_ERR_NO;
        goto err;
      }
      if (!GRAPH_GUID_IS_NULL(lto->lto_hint_guid)) {
        graph_guid tmp_guid;
        if (!pdb_primitive_has_linkage(&pr, lto->lto_hint_linkage)) {
          pdb_primitive_finish(pdb, &pr);
          pdb_rxs_pop(pdb,
                      "CHECK %p linksto %llx no hint "
                      "linkage ($%lld)",
                      (void *)it, (unsigned long long)check_id,
                      (long long)(budget_in - *budget_inout));
          err = GRAPHD_ERR_NO;
          goto err;
        }
        pdb_primitive_linkage_get(&pr, lto->lto_hint_linkage, tmp_guid);
        if (!GRAPH_GUID_EQ(tmp_guid, lto->lto_hint_guid)) {
          pdb_primitive_finish(pdb, &pr);
          pdb_rxs_pop(pdb,
                      "CHECK %p linksto %llx wrong "
                      "hint linkage ($%lld)",
                      (void *)it, (unsigned long long)check_id,
                      (long long)(budget_in - *budget_inout));
          err = GRAPHD_ERR_NO;
          goto err;
        }
      }
      pdb_primitive_linkage_get(&pr, lto->lto_linkage, guid);
      pdb_primitive_finish(pdb, &pr);

      err = pdb_id_from_guid(pdb, &lto->lto_sub_id, &guid);
      if (err != 0)
        cl_log_errno(lto->lto_cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err,
                     "guid=%s", graph_guid_to_string(&guid, buf, sizeof buf));
      else {
        pdb_iterator_call_reset(pdb, lto->lto_sub);
        RESUME_STATE(it, 1)
        err = pdb_iterator_check(pdb, lto->lto_sub, lto->lto_sub_id,
                                 budget_inout);
        if (err != 0) {
          if (err == PDB_ERR_MORE)
            it->it_call_state = 1;

          else if (err != GRAPHD_ERR_NO)
            cl_log_errno(
                lto->lto_cl, CL_LEVEL_FAIL, "pdb_iterator_check", err,
                "unexpected error from %s",
                pdb_iterator_to_string(pdb, lto->lto_sub, buf, sizeof buf));
        }
      }
      break;
  }

  if (err != PDB_ERR_MORE) {
    lto->lto_check_cached_id = check_id;
    lto->lto_check_cached_result = !!err;
  }

  pdb_rxs_pop_test(pdb, err, budget_in - *budget_inout, "CHECK %p linksto %llx",
                   (void *)it, (unsigned long long)check_id);

err:
  pdb_iterator_account_charge_budget(pdb, it, check);
  return err;
}

/*  Do statistics using the "type and then down" method.
 *  Pull an ID out of the hint, follow its linkage pointer,
 *  and check that endpoint for membership in the
 *  subiterator.
 *
 *  GRAPHD_ERR_MORE	still running, please call again
 *
 *  0			either got enough data, or ran out
 *			and turned into a fixed array.
 *			Caller must check iterator ID to
 *			figure out what happened.
 */
static int linksto_statistics_typecheck(pdb_handle *pdb, pdb_iterator *it,
                                        pdb_budget *budget_inout) {
  graphd_iterator_linksto *lto = it->it_theory;
  graphd_request *greq = lto->lto_greq;
  cl_handle *cl = lto->lto_cl;
  int err;
  pdb_budget budget_in = *budget_inout;
  char buf[200];
  pdb_iterator *new_it;
  int tmp;

  PDB_IS_ITERATOR(cl, it);
  if (GRAPHD_SABOTAGE(lto->lto_graphd, *budget_inout < -100))
    return PDB_ERR_MORE;

  cl_enter(cl, CL_LEVEL_VERBOSE,
           "%s; budget=$%lld (total $%lld), state=%d (%p)",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf),
           (long long)*budget_inout, (long long)lto->lto_stat_tc_cost,
           lto->lto_stat_tc_state, (void *)it);

  tmp = lto->lto_stat_tc_state;
  lto->lto_stat_tc_state = LTO_TYPECHECK_INITIAL;
  switch (tmp) {
    case LTO_TYPECHECK_CHECK_MORE:
      goto check_some_more;
    case LTO_TYPECHECK_NEXT_MORE:
      goto next_some_more;
    default:
      break;
  }

  for (;;) {
    pdb_primitive pr;
    graph_guid endpoint_guid;

    /*  If we don't yet have a type iterator, make one.
     */
    if (lto->lto_stat_tc_hint == NULL) {
      err = linksto_hint_it(pdb, it, &lto->lto_stat_tc_hint);
      if (err != 0) {
        if (GRAPHD_ERR_NO == err) goto turn_into_small_array;
        cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
        return err;
      }
    }

  /*  Pull another ID out of the type iterator.
   */
  next_some_more:
    err = pdb_iterator_next(pdb, lto->lto_stat_tc_hint,
                            lto->lto_stat_tc_id + lto->lto_stat_tc_id_n,
                            budget_inout);
    if (err != 0) {
      if (err == PDB_ERR_MORE) {
        lto->lto_stat_tc_state = LTO_TYPECHECK_NEXT_MORE;
        lto->lto_stat_tc_cost += budget_in - *budget_inout;

        cl_leave(cl, CL_LEVEL_VERBOSE,
                 "suspended in sub next ($%lld, "
                 "total $%lld)",
                 (long long)(budget_in - *budget_inout),
                 (long long)lto->lto_stat_tc_cost);
        return PDB_ERR_MORE;
      }
      if (err == GRAPHD_ERR_NO) goto turn_into_small_array;

      cl_log_errno(
          cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "it=%s",
          pdb_iterator_to_string(pdb, lto->lto_stat_tc_hint, buf, sizeof buf));
      cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
      return err;
    }
    lto->lto_stat_tc_trial_n++;

    /*  Read the primitive associated with that ID.
     */
    *budget_inout -= PDB_COST_PRIMITIVE;
    if ((err = pdb_id_read(pdb, lto->lto_stat_tc_id[lto->lto_stat_tc_id_n],
                           &pr)) != 0) {
      cl_log_errno(
          cl, CL_LEVEL_FAIL, "pdb_id_read", err, "id=%s",
          pdb_id_to_string(pdb, lto->lto_stat_tc_id[lto->lto_stat_tc_id_n], buf,
                           sizeof buf));

      if (err == GRAPHD_ERR_NO) goto next;
      cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
      return err;
    }

    /*  Follow the linkage relationship from the ID.
     */
    if (!pdb_primitive_has_linkage(&pr, lto->lto_linkage)) {
      pdb_primitive_finish(pdb, &pr);
      goto next;
    }
    pdb_primitive_linkage_get(&pr, lto->lto_linkage, endpoint_guid);
    pdb_primitive_finish(pdb, &pr);

    /* Translate that GUID back into an ID .
     */
    err = pdb_id_from_guid(pdb, &lto->lto_stat_tc_endpoint_id, &endpoint_guid);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "guid=%s",
                   graph_guid_to_string(&endpoint_guid, buf, sizeof buf));
      if (err == GRAPHD_ERR_NO) goto next;

      cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
      return err;
    }

    /*  Check that ID against the subiterator.
     */
    if (lto->lto_stat_tc_sub == NULL) {
      err = pdb_iterator_clone(pdb, lto->lto_sub, &lto->lto_stat_tc_sub);
      if (err != 0) {
        cl_log_errno(
            cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "can't clone %s",
            pdb_iterator_to_string(pdb, lto->lto_sub, buf, sizeof buf));
        cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
        return err;
      }
    }
    cl_assert(cl, lto->lto_stat_tc_sub != NULL);

  /* We may have to resume this - there's no
   * telling how long these subiterator checks take!
   */
  check_some_more:
    err = pdb_iterator_check(pdb, lto->lto_stat_tc_sub,
                             lto->lto_stat_tc_endpoint_id, budget_inout);
    if (err != 0) {
      if (err == GRAPHD_ERR_NO) goto next;

      if (err == PDB_ERR_MORE) {
        lto->lto_stat_tc_state = LTO_TYPECHECK_CHECK_MORE;
        lto->lto_stat_tc_cost += budget_in - *budget_inout;

        cl_leave(cl, CL_LEVEL_VERBOSE,
                 "suspended in sub check ($%lld, "
                 "total $%lld)",
                 (long long)(budget_in - *budget_inout),
                 (long long)lto->lto_stat_tc_cost);
        return PDB_ERR_MORE;
      }
      cl_log_errno(
          cl, CL_LEVEL_FAIL, "pdb_iterator_check", err,
          "checking %lld against %s", (long long)lto->lto_stat_tc_endpoint_id,
          pdb_iterator_to_string(pdb, lto->lto_stat_tc_sub, buf, sizeof buf));
      cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
      return err;
    }

    /*  The subiterator accepted the ID.  It is
     *  part of our result set.  Yay.
     */
    lto->lto_stat_tc_id_n++;
    if (lto->lto_stat_tc_id_n >= GRAPHD_LINKSTO_N_SAMPLES) break;
  next:
    if (GRAPHD_SABOTAGE(lto->lto_graphd, *budget_inout <= 0)) {
      cl_leave(cl, CL_LEVEL_VERBOSE,
               "suspended iterating "
               "over type instances ($%lld)",
               budget_in - *budget_inout);
      lto->lto_stat_tc_cost += budget_in - *budget_inout;
      return PDB_ERR_MORE;
    }
  }

  /*  We read enough results, and are done.
   */
  lto->lto_stat_tc_cost += budget_in - *budget_inout;
  cl_leave(cl, CL_LEVEL_VERBOSE, "done ($%lld, total $%lld)",
           (long long)(budget_in - *budget_inout),
           (long long)lto->lto_stat_tc_cost);
  return 0;

turn_into_small_array:
  err = graphd_iterator_fixed_create_array(
      lto->lto_graphd, lto->lto_stat_tc_id, lto->lto_stat_tc_id_n, it->it_low,
      it->it_high, it->it_forward, &new_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_fixed_create_array", err,
                 "can't become small array?!");
    cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
  }

  graphd_iterator_substitute(greq, it, new_it);
  cl_leave(cl, CL_LEVEL_VERBOSE, "become %s",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf));

  return err;
}

#if 0
/*
 * Calculate a VIP iterator for two linkage endpoints or
 * return an error if we don't index your shape
 */
static int linksto_make_one_vip(
	graphd_handle		* graph,
	pdb_handle		* pdb,
	int			  linkage1,
	pdb_id			  id1,
	int			  linkage2,
	pdb_id			  id2,
	pdb_id			  low,
	pdb_id			  high,
	bool			  forward,
	bool			  error_if_null,
	pdb_iterator		** new_it)
{
	int rllinkage;
	int err;
	pdb_id typeid, rlid;
	graph_guid guid;

	if ((linkage1 == PDB_LINKAGE_LEFT ||
		linkage1 == PDB_LINKAGE_RIGHT)
	    && linkage2 == PDB_LINKAGE_TYPEGUID)
	{
		rllinkage = linkage1;
		typeid = id2;
		rlid = id1;
	}
	else if ((linkage2 == PDB_LINKAGE_LEFT ||
		   linkage2 == PDB_LINKAGE_RIGHT)
		&& linkage1 == PDB_LINKAGE_TYPEGUID)
	{
		rllinkage = linkage2;
		typeid = id1;
		rlid = id2;
	}
	else
	{
		/*
		 * Shape not indexed
		 */
		return GRAPHD_ERR_NOT_SUPPORTED;
	}
	err = pdb_id_to_guid(pdb, typeid, &guid);

	if (err)
		return err;

	err = graphd_iterator_vip_create(
		graph,
		rlid,
		rllinkage,
		typeid,
		&guid,
		low,
		high,
		forward,
		error_if_null,
		new_it);
	return err;

}
#endif

static int linksto_statistics_subfanin(pdb_handle *pdb, pdb_iterator *it,
                                       pdb_budget *budget_inout) {
  graphd_iterator_linksto *lto = it->it_theory;
  cl_handle *cl = lto->lto_cl;
  int err;
  unsigned long long upper_bound = pdb_primitive_n(pdb), n;
  char buf[200];
  pdb_iterator *new_it;
  pdb_budget budget_in = *budget_inout;

  PDB_IS_ITERATOR(cl, it);
  if (GRAPHD_SABOTAGE(lto->lto_graphd, *budget_inout < -100))
    return PDB_ERR_MORE;

  cl_enter(cl, CL_LEVEL_VERBOSE, "%s; budget=%lld, state=%d, id=%llx (%p)",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf),
           (long long)*budget_inout, lto->lto_statistics_state,
           (unsigned long long)it->it_id, (void *)it);

  /*  is-sorted:
   * 	as long as we have a tractable (small) number of destinations,
   *	a "links-to" iterator can transform into an "or"
   *  	iterator of the linkages of the single values.
   *      Those are sorted.  (A merge of sorted lists is itself sorted.)
   *
   *  estimate n:
   *	Pull a few values of out the destination; see what
   * 	the average fan-out is; add that.
   *
   *  estimate check chance:
   *	divide everything by the estimated n.
   *
   *  estimate check cost:
   *	read a primitive, figure out what it points to,
   *	and pass that to the destination for a check.
   *
   *  estimate production cost:
   *	destination's production cost, plus cost for
   *	following the fan-out.
   */

  if (upper_bound > it->it_high - it->it_low)
    upper_bound = it->it_high - it->it_low;

  if (upper_bound == 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "null");
    return pdb_iterator_null_become(pdb, it);
  }

  /*  Redo the subiterator statistics,
   *  if we need to.
   */
  switch (lto->lto_statistics_state) {
    default:
    case 0:
      err = pdb_iterator_statistics(pdb, lto->lto_sub, budget_inout);
      if (err != 0) {
        if (err == PDB_ERR_MORE) {
          lto->lto_stat_sf_cost += budget_in - *budget_inout;

          cl_leave(cl, CL_LEVEL_VERBOSE,
                   "still thinking (total $%lld) "
                   "(sub.stats; budget: $%lld)",
                   (long long)lto->lto_stat_sf_cost, (long long)*budget_inout);
          lto->lto_statistics_state = 0;
          return PDB_ERR_MORE;
        }
        cl_leave(cl, CL_LEVEL_VERBOSE, "sub.stats fails: %s",
                 graphd_strerror(err));
        return err;
      }
      lto->lto_statistics_state = 0;

      err = pdb_iterator_refresh_pointer(pdb, &lto->lto_sub);
      if (err == 0) {
        unsigned long long old_id = it->it_id;
        it->it_id = pdb_iterator_new_id(pdb);
        cl_log(cl, CL_LEVEL_DEBUG,
               "linksto_statistics_subfanin: "
               "NEW ID %llx -> %llx"
               " for %s after pdb_iterator_refresh_pointer",
               old_id, (unsigned long long)it->it_id,
               pdb_iterator_to_string(pdb, it, buf, sizeof buf));

        pdb_iterator_destroy(pdb, &lto->lto_statistics_sub);
      } else if (err != PDB_ERR_ALREADY) {
        cl_leave(cl, CL_LEVEL_VERBOSE, "sub.stats fails: %s",
                 graphd_strerror(err));
        return err;
      }

      /*  Make a copy of the subiterator for purposes of
       *  gathering statistics.
       */
      if (lto->lto_statistics_sub == NULL) {
        err = pdb_iterator_clone(pdb, lto->lto_sub, &lto->lto_statistics_sub);
        if (err != 0) {
          cl_log_errno(
              cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "%s",
              pdb_iterator_to_string(pdb, lto->lto_sub, buf, sizeof buf));
          cl_leave(cl, CL_LEVEL_VERBOSE, "sub.clone fails: %s",
                   graphd_strerror(err));
          return err;
        }
      }
      lto->lto_statistics_sub_n = 0;
      lto->lto_statistics_fanin_n = 0;
      lto->lto_statistics_id_n = 0;
      lto->lto_stat_sf_cost = 0;

    /*  We're pulling out samples from our destination, and
     *  will keep pulling until we either run out of budget
     *  or GRAPHD_LINKSTO_N_SAMPLES have been found that actually
     *  have the linkage we're following.
     *
     *  Depending on the shape of the data, this may take
     *  a while, and be interrupted; indeed the whole branch
     *  of nested iterators we're in may turn out to be
     *  fruitless and may be neglected in favor of another,
     *  more productive branch.
     */
    case 2:
      while (lto->lto_statistics_id_n < GRAPHD_LINKSTO_N_SAMPLES
             /* && lto->lto_statistics_sub_n <= GRAPHD_LINKSTO_EMPTY_MAX */) {
        lto->lto_statistics_state = 2;
        cl_assert(cl, lto->lto_statistics_sub != NULL);

        err = pdb_iterator_next(
            pdb, lto->lto_statistics_sub,
            lto->lto_statistics_id + lto->lto_statistics_id_n, budget_inout);
        if (err == PDB_ERR_MORE) {
          lto->lto_stat_sf_cost += budget_in - *budget_inout;
          cl_leave(cl, CL_LEVEL_VERBOSE, "still thinking (sub.next/1)");
          return PDB_ERR_MORE;
        }
        if (err == GRAPHD_ERR_NO) {
          if (lto->lto_statistics_id_n > 1) goto become_small_set;

          goto small_set_of_destinations;
        } else if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next", err,
                       "sub-iterator: %s",
                       pdb_iterator_to_string(pdb, lto->lto_statistics_sub, buf,
                                              sizeof buf));
          cl_leave(cl, CL_LEVEL_VERBOSE, "sub.next fails: %s",
                   graphd_strerror(err));
          return err;
        }

        lto->lto_statistics_sub_n++;

        if (GRAPH_GUID_IS_NULL(lto->lto_hint_guid)) {
          /*  How many other IDs point to this?
           */
          err = pdb_linkage_count_est(
              pdb, lto->lto_linkage,
              lto->lto_statistics_id[lto->lto_statistics_id_n], it->it_low,
              it->it_high, upper_bound, &n);
          *budget_inout -= PDB_COST_GMAP_ARRAY;
        } else {
          /*  How many other links with the right
           *  typeguid point to this?
           */
          cl_assert(cl, lto->lto_statistics_id_n <
                            sizeof(lto->lto_statistics_id) /
                                sizeof(*lto->lto_statistics_id));

          err = linksto_vip_count(
              pdb, it, lto->lto_statistics_id[lto->lto_statistics_id_n],
              upper_bound, budget_inout, &n);
        }
        cl_assert(cl, err != PDB_ERR_MORE);

        if (err == GRAPHD_ERR_NO || (err == 0 && n == 0)) {
          cl_log(cl, CL_LEVEL_VERBOSE,
                 "subiterator produces %llx (#%zu), but nobody in %llx..%llx "
                 "(upper_bound %llx) points to it.",
                 (long long)lto->lto_statistics_id[lto->lto_statistics_id_n],
                 lto->lto_statistics_sub_n, (unsigned long long)it->it_low,
                 (unsigned long long)it->it_high, upper_bound);

          goto next_round;
        }
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_linkage_count", err, "%s(%llx)",
                       pdb_linkage_to_string(lto->lto_linkage),
                       (unsigned long long)
                           lto->lto_statistics_id[lto->lto_statistics_id_n]);
          cl_leave(cl, CL_LEVEL_VERBOSE, "pdb_linkage_count fails: %s",
                   graphd_strerror(err));
          return err;
        }
        lto->lto_statistics_id_n++;
        lto->lto_statistics_fanin_n += n;

        if (lto->lto_statistics_id_n >= GRAPHD_LINKSTO_N_SAMPLES) break;

      next_round:
        if (GRAPHD_SABOTAGE(lto->lto_graphd, *budget_inout <= 0)) {
          cl_log(cl, CL_LEVEL_VERBOSE,
                 "graphd_iterator_linksto: looked at "
                 "%lu source results; found %lu links. "
                 " Out of budget; to be continued ..(3)",
                 (unsigned long)lto->lto_statistics_sub_n,
                 (unsigned long)lto->lto_statistics_id_n);

          lto->lto_statistics_state = 3;
          lto->lto_stat_sf_cost += budget_in - *budget_inout;

          cl_leave(cl, CL_LEVEL_VERBOSE,
                   "still thinking (got %d so far, "
                   "for %lld total) "
                   "($%lld)",
                   (int)lto->lto_statistics_id_n, lto->lto_stat_sf_cost,
                   budget_in - *budget_inout);
          return PDB_ERR_MORE;
        }
        case 3:;
      }
  }

  lto->lto_stat_sf_cost += budget_in - *budget_inout;
  cl_leave(cl, CL_LEVEL_VERBOSE, "done ($%lld, total $%lld)",
           budget_in - *budget_inout, lto->lto_stat_sf_cost);
  return 0;

become_small_set:

  /*  There's no ordering in play,
   *  we ran out of sub-samples early,
   *  and there was relatively little fan-in?
   */
  if (pdb_iterator_ordering(pdb, it) == NULL &&
      lto->lto_statistics_id_n < GRAPHD_LINKSTO_N_SAMPLES &&
      lto->lto_statistics_fanin_n <= GRAPHD_LINKSTO_FANIN_FIXED_MAX) {
    pdb_iterator *new_it;

    err = linksto_become_small_set(lto->lto_graphd, it, &new_it);
    if (err == 0) {
      err = graphd_iterator_substitute(lto->lto_greq, it, new_it);
      if (err != 0) {
        char b1[200], b2[200];

        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_substitute", err,
                     "%s by %s", pdb_iterator_to_string(pdb, it, b1, sizeof b1),
                     pdb_iterator_to_string(pdb, new_it, b2, sizeof b2));

        cl_leave(cl, CL_LEVEL_VERBOSE, "%p: error %s", (void *)it,
                 graphd_strerror(err));
        return err;
      }
      cl_leave(cl, CL_LEVEL_VERBOSE, "%p linksto -> fixed", it);
      return 0;
    }
  }

small_set_of_destinations:

  /*  We have a tractably small set of destinations.
   *  They're in lto->lto_statistics_id[0.._n-1]
   *
   *  What's our setsize?
   *	0  -- turn it into null.
   *	1  -- turn it into a single GMAP
   * 	>1 -- make an OR of the specific GMAPs.
   */
  if (lto->lto_statistics_id_n == 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "no sub-IDs");
    return pdb_iterator_null_become(pdb, it);
  }

  err = linksto_become_small_or(it, &new_it);
  if (err != 0) {
    cl_leave_err(cl, CL_LEVEL_FAIL, err,
                 "pdb_linkage_id_iterator fails unexpectedly: id=%llx",
                 (unsigned long long)lto->lto_statistics_id[0]);
    return err;
  }

  /*  We've changed into a simpler iterator.
   *  Destroy our local state data, and let the new iterator
   *  figure out the statistics in our stead.
   */
  err = pdb_iterator_substitute(pdb, it, new_it);
  cl_assert(cl, err == 0);

  cl_leave(cl, CL_LEVEL_VERBOSE, "redirect");
  return pdb_iterator_statistics(pdb, it, budget_inout);
}

/*  How much of the overall value range have we explored
 *  during the subfanin statistics?
 *
 *  If we don't know, the call returns false.
 */
static bool subfanin_coverage(pdb_handle *pdb, pdb_iterator *it,
                              double *coverage_out) {
  graphd_iterator_linksto *lto = it->it_theory;
  unsigned long long sub_n;

  if (lto->lto_sub == NULL || !pdb_iterator_n_valid(pdb, lto->lto_sub))
    return false;

  sub_n = pdb_iterator_n(pdb, lto->lto_sub);
  if (sub_n < lto->lto_statistics_sub_n) {
    *coverage_out = 0.9;
    return true;
  }

  if (lto->lto_statistics_sub_n == 0) {
    *coverage_out = 0.0;
    return true;
  }
  *coverage_out = (double)lto->lto_statistics_sub_n / sub_n;
  return true;
}

/*  How much of the overall value range have we explored
 *  during the typecheck statistics?
 *
 *  If we don't know, the call returns false.
 */
static bool typecheck_coverage(pdb_handle *pdb, pdb_iterator *it,
                               double *coverage_out) {
  graphd_iterator_linksto *lto = it->it_theory;
  unsigned long long sub_n;

  if (lto->lto_stat_tc_trial_n == 0) {
    *coverage_out = 0;
    return true;
  }

  /*  If we don't yet have a type iterator, make one.
   */
  if (lto->lto_hint_it == NULL) {
    int err = linksto_hint_it(pdb, it, &lto->lto_hint_it);
    if (err != 0) {
      char buf[200];
      cl_log_errno(lto->lto_cl, CL_LEVEL_FAIL, "linksto_hint_it", err, "it=%s",
                   pdb_iterator_to_string(pdb, it, buf, sizeof buf));
      return false;
    }
  }
  if (!pdb_iterator_n_valid(pdb, lto->lto_hint_it)) return false;

  sub_n = pdb_iterator_n(pdb, lto->lto_hint_it);
  if (sub_n < lto->lto_stat_tc_trial_n)
    *coverage_out = 0.9;
  else {
    *coverage_out = (double)lto->lto_stat_tc_trial_n / sub_n;
  }
  return true;
}

static int linksto_statistics(pdb_handle *pdb, pdb_iterator *it,
                              pdb_budget *budget_inout) {
  pdb_budget budget_in = *budget_inout;
  graphd_iterator_linksto *lto = it->it_theory;
  cl_handle *cl = lto->lto_cl;
  int err;
  pdb_budget sub_budget, subfanin_budget, typecheck_budget;
  unsigned long long upper_bound = pdb_primitive_n(pdb), est, sub_n;
  double average_fan_out;
  char buf[200];
  pdb_id me;
  char const *sub_ordering;
  bool have_preference = false;

  PDB_IS_ITERATOR(cl, it);
  if (GRAPHD_SABOTAGE(lto->lto_graphd, *budget_inout <= 0)) return PDB_ERR_MORE;

  pdb_rxs_push(pdb, "STAT %p linksto state=%d", (void *)it, it->it_call_state);

  switch (lto->lto_next_method) {
    case LTO_NEXT_TYPECHECK:
      typecheck_budget = *budget_inout;
      subfanin_budget = 0;
      have_preference = true;
      break;

    case LTO_NEXT_SUBFANIN:
      subfanin_budget = *budget_inout;
      typecheck_budget = 0;
      have_preference = true;
      break;

    default:
      /*  If we have an ordering and an ordered sort root
       *  in the subiterator, give preference to the subfanin.
       *
       *  If we have a direction, give preference to the
       *  type iterator.
       *
       *  Otherwise, split the budget evenly.
       */
      if (lto->lto_direction == GRAPHD_DIRECTION_ORDERING &&
          pdb_iterator_ordering(pdb, it) != NULL &&
          (sub_ordering = pdb_iterator_ordering(pdb, lto->lto_sub)) != NULL &&
          (!pdb_iterator_ordered_valid(pdb, lto->lto_sub) ||
           pdb_iterator_ordered(pdb, lto->lto_sub)) &&
          pdb_iterator_ordering_wants(pdb, it, sub_ordering)) {
        /*  Prefer subfanin.
         */
        subfanin_budget = 1 + *budget_inout * 9 / 10;
        typecheck_budget = 1 + *budget_inout - subfanin_budget;
        have_preference = true;
      } else if (lto->lto_direction == GRAPHD_DIRECTION_FORWARD ||
                 lto->lto_direction == GRAPHD_DIRECTION_BACKWARD) {
        /*  Prefer typecheck.
         */
        typecheck_budget = 1 + *budget_inout * 9 / 10;
        subfanin_budget = 1 + *budget_inout - typecheck_budget;
        have_preference = true;
      } else
        typecheck_budget = subfanin_budget = (*budget_inout + 1) / 2;
      break;
  }

  cl_assert(cl, subfanin_budget >= 0);
  cl_assert(cl, typecheck_budget >= 0);

  me = it->it_id;

  for (;;) {
    /*  At least one of the two is done measuring?
     */
    if (lto->lto_stat_tc_id_n >= GRAPHD_LINKSTO_N_SAMPLES ||
        lto->lto_statistics_id_n >= GRAPHD_LINKSTO_N_SAMPLES) {
      double A_sf;
      double A_tc;
      pdb_budget C_sf, C_tc;

      int method_by_done = LTO_NEXT_UNSPECIFIED;
      int method_by_area = LTO_NEXT_UNSPECIFIED;

      /*  We may be in this just for the counting -- in that
       *  case, our next method is already set; we're done.
       */
      if (lto->lto_next_method != LTO_NEXT_UNSPECIFIED) {
        cl_assert(lto->lto_cl, lto->lto_next_method == LTO_NEXT_SUBFANIN ||
                                   lto->lto_next_method == LTO_NEXT_TYPECHECK);
        goto have_method;
      }

      /*  If we used the budget to mediate preference,
       *  if we were thawed and lost part of our statistics,
       *  or if for some reason can't determine our
       *  coverage, we do what we used to, which is
       *  go by who completed first.
       */
      if (lto->lto_thawed || have_preference ||
          !subfanin_coverage(pdb, it, &A_sf) ||
          !typecheck_coverage(pdb, it, &A_tc)) {
        lto->lto_next_method = lto->lto_stat_tc_id_n >= GRAPHD_LINKSTO_N_SAMPLES
                                   ? LTO_NEXT_TYPECHECK
                                   : LTO_NEXT_SUBFANIN;
        cl_assert(lto->lto_cl, lto->lto_next_method == LTO_NEXT_SUBFANIN ||
                                   lto->lto_next_method == LTO_NEXT_TYPECHECK);
        goto have_method;
      }

      /*  Calculate the winner in two ways: by
       *  area covered per cost, and by who's done
       *  first (found five results first).
       *
       *  If they agree, we can stop now.
       */
      cl_assert(lto->lto_cl, A_tc < 1.1);
      cl_assert(lto->lto_cl, A_sf < 1.1);

      if (A_tc < 0.0001) A_tc = 0.0001;
      if (A_sf < 0.0001) A_sf = 0.0001;

      if ((C_tc = lto->lto_stat_tc_cost) == 0) C_tc = 1;
      if ((C_sf = lto->lto_stat_sf_cost) == 0) C_sf = 1;

      method_by_area = ((A_tc / C_tc) > (A_sf / C_sf) ? LTO_NEXT_TYPECHECK
                                                      : LTO_NEXT_SUBFANIN);

      method_by_done =
          (lto->lto_stat_tc_id_n < GRAPHD_LINKSTO_N_SAMPLES
               ? LTO_NEXT_SUBFANIN
               : (lto->lto_statistics_id_n >= GRAPHD_LINKSTO_N_SAMPLES
                      ? method_by_area
                      : LTO_NEXT_TYPECHECK));

      cl_log(cl, CL_LEVEL_VERBOSE,
             "linksto_statistics: "
             "Atc=%.2lf, Ctc=$%lld, "
             "Asf=%.2lf, Csf=$%lld; "
             "mbA: %d; mbD: %d",
             A_tc, (long long)C_tc, A_sf, (long long)C_sf, method_by_area,
             method_by_done);

      if (method_by_done == method_by_area) {
        lto->lto_next_method = method_by_area;
        cl_assert(lto->lto_cl, lto->lto_next_method == LTO_NEXT_SUBFANIN ||
                                   lto->lto_next_method == LTO_NEXT_TYPECHECK);

        goto have_method;
      }

      /*  One is done, but the other is more effective.
       *  (The one that is done probably just got lucky.)
       *  Give the remaining competitor the other
       *  guy's budget.
       */
      if (lto->lto_statistics_id_n >= GRAPHD_LINKSTO_N_SAMPLES) {
        typecheck_budget += subfanin_budget;
        subfanin_budget = 0;
      } else {
        subfanin_budget += typecheck_budget;
        typecheck_budget = 0;
      }
    }
    if (typecheck_budget + subfanin_budget <= 0) break;

    /*  (A) Pull IDs out of the subiterator
     *      and then traverse over their fan-in.
    */
    if (subfanin_budget > 0) {
      sub_budget = subfanin_budget;
      if (sub_budget > lto->lto_stat_budget_max)
        sub_budget = lto->lto_stat_budget_max;

      subfanin_budget -= sub_budget;
      *budget_inout -= sub_budget;

      err = linksto_statistics_subfanin(pdb, it, &sub_budget);

      /* Put back in what's left.
       */
      *budget_inout += sub_budget;
      subfanin_budget += sub_budget;

      /*      The iterator itself changed identity?  We must
       * 	have turned into a fixed array nor null iterator
       *      or something.  Roll with that.
       */
      if (it->it_id != me) {
        pdb_rxs_pop(pdb, "STAT %p linksto changed id", (void *)it);
        if (err != 0) return err;
        return pdb_iterator_statistics(pdb, it, budget_inout);
      }

      /*  The iterator completed?
       *  Great, let's stick with this method.
       */
      if (err == 0) {
        lto->lto_next_method = LTO_NEXT_SUBFANIN;
        goto have_method;
      }

      /*      There was some other terrible error?
       */
      if (err != PDB_ERR_MORE) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "linksto_statistics_subfanin", err,
                     "it=%s", pdb_iterator_to_string(pdb, it, buf, sizeof buf));
        pdb_rxs_pop(pdb, "STAT %p linksto error: %s", (void *)it,
                    graphd_strerror(err));
        return err;
      }
    }
    if (typecheck_budget > 0) {
      /*  (A) Pull IDs out of the type.  See where they point,
       *  	and check that endpoint against the subiterator.
       */

      sub_budget = typecheck_budget;
      if (sub_budget > lto->lto_stat_budget_max)
        sub_budget = lto->lto_stat_budget_max;

      *budget_inout -= sub_budget;
      typecheck_budget -= sub_budget;

      err = linksto_statistics_typecheck(pdb, it, &sub_budget);

      *budget_inout += sub_budget;
      typecheck_budget += sub_budget;

      /*  The iterator changed identity?
       *  Great, we must be tractable.
       */
      if (it->it_id != me) {
        pdb_rxs_pop(pdb, "STAT %p linksto redirect ($%lld)", (void *)it,
                    (long long)(budget_in - *budget_inout));
        return err;
      }

      /*  	We're done?
       */
      if (err == 0) {
        lto->lto_next_method = LTO_NEXT_TYPECHECK;
        goto have_method;
      } else if (err != PDB_ERR_MORE) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "linksto_statistics_typecheck", err,
                     "it=%s", pdb_iterator_to_string(pdb, it, buf, sizeof buf));
        goto unexpected_error;
      }
    }
  }

  if (lto->lto_stat_budget_max < budget_in) lto->lto_stat_budget_max *= 10;

  pdb_rxs_pop(pdb, "STAT %p linksto suspended; state=%d ($%lld)", (void *)it,
              it->it_call_state, (long long)(budget_in - *budget_inout));
  return PDB_ERR_MORE;

have_method:
  cl_assert(lto->lto_cl, lto->lto_next_method == LTO_NEXT_SUBFANIN ||
                             lto->lto_next_method == LTO_NEXT_TYPECHECK);

  /*  is-sorted:
   * 	as long as we have a tractable (small) number of destinations,
   *	a "links-to" iterator can transform into an "or"
   *  	iterator of the linkages of the single values.
   *      Those are sorted.  (A merge of sorted lists is itself sorted.)
   *
   *  estimate n:
   *	Pull a few values of out the destination; see what
   * 	the average fan-out is; add that.
   *
   *  estimate check chance:
   *	divide everything by the estimated n.
   *
   *  estimate check cost:
   *	read a primitive, figure out what it points to,
   *	and pass that to the destination for a check.
   *
   *  estimate production cost:
   *	destination's production cost, plus cost for
   *	following the fan-out.
   */

  /*
   *  estimate check cost:
   *	read a primitive, figure out what it points to,
   *	and pass that to the destination for a check.
   *
   *	XXX not factored in: the chance of the primitive
   *		not having a link out to begin with.
   */
  if (pdb_iterator_check_cost_valid(pdb, lto->lto_sub)) {
    pdb_iterator_check_cost_set(
        pdb, it,
        PDB_COST_PRIMITIVE + pdb_iterator_check_cost(pdb, lto->lto_sub));
  } else {
    pdb_budget est;

    cl_assert(cl, lto->lto_next_method == LTO_NEXT_TYPECHECK);

    /*  Estimate our check cost:
     *
     *  Our total cost for the typecheck stat run is
     *  lto->lto_stat_tc_cost.
     *
     *  Divide that by lto->lto_stat_tc_trial_n.
     *  That's the number of trials we ran.
     *
     *  Subtract from that (if there's space to do that)
     *  the next cost for the type-based iterator.
     *
     *  The reminder is the primitive cost plus the
     *  subiterator check cost, as experienced
     *  during the trial run.
     */

    est = lto->lto_stat_tc_cost;
    cl_assert(cl, lto->lto_stat_tc_trial_n > 0);

    est /= lto->lto_stat_tc_trial_n;
    if (est <= 0) est = 1;

    if (est > pdb_iterator_next_cost(pdb, lto->lto_stat_tc_hint))
      est -= pdb_iterator_next_cost(pdb, lto->lto_stat_tc_hint);
    else
      est = 1;

    if (est < PDB_COST_PRIMITIVE) est = PDB_COST_PRIMITIVE;

    pdb_iterator_check_cost_set(pdb, it, est);
  }

  if (lto->lto_next_method == LTO_NEXT_TYPECHECK) {
    cl_assert(cl, lto->lto_stat_tc_id_n > 0);
    pdb_iterator_next_cost_set(
        pdb, it, 1 + lto->lto_stat_tc_cost / lto->lto_stat_tc_id_n);
    pdb_iterator_find_cost_set(
        pdb, it, PDB_COST_GMAP_ARRAY + pdb_iterator_next_cost(pdb, it));
    pdb_iterator_sorted_set(pdb, it, true);

    /*  Estimated N: total number of the type iterator
     *  divided by the chance of the subiterator accepting
     *  the thing we offered it.
     */
    cl_assert(cl, lto->lto_stat_tc_trial_n > 0);
    cl_assert(cl, lto->lto_stat_tc_trial_n >= lto->lto_stat_tc_id_n);
    cl_assert(cl, pdb_iterator_n_valid(pdb, lto->lto_stat_tc_hint));

    /*  x : N(type) = accepted results : trial results
     */
    pdb_iterator_n_set(pdb, it, (pdb_iterator_n(pdb, lto->lto_stat_tc_hint) *
                                 lto->lto_stat_tc_id_n) /
                                    lto->lto_stat_tc_trial_n);
  } else {
    cl_assert(cl, lto->lto_next_method == LTO_NEXT_SUBFANIN);

    cl_log(cl, CL_LEVEL_DEBUG, "PDB STAT lto %s; id_n %zu, fanin_n %zu",
           pdb_iterator_to_string(lto->lto_pdb, it, buf, sizeof buf),
           lto->lto_statistics_id_n, lto->lto_statistics_fanin_n);

    /*  Calculate the average fan-out per destination element.
     *
     *  If we saw a zero fan-out -- none of our trial ids
     *  had anything linking to them -- assume a small fan out
     *  rather than none.
     *
     *  XXX As an alternative, we could keep pulling out new
     *  trial candidates (and counting them) until we either run
     *  out or find something that fits us.  But that could take
     *  a long time.
     */

    /*  Nobody ever had any fan-in?
     */
    if (lto->lto_statistics_fanin_n == 0) {
      /*  Estimate that the very next thing
       *  you pull out will have a single fan-in.
       *  (In reality, it might have more - this
       *  guess can be arbitrarily wrong.)
       */
      average_fan_out = 1.0 / lto->lto_statistics_sub_n;
    } else {
      cl_assert(cl, lto->lto_statistics_id_n > 0);
      average_fan_out =
          (double)lto->lto_statistics_fanin_n / lto->lto_statistics_sub_n;
    }

    /*  estimate n:
     *	Average fan-out * estimated number of fan sources.
     */

    /* The subiterator has its statistics - we made
     * it do them in the beginning.
     */
    sub_n = pdb_iterator_n(pdb, lto->lto_sub);
    if (sub_n < lto->lto_statistics_sub_n) sub_n = lto->lto_statistics_sub_n;
    est = average_fan_out * sub_n;

    cl_log(cl, CL_LEVEL_VERBOSE,
           "linksto_statistics: "
           "sub_n is %llu (sub_id_n %llu, sub_id_sub_n %llu), "
           "average fan out %g, estimate %llu",
           (unsigned long long)sub_n,
           (unsigned long long)lto->lto_statistics_id_n,
           (unsigned long long)lto->lto_statistics_sub_n, average_fan_out,
           (unsigned long long)est);

    if (est <= 0)
      est = 1;
    else if (est > upper_bound)
      est = upper_bound;
    cl_assert(cl, est >= 1);

    /*  If we have a hint, limit the estimate to the
     *  maximum number of hint instances.
     */
    if (!GRAPH_GUID_IS_NULL(lto->lto_hint_guid) && est > 10) {
      pdb_id hint_id;
      unsigned long long n_instances;

      err = pdb_id_from_guid(pdb, &hint_id, &lto->lto_hint_guid);
      if (err != 0) {
        cl_log_errno(
            cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "guid=%s",
            graph_guid_to_string(&lto->lto_hint_guid, buf, sizeof buf));
        goto unexpected_error;
      }

      err = pdb_linkage_count_est(lto->lto_pdb, lto->lto_hint_linkage, hint_id,
                                  it->it_low, it->it_high, PDB_COUNT_UNBOUNDED,
                                  &n_instances);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_linkage_count_est", err, "id=%llx",
                     (unsigned long long)hint_id);
        goto unexpected_error;
      }
      if (n_instances < est) est = n_instances;
    }
    pdb_iterator_n_set(pdb, it, est);

    /*  estimate production cost:
     *	linkage destination's production cost,
     * 	plus cost for following the fan-out.
     */
    sub_budget = pdb_iterator_next_cost(pdb, lto->lto_sub);
    if (average_fan_out == 0)
      /*  Guess that we're going to pull 2 *
       *  GRAPHD_LINKSTO_N_SAMPLES) until we finally
       *  find one that has the fan-out we're looking
       *  for.
       */
      est = (PDB_COST_GMAP_ELEMENT + (sub_budget + PDB_COST_GMAP_ARRAY)) *
            (2 * GRAPHD_LINKSTO_N_SAMPLES);
    else
      est = PDB_COST_GMAP_ELEMENT +
            ((sub_budget + PDB_COST_GMAP_ARRAY) / average_fan_out);
    pdb_iterator_next_cost_set(pdb, it, est);

    /* traversal cost: n/a (not sorted)
     */
    pdb_iterator_find_cost_set(pdb, it, 0);
    pdb_iterator_sorted_set(pdb, it, false);

    /* Ordering: same as the subfanin.
     *
     * Clearing the pdb_iterator_ordering() at this
     * point is important - it'll be frozen
     * indiscriminately, and will imply orderedness
     * when thawed for a linksto iterator that has
     * statistics.
     */
    if (pdb_iterator_ordered(pdb, lto->lto_sub)) {
      pdb_iterator_ordered_set(pdb, it, true);
      pdb_iterator_ordering_set(pdb, it,
                                pdb_iterator_ordering(pdb, lto->lto_sub));
    } else {
      pdb_iterator_ordered_set(pdb, it, false);
      pdb_iterator_ordering_set(pdb, it, NULL);
    }
  }

  /*  Free subiterators used only during statistics.
   */
  pdb_iterator_destroy(pdb, &lto->lto_stat_tc_hint);
  pdb_iterator_destroy(pdb, &lto->lto_stat_tc_sub);

  pdb_iterator_statistics_done_set(pdb, it);
  cl_log(cl, CL_LEVEL_DEBUG,
         "PDB STAT %p linksto %s: n=%llu cc=%lld, "
         "nc=%lld; fc=%lld%s%s%s",
         (void *)it, pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         (unsigned long long)pdb_iterator_n(pdb, it),
         (long long)pdb_iterator_check_cost(pdb, it),
         (long long)pdb_iterator_next_cost(pdb, it),
         (long long)pdb_iterator_find_cost(pdb, it),
         pdb_iterator_sorted(pdb, it) ? ", sorted" : "",
         pdb_iterator_ordered(pdb, it) ? ", o=" : "",
         pdb_iterator_ordered(pdb, it) ? pdb_iterator_ordering(pdb, it) : "");

  pdb_rxs_pop(
      pdb,
      "STAT %p linksto %s: n=%llu cc=%lld, "
      "nc=%lld; fc=%lld%s%s%s",
      (void *)it, pdb_iterator_to_string(pdb, it, buf, sizeof buf),
      (unsigned long long)pdb_iterator_n(pdb, it),
      (long long)pdb_iterator_check_cost(pdb, it),
      (long long)pdb_iterator_next_cost(pdb, it),
      (long long)pdb_iterator_find_cost(pdb, it),
      pdb_iterator_sorted(pdb, it) ? ", sorted" : "",
      pdb_iterator_ordered(pdb, it) ? ", o=" : "",
      pdb_iterator_ordered(pdb, it) ? pdb_iterator_ordering(pdb, it) : "");
  return 0;

unexpected_error:
  pdb_rxs_pop(pdb, "STAT %p linksto unexpected error %s ($%lld)", (void *)it,
              graphd_strerror(err), (long long)(budget_in - *budget_inout));
  return err;
}

/**
 * @brief access the next primitive in an iteration
 *
 * @param pdb		opaque module handle, created with pdb_create()
 * @param it		iteration to set through
 * @param id_out	assign the pdb_id to this
 * @param budget_inout	subtract cost from this
 *
 * @return 0 on success, a nonzero error code on error
 * @return GRAPHD_ERR_NO after running out of primitives.
 */
static int linksto_next_typecheck(pdb_handle *pdb, pdb_iterator *it,
                                  pdb_id *id_out, pdb_budget *budget_inout) {
  graphd_iterator_linksto *lto = it->it_theory;
  cl_handle *cl = lto->lto_cl;
  int err;
  char buf[200];
  pdb_budget budget_in = *budget_inout;

  PDB_IS_ITERATOR(cl, it);
  if (GRAPHD_SABOTAGE(lto->lto_graphd, *budget_inout < -100))
    return PDB_ERR_MORE;

  cl_enter(cl, CL_LEVEL_VERBOSE, "%s; budget=%lld, state=%d (%p)",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf),
           (long long)*budget_inout, it->it_call_state, (void *)it);

  /*  If we don't yet have a type iterator, make one.
   */
  if (lto->lto_hint_it == NULL) {
    err = linksto_hint_it(pdb, it, &lto->lto_hint_it);
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
      return err;
    }
  }

  switch (it->it_call_state) {
    case 0:
      do {
        pdb_primitive pr;
        graph_guid endpoint_guid;

        /*  Pull another ID out of the type iterator.
         */
        if (lto->lto_resume_id != PDB_ID_NONE) {
          pdb_id id_found;

          case LTO_TYPECHECK_FIND_MORE:
            it->it_call_state = 0;
            cl_assert(cl, lto->lto_resume_id != PDB_ID_NONE);

            err = pdb_iterator_find(pdb, lto->lto_hint_it, lto->lto_resume_id,
                                    &id_found, budget_inout);

            if (err == PDB_ERR_MORE) {
              cl_assert(cl, lto->lto_resume_id != PDB_ID_NONE);
              it->it_call_state = LTO_TYPECHECK_FIND_MORE;
              cl_leave(cl, CL_LEVEL_VERBOSE, "suspended in find ($%lld)",
                       (long long)(budget_in - *budget_inout));
              return PDB_ERR_MORE;
            }

            if (err != 0) {
              lto->lto_resume_id = PDB_ID_NONE;

              cl_assert(cl, err != PDB_ERR_MORE);
              cl_leave(cl, CL_LEVEL_VERBOSE, "find: %s ($%lld)",
                       graphd_strerror(err),
                       (long long)(budget_in - *budget_inout));
              return err;
            }

            if (id_found != lto->lto_resume_id) {
              cl_log(cl, CL_LEVEL_ERROR,
                     "linksto_next_typecheck: "
                     "odd: we're repositioning on %llx, "
                     "but find %s fails to find it (goes "
                     "to %llx instead), even though it "
                     "was pulled out of this iterator "
                     "before? [ignored]",
                     (unsigned long long)lto->lto_resume_id,
                     pdb_iterator_to_string(pdb, lto->lto_hint_it, buf,
                                            sizeof buf),
                     (unsigned long long)id_found);

              lto->lto_id = lto->lto_resume_id;
              lto->lto_resume_id = PDB_ID_NONE;

              goto have_next;
            }

            lto->lto_id = lto->lto_resume_id;
            lto->lto_resume_id = PDB_ID_NONE;
        }
        /* Fall through */

        case LTO_TYPECHECK_NEXT_MORE:
          it->it_call_state = 0;
          err = pdb_iterator_next(pdb, lto->lto_hint_it, &lto->lto_id,
                                  budget_inout);
          if (err != 0) {
            if (err == PDB_ERR_MORE) {
              it->it_call_state = LTO_TYPECHECK_NEXT_MORE;
              cl_leave(cl, CL_LEVEL_VERBOSE, "suspended in next ($%lld)",
                       (long long)(budget_in - *budget_inout));
              return PDB_ERR_MORE;
            }
            cl_assert(cl, err != PDB_ERR_MORE);
            if (err == GRAPHD_ERR_NO) {
              cl_leave(cl, CL_LEVEL_VERBOSE, "done ($%lld)",
                       (long long)(budget_in - *budget_inout));
              return err;
            }

            cl_log_errno(
                cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "it=%s",
                pdb_iterator_to_string(pdb, lto->lto_hint_it, buf, sizeof buf));
            cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
            return err;
          }

        have_next:
          /*  Read the primitive associated with that ID.
           */
          *budget_inout -= PDB_COST_PRIMITIVE;
          err = pdb_id_read(pdb, lto->lto_id, &pr);
          if (err != 0) {
            cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err, "id=%s",
                         pdb_id_to_string(pdb, lto->lto_id, buf, sizeof buf));

            if (err == GRAPHD_ERR_NO) goto next;
            cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
            return err;
          }

          /*  Follow the linkage relationship from the ID.
           */
          if (!pdb_primitive_has_linkage(&pr, lto->lto_linkage)) {
            pdb_primitive_finish(pdb, &pr);
            goto next;
          }
          pdb_primitive_linkage_get(&pr, lto->lto_linkage, endpoint_guid);
          pdb_primitive_finish(pdb, &pr);

          /* Translate that GUID back into an ID .
           */
          err = pdb_id_from_guid(pdb, &lto->lto_sub_id, &endpoint_guid);
          if (err != 0) {
            cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "guid=%s",
                         graph_guid_to_string(&endpoint_guid, buf, sizeof buf));
            if (err == GRAPHD_ERR_NO) goto next;

            cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
            return err;
          }

          /*  Check that ID against the subiterator.
           */
          cl_assert(cl, lto->lto_sub != NULL);

        /* We may have to resume this - there's no
         * telling how long these subiterator checks take!
         */
        case LTO_TYPECHECK_CHECK_MORE:
          it->it_call_state = 0;
          err = pdb_iterator_check(pdb, lto->lto_sub, lto->lto_sub_id,
                                   budget_inout);
          if (err != 0) {
            if (err == GRAPHD_ERR_NO) goto next;

            if (err == PDB_ERR_MORE) {
              it->it_call_state = LTO_TYPECHECK_CHECK_MORE;
              cl_leave(cl, CL_LEVEL_VERBOSE,
                       "suspended in sub check "
                       "($%lld)",
                       budget_in - *budget_inout);
              return PDB_ERR_MORE;
            }
            cl_log_errno(
                cl, CL_LEVEL_FAIL, "pdb_iterator_check", err,
                "checking %lld against %s", (long long)lto->lto_sub_id,
                pdb_iterator_to_string(pdb, lto->lto_sub, buf, sizeof buf));
            cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
            return err;
          }

          /*  The subiterator accepted the ID.  It is
           *  part of our result set.  Yay.
           */
          *id_out = lto->lto_id;
          cl_leave(cl, CL_LEVEL_VERBOSE, "%llx ($%lld)",
                   (unsigned long long)lto->lto_id,
                   (long long)budget_in - *budget_inout);
          return 0;

        next:;
      } while (GRAPHD_SABOTAGE(lto->lto_graphd, *budget_inout > 0));
      break;

    default:
      cl_notreached(cl, "state %i is invalid", it->it_call_state);
  }

  cl_leave(cl, CL_LEVEL_VERBOSE,
           "suspended iterating "
           "over type instances ($%lld)",
           budget_in - *budget_inout);
  return PDB_ERR_MORE;
}

/**
 * @brief access the next primitive in an iteration
 *
 * @param pdb		opaque module handle, created with pdb_create()
 * @param it		iteration to set through
 * @param pdb_id_out	assign the pdb_id to this
 *
 * @return 0 on success, a nonzero error code on error
 * @return GRAPHD_ERR_NO after running out of primitives.
 */
static int linksto_next_loc(pdb_handle *pdb, pdb_iterator *it, pdb_id *id_out,
                            pdb_budget *budget_inout, char const *file,
                            int line) {
  graphd_iterator_linksto *lto = it->it_theory, *olto;
  cl_handle *cl = lto->lto_cl;
  int err;
  pdb_id id;
  char buf[200];
  pdb_budget budget_in = *budget_inout;
  unsigned long long me = it->it_id;

  /*  Make sure we have statistics before continuing
   *  with the "next".  Statistics tells us how the
   *  next is going to work.
   */
  pdb_rxs_push(pdb, "NEXT %p linksto (state=%d) [%s:%d]", (void *)it,
               it->it_call_state, file, line);

  if (!pdb_iterator_statistics_done(pdb, it)) {
    err = pdb_iterator_statistics(pdb, it, budget_inout);
    if (err != 0) goto done;
  }
  err = pdb_iterator_refresh(pdb, it);
  if (err != PDB_ERR_ALREADY || it->it_id != me) {
    pdb_rxs_pop(pdb, "NEXT %p linksto: redirect ($%lld)", (void *)it,
                (long long)(budget_in - *budget_inout));
    pdb_iterator_account_charge_budget(pdb, it, next);

    return pdb_iterator_next_loc(pdb, it, id_out, budget_inout, file, line);
  }

  olto = it->it_original->it_theory;
  if (olto->lto_next_method == LTO_NEXT_TYPECHECK) {
    err = linksto_next_typecheck(pdb, it, id_out, budget_inout);
    goto done;
  }

  switch (it->it_call_state)
    for (;;) {
      default:
        RESUME_STATE(it, 0)
        if (lto->lto_statistics_sub != NULL &&
            pdb_iterator_statistics_done(pdb, it) &&
            (lto->lto_statistics_sub->it_id != lto->lto_sub->it_id)) {
          /*  While optimizing, our sub-iterator
           *  changed.  Resume where we were
           *  in the new iterator.
           */
          pdb_iterator_destroy(pdb, &lto->lto_sub);
          lto->lto_sub = lto->lto_statistics_sub;
          lto->lto_statistics_sub = NULL;
          RESUME_STATE(it, 1)
          RESUME_STATE(it, 2)
          err = linksto_next_resume(it, budget_inout);
          if (err != 0) goto done;
        }

        if (lto->lto_fanin != NULL) {
          pdb_iterator_call_reset(pdb, lto->lto_fanin);
          RESUME_STATE(it, 3)
          err = pdb_iterator_next(pdb, lto->lto_fanin, id_out, budget_inout);

          if (err == PDB_ERR_MORE) {
            it->it_call_state = 3;
            goto suspend;
          }

          if (err != GRAPHD_ERR_NO) {
            if (err != 0)
              cl_log_errno(
                  cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "fan-in: %s",
                  pdb_iterator_to_string(pdb, lto->lto_fanin, buf, sizeof buf));
            break;
          }
          cl_log(cl, CL_LEVEL_VERBOSE,
                 "pdb_iterator_next: done exploiting "
                 "fan-in from %s",
                 pdb_iterator_to_string(pdb, lto->lto_fanin, buf, sizeof buf));
          pdb_iterator_destroy(pdb, &lto->lto_fanin);
        }

        /*  Pull the next element from our source
         *  iterator lto_sub.
         */
        pdb_iterator_call_reset(pdb, lto->lto_sub);
        RESUME_STATE(it, 4)
        err = pdb_iterator_next(pdb, lto->lto_sub, &id, budget_inout);
        if (err != 0) {
          if (err == PDB_ERR_MORE) {
            it->it_call_state = 4;
            goto suspend;
          }
          if (err != GRAPHD_ERR_NO)
            cl_log_errno(
                cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "sub-iterator: %s",
                pdb_iterator_to_string(pdb, lto->lto_sub, buf, sizeof buf));
          goto done;
        }

        /*  Load the set of primitives that point to that one
         *  particular id into the fan-in iterator.
         */
        err = linksto_fanin(lto->lto_graphd, lto->lto_linkage,
                            lto->lto_hint_linkage, &lto->lto_hint_guid, id,
                            it->it_low, it->it_high, it->it_forward,
                            &lto->lto_fanin);

        /*  Charge a map lookup.
         */
        *budget_inout -= PDB_COST_GMAP_ARRAY;

        if (err != 0) {
          if (err == GRAPHD_ERR_NO) {
            cl_log(cl, CL_LEVEL_VERBOSE,
                   "pdb_iterator_next: no "
                   "fan-in from source %llx",
                   (unsigned long long)id);
            goto check_budget_and_continue;
          }
          cl_log_errno(cl, CL_LEVEL_FAIL, "linksto_fanin", err,
                       "fan-in from source %llx", (unsigned long long)id);
          break;
        }

        /*  The fan-in iterator inherits our account.
         */
        pdb_iterator_account_set(pdb, lto->lto_fanin,
                                 pdb_iterator_account(pdb, it));

        cl_log(lto->lto_cl, CL_LEVEL_VERBOSE, "linksto_next made fanin=%p",
               (void *)lto->lto_fanin);

      check_budget_and_continue:
        if (GRAPHD_SABOTAGE(lto->lto_graphd, *budget_inout < 0)) {
          it->it_call_state = 0;
          goto suspend;
        }
    }

done:
  switch (err) {
    case 0:
      pdb_rxs_pop(pdb, "NEXT %p linksto %llx ($%lld)", (void *)it,
                  (unsigned long long)*id_out,
                  (long long)(budget_in - *budget_inout));
      err = 0;
      break;

    case GRAPHD_ERR_NO:
      pdb_rxs_pop(pdb, "NEXT %p linksto EOF ($%lld)", (void *)it,
                  (long long)(budget_in - *budget_inout));
      break;

    suspend:
      err = GRAPHD_ERR_MORE;
    case PDB_ERR_MORE:
      pdb_rxs_pop(pdb, "NEXT %p linksto suspended; state=%d ($%lld)",
                  (void *)it, it->it_call_state,
                  (long long)(budget_in - *budget_inout));
      break;

    default:
      pdb_rxs_pop(pdb, "NEXT %p unexpected error: %s ($%lld)", (void *)it,
                  graphd_strerror(err), (long long)(budget_in - *budget_inout));
      break;
  }
  pdb_iterator_account_charge_budget(pdb, it, next);
  return err;
}

static int linksto_reset(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_linksto *lto = it->it_theory;
  int err;

  lto->lto_resume_id = PDB_ID_NONE;
  lto->lto_id = PDB_ID_NONE;
  lto->lto_sub_id = PDB_ID_NONE;
  lto->lto_source = PDB_ID_NONE;

  pdb_rxs_log(pdb, "RESET %p linksto", (void *)it);

  err = pdb_iterator_reset(pdb, lto->lto_sub);
  if (err != 0) {
    char buf[200];
    cl_log_errno(lto->lto_cl, CL_LEVEL_FAIL, "pdb_iterator_reset", err,
                 "sub=%s",
                 pdb_iterator_to_string(pdb, lto->lto_sub, buf, sizeof buf));
    return err;
  }

  if (lto->lto_hint_it != NULL) {
    err = pdb_iterator_reset(pdb, lto->lto_hint_it);
    if (err != 0) {
      char buf[200];
      cl_log_errno(lto->lto_cl, CL_LEVEL_FAIL, "pdb_iterator_reset", err,
                   "hint=%s",
                   pdb_iterator_to_string(pdb, lto->lto_sub, buf, sizeof buf));
      return err;
    }
  }

  pdb_iterator_destroy(pdb, &lto->lto_fanin);
  pdb_iterator_call_reset(pdb, it);

  return 0;
}

/*  linksto:[~]LOW[-HIGH]:LINKAGE[+TYPE]->(SUBIT)
 *		[md:METHOD][o:ORDERING][a:ACCOUNT][h:LINKAGE=GUID]
 */
static int linksto_freeze_set(pdb_handle *pdb, unsigned long long low,
                              unsigned long long high,
                              graphd_direction direction, char const *ordering,
                              pdb_iterator_account const *acc, int method,
                              int linkage, int hint_linkage,
                              graph_guid const *hint_guid, pdb_iterator *sub_it,
                              cm_buffer *buf) {
  char sbuf[200];
  int err;

  /*    	linksto:[~]LOW[-HIGH]:LINKAGE[+TYPE]->(SUBSET)[md:...][o:...]
   */
  err = cm_buffer_sprintf(buf, "linksto:%c%llu",
                          graphd_iterator_direction_to_char(direction), low);
  if (err != 0) return err;

  if (high != PDB_ITERATOR_HIGH_ANY) {
    err = cm_buffer_sprintf(buf, "-%llu", high);
    if (err != 0) return err;
  }

  err = cm_buffer_sprintf(buf, ":%.1s", pdb_linkage_to_string(linkage));
  if (err != 0) return err;

  /*  We can encode the hint in the mainstream iterator only if
   *  its linkage is TYPEGUID; the expanded hint goes in the
   *  optional section.
   */
  if (hint_guid != NULL && !GRAPH_GUID_IS_NULL(*hint_guid) &&
      hint_linkage == PDB_LINKAGE_TYPEGUID) {
    err = cm_buffer_sprintf(buf, "+%s",
                            graph_guid_to_string(hint_guid, sbuf, sizeof sbuf));
    if (err != 0) return err;
  }

  err = cm_buffer_add_string(buf, "->(");
  if (err != 0) return err;

  err = pdb_iterator_freeze(pdb, sub_it, PDB_ITERATOR_FREEZE_SET, buf);
  if (err != 0) return err;

  err = cm_buffer_add_string(buf, ")");
  if (err != 0) return err;

  if (method != LTO_NEXT_UNSPECIFIED) {
    err = cm_buffer_sprintf(buf, "[md:%d]", method);
    if (err != 0) return err;
  }

  if (ordering != NULL) {
    err = cm_buffer_sprintf(buf, "[o:%s]", ordering);
    if (err != 0) return err;
  }

  if (acc != NULL) {
    err = cm_buffer_sprintf(buf, "[a:%zu]", acc->ia_id);
    if (err != 0) return err;
  }

  if (hint_guid != NULL && !GRAPH_GUID_IS_NULL(*hint_guid) &&
      hint_linkage != PDB_LINKAGE_TYPEGUID && hint_linkage != PDB_LINKAGE_N) {
    err = cm_buffer_sprintf(buf, "[h:%.1s=%s]",
                            pdb_linkage_to_string(hint_linkage),
                            graph_guid_to_string(hint_guid, sbuf, sizeof sbuf));
    if (err != 0) return err;
  }
  return 0;
}

static int linksto_thaw_statistics_state(pdb_iterator *it, char const **s_ptr,
                                         char const *e, pdb_iterator_base *pib,
                                         cl_loglevel loglevel) {
  graphd_iterator_linksto *lto = it->it_theory;
  graphd_handle *g = lto->lto_graphd;
  pdb_handle *pdb = lto->lto_pdb;
  cl_handle *cl = lto->lto_cl;
  int err;
  size_t i;
  char const *s = *s_ptr;

  if (e - s < sizeof("[stat.sf:") ||
      strncasecmp(s, "[stat.sf:", sizeof("[stat.sf:") - 1) != 0)
    return GRAPHD_ERR_LEXICAL;

  s += sizeof("[stat.sf:") - 1;

  err = graphd_iterator_util_thaw_subiterator(g, &s, e, pib, loglevel,
                                              &lto->lto_statistics_sub);
  if (err != 0) {
    return err;
  }
  err = pdb_iterator_util_thaw(
      pdb, &s, e, ":%d:%zu:%zu:%zu", &lto->lto_statistics_state,
      &lto->lto_statistics_id_n, &lto->lto_statistics_fanin_n,
      &lto->lto_statistics_sub_n);
  if (err != 0) {
    return err;
  }

  if (lto->lto_statistics_id_n > GRAPHD_LINKSTO_N_SAMPLES) {
    cl_log(cl, loglevel,
           "Linksto_thaw_statistics_state: "
           "id_n is %zi which must be less than "
           " or equal to %i",
           lto->lto_statistics_id_n, GRAPHD_LINKSTO_N_SAMPLES);

    return GRAPHD_ERR_LEXICAL;
  }
  for (i = 0; i < lto->lto_statistics_id_n; i++) {
    if (s < e && (*s == ':' || *s == ',')) s++;
    err =
        pdb_iterator_util_thaw(pdb, &s, e, "%{id}", lto->lto_statistics_id + i);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_util_thaw", err,
                   "expected %zu statistics ids, "
                   "got %zu",
                   lto->lto_statistics_id_n, i);
      return err;
    }
  }
  if (s < e && (*s == ':' || *s == ',')) s++;
  if (s >= e || *s != ']') {
    cl_log(cl, CL_LEVEL_FAIL,
           "linksto_thaw_statistics_state: "
           " expected [stat.sf:...], missing ] in"
           " \"%.*s\" at #%d",
           (int)(e - *s_ptr), *s_ptr, (int)(s - *s_ptr));
    return GRAPHD_ERR_LEXICAL;
  }
  s++;

  if (e - s < sizeof("[stat.tc:") ||
      strncasecmp(s, "[stat.tc:", sizeof("[stat.tc:") - 1) != 0)
    return GRAPHD_ERR_LEXICAL;

  s += sizeof("[stat.tc:") - 1;

  err = graphd_iterator_util_thaw_subiterator(g, &s, e, pib, loglevel,
                                              &lto->lto_stat_tc_sub);
  if (err != 0) return err;

  err = graphd_iterator_util_thaw_subiterator(g, &s, e, pib, loglevel,
                                              &lto->lto_stat_tc_hint);
  if (err != 0) return err;

  err = pdb_iterator_util_thaw(
      pdb, &s, e, ":%d:%zu:%zu:%{budget}:%{id}", &lto->lto_stat_tc_state,
      &lto->lto_stat_tc_id_n, &lto->lto_stat_tc_trial_n, &lto->lto_stat_tc_cost,
      &lto->lto_stat_tc_endpoint_id);
  if (err != 0) return err;

  for (i = 0; i <= lto->lto_stat_tc_id_n && i < GRAPHD_LINKSTO_N_SAMPLES; i++) {
    if (s < e && (*s == ':' || *s == ',')) s++;
    err = pdb_iterator_util_thaw(pdb, &s, e, "%{id}", lto->lto_stat_tc_id + i);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_util_thaw", err,
                   "expected %zu sample(s), "
                   "got %zu",
                   lto->lto_stat_tc_id_n, i);
      return err;
    }
  }
  if (s < e && (*s == ':' || *s == ',')) s++;

  err = pdb_iterator_util_thaw(pdb, &s, e, "%{extensions}",
                               (pdb_iterator_property *)NULL);
  if (err != 0) return err;

  if (s >= e || *s != ']') {
    cl_log(cl, CL_LEVEL_FAIL,
           "linksto_thaw_statistics_state: "
           " expected [stat.sf:..][stat.tc:..], missing ] in"
           "\"%.*s\"",
           (int)(e - *s_ptr), *s_ptr);
    return GRAPHD_ERR_LEXICAL;
  }
  s++;

  *s_ptr = s;
  return 0;
}

static int linksto_freeze_statistics_state(pdb_handle *pdb, pdb_iterator *it,
                                           cm_buffer *buf) {
  graphd_iterator_linksto *lto = it->it_theory;
  char const *sep = "";
  int err;
  size_t i;
  char b2[200];
  cl_handle *cl = lto->lto_cl;

  err = cm_buffer_add_string(buf, "[stat.sf:");
  if (err != 0) return err;

  err = graphd_iterator_util_freeze_subiterator(
      pdb, lto->lto_statistics_sub, PDB_ITERATOR_FREEZE_EVERYTHING, buf);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_util_freeze_subiterator",
                 err, "it=%s", pdb_iterator_to_string(pdb, it, b2, sizeof b2));
    return err;
  }

  err = cm_buffer_sprintf(buf, ":%d:%zu:%zu:%zu:", lto->lto_statistics_state,
                          lto->lto_statistics_id_n, lto->lto_statistics_fanin_n,
                          lto->lto_statistics_sub_n);
  if (err != 0) return err;

  for (i = 0; i < lto->lto_statistics_id_n; i++) {
    char b2[200];
    err = cm_buffer_sprintf(
        buf, "%s%s", sep,
        pdb_id_to_string(pdb, lto->lto_statistics_id[i], b2, sizeof b2));
    if (err != 0) return err;
    sep = ",";
  }

  err = cm_buffer_add_string(buf, "][stat.tc:");
  if (err != 0) return err;

  err = graphd_iterator_util_freeze_subiterator(
      pdb, lto->lto_stat_tc_sub, PDB_ITERATOR_FREEZE_EVERYTHING, buf);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_util_freeze_subiterator",
                 err, "it=%s", pdb_iterator_to_string(pdb, it, b2, sizeof b2));
    return err;
  }

  err = graphd_iterator_util_freeze_subiterator(
      pdb, lto->lto_stat_tc_hint, PDB_ITERATOR_FREEZE_EVERYTHING, buf);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_util_freeze_subiterator",
                 err, "it=%s", pdb_iterator_to_string(pdb, it, b2, sizeof b2));
    return err;
  }

  err = cm_buffer_sprintf(
      buf, ":%d:%zu:%zu:%lld:%s:", lto->lto_stat_tc_state,
      lto->lto_stat_tc_id_n, lto->lto_stat_tc_trial_n,
      (long long)lto->lto_stat_tc_cost,
      pdb_id_to_string(pdb, lto->lto_stat_tc_endpoint_id, b2, sizeof b2));
  if (err != 0) return err;

  sep = "";
  for (i = 0; i <= lto->lto_stat_tc_id_n && i < GRAPHD_LINKSTO_N_SAMPLES; i++) {
    char b2[200];
    err = cm_buffer_sprintf(
        buf, "%s%s", sep,
        pdb_id_to_string(pdb, lto->lto_stat_tc_id[i], b2, sizeof b2));
    if (err != 0) return err;
    sep = ",";
  }

  err = cm_buffer_add_string(buf, "]");
  if (err != 0) return err;

  return 0;
}

static int linksto_freeze(pdb_handle *pdb, pdb_iterator *it, unsigned int flags,
                          cm_buffer *buf) {
  graphd_iterator_linksto *lto = it->it_theory;
  graphd_iterator_linksto *olto;
  cl_handle *cl = lto->lto_cl;
  char sbuf[200];
  char const *separator = "";
  int err = 0;
  size_t off = buf->buf_n;

  if (graphd_request_timer_check(lto->lto_greq)) return GRAPHD_ERR_TOO_HARD;

  if (it->it_id != it->it_original->it_id)
    return pdb_iterator_freeze(pdb, it->it_original, flags, buf);

  cl_enter(cl, CL_LEVEL_VERBOSE, "it=%p, flags=%u", (void *)it, flags);

  olto = it->it_original->it_theory;
  if (flags & PDB_ITERATOR_FREEZE_SET) {
    err = linksto_freeze_set(
        pdb, it->it_low, it->it_high, lto->lto_direction,
        pdb_iterator_ordering(pdb, it), pdb_iterator_account(pdb, it),
        lto->lto_next_method, lto->lto_linkage, lto->lto_hint_linkage,
        GRAPH_GUID_IS_NULL(lto->lto_hint_guid) ? NULL : &lto->lto_hint_guid,
        lto->lto_sub, buf);
    if (err != 0) goto buffer_error;
    separator = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    char s1[200], s2[200];
    /* 	/ NEXT-METHOD : LAST-ID : LAST-SUB-ID
     */
    err = cm_buffer_sprintf(
        buf, "%s%d:%s:%s", separator, olto->lto_next_method,
        pdb_id_to_string(pdb, lto->lto_id, s1, sizeof s1),
        pdb_id_to_string(pdb, lto->lto_sub_id, s2, sizeof s2));
    if (err != 0) goto buffer_error;
    separator = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_STATE) {
    /* 	/ CALL-STATE: SUBSPOSSTATE
     */
    err = cm_buffer_sprintf(buf, "%s%d:", separator, it->it_call_state);
    if (err != 0) goto buffer_error;

    err = graphd_iterator_util_freeze_subiterator(
        pdb, lto->lto_sub,
        PDB_ITERATOR_FREEZE_POSITION | PDB_ITERATOR_FREEZE_STATE, buf);
    if (err != 0) goto buffer_error;

    /* 	: FANIN :
     */
    err = cm_buffer_add_string(buf, ":");
    if (err != 0) goto buffer_error;

    err = graphd_iterator_util_freeze_subiterator(
        pdb, lto->lto_fanin, PDB_ITERATOR_FREEZE_EVERYTHING, buf);
    if (err != 0) goto buffer_error;

    if ((err = cm_buffer_add_string(buf, ":")) != 0) goto buffer_error;

    /*  	CC : NC [+FC] : N
     */
    if (pdb_iterator_statistics_done(pdb, it)) {
      err = cm_buffer_sprintf(buf, "%lld:%lld",
                              (long long)pdb_iterator_check_cost(pdb, it),
                              (long long)pdb_iterator_next_cost(pdb, it));
      if (err != 0) goto buffer_error;

      if (pdb_iterator_find_cost(pdb, it)) {
        err = cm_buffer_sprintf(buf, "+%lld",
                                (long long)pdb_iterator_find_cost(pdb, it));
        if (err != 0) goto buffer_error;
      }
      err = cm_buffer_sprintf(buf, ":%llu:",
                              (unsigned long long)pdb_iterator_n(pdb, it));
      if (err != 0) goto buffer_error;
    } else {
      /*  Ongoing statistics state.
       */
      err = linksto_freeze_statistics_state(pdb, it->it_original, buf);
      if (err != 0) goto buffer_error;
    }
  }

  cl_leave(cl, CL_LEVEL_VERBOSE, "%.*s", (int)(buf->buf_n - off),
           buf->buf_s + off);
  return err;

buffer_error:
  cl_log_errno(cl, CL_LEVEL_FAIL, "cm_buffer_sprintf/add_string", err, "it=%s",
               pdb_iterator_to_string(pdb, it, sbuf, sizeof sbuf));
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
  return err;
}

static int linksto_clone(pdb_handle *pdb, pdb_iterator *it,
                         pdb_iterator **it_out) {
  pdb_iterator *it_orig = it->it_original;
  graphd_iterator_linksto *lto = it->it_theory;
  graphd_iterator_linksto *lto_out;
  int err;

  {
    char ibuf[200];
    cl_log(lto->lto_cl, CL_LEVEL_VERBOSE, "linksto_clone %p:%s, fanin %p",
           (void *)it, pdb_iterator_to_string(pdb, it, ibuf, sizeof ibuf),
           (void *)lto->lto_fanin);
  }

  PDB_IS_ITERATOR(lto->lto_cl, it);
  PDB_IS_ORIGINAL_ITERATOR(lto->lto_cl, it_orig);

  /*  If the original iterator has evolved into something
   *  other than a "linksto" iterator, clone that iterator
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

  *it_out = NULL;

  lto_out = cm_malcpy(lto->lto_cm, lto, sizeof(*lto));
  if (lto_out == NULL) return errno ? errno : ENOMEM;

  err = pdb_iterator_clone(pdb, lto->lto_sub, &lto_out->lto_sub);
  if (err != 0) {
    cm_free(lto->lto_cm, lto_out);
    return err;
  }

  if (lto->lto_hint_it != NULL) {
    err = pdb_iterator_clone(pdb, lto->lto_hint_it, &lto_out->lto_hint_it);
    if (err != 0) {
      pdb_iterator_destroy(pdb, &lto_out->lto_sub);
      cm_free(lto->lto_cm, lto_out);
      return err;
    }
  }
  if (lto->lto_fanin != NULL) {
    err = pdb_iterator_clone(pdb, lto->lto_fanin, &lto_out->lto_fanin);
    if (err != 0) {
      pdb_iterator_destroy(pdb, &lto_out->lto_sub);
      pdb_iterator_destroy(pdb, &lto_out->lto_hint_it);

      cm_free(lto->lto_cm, lto_out);
      return err;
    }
  }

  lto_out->lto_statistics_state = 0;
  lto_out->lto_statistics_sub = NULL;

  lto_out->lto_stat_tc_hint = NULL;
  lto_out->lto_stat_tc_sub = NULL;
  lto_out->lto_source = PDB_ID_NONE;

  if ((err = pdb_iterator_make_clone(pdb, it_orig, it_out)) != 0) {
    pdb_iterator_destroy(pdb, &lto_out->lto_sub);
    cm_free(lto->lto_cm, lto_out);

    return err;
  }

  (*it_out)->it_theory = lto_out;
  (*it_out)->it_has_position = true;

  return 0;
}

static void linksto_finish(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_linksto *lto = it->it_theory;

  if (lto != NULL) {
    cl_cover(lto->lto_cl);

    pdb_iterator_destroy(pdb, &lto->lto_statistics_sub);
    pdb_iterator_destroy(pdb, &lto->lto_sub);
    pdb_iterator_destroy(pdb, &lto->lto_fanin);
    pdb_iterator_destroy(pdb, &lto->lto_hint_it);
    pdb_iterator_destroy(pdb, &lto->lto_stat_tc_hint);
    pdb_iterator_destroy(pdb, &lto->lto_stat_tc_sub);

    cm_free(lto->lto_cm, it->it_displayname);
    it->it_displayname = NULL;

    cm_free(lto->lto_cm, lto);
    it->it_theory = NULL;
  }
  it->it_type = NULL;
  it->it_magic = 0;
}

static char const *linksto_to_string(pdb_handle *pdb, pdb_iterator *it,
                                     char *buf, size_t size) {
  graphd_iterator_linksto *lto = it->it_theory;
  char sub[200], b2[200];

  if (GRAPH_GUID_IS_NULL(lto->lto_hint_guid))
    snprintf(buf, size, "%s%s%.1s->[%s]",
             pdb_iterator_forward(pdb, it) ? "" : "~",
             pdb_iterator_statistics_done(pdb, it) ? "" : "*",
             pdb_linkage_to_string(lto->lto_linkage),
             pdb_iterator_to_string(pdb, lto->lto_sub, sub, sizeof sub));
  else
    snprintf(buf, size, "%s%s%.1s(%.1s=%s)->[%s]",
             pdb_iterator_forward(pdb, it) ? "" : "~",
             pdb_iterator_statistics_done(pdb, it) ? "" : "*",
             pdb_linkage_to_string(lto->lto_linkage),
             pdb_linkage_to_string(lto->lto_hint_linkage),
             graph_guid_to_string(&lto->lto_hint_guid, b2, sizeof b2),
             pdb_iterator_to_string(pdb, lto->lto_sub, sub, sizeof sub));
  return buf;
}

/**
 * @brief Return the primitive summary for a LINKSTO iterator.
 *
 * @param pdb		module handle
 * @param it		a linksto iterator
 * @param psum_out	out: summary
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int linksto_primitive_summary(pdb_handle *pdb, pdb_iterator *it,
                                     pdb_primitive_summary *psum_out) {
  graphd_iterator_linksto *lto;

  /*  Defer to the original.  It may have a different type.
   */
  if (it->it_original != it)
    return pdb_iterator_primitive_summary(pdb, it->it_original, psum_out);

  lto = it->it_theory;
  if (GRAPH_GUID_IS_NULL(lto->lto_hint_guid)) return GRAPHD_ERR_NO;

  /* There are additional constraints that the
   * primitive summary cannot express.
   */
  psum_out->psum_complete = false;

  /* But all our results have this <linkage guid>:
   */
  psum_out->psum_locked = 1 << lto->lto_hint_linkage;
  psum_out->psum_guid[lto->lto_hint_linkage] = lto->lto_hint_guid;
  psum_out->psum_result = PDB_LINKAGE_N;

  return 0;
}

/**
 * @brief Has this iterator progressed beyond this value?
 *
 * @param graphd	module handle
 * @param it		iterator we're asking about
 * @param s		start of comparison value
 * @param e		end of comparison value
 * @param beyond_out	out: true if we're safely beyond this value.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int linksto_beyond(pdb_handle *pdb, pdb_iterator *it, char const *s,
                          char const *e, bool *beyond_out) {
  graphd_iterator_linksto *lto = it->it_theory;
  pdb_iterator *redirect_it;
  int err;
  char buf[200];

  /*  Something is out of sync?
   */
  if (!pdb_iterator_statistics_done(pdb, it) ||
      it->it_id != it->it_original->it_id || !pdb_iterator_ordered(pdb, it)) {
    cl_log(lto->lto_cl, CL_LEVEL_VERBOSE,
           "linksto_beyond: %s - returning false",
           !pdb_iterator_statistics_done(pdb, it)
               ? "no statistics yet"
               : (it->it_id != it->it_original->it_id
                      ? "original and instance ids don't match"
                      : "iterator isn't ordered"));
    *beyond_out = false;
    return 0;
  }

  if (pdb_iterator_sorted(pdb, it)) {
    if (lto->lto_hint_it == NULL) {
      cl_log(lto->lto_cl, CL_LEVEL_VERBOSE,
             "linksto_beyond: no type iterator yet");
      *beyond_out = false;
      return 0;
    }
    redirect_it = lto->lto_hint_it;
  } else {
    redirect_it = lto->lto_sub;
  }
  err = pdb_iterator_beyond(pdb, redirect_it, s, e, beyond_out);
  cl_log(lto->lto_cl, CL_LEVEL_VERBOSE, "linksto_beyond: %s: %s",
         pdb_iterator_to_string(pdb, redirect_it, buf, sizeof buf),
         err ? graphd_strerror(err)
             : (*beyond_out ? "we're done" : "no, we can still go below that"));
  return err;
}

static int linksto_range_estimate(pdb_handle *pdb, pdb_iterator *it,
                                  pdb_range_estimate *range) {
  pdb_range_estimate sub_range, fanin_range;
  graphd_iterator_linksto *lto = it->it_theory;
  int err;

  /*  If we're at the end of the cache, the lower bound
   *  of our subiterator values is a lower bound for
   *  ourselves.  (You can't point to something that was
   *  created after you.)
   */
  pdb_iterator_range_estimate_default(pdb, it, range);

  range->range_n_max = range->range_n_exact = PDB_COUNT_UNBOUNDED;

  switch (lto->lto_next_method) {
    case LTO_NEXT_TYPECHECK:
      if (lto->lto_hint_it == NULL) {
        err = linksto_hint_it(pdb, it, &lto->lto_hint_it);
        if (err != 0) return err;
      }
      err = pdb_iterator_range_estimate(pdb, lto->lto_hint_it, &sub_range);
      if (err != 0) {
        if (err != PDB_ERR_NO) return err;
      } else {
        if (range->range_low < sub_range.range_low)
          range->range_low = sub_range.range_low;

        if (range->range_high > sub_range.range_high)
          range->range_high = sub_range.range_high;

        range->range_n_max = sub_range.range_n_max;
      }
      break;

    case LTO_NEXT_SUBFANIN:
      err = pdb_iterator_range_estimate(pdb, lto->lto_sub, &sub_range);
      if (err != 0) {
        if (err != PDB_ERR_NO) return err;
      } else {
        if (lto->lto_fanin != NULL) {
          err = pdb_iterator_range_estimate(pdb, lto->lto_sub, &fanin_range);
          if (err == 0 && fanin_range.range_n_exact != 0) {
            if (fanin_range.range_low == 0) fanin_range.range_low = 1;

            if (fanin_range.range_low < sub_range.range_low + 1)
              sub_range.range_low = fanin_range.range_low - 1;
          } else if (err == 0 && fanin_range.range_n_exact == 0 &&
                     sub_range.range_n_exact == 0) {
            range->range_n_exact = 0;
            range->range_low = range->range_high;

            return 0;
          } else if (lto->lto_source != PDB_ID_NONE &&
                     sub_range.range_low > lto->lto_source)
            sub_range.range_low = lto->lto_source;

          if (sub_range.range_n_exact != PDB_COUNT_UNBOUNDED)
            sub_range.range_n_exact++;
        } else if (sub_range.range_n_exact == 0 ||
                   sub_range.range_low >= sub_range.range_high) {
          /*  No fanin, and the subiterator is done.
           */
          range->range_n_max = range->range_n_exact = 0;
          range->range_low = range->range_high;

          return 0;
        }

        if (sub_range.range_low >= range->range_low)
          range->range_low = sub_range.range_low + 1;
        range->range_low_rising |= sub_range.range_low_rising;
      }
      break;

    default:
      break;
  }

  cl_log(lto->lto_cl, CL_LEVEL_VERBOSE,
         "linksto_range_estimate %p: exact_n %llx, low %llx, high %llx",
         (void *)it, range->range_n_exact, range->range_low, range->range_high);
  return 0;
}

static int linksto_restrict(pdb_handle *pdb, pdb_iterator *it,
                            pdb_primitive_summary const *psum,
                            pdb_iterator **it_out) {
  graphd_iterator_linksto *lto = it->it_theory;
  int err;
  int linkage;

  /*  We can only do this for psums whose result
   *  is the primitive GUID.
   */
  if (psum->psum_result != PDB_LINKAGE_N) return PDB_ERR_ALREADY;

  /*  Does our hint conflict with the restriction?
   */
  if (lto->lto_hint_linkage != PDB_LINKAGE_N) {
    /*  Do we conflict with the restriction?
     */
    if (psum->psum_locked & (1 << lto->lto_hint_linkage))
      return GRAPH_GUID_EQ(psum->psum_guid[lto->lto_hint_linkage],
                           lto->lto_hint_guid)
                 ? PDB_ERR_ALREADY
                 : PDB_ERR_NO;

    return PDB_ERR_ALREADY;
  }

  /*  Adopt the restriction as a hint?
   */

  /*  We're returning right or left, the hint is a type?
   */
  if ((psum->psum_locked & (1 << PDB_LINKAGE_TYPEGUID)) &&
      (lto->lto_linkage == PDB_LINKAGE_RIGHT ||
       lto->lto_linkage == PDB_LINKAGE_LEFT)) {
    pdb_iterator *sub_clone;
    graphd_iterator_linksto *olto = it->it_original->it_theory;

    /*  Make a clone of our subiterator.
     */
    err = pdb_iterator_clone(pdb, olto->lto_sub, &sub_clone);
    if (err != 0) return err;

    err = graphd_iterator_linksto_create(
        lto->lto_greq, lto->lto_linkage, PDB_LINKAGE_TYPEGUID,
        psum->psum_guid + PDB_LINKAGE_TYPEGUID, &sub_clone, it->it_low,
        it->it_high, lto->lto_direction, pdb_iterator_ordering(pdb, it),
        it_out);
    pdb_iterator_destroy(pdb, &sub_clone);
    return err;
  }

  /*  We're returning a type, the hint is right or left?
   */
  if (lto->lto_linkage == PDB_LINKAGE_TYPEGUID &&
      ((psum->psum_locked &
        ((1 << PDB_LINKAGE_RIGHT) | (1 << PDB_LINKAGE_LEFT))) ==
           (1 << (linkage = PDB_LINKAGE_RIGHT)) ||
       (psum->psum_locked &
        ((1 << PDB_LINKAGE_RIGHT) | (1 << PDB_LINKAGE_LEFT))) ==
           (1 << (linkage = PDB_LINKAGE_LEFT)))) {
    pdb_iterator *sub_clone;
    graphd_iterator_linksto *olto = it->it_original->it_theory;

    /*  Make a clone of our subiterator.
     */
    err = pdb_iterator_clone(pdb, olto->lto_sub, &sub_clone);
    if (err != 0) return err;

    err = graphd_iterator_linksto_create(
        lto->lto_greq, lto->lto_linkage, linkage, psum->psum_guid + linkage,
        &sub_clone, it->it_low, it->it_high, lto->lto_direction,
        pdb_iterator_ordering(pdb, it), it_out);
    pdb_iterator_destroy(pdb, &sub_clone);
    return err;
  }
  return PDB_ERR_ALREADY;
}

static const pdb_iterator_type graphd_iterator_linksto_type = {
    "linksto",
    linksto_finish,
    linksto_reset,
    linksto_clone,
    linksto_freeze,
    linksto_to_string,

    linksto_next_loc,
    linksto_find_loc,
    linksto_check,
    linksto_statistics,

    NULL, /* idarray */
    linksto_primitive_summary,
    linksto_beyond,
    linksto_range_estimate,
    linksto_restrict,

    NULL /* suspend */,
    NULL /* unsuspend */
};

/**
 * @brief Create an "linksto" iterator structure.
 *
 *  The new iterator L is derived from another iterator S.
 *  The primitives in L point to the primitives in S with their
 *  linkage pointer.
 *
 *  The subconstraint sub is implicitly moved into the new
 *  iterator and must not be referenced by clones.
 *
 * @param g		server for whom we're doing this
 * @param linkage	how do I connect to the records from sub?
 * @param hint_linkage	PDB_LINKAGE_N or the <linkage> of hint_guid
 * @param hint_guid	NULL or <linkage> GUID of our links
 * @param sub		iterator for the values I connect to.
 * @param low		lowest included value
 * @param high		highest value that isn't included
 * @param direction	sort order
 * @param ordering	NULL or the interned orderin
 * @param next_method	how do we do our nexts, LTO_NEXT_something
 * @param it_out	out: the new iterator
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int linksto_create(graphd_request *greq, int linkage, int hint_linkage,
                          graph_guid const *hint_guid, pdb_iterator **sub,
                          unsigned long long low, unsigned long long high,
                          graphd_direction direction, char const *ordering,
                          int next_method, pdb_iterator **it_out,
                          char const *file, int line) {
  graphd_handle *graphd = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = pdb_mem(graphd->g_pdb);
  bool forward = direction != GRAPHD_DIRECTION_BACKWARD;
  graphd_iterator_linksto *lto;

  if ((lto = cm_zalloc(cm, sizeof(*lto))) == NULL ||
      (*it_out = cm_zalloc(cm, sizeof(**it_out))) == NULL) {
    if (lto != NULL) cm_free(cm, lto);
    *it_out = NULL;
    return errno ? errno : ENOMEM;
  }

  lto->lto_direction = direction;

  lto->lto_hint_it = NULL;
  lto->lto_sub = NULL;
  lto->lto_fanin = NULL;

  lto->lto_statistics_sub = NULL;
  lto->lto_stat_tc_sub = NULL;
  lto->lto_stat_tc_hint = NULL;

  lto->lto_check_cached_id = PDB_ID_NONE;
  lto->lto_id = PDB_ID_NONE;
  lto->lto_sub_id = PDB_ID_NONE;
  lto->lto_resume_id = PDB_ID_NONE;
  lto->lto_source = PDB_ID_NONE;
  lto->lto_graphd = graphd_request_graphd(greq);

  lto->lto_pdb = lto->lto_graphd->g_pdb;
  lto->lto_cl = cl;
  lto->lto_cm = cm;
  lto->lto_greq = greq;
  lto->lto_linkage = linkage;
  lto->lto_next_method = next_method;
  lto->lto_stat_budget_max = 50;

  if (hint_guid == NULL || GRAPH_GUID_IS_NULL(*hint_guid)) {
    GRAPH_GUID_MAKE_NULL(lto->lto_hint_guid);
    lto->lto_hint_linkage = PDB_LINKAGE_N;
  } else {
    lto->lto_hint_guid = *hint_guid;
    lto->lto_hint_linkage = hint_linkage;
  }

  cl_assert(cl, PDB_IS_LINKAGE(linkage));
  pdb_iterator_make_loc(lto->lto_pdb, *it_out, low, high, forward, file, line);
  pdb_iterator_ordering_set(lto->lto_pdb, *it_out, ordering);

  lto->lto_sub = *sub;
  *sub = NULL;

  (*it_out)->it_theory = lto;
  (*it_out)->it_type = &graphd_iterator_linksto_type;

  return 0;
}

static int graphd_iterator_linksto_set_fixed_masquerade(
    pdb_handle *pdb, pdb_iterator *fix_it, int linkage, int hint_linkage,
    graph_guid const *hint_guid, pdb_iterator *sub_it) {
  cl_handle *cl = pdb_log(pdb);
  cm_buffer mq;
  cm_handle *cm = pdb_mem(pdb);
  int err;

  cl_assert(cl, sub_it != NULL);

  /* Don't bother if it's small. */
  if (pdb_iterator_n(pdb, fix_it) <= 7) return 0;

  cm_buffer_initialize(&mq, cm);
  err = cm_buffer_add_string(&mq, "fixed-");
  if (err != 0) return err;

  err = linksto_freeze_set(
      pdb, fix_it->it_low, fix_it->it_high,
      pdb_iterator_forward(pdb, fix_it) ? GRAPHD_DIRECTION_FORWARD
                                        : GRAPHD_DIRECTION_BACKWARD,
      pdb_iterator_ordering(pdb, fix_it), pdb_iterator_account(pdb, fix_it),
      LTO_NEXT_UNSPECIFIED, linkage, hint_linkage, hint_guid, sub_it, &mq);
  if (err != 0) return err;

  err = graphd_iterator_fixed_set_masquerade(fix_it, cm_buffer_memory(&mq));

  /* GRAPHD_ERR_NO from graphd_iterator_fixed_set_masquerade means:
   * "I'm not an 'fixed'-iterator!"
   *  That's okay, we'll just take the unmasqueraded freeze, then.
   */
  if (err == GRAPHD_ERR_NO) err = 0;

  cm_buffer_finish(&mq);
  return err;
}

static int graphd_iterator_linksto_set_or_masquerade(
    pdb_handle *pdb, pdb_iterator *or_it, int linkage, int hint_linkage,
    graph_guid const *hint_guid, pdb_iterator *sub_it) {
  cm_buffer mq;
  cm_handle *cm = pdb_mem(pdb);
  cl_handle *cl = pdb_log(pdb);
  int err;

  cm_buffer_initialize(&mq, cm);

  /*  Make a masqerade string for the "or".
   *  The "or" will call the linksto iterator to produce
   *  its skeleton, then proceed with the rest.
   */
  err = cm_buffer_add_string(&mq, "or-");
  if (err != 0) return err;

  err = linksto_freeze_set(
      pdb, or_it->it_low, or_it->it_high,
      pdb_iterator_forward(pdb, or_it) ? GRAPHD_DIRECTION_FORWARD
                                       : GRAPHD_DIRECTION_BACKWARD,
      pdb_iterator_ordering(pdb, or_it), pdb_iterator_account(pdb, or_it),
      LTO_NEXT_UNSPECIFIED, linkage, hint_linkage, hint_guid, sub_it, &mq);
  if (err != 0) return err;

  err = graphd_iterator_or_set_masquerade(or_it, cm_buffer_memory(&mq));

  /* GRAPHD_ERR_NO from the set function means:
   * "I'm not the kind of iterator you think I am!"
   */
  if (err == GRAPHD_ERR_NO) {
    /*  The OR may have turned into a FIXED.  OK, sure.
     *  "Hey, fixed, can you hear me?  Could you put this on?"
     */
    cl_assert(cl, sub_it != NULL);
    err = graphd_iterator_linksto_set_fixed_masquerade(
        pdb, or_it, linkage, hint_linkage, hint_guid, sub_it);
    if (err == GRAPHD_ERR_NO) /* "Fixed, what fixed?" */
      err = 0;
  }

  cm_buffer_finish(&mq);
  return err;
}

/**
 * @brief Make an "or" iterator, given ingredients for a "linksto".
 *
 * @param graphd	server for whom we're doing this
 * @param linkage	the linkage code, PDB_LINKAGE_..
 * @param hint_linkage	linkage that has the hint_guid.
 * @param hint_guid	NULL or a hint_guid guid for all those links
 * @param sub		subiterator
 * @param low		low end of result spectrum, included
 * @param high		high end of result spectrum, excluded
 * @param forward	direction that the caller wants
 * @param it_out	out: the newly constructed iterator.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int graphd_iterator_linksto_or(graphd_request *greq, int linkage,
                                      int hint_linkage,
                                      graph_guid const *hint_guid,
                                      pdb_iterator *sub, unsigned long long low,
                                      unsigned long long high, bool forward,
                                      pdb_iterator **it_out) {
  graphd_handle *graphd = graphd_request_graphd(greq);
  pdb_handle *pdb = graphd->g_pdb;
  cl_handle *cl = pdb_log(pdb);

  pdb_id sub_id;
  int err = 0;
  char buf[200];

  *it_out = NULL;
  PDB_IS_ITERATOR(cl, sub);

  err = graphd_iterator_or_create(greq, 0, forward, it_out);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_create", err,
                 "%lld-%lld", (long long)low, (long long)high);
    pdb_iterator_destroy(pdb, it_out);

    return err;
  }

  /*  For all our subiterator IDs...
   */
  while ((err = pdb_iterator_next_nonstep(pdb, sub, &sub_id)) == 0) {
    pdb_iterator *part_it;

    /*  Create a linkage iterator over that sub id,
     *  and store it in part_it.
     */
    err = linksto_fanin(graphd, linkage, hint_linkage, hint_guid, sub_id, low,
                        high, forward, &part_it);
    if (err != 0) {
      if (err == GRAPHD_ERR_NO) continue;

      cl_log_errno(cl, CL_LEVEL_FAIL, "linksto_fanin", err, "sub_id=%lld",
                   (long long)sub_id);
      pdb_iterator_destroy(pdb, it_out);
      return err;
    }
    if (pdb_iterator_null_is_instance(pdb, part_it)) {
      pdb_iterator_destroy(pdb, &part_it);
      continue;
    }

    err = graphd_iterator_or_add_subcondition(*it_out, &part_it);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_add_subcondition",
                   err, "part_it=%s",
                   pdb_iterator_to_string(pdb, part_it, buf, sizeof buf));

      pdb_iterator_destroy(pdb, &part_it);
      pdb_iterator_destroy(pdb, it_out);

      return err;
    }
    pdb_iterator_destroy(pdb, &part_it);
  }
  if ((err = graphd_iterator_or_create_commit(*it_out)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_create_commit", err,
                 "it=%s",
                 pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
    pdb_iterator_destroy(pdb, it_out);

    return err;
  }

  /*  Install a masquerade string in the OR to allow it
   *  to be reconstituted from the linksto.
   */
  (void)graphd_iterator_linksto_set_or_masquerade(pdb, *it_out, linkage,
                                                  hint_linkage, hint_guid, sub);
  return 0;
}

/**
 * @brief Agressively evaluate a "linksto" iterator structure.
 *
 *  If the destination of a "linksto" iterator is tractable at
 *  all, try to evaluate it at create-time into a fixed set of values.
 *
 *  Low hanging fruit:
 *
 *      (a)  Our subiterator is NULL; therefore, we are null.
 *
 *   	(b)  Our subiterator is a single entry; therefore,
 *	     we are a simple GMAP.
 *
 *  Two good places beyond that which we can go:
 *
 *	(c) FIXED iterator.  We evaluate into so few values
 *	    that we can just pull those out and stash them
 *	    in an array, then sort them.
 *
 *	(d) OR iterator.  Our subiterator has so few values
 *	    that we can create a GMAP for each of them, then
 *	    merge them in a sorted OR.
 *
 *  The call can return iterators other than linksto iterators;
 *  notably, a null iterator, fixed iterator, gmap,
 *  or "or" iterator.
 *
 * @param graphd	server for whom we're doing this
 * @param linkage	the linkage code, PDB_LINKAGE_..
 * @param sub		subiterator, moves into the new iterator
 * @param low		low end of result spectrum (included)
 * @param high		high end of result spectrum (excluded)
 * @param forward	forward (true) or backward (false)
 * @param it_out	out: the newly constructed iterator.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int linksto_preevaluate(graphd_request *greq, int linkage,
                               int hint_linkage, graph_guid const *hint_guid,
                               pdb_iterator **sub, unsigned long long low,
                               unsigned long long high, bool forward,
                               pdb_iterator **it_out) {
  graphd_handle *graphd = graphd_request_graphd(greq);
  pdb_handle *const pdb = graphd->g_pdb;
  cl_handle *const cl = pdb_log(pdb);

  pdb_iterator *fixed_it = NULL, *linksto_it = NULL;
  pdb_iterator *part_it[GRAPHD_LINKSTO_PREEVALUATE_N];
  size_t part_it_n = 0, i;
  unsigned long long total_id_n = 0;
  pdb_budget total_budget = GRAPHD_LINKSTO_PREEVALUATE_BUDGET;
  pdb_id sub_id, id;
  int err = 0;
  char buf[200];

  *it_out = NULL;
  PDB_IS_ITERATOR(cl, *sub);

  /*  We can't link to something that doesn't exist, so
   *  our "low" must be higher than the sub low.
   */
  if (low <= (*sub)->it_low) low = (*sub)->it_low + 1;

  /*  Our subiterator doesn't yet know how many
   *  elements it has?  Ick, we'll just have to wait.
   */
  if (!pdb_iterator_n_valid(pdb, *sub) ||
      !pdb_iterator_next_cost_valid(pdb, *sub))
    return PDB_ERR_MORE;

  if (pdb_iterator_n(pdb, *sub) >= GRAPHD_LINKSTO_PREEVALUATE_N ||
      (pdb_iterator_n(pdb, *sub) * pdb_iterator_next_cost(pdb, *sub) >
       GRAPHD_LINKSTO_PREEVALUATE_BUDGET)) {
    cl_log(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
           "linksto_preevaluate: "
           "subiterator %s announces %llu ids -- too many.",
           pdb_iterator_to_string(pdb, *sub, buf, sizeof buf),
           (unsigned long long)pdb_iterator_n(pdb, *sub));

    return PDB_ERR_MORE;
  }

  /*  For all our subiterator IDs...
   */
  cl_enter(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
           "%s; %s=%s; sub-IDs: %llu", pdb_linkage_to_string(linkage),
           pdb_linkage_to_string(hint_linkage),
           graph_guid_to_string(hint_guid, buf, sizeof buf),
           (unsigned long long)pdb_iterator_n(pdb, *sub));
  for (;;) {
    /*  Too many subiterator IDs?
     */
    if (part_it_n >= GRAPHD_LINKSTO_PREEVALUATE_N) {
      cl_log(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
             "linksto_preevaluate: "
             "more than %zu subiterators -- bailing out",
             part_it_n);
      err = 0;
      goto cancel;
    }

    /*  Pull another id from the subiterator.
     */
    err = pdb_iterator_next(pdb, *sub, &sub_id, &total_budget);
    if (err != 0) {
      if (err == GRAPHD_ERR_NO) {
        err = 0;
        break;
      }
      if (err == PDB_ERR_MORE)
        cl_log(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
               "linksto_preevaluate: "
               "out of pre-evaluation budget - bailing out");
      else
        cl_log_errno(cl, CL_LEVEL_FAIL | GRAPHD_FACILITY_LINKSTO,
                     "pdb_iterator_next", err, "it=%s",
                     pdb_iterator_to_string(pdb, *sub, buf, sizeof buf));
      goto cancel;
    }
    if (total_budget <= 0) {
      cl_log(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
             "linksto_preevaluate: "
             "out of pre-evaluation budget - bailing out");
      err = PDB_ERR_MORE;
      goto cancel;
    }

    /*  Create a linkage iterator over that sub id,
     *  and store it in part_it[part_it_n].
     */
    err = linksto_fanin(graphd, linkage, hint_linkage, hint_guid, sub_id, low,
                        high, forward, part_it + part_it_n);
    if (err != 0) {
      if (err == GRAPHD_ERR_NO) continue;
      goto cancel;
    }

    if (!pdb_iterator_n_valid(pdb, part_it[part_it_n])) {
      /* This shouldn't happen -- this is just
       * a simple PDB iterator.
       */
      cl_log(cl, CL_LEVEL_DEBUG | GRAPHD_FACILITY_LINKSTO,
             "linksto_preevaluate: "
             "part iterator %s doesn't know its own size?",
             pdb_iterator_to_string(pdb, part_it[part_it_n], buf, sizeof(buf)));
      total_id_n = GRAPHD_LINKSTO_PREEVALUATE_ID_N + 1;
    } else {
      if (pdb_iterator_n(pdb, part_it[part_it_n]) == 0) {
        pdb_iterator_destroy(pdb, part_it + part_it_n);
        continue;
      }
      total_id_n += pdb_iterator_n(pdb, part_it[part_it_n]);
    }
    part_it_n++;
  }

  if (part_it_n == 0) {
    pdb_iterator_destroy(pdb, sub);
    cl_leave(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
             "no non-null fanins -> null");
    return pdb_iterator_null_create(pdb, it_out);
  }

  cl_assert(cl, total_id_n > 0);
  if (part_it_n == 1) {
    pdb_id *id_dummy;
    size_t id_n_dummy;

    if (total_id_n == 1 &&
        !graphd_iterator_fixed_is_instance(pdb, part_it[0], &id_dummy,
                                           &id_n_dummy)) {
      pdb_budget bud = 1000;
      pdb_id id, dummy;

      if ((err = pdb_iterator_reset(pdb, *part_it)) != 0) {
        cl_log_errno(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
                     "pdb_iterator_reset", err, "it=%s",
                     pdb_iterator_to_string(pdb, part_it[0], buf, sizeof buf));
        cl_leave(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
                 "reset failed");
        return err;
      }

      /* First element exists.
       */
      err = pdb_iterator_next(pdb, *part_it, &id, &bud);
      if (err != 0) {
        /*  Nothing in there?
         */
        if (err == GRAPHD_ERR_NO) {
          pdb_iterator_destroy(pdb, sub);
          pdb_iterator_destroy(pdb, part_it);

          cl_leave(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
                   "no non-null fanins -> null");
          return pdb_iterator_null_create(pdb, it_out);
        }

        /*  Some other error?
         */
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "it=%s",
                     pdb_iterator_to_string(pdb, part_it[0], buf, sizeof buf));
        cl_leave(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO, "next failed");
        return err;
      }

      /* Second element doesn't exist.
       */
      err = pdb_iterator_next(pdb, *part_it, &dummy, &bud);
      if (err == GRAPHD_ERR_NO) {
        /*  Yep, we're a single value.  Make us
         *  a "fixed".
         */
        pdb_iterator_destroy(pdb, part_it);
        pdb_iterator_destroy(pdb, sub);

        cl_leave(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
                 "turning into single id %llx", (unsigned long long)id);

        /*  Don't try to install a masquerade here -
         *  it's unlikely to be beneficial.
         */
        return graphd_iterator_fixed_create_array(graphd, &id, 1, low, high,
                                                  forward, it_out);
      }
    }

    *it_out = part_it[0];
    pdb_iterator_destroy(pdb, sub);

    cl_leave(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
             "turning into first iterator %s",
             pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
    return 0;
  }

  if (total_id_n <= GRAPHD_LINKSTO_PREEVALUATE_ID_N) {
    size_t actual_id_n;

    /*  OK. Up to GRAPHD_LINKSTO_PREEVALUATE_N iterators now
     *  occupy the first part_it_n entries of part_it.
     *
     *  Each subiterator claims result counts that together do
     *  not exceed GRAPHD_LINKSTO_PREEVALUATE_ID_N.  But those
     *  are just estimates; when we actually pull out the ids,
     *  there can be fewer or more.
     *
     *  Pull their IDs, and store those IDs in a fixed iterator..
     *  If more than GRAPHD_LINKSTO_PREEVALUATE_ID_N of them
     *  end up showing up, cancel the whole thing.
     */
    err = graphd_iterator_fixed_create(graphd, GRAPHD_LINKSTO_PREEVALUATE_ID_N,
                                       low, high, forward, &fixed_it);
    if (err != 0) goto cancel;

    actual_id_n = 0;
    for (i = 0; i < part_it_n; i++) {
      while ((err = pdb_iterator_next(pdb, part_it[i], &id, &total_budget)) ==
             0) {
        if (actual_id_n >= total_id_n) {
          cl_log(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
                 "linksto_preevaluate: "
                 "too many ids (%zu) after %s",
                 actual_id_n,
                 pdb_iterator_to_string(pdb, part_it[i], buf, sizeof buf));
          break;
        }
        if (total_budget <= 0) {
          cl_log_errno(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
                       "linksto_preevaluate: "
                       "takes too long",
                       err, "it=%s", pdb_iterator_to_string(pdb, part_it[i],
                                                            buf, sizeof buf));
          goto cancel;
        }

        actual_id_n++;
        err = graphd_iterator_fixed_add_id(fixed_it, id);
        if (err != 0) {
          cl_log_errno(
              cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
              "graphd_iterator_fixed_add_id", err, "it=%s",
              pdb_iterator_to_string(pdb, part_it[i], buf, sizeof buf));
          goto cancel;
        }
      }
      if (err != GRAPHD_ERR_NO) {
        cl_log_errno(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
                     "pdb_iterator_next", err, "it=%s",
                     pdb_iterator_to_string(pdb, part_it[i], buf, sizeof buf));
        goto cancel;
      }
    }

    /*  We ran to completion?
     */
    if (i >= part_it_n) {
      /*  Free the iterators and the subiterator.
       */
      for (i = 0; i < part_it_n; i++) pdb_iterator_destroy(pdb, part_it + i);

      /*  Set a masquerade in the fixed iterator.
       */
      (void)graphd_iterator_linksto_set_fixed_masquerade(
          pdb, fixed_it, linkage, hint_linkage, hint_guid, *sub);
      pdb_iterator_destroy(pdb, sub);

      graphd_iterator_fixed_create_commit(fixed_it);
      *it_out = fixed_it;

      cl_leave(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO, "%s",
               pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
      return 0;
    }

    /*  No.  Oh well.  Reset the iterators we used.
     */
    do {
      err = pdb_iterator_reset(pdb, part_it[i]);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
                     "pdb_iterator_reset", err, "it=%s",
                     pdb_iterator_to_string(pdb, part_it[i], buf, sizeof buf));
        goto cancel;
      }
    } while (i-- > 0);
  }

  cl_log(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
         "linksto_preevaluate: "
         "too many ids (%llu) to turn into a fixed array; but few "
         "enough iterators (%zu) to use an \"or\".",
         total_id_n, part_it_n);

  err = graphd_iterator_or_create(greq, part_it_n, forward, it_out);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_create", err, "n=%zu",
                 part_it_n);
    cl_leave(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
             "graphd_iterator_or_create failed");
    return err;
  }
  for (i = 0; i < part_it_n; i++)
    graphd_iterator_or_add_subcondition(*it_out, part_it + i);
  if ((err = graphd_iterator_or_create_commit(*it_out)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL | GRAPHD_FACILITY_LINKSTO,
                 "graphd_iterator_or_create_commit", err, "it=%s",
                 pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
    pdb_iterator_destroy(pdb, it_out);
    cl_leave(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
             "error in or_commit: %s", graphd_strerror(err));
    return err;
  }

  cl_assert(cl, pdb_iterator_statistics_done(pdb, *it_out));
  cl_assert(cl, pdb_iterator_sorted(pdb, *it_out));

  if (part_it_n >= 7) {
    err = graphd_iterator_linksto_set_or_masquerade(
        pdb, *it_out, linkage, hint_linkage, hint_guid, *sub);
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
               "error in set_or_masquerade: %s", graphd_strerror(err));
      return err;
    }
  }

  err = linksto_create(
      greq, linkage, hint_linkage, hint_guid, sub, low, high,
      forward ? GRAPHD_DIRECTION_FORWARD : GRAPHD_DIRECTION_BACKWARD,
      NULL, /* doesn't matter ! */
      LTO_NEXT_UNSPECIFIED, &linksto_it, __FILE__, __LINE__);
  pdb_iterator_destroy(pdb, sub);

  if (err == 0) graphd_iterator_or_set_check(*it_out, &linksto_it);

  pdb_iterator_destroy(pdb, &linksto_it);

  if (err != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO,
             "error in linksto_create: %s", graphd_strerror(err));
    return err;
  }
  cl_leave(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO, "become %s",
           pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));

  return 0;

cancel:
  if (fixed_it != NULL) pdb_iterator_destroy(pdb, &fixed_it);
  for (i = 0; i < part_it_n; i++) pdb_iterator_destroy(pdb, part_it + i);
  pdb_iterator_destroy(pdb, it_out);

  cl_leave(cl, CL_LEVEL_VERBOSE | GRAPHD_FACILITY_LINKSTO, "%s",
           err ? graphd_strerror(err)
               : pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
  if (err) return err;

  err = pdb_iterator_reset(pdb, *sub);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_VERBOSE, "pdb_iterator_reset", err, "it=%s",
                 pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
    return err;
  }
  return PDB_ERR_MORE;
}

/*
 * Are l1 and l2 vipable
 */
static bool vip_compatable(int l1, int l2) {
  if ((l1 == PDB_LINKAGE_LEFT || l1 == PDB_LINKAGE_RIGHT) &&
      l2 == PDB_LINKAGE_TYPEGUID)
    return true;

  if ((l2 == PDB_LINKAGE_LEFT || l2 == PDB_LINKAGE_RIGHT) &&
      l1 == PDB_LINKAGE_TYPEGUID)
    return true;
  return false;
}

/**
 * @brief Create an "linksto" iterator structure.
 *
 *  The new iterator L is derived from another iterator S.
 *  The primitives in L point to the primitives in S with their
 *  linkage pointer.
 *
 *  The subiterator, *sub, moves into the created iterator.
 *  The pointer is zero'd out after a successful call.
 *
 *  The call can return iterators other than linksto
 *  iterators; notably, a null iterator, fixed iterator,
 *  or gmap.
 *
 * @param greq		request for whom we're doing this
 * @param linkage	the linkage code, PDB_LINKAGE_..
 * @param hint_linkage	the linkage of the constant hint GUID
 * @param hint_guid	NULL or a pointer to a hint GUID
 * @param sub		subiterator, moves into the new iterator
 * @param low		low end of result spectrum (included)
 * @param high		high end of result spectrum (excluded)
 * @param direction	how do we want our results?
 * @param ordering	ordering, must be NULL or internalized already
 * @param it_out	out: the newly constructed iterator.
 * @param file		calling code, filename (filled in by macro)
 * @param line		calling code, line (filled in by macro)
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_linksto_create_loc(
    graphd_request *greq, int linkage, int hint_linkage,
    graph_guid const *hint_guid, pdb_iterator **sub, unsigned long long low,
    unsigned long long high, graphd_direction direction, char const *ordering,
    pdb_iterator **it_out, char const *file, int line) {
  graphd_handle *const g = graphd_request_graphd(greq);
  pdb_handle *const pdb = g->g_pdb;
  cl_handle *const cl = pdb_log(pdb);
  int err = 0;
  char buf[200], gbuf[GRAPH_GUID_SIZE];

  PDB_IS_ITERATOR(cl, *sub);
  cl_assert(cl, GRAPHD_DIRECTION_VALID(direction));
  cl_assert(cl, direction != GRAPHD_DIRECTION_ORDERING || ordering != NULL);

  cl_enter(
      cl, CL_LEVEL_VERBOSE, "%s(%s=%s)[%lld...%lld]->%s%s%s [from %s:%d]",
      pdb_linkage_to_string(linkage),
      hint_guid ? pdb_linkage_to_string(hint_linkage) : "",
      hint_guid ? graph_guid_to_string(hint_guid, gbuf, sizeof gbuf) : "null",
      (long long)low, (long long)high,
      pdb_iterator_to_string(pdb, *sub, buf, sizeof buf),
      ordering ? ", ordering=" : "", ordering ? ordering : "", file, line);

  /*  We can't link to something that doesn't exist, so
   *  our "low" must be higher than the sub low.
   */
  if ((*sub)->it_low >= low) low = (*sub)->it_low + 1;

  if (pdb_iterator_null_is_instance(pdb, *sub)) {
    *it_out = *sub;
    *sub = NULL;

    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_linksto_create: "
           "returning null in place of %s->(null)",
           pdb_linkage_to_string(linkage));
    goto done;
  }

  if (ordering != NULL)
    ordering = graphd_iterator_ordering_internalize_request(
        greq, ordering, ordering + strlen(ordering));

  err =
      linksto_preevaluate(greq, linkage, hint_linkage, hint_guid, sub, low,
                          high, direction != GRAPHD_DIRECTION_BACKWARD, it_out);
  if (err != PDB_ERR_MORE) {
    if (err != 0)
      cl_log_errno(cl, CL_LEVEL_FAIL, "linksto_preevaluate", err, "linkage=%s",
                   pdb_linkage_to_string(linkage));
    else {
      if ((direction == GRAPHD_DIRECTION_FORWARD ||
           direction == GRAPHD_DIRECTION_BACKWARD) &&
          ordering != NULL && pdb_iterator_sorted_valid(pdb, *it_out) &&
          pdb_iterator_sorted(pdb, *it_out)) {
        pdb_iterator_ordered_set(pdb, *it_out, true);
        pdb_iterator_ordering_set(pdb, *it_out, ordering);
      }
      pdb_iterator_destroy(pdb, sub);
    }
    goto done;
  }

  if (pdb_iterator_all_is_instance(pdb, *sub) &&
      (pdb_iterator_n(pdb, *sub) >=
       ((high == PDB_ITERATOR_HIGH_ANY ? pdb_primitive_n(pdb) : high) - low) /
           10) &&
      (direction != GRAPHD_DIRECTION_ORDERING ||
       !pdb_iterator_ordering_wants(pdb, *sub, ordering))) {
    /*  The subiterator is an "all", and its n isn't
     *  more than an order of magnitude smaller
     *  than ours, and we're not using its ordering...
     */
    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_linksto_create: "
           "returning all in place of %s->(all)",
           pdb_linkage_to_string(linkage));

    if ((*sub)->it_low + 1 > low) low = (*sub)->it_low + 1;
    err = pdb_iterator_all_create(
        pdb, low, high, direction != GRAPHD_DIRECTION_BACKWARD, it_out);
    if (err == 0) {
      graphd_iterator_set_direction_ordering(pdb, *it_out, direction, ordering);
      pdb_iterator_destroy(pdb, sub);
    }
    goto done;
  }

  /*
   * Drop the hint if we won't be able to make VIPs out of it.
   */
  if (!vip_compatable(linkage, hint_linkage)) {
    err = linksto_create(greq, linkage, PDB_LINKAGE_N, NULL, sub, low, high,
                         direction, ordering, LTO_NEXT_UNSPECIFIED, it_out,
                         file, line);
  } else {
    err = linksto_create(greq, linkage, hint_linkage, hint_guid, sub, low, high,
                         direction, ordering, LTO_NEXT_UNSPECIFIED, it_out,
                         file, line);
  }

done:
  if (err)
    cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
  else
    cl_leave(cl, CL_LEVEL_VERBOSE, "%s [%lld..%lld[",
             pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf),
             (*it_out)->it_low, (*it_out)->it_high);
  return err;
}

/**
 * @brief Desequentialize a linksto iterator
 *
 * @param graphd	module handle
 * @param pit		the iterator text
 * @param pib		iterator base
 * @param it_out	out: a new iterator.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_linksto_thaw_loc(
    graphd_handle *g, pdb_iterator_text const *pit, pdb_iterator_base *pib,
    graphd_iterator_hint hint, cl_loglevel loglevel, pdb_iterator **it_out,
    char const *file, int line) {
  pdb_handle *pdb = g->g_pdb;
  cl_handle *cl = pdb_log(pdb);
  pdb_iterator *sub_it = NULL, *fanin_it = NULL;
  pdb_id resume_id = PDB_ID_NONE, sub_id = PDB_ID_NONE;
  int linkage, hint_linkage = PDB_LINKAGE_TYPEGUID;
  unsigned long long low, high, estimate_n, upper_bound;
  pdb_budget check_cost, next_cost, find_cost;
  int err;
  graph_guid hint_guid;
  char const *s = pit->pit_set_s, *e = pit->pit_set_e;
  pdb_iterator_text subpit;
  graphd_direction direction;
  bool forward;
  char dirchar;
  int call_state = 0, method = 0;
  char const *ordering = NULL;
  char const *ord_s = NULL, *ord_e;
  pdb_iterator_account *acc = NULL;
  graphd_request *greq;

  *it_out = NULL;

  if ((upper_bound = pdb_primitive_n(pdb)) == 0)
    return pdb_iterator_null_create(pdb, it_out);

  greq = pdb_iterator_base_lookup(pdb, pib, "graphd.request");
  if (greq == NULL) return ENOMEM;

  if (graphd_request_timer_check(greq)) return GRAPHD_ERR_TOO_HARD;

  cl = graphd_request_cl(greq);

  err = pdb_iterator_util_thaw(
      pdb, &s, e, "%c%{low[-high]}:%{linkage[+guid]}->%{(bytes)}", &dirchar,
      &low, &high, &linkage, &hint_guid, &subpit.pit_set_s, &subpit.pit_set_e);
  if (err != 0) goto scan_error;

  direction = graphd_iterator_direction_from_char(dirchar);
  forward = direction != GRAPHD_DIRECTION_BACKWARD;

  if (s < e && *s == '[') {
    char const *s1 = s;
    if (pdb_iterator_util_thaw(pdb, &s1, e, "[md:%d]", &method) != 0)
      method = LTO_NEXT_UNSPECIFIED;
    else
      s = s1;
  }

  err = pdb_iterator_util_thaw(pdb, &s, e, "%{orderingbytes}%{account}", &ord_s,
                               &ord_e, pib, &acc);
  if (err != 0) return err;

  if (ord_s != NULL)

    /*  We did get an ordering from the frozen text.
     *  Translate it into a pointer to the same ordering
     *  in the place that it designates.
     */
    ordering = graphd_iterator_ordering_internalize(g, pib, ord_s, ord_e);

  if (e - s >= sizeof("[h:]") && *s == '[' && s[1] == 'h' && s[2] == ':') {
    err = pdb_iterator_util_thaw(pdb, &s, e, "[h:%{linkage}=%{guid}]",
                                 &hint_linkage, &hint_guid);
    if (err != 0) goto scan_error;
  }

  err = pdb_iterator_util_thaw(pdb, &s, e, "%{extensions}%{end}", NULL);
  if (err != 0) return err;

  if ((s = pit->pit_position_s) != NULL && s < (e = pit->pit_position_e)) {
    char const *s0 = s;

    /* METHOD : RESUME-ID : SUB-ID [OPT]
     */
    err = pdb_iterator_util_thaw(
        pdb, &s, e, "%d:%{id}:%{id}%{extensions}%{end}", &method, &resume_id,
        &sub_id, (pdb_iterator_property *)NULL);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "graphd_iterator_linksto_create", err,
                   "couldn't scan position \"%.*s\"", (int)(e - s0), s0);

      pdb_iterator_destroy(pdb, &sub_it);
      pdb_iterator_destroy(pdb, &fanin_it);
      pdb_iterator_destroy(pdb, it_out);

      return err;
    }
  }
  if ((s = pit->pit_state_s) != NULL && s < (e = pit->pit_state_e)) {
    /* CALLSTATE:[OPT](SUBPOS/SUBSTATE):(FANIN)
     */
    err = pdb_iterator_util_thaw(
        pdb, &s, e, "%d:%{extensions}%{(position/state)}:", &call_state,
        (pdb_iterator_property *)NULL, &subpit);
    if (err != 0) goto scan_error;

    err = graphd_iterator_util_thaw_subiterator(g, &s, e, pib, loglevel,
                                                &fanin_it);
    if (err != 0) {
      goto scan_error;
    }

    if (s < e && *s == ':') s++;

    /*  Leave s and e valid; we'll get back to them
     *  later, once we have an actual lto iterator state.
     */
  } else {
    subpit.pit_state_s = subpit.pit_state_e = subpit.pit_position_s =
        subpit.pit_position_e = NULL;
  }

  /*  Reconstitute the subiterator
   */
  err = graphd_iterator_thaw_loc(g, &subpit, pib, 0, loglevel, &sub_it, NULL,
                                 file, line);
  if (err != 0) goto scan_error;

  /*  Preevaluate and reinstall masquerades.  If these hints are
   *  set, we're really building something that isn't a linksto
   *  iterator - we're just using the linksto specification as
   *  a shorthand for saying what's in the "fixed" or "or" iterator.
   */
  if (hint & GRAPHD_ITERATOR_HINT_OR) {
    pdb_iterator *linksto_it;

    err = graphd_iterator_linksto_or(greq, linkage, hint_linkage, &hint_guid,
                                     sub_it, low, high, forward, it_out);
    if (err == 0) {
      err = linksto_create(greq, linkage, hint_linkage, &hint_guid, &sub_it,
                           low, high, direction, ordering, method, &linksto_it,
                           file, line);
      if (err == 0) {
        (void)graphd_iterator_or_set_check(*it_out, &linksto_it);
        pdb_iterator_destroy(pdb, &linksto_it);
      }
    }
  } else if (hint & GRAPHD_ITERATOR_HINT_FIXED) {
    err = linksto_preevaluate(greq, linkage, hint_linkage, &hint_guid, &sub_it,
                              low, high, forward, it_out);
    if (err == PDB_ERR_MORE)
      err =
          linksto_create(greq, linkage, hint_linkage, &hint_guid, &sub_it, low,
                         high, direction, ordering, method, it_out, file, line);
  } else {
    err = linksto_create(greq, linkage, hint_linkage, &hint_guid, &sub_it, low,
                         high, direction, ordering, method, it_out, file, line);
  }

  if (err == 0 && *it_out != NULL) pdb_iterator_account_set(pdb, *it_out, acc);

  /*  Now that - well, if - we have a linksto iterator,
   *  thaw the rest of the iterator state and assign it.
   */
  if (err == 0 && (*it_out) != NULL &&
      (*it_out)->it_type == &graphd_iterator_linksto_type && s != NULL &&
      s < e) {
    graphd_iterator_linksto *lto = NULL;
    pdb_iterator *it = *it_out;

    cl_assert(cl, it != NULL);
    lto = it->it_theory;

    lto->lto_fanin = fanin_it;
    lto->lto_sub_id = sub_id;
    lto->lto_id = lto->lto_resume_id = resume_id;
    lto->lto_direction = direction;
    lto->lto_next_method = method;
    lto->lto_thawed = true;

    fanin_it = NULL;
    it->it_call_state = call_state;

    if (s < e && *s == ':') s++;

    if (*s == '[') {
      err = linksto_thaw_statistics_state(it, &s, e, pib, loglevel);
      if (err != 0) goto scan_error;
    } else {
      err = pdb_iterator_util_thaw(pdb, &s, e, "%{budget}:%{next[+find]}:%llu",
                                   &check_cost, &next_cost, &find_cost,
                                   &estimate_n);
      if (err != 0) goto scan_error;

      pdb_iterator_next_cost_set(pdb, it, next_cost);
      pdb_iterator_check_cost_set(pdb, it, check_cost);
      pdb_iterator_find_cost_set(pdb, it, find_cost);
      pdb_iterator_n_set(pdb, it, estimate_n);

      pdb_iterator_sorted_set(pdb, it,
                              lto->lto_next_method == LTO_NEXT_TYPECHECK);

      /*  If we thawed an ordering, and we're finished
       *  with statistics, that ordering was relevant -
       *  the iterator actually is ordered.
       */
      pdb_iterator_ordered_set(pdb, it, pdb_iterator_ordering(pdb, it) != NULL);

      /*  If we are ordered, and we're using SUBFANIN
       *  nexts, our subiterator is ordered, too.
       */
      if (pdb_iterator_ordered(pdb, it) &&
          lto->lto_next_method == LTO_NEXT_SUBFANIN)
        pdb_iterator_ordered_set(pdb, lto->lto_sub, true);

      pdb_iterator_statistics_done_set(pdb, it);
    }
  }
  pdb_iterator_destroy(pdb, &sub_it);
  pdb_iterator_destroy(pdb, &fanin_it);

  if (err != 0) {
  scan_error:
    cl_log_errno(cl, loglevel, "graphd_iterator_linksto_create", err,
                 "couldn't create \"%.*s\"",
                 (int)(pit->pit_set_e - pit->pit_set_s), pit->pit_set_s);

    pdb_iterator_destroy(pdb, &sub_it);
    pdb_iterator_destroy(pdb, &fanin_it);
    pdb_iterator_destroy(pdb, it_out);

    return err;
  }
  return 0;
}

/**
 * @brief Is this a links-to iterator?  Which one?
 *
 * @param pdb		module handle
 * @param it		iterator the caller is asking about
 * @param linkage_out	what's the connection to the subiterator?
 * @param sub_out	what's the subiterator?
 *
 * @return true if this is an is-a iterator, false otherwise
 */
bool graphd_iterator_linksto_is_instance(pdb_handle *pdb, pdb_iterator *it,
                                         int *linkage_out,
                                         pdb_iterator **sub_out) {
  graphd_iterator_linksto *lto;

  if (it->it_type != &graphd_iterator_linksto_type) return false;

  lto = it->it_theory;

  if (sub_out != NULL) *sub_out = lto->lto_sub;

  if (linkage_out != NULL) *linkage_out = lto->lto_linkage;

  return true;
}
