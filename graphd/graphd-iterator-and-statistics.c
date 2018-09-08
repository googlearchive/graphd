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

#define IS_EASY_FAST_ITERATOR(pdb, it)                                      \
  (pdb_iterator_statistics_done(pdb, it) && pdb_iterator_sorted(pdb, it) && \
   pdb_iterator_check_cost(pdb, it) <= 200 &&                               \
   pdb_iterator_next_cost(pdb, it) <= 100 &&                                \
   pdb_iterator_find_cost(pdb, it) <= 300)

#define ESTIMATE_HIGH (true)
#define ESTIMATE_LOW (false)

/*  Return whether the subcondition <sc> could turn out to be sorted
 *  in a direction that's useful for the desired ordering of the
 *  containing <and>.
 */
static bool sc_may_be_usefully_sorted(pdb_handle *pdb, pdb_iterator *it,
                                      graphd_subcondition const *sc) {
  graphd_iterator_and const *gia = it->it_theory;
  char const *sub_ordering;

  cl_handle *cl = pdb_log(pdb);

  cl_log(
      cl, CL_LEVEL_VERBOSE,
      "sc_may_be_usefully_sorted "
      "gia direction: %d;o=%s -- sc o=%s %d %d",
      gia->gia_direction,
      pdb_iterator_ordering(pdb, it) ? pdb_iterator_ordering(pdb, it) : "null",
      pdb_iterator_ordering(pdb, sc->sc_it)
          ? pdb_iterator_ordering(pdb, sc->sc_it)
          : "null",
      pdb_iterator_ordered_valid(pdb, sc->sc_it),
      pdb_iterator_ordered(pdb, sc->sc_it));

  switch (gia->gia_direction) {
    case GRAPHD_DIRECTION_ANY:
      return true;

    case GRAPHD_DIRECTION_FORWARD:
      return !pdb_iterator_sorted_valid(pdb, sc->sc_it) ||
             (pdb_iterator_sorted(pdb, sc->sc_it) &&
              pdb_iterator_forward(pdb, sc->sc_it));

    case GRAPHD_DIRECTION_BACKWARD:
      return !pdb_iterator_sorted_valid(pdb, sc->sc_it) ||
             (pdb_iterator_sorted(pdb, sc->sc_it) &&
              !pdb_iterator_forward(pdb, sc->sc_it));

    case GRAPHD_DIRECTION_ORDERING:
      sub_ordering = pdb_iterator_ordering(pdb, sc->sc_it);
      return sub_ordering != NULL &&
             pdb_iterator_ordering_wants(pdb, it, sub_ordering) &&
             (!pdb_iterator_ordered_valid(pdb, sc->sc_it) ||
              pdb_iterator_ordered(pdb, sc->sc_it));
    default:
      break;
  }

  cl_notreached(gia->gia_cl, "unexpected direction %d", gia->gia_direction);
  return false;
}

/*  Return whether the subcondition <sc> is sorted in a direction
 *  that's useful for the desired ordering of the containing
 *  <and>.
 */
static bool sc_is_usefully_sorted(pdb_handle *pdb, pdb_iterator *it,
                                  graphd_subcondition const *sc) {
  graphd_iterator_and const *gia = it->it_theory;
  char const *sub_ordering;

  switch (gia->gia_direction) {
    case GRAPHD_DIRECTION_ANY:
      return true;

    case GRAPHD_DIRECTION_FORWARD:
      return pdb_iterator_sorted_valid(pdb, sc->sc_it) &&
             pdb_iterator_sorted(pdb, sc->sc_it) &&
             pdb_iterator_forward(pdb, sc->sc_it);

    case GRAPHD_DIRECTION_BACKWARD:
      return pdb_iterator_sorted_valid(pdb, sc->sc_it) &&
             pdb_iterator_sorted(pdb, sc->sc_it) &&
             !pdb_iterator_forward(pdb, sc->sc_it);

    case GRAPHD_DIRECTION_ORDERING:

      sub_ordering = pdb_iterator_ordering(pdb, sc->sc_it);
      return sub_ordering != NULL &&
             pdb_iterator_ordering_wants(pdb, it, sub_ordering) &&
             pdb_iterator_ordered_valid(pdb, sc->sc_it) &&
             pdb_iterator_ordered(pdb, sc->sc_it);
    default:
      break;
  }

  cl_notreached(gia->gia_cl, "unexpected direction %d", gia->gia_direction);
  return false;
}

/*  Estimate the setsize for the given AND iterator.
 */
static void gia_invalidate_cached_setsize(graphd_iterator_and *ogia) {
  ogia->gia_context_setsize_cached = false;
}

static void gia_invalidate_sort(graphd_iterator_and *ogia) {
  ogia->gia_resort = true;
}

/*  How many values would this and-iterator return if we were to
 *  run all the way through?
 */
static unsigned long long gia_estimate_setsize(pdb_iterator *it,
                                               graphd_iterator_and *ogia) {
  pdb_handle *pdb = ogia->gia_pdb;
  cl_handle *cl = ogia->gia_cl;
  size_t i;
  graphd_subcondition *sc;
  unsigned long long setsize;
  unsigned long long total_est = 0;
  size_t total_est_n = 0;
  char buf[200];

  if (ogia->gia_context_setsize_cached) return ogia->gia_context_setsize;

  setsize = ogia->gia_context_setsize_valid ? ogia->gia_context_setsize
                                            : pdb_iterator_spread(pdb, it);

  for (i = 0, sc = ogia->gia_sc; i < ogia->gia_n; i++, sc++) {
    unsigned long long sub_n, est;
    size_t weight;
    pdb_range_estimate range;

    if (pdb_iterator_range_estimate(pdb, sc->sc_it, &range)) {
      unsigned long long max = PDB_COUNT_UNBOUNDED;

      if (range.range_n_exact != PDB_COUNT_UNBOUNDED)
        max = range.range_n_exact;
      else if (range.range_n_max != PDB_COUNT_UNBOUNDED)
        max = range.range_n_max;

      if (max != PDB_COUNT_UNBOUNDED && max < setsize) {
        cl_log(cl, CL_LEVEL_VERBOSE,
               "gia_estimate_setsize: %zu (subiterator %s): "
               "lower overall setsize from %llu to range "
               "estimate %llu",
               i, pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf),
               setsize, max);
        setsize = max;
      }

      if (range.range_n_exact != PDB_COUNT_UNBOUNDED) {
        weight = 20;
        sub_n = range.range_n_exact;
      } else {
        if (pdb_iterator_n_valid(pdb, sc->sc_it)) {
          sub_n = pdb_iterator_n(pdb, sc->sc_it);
          weight = 10;
        } else {
          sub_n = max;
          weight = 2;
        }
      }
    } else if (pdb_iterator_n_valid(pdb, sc->sc_it)) {
      sub_n = pdb_iterator_n(pdb, sc->sc_it);
      weight = 10;
    } else {
      sub_n = pdb_primitive_n(pdb);
      weight = 1;
    }

    if (setsize > sub_n) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "gia_estimate_setsize: %zu (subiterator %s): "
             "would lower overall setsize from %llu to %llu",
             i, pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf),
             setsize, sub_n);
    }

    /*  Has this subiterator managed to actually produce anything
     *  at the input of the check pipeline?
     */
    if (!sc->sc_contest_ps.ps_run_produced_n) continue;

    /*  Has anything come out at the end of the
     *  check pipeline?
     */
    if (sc->sc_contest_id_n > 0) {
      /*  X : N = id_n : produced_N.
       */
      est = (sc->sc_contest_id_n * sub_n) / sc->sc_contest_ps.ps_run_produced_n;
      weight += sc->sc_contest_id_n;
    } else {
      /*  Assume we'll look for twice as long
       *  before we find anything.
       */
      est = sub_n / (sc->sc_contest_ps.ps_run_produced_n * 2);
    }

    cl_log(cl, CL_LEVEL_VERBOSE,
           "gia_estimate_setsize: %zu (%s): estimate %llu with "
           "weight %zu",
           i, pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf), est,
           weight);

    /*  Value our total estimate more highly if
     *  .. it's based on actual n, not a primitive size
     *  .. it's based on a larger sample size.
     */
    total_est += est * weight;
    total_est_n += weight;
  }

  if (total_est_n > 0) {
    total_est /= total_est_n;
    if (setsize > total_est) setsize = total_est;
  }

  ogia->gia_context_setsize_cached = true;
  return ogia->gia_context_setsize = setsize;
}

/*  How much would it cost to produce all the output IDs
 *  needed using <sc> as a producer?
 *
 *  We keep running the competition as long as there's an <sc>
 *  whose total cost estimate is lower than the total cost
 *  estimate of the winner.
 */
static pdb_budget sc_loser_total_cost_estimate(pdb_handle *pdb,
                                               pdb_iterator *it,
                                               graphd_subcondition *sc) {
  graphd_iterator_and *ogia = it->it_theory;
  cl_handle *const cl = ogia->gia_cl;
  unsigned long long n;

  /*  We've not produced anything yet?
   */
  if (sc->sc_contest_ps.ps_run_produced_n == 0) return 0;

  /*  Upper bound for the values produced by this iterator?
   */
  n = (pdb_iterator_n_valid(pdb, sc->sc_it)
           ? pdb_iterator_n(pdb, sc->sc_it)
           : pdb_iterator_spread(pdb, sc->sc_it));

  /*  The maximum cost of producing everything through this guy:
   *
   *  cost[AND] : cost-so-far[AND] = |n[producer]| : |n-so-far[producer]|
   */
  cl_assert(cl, sc->sc_contest_ps.ps_run_produced_n > 0);
  return 1 + (n * sc->sc_contest_cost) / sc->sc_contest_ps.ps_run_produced_n;
}

/*  How much would it cost to produce all the output IDs
 *  needed using <sc> as a producer?
 *
 *  We keep running the competition as long as there's an <sc>
 *  whose total cost estimate is lower than the total cost
 *  estimate of the winner.
 */
static pdb_budget sc_total_cost_estimate(pdb_handle *pdb, pdb_iterator *it,
                                         graphd_subcondition *sc,
                                         unsigned long long n_to_produce,
                                         bool low) {
  graphd_iterator_and *ogia = it->it_theory;
  cl_handle *const cl = ogia->gia_cl;
  unsigned long long n;

  /*  How much will it cost the <and> to produce up to <n_to_produce>
   *  elements using <sc> as a producer?
   */
  if (n_to_produce <= 0) n_to_produce = 1;

  n = (pdb_iterator_n_valid(pdb, sc->sc_it)
           ? pdb_iterator_n(pdb, sc->sc_it)
           : pdb_iterator_spread(pdb, sc->sc_it));

  if (n_to_produce > n) n_to_produce = n;

  if (low) {
    /*  Lower n_to_produce to the estimated "and" result
     *  set size for this producer.
     */
    unsigned long long n_prod, n_id;

    n_prod = sc->sc_contest_ps.ps_run_produced_n;
    n_id = sc->sc_contest_id_n;

    if (n_prod == 0)
      n_prod = n_id = 1;

    else if (n_id == 0) {
      /*  Assume that we're halfway
       *  through producing the first
       *  passable id.
       */
      n_id = 1;
      n_prod *= 2;
    }
    n = (n_id * n) / n_prod;
    if (n_to_produce > n) n_to_produce = n;
  }

  if (n_to_produce <= sc->sc_contest_id_n) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "sc_total_cost_estimate: produced: %llu into %zu at cost %lld; "
           "total estimate for %llu is %lld",
           sc->sc_contest_ps.ps_run_produced_n, sc->sc_contest_id_n,
           sc->sc_contest_cost, n_to_produce, sc->sc_contest_cost);

    cl_assert(cl, sc->sc_contest_cost > 0);
    return sc->sc_contest_cost;
  }

  if (sc->sc_contest_id_n > 0) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "sc_total_cost_estimate: produced: %llu into %zu at cost %lld; "
           "total estimate for %llu is %lld",
           sc->sc_contest_ps.ps_run_produced_n, sc->sc_contest_id_n,
           sc->sc_contest_cost, n_to_produce,
           (long long)((sc->sc_contest_cost * n_to_produce) /
                       sc->sc_contest_id_n));

    cl_assert(cl, sc->sc_contest_cost >= sc->sc_contest_id_n);

    return (sc->sc_contest_cost * n_to_produce) / sc->sc_contest_id_n;
  }

  /*  Estimate that it'll take us twice as long
   *  as our current budget to get the first
   *  primitive out, and about PDB_COST_PRIMITIVE to check it.
   */
  cl_log(cl, CL_LEVEL_VERBOSE,
         "sc_total_cost_estimate: produced: %llu at cost %lld; total "
         "%sestimate for %llu is %lld",
         sc->sc_contest_ps.ps_run_produced_n, sc->sc_contest_cost,
         low ? "low " : "", n_to_produce,
         ((low ? 2 : 1) * sc->sc_contest_cost + PDB_COST_PRIMITIVE) *
             n_to_produce);

  cl_assert(cl, sc->sc_contest_cost > 0);
  return ((low ? 2 : 1) * sc->sc_contest_cost + PDB_COST_PRIMITIVE) *
         n_to_produce;
}

/*  If this were to run, how much budget should we at most spend on it,
 *  given that another thread has won with a total estimate of
 *  <winning_cost> ?
 */
static pdb_budget sc_maximum_budget(pdb_handle *pdb, pdb_iterator *it,
                                    graphd_subcondition *sc,
                                    pdb_budget winning_cost,
                                    unsigned long long n_to_produce) {
  pdb_budget budget_for_5;

  cl_assert(pdb_log(pdb), winning_cost > 0);

  if (!sc->sc_compete || sc->sc_contest_cost >= winning_cost ||
      sc->sc_contest_id_n >= GRAPHD_AND_CONTEST_GOAL)

    return 0;

  if (n_to_produce < 5) n_to_produce = 5;

  /*  winning_cost     budget_for_5
   *  ------------  =  ------------
   *  n_to_produce          5
   */
  budget_for_5 = (winning_cost * 5) / n_to_produce;

  /*  If we've already exceeded our budget,
   *  we're not going to run.
   */
  if (budget_for_5 <= sc->sc_contest_cost) return 0;

  return budget_for_5 - sc->sc_contest_cost;
}

/*  Should we keep running?  If not, who won?
 */
static bool keep_running(pdb_handle *pdb, pdb_iterator *it,
                         unsigned long long pagesize,
                         unsigned long long setsize, size_t *winning_i_out,
                         pdb_budget *winning_cost_out,
                         size_t *competing_n_out) {
  size_t winning_i = 0, i, n_winning = 0, n_competing = 0;
  graphd_subcondition *sc;
  graphd_iterator_and *ogia = it->it_theory;
  cl_handle *cl = ogia->gia_cl;
  pdb_budget winning_cost, cost;

  winning_cost = -1;

  cl_log(cl, CL_LEVEL_VERBOSE, "keep_running: pagesize=%llu, setsize=%llu",
         pagesize, setsize);

  for (i = 0, sc = ogia->gia_sc; i < ogia->gia_n; i++, sc++) {
    if (!sc->sc_compete) continue;

    if (sc->sc_contest_id_n >= GRAPHD_AND_CONTEST_GOAL) {
      cost = sc_total_cost_estimate(
          pdb, it, sc, sc_is_usefully_sorted(pdb, it, sc) ? pagesize : setsize,
          ESTIMATE_HIGH);
      cl_assert(cl, cost > 0);

      if (winning_cost < 0 || cost < winning_cost) {
        winning_cost = cost;
        winning_i = i;

        cl_assert(cl, winning_cost > 0);
      }
      n_winning++;
    }
  }

  for (i = 0, sc = ogia->gia_sc; i < ogia->gia_n; i++, sc++) {
    if (!sc->sc_compete || sc->sc_contest_id_n >= GRAPHD_AND_CONTEST_GOAL)
      continue;

    if (winning_cost < 0)
      n_competing++;

    else if (sc_maximum_budget(
                 pdb, it, sc, winning_cost,
                 sc_may_be_usefully_sorted(pdb, it, sc) ? pagesize : setsize) >
             0)

      n_competing++;

    else {
      char buf[200];
      cl_log(cl, CL_LEVEL_VERBOSE,
             "keep_running: take subiterator #%zu (%s)"
             " out of the running - it has a zero "
             "maximum budget",
             i, pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
      /* Take this one out of the running. */
      sc->sc_compete = false;
    }
  }

  if (n_competing == 0) {
    /*  Final pass over the candidates who didn't win.
     *  For any of them, do we know enough to have them
     *  take over the winning slot?
     */
    for (i = 0, sc = ogia->gia_sc; i < ogia->gia_n; i++, sc++) {
      if (sc->sc_contest_id_n >= GRAPHD_AND_CONTEST_GOAL) continue;

      cost = sc_loser_total_cost_estimate(pdb, it, sc);

      /* We don't know enough?
       */
      if (cost <= 0) continue;

      if (winning_cost < 0 || cost < winning_cost) {
        winning_cost = cost;
        winning_i = i;

        cl_assert(cl, winning_cost > 0);
      }
    }
  }

  *competing_n_out = n_competing;
  *winning_cost_out = winning_cost;
  *winning_i_out = winning_i;

  cl_log(cl, CL_LEVEL_VERBOSE,
         "keep_running (d=%d;o=%s;%llu/%llu): %zu competing",
         ogia->gia_direction,
         pdb_iterator_ordering(pdb, it) ? pdb_iterator_ordering(pdb, it) : "*",
         pagesize, setsize, n_competing);

  if (winning_cost >= 0)
    cl_log(cl, CL_LEVEL_VERBOSE, "keep_running: winning so far: %zu at $%lld",
           winning_i, *winning_cost_out);

  /*  We need to keep going as long as nobody has won
   *  and someone's still competing,
   *  or as long as the winning cost exceeds the lowest
   *  still-competing estimate.
   */
  return n_competing > 0;
}

static int and_compare_costs(pdb_handle *pdb, pdb_iterator *a,
                             pdb_iterator *b) {
  double cost_a, cost_b;
  cl_handle *cl = pdb_log(pdb);

  PDB_IS_ITERATOR(cl, a);
  PDB_IS_ITERATOR(cl, b);

  /*  If any of a and b haven't even finished their
   *  statistics computation by now,
   *  they're expensive and move to the end.
   */
  if (!pdb_iterator_check_cost_valid(pdb, a) || !pdb_iterator_n_valid(pdb, a)) {
    if (pdb_iterator_check_cost_valid(pdb, b) && pdb_iterator_n_valid(pdb, b)) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "and_compare_costs: a is invalid, "
             "b is valid -> 1.");

      return 1;
    }
    cl_log(cl, CL_LEVEL_VERBOSE,
           "and_compare_costs: both are invalid "
           "-> sorted");
  } else if (!pdb_iterator_check_cost_valid(pdb, b) ||
             !pdb_iterator_n_valid(pdb, b)) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "and_compare_costs: a is valid, "
           "b is invalid -> -1.");
    return -1;
  } else {
    /* A and B are subcriterions that must be met for
     * a primitive to be accepted.
     *
     * Which is cheaper, testing first an and then b,
     * or testing first b and then a?
     *
     * Cost of testing "a" first:
     *
     * 	cost(a) + chance-of-matching(a) * cost(b)
     *
     * Cost of testing "b" first:
     *
     *      cost(b) + chance-of-matching(b) * cost(a);
     */
    char buf_a[200], buf_b[200];
    double check_chance_a, check_chance_b;
    unsigned long long total_n;

    total_n = pdb_primitive_n(pdb);
    if (total_n == 0) total_n = 1;

    check_chance_a = (double)pdb_iterator_n(pdb, a) / total_n;
    check_chance_b = (double)pdb_iterator_n(pdb, b) / total_n;

    cost_a = pdb_iterator_check_cost(pdb, a) +
             check_chance_a * pdb_iterator_check_cost(pdb, b);

    cost_b = pdb_iterator_check_cost(pdb, b) +
             check_chance_b * pdb_iterator_check_cost(pdb, a);

    cl_log(cl, CL_LEVEL_VERBOSE,
           "and_compare_costs: "
           "%s: Aco:%lld + Ach:%g * Bco:%lld = %.3lf,"
           " %s: Bco:%lld + Bch:%g * Aco:%lld = %.3lf",

           pdb_iterator_to_string(pdb, a, buf_a, sizeof buf_a),
           (long long)pdb_iterator_check_cost(pdb, a), check_chance_a,
           (long long)pdb_iterator_check_cost(pdb, b), cost_a,
           pdb_iterator_to_string(pdb, b, buf_b, sizeof buf_b),
           (long long)pdb_iterator_check_cost(pdb, b), check_chance_b,
           (long long)pdb_iterator_check_cost(pdb, a), cost_b);

    if (cost_a < cost_b) return -1;

    if (cost_a > cost_b) return 1;
  }

  /* Among two equally expensive, the sorted is cheaper.
   */
  if (!pdb_iterator_sorted(pdb, a) != !pdb_iterator_sorted(pdb, b))
    return pdb_iterator_sorted(pdb, a) ? -1 : 1;
  return 0;
}

/*
 *  Reorder subiterators.
 *
 *  ogia->gia_contest_order is listing producer alternative indexes,
 *  with diminishing belief in their cheapness.  (If there is a
 *  cheap producer, we want to get an offer from it early, so we
 *  can limit the budget spent on testing unknown producers.)
 *
 *  This allocates and updates that ogia->gia_contest_order array
 *  to align with iterators' statistic predictions about their
 *  next-cost, n, and check cost.
 *
 *  (It doesn't actually move iterators themselves around.)
 *
 *  The contest order is recalculated between rounds of
 *  graphd_iterator_and_statistics() only.
 */
static int and_contest_order_sort(pdb_iterator *it) {
  graphd_iterator_and *ogia = it->it_theory;
  pdb_handle *pdb = ogia->gia_pdb;
  cl_handle *cl = ogia->gia_cl;
  size_t i;
  bool any = false;
  int err;

  cl_assert(cl, it->it_original == it);
  if (ogia->gia_contest_order == NULL) {
    ogia->gia_contest_order =
        cm_malloc(ogia->gia_cm, sizeof(*ogia->gia_contest_order) * ogia->gia_n);
    if (ogia->gia_contest_order == NULL) {
      err = errno ? errno : ENOMEM;
      cl_log_errno(
          cl, CL_LEVEL_FAIL, "cm_malcpy", err, "%lu bytes",
          (unsigned long)(ogia->gia_n * sizeof(*ogia->gia_contest_order)));
      return err;
    }
    for (i = 0; i < ogia->gia_n; i++) ogia->gia_contest_order[i] = i;
  }

  for (i = 0; i < ogia->gia_n - 1; i++) {
    size_t j, tmp;
    char buf[200];

    if (and_compare_costs(pdb, ogia->gia_sc[ogia->gia_contest_order[i]].sc_it,
                          ogia->gia_sc[ogia->gia_contest_order[i + 1]].sc_it) <=
        0)

      continue;

    /*  i + 1 is cheaper than i.  It needs to
     *  trade places with i.
     */
    tmp = ogia->gia_contest_order[i + 1];
    ogia->gia_contest_order[i + 1] = ogia->gia_contest_order[i];
    ogia->gia_contest_order[i] = tmp;

    /*  It may also be cheaper than the records before
     *  i -- bubble it up until it's in the right place.
     */
    for (j = i; j > 0; j--) {
      if (and_compare_costs(
              pdb, ogia->gia_sc[ogia->gia_contest_order[j - 1]].sc_it,
              ogia->gia_sc[ogia->gia_contest_order[j]].sc_it) <= 0)
        break;

      tmp = ogia->gia_contest_order[j - 1];
      ogia->gia_contest_order[j - 1] = ogia->gia_contest_order[j];
      ogia->gia_contest_order[j] = tmp;
    }
    cl_log(ogia->gia_cl, CL_LEVEL_VERBOSE,
           "and_contest_order_sort: moved #%d:%s to spot #%d", (int)i + 1,
           pdb_iterator_to_string(
               pdb, ogia->gia_sc[ogia->gia_contest_order[j]].sc_it, buf,
               sizeof buf),
           (int)j);
    any = true;
  }
  if (any)
    if (it->it_displayname != NULL) {
      cm_free(ogia->gia_cm, it->it_displayname);
      it->it_displayname = NULL;
    }
  return 0;
}

/*  Calculate the cost and chance for the whole ANDed expression,
 *  based on its members.
 *
 *  The total cost of a check all the way through is, for each
 *  check, the cost of all the false starts, plus the cost of
 *  the final check.
 */
pdb_budget graphd_iterator_and_calculate_check_cost(
    pdb_iterator const *const it, graphd_iterator_and const *const ogia) {
  pdb_handle *pdb = ogia->gia_pdb;
  cl_handle *cl = ogia->gia_cl;
  double check_prob = 1.0;
  pdb_budget check_cost = 0;
  size_t i;
  unsigned long long total_n;

  total_n = pdb_iterator_spread(pdb, it);
  if (total_n == 0) return 0;

  cl_assert(cl, ogia->gia_sc != NULL);
  for (i = 0; i < ogia->gia_n; i++) {
    pdb_iterator *c_it;
    pdb_budget cc;
    double prob;
    long long n_claim;

    c_it = ogia->gia_sc[ogia->gia_check_order[i]].sc_it;

    /* If we don't know, assume that it's slow and inefficient.
     */
    cc = pdb_iterator_check_cost_valid(pdb, c_it)
             ? pdb_iterator_check_cost(pdb, c_it)
             : UNKNOWN_CHECK_COST;

    if (pdb_iterator_n_valid(pdb, c_it)) {
      /*  This subiterator claims to return more
       *  results than we know we can possibly return.
       *  Clearly, it's off in its own little world.
       *  Measure it against that.
       */
      n_claim = pdb_iterator_n(pdb, c_it);
      prob = (double)pdb_iterator_n(pdb, c_it) / pdb_iterator_spread(pdb, c_it);
    } else
      prob = UNKNOWN_CHECK_CHANCE;

    if (cc <= 0) cc = 1;
    if (prob <= 0.0)
      prob = 0.000001;
    else if (prob >= 1.0)
      prob = 0.999999;

    check_cost += 1 + cc * check_prob;
    check_prob *= prob;

    {
      char buf[200];
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_and_calculate_check_cost: %s has cost %lld, prob "
             "%.6lf (total after: %lld, %.6lf) [total=%llu, spread=%llu, "
             "n=%llu]",
             pdb_iterator_to_string(pdb, c_it, buf, sizeof buf), (long long)cc,
             check_prob, (long long)check_cost, check_prob,
             (unsigned long long)total_n, pdb_iterator_spread(pdb, c_it),
             (unsigned long long)pdb_iterator_n(pdb, c_it));
    }
  }
  return check_cost;
}

static int and_iterator_statistics_redirect(pdb_handle *pdb, pdb_iterator *it,
                                            pdb_budget *budget_inout,
                                            pdb_id *ar, size_t ar_n) {
  graphd_iterator_and *ogia = it->it_theory;
  cl_handle *cl = ogia->gia_cl;
  int err;
  pdb_iterator *new_it;

  PDB_IS_ITERATOR(cl, it);

  if (ar_n == 0) {
    err = pdb_iterator_null_become(pdb, it);
    PDB_IS_ITERATOR(cl, it);
    return err;
  }

  err =
      graphd_iterator_fixed_create_array(ogia->gia_graphd, ar, ar_n, it->it_low,
                                         it->it_high, it->it_forward, &new_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_fixed_create_array", err,
                 "failed to create fixed array");
    return err;
  }
  PDB_IS_ITERATOR(cl, new_it);

  err = graphd_iterator_substitute(ogia->gia_greq, it, new_it);
  cl_assert(cl, err == 0);
  PDB_IS_ITERATOR(cl, it);

  return pdb_iterator_statistics(pdb, it, budget_inout);
}

static void and_iterator_statistics_prepare(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_and *ogia = it->it_theory;
  graphd_subcondition *sc;
  size_t i, best_i = ogia->gia_n, n_easy = 0;
  unsigned long long best_n = (unsigned long long)-1;

  for (i = 0, sc = ogia->gia_sc; i < ogia->gia_n; i++, sc++) {
    sc->sc_compete = true;

    /*  If there is a producer hint, and this isn't the
     *  producer in that hint, it doesn't get to run.
     */
    if (ogia->gia_producer_hint != -1 && i != ogia->gia_producer_hint) {
      char buf[200];
      cl_log(ogia->gia_cl, CL_LEVEL_VERBOSE,
             "and_iterator_statistics_prepare: "
             "take subiterator #%zu (%s) out of "
             "the running - there is a producer "
             "hint, and it is not it",
             i, pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
      sc->sc_compete = false;
      continue;
    }

    if (!IS_EASY_FAST_ITERATOR(ogia->gia_pdb, sc->sc_it)) {
      /*  Iterators that aren't easy - that is,
       *  transparent, tractable, sorted, findable
       *  - may turn out to be more efficient than
       *  tractable ones; so they always have to at
       *  least _try_ to run.
       */
      sc->sc_compete = true;
      continue;
    }

    n_easy++;

    /*  Of all EASY_FAST_ITERATORs that start out easy,
     *  only one - the one with the fewest results -
     *  particpates in the statistics contest.
     *
     *  All others just support the first one,
     *  moved to early positions in the iterator check
     *  order, hopefully using find() to skip more than
     *  just a single entry.
     */
    if (pdb_iterator_n(pdb, sc->sc_it) < best_n) {
      /*  We found a smaller competitor.  Remove
       *  the previous best choice from the competition,
       *  and mark this one as competing.
       */
      if (best_i < ogia->gia_n) {
        char buf[200];
        cl_log(ogia->gia_cl, CL_LEVEL_VERBOSE,
               "and_iterator_statistics_prepare: "
               "take subiterator #%zu (%s)"
               " out of the running - there is a "
               "smaller (%llu vs. %llu),  "
               "easier producer",
               best_i, pdb_iterator_to_string(pdb, ogia->gia_sc[best_i].sc_it,
                                              buf, sizeof buf),
               best_n, (unsigned long long)pdb_iterator_n(pdb, sc->sc_it));

        ogia->gia_sc[best_i].sc_compete = false;
      }
      sc->sc_compete = true;

      best_i = i;
      best_n = pdb_iterator_n(pdb, sc->sc_it);

      continue;
    }

    if (sc->sc_contest_id_n < GRAPHD_AND_CONTEST_GOAL) {
      /*  This one is too big and hasn't finished yet.
       *  Remove it from the producer competition
       *  (it'll still work as a checker).
       */
      char buf[200];

      sc->sc_compete = false;
      cl_log(ogia->gia_cl, CL_LEVEL_VERBOSE,
             "and_iterator_statistics_prepare: "
             "take subiterator #%zu (%s)"
             "out of the running - there is a smaller "
             "(%llu vs. %llu), easier producer",
             i, pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf),
             (unsigned long long)pdb_iterator_n(pdb, sc->sc_it), best_n);
    }
  }
  cl_log(ogia->gia_cl, CL_LEVEL_VERBOSE,
         "and_iterator_statistics_prepare: %zu easy", n_easy);
}

static void and_iterator_statistics_complete(pdb_iterator *it) {
  graphd_iterator_and *ogia = it->it_theory;
  graphd_subcondition *sc;
  size_t i;

  cl_assert(ogia->gia_cl, it == it->it_original);
  cl_enter(ogia->gia_cl, CL_LEVEL_VERBOSE, "it=%p", (void *)it);

  for (i = 0, sc = ogia->gia_sc; i < ogia->gia_n; i++, sc++) {
    if (i == ogia->gia_producer) {
      /*  Move the contest iterator into the
       *  cache producer slot.
       */
      ogia->gia_cache_ps = sc->sc_contest_ps;
      graphd_iterator_and_process_state_clear(&sc->sc_contest_ps);

      cl_log(ogia->gia_cl, CL_LEVEL_VERBOSE,
             "and_iterator_statistics_complete: "
             "moved contest ps into cache ps. "
             "Self producer is: %p",
             ogia->gia_cache_ps.ps_it[ogia->gia_producer]);
    } else {
      /* Loser.
       */
      graphd_iterator_and_process_state_finish(ogia, &sc->sc_contest_ps);
    }
  }

  /*  Free the ordering information used to check produced candidates.
   */
  if (ogia->gia_contest_order != NULL) {
    cm_free(ogia->gia_cm, ogia->gia_contest_order);
    ogia->gia_contest_order = NULL;
  }

  cl_leave(ogia->gia_cl, CL_LEVEL_VERBOSE, "%p", (void *)it);
}

static int and_iterator_statistics_work(pdb_handle *pdb, pdb_iterator *it,
                                        size_t producer,
                                        pdb_budget *budget_inout) {
  graphd_iterator_and *ogia = it->it_theory;
  cl_handle *cl = ogia->gia_cl;
  graphd_subcondition *sc = ogia->gia_sc + producer;
  pdb_iterator *p_it;
  pdb_budget budget_in = *budget_inout;
  int err;
  char buf[200];

  cl_enter(cl, CL_LEVEL_VERBOSE,
           "producer #%d "
           "(contest_state=%d, $%lld) %p@%s",
           (int)producer, (int)sc->sc_contest_state, (long long)*budget_inout,
           (void *)sc->sc_it,
           pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
  cl_assert(cl, it == it->it_original);

  switch (sc->sc_contest_state) {
    default:
      cl_notreached(cl, "unexpected contest state %d", sc->sc_contest_state);
    case 0:
      if (!pdb_iterator_statistics_done(pdb, sc->sc_it)) {
        /* Get the statistics for our producer.
         */
        case 1:
          err = pdb_iterator_statistics(pdb, sc->sc_it, budget_inout);
          if (err == PDB_ERR_MORE) {
            sc->sc_contest_state = 1;
            goto suspend;
          } else if (err != 0) {
            cl_log_errno(
                cl, CL_LEVEL_FAIL, "pdb_iterator_statistics", err,
                "testing producer #%d %s", (int)producer,
                pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
            cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
            return err;
          }

          /*  Refresh the sc->sc_it pointer.
           */
          err = pdb_iterator_refresh_pointer(pdb, &sc->sc_it);
          if (err == 0) {
            it->it_id = pdb_iterator_new_id(ogia->gia_pdb);
            cl_log(cl, CL_LEVEL_VERBOSE,
                   "and_iterator_statistics_work: "
                   "got new id %llu to indicate refreshed "
                   "subiterator pointer #%d",
                   (unsigned long long)it->it_id, (int)producer);
          } else if (err != PDB_ERR_ALREADY) {
            cl_log_errno(
                cl, CL_LEVEL_FAIL, "pdb_iterator_refresh", err,
                "producer #%d=%s", (int)producer,
                pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
            cl_leave_err(cl, CL_LEVEL_VERBOSE, err,
                         "error from "
                         "pdb_iterator_refresh_pointer");
            return err;
          }

          /*  We just may have learned something new about one of
           *  our subiterators.
           */
          gia_invalidate_cached_setsize(ogia);
          gia_invalidate_sort(ogia);
      }

      /*  Clone this producer and its neighbors into
       *  the contest state.
       */
      err = graphd_iterator_and_process_state_initialize(pdb, it,
                                                         &sc->sc_contest_ps);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL,
                     "graphd_iterator_and_process_state_initialize", err,
                     "producer: %s",
                     pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
        cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
        return err;
      }

      /*  Reset the producer-to-be.  Even if this
       *  is a descendant of a cursor, we still want the
       *  statistics as calculated from the beginning.
       */
      err = pdb_iterator_reset(pdb, sc->sc_contest_ps.ps_it[producer]);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_reset", err,
                     "producer: %s",
                     pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
        cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
        return err;
      }

      /*  Have this producer produce, and have its checkers
       *  check, until they've made five items together that
       *  have passed all checks.
       */
      cl_assert(cl, sc->sc_contest_ps.ps_it != NULL);
      p_it = sc->sc_contest_ps.ps_it[producer];
      cl_assert(cl, pdb_iterator_has_position(pdb, p_it));

      for (;;) {
        pdb_iterator_call_reset(pdb, p_it);
        sc->sc_contest_ps.ps_run_call_state = 0;
        case 2:
          sc->sc_contest_state = 0;

          /*  If we're already above the contest goal,
           *  we're just wasting our time until everybody
           *  else has caught up and we get a non-negative
           *  budget.
           */
          if (sc->sc_contest_id_n >= GRAPHD_AND_CONTEST_GOAL) {
            err = (*budget_inout < 0) ? PDB_ERR_MORE : 0;
            break;
          }

          /*  Produce an item and run it past the checkers.
           */
          err = graphd_iterator_and_run(it, producer, &sc->sc_contest_ps,
                                        budget_inout);
          if (err != 0) {
            if (err == PDB_ERR_MORE) {
              sc->sc_contest_state = 2;
              goto suspend;
            } else if (err == GRAPHD_ERR_NO) {
              /*  The caller will turn the "and"
               *  iterator into a NULL or fixed
               *  iterator - the whole iterator
               *  doesn't contain more than five
               *  items!
               */
              pdb_rxs_log(pdb, "%p and[%zu] done; found all %zu", (void *)it,
                          producer, (size_t)sc->sc_contest_id_n);

              cl_leave(cl, CL_LEVEL_VERBOSE, "done ($%lld)",
                       (long long)(budget_in - *budget_inout));
              return err;
            }

            cl_log_errno(
                cl, CL_LEVEL_FAIL, "pdb_iterator_statistics", err,
                "testing producer #%d %s", (int)producer,
                pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
            cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
            return err;
          }

          /*  Store the ID we produced.
           */
          sc->sc_contest_id[sc->sc_contest_id_n] = sc->sc_contest_ps.ps_id;
          sc->sc_contest_id_n++;

          pdb_rxs_log(pdb, "STAT-%zu %p and result[%zu of %d] is %llx",
                      producer, (void *)it, sc->sc_contest_id_n,
                      GRAPHD_AND_CONTEST_GOAL,
                      (unsigned long long)sc->sc_contest_ps.ps_id);

          if (sc->sc_contest_id_n >= GRAPHD_AND_CONTEST_GOAL) break;

          /*  Running out of time?
           */
          if (*budget_inout <= 0) {
            sc->sc_contest_state = 3;
            goto suspend;
          }
        case 3:
          p_it = sc->sc_contest_ps.ps_it[producer];
      }
  }

  /*  Done.  This team has produced and checked 5 items.
   */
  cl_assert(cl, *budget_inout <= budget_in);
  sc->sc_contest_cost += budget_in - *budget_inout;
  cl_assert(cl, sc->sc_contest_cost >= 0);

  cl_leave(cl, CL_LEVEL_VERBOSE, "got all %d for $%lld.",
           GRAPHD_AND_CONTEST_GOAL, (long long)sc->sc_contest_cost);
  return 0;

suspend:
  cl_assert(cl, *budget_inout <= budget_in);
  cl_assert(cl, sc->sc_contest_cost >= 0);
  sc->sc_contest_cost += budget_in - *budget_inout;
  cl_assert(cl, sc->sc_contest_cost >= 0);

  cl_leave(cl, CL_LEVEL_VERBOSE,
           "suspend; contest_state=%d; got %zu of %d ($%lld; total $%lld)",
           sc->sc_contest_state, sc->sc_contest_id_n, GRAPHD_AND_CONTEST_GOAL,
           (long long)(budget_in - *budget_inout),
           (long long)sc->sc_contest_cost);
  return PDB_ERR_MORE;
}

/*  Return -1 if no total is available, otherwise the
 *  smallest available total instance count from any of
 *  the subiterators.
 */
static long long and_iterator_sub_n(pdb_handle *pdb, pdb_iterator *it) {
  long long best_n = -1;
  graphd_iterator_and *ogia = it->it_theory;
  graphd_subcondition const *sc = ogia->gia_sc;
  size_t i;

  for (i = ogia->gia_n; i-- > 0; sc++)

    if (pdb_iterator_n_valid(pdb, sc->sc_it) &&
        (best_n == -1 || pdb_iterator_n(pdb, sc->sc_it) < best_n)) {
      char buf[200];

      best_n = pdb_iterator_n(pdb, sc->sc_it);
      cl_log(ogia->gia_cl, CL_LEVEL_VERBOSE, "and_iterator_sub_n: %lld from %s",
             best_n, pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
      cl_assert(ogia->gia_cl, best_n >= 0);
    }

  return best_n;
}

/*  A subiterator has produced its 5th result.
 */
static void sc_completed_run(pdb_handle *pdb, pdb_iterator *it,
                             graphd_subcondition *sc) {
  graphd_iterator_and *ogia = it->it_theory;
  cl_handle *cl = ogia->gia_cl;
  unsigned long long est_n, sub_n, sibling_n;
  pdb_budget primitive_cost;
  char buf[200];

  if (!sc->sc_compete) return;

  /* We already did the statistics on this guy.
   */
  cl_assert(cl, pdb_iterator_n_valid(pdb, sc->sc_it));

  /*  Invalidate the "setsize" cache - our guess may change.
   */
  gia_invalidate_cached_setsize(ogia);

  /*  This contestant just completed its run.
   *  It may have been lucky.
   *
   *  We can tell that it's lucky if its result set is
   *  much larger than that of the other filters. (In
   *  other words, all the overlap between it and its
   *  neighbors happened at the beginning.)
   *
   *  If we were to exhaust this producer P, we would have
   *  to produce and throw out P.n - max(sub.N) primitives
   *  to eventually yield max(sub.N) or fewer primitives.
   *  We can guess how much that costs based on
   *  the production cost and the ps_run_produced_n count.
   */

  sub_n = pdb_iterator_n(pdb, sc->sc_it);
  sibling_n = and_iterator_sub_n(pdb, it);

  /*  est_n: if the producer keeps producing with the
   *  same success rate it displayed so far, how many
   *  results will we end up with?
   */
  est_n =
      (GRAPHD_AND_CONTEST_GOAL * sub_n) / sc->sc_contest_ps.ps_run_produced_n;
  if (est_n < sc->sc_contest_ps.ps_run_produced_n)
    est_n = sc->sc_contest_ps.ps_run_produced_n;

  /*  sibling_n: how many entries do the other records
   *  know about?
   */
  if (sibling_n > 0 && sibling_n < GRAPHD_AND_CONTEST_GOAL) {
    cl_log(cl, CL_LEVEL_DEBUG,
           "graphd_iterator_and_statistics: sibling "
           "iterator predicts only %lld results?",
           sibling_n);
    sibling_n = GRAPHD_AND_CONTEST_GOAL;
  }
  if (sibling_n <= 0 || est_n < sibling_n) return;

  cl_assert(cl, sc->sc_contest_ps.ps_run_produced_n >= GRAPHD_AND_CONTEST_GOAL);

  primitive_cost = sc->sc_contest_cost / sc->sc_contest_ps.ps_run_produced_n;
  if (primitive_cost <= 0) primitive_cost = 1;

  cl_log(cl, CL_LEVEL_VERBOSE,
         "sc_completed_run: "
         "iterator %s won $%lld per %llu-%llu=%llu "
         "primitive(s); total adjustment $%lld",
         pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf),
         (long long)primitive_cost, (unsigned long long)est_n,
         (unsigned long long)sibling_n, (unsigned long long)(est_n - sibling_n),
         (long long)(((double)(est_n - sibling_n) * primitive_cost) *
                     sc->sc_contest_id_n / sibling_n));

  cl_assert(cl, sibling_n > 0);
  sc->sc_contest_cost += ((double)(est_n - sibling_n) * primitive_cost) *
                         sc->sc_contest_id_n / sibling_n;
  cl_assert(cl, sc->sc_contest_cost >= 0);
}

int graphd_iterator_and_statistics(pdb_handle *const pdb,
                                   pdb_iterator *const it,
                                   pdb_budget *const budget_inout) {
  unsigned long long upper_bound;
  long long sub_n;
  long long est_n;
  graphd_iterator_and *ogia = it->it_theory;
  cl_handle *cl = ogia->gia_cl;
  graphd_subcondition *sc;
  pdb_budget turn_budget, next_cost, budget_in = *budget_inout;
  size_t winning_i;
  size_t n_players = 0;
  int err;
  size_t i, k;
  char buf[200];
  unsigned long long setsize, pagesize;
  pdb_budget winning_cost;
  pdb_budget budget_effective;
  bool any;
  bool cache_contest_results = true;
  pdb_iterator *sort_it;

  if (GRAPHD_SABOTAGE(ogia->gia_graphd, *budget_inout <= 0))
    return PDB_ERR_MORE;

  pdb_rxs_push(pdb, "STAT %p and", (void *)it);

  /* The caller must have called graphd_iterator_and_commit()
   * after graphd_iterator_and_create().
   */
  cl_assert(cl, ogia->gia_committed);

  /*  The pdb_iterator macros make sure that this is only
   *  ever invoked on the original.
   */
  cl_assert(cl, it == it->it_original);
  cl_assert(cl, ogia->gia_n > 1);

  /*  1   If we don't have enough budget, save up for some.
   *  ====================================================
   */

  /*  If we don't have enough budget to run a contest
   *  round, save more until we have.
   */
  if (*budget_inout < ogia->gia_contest_to_save) {
    ogia->gia_contest_to_save -= *budget_inout;
    *budget_inout = 0;

    goto suspend;
  }

  /*  The effective budget is the passed-in budget
   *  plus what we saved.  By default, we've not saved
   *  anything.
   *
   *  We account for this budget separately (rather
   *  than just adding into *budget_inout) to avoid
   *  returning saved $$$ to the caller - usually, negative
   *  cost is a sign that something is very wrong and is
   *  okay to assertion-fail against.
   */
  budget_effective = *budget_inout;
  if (ogia->gia_contest_to_save > 0) {
    /*  We saved up for ogia->gia_n;
     *  add them now to our budget.
     */
    *budget_inout -= ogia->gia_contest_to_save;
    ogia->gia_contest_to_save = 0;

    /* We have more budget than what was
     * passed in because we saved up for it.
     */
    budget_effective = *budget_inout + ogia->gia_n;
  } else if (*budget_inout < ogia->gia_n) {
    ogia->gia_contest_to_save = ogia->gia_n - *budget_inout;
    *budget_inout = 0;
    goto suspend;
  }

/*  2  Resort; determine upper boundaries, whether or
 *     not we're still running, and how long.
 *  ====================================================
 */

rerun:
  if (ogia->gia_contest_order == NULL || ogia->gia_resort) {
    ogia->gia_resort = false;
    if (ogia->gia_contest_order == NULL)
      and_iterator_statistics_prepare(pdb, it);

    /* Update the centrally managed "check" and "most
     * promising producer" orders.
     */
    if ((err = graphd_iterator_and_check_sort(it)) ||
        (err = and_contest_order_sort(it)))
      goto err;
  }

  /* If this had just a single entry, we wouldn't be an AND,
   *  we'd just be that entry.
   */
  cl_assert(cl, ogia->gia_n > 1);

  pagesize = setsize = gia_estimate_setsize(it, ogia);
  if (ogia->gia_context_pagesize_valid && ogia->gia_context_pagesize < pagesize)
    pagesize = ogia->gia_context_pagesize;

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_and_statistics: pagesize %llu, setsize %llu",
         pagesize, setsize);

  if (!keep_running(pdb, it, pagesize, setsize, &winning_i, &winning_cost,
                    &n_players))
    goto done;

  if (budget_effective < 0 || (turn_budget = budget_effective / n_players) <= 0)
    turn_budget = 1;
  if (turn_budget > ogia->gia_contest_max_turn) {
    cl_assert(cl, ogia->gia_contest_max_turn > 0);
    turn_budget = ogia->gia_contest_max_turn;

    /*  Increment the maximum per-turn budget for next time,
     *  up to a total of 10,000.  We do that to not spend a lot
     *  of time in bad branches when a small amount of time in
     *  a good branch would solve the whole thing.
     */
    cl_assert(cl, ogia->gia_contest_max_turn > 0);
    if (ogia->gia_contest_max_turn < 10000) ogia->gia_contest_max_turn *= 10;
  }
  cl_assert(cl, turn_budget > 0);

  any = false;
  for (k = 0; k < ogia->gia_n; k++) {
    pdb_budget sc_budget, sc_budget_before;

    i = ogia->gia_contest_order[k];
    sc = ogia->gia_sc + i;

    if (!sc->sc_compete || sc->sc_contest_id_n >= GRAPHD_AND_CONTEST_GOAL) {
      if (!pdb_iterator_n_valid(pdb, sc->sc_it)) {
        err = pdb_iterator_statistics(pdb, sc->sc_it, budget_inout);
        if (err != 0) return err;

        sc_completed_run(pdb, it, sc);
      }
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_and_statistics: "
             "skip k=%zu i=%zu; %s",
             k, i, sc->sc_compete ? "already successful" : "not competing");

      continue;
    }

    sc_budget = turn_budget;
    if (winning_cost >= 0) {
      pdb_budget max_budget = sc_maximum_budget(
          pdb, it, sc, winning_cost,
          sc_may_be_usefully_sorted(pdb, it, sc) ? pagesize : setsize);

      /*  Normally, if we didn't have a sc_maximum_budget,
       *  the last round of keep_running would have
       *  marked us as no longer competing.
       *
       *  This may break down if a subiterator's sortedness
       *  suddenly becomes well-defined as absent,
       *  and the resulting iterator with its new "setsize"
       *  production limit no longer rates.
       */
      if (max_budget == 0) {
        cl_assert(cl, !sc_may_be_usefully_sorted(pdb, it, sc));
        sc->sc_compete = false;

        continue;
      }
      cl_assert(cl, max_budget > 0);

      if (sc_budget > max_budget) sc_budget = max_budget;
    }

    cl_assert(cl, sc_budget > 0);

    /*  Run this competitor.
     */
    any = true;
    sc_budget_before = sc_budget;

    pdb_rxs_push(pdb, "STAT-%zu %p and", i, (void *)it);
    err = and_iterator_statistics_work(pdb, it, i, &sc_budget);
    pdb_rxs_pop(pdb, "STAT-%zu %p and %s($%lld)", i, (void *)it,
                err == GRAPHD_ERR_NO ? "done "
                                     : (err == PDB_ERR_MORE ? "suspend " : ""),
                (long long)(sc_budget_before - sc_budget));

    /* Take out of the budget what was consumed.
     */
    cl_assert(cl, sc_budget_before >= sc_budget);
    budget_effective -= (sc_budget_before - sc_budget);

    /* Update the budget for the following return()s.
     * Specifically, if we used more than we saved,
     * charge the overuse.
     */
    if (budget_effective < *budget_inout) *budget_inout = budget_effective;

    if (err == PDB_ERR_MORE) {
      cl_assert(cl, sc->sc_contest_id_n < GRAPHD_AND_CONTEST_GOAL);
      continue;
    } else if (err == GRAPHD_ERR_NO) {
      cl_assert(cl, budget_in >= *budget_inout);
      pdb_rxs_pop(pdb, "STAT %p and redirect ($%lld)", (void *)it,
                  (long long)(budget_in - *budget_inout));

      return and_iterator_statistics_redirect(
          pdb, it, budget_inout, sc->sc_contest_id, sc->sc_contest_id_n);
    } else if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "and_iterator_statistics_work", err,
                   "unexpected error from producer #%d %s", (int)i,
                   pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
      cl_assert(cl, budget_in >= *budget_inout);
      goto err;
    }

    cl_assert(cl, sc->sc_contest_cost >= 0);
    if (sc->sc_contest_id_n >= GRAPHD_AND_CONTEST_GOAL) {
      if (!pdb_iterator_n_valid(pdb, sc->sc_it)) {
        err = pdb_iterator_statistics(pdb, sc->sc_it, budget_inout);
        if (err != 0) return err;
      }
      sc_completed_run(pdb, it, sc);
    }
  }

  /*  Do we have at least a budget's worth left?
   */
  if (any && turn_budget > 0 && budget_effective >= turn_budget) goto rerun;

  /*  Check whether to keep running, even if we don't
   *  have the budget.  If no, we finish without first
   *  returning PDB_ERR_MORE.
   */
  if (keep_running(pdb, it, pagesize, setsize, &winning_i, &winning_cost,
                   &n_players))
    goto suspend;

done:
  /*  No suspends after this line. ==========================
   */

  /*  If we overused budget, charge the overuse.
   *
   *  There are no more uses of either budget_effective
   *  or budget_inout in this function below this line.
   */
  if (budget_effective < *budget_inout) *budget_inout = budget_effective;

  ogia->gia_total_cost_statistics += budget_in - *budget_inout;

  cl_assert(cl, winning_i < ogia->gia_n);
  sc = ogia->gia_sc + (ogia->gia_producer = winning_i);

  /*  Estimate the number of total elements in the iterator
   *  based on the contest winner.  We'll use that a few
   *  paragraphs further down.
   */
  sub_n = pdb_iterator_n(pdb, sc->sc_it);
  if (sub_n < sc->sc_contest_ps.ps_run_produced_n)
    sub_n = sc->sc_contest_ps.ps_run_produced_n;

  /*  If the winner isn't sorted, but the caller wants us
   *  to be sorted (and there's a pagesize), wrap the
   *  producer into a sort and go be sorted anyway.
   *
   *  Unless the producer is very large, in which case
   *  we can sort the results of the and, not the input
   *  from the producer.  But that's really only if the
   *  producer is very large, in which case, how did we
   *  get here anyway? The "and" should have won.
   *
   *  So, let's just wrap the guy and be done with it.
   */
  if ((ogia->gia_direction == GRAPHD_DIRECTION_BACKWARD ||
       ogia->gia_direction == GRAPHD_DIRECTION_FORWARD) &&
      !pdb_iterator_sorted(pdb, sc->sc_it)) {
    err = pdb_iterator_reset(pdb, sc->sc_it);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_reset", err,
                   "for subiterator %s",
                   pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
      return err;
    }

    err = graphd_iterator_sort_create(
        ogia->gia_greq, ogia->gia_direction != GRAPHD_DIRECTION_BACKWARD,
        &sc->sc_it, &sort_it);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_sort_create", err,
                   "for subiterator %s",
                   pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));

      return err;
    }
    sc->sc_it = sort_it;

    /*  Swap the iterator in the contest process
     *  state for a clone of sort_it; it will be
     *  used to refill our cache.
     */
    pdb_iterator_destroy(pdb, sc->sc_contest_ps.ps_it + ogia->gia_producer);
    err = pdb_iterator_clone(pdb, sort_it,
                             sc->sc_contest_ps.ps_it + ogia->gia_producer);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err,
                   "sort iterator %s",
                   pdb_iterator_to_string(pdb, sort_it, buf, sizeof buf));
      return err;
    }

    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_and_statistics: "
           "cloned sort iterator into producer's self-producer, %p",
           sc->sc_contest_ps.ps_it[ogia->gia_producer]);

    /*  What we just did means that we can't
     *  use the cached results to start out with -
     *  those weren't sorted.
     */
    cache_contest_results = false;
  }

  /*  est_n : sub_n = GRAPHD_AND_CONTEST_GOAL : run_produced_n
   */
  cl_assert(cl, sc->sc_contest_ps.ps_run_produced_n > 0);
  est_n =
      (GRAPHD_AND_CONTEST_GOAL * sub_n) / sc->sc_contest_ps.ps_run_produced_n;

  /* Limit est_n to the minimum of any subiterator's count.
   */
  for (i = 0; i < ogia->gia_n; i++) {
    unsigned long long x = 0;
    if (!ogia->gia_sc[i].sc_compete) continue;

    if (pdb_iterator_n_valid(pdb, ogia->gia_sc[i].sc_it) &&
        (x = pdb_iterator_n(pdb, ogia->gia_sc[i].sc_it)) < est_n &&
        x >= GRAPHD_AND_CONTEST_GOAL)
      est_n = x;
  }
  if (est_n !=
      (GRAPHD_AND_CONTEST_GOAL * sub_n) / sc->sc_contest_ps.ps_run_produced_n)
    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_and_statistics: "
           "lowered estimate to %llu",
           (unsigned long long)est_n);

  upper_bound = pdb_primitive_n(pdb);
  cl_assert(cl, upper_bound != 0);

  if (est_n > upper_bound) est_n = upper_bound;

  if (sc->sc_contest_id_n > 0) {
    next_cost = sc->sc_contest_cost / sc->sc_contest_id_n;
    if (next_cost == 0) next_cost = 1;
  } else if (est_n > 0)
    next_cost = winning_cost / est_n;
  else
    next_cost = winning_cost;

  if (next_cost == 0) next_cost = 1;

  /*  Pre-load our cache with the result of the producer's
   *  test run.
   */
  else if (cache_contest_results) {
    cl_assert(cl, graphd_iterator_cache_n(ogia->gia_cache) == 0);
    for (i = 0; i < sc->sc_contest_id_n; i++) {
      err = graphd_iterator_cache_add(ogia->gia_cache, sc->sc_contest_id[i],
                                      next_cost);
      if (err != 0) {
        cl_assert(cl, err != PDB_ERR_MORE);
        goto err;
      }
    }
  }

  /*  Now that that's settled, derive the statistics variables.
   */
  pdb_iterator_ordered_set(pdb, it, pdb_iterator_ordered(pdb, sc->sc_it));
  if (pdb_iterator_ordered(pdb, it))
    pdb_iterator_ordering_set(pdb, it, pdb_iterator_ordering(pdb, sc->sc_it));
  pdb_iterator_sorted_set(pdb, it, pdb_iterator_sorted(pdb, sc->sc_it));
  pdb_iterator_next_cost_set(pdb, it, next_cost);
  pdb_iterator_check_cost_set(
      pdb, it, graphd_iterator_and_calculate_check_cost(it, ogia));
  pdb_iterator_find_cost_set(pdb, it, pdb_iterator_find_cost(pdb, sc->sc_it) +
                                          pdb_iterator_next_cost(pdb, it));
  pdb_iterator_n_set(pdb, it, est_n);
  pdb_iterator_statistics_done_set(pdb, it);

  /*  Free non-producer resources.
   */
  and_iterator_statistics_complete(it);

  /*  If, at the end of the statistics round,
   *  we have picked a producer that is *not* an "all",
   *  then we can remove any "all" from the set.
   *  If that drops us to a single iterator, we can
   *  replace the "and" with that single iterator.
   */
  err = graphd_iterator_and_evolve(pdb, it);
  if (err != 0 && err != GRAPHD_ERR_ALREADY) {
    cl_assert(cl, err != PDB_ERR_MORE);
    goto err;
  }

  /*  At this point, "it" may or may not be an "and"-iterator.
   *  "and_delete_spurious_all" may have replaced the and-iterator
   *  with one of its subiterators.
   */

  /* The producer is part of the displayname -
   * invalidate the cache.
   */
  if (it->it_displayname != NULL) {
    cm_handle *cm = pdb_mem(pdb);

    cm_free(cm, it->it_displayname);
    it->it_displayname = NULL;
  }

  if (!pdb_iterator_statistics_done(pdb, it)) {
    pdb_rxs_pop(pdb, "STAT %p and: redirect to %s ($%lld)", (void *)it,
                pdb_iterator_to_string(pdb, it, buf, sizeof buf),
                *budget_inout - budget_in);
    return pdb_iterator_statistics(pdb, it, budget_inout);
  }

  cl_log(cl, CL_LEVEL_VERBOSE | PDB_FACILITY_ITERATOR,
         "PDB STAT for AND[%llu] %s: n=%llu cc=%llu "
         "nc=%llu fc=%llu %ssorted%s%s",
         (unsigned long long)it->it_id,
         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         (unsigned long long)pdb_iterator_n(pdb, it),
         (unsigned long long)pdb_iterator_check_cost(pdb, it),
         (unsigned long long)pdb_iterator_next_cost(pdb, it),
         (unsigned long long)pdb_iterator_find_cost(pdb, it),
         pdb_iterator_sorted(pdb, it) ? "" : "un",
         pdb_iterator_ordered(pdb, it) ? ", o=" : "",
         pdb_iterator_ordered(pdb, it) ? pdb_iterator_ordering(pdb, it) : "");

  pdb_rxs_pop(
      pdb,
      "STAT %p and %s "
      "n=%llu cc=%llu nc=%llu fc=%llu %ssorted%s%s",
      (void *)it, pdb_iterator_to_string(pdb, it, buf, sizeof buf),
      (unsigned long long)pdb_iterator_n(pdb, it),
      (unsigned long long)pdb_iterator_check_cost(pdb, it),
      (unsigned long long)pdb_iterator_next_cost(pdb, it),
      (unsigned long long)pdb_iterator_find_cost(pdb, it),
      pdb_iterator_sorted(pdb, it) ? "" : "un",
      pdb_iterator_ordered(pdb, it) ? ", o=" : "",
      pdb_iterator_ordered(pdb, it) ? pdb_iterator_ordering(pdb, it) : "");

  return 0;

err:
  pdb_rxs_pop(pdb, "STAT %p and error: %s ($%lld)", (void *)it,
              graphd_strerror(err), (long long)(budget_in - *budget_inout));
  return err;

suspend:
  pdb_rxs_pop(pdb, "STAT %p and suspend ($%lld)", (void *)it,
              (long long)(budget_in - *budget_inout));
  return PDB_ERR_MORE;
}
