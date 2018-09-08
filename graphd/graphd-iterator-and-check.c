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

#define RESUME_STATE(it, st)   \
  while (0) {                  \
    case st:                   \
      (it)->it_call_state = 0; \
  }

GRAPHD_SABOTAGE_DECL;

extern const pdb_iterator_type graphd_iterator_and_type;

typedef struct slow_check_slot {
  pdb_iterator *scs_it;
  size_t scs_index;
  unsigned int scs_find : 1;
  unsigned int scs_yes : 1;

} slow_check_slot;

struct graphd_and_slow_check_state {
  slow_check_slot *scs_slot;
  size_t scs_n;
  size_t scs_n_in_play;
};

int graphd_iterator_and_check_freeze_slow(graphd_iterator_and *gia,
                                          cm_buffer *buf) {
  int err;
  graphd_and_slow_check_state *const scs = gia->gia_scs;
  slow_check_slot *slot;
  char const *sep = "";
  size_t i;

  if (scs == NULL) return 0;

  err = cm_buffer_sprintf(buf, "[slow-check:%zu:%zu:", scs->scs_n,
                          scs->scs_n_in_play);
  if (err != 0) return err;

  for (i = 0, slot = scs->scs_slot; i < scs->scs_n; i++, slot++) {
    err = cm_buffer_add_string(buf, sep);
    if (err != 0) return err;
    sep = ",";

    if (slot->scs_yes) {
      err = cm_buffer_add_string(buf, "+");
      if (err != 0) return err;

      continue;
    }
    if (slot->scs_find) {
      err = cm_buffer_add_string(buf, "~");
      if (err != 0) return err;
    }
    if (slot->scs_it != NULL)
      err = graphd_iterator_util_freeze_subiterator(
          gia->gia_pdb, slot->scs_it, PDB_ITERATOR_FREEZE_EVERYTHING, buf);
    else
      err = cm_buffer_sprintf(buf, "#%zu", slot->scs_index);

    if (err != 0) return err;
  }
  return cm_buffer_add_string(buf, "]");
}

int graphd_iterator_and_check_thaw_slow(graphd_iterator_and *gia,
                                        char const **s_ptr, char const *e,
                                        pdb_iterator_base *pib,
                                        cl_loglevel loglevel) {
  cl_handle *const cl = gia->gia_cl;
  pdb_handle *pdb = gia->gia_pdb;
  graphd_and_slow_check_state *scs;
  slow_check_slot *slot;
  char const *s = *s_ptr;
  size_t n, n_in_play;
  int err;
  size_t i;

  if (s == NULL || e - s < sizeof("[slow-check") ||
      strncasecmp(s, "[slow-check", sizeof("[slow-check") - 1) != 0) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_and_check_thaw_slow: "
           "doesn't stat with [slow-check");
    return GRAPHD_ERR_LEXICAL;
  }
  err = pdb_iterator_util_thaw(pdb, s_ptr, e, "[slow-check:%zu:%zu:", &n,
                               &n_in_play);
  if (err != 0) {
    cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                 "expected [slow-check:N:N:");
    return err;
  }

  if (e - *s_ptr < n) {
    cl_log(cl, loglevel,
           "graphd_iterator_and_check_thaw_slow: "
           "n is %zu, yet cursor only has %lu bytes?",
           n, (unsigned long)(e - *s_ptr));
    return GRAPHD_ERR_LEXICAL;
  }

  if (n_in_play > n) {
    cl_log(cl, loglevel,
           "graphd_iterator_and_check_thaw_slow: "
           "n_in_play %zu > n %zu?",
           n_in_play, n);
    return GRAPHD_ERR_LEXICAL;
  }
  s = *s_ptr;
  scs = cm_zalloc(gia->gia_cm, sizeof(*scs) + sizeof(*scs->scs_slot) * n);

  scs->scs_n = n;
  scs->scs_n_in_play = n_in_play;
  scs->scs_slot = (void *)(scs + 1);

  for (i = 0, slot = scs->scs_slot; i < n; i++, slot++) {
    while (s < e && (*s == ':' || *s == ',')) s++;

    if (s < e && *s == '+') {
      slot->scs_yes = true;
      s++;
      continue;
    }

    if (s < e && *s == '~') {
      s++;
      slot->scs_find = true;
    }
    if (s < e && *s == '#') {
      err =
          pdb_iterator_util_thaw(gia->gia_pdb, &s, e, "#%zu", &slot->scs_index);
      if (err != 0) goto err;
    } else {
      err = graphd_iterator_util_thaw_subiterator(gia->gia_graphd, &s, e, pib,
                                                  loglevel, &slot->scs_it);
      if (err != 0) goto err;
    }
  }
  if (s < e && *s == ']')
    s++;
  else {
    cl_log(cl, loglevel,
           "graphd_iterator_and_check_thaw_slow: "
           "expected ], got \"%.*s\"?",
           (int)(e - s), s);
    goto err;
  }
  *s_ptr = s;

  cl_assert(cl, gia->gia_scs == NULL);
  gia->gia_scs = scs;

  return 0;

err:
  while (i > 0) {
    i--;
    pdb_iterator_destroy(pdb, &scs->scs_slot[i].scs_it);
  }
  cm_free(gia->gia_cm, scs);

  return err;
}

static int and_compare_costs(pdb_handle *pdb, unsigned long long range_n,
                             pdb_iterator *a, pdb_iterator *b) {
  double cost_a, cost_b;
  cl_handle *cl = pdb_log(pdb);

  /*  If we don't have both Ns, but we do have both check-costs,
   *  just prefer the lower check-cost.
   */
  if ((!pdb_iterator_n_valid(pdb, a) || !pdb_iterator_n_valid(pdb, b)) &&
      pdb_iterator_check_cost_valid(pdb, a) &&
      pdb_iterator_check_cost_valid(pdb, b)) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "and_compare_costs: don't have both "
           "N - prefer the smaller check-cost");

    return pdb_iterator_check_cost(pdb, a) < pdb_iterator_check_cost(pdb, b)
               ? -1
               : pdb_iterator_check_cost(pdb, a) >
                     pdb_iterator_check_cost(pdb, b);
  }

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

    total_n = range_n;
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

/**
 * @brief Delete a subconstraint from the subiterator check sort order.
 */
void graphd_iterator_and_check_delete_subcondition(pdb_iterator *it, size_t i) {
  graphd_iterator_and *ogia = it->it_original->it_theory;
  size_t k;

  if (ogia->gia_check_order == NULL) return;

  for (k = 0; k < ogia->gia_n; k++) {
    if (ogia->gia_check_order[k] > i)
      ogia->gia_check_order[k]--;

    else if (ogia->gia_check_order[k] == i && k != ogia->gia_n - 1) {
      memmove(ogia->gia_check_order + k, ogia->gia_check_order + k + 1,
              (ogia->gia_n - (k + 1)) * sizeof(*ogia->gia_check_order));

      /*  Reexamine the index we just
       *  pulled over the deleted one!
       */
      k--;
    }
  }
  ogia->gia_check_order_version++;
}

/**
 * @brief Update the subiterator check sort order.
 *
 *  Called from
 *   	- create_commit, after initializing the sort order
 *	- statistics, after a subiterator completes its statistics
 *
 *  The "and" original keeps track of who the subiterators are and
 *  what, if any, constitues an optimal order of consulting them
 *  for a check.
 */
int graphd_iterator_and_check_sort(pdb_iterator *it) {
  graphd_iterator_and *ogia = it->it_original->it_theory;
  pdb_handle *pdb = ogia->gia_pdb;
  cl_handle *cl = ogia->gia_cl;
  size_t i, *ord;
  bool any = false;
  char buf[200];
  unsigned long long const range_n = pdb_iterator_spread(pdb, it);

  cl_enter(cl, CL_LEVEL_VERBOSE, "it=%s",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf));

  if ((ord = ogia->gia_check_order) == NULL) {
    ogia->gia_check_order_version = 0;
    ogia->gia_check_order = ord =
        cm_malloc(ogia->gia_cm, sizeof(*ogia->gia_check_order) * ogia->gia_n);
    if (ogia->gia_check_order == NULL) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "out of memory");
      return errno ? errno : ENOMEM;
    }

    for (i = 0; i < ogia->gia_n; i++) ogia->gia_check_order[i] = i;
  }
  for (i = 0; i < ogia->gia_n - 1; i++) {
    size_t j, tmp;
    char buf[200];

    if (and_compare_costs(pdb, range_n, ogia->gia_sc[ord[i]].sc_it,
                          ogia->gia_sc[ord[i + 1]].sc_it) <= 0)

      continue;

    /*  i + 1 is cheaper than i.  It needs to
     *  trade places with i.
     */
    tmp = ord[i + 1];
    ord[i + 1] = ord[i];
    ord[i] = tmp;

    /*  It may also be cheaper than the records before
     *  i -- bubble it up until it's in the right place.
     */
    for (j = i; j > 0; j--) {
      if (and_compare_costs(pdb, range_n, ogia->gia_sc[ord[j - 1]].sc_it,
                            ogia->gia_sc[ord[j]].sc_it) <= 0)
        break;

      tmp = ord[j - 1];
      ord[j - 1] = ord[j];
      ord[j] = tmp;
    }

    cl_log(ogia->gia_cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_and_check_sort: "
           "moved #%d:%s to spot #%d",
           (int)i + 1, pdb_iterator_to_string(pdb, ogia->gia_sc[ord[j]].sc_it,
                                              buf, sizeof buf),
           (int)j);
    any = true;
  }

  /* Anything changed? */
  if (any) {
    ogia->gia_check_order_version++;
    if (it->it_displayname != NULL) {
      cm_free(ogia->gia_cm, it->it_displayname);
      it->it_displayname = NULL;
    }

    /*  Update the check cost.  It's not final, of course,
     *  but the slow check uses it as an upper bound for
     *  how much time to spend in analysis.
     */
    it->it_original->it_check_cost =
        graphd_iterator_and_calculate_check_cost(it, ogia);

    cl_log(cl, CL_LEVEL_VERBOSE, "graphd_iterator_check_sort: new order:\n");
    for (i = 0; i < ogia->gia_n; i++) {
      char buf[200];
      cl_log(cl, CL_LEVEL_VERBOSE, "[%zu -> %zu]: %s", i, ord[i],
             pdb_iterator_to_string(pdb, ogia->gia_sc[ord[i]].sc_it, buf,
                                    sizeof buf));
    }
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "%sthing changed", any ? "some" : "no");
  return 0;
}

/**
 * @brief If needed, update my process state with the new sort order.
 *
 * @param it	AND iterator
 * @param ps	process state in the AND iterator.
 *
 *  Called by process state managers before doing checks.
 *
 *  We can't update the check order asynchronously because
 *  the process state managers may be in the middle of doing
 *  something "once for each subcondition" - if the order of
 *  those subconditions changes in mid-iteration, subconditions
 *  might get skipped or rerun.
 *
 * @return 0 normally
 * @return ENOMEM on allocation error.
 */
int graphd_iterator_and_check_sort_refresh(pdb_iterator *it,
                                           and_process_state *ps) {
  graphd_iterator_and *gia = it->it_theory;
  graphd_iterator_and *ogia = ogia(it);
  cl_handle *cl = gia->gia_cl;
  int err;

  /* XXX sabotage this. */

  GRAPHD_AND_IS_PROCESS_STATE(cl, ps);

  if (ogia->gia_check_order == NULL) {
    err = graphd_iterator_and_check_sort(it);
    if (err != 0) return err;
    err = 0;
  }
  cl_assert(cl, ogia->gia_check_order != NULL);
  cl_assert(cl, it == it->it_original || gia->gia_check_order == NULL);

  if (ps->ps_check_order == NULL) {
    ps->ps_check_order =
        cm_malloc(gia->gia_cm, sizeof(*ps->ps_check_order) * ogia->gia_n);
    if (ps->ps_check_order == NULL) return errno ? errno : ENOMEM;
  } else if (ps->ps_check_order_version == ogia->gia_check_order_version)
    /*  Nothing changed. */
    return 0;
  else {
    size_t *tmp;
    tmp = cm_realloc(gia->gia_cm, ps->ps_check_order,
                     sizeof(*ps->ps_check_order) * ogia->gia_n);
    if (tmp == NULL) return errno ? errno : ENOMEM;
    ps->ps_check_order = tmp;
  }

  cl_assert(cl, ogia->gia_check_order != NULL);
  cl_assert(cl, ps->ps_check_order != NULL);

  memcpy(ps->ps_check_order, ogia->gia_check_order,
         ogia->gia_n * sizeof(*ps->ps_check_order));
  ps->ps_check_order_version = ogia->gia_check_order_version;
  ps->ps_n = ogia->gia_n;

  return 0;
}

/*  Set up a slow check() call.
 *
 *  Executed once at the beginning of a check() method
 *  for an AND iterator that doesn't have its statistics yet.
 */
static int and_iterator_slow_check_initialize(pdb_iterator const *const it) {
  graphd_iterator_and *gia = it->it_theory;
  graphd_iterator_and *ogia = ogia(it);
  graphd_and_slow_check_state *scs;
  cl_handle *cl = gia->gia_cl;
  size_t i;

  cl_assert(cl, ogia->gia_n > 0);
  if ((scs = gia->gia_scs) == NULL) {
    size_t i;

    gia->gia_scs = scs = cm_malloc(
        gia->gia_cm,
        sizeof(*gia->gia_scs) + ogia->gia_n * sizeof(*gia->gia_scs->scs_slot));
    if (scs == NULL) return errno ? errno : ENOMEM;

    scs->scs_slot = (void *)(scs + 1);
    scs->scs_n = ogia->gia_n;

    for (i = 0; i < scs->scs_n; i++) scs->scs_slot[i].scs_it = NULL;
  }
  for (i = 0; i < scs->scs_n; i++) {
    pdb_iterator_destroy(gia->gia_pdb, &scs->scs_slot[i].scs_it);
    scs->scs_slot[i].scs_index = ogia->gia_check_order[i];
    scs->scs_slot[i].scs_find = false;
    scs->scs_slot[i].scs_yes = false;
  }
  scs->scs_n_in_play = scs->scs_n;

  return 0;
}

/**
 * @brief Check an id against subiterators.
 *
 *  Statistics hasn't completed yet.
 *
 * @param pdb		database module handle
 * @param it		an and-iterator
 * @param id		the id to check
 * @param budget_inout	how much time before we have to return?
 *
 * @return 0 on "yes", GRAPHD_ERR_NO on "no"
 * @return other nonzero error codes on error.
 */
static int and_iterator_slow_check(pdb_handle *pdb, pdb_iterator *it, pdb_id id,
                                   pdb_budget *budget_inout) {
  graphd_iterator_and *gia = it->it_theory;
  cl_handle *cl = gia->gia_cl;
  int err = 0;
  char buf[200];
  slow_check_slot *scs;
  pdb_budget budget_in = *budget_inout, fair_budget, part_budget;
  and_process_state *ps = &gia->gia_ps;
  size_t i;

  if (GRAPHD_SABOTAGE(gia->gia_graphd, *budget_inout <= 0)) return PDB_ERR_MORE;

  cl_enter(cl, CL_LEVEL_VERBOSE, "%llx (it=%p, state=%d, scs=%p, $%lld)",
           (unsigned long long)id, (void *)it, it->it_call_state,
           (void *)gia->gia_scs, (long long)*budget_inout);

  cl_assert(cl, ogia(it)->gia_check_order != NULL);

  if (it->it_call_state == 0) {
    if (ps->ps_check_exclude_low <= id && ps->ps_check_exclude_high > id) {
      cl_leave(cl, CL_LEVEL_VERBOSE,
               "%lld excluded "
               "by cached exclude range %lld..%lld",
               (long long)id, (long long)ps->ps_check_exclude_low,
               (long long)ps->ps_check_exclude_high);
      return GRAPHD_ERR_NO;
    }

    if ((err = and_iterator_slow_check_initialize(it)) != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "and_iterator_slow_check_initialize: %s",
               graphd_strerror(err));
      return err;
    }

    it->it_call_state = 1;
    ps->ps_check_exclude_low = ps->ps_check_exclude_high = id;
  }
  cl_assert(cl, !pdb_iterator_statistics_done(pdb, it));
  cl_assert(cl, gia->gia_scs != NULL);
  cl_assert(cl, gia->gia_scs->scs_n_in_play > 0);

  fair_budget = (*budget_inout + (gia->gia_scs->scs_n_in_play - 1)) /
                gia->gia_scs->scs_n_in_play;

  for (i = 0, scs = gia->gia_scs->scs_slot;
       i < gia->gia_n && gia->gia_scs->scs_n_in_play > 0; i++, scs++) {
    if (scs->scs_yes) continue;

    if (scs->scs_it == NULL) {
      pdb_iterator *source;

      source = ogia(it)->gia_sc[scs->scs_index].sc_it;
      err = pdb_iterator_clone(pdb, source, &scs->scs_it);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err,
                     "iterator=%s",
                     pdb_iterator_to_string(pdb, source, buf, sizeof buf));
        cl_leave(cl, CL_LEVEL_VERBOSE, "clone failed: %s",
                 graphd_strerror(err));
        return err;
      }

      /*  Use find if it's cheap.
       */
      scs->scs_find = (pdb_iterator_check_cost_valid(pdb, scs->scs_it) &&
                       pdb_iterator_find_cost_valid(pdb, scs->scs_it) &&
                       pdb_iterator_sorted(pdb, scs->scs_it) &&
                       2 + pdb_iterator_check_cost(pdb, scs->scs_it) >=
                           pdb_iterator_find_cost(pdb, scs->scs_it));
    }
    part_budget = fair_budget;
    if (scs->scs_find) {
      pdb_id find_id;

      err = pdb_iterator_find(pdb, scs->scs_it, id, &find_id, &part_budget);
      if (err == 0 && id != find_id) {
        err = GRAPHD_ERR_NO;

        if (pdb_iterator_forward(pdb, it)) {
          if (ps->ps_check_exclude_high < find_id)
            ps->ps_check_exclude_high = find_id;
        } else {
          if (ps->ps_check_exclude_low > find_id + 1)
            ps->ps_check_exclude_low = find_id + 1;
        }
      }
    } else {
      err = pdb_iterator_check(pdb, scs->scs_it, id, &part_budget);
    }

    cl_assert(cl, part_budget <= fair_budget);
    *budget_inout -= fair_budget - part_budget;
    if (err == 0) {
      /*  Accepted by this iterator.
       *  We'll still need to ask the others.
       */
      cl_log(cl, CL_LEVEL_VERBOSE,
             "and_iterator_slow_check: subiterator %s "
             "accepts %lld ($%lld)",
             pdb_iterator_to_string(pdb, scs->scs_it, buf, sizeof buf),
             (long long)id, (long long)(fair_budget - part_budget));
      scs->scs_yes = true;
      pdb_iterator_destroy(pdb, &scs->scs_it);

      gia->gia_scs->scs_n_in_play--;
    } else if (err != PDB_ERR_MORE) {
      /*  One of the checkers says "no" or "error".
       *  End of checking.
       */
      it->it_call_state = 0;

      cl_leave(cl, CL_LEVEL_VERBOSE, "subiterator %s rejects %lld ($%lld)",
               pdb_iterator_to_string(pdb, scs->scs_it, buf, sizeof buf),
               (long long)id, (long long)(budget_in - *budget_inout));
      graphd_iterator_and_slow_check_finish(pdb, it);
      return err;
    }
  }
  if (gia->gia_scs->scs_n_in_play <= 0) {
    it->it_call_state = 0;
    cl_leave(cl, CL_LEVEL_VERBOSE, "and_iterator_slow_check: %lld ok ($%lld)",
             (long long)id, (long long)(budget_in - *budget_inout));
    return 0;
  }
  cl_assert(cl, it->it_call_state == 1);
  cl_leave(cl, CL_LEVEL_VERBOSE,
           "and_iterator_slow_check: %lld suspended ($%lld)", (long long)id,
           (long long)(budget_in - *budget_inout));
  return PDB_ERR_MORE;
}

/**
 * @brief Check an ID against a list of subconstraints (iterator method)
 *
 * @param pdb 		database module handle
 * @param it 		and-iterator
 * @param id 		check this id
 * @param budget_inout	deduct the cost from ths
 *
 * @return 0 on completion and acceptance (id is in the iterator)
 * @return GRAPHD_ERR_NO on completion and rejection (id is not in the iterator)
 * @return PDB_ERR_MORE on non-completion (need more time)
 * @return other nonzero error codes on unexpected system error
 *
 */
int graphd_iterator_and_check(pdb_handle *const pdb, pdb_iterator *const it,
                              pdb_id const id, pdb_budget *const budget_inout) {
  graphd_iterator_and *gia = it->it_theory;
  cl_handle *cl = gia->gia_cl;
  and_process_state *ps = &gia->gia_ps;
  pdb_budget budget_in = *budget_inout, slow_check_cost;

  int err;
  pdb_iterator *sub_it;
  double research_part;

  if (GRAPHD_SABOTAGE(gia->gia_graphd, *budget_inout <= 0)) return PDB_ERR_MORE;

  pdb_rxs_push(pdb, "CHECK %p and %llx (state=%d)", (void *)it,
               (unsigned long long)id, it->it_call_state);

  /*  Checking destroys the position both within the cache and without.
   *  Even if a cursor included a saved position, we don't need
   *  to resume after the check.
   */
  gia->gia_cache_offset = 0;
  gia->gia_cache_offset_valid = false;
  gia->gia_resume_id = PDB_ID_NONE;
  gia->gia_id = PDB_ID_NONE;
  gia->gia_ps.ps_eof = false;

  if ((err = pdb_iterator_refresh(pdb, it)) != PDB_ERR_ALREADY) {
    char buf[200];
    if (err == 0) goto redirect;
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_refresh", err, "it=%s",
                 pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    goto done;
  }

  if (it->it_call_state == 0 && ps->ps_check_exclude_low <= id &&
      ps->ps_check_exclude_high > id) {
    --*budget_inout;
    err = GRAPHD_ERR_NO;
    goto done;
  }

  if ((err = graphd_iterator_and_check_sort_refresh(it, ps)) != 0) goto done;

  cl_assert(cl, ps->ps_check_order != NULL);

  /*  If the incoming budget is larger than what a
   *  slow check would take, spend only as much on
   *  research as the slow check would take.
   *
   *  (In other words, research is more important if
   *  the world without research is more expensive.)
   */
  if (pdb_iterator_check_cost_valid(pdb, it)) {
    slow_check_cost = pdb_iterator_check_cost(pdb, it);
  } else {
    graphd_iterator_and const *ogia = it->it_original->it_theory;
    size_t i;

    slow_check_cost = 0;
    for (i = 0; i < ogia->gia_n; i++) {
      if (!pdb_iterator_check_cost_valid(pdb, ogia->gia_sc[i].sc_it)) break;

      slow_check_cost += pdb_iterator_check_cost(pdb, ogia->gia_sc[i].sc_it);
    }
    if (i >= ogia->gia_n)
      slow_check_cost /= 2;
    else
      slow_check_cost = 0.2 * *budget_inout;
  }

  research_part = (slow_check_cost >= *budget_inout)
                      ? 0.2
                      : (double)slow_check_cost / *budget_inout;

  err = graphd_iterator_and_access(pdb, it, budget_inout, research_part);
  if (err != GRAPHD_ERR_ALREADY) {
    if (err == 0) goto redirect;

    if (err != PDB_ERR_MORE) goto done;

    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_and_check: suspending research "
           "in favor of doing some slow checking.  (left: $%lld)",
           (long long)*budget_inout);

    err = and_iterator_slow_check(pdb, it, id, budget_inout);
    goto done;
  }

  /*  There is a producer, and it knows its statistics.
   *  (Otherwise, we'd be in the slow check branch.)
   */
  cl_assert(cl, pdb_iterator_statistics_done(pdb, it));
  switch (it->it_call_state) {
    default:
      cl_notreached(cl, "unexpected it_call_state %d", it->it_call_state);

      RESUME_STATE(it, 1)

      /*  Resume from a slow check; the original has
       *  evolved into something that now has statistics
       *  and fast checks.  (I.e. we're throwing the slow check
       *  results away.)
       */

      RESUME_STATE(it, 0)

      /*  If we cached that the id is in a range
       *  that doesn't exist, say so.
       */
      if (ps->ps_check_exclude_low <= id && ps->ps_check_exclude_high > id) {
        *budget_inout -= PDB_COST_FUNCTION_CALL;
        err = GRAPHD_ERR_NO;

        goto done;
      }

      /*  Is this in the range for which we cached results?
       */
      err = graphd_iterator_cache_check(pdb, it, ogia(it)->gia_cache, id);
      if (err != PDB_ERR_MORE) {
        *budget_inout -= PDB_COST_FUNCTION_CALL;
        goto done;
      }
      ps->ps_check_exclude_low = ps->ps_check_exclude_high = id;

      if (ps->ps_it == NULL) {
        err = graphd_iterator_and_process_state_initialize(pdb, it, ps);
        if (err != 0) goto done;
      }
      cl_assert(cl, ps->ps_it != NULL);

      for (ps->ps_check_i = 0; ps->ps_check_i < gia->gia_n; ps->ps_check_i++) {
        size_t check_i;

        cl_assert(cl, ps->ps_check_order != NULL);
        cl_assert(cl, ps->ps_it != NULL);
        cl_assert(cl, ps->ps_check_i < gia->gia_n);

        check_i = ps->ps_check_order[ps->ps_check_i];
        cl_assert(cl, check_i < ps->ps_n);

        sub_it = ps->ps_it[check_i];
        PDB_IS_ITERATOR(cl, sub_it);
        pdb_iterator_call_reset(pdb, sub_it);

        RESUME_STATE(it, 2)
        cl_assert(cl, ps->ps_check_i < ps->ps_n);
        check_i = ps->ps_check_order[ps->ps_check_i];
        cl_assert(cl, check_i < ps->ps_n);
        sub_it = ps->ps_it[check_i];

        /*  If checking costs about the same as finding,
         *  find instead of checking, and cache the
         *  upper boundary found in the exclude range cache.
         */

        if (pdb_iterator_check_cost_valid(pdb, sub_it) &&
            pdb_iterator_find_cost_valid(pdb, sub_it) &&
            pdb_iterator_sorted(pdb, sub_it) &&
            2 + pdb_iterator_check_cost(pdb, sub_it) >=
                pdb_iterator_find_cost(pdb, sub_it)) {
          err = pdb_iterator_find(pdb, sub_it, id, &ps->ps_id, budget_inout);
          if (err == 0 && ps->ps_id != id) {
            err = GRAPHD_ERR_NO;

            if (pdb_iterator_forward(pdb, it))
              ps->ps_check_exclude_high = id;
            else
              ps->ps_check_exclude_low = id + 1;

            cl_log(cl, CL_LEVEL_VERBOSE, "xcache primed for %lld..%lld",
                   (long long)ps->ps_check_exclude_low,
                   (long long)ps->ps_check_exclude_high);
          }
        } else {
          err = pdb_iterator_check(pdb, sub_it, id, budget_inout);
        }
        if (err != 0) {
          if (err == PDB_ERR_MORE) it->it_call_state = 2;
          goto done;
        }

        if (ps->ps_check_i < gia->gia_n - 1 &&
            GRAPHD_SABOTAGE(gia->gia_graphd, *budget_inout <= 0)) {
          it->it_call_state = 3;
          err = PDB_ERR_MORE;
          goto done;
        }
        RESUME_STATE(it, 3);
      }
  }
  err = 0;

done:
  if (err == PDB_ERR_MORE)
    pdb_rxs_pop(pdb, "CHECK %p and %llx suspend; state=%d ($%lld)", (void *)it,
                (unsigned long long)id, it->it_call_state,
                (long long)(budget_in - *budget_inout));
  else
    pdb_rxs_pop_test(pdb, err, budget_in - *budget_inout, "CHECK %p and %llx",
                     (void *)it, (unsigned long long)id);

  /*  If our original is still an "and" iterator, update
   *  its statistics.
   */
  if (it->it_original->it_type == &graphd_iterator_and_type) {
    cl_assert(cl, ogia(it)->gia_magic == GRAPHD_AND_MAGIC);

    ogia(it)->gia_total_cost_check += budget_in - *budget_inout;
    if (err != PDB_ERR_MORE) ogia(it)->gia_n_checked++;
  }
  pdb_iterator_account_charge_budget(pdb, it, check);
  return err;

redirect:
  pdb_rxs_pop(pdb, "CHECK %p and %llx redirect ($%lld)", (void *)it,
              (unsigned long long)id, (long long)(budget_in - *budget_inout));
  return pdb_iterator_check(pdb, it, id, budget_inout);
}

/* Free "slow check" resources in an AND iterator.
 */
void graphd_iterator_and_slow_check_finish(pdb_handle *const pdb,
                                           pdb_iterator *const it) {
  graphd_iterator_and *gia = it->it_theory;
  graphd_and_slow_check_state *scs;

  if ((scs = gia->gia_scs) != NULL) {
    size_t i;

    if (scs->scs_n_in_play != 0) {
      for (i = 0; i < scs->scs_n; i++)
        if (scs->scs_slot[i].scs_it != NULL)
          pdb_iterator_destroy(gia->gia_pdb, &scs->scs_slot[i].scs_it);
    }
    cm_free(gia->gia_cm, scs);
    gia->gia_scs = NULL;
  }
}
