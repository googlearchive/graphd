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

GRAPHD_SABOTAGE_DECL;

/*  How much production cost are we going to spend on pulling
 *  ids out of any single subconstraint?
 */
#define GRAPHD_OR_PRODUCTION_COST_MERGE_MAX 200
#define GRAPHD_OR_N_MERGE_MAX 20

/*  How much production cost are we going to spend on turning
 *  into a fixed array?
 */
#define GRAPHD_OR_PRODUCTION_COST_FIXED_MAX 5000
#define GRAPHD_OR_N_FIXED_MAX 200

/*  Maximum cost we're willing to spend to produce and check the
 *  contents of the easiest available producer during create-commit.
 */
#define GRAPHD_OR_PREEVALUATE_COST_MAX (1024 * 10)

/*  How many patterns we're keeping track of
 */
#define GRAPHD_OR_PATTERN_N 3

/*  A magic number to guard the local state.
 */
#define GRAPHD_OR_MAGIC 0xdecaffad
#define GRAPHD_IS_OR(cl, gio) cl_assert(cl, (gio)->gio_magic == GRAPHD_OR_MAGIC)

/*  Added flag in freeze call
 */
#define GRAPHD_OR_FREEZE_WITHOUT_MASQUERADE 0x100

/*  > or <, depending on forward/backwards
 */
#define GRAPHD_OR_AFTER(it, id1, id2)                                     \
  (pdb_iterator_forward(((graphd_iterator_or *)(it)->it_theory)->gio_pdb, \
                        (it))                                             \
       ? (id1) > (id2)                                                    \
       : (id1) < (id2))

#define GRAPHD_OR_ON_OR_AFTER(it, id1, id2)                               \
  (pdb_iterator_forward(((graphd_iterator_or *)(it)->it_theory)->gio_pdb, \
                        (it))                                             \
       ? (id1) >= (id2)                                                   \
       : (id1) <= (id2))

#define GRAPHD_OR_ON_OR_BEFORE(it, id1, id2)                              \
  (pdb_iterator_forward(((graphd_iterator_or *)(it)->it_theory)->gio_pdb, \
                        (it))                                             \
       ? (id1) <= (id2)                                                   \
       : (id1) >= (id2))

#define GRAPHD_OR_BEFORE(it, id1, id2)                                    \
  (pdb_iterator_forward(((graphd_iterator_or *)(it)->it_theory)->gio_pdb, \
                        (it))                                             \
       ? (id1) < (id2)                                                    \
       : (id1) > (id2))

static const pdb_iterator_type or_iterator_type;

typedef struct graphd_or_subcondition {
  /*  Subiterator.  Used to produce; state.
   */
  pdb_iterator *oc_it;

  /*  PDB_ID_NONE or the most recently produced, and not
   *  yet consumed, id.
   */
  pdb_id oc_id;

  /*  While producing, subconditions after oc_next
   *  have oc_ids greater or equal to this one, or
   *  have (beyond that) completely run out.
   */
  struct graphd_or_subcondition *oc_next;

  /*  While producing, subconditions before oc_next
   *  have oc_ids smaller or equal to this one.
   */
  struct graphd_or_subcondition *oc_prev;

  unsigned int oc_eof : 1;

} graphd_or_subcondition;

static const cm_list_offsets graphd_or_subcondition_offsets =
    CM_LIST_OFFSET_INIT(graphd_or_subcondition, oc_next, oc_prev);

typedef struct graphd_iterator_or {
  unsigned long gio_magic;
  graphd_handle *gio_graphd;
  graphd_request *gio_greq;
  pdb_handle *gio_pdb;
  cm_handle *gio_cm;
  cl_handle *gio_cl;

  /*  Flat array of subiterators.
   *  M are allocated; N of those are in use.
   */
  graphd_or_subcondition *gio_oc;
  size_t gio_m;
  size_t gio_n;

  /*  Most recently returned ID, or PDB_ID_NONE at the start.
   */
  pdb_id gio_id;

  /*  The ID we're waiting for while catching up with a
   *  previous position in an unsorted OR.
   */
  pdb_id gio_resume_id;

  /*  ID used during "next" with unsorted iterators.  If any of the
   *  iterators in the "EOF" chain check(gio_check_id) == 0, it cannot
   *  be returned from the "next" call, because they already returned
   *  it in their earlier lives.
   */
  pdb_id gio_check_id;

  /*  Pointer used in resumable "next" (unsorted case) and
   * "check" to loop over all iterators.
   */
  graphd_or_subcondition *gio_this_oc;

  graphd_or_subcondition *gio_active_head;
  graphd_or_subcondition *gio_active_tail;
  graphd_or_subcondition *gio_active_last;

  graphd_or_subcondition *gio_eof_head;
  graphd_or_subcondition *gio_eof_tail;

  unsigned int gio_eof : 1;

  /*  Set if we're thawing; makes commit be more gentle.
   */
  unsigned int gio_thaw : 1;

  /* Subiterator state used in "statistics".
   */
  int gio_statistics_state;
  graphd_or_subcondition *gio_statistics_oc;

  /*  In the original only:
   *
   *  When freezing, masquerade as this rather than
   *  iterating over all the subiterators.
   *
   *  low and high are injected into the first :: in the string.
   */
  char *gio_masquerade;

  graphd_or_subcondition **gio_sort_me;
  size_t gio_sort_me_n;

  /*  While we're being built, move small fixed
   *  iterator contents into this "fixed" iterator.
   */
  pdb_iterator *gio_fixed;

  unsigned int gio_primitive_summary_tried : 1;
  unsigned int gio_primitive_summary_successful : 1;
  pdb_primitive_summary gio_primitive_summary;

  /*  A delegate iterator to use for fast check.
   */
  pdb_iterator *gio_check_it;

} graphd_iterator_or;

#define ogio_nocheck(it) ((graphd_iterator_or *)(it->it_original->it_theory))

#define ogio(it)                                                               \
  ((ogio_nocheck(it)->gio_magic == GRAPHD_OR_MAGIC)                            \
       ? ogio_nocheck(it)                                                      \
       : (cl_notreached(                                                       \
              ((graphd_iterator_or *)((it)->it_theory))->gio_cl,               \
              "ISA iterator %p has an original (%p) that is not an ISA", (it), \
              (it)->it_original),                                              \
          (graphd_iterator_or *)NULL))

/*  The code below features four restartable functions.
 *
 *  If a restartable function returns PDB_ERR_MORE, the call state will
 *  be set to values that causes the call to resume where it left off,
 *  if called again.
 *
 *  The iteration state always consists of:
 *  	- the sub-iterator-state ("its")
 *	- i (the index into the subcondition array)
 *	- a numeric "state" used to switch (...) to the location
 * 	  we returned from.
 *
 *  The RESUME_STATE(it, ..) macro is a "case:" target to the
 *  initial switch.
 */
#define RESUME_STATE(it, st) \
  case st:                   \
    (it)->it_call_state = 0;

/*  The SAVE_STATE(..) macro prepares a resume, usually to a
 *  RESUME_STATE(..) macro a few lines above the SAVE_STATE.
 */
#undef LEAVE_SAVE_STATE
#define LEAVE_SAVE_STATE(it, st)                                      \
  return (cl_leave(cl, CL_LEVEL_VERBOSE, "suspend [%s:%d; state=%d]", \
                   __FILE__, __LINE__, (int)(st)),                    \
          ((it)->it_call_state = (st)), PDB_ERR_MORE)

static void or_deactivate_oc(graphd_iterator_or *gio,
                             graphd_or_subcondition *oc) {
  cm_list_remove(graphd_or_subcondition, graphd_or_subcondition_offsets,
                 &gio->gio_active_head, &gio->gio_active_tail, oc);
  if (gio->gio_active_last == oc) gio->gio_active_last = NULL;
}

static int or_set_oc_id(graphd_iterator_or *gio, graphd_or_subcondition *oc,
                        pdb_id id) {
  or_deactivate_oc(gio, oc);
  oc->oc_id = id;

  if (gio->gio_sort_me == NULL) {
    gio->gio_sort_me =
        cm_malloc(gio->gio_cm, sizeof(*gio->gio_sort_me) * gio->gio_n);
    if (gio->gio_sort_me == NULL) return errno ? errno : ENOMEM;
  }

  cl_assert(gio->gio_cl, gio->gio_sort_me_n < gio->gio_n);
  gio->gio_sort_me[gio->gio_sort_me_n++] = oc;

  return 0;
}

/**
 * @brief move a subcondition from the "active" chain into the "EOF" chain.
 *
 * @param gio		parent iterator handle
 * @param oc		subcondition to move.
 */
static void or_retire_oc(graphd_iterator_or *gio, graphd_or_subcondition *oc) {
  or_deactivate_oc(gio, oc);
  cm_list_push(graphd_or_subcondition, graphd_or_subcondition_offsets,
               &gio->gio_eof_head, &gio->gio_eof_tail, oc);
  oc->oc_eof = true;
}

/*  An invariant that always holds true for the oc->oc_active_head,tail chain:
 *
 *  - The empty iterators that need to be reloaded are at the beginning
 *    of the chain.
 *  - The non-empty iterators further to the back are sorted by
 *    waiting ID, in ascending order.
 */
#if 0
#define or_chain_invariant(it) or_chain_invariant_loc(it, __LINE__)

static void or_chain_invariant_loc(pdb_iterator * it, int line)
{
	graphd_iterator_or	* gio = it->it_theory;
	graphd_or_subcondition	* oc;
	cl_handle		* cl  = gio->gio_cl;

	oc = gio->gio_active_head;
	while (oc != NULL && oc->oc_id == PDB_ID_NONE)
	{
		cl_assert(cl, oc != oc->oc_prev);
		cl_assert(cl, oc != oc->oc_next);
		if (oc->oc_next == NULL)
		{
			cl_assert(cl, oc == gio->gio_active_tail);
			if (oc != gio->gio_active_tail)
				cl_notreached(cl, "%s:%d: gio->gio_active_tail "
					"%p != oc %p\n", __FILE__, line,
					(void *)gio->gio_active_tail,
					(void *)oc);
		}
		oc = oc->oc_next;
	}

	if (oc != NULL)
	{
		for (;;)
		{
			cl_assert(cl, oc->oc_id != PDB_ID_NONE);

			if (oc->oc_next == NULL)
				break;

			cl_assert(cl,
		GRAPHD_OR_ON_OR_BEFORE(it, oc->oc_id, oc->oc_next->oc_id));
			oc = oc->oc_next;
		}

		if (gio->gio_active_tail != oc)
			cl_notreached(cl, "%s:%d: gio->gio_active_tail "
				"%p != oc %p\n", __FILE__, line,
				(void *)gio->gio_active_tail,
				(void *)oc);

		cl_assert(cl, gio->gio_active_tail == oc);
	}
}

#else
#define or_chain_invariant(it) /**/
#endif

/**
 * @brief move all subconditions into the "active" chain.
 *
 * @param gio		parent iterator handle
 * @param oc		subcondition to move.
 */
static void or_activate_all(pdb_iterator *it) {
  graphd_iterator_or *gio = it->it_theory;
  size_t i;
  graphd_or_subcondition *oc;

  for (i = gio->gio_n, oc = gio->gio_oc; i--; oc++) {
    oc->oc_id = PDB_ID_NONE;
    oc->oc_eof = false;
    oc->oc_next = oc + 1;
    oc->oc_prev = oc - 1;
  }
  if (gio->gio_n == 0)
    gio->gio_active_head = gio->gio_active_tail = NULL;
  else {
    gio->gio_oc[0].oc_prev = NULL;
    gio->gio_oc[gio->gio_n - 1].oc_next = NULL;
    gio->gio_active_head = gio->gio_oc + 0;
    gio->gio_active_tail = gio->gio_oc + gio->gio_n - 1;
  }
  gio->gio_eof_head = gio->gio_eof_tail = NULL;
  gio->gio_this_oc = NULL;
  gio->gio_active_last = NULL;

  or_chain_invariant(it);
}

static void or_subcondition_initialize(graphd_iterator_or *gio,
                                       graphd_or_subcondition *oc) {
  memset(oc, 0, sizeof(*oc));

  oc->oc_eof = false;
  oc->oc_it = NULL;
  oc->oc_id = PDB_ID_NONE;
  oc->oc_next = NULL;
  oc->oc_prev = NULL;
}

/**
 * @brief Preevaluate an "or" that's based on merging small, fixed sets.
 *
 *  Not so much because iterating over small sets in a "fixed"
 *  iterator is that much faster, but because it's good for
 *  our callers to be able to recognize small sets.
 *
 * @return PDB_ERR_MORE if that would take too long
 * @return 0 on success
 * @return other errors on unexpected system error.
 */
static int or_become_small_set(pdb_iterator *it) {
  graphd_iterator_or *gio = it->it_theory;
  graphd_handle *graphd = gio->gio_graphd;
  graphd_or_subcondition *oc = gio->gio_oc;
  cl_handle *cl = gio->gio_cl;
  pdb_handle *pdb = gio->gio_pdb;
  int err = 0;
  size_t id_n, i;
  pdb_id id, pred_id;
  pdb_iterator *fixed_it = NULL;
  char buf[200];
  unsigned long long total_cost, total_n;

  cl_enter(cl, CL_LEVEL_VERBOSE, "%s",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf));

  total_cost = 0;
  total_n = 0;
  for (i = gio->gio_n, oc = gio->gio_oc; i--; oc++) {
    if (!pdb_iterator_next_cost_valid(pdb, oc->oc_it) ||
        !pdb_iterator_n_valid(pdb, oc->oc_it)) {
      cl_leave(cl, CL_LEVEL_VERBOSE,
               "production cost or n from %s "
               "is not valid - defaulting.",
               pdb_iterator_to_string(pdb, oc->oc_it, buf, sizeof buf));
      return PDB_ERR_MORE;
    }
    total_n += pdb_iterator_n(pdb, oc->oc_it);
    total_cost +=
        pdb_iterator_next_cost(pdb, oc->oc_it) * pdb_iterator_n(pdb, oc->oc_it);
  }

  if (total_cost >= GRAPHD_OR_PREEVALUATE_COST_MAX) {
    cl_leave(cl, CL_LEVEL_VERBOSE,
             "total production cost %llu is too large - defaulting.",
             total_cost);
    return PDB_ERR_MORE;
  }

  /*  Looks like it can be done -- so let's do it.
   */
  err = graphd_iterator_fixed_create(graphd, (size_t)total_n, it->it_low,
                                     it->it_high, pdb_iterator_forward(pdb, it),
                                     &fixed_it);
  if (err != 0) {
    cl_leave_err(cl, CL_LEVEL_FAIL, err,
                 "graphd_iterator_fixed_create: low=%llx, high=%llx, "
                 "forward=%d, n=%llu",
                 it->it_low, it->it_high, (int)it->it_forward, total_n);
    return err;
  }

  pred_id = PDB_ID_NONE;
  id_n = 0;
  for (i = gio->gio_n, oc = gio->gio_oc; i--; oc++) {
    while (!(err = pdb_iterator_next_nonstep(pdb, oc->oc_it, &id))) {
      if (id_n >= total_n) {
        cl_log(cl, CL_LEVEL_FAIL, "more than %llu ids -- cancelling",
               (unsigned long long)total_n);
        goto cancel;
      }
      err = graphd_iterator_fixed_add_id(fixed_it, id);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_fixed_add_id", err,
                     "id=%lld", (long long)id);
        goto cancel;
      }
      id_n++;
    }
    if (err != GRAPHD_ERR_NO) goto cancel;
  }
  graphd_iterator_fixed_create_commit(fixed_it);

  err = graphd_iterator_substitute(gio->gio_greq, it, fixed_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_substitute", err, "%s",
                 pdb_iterator_to_string(pdb, fixed_it, buf, sizeof buf));
    pdb_iterator_destroy(pdb, &fixed_it);
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s",
           err ? graphd_strerror(err)
               : pdb_iterator_to_string(pdb, it, buf, sizeof buf));
  return err;

cancel:
  /* Reset and rechain the iterators.
   */
  pdb_iterator_reset(pdb, it);
  pdb_iterator_destroy(pdb, &fixed_it);

  cl_leave(cl, CL_LEVEL_VERBOSE, "%s",
           err ? graphd_strerror(err) : "too complicated");
  return err ? err : PDB_ERR_MORE;
}

static int or_iterator_statistics(pdb_handle *pdb, pdb_iterator *it,
                                  pdb_budget *budget_inout) {
  unsigned long long total_nc = 0, total_cc = 0, total_n = 0, total_fc = 0;
  pdb_budget budget_in = *budget_inout;
  graphd_iterator_or *gio = it->it_theory;
  cl_handle *const cl = gio->gio_cl;
  graphd_or_subcondition *oc;
  int err;
  size_t i;
  char buf[200];
  bool sorted;

  cl_enter(cl, CL_LEVEL_VERBOSE, "(%p:%s, state=%d; budget=%lld)", (void *)it,
           pdb_iterator_to_string(pdb, it, buf, sizeof buf),
           gio->gio_statistics_state, (long long)*budget_inout);

  /*  Do statistics for all our subiterators.
   */
  switch (gio->gio_statistics_state) {
    case 0:
      if (gio->gio_n == 0) break;

      cl_assert(cl, gio->gio_oc != NULL);
      for (gio->gio_statistics_oc = gio->gio_oc;
           gio->gio_statistics_oc < gio->gio_oc + gio->gio_n;
           gio->gio_statistics_oc++) {
        case 1:
          oc = gio->gio_statistics_oc;
          cl_assert(cl, oc != NULL);
          PDB_IS_ITERATOR(cl, oc->oc_it);

          gio->gio_statistics_state = 0;
          err = pdb_iterator_statistics(pdb, oc->oc_it, budget_inout);
          if (err != 0) {
            if (err == PDB_ERR_MORE) {
              gio->gio_statistics_state = 1;
              cl_leave(cl, CL_LEVEL_VERBOSE,
                       "suspended in "
                       "subiterator statistics "
                       "($%lld)",
                       (long long)(budget_in - *budget_inout));
              return PDB_ERR_MORE;
            }
            cl_leave_err(
                cl, CL_LEVEL_FAIL, err,
                "error from pdb_iterator_"
                "statistics for %s",
                pdb_iterator_to_string(pdb, oc->oc_it, buf, sizeof(buf)));
            return err;
          }

          if ((gio->gio_statistics_oc < gio->gio_oc + gio->gio_n - 1) &&
              GRAPHD_SABOTAGE(gio->gio_graphd, *budget_inout <= 0)) {
            gio->gio_statistics_oc++;
            gio->gio_statistics_state = 1;

            cl_leave(cl, CL_LEVEL_VERBOSE,
                     "suspended between calls to "
                     "subiterator statistics ($%lld)",
                     (long long)(budget_in - *budget_inout));
            return PDB_ERR_MORE;
          }
      }
      break;
  }

  if (gio->gio_check_it != NULL &&
      !pdb_iterator_check_cost_valid(pdb, gio->gio_check_it)) {
    err = pdb_iterator_statistics(pdb, gio->gio_check_it, budget_inout);
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE,
               "suspended in call to check-iterator "
               "statistics ($%lld)",
               (long long)(budget_in - *budget_inout));
      return err;
    }
  }

  /*  We're sorted if all our subiterators are sorted.
   *  Infer statistics.
   */
  sorted = true;
  total_n = 0;

  for (i = gio->gio_n, oc = gio->gio_oc; i--; oc++) {
    unsigned long long n;

    sorted &= !!pdb_iterator_sorted(pdb, oc->oc_it);
    total_n += n = pdb_iterator_n(pdb, oc->oc_it);
    total_nc += n * pdb_iterator_next_cost(pdb, oc->oc_it);
    total_cc += pdb_iterator_check_cost(pdb, oc->oc_it);
    total_fc += pdb_iterator_find_cost(pdb, oc->oc_it);
  }
  pdb_iterator_sorted_set(pdb, it, sorted);

  if (gio->gio_check_it != NULL)
    pdb_iterator_check_cost_set(
        pdb, it, pdb_iterator_check_cost(pdb, gio->gio_check_it));
  else
    /*  Rough assumption: on average, checks will succeed
     *  in the first half of the checks.
     */
    pdb_iterator_check_cost_set(pdb, it, (total_cc + 1) / 2);

  /*  Rough assumption: there's no overlap.
   */
  pdb_iterator_n_set(pdb, it, total_n);

  pdb_iterator_find_cost_set(pdb, it, total_fc);
  pdb_iterator_next_cost_set(pdb, it,
                             total_n == 0 ? total_nc : total_nc / total_n);

  pdb_iterator_statistics_done_set(pdb, it);

  cl_log(cl, CL_LEVEL_VERBOSE | PDB_FACILITY_ITERATOR,
         "PDB STAT for %s: n=%llu cc=%llu; "
         "nc=%llu; fc=%llu; %ssorted",

         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         (unsigned long long)pdb_iterator_n(pdb, it),
         (unsigned long long)pdb_iterator_check_cost(pdb, it),
         (unsigned long long)pdb_iterator_next_cost(pdb, it),
         (unsigned long long)pdb_iterator_find_cost(pdb, it),
         pdb_iterator_sorted(pdb, it) ? "" : "un");

  cl_leave(cl, CL_LEVEL_VERBOSE, "done ($%lld)",
           (long long)(budget_in - *budget_inout));
  return 0;
}

static int or_sort_and_refile_compar_forward(const void *A, const void *B) {
  graphd_or_subcondition const *const *a = A;
  graphd_or_subcondition const *const *b = B;

  if ((*a)->oc_id == PDB_ID_NONE) return -1;

  return ((*a)->oc_id < (*b)->oc_id) ? -1 : (*a)->oc_id > (*b)->oc_id;
}

static int or_sort_and_refile_compar_backward(const void *A, const void *B) {
  graphd_or_subcondition const *const *a = A;
  graphd_or_subcondition const *const *b = B;

  if ((*a)->oc_id == PDB_ID_NONE) return -1;

  return ((*a)->oc_id > (*b)->oc_id) ? -1 : (*a)->oc_id < (*b)->oc_id;
}

static void or_sort_and_refile(pdb_iterator *it) {
  graphd_iterator_or *gio = it->it_theory;
  cl_handle *cl = gio->gio_cl;
  graphd_or_subcondition **oc_s = gio->gio_sort_me;
  graphd_or_subcondition **oc_e = oc_s + gio->gio_sort_me_n;
  graphd_or_subcondition *oc_head, *oc_tail;
  size_t hop = 0;

  cl_assert(cl, gio->gio_sort_me_n <= gio->gio_n);

  if (oc_s == oc_e) return;

  cl_log(cl, CL_LEVEL_VERBOSE,
         "or_sort_and_refile: %zu candidate%s into %zu @%p.",
         (size_t)(oc_e - oc_s), (oc_e - oc_s) == 1 ? "" : "s", gio->gio_n,
         (void *)gio);
  or_chain_invariant(it);

  if (oc_s + 1 < oc_e)
    qsort(oc_s, oc_e - oc_s, sizeof(*oc_s),
          (pdb_iterator_forward(gio->gio_pdb, it)
               ? or_sort_and_refile_compar_forward
               : or_sort_and_refile_compar_backward));

  oc_head = gio->gio_active_head;
  oc_tail = gio->gio_active_tail;

  /*  If we don't have anyting in the current active chain,
   *  insert everything in sort-order.
   */
  if (oc_head == NULL) {
    for (; oc_s < oc_e; oc_s++) {
      cl_assert(cl, *oc_s != gio->gio_active_tail);
      cl_assert(cl, *oc_s != gio->gio_active_head);

      cm_list_enqueue(graphd_or_subcondition, graphd_or_subcondition_offsets,
                      &gio->gio_active_head, &gio->gio_active_tail, *oc_s);
    }
    goto done;
  }

  cl_assert(cl, oc_head != NULL);
  cl_assert(cl, oc_tail != NULL);

  /*  oc_head and oc_tail point into our existing records.
   *  oc_head is the highest slot we might fit just before.
   *  oc_tail is the lowest slot we might fit just behind.
   *
   *  We're moving both of them to insertion points (from
   *  both ends) because the lists can get long, and picking
   *  the wrong end may carry large penalties.
   */

  /*  The empty subconditions always stay at the beginning.
   */
  while (oc_s < oc_e && (*oc_s)->oc_id == PDB_ID_NONE) {
    cl_assert(cl, *oc_s != gio->gio_active_tail);
    cl_assert(cl, *oc_s != gio->gio_active_head);

    cm_list_push(graphd_or_subcondition, graphd_or_subcondition_offsets,
                 &gio->gio_active_head, &gio->gio_active_tail, *oc_s);
    oc_s++;
  }
  if (oc_s >= oc_e) goto done;

  /*  If we have a useful memory of inserting something else, use that.
   */
  if (gio->gio_active_last != NULL &&
      gio->gio_active_last->oc_id != PDB_ID_NONE) {
    if (GRAPHD_OR_ON_OR_AFTER(it, (*oc_s)->oc_id, gio->gio_active_last->oc_id))
      oc_head = gio->gio_active_last->oc_next;

    if (GRAPHD_OR_ON_OR_BEFORE(it, oc_e[-1]->oc_id,
                               gio->gio_active_last->oc_id))
      oc_tail = gio->gio_active_last->oc_prev;
  }

  if (oc_s >= oc_e) goto done;

  /*  In each round:
   *
   *	(a1) move the head up, it's too low
   *  or  (a2) Insert from the start just before the head
   *
   *	(b1) move the tail down, it's too high
   *  or	(b2) insert from the end just after the tail
   */
  if (oc_head != NULL && oc_tail != NULL && oc_tail->oc_id != PDB_ID_NONE)
    for (;;) {
      cl_assert(cl, oc_tail != oc_tail->oc_prev);

      /*   oc_tail > E ?  oc_tail--.
       */
      if (oc_tail->oc_id != PDB_ID_NONE &&
          GRAPHD_OR_AFTER(it, oc_tail->oc_id, oc_e[-1]->oc_id)) {
        hop++;
        if ((oc_tail = oc_tail->oc_prev) == NULL ||
            oc_tail->oc_id == PDB_ID_NONE || oc_head == oc_tail)
          break;
      } else {
        char buf[200];

        /*   oc_tail <= E ?  insert E after oc_tail.
         */
        cl_log(cl, CL_LEVEL_VERBOSE,
               "or_sort_and_refile: enqueue %s after "
               "%zu hop%s (%llx after \"tail\" %llx)",
               pdb_iterator_to_string(gio->gio_pdb, oc_e[-1]->oc_it, buf,
                                      sizeof buf),
               hop, hop == 1 ? "" : "s", (unsigned long long)oc_e[-1]->oc_id,
               (unsigned long long)oc_tail->oc_id);

        /*  oc_tail is smaller.
         *  Append this node to oc_tail.
         */
        cl_assert(cl, oc_e[-1]->oc_id != PDB_ID_NONE);
        cm_list_insert_after(
            graphd_or_subcondition, graphd_or_subcondition_offsets,
            &gio->gio_active_head, &gio->gio_active_tail, oc_tail, oc_e[-1]);
        oc_tail = oc_tail->oc_next;
        if (oc_s >= --oc_e) goto done;
      }

      /*  oc_head < S ? oc_head++;
       */
      if (oc_head->oc_id == PDB_ID_NONE ||
          GRAPHD_OR_BEFORE(it, oc_head->oc_id, (*oc_s)->oc_id)) {
        hop++;
        if ((oc_head = oc_head->oc_next) == NULL || oc_head == oc_tail) break;
      } else {
        /*  oc_head >= S.
         *  Insert S right before oc_head.
         */
        char buf[200];
        cl_log(cl, CL_LEVEL_VERBOSE,
               "or_sort_and_refile: "
               "enqueue %s after %zu hop%s "
               "(%llx before %llx)",
               pdb_iterator_to_string(gio->gio_pdb, (*oc_s)->oc_it, buf,
                                      sizeof buf),
               hop, hop == 1 ? "" : "s", (unsigned long long)(*oc_s)->oc_id,
               (unsigned long long)oc_head->oc_id);

        cl_assert(cl, (*oc_s)->oc_id != PDB_ID_NONE);
        cm_list_insert_before(
            graphd_or_subcondition, graphd_or_subcondition_offsets,
            &gio->gio_active_head, &gio->gio_active_tail, oc_head, *oc_s);
        if (++oc_s >= oc_e) goto done;
      }
    }

  /*  Either they're both the same,
   *  or one of the list endpoints is NULL (or, in tail's case,
   *  points to PDB_ID_NONE).
   *
   *  If the tail is null (or points to PDB_ID_NONE), all the
   *  remaining records fit right after it.
   *
   *  If the head is null, all the remaining data
   *  fits on the end.
   *
   *  If both are NULL, the array must be empty, and
   *  the front is = the end; we handled that case above.
   */
  cl_assert(cl, oc_s < oc_e && (oc_head != NULL || oc_tail != NULL));
  cl_log(cl, CL_LEVEL_VERBOSE, "or_sort_and_refile: enqueue %zu remaining",
         (size_t)(oc_e - oc_s));

  if (oc_head == NULL) {
    while (oc_s < oc_e) {
      cm_list_enqueue(graphd_or_subcondition, graphd_or_subcondition_offsets,
                      &gio->gio_active_head, &gio->gio_active_tail, *oc_s);
      or_chain_invariant(it);

      oc_s++;
    }
  } else if (oc_tail == NULL || oc_tail->oc_id == PDB_ID_NONE) {
    cl_assert(cl, oc_s < oc_e);
    cl_assert(cl, (*oc_s)->oc_id != PDB_ID_NONE);

    if (oc_tail == NULL) {
      cm_list_push(graphd_or_subcondition, graphd_or_subcondition_offsets,
                   &gio->gio_active_head, &gio->gio_active_tail, *oc_s);
      oc_tail = *oc_s++;
    }

    while (oc_s < oc_e) {
      cl_assert(cl, (*oc_s)->oc_id != PDB_ID_NONE);
      cm_list_insert_after(
          graphd_or_subcondition, graphd_or_subcondition_offsets,
          &gio->gio_active_head, &gio->gio_active_tail, oc_tail, *oc_s);
      or_chain_invariant(it);

      /*  Move our insertion points to the record
       *  we just inserted.
       */
      oc_tail = *oc_s++;
    }
  } else {
    cl_assert(cl, oc_head == oc_tail);
    cl_assert(cl, oc_head->oc_id != PDB_ID_NONE);
    cl_assert(cl, oc_tail->oc_id != PDB_ID_NONE);

    for (; oc_s < oc_e && GRAPHD_OR_BEFORE(it, (*oc_s)->oc_id, oc_head->oc_id);
         oc_s++) {
      cm_list_insert_before(
          graphd_or_subcondition, graphd_or_subcondition_offsets,
          &gio->gio_active_head, &gio->gio_active_tail, oc_head, *oc_s);
      or_chain_invariant(it);
    }
    for (; oc_s < oc_e; oc_s++) {
      cl_assert(cl, GRAPHD_OR_ON_OR_AFTER(it, (*oc_s)->oc_id, oc_tail->oc_id));

      cm_list_insert_after(
          graphd_or_subcondition, graphd_or_subcondition_offsets,
          &gio->gio_active_head, &gio->gio_active_tail, oc_tail, *oc_s);
      oc_tail = *oc_s;
      or_chain_invariant(it);
    }
  }
done:
  gio->gio_sort_me_n = 0;
  or_chain_invariant(it);
}

/**
 * @brief move all subconditions into the chain and position they belong to.
 *
 *   After a thaw with state.
 *
 * @param gio		parent iterator handle
 * @param oc		subcondition to move.
 */
static int or_refile_all(pdb_iterator *it) {
  graphd_iterator_or *gio = it->it_theory;
  graphd_or_subcondition *oc;
  size_t i;

  cl_enter(gio->gio_cl, CL_LEVEL_VERBOSE, "enter");
  cl_assert(gio->gio_cl, gio->gio_n > 0);

  gio->gio_eof_head = gio->gio_eof_tail = NULL;
  gio->gio_active_head = gio->gio_active_tail = NULL;
  gio->gio_this_oc = NULL;
  gio->gio_active_last = NULL;

  gio->gio_sort_me_n = 0;

  /*  gio_sort_me can be NULL or non-NULL; we allocate
   *  it if we need to.
   */
  for (i = gio->gio_n, oc = gio->gio_oc; i--; oc++) {
    oc->oc_next = oc->oc_prev = NULL;
    if (oc->oc_eof)
      cm_list_push(graphd_or_subcondition, graphd_or_subcondition_offsets,
                   &gio->gio_eof_head, &gio->gio_eof_tail, oc);

    /*  We special-case this comon case (all oc_ids start
     *  out as PDB_ID_NONE, and the subiterators are never
     *  used for anything but checking) to avoid allocating
     *  the gio_sort_me array if it isn't needed.
     */
    else if (oc->oc_id == PDB_ID_NONE)
      cm_list_push(graphd_or_subcondition, graphd_or_subcondition_offsets,
                   &gio->gio_active_head, &gio->gio_active_tail, oc);
    else {
      if (gio->gio_sort_me == NULL) {
        gio->gio_sort_me =
            cm_malloc(gio->gio_cm, gio->gio_n * sizeof(*gio->gio_sort_me));
        if (gio->gio_sort_me == NULL) return errno ? errno : ENOMEM;
      }
      gio->gio_sort_me[gio->gio_sort_me_n++] = oc;
    }
  }
  or_sort_and_refile(it);

  cl_leave(gio->gio_cl, CL_LEVEL_VERBOSE, "leave");
  return 0;
}

static int or_iterator_find_loc(pdb_handle *pdb, pdb_iterator *it, pdb_id id_in,
                                pdb_id *id_out, pdb_budget *budget_inout,
                                char const *file, int line) {
  graphd_iterator_or *gio = it->it_theory;
  cl_handle *cl = gio->gio_cl;
  graphd_or_subcondition *oc;
  pdb_budget budget_in = *budget_inout;
  int err;

#undef LEAVE_SAVE_STATE
#define LEAVE_SAVE_STATE(it, state)                                     \
  do {                                                                  \
    pdb_rxs_pop(pdb, "NEXT %p or suspend state=%d ($%lld)", (void *)it, \
                (int)(state), (long long)(budget_in - *budget_inout));  \
    err = PDB_ERR_MORE;                                                 \
    (it)->it_call_state = (state);                                      \
    goto err;                                                           \
  } while (0)

  pdb_rxs_push(pdb, "FIND %p or %llx (state=%d) [%s:%d] ($%lld)", (void *)it,
               (unsigned long long)id_in, it->it_call_state, file, line,
               (long long)*budget_inout);

  gio->gio_sort_me_n = 0;
  gio->gio_eof = false;

  /*  One can't call on-or-after unless the receiving
   *  iterator is sorted.
   */
  cl_assert(cl, pdb_iterator_sorted(pdb, it));

  switch (it->it_call_state) {
    default:
      RESUME_STATE(it, 0)

      /*  We don't have a current position,
       *  or the position we want is one we've walked
       *  past?
       */
      if (gio->gio_id == PDB_ID_NONE ||
          GRAPHD_OR_AFTER(it, gio->gio_id, id_in)) {
        /*  Reset the iterators to all be in the active chain.
         */
        or_activate_all(it);
      }

    /*  Go forward, calling on-or-after on all iterators
     *  whose position is undefined or below our cut-off point.
     */
    redo:
      while (gio->gio_active_head != NULL &&
             (gio->gio_active_head->oc_id == PDB_ID_NONE ||
              GRAPHD_OR_BEFORE(it, gio->gio_active_head->oc_id, id_in))) {
        pdb_id id_found;

        RESUME_STATE(it, 1)
        oc = gio->gio_active_head;
        err = pdb_iterator_find_loc(pdb, oc->oc_it, id_in, &id_found,
                                    budget_inout, file, line);

        if (err == PDB_ERR_MORE) {
          or_chain_invariant(it);
          or_sort_and_refile(it);
          LEAVE_SAVE_STATE(it, 1);
        } else if (err == GRAPHD_ERR_NO) {
          /*   Move this iterator to the "EOF" chain.
           */
          or_retire_oc(gio, oc);
          continue;
        } else if (err != 0) {
          char buf[200];

          or_chain_invariant(it);
          or_sort_and_refile(it);

          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_find_loc", err,
                       "it=%s, id=%llx",
                       pdb_iterator_to_string(pdb, oc->oc_it, buf, sizeof buf),
                       (unsigned long long)id_in);

        pdb_rxs_err:
          pdb_rxs_pop(pdb, "FIND %p or %llx error: %s [%s:%d] ($%lld)",
                      (void *)it, (unsigned long long)id_in,
                      graphd_strerror(err), file, line,
                      (long long)(budget_in - *budget_inout));
          goto err;
        }

        /*  Move this iterator onto the pile of
         *  things to be resorted later.
         */
        err = or_set_oc_id(gio, oc, id_found);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "or_set_oc_id", err, "id=%llx",
                       (unsigned long long)id_found);
          goto pdb_rxs_err;
        }
        if (GRAPHD_SABOTAGE(gio->gio_graphd, *budget_inout < 0)) {
          or_sort_and_refile(it);
          LEAVE_SAVE_STATE(it, 2);
        }
        RESUME_STATE(it, 2);
      }
      if (gio->gio_sort_me_n > 0) {
        or_chain_invariant(it);
        or_sort_and_refile(it);

        goto redo;
      }
      break;
  }

  if (gio->gio_active_head == NULL) {
    gio->gio_eof = true;
    pdb_rxs_pop(pdb, "FIND %p or %llx eof [%s:%d] ($%lld)", (void *)it,
                (unsigned long long)id_in, file, line,
                (long long)(budget_in - *budget_inout));
    err = GRAPHD_ERR_NO;
    goto err;
  }

  /*  Empty out the leading prefix of the active chain
   *  that holds the ID we're returning.
   */
  gio->gio_id = *id_out = gio->gio_active_head->oc_id;

  for (oc = gio->gio_active_head; oc != NULL; oc = oc->oc_next) {
    if (oc->oc_id != *id_out) break;

    oc->oc_id = PDB_ID_NONE;
  }
  or_chain_invariant(it);
  err = 0;

  pdb_rxs_pop(pdb, "FIND %p or %llx -> %llx [%s:%d] ($%lld)", (void *)it,
              (unsigned long long)id_in, (unsigned long long)*id_out, file,
              line, (long long)(budget_in - *budget_inout));

err:
  pdb_iterator_account_charge_budget(pdb, it, find);
  return err;
}

/**
 * @brief Check an ID against a list of subconditions (iterator method)
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
static int or_iterator_check(pdb_handle *pdb, pdb_iterator *it, pdb_id id,
                             pdb_budget *budget_inout) {
  graphd_iterator_or *gio = it->it_theory;
  cl_handle *cl = gio->gio_cl;
  pdb_budget budget_in = *budget_inout;
  int err;

#undef LEAVE_SAVE_STATE
#define LEAVE_SAVE_STATE(it, state)                                      \
  do {                                                                   \
    pdb_rxs_pop(pdb, "CHECK %p or suspend state=%d ($%lld)", (void *)it, \
                (int)(state), (long long)(budget_in - *budget_inout));   \
    err = PDB_ERR_MORE;                                                  \
    (it)->it_call_state = (state);                                       \
    goto err;                                                            \
  } while (0)

  pdb_rxs_push(pdb, "CHECK %p or %llx (state=%d) ($%lld)", (void *)it,
               (unsigned long long)id, it->it_call_state,
               (long long)*budget_inout);

  PDB_IS_ITERATOR(cl, it);
  GRAPHD_IS_OR(cl, gio);

  /*  A plain linear search over the subiterators.
   *  If anyone adopts the sub-id, the whole call succeeds;
   *  if no-one does, it fails.
   */
  switch (it->it_call_state) {
    default:
      RESUME_STATE(it, 0)

      if (ogio(it)->gio_check_it != NULL) {
        err = pdb_iterator_check(pdb, ogio(it)->gio_check_it, id, budget_inout);
        pdb_rxs_pop(pdb, "CHECK %p or %llx delegated: %s ($%lld)", (void *)it,
                    (unsigned long long)id,
                    err == PDB_ERR_NO
                        ? "no"
                        : (err == 0 ? "yes" : graphd_strerror(err)),
                    (long long)(budget_in - *budget_inout));
        goto err;
      }

      for (gio->gio_this_oc = gio->gio_oc;
           gio->gio_this_oc < gio->gio_oc + gio->gio_n; gio->gio_this_oc++) {
        RESUME_STATE(it, 1)
        err =
            pdb_iterator_check(pdb, gio->gio_this_oc->oc_it, id, budget_inout);
        if (err != 0) {
          if (err == PDB_ERR_MORE)
            LEAVE_SAVE_STATE(it, 1);

          else if (err != GRAPHD_ERR_NO) {
            pdb_rxs_pop(pdb, "CHECK %p or %llx error: %s ($%lld)", (void *)it,
                        (unsigned long long)id, graphd_strerror(err),
                        (long long)(budget_in - *budget_inout));
            goto err;
          }

          /*  err = GRAPHD_ERR_NO; we continue searching for
           *  other iterators that might recognize id
           *  as one of theirs.
           */
        } else if (err == 0) {
          pdb_rxs_pop(pdb, "CHECK %p or %llx yes ($%lld)", (void *)it,
                      (unsigned long long)id,
                      (long long)(budget_in - *budget_inout));
          goto err;
        }
      }
  }
  pdb_rxs_pop(pdb, "CHECK %p or %llx no ($%lld)", (void *)it,
              (unsigned long long)id, (long long)(budget_in - *budget_inout));
  err = GRAPHD_ERR_NO;

err:
  pdb_iterator_account_charge_budget(pdb, it, check);
  return err;
}

/**
 * @brief access the next primitive in an iteration
 *
 * @param pdb		opaque module handle, created with pdb_create()
 * @param it		iteration to set through
 * @param id_out	assign the pdb_id to this
 * @param budget_inout	budget for the operation, return if < 0
 * @param file		calling source file name
 * @param line		calling source line
 *
 * @return 0 on success, a nonzero error code on error
 * @return GRAPHD_ERR_NO after running out of primitives.
 */
static int or_iterator_next_loc(pdb_handle *pdb, pdb_iterator *it,
                                pdb_id *id_out, pdb_budget *budget_inout,
                                char const *file, int line) {
  graphd_iterator_or *gio = it->it_theory;
  cl_handle *cl = gio->gio_cl;
  graphd_or_subcondition *oc;
  pdb_budget budget_in = *budget_inout;
  int err;
  char buf[200];

  pdb_rxs_push(pdb, "NEXT %p or (state=%d) [%s:%d] ($%lld)", (void *)it,
               it->it_call_state, file, line, (long long)*budget_inout);

  or_chain_invariant(it);

#undef LEAVE_SAVE_STATE
#define LEAVE_SAVE_STATE(it, state)                                     \
  do {                                                                  \
    pdb_rxs_pop(pdb, "NEXT %p or suspend state=%d ($%lld)", (void *)it, \
                (int)(state), (long long)(budget_in - *budget_inout));  \
    err = PDB_ERR_MORE;                                                 \
    (it)->it_call_state = (state);                                      \
    goto err;                                                           \
  } while (0)

  if (gio->gio_eof) {
    --*budget_inout;

    pdb_rxs_pop(pdb, "NEXT %p or EOF ($%lld)", (void *)it,
                (long long)(budget_in - *budget_inout));
    err = GRAPHD_ERR_NO;
    goto err;
  }

  /*   Subiterators start out in the ACTIVE CHAIN.  They move
   *   into the EOF CHAIN when they've exhausted their IDs,
   *   and back into the active chain during a RESET.
   */

  if (!pdb_iterator_sorted(pdb, it)) goto unsorted;

  /*
   *  The SORTED algorithm:
   *
   *   IF THERE IS A RESUME ID
   *   for each iterator, position them just past that ID
   *   using FIND.
   *
   *   EACH SUBITERATOR HAS A "PENDING ID" that would be
   *   the next one returned from it.  The iterators are kept
   *   sorted in order of ascending pending id.   The pending ID is
   *   optional; if an iterator doesn't have one, it is
   *   kept at the head of the chain.
   *
   *   RETURNING A PENDING ID "uses it up"; the iterator
   *   who held it (and all its neighbors who also hold the
   *   same ID) are marked as empty.  Since IDs are returned
   *   in order (from the head of the chain), these iterators
   *   start out and remain at the head of the list.
   *
   *   WHEN AN EMPTY ITERATOR IS "FILLED" (by completing
   *   a "next" call on it), it moves towards the back of
   *   the list and is insertion-sorted into its correct slot.
   */
  switch (it->it_call_state) {
    default:
      cl_notreached(cl,
                    "or_iterator_next_loc: "
                    "unexpected state %d [from %s:%d]",
                    it->it_call_state, file, line);

      RESUME_STATE(it, 0)
      gio->gio_sort_me_n = 0;

      or_chain_invariant(it);
      if (gio->gio_resume_id != PDB_ID_NONE) {
        or_activate_all(it);
        gio->gio_this_oc = gio->gio_oc;
        RESUME_STATE(it, 1)
        or_chain_invariant(it);
        while (gio->gio_this_oc < gio->gio_oc + gio->gio_n) {
          pdb_id first_id, found_id;

          err = 0;

          oc = gio->gio_this_oc;
          first_id = gio->gio_resume_id;
          if (pdb_iterator_forward(pdb, oc->oc_it))
            first_id++;
          else {
            if (first_id > 0)
              first_id--;
            else
              err = GRAPHD_ERR_NO;
          }

          err = pdb_iterator_find(pdb, oc->oc_it, first_id, &found_id,
                                  budget_inout);

          if (err == PDB_ERR_MORE) LEAVE_SAVE_STATE(it, 1);

          if (err == GRAPHD_ERR_NO)
            or_retire_oc(gio, oc);

          else if (err != 0) {
            cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_find", err, "id=%llx",
                         (unsigned long long)first_id);
          rxs_pop_error:
            pdb_rxs_pop(pdb, "NEXT %p or %s ($%lld)", (void *)it,
                        graphd_strerror(err),
                        (long long)(budget_in - *budget_inout));
            goto err;
          } else {
            err = or_set_oc_id(gio, oc, found_id);
            if (err != 0) {
              cl_log_errno(cl, CL_LEVEL_FAIL, "or_set_oc_id", err, "id=%llx",
                           (unsigned long long)found_id);
              goto rxs_pop_error;
            }
            if (GRAPHD_SABOTAGE(gio->gio_graphd, *budget_inout <= 0))
              LEAVE_SAVE_STATE(it, 1);
          }
          gio->gio_this_oc++;
        }

        /* OK, we're caught up.
         */
        gio->gio_id = gio->gio_resume_id;
        gio->gio_resume_id = PDB_ID_NONE;
      }
      or_chain_invariant(it);

      RESUME_STATE(it, 2)

      or_chain_invariant(it);
      while (gio->gio_active_head != NULL) {
        char ibuf[200];

        cl_log(cl, CL_LEVEL_VERBOSE,
               "or_iterator_next_loc: "
               "looking at id=%s, it=%s",
               pdb_id_to_string(pdb, gio->gio_active_head->oc_id, ibuf,
                                sizeof ibuf),
               pdb_iterator_to_string(pdb, gio->gio_active_head->oc_it, buf,
                                      sizeof buf));

        /*  If the first subiterator is empty,
         *  fill it and refile it.
         */
        if (gio->gio_active_head->oc_id == PDB_ID_NONE) {
          pdb_id new_id;
          RESUME_STATE(it, 3)
          /*  Refill the empty subiterator.
           */
          oc = gio->gio_active_head;
          err = pdb_iterator_next(pdb, oc->oc_it, &new_id, budget_inout);
          if (err == PDB_ERR_MORE) {
            or_chain_invariant(it);
            or_sort_and_refile(it);
            LEAVE_SAVE_STATE(it, 3);
          }

          /*  If it ran out, move it out of the
           *  active chain and into the "EOF" chain.
           */
          if (err == GRAPHD_ERR_NO) {
            or_retire_oc(gio, oc);
            or_chain_invariant(it);

            if (GRAPHD_SABOTAGE(gio->gio_graphd, *budget_inout < 0))
              LEAVE_SAVE_STATE(it, 2);

            continue;
          } else if (err != 0) {
            or_chain_invariant(it);
            or_sort_and_refile(it);
            or_chain_invariant(it);

            goto rxs_pop_error;
          }

          cl_assert(cl, new_id != PDB_ID_NONE);

          err = or_set_oc_id(gio, oc, new_id);
          if (err != 0) {
            cl_log_errno(cl, CL_LEVEL_FAIL, "or_set_oc_id", err, "new_id=%llx",
                         (unsigned long long)new_id);
            goto rxs_pop_error;
          }

          /*  Out of time?  Take a break.
           */
          if (GRAPHD_SABOTAGE(gio->gio_graphd, *budget_inout <= 0)) {
            or_chain_invariant(it);
            or_sort_and_refile(it);
            LEAVE_SAVE_STATE(it, 0);
          }
          or_chain_invariant(it);
          continue;
        }

        /*  We're standing on the smallest available ID.
         */
        or_chain_invariant(it);
        or_sort_and_refile(it);

      have_smallest_id:
        cl_assert(cl, gio->gio_active_head != NULL);

        oc = gio->gio_active_head;
        cl_assert(cl, oc->oc_id != PDB_ID_NONE);
        *id_out = gio->gio_id = oc->oc_id;

        /*  Invalidate this ID and any others that match it.
         */
        do
          oc->oc_id = PDB_ID_NONE;
        while ((oc = oc->oc_next) != NULL && oc->oc_id == *id_out);

        or_chain_invariant(it);
        pdb_rxs_pop(pdb, "NEXT %p or %llx ($%lld)", (void *)it,
                    (unsigned long long)*id_out,
                    (long long)(budget_in - *budget_inout));
        err = 0;
        goto err;
      }
      or_chain_invariant(it);
      or_sort_and_refile(it);
      if (gio->gio_active_head != NULL) goto have_smallest_id;

      /*  The "next" algorithm terminates when the "active"
       *  list is empty.
       */
      cl_assert(cl, gio->gio_active_head == NULL);
      break;
  }
  gio->gio_eof = true;

  pdb_rxs_pop(pdb, "NEXT %p or eof ($%lld)", (void *)it,
              (long long)(budget_in - *budget_inout));
  err = GRAPHD_ERR_NO;
  goto err;

unsorted:
  /*  The UNSORTED algorithm:
   *
   *   PRODUCTION PHASE:
   *  	Get the next id from the current iterator.
   *	(If we run out, go to the next.)
   *
   *   CHECK PHASE:
   *	If any of the iterators *before* the current
   * 	candidate check() "OK", reject the id.  (We've
   *  	we've produced it before.)
   */
  switch (it->it_call_state) {
    default:
      RESUME_STATE(it, 0)
      while ((oc = gio->gio_active_head) != NULL) {
        pdb_iterator_call_reset(pdb, oc->oc_it);
        RESUME_STATE(it, 1)
        oc = gio->gio_active_head;
        err = pdb_iterator_next(pdb, oc->oc_it, &oc->oc_id, budget_inout);

        if (err == PDB_ERR_MORE) LEAVE_SAVE_STATE(it, 1);

        if (err == GRAPHD_ERR_NO) {
          /*  We ran out of this one.  Move it
           *  to the EOF chain, where it'll spend a
           *  useful and rewarding retirement checking
           *  the IDs from the remaining subiterators
           *  for duplicates.
           */
          cl_log(cl, CL_LEVEL_VERBOSE, "or_iterator_next: done with %s",
                 pdb_iterator_to_string(pdb, oc->oc_it, buf, sizeof buf));
          or_retire_oc(gio, oc);
          continue;
        }
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "it=%s",
                       pdb_iterator_to_string(pdb, oc->oc_it, buf, sizeof buf));
          goto rxs_pop_error;
        }
        cl_assert(cl, oc->oc_id != PDB_ID_NONE);

        /*  See if any of the EOF'ed iterators checks
         *  this ID.  If yes, we need another one -- we
         *  already returned this one when the EOF'ed
         *  iterator was running!
         */
        gio->gio_check_id = oc->oc_id;
        for (gio->gio_this_oc = gio->gio_eof_head; gio->gio_this_oc != NULL;
             gio->gio_this_oc = oc->oc_next) {
          oc = gio->gio_this_oc;
          pdb_iterator_call_reset(pdb, oc->oc_it);
          RESUME_STATE(it, 2)
          oc = gio->gio_this_oc;
          err = pdb_iterator_check(pdb, oc->oc_it, gio->gio_check_id,
                                   budget_inout);
          if (err == 0)
            break;

          else if (err == PDB_ERR_MORE)
            LEAVE_SAVE_STATE(it, 2);

          else if (err != GRAPHD_ERR_NO) {
            cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_check", err,
                         "id=%llx", (unsigned long long)gio->gio_check_id);
            goto rxs_pop_error;
          }
        }
        if (gio->gio_this_oc == NULL) {
          /*  Not a duplicate.  Yay!
           */
          gio->gio_id = gio->gio_check_id;

          /*  Final hurdle: are we waiting for a
           *  last known ID to float past so we
           *  can resume in an unsorted ID stream?
           */
          if (gio->gio_resume_id == PDB_ID_NONE) {
            *id_out = gio->gio_id;
            pdb_rxs_pop(pdb, "NEXT %p or %llx ($%lld)", (void *)it,
                        (unsigned long long)*id_out,
                        (long long)(budget_in - *budget_inout));
            err = 0;
            goto err;
          }
          if (gio->gio_id == gio->gio_resume_id)
            gio->gio_resume_id = PDB_ID_NONE;
        }
        cl_log(cl, CL_LEVEL_VERBOSE,
               "or_iterator_next_loc: %llx is a "
               "duplicate/resume; skipped.",
               (unsigned long long)gio->gio_check_id);

        if (GRAPHD_SABOTAGE(gio->gio_graphd, *budget_inout <= 0))
          LEAVE_SAVE_STATE(it, 0);
      }
      break;
  }
  gio->gio_eof = true;
  pdb_rxs_pop(pdb, "NEXT %p or EOF ($%lld)", (void *)it,
              (long long)(budget_in - *budget_inout));
  err = GRAPHD_ERR_NO;

err:
  pdb_iterator_account_charge_budget(pdb, it, next);
  return err;
}

static int or_freeze_subcondition_state(graphd_handle *g,
                                        graphd_or_subcondition *oc,
                                        cm_buffer *buf) {
  int err;
  pdb_handle *pdb = g->g_pdb;
  char b1[200];

  /*  If the subcondition's original has evolved,
   *  we know that we cannot have progressed into it
   *  in the instance, and can safely reclone it here.
   */
  err = pdb_iterator_refresh_pointer(pdb, &oc->oc_it);
  if (err != 0 && err != PDB_ERR_ALREADY) {
    cl_log_errno(g->g_cl, CL_LEVEL_FAIL, "pdb_iterator_refresh_pointer", err,
                 "%s", pdb_iterator_to_string(pdb, oc->oc_it, b1, sizeof b1));
    return err;
  }

  err = cm_buffer_sprintf(
      buf, "(%s.",
      oc->oc_eof ? "$" : pdb_id_to_string(pdb, oc->oc_id, b1, sizeof b1));
  if (err != 0) return err;

  err = pdb_iterator_freeze(
      g->g_pdb, oc->oc_it,
      PDB_ITERATOR_FREEZE_POSITION | PDB_ITERATOR_FREEZE_STATE, buf);
  if (err != 0) return err;

  return cm_buffer_add_string(buf, ")");
}

static int or_iterator_freeze(pdb_handle *pdb, pdb_iterator *it,
                              unsigned int flags, cm_buffer *buf) {
  graphd_iterator_or *gio = it->it_theory;
  graphd_iterator_or *ogio;
  cl_handle *cl = gio->gio_cl;
  graphd_or_subcondition *oc;
  size_t b0 = cm_buffer_length(buf);
  size_t i;
  int err;
  char const *sep = "";

  if (graphd_request_timer_check(gio->gio_greq)) return GRAPHD_ERR_TOO_HARD;

  if (it->it_original->it_type != it->it_type)
    return pdb_iterator_freeze(pdb, it->it_original, flags, buf);

  ogio = it->it_original->it_theory;
  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");
  if (flags & PDB_ITERATOR_FREEZE_SET) {
    if (ogio->gio_masquerade != NULL &&
        !(flags & GRAPHD_OR_FREEZE_WITHOUT_MASQUERADE)) {
      err = cm_buffer_sprintf(buf, "or:(%s)", ogio->gio_masquerade);
      if (err != 0) return err;
    } else {
      if ((err = pdb_iterator_freeze_intro(buf, it, "or")) != 0 ||
          (err = cm_buffer_sprintf(buf, ":%zu:", ogio->gio_n)) != 0)
        return err;

      for (i = ogio->gio_n, oc = ogio->gio_oc; i--; oc++) {
        /*  Regardless of what our flags are, we're
         *  only freezing the "set" component of the
         *  subiterators here.  Their positions and
         *  states will be part of the OR iterator's
         *  state.
         */
        if ((err = cm_buffer_add_string(buf, "(")) != 0 ||
            (err = pdb_iterator_freeze(pdb, oc->oc_it, PDB_ITERATOR_FREEZE_SET,
                                       buf)) != 0 ||
            (err = cm_buffer_add_string(buf, ")")) != 0)
          return err;
      }
    }

    err = pdb_iterator_freeze_ordering(pdb, buf, it);
    if (err != 0) return err;

    err = pdb_iterator_freeze_account(pdb, buf, it);
    if (err != 0) return err;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    err = cm_buffer_add_string(buf, sep);
    if (err != 0) return err;

    if (gio->gio_resume_id != PDB_ID_NONE) {
      cl_log(cl, CL_LEVEL_ERROR,
             "FYI - freeze during resume?  That's not good...");
    }

    err = graphd_iterator_util_freeze_position(pdb, gio->gio_eof, gio->gio_id,
                                               gio->gio_resume_id, buf);
    if (err != 0) return err;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_STATE) {
    err = cm_buffer_add_string(buf, sep);
    if (err != 0) return err;

    if (ogio->gio_masquerade != NULL &&
        !(flags & GRAPHD_OR_FREEZE_WITHOUT_MASQUERADE)) {
      /* In the masquerade case, the whole
       * OR set and state become our "state".
       * (We don't need another copy of our position.)
       */
      if ((err = cm_buffer_add_string(buf, "(")) != 0 ||
          (err = pdb_iterator_freeze(
               pdb, it, PDB_ITERATOR_FREEZE_SET | PDB_ITERATOR_FREEZE_STATE |
                            GRAPHD_OR_FREEZE_WITHOUT_MASQUERADE,
               buf)) != 0 ||
          (err = cm_buffer_add_string(buf, ")")) != 0)
        return err;
    } else {
      for (i = gio->gio_n, oc = gio->gio_oc; i--; oc++) {
        err = or_freeze_subcondition_state(gio->gio_graphd, oc, buf);
        if (err != 0) return err;
      }

      /*  	:THIS_OC
       */
      if (gio->gio_this_oc != NULL)
        err = cm_buffer_sprintf(buf, ":%zu",
                                (size_t)(gio->gio_this_oc - gio->gio_oc));
      else
        err = cm_buffer_add_string(buf, ":-");
      if (err != 0) return err;

      /*  	:STATISTICS
       */
      if (pdb_iterator_statistics_done(pdb, it)) {
        /*  Statistics results.
         *
         *  :CHECK:NEXT[+FIND]:N
         */
        if (pdb_iterator_sorted(pdb, it))
          err = cm_buffer_sprintf(buf, ":%lld:%lld+%lld:%lld",
                                  (long long)pdb_iterator_check_cost(pdb, it),
                                  (long long)pdb_iterator_next_cost(pdb, it),
                                  (long long)pdb_iterator_find_cost(pdb, it),
                                  (long long)pdb_iterator_n(pdb, it));
        else
          err = cm_buffer_sprintf(buf, ":%lld:%lld:%lld",
                                  (long long)pdb_iterator_check_cost(pdb, it),
                                  (long long)pdb_iterator_next_cost(pdb, it),
                                  (long long)pdb_iterator_n(pdb, it));
        if (err != 0) return err;
      }
    }
  }

  cl_leave(cl, CL_LEVEL_VERBOSE, "%.*s", (int)(cm_buffer_length(buf) - b0),
           cm_buffer_memory(buf) + b0);
  return 0;
}

static int or_iterator_reset(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_or *gio = it->it_theory;
  graphd_or_subcondition *oc;
  size_t i;
  int err;

  gio->gio_eof = false;
  gio->gio_id = PDB_ID_NONE;
  gio->gio_resume_id = PDB_ID_NONE;

  /*  Reset the OCs.
   */
  for (i = gio->gio_n, oc = gio->gio_oc; i--; oc++)
    if ((err = pdb_iterator_reset(pdb, oc->oc_it)) != 0) {
      char buf[200];
      cl_log_errno(gio->gio_cl, CL_LEVEL_FAIL, "pdb_iterator_reset", err,
                   "it=%s",
                   pdb_iterator_to_string(pdb, oc->oc_it, buf, sizeof buf));

      return err;
    }

  or_activate_all(it);
  or_chain_invariant(it);

  pdb_iterator_call_reset(pdb, it);

  return 0;
}

static int or_subcondition_clone(graphd_iterator_or *gio,
                                 graphd_or_subcondition *oc,
                                 graphd_or_subcondition *oc_out) {
  cl_handle *cl = gio->gio_cl;
  pdb_handle *pdb = gio->gio_pdb;
  int err;

  PDB_IS_ITERATOR(cl, oc->oc_it);
  cl_assert(cl, oc != oc_out);

  oc_out->oc_id = oc->oc_id;
  oc_out->oc_eof = oc->oc_eof;
  oc_out->oc_it = NULL;
  oc_out->oc_next = NULL;
  oc_out->oc_prev = NULL;

  if ((err = pdb_iterator_clone(pdb, oc->oc_it, &oc_out->oc_it)) != 0) {
    char buf[200];

    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "it=%s",
                 pdb_iterator_to_string(pdb, oc->oc_it, buf, sizeof buf));
    return err;
  }
  PDB_IS_ITERATOR(cl, oc->oc_it);

  return 0;
}

static int or_iterator_clone(pdb_handle *pdb, pdb_iterator *it,
                             pdb_iterator **it_out) {
  graphd_iterator_or *gio = it->it_theory;
  graphd_iterator_or *gio_out;
  cm_handle *cm = gio->gio_cm;
  cl_handle *cl = gio->gio_cl;
  size_t i, n;
  int err;
  char buf[200];

  PDB_IS_ITERATOR(cl, it);
  PDB_IS_ORIGINAL_ITERATOR(cl, it->it_original);

  /*  Or iterators do not evolve.  (They do all their
   *  evolving in their create_commit constructor.)
   */
  cl_assert(cl, it->it_type == it->it_original->it_type);
  cl_assert(cl, gio->gio_n > 0);

  cl_log(cl, CL_LEVEL_SPEW, "or_iterator_clone(%s)",
         pdb_iterator_to_string(pdb, it, buf, sizeof buf));

  if ((gio_out = cm_malcpy(cm, gio, sizeof(*gio))) == NULL)
    return errno ? errno : ENOMEM;

  /* Use that of the original */
  gio_out->gio_masquerade = NULL;
  gio_out->gio_check_it = NULL;

  cl_assert(cl, gio->gio_sort_me_n == 0);
  gio_out->gio_sort_me =
      cm_malloc(cm, gio->gio_n * sizeof(*gio_out->gio_sort_me));
  if (gio_out->gio_sort_me == NULL) {
    err = errno ? errno : ENOMEM;
    cm_free(cm, gio_out);
    return err;
  }

  n = gio->gio_n;
  gio_out->gio_oc = cm_malcpy(cm, gio->gio_oc, n * sizeof(*gio->gio_oc));
  if (gio_out->gio_oc == NULL) {
    cm_free(cm, gio_out);
    cm_free(cm, gio_out->gio_sort_me);

    *it_out = NULL;
    return errno ? errno : ENOMEM;
  }
  gio_out->gio_m = gio_out->gio_n = n;

  for (i = 0; i < n; i++) {
    PDB_IS_ITERATOR(cl, gio->gio_oc[i].oc_it);
    err = or_subcondition_clone(gio, gio->gio_oc + i, gio_out->gio_oc + i);
    if (err != 0) {
      while (i-- > 0) pdb_iterator_destroy(pdb, &gio->gio_oc[i].oc_it);
      cm_free(cm, gio_out->gio_oc);
      *it_out = NULL;

      return err;
    }
  }

  if ((err = pdb_iterator_make_clone(pdb, it->it_original, it_out)) != 0) {
    for (i = 0; i < gio->gio_n; i++) {
      pdb_iterator_destroy(pdb, &gio_out->gio_oc[i].oc_it);
      cm_free(cm, gio_out->gio_oc);

      return err;
    }
    return err;
  }
  (*it_out)->it_has_position = true;
  (*it_out)->it_theory = gio_out;

  or_refile_all(*it_out);
  or_chain_invariant(*it_out);

  cl_log(cl, CL_LEVEL_VERBOSE, "or_iterator_clone: %p -> %p [ref=%d]",
         (void *)it, (void *)*it_out, (int)(*it_out)->it_original->it_refcount);

  return 0;
}

static void or_iterator_finish(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_or *gio = it->it_theory;
  size_t i;

  if (gio != NULL) {
    cl_cover(gio->gio_cl);
    pdb_iterator_destroy(gio->gio_pdb, &gio->gio_fixed);
    pdb_iterator_destroy(gio->gio_pdb, &gio->gio_check_it);
    if (gio->gio_oc != NULL) {
      graphd_or_subcondition *oc;

      for (i = gio->gio_n, oc = gio->gio_oc; i--; oc++)
        pdb_iterator_destroy(gio->gio_pdb, &oc->oc_it);

      cm_free(gio->gio_cm, gio->gio_oc);
    }
    cm_free(gio->gio_cm, it->it_displayname);
    it->it_displayname = NULL;

    cm_free(gio->gio_cm, gio->gio_sort_me);
    cm_free(gio->gio_cm, gio->gio_masquerade);
    cm_free(gio->gio_cm, gio);

    it->it_theory = NULL;
  }
  it->it_type = NULL;
  it->it_magic = 0;
  it->it_original = NULL;
}

static char const *or_iterator_to_string(pdb_handle *pdb, pdb_iterator *it,
                                         char *buf, size_t size) {
  graphd_iterator_or *gio = it->it_theory;
  char sub_buf[200], id_buf[50], *w;
  int i;
  graphd_or_subcondition *oc;
  char const *separator = "or[";

  if (gio == NULL || gio->gio_n == 0) return "or:null";

  if (size <= 10) return "or:[..]";

  w = buf;
  for (i = 0, oc = gio->gio_oc; i < 3 && i < gio->gio_n; i++, oc++) {
    if (oc->oc_id == PDB_ID_NONE)
      snprintf(w, size, "%s%s", separator,
               pdb_iterator_to_string(pdb, oc->oc_it, sub_buf, sizeof sub_buf));
    else
      snprintf(w, size, "%s%s.%s", separator,
               pdb_id_to_string(pdb, oc->oc_id, id_buf, sizeof id_buf),
               pdb_iterator_to_string(pdb, oc->oc_it, sub_buf, sizeof sub_buf));
    separator = " | ";

    while (size > 4 && *w != '\0') w++, size--;

    if (size <= 15) {
      snprintf(w, size, "..]");
      return buf;
    }
  }
  snprintf(w, size, i >= gio->gio_n ? "]" : "..]");
  return buf;
}

/**
 * @brief Return the primitive summary for an OR iterator.
 *
 * @param pdb		module handle
 * @param it		an and iterator
 * @param psum_out	out: summary
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int or_iterator_primitive_summary(pdb_handle *pdb, pdb_iterator *it,
                                         pdb_primitive_summary *psum_out) {
  int err;
  size_t i;
  bool have_result = false;
  graphd_iterator_or *gio;

  /*  Defer to the original.  It may have a different type.
   */
  if (it->it_original != it)
    return pdb_iterator_primitive_summary(pdb, it->it_original, psum_out);

  gio = it->it_theory;

  /*  Use a cached summary?
   */
  if (gio->gio_primitive_summary_tried) {
    if (!gio->gio_primitive_summary_successful) return PDB_ERR_NO;

    *psum_out = gio->gio_primitive_summary;
    return 0;
  }

  /*  Assume that we fail, and remember that we tried.
   *  If we actually do come up with a summary,
   *  we'll set gio_primitive_summary_successful to true.
   */
  gio->gio_primitive_summary_tried = true;
  gio->gio_primitive_summary_successful = false;

  psum_out->psum_result = 0;
  psum_out->psum_complete = true;

  for (i = 0; i < gio->gio_n; i++) {
    pdb_primitive_summary sub;
    int l;

    err = pdb_iterator_primitive_summary(pdb, gio->gio_oc[i].oc_it, &sub);
    if (err != 0) {
      if (err == GRAPHD_ERR_NO) return GRAPHD_ERR_NO;
      return err;
    }

    if (!have_result) {
      *psum_out = sub;
      have_result = true;

      continue;
    }

    if (psum_out->psum_result != sub.psum_result) return GRAPHD_ERR_NO;

    psum_out->psum_complete &= sub.psum_complete;
    psum_out->psum_locked &= sub.psum_locked;

    /*  If linkages contradict each other, drop them.
     */
    for (l = 0; l < PDB_LINKAGE_N; l++)
      if (psum_out->psum_locked & (1 << l)) {
        if (!GRAPH_GUID_EQ(psum_out->psum_guid[l], sub.psum_guid[l])) {
          psum_out->psum_locked &= ~(1 << l);
          psum_out->psum_complete = false;
        }
      }
    if (!psum_out->psum_locked) return GRAPHD_ERR_NO;
  }

  if (!psum_out->psum_locked || !have_result) return GRAPHD_ERR_NO;

  /*  Cache the result for later.
   */
  gio->gio_primitive_summary_successful = true;
  gio->gio_primitive_summary = *psum_out;

  return 0;
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
static int or_iterator_beyond(pdb_handle *pdb, pdb_iterator *it, char const *s,
                              char const *e, bool *beyond_out) {
  graphd_iterator_or *gio = it->it_theory;
  pdb_id id;

  /*  Something is out of sync?
   */
  if (!pdb_iterator_statistics_done(pdb, it) ||
      it->it_id != it->it_original->it_id || !pdb_iterator_ordered(pdb, it)) {
    cl_log(gio->gio_cl, CL_LEVEL_VERBOSE,
           "or_iterator_beyond: %s - returning false",
           !pdb_iterator_statistics_done(pdb, it)
               ? "no statistics yet"
               : (it->it_id != it->it_original->it_id
                      ? "original and instance ids don't match"
                      : "iterator isn't ordered"));

    *beyond_out = false;
    return 0;
  }

  if (!pdb_iterator_sorted(pdb, it)) {
    cl_log(gio->gio_cl, CL_LEVEL_VERBOSE,
           "or_iterator_beyond: "
           "not sorted, no clue");
    *beyond_out = false;
    return 0;
  }

  if (e - s != sizeof(id)) {
    *beyond_out = false;
    cl_log(gio->gio_cl, CL_LEVEL_ERROR,
           "or_iterator_beyond: unexpected "
           "value size (%zu bytes; expected %zu)",
           (size_t)(e - s), sizeof(id));
    return GRAPHD_ERR_SEMANTICS;
  }

  if (gio->gio_id == PDB_ID_NONE) {
    cl_log(gio->gio_cl, CL_LEVEL_VERBOSE,
           "or_iterator_beyond: "
           "still at the beginning");
    *beyond_out = false;
    return 0;
  }

  memcpy(&id, s, (size_t)(e - s));
  *beyond_out =
      (pdb_iterator_forward(pdb, it) ? id < gio->gio_id : id > gio->gio_id);
  cl_log(gio->gio_cl, CL_LEVEL_VERBOSE, "or_iterator_beyond: %s",
         (*beyond_out ? "yes, we're done" : "no, not yet"));
  return 0;
}

static int or_iterator_partial_dup(pdb_iterator const *or_in, size_t first_n,
                                   pdb_iterator **it_out) {
  pdb_handle *pdb = ogio(or_in)->gio_pdb;
  size_t i;
  int err;

  err = graphd_iterator_or_create(ogio(or_in)->gio_greq, ogio(or_in)->gio_n,
                                  pdb_iterator_forward(pdb, or_in), it_out);
  if (err != 0) return err;

  for (i = 0; i < first_n; i++) {
    pdb_iterator *it_clone;

    err = pdb_iterator_clone(pdb, ogio(or_in)->gio_oc[i].oc_it, &it_clone);
    if (err != 0) return err;

    err = graphd_iterator_or_add_subcondition(*it_out, &it_clone);
    pdb_iterator_destroy(pdb, &it_clone);
    if (err != 0) return err;
  }
  return 0;
}

static int or_iterator_restrict(pdb_handle *pdb, pdb_iterator *it,
                                pdb_primitive_summary const *psum,
                                pdb_iterator **it_out) {
  size_t n_conflicting = 0, i;
  int err = 0;
  pdb_iterator *pending_it = NULL;
  pdb_iterator *res_it = NULL;
  cl_handle *cl = ogio(it)->gio_cl;
  char buf[200];

  /*  If one of our subconstraints has a primitive
   *  summary that directly contradicts {psum}, make
   *  a new "or" iterator that doesn't contain that
   *  subconstraint.  Or any of the others that contradict
   *  it.
   */

  *it_out = NULL;
  for (i = 0; i < ogio(it)->gio_n; i++) {
    err = pdb_iterator_restrict(pdb, ogio(it)->gio_oc[i].oc_it, psum, &res_it);

    /*  Most common case: they're all included unchanged?
     */
    if (n_conflicting == 0 && *it_out == NULL && err == PDB_ERR_ALREADY)
      continue;

    /*  They're all conflicting, or there's one
     *  special case so far, and this one is conflicted?
     */
    if (err == PDB_ERR_NO &&
        (n_conflicting == i || *it_out != NULL || pending_it != NULL)) {
      n_conflicting++;
      continue;
    }

    if (err == PDB_ERR_ALREADY) {
      cl_assert(cl, res_it == NULL);

      /*  Make the accepted iterator a clone.
       */
      err = pdb_iterator_clone(pdb, ogio(it)->gio_oc[i].oc_it, &res_it);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "it=%s",
                     pdb_iterator_to_string(pdb, ogio(it)->gio_oc[i].oc_it, buf,
                                            sizeof buf));
        goto err;
      }
    }

    if (err != PDB_ERR_NO && err != 0) goto err;

    /*  This is the first rejected or modified one
     *  after a series of accepted ones?
     */
    if (*it_out == NULL && pending_it == NULL && i > 0 && n_conflicting < i) {
      int e2;

      cl_assert(cl, n_conflicting == 0);
      if (i == 1) {
        e2 = pdb_iterator_clone(pdb, ogio(it)->gio_oc->oc_it, &pending_it);
        if (e2 != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "it=%s",
                       pdb_iterator_to_string(pdb, ogio(it)->gio_oc->oc_it, buf,
                                              sizeof buf));
          err = e2;
          goto err;
        }
      } else {
        /*  Make an OR with accepted subiterators
         *  up to, excluding, i.
         */
        e2 = or_iterator_partial_dup(it, i, it_out);
        if (e2 != 0) {
          err = e2;
          goto err;
        }
      }
    }
    if (err == PDB_ERR_NO) {
      n_conflicting++;
      continue;
    }

    cl_assert(cl, err == 0);
    cl_assert(cl, res_it != NULL);

    /*  If we will have to keep track of two iterators,
     *  create an "or" to hold them.  If there was a pending
     *  single iterator, fold it into the "or".
     */
    if (pending_it != NULL && *it_out == NULL) {
      err = or_iterator_partial_dup(it, 0, it_out);
      if (err != 0) goto err;

      err = graphd_iterator_or_add_subcondition(*it_out, &pending_it);
      pdb_iterator_destroy(pdb, &pending_it);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_add_subcondition",
                     err, "it=%s",
                     pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
        goto err;
      }
    }

    /*  We've transferred all the information we were
     *  keeping track of in <i, n_conflicting> into <*it_out>.
     *  <pending_it>, if needed, has been moved.
     */
    cl_assert(cl, res_it != NULL);
    if (*it_out) {
      err = graphd_iterator_or_add_subcondition(*it_out, &res_it);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_add_subcondition",
                     err, "it=%s",
                     pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
        goto err;
      }
    } else {
      pending_it = res_it;
      res_it = NULL;
    }
  }

  if (*it_out == NULL) {
    if (pending_it != NULL) {
      *it_out = pending_it;
      return 0;
    }
    return n_conflicting == ogio(it)->gio_n ? PDB_ERR_NO : PDB_ERR_ALREADY;
  }

  graphd_iterator_or_create_commit(*it_out);
  return 0;

err:
  pdb_iterator_destroy(pdb, &pending_it);
  pdb_iterator_destroy(pdb, &res_it);
  pdb_iterator_destroy(pdb, it_out);

  return err;
}

static int or_range_estimate(pdb_handle *pdb, pdb_iterator *it,
                             pdb_range_estimate *range) {
  graphd_iterator_or *gio = ogio(it);
  pdb_range_estimate sub_range;
  int err;
  char buf[200];
  size_t i;

  /*  Initialize with minimum permissive values.
   */
  range->range_low = PDB_ITERATOR_HIGH_ANY;
  range->range_high = PDB_ITERATOR_LOW_ANY;
  range->range_n_exact = PDB_COUNT_UNBOUNDED;
  range->range_n_max = 0;

  /*  Something is out of sync?
   */
  if (!pdb_iterator_statistics_done(pdb, it) ||
      it->it_id != it->it_original->it_id)
    return 0;

  for (i = 0; i < gio->gio_n; i++) {
    err = pdb_iterator_range_estimate(pdb, gio->gio_oc[i].oc_it, &sub_range);
    if (err != 0) {
      if (err != PDB_ERR_NO) {
        cl_log_errno(
            gio->gio_cl, CL_LEVEL_FAIL, "pdb_iterator_range_estimate", err,
            "it=%s",
            pdb_iterator_to_string(pdb, gio->gio_oc[i].oc_it, buf, sizeof buf));
        return err;
      }

      /*  Since we don't know one of our subconstraints
       *  well enough to give estimates, widen the range
       *  estimate to the largest permissible.
       */
      range->range_n_max = PDB_COUNT_UNBOUNDED;
      if (range->range_low > it->it_low) range->range_low = it->it_low;
      if (range->range_high < it->it_high) range->range_low = it->it_high;
      return 0;
    }

    if (sub_range.range_low < range->range_low)
      range->range_low = sub_range.range_low;

    if (sub_range.range_high > range->range_high)
      range->range_high = sub_range.range_high;

    if (sub_range.range_n_max == PDB_COUNT_UNBOUNDED)
      range->range_n_max = PDB_COUNT_UNBOUNDED;
    else if (range->range_n_max != PDB_COUNT_UNBOUNDED)
      range->range_n_max += sub_range.range_n_max;
  }
  return 0;
}

static const pdb_iterator_type or_iterator_type = {
    "or",

    or_iterator_finish,
    or_iterator_reset,
    or_iterator_clone,
    or_iterator_freeze,
    or_iterator_to_string,

    or_iterator_next_loc,
    or_iterator_find_loc,
    or_iterator_check,
    or_iterator_statistics,

    NULL,
    or_iterator_primitive_summary,
    or_iterator_beyond,
    or_range_estimate,
    or_iterator_restrict,

    NULL, /* suspend */
    NULL, /* unsuspend */
};

/**
 * @brief Create an "or" iterator.
 *
 * @param graphd	server for whom we're doing this
 * @param n		number of subconditions we want to manage.
 * @param low		low end of the value space (first included)
 * @param high		high end of the value space (first not included)
 * @param forward	running from low to high?
 * @param it_out	assign result to this.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_or_create_loc(graphd_request *greq, size_t n, bool forward,
                                  pdb_iterator **it_out, char const *file,
                                  int line) {
  graphd_handle *graphd = graphd_request_graphd(greq);
  pdb_iterator *it = NULL;
  graphd_iterator_or *gio = NULL;
  cm_handle *cm = pdb_mem(graphd->g_pdb);
  cl_handle *cl = graphd_request_cl(greq);

  if ((it = cm_malloc(cm, sizeof(*it))) == NULL ||
      (gio = cm_zalloc(cm, sizeof(*gio))) == NULL ||
      (n > 0 &&
       ((gio->gio_oc = cm_zalloc(cm, sizeof(*gio->gio_oc) * n)) == NULL))) {
    int err = errno ? errno : ENOMEM;

    cm_free(cm, it);
    cm_free(cm, gio);

    return err;
  }

  if (n == 0) gio->gio_oc = NULL;

  gio->gio_magic = GRAPHD_OR_MAGIC;
  gio->gio_graphd = graphd;
  gio->gio_pdb = graphd->g_pdb;
  gio->gio_greq = greq;
  gio->gio_cl = cl;
  gio->gio_cm = cm;
  gio->gio_n = 0;
  gio->gio_m = n;
  gio->gio_id = PDB_ID_NONE;
  gio->gio_resume_id = PDB_ID_NONE;
  gio->gio_check_id = PDB_ID_NONE;
  gio->gio_sort_me = NULL;
  gio->gio_sort_me_n = 0;
  gio->gio_this_oc = NULL;
  gio->gio_statistics_oc = NULL;
  gio->gio_fixed = NULL;

  pdb_iterator_make_loc(graphd->g_pdb, it, 0, PDB_ITERATOR_HIGH_ANY, forward,
                        file, line);

  it->it_theory = gio;
  it->it_type = &or_iterator_type;

  PDB_IS_ITERATOR(cl, it);
  GRAPHD_IS_OR(cl, gio);

  cl_log(cl, CL_LEVEL_SPEW, "graphd_iterator_or_create(up to %d slots): %p",
         (int)gio->gio_m, (void *)it);
  *it_out = it;

  return 0;
}

static int or_add_subcondition(pdb_iterator *it, pdb_iterator **sub) {
  graphd_iterator_or *gio = it->it_theory;
  pdb_handle *pdb = gio->gio_graphd->g_pdb;
  cl_handle *cl = gio->gio_cl;
  graphd_or_subcondition *oc;

  if (gio->gio_n >= gio->gio_m) {
    size_t need = gio->gio_n + 16;
    cm_handle *cm = gio->gio_cm;

    /*  Make space for a few more subiterator elements.
     */
    oc = cm_realloc(cm, gio->gio_oc, need * sizeof(*gio->gio_oc));
    if (oc == NULL) {
      int err = errno ? errno : ENOMEM;
      cl_log_errno(cl, CL_LEVEL_FAIL, "cm_realloc", errno,
                   "failed to reallocate space for %zu "
                   "subconditions",
                   need);
      return err;
    }
    gio->gio_oc = oc;
    gio->gio_m = gio->gio_n + 16;
  }

  /*  Expand the or's it_low, as
   *  needed to accomodate the subiterator.
   */
  if (gio->gio_n == 0) {
    /* First subiterator.  Adopt its boundaries. */

    it->it_low = (*sub)->it_low;
    it->it_high = (*sub)->it_high;
  } else {
    /* N+1st subiterator.  Expand to fit it.  */
    if (it->it_low > (*sub)->it_low) it->it_low = (*sub)->it_low;
    if (it->it_high < (*sub)->it_high) it->it_high = (*sub)->it_high;
  }
  cl_assert(cl, gio->gio_n < gio->gio_m);
  oc = gio->gio_oc + gio->gio_n++;

  or_subcondition_initialize(gio, oc);

  /*  Accounts are inherited.
   */
  if (pdb_iterator_account(pdb, it) != NULL &&
      pdb_iterator_account(pdb, *sub) == NULL)

    pdb_iterator_account_set(pdb, *sub, pdb_iterator_account(pdb, it));

  /* Move the iterator into the subcondition; zero out
   * the incoming iterator.
   */
  oc->oc_it = *sub;
  *sub = NULL;

  PDB_IS_ITERATOR(cl, oc->oc_it);
  PDB_IS_ITERATOR(cl, it);

  return 0;
}

static int or_merge_complete(pdb_iterator *it) {
  graphd_iterator_or *gio = it->it_theory;

  if (gio->gio_fixed == NULL) return 0;

  graphd_iterator_fixed_create_commit(gio->gio_fixed);
  return or_add_subcondition(it, &gio->gio_fixed);
}

/**
 * @brief Set a check delegate for an "or" iterator.
 *
 *  	It is up to the masquerade string to make sure the
 *  	check delegate survives freezing/thawing.
 */
int graphd_iterator_or_set_check(pdb_iterator *it, pdb_iterator **check_it) {
  if (it->it_original->it_type != &or_iterator_type)
    return PDB_ERR_NOT_SUPPORTED;

  pdb_iterator_destroy(ogio(it)->gio_pdb, &ogio(it)->gio_check_it);
  ogio(it)->gio_check_it = *check_it;
  *check_it = NULL;

  return 0;
}

/**
 * @brief Finish creating an "or" structure.
 *
 * @param it	the completed iterator
 * @return 0 on success, a nonzero error code on error
 */
int graphd_iterator_or_create_commit(pdb_iterator *it) {
  graphd_iterator_or *gio = it->it_theory;
  graphd_or_subcondition *oc;
  cl_handle *cl = gio->gio_cl;
  pdb_handle *pdb = gio->gio_pdb;
  pdb_iterator *new_it;
  char buf[200];
  int err;
  unsigned long long upper_bound;
  size_t i;

  PDB_IS_ITERATOR(cl, it);

  if ((upper_bound = pdb_primitive_n(pdb)) == 0) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_or_create_commit: "
           "becoming null");

    pdb_iterator_null_become(pdb, it);
    return 0;
  }

  cl_enter(cl, CL_LEVEL_VERBOSE, "(%p:%s)", it,
           pdb_iterator_to_string(pdb, it, buf, sizeof buf));
  cl_assert(cl, it->it_type == &or_iterator_type);

  /*  Merge the fixed iterator we've been accumulating.
   */
  if (!gio->gio_thaw && (err = or_merge_complete(it)) != 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE,
             "unexpected "
             "error from or_merge_complete: %s",
             graphd_strerror(err));
    return err;
  }

  /*  OR(0)	->	0
   */
  if (gio->gio_n == 0) {
    /* No conditions - return nothing. */

    if ((err = pdb_iterator_null_create(pdb, &new_it)) != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE,
               "unexpected "
               "error from pdb_iterator_null_create: %s",
               graphd_strerror(err));
      return err;
    }
    err = graphd_iterator_substitute(gio->gio_greq, it, new_it);
    cl_assert(cl, err == 0);

    cl_leave(cl, CL_LEVEL_VERBOSE, "null");
    return 0;
  }

  /* OR(X)	->	X
   */
  if (gio->gio_n == 1) {
    /* Replace the OR with its only subcondition. */
    pdb_iterator_dup(pdb, gio->gio_oc[0].oc_it);
    err = graphd_iterator_substitute(gio->gio_greq, it, gio->gio_oc[0].oc_it);
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "substitute failed");
      return err;
    }
    PDB_IS_ITERATOR(cl, it);
    cl_leave(cl, CL_LEVEL_VERBOSE, "became %s",
             pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    return 0;
  }

  cl_assert(cl, it->it_type == &or_iterator_type);

  if (!gio->gio_thaw && (err = or_become_small_set(it)) != PDB_ERR_MORE) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "smallset");
    return err;
  }

  cl_assert(cl, it->it_type == &or_iterator_type);

  if (!pdb_iterator_statistics_done(pdb, it)) {
    unsigned long long total_cc, total_fc;
    unsigned long long total_n, total_nc;
    bool sorted, n_valid, nc_valid, cc_valid, fc_valid;
    /*  We're sorted if all our subiterators are sorted.
     *  Infer statistics.
     */
    sorted = true;
    n_valid = true;
    total_n = 0;
    total_nc = 0;
    total_cc = 0;
    total_fc = 0;
    nc_valid = true;
    cc_valid = true;
    fc_valid = true;

    gio->gio_sort_me =
        cm_malloc(gio->gio_cm, sizeof(*gio->gio_sort_me) * gio->gio_n);
    if (gio->gio_sort_me == NULL) return errno ? errno : ENOMEM;

    for (i = gio->gio_n, oc = gio->gio_oc; i--; oc++) {
      sorted &= !!pdb_iterator_sorted(pdb, oc->oc_it);
      n_valid &= !!pdb_iterator_n_valid(pdb, oc->oc_it);
      if (n_valid) {
        unsigned long long const n = pdb_iterator_n(pdb, oc->oc_it);

        total_n += n;
        nc_valid &= !!pdb_iterator_next_cost_valid(pdb, oc->oc_it);
        if (nc_valid) total_nc += n * pdb_iterator_next_cost(pdb, oc->oc_it);
        cc_valid &= !!pdb_iterator_check_cost_valid(pdb, oc->oc_it);
        if (cc_valid) total_cc += pdb_iterator_check_cost(pdb, oc->oc_it);

        fc_valid &= !!pdb_iterator_find_cost_valid(pdb, oc->oc_it);
        if (fc_valid) total_fc += pdb_iterator_find_cost(pdb, oc->oc_it);
      }
    }

    /* If we already know our statistics, publish them.
     */
    pdb_iterator_sorted_set(pdb, it, sorted);
    if (cc_valid) pdb_iterator_check_cost_set(pdb, it, total_cc);
    if (n_valid) {
      pdb_iterator_n_set(pdb, it, total_n);
      cl_assert(cl, !total_nc == !total_n);

      if (nc_valid)
        /*  Our average production cost is the
         *  sum of the total production costs,
         *  each multiplied by the number of items
         *  we intend to produce, divided by the
         *  total number of items all together.
         */
        pdb_iterator_next_cost_set(pdb, it, total_n ? total_nc / total_n : 0);
    }
    if (fc_valid) pdb_iterator_find_cost_set(pdb, it, total_n ? total_fc : 0);

    if (cc_valid && n_valid && fc_valid && nc_valid) {
      pdb_iterator_statistics_done_set(pdb, it);
      cl_log(cl, CL_LEVEL_VERBOSE | PDB_FACILITY_ITERATOR,
             "PDB STAT for %s: n=%llu cc=%lld "
             "nc=%lld fc=%lld; %ssorted",
             pdb_iterator_to_string(pdb, it, buf, sizeof buf),
             (unsigned long long)pdb_iterator_n(pdb, it),
             (long long)pdb_iterator_check_cost(pdb, it),
             (long long)pdb_iterator_next_cost(pdb, it),
             (long long)pdb_iterator_find_cost(pdb, it),
             pdb_iterator_sorted(pdb, it) ? "" : "un");
    }
  }

  cl_assert(cl, it->it_type != NULL);
  cl_assert(cl, it->it_type == &or_iterator_type);
  cl_assert(cl, gio == it->it_theory);
  PDB_IS_ITERATOR(cl, it);

  /* Line everybody up in the "active" chain.
   */
  if (!gio->gio_thaw) {
    or_activate_all(it);
    or_chain_invariant(it);

    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_or_create_commit: "
           "%zu subiterators",
           gio->gio_n);
  } else {
    err = or_refile_all(it);
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "or_refile_all: %s", graphd_strerror(err));
      return err;
    }
  }

  cl_leave(cl, CL_LEVEL_VERBOSE, "%s",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf));

  return 0;
}

static int or_merge_subcondition(pdb_iterator *it, pdb_iterator **sub_ptr) {
  graphd_iterator_or *gio = it->it_theory;
  pdb_id id;
  int err;

  if (gio->gio_fixed == NULL) {
    err = graphd_iterator_fixed_create(
        gio->gio_graphd, 0, 0, PDB_ITERATOR_HIGH_ANY,
        pdb_iterator_forward(gio->gio_pdb, it), &gio->gio_fixed);
    if (err != 0) return err;
  }
  for (;;) {
    err = pdb_iterator_next_nonstep(gio->gio_pdb, *sub_ptr, &id);
    if (err != 0) break;

    err = graphd_iterator_fixed_add_id(gio->gio_fixed, id);
    if (err != 0) break;
  }
  pdb_iterator_destroy(gio->gio_pdb, sub_ptr);
  return 0;
}

/**
 * @brief Add a condition to an OR.
 *
 * @param gio_it structure to add to
 * @param sub_it pdb iterator for the subcondition.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_or_add_subcondition(pdb_iterator *it,
                                        pdb_iterator **sub_it) {
  graphd_iterator_or *gio = it->it_theory;
  pdb_handle *pdb = gio->gio_pdb;
  cl_handle *cl = gio->gio_cl;
  cm_handle *cm = gio->gio_cm;
  graphd_or_subcondition *oc;
  char buf[200];

  cl_log(cl, CL_LEVEL_SPEW, "graphd_iterator_or_add_subcondition %p:%s to %p",
         (void *)*sub_it, pdb_iterator_to_string(pdb, *sub_it, buf, sizeof buf),
         (void *)it);

  PDB_IS_ITERATOR(cl, it);
  cl_assert(cl, *sub_it != NULL);
  cl_assert(cl, it->it_type == &or_iterator_type);
  cl_assert(cl, it->it_original == it);

  PDB_IS_ITERATOR(cl, *sub_it);
  cl_assert(cl, (*sub_it)->it_original->it_original == (*sub_it)->it_original);

  if ((*sub_it)->it_type == &or_iterator_type) {
    graphd_iterator_or *sub_gio = (*sub_it)->it_theory;
    size_t i;
    int err;
    size_t need;

    need = (gio->gio_m - 1) + sub_gio->gio_n;

    /* Make room for the unexpected new subiterators.
     */
    if (need > gio->gio_m) {
      oc = cm_realloc(cm, gio->gio_oc, need * sizeof(*gio->gio_oc));
      if (oc == NULL) {
        return ENOMEM;
      }
      gio->gio_oc = oc;
      gio->gio_m = need;
    }

    /* Add all the subconditions individually.
     */
    for (i = 0; i < sub_gio->gio_n; i++) {
      PDB_IS_ITERATOR(cl, sub_gio->gio_oc[i].oc_it);
      err = graphd_iterator_or_add_subcondition(it, &sub_gio->gio_oc[i].oc_it);
    }
    pdb_iterator_destroy(pdb, sub_it);

    cl_log(cl, CL_LEVEL_SPEW,
           "graphd_iterator_or_add_subcondition: pulled in "
           "the whole subcondition for %p.",
           it);
    return 0;
  }

  if (pdb_iterator_null_is_instance(pdb, *sub_it)) {
    /*  X or null = X
     */
    pdb_iterator_destroy(pdb, sub_it);
    return 0;
  }

  if (pdb_iterator_n_valid(pdb, *sub_it) &&
      pdb_iterator_n(pdb, *sub_it) <= GRAPHD_OR_N_MERGE_MAX &&
      pdb_iterator_next_cost_valid(pdb, *sub_it) &&
      pdb_iterator_next_cost(pdb, *sub_it) <
          GRAPHD_OR_PRODUCTION_COST_MERGE_MAX)

    return or_merge_subcondition(it, sub_it);
  else
    return or_add_subcondition(it, sub_it);
}

static int or_thaw_subcondition(pdb_iterator *it, char const **set_s,
                                char const *set_e, char const **state_s,
                                char const *state_e, pdb_iterator_base *pib,
                                cl_loglevel loglevel) {
  graphd_iterator_or *ogio = ogio(it);
  pdb_handle *pdb = ogio->gio_pdb;
  pdb_id oc_id = PDB_ID_NONE;
  bool oc_eof = false;
  pdb_iterator_text sub_pit;
  int err;
  graphd_or_subcondition *oc;
  pdb_iterator *sub_it;

  err = pdb_iterator_util_thaw(pdb, set_s, set_e, "%{(bytes)}",
                               &sub_pit.pit_set_s, &sub_pit.pit_set_e);
  if (err) return err;

  if (*state_s != NULL && *state_s < state_e) {
    err = pdb_iterator_util_thaw(
        pdb, state_s, state_e, "(%{extensions}%{eof/id}.%{position/state})",
        (pdb_iterator_property *)NULL, &oc_eof, &oc_id, &sub_pit);
    if (err != 0) return err;
  } else {
    sub_pit.pit_state_s = sub_pit.pit_state_e = NULL;
    sub_pit.pit_position_s = sub_pit.pit_position_e = NULL;
  }

  /*  Thaw the subiterator.
   */
  err = graphd_iterator_thaw(ogio->gio_graphd, &sub_pit, pib, 0, loglevel,
                             &sub_it, NULL);
  if (err != 0) return err;

  /*  Add it to the list of subconditions.
   */
  err = or_add_subcondition(it, &sub_it);
  pdb_iterator_destroy(ogio->gio_pdb, &sub_it);

  if (err != 0) {
    cl_log_errno(ogio->gio_cl, CL_LEVEL_FAIL, "or_add_subcondition", err,
                 "can't add thawed subcondition");
    return err;
  }

  oc = ogio->gio_oc + ogio->gio_n - 1;
  oc->oc_id = oc_id;
  oc->oc_eof = oc_eof;

  return 0;
}

static int or_thaw_masquerade(graphd_handle *graphd, char const *mas_s,
                              char const *mas_e, pdb_iterator_text const *pit,
                              pdb_iterator_base *pib, bool *has_position,
                              cl_loglevel loglevel, pdb_iterator **it_out) {
  pdb_handle *pdb = graphd->g_pdb;
  graphd_iterator_or *gio;
  int err;
  cl_handle *cl = pdb_log(pdb);
  pdb_iterator_text sub_pit;
  char const *state_s, *state_e;

  *it_out = NULL;
  *has_position = false;

  /*  Mas_s..e is a 'masquerade string' that we can use
   *  to reconstitute the iterator if we need to.
   *
   *  But maybe we also have a *state* that tells us
   *  who we are without that.
   *
   *  Note that the syntax of the state is different between
   *  masquerading and non-masquerading iterators - the
   *  masqueraded state includes the set data for the "OR";
   *  the unmasqueraded state doesn't.
   */
  state_s = pit->pit_state_s;
  state_e = pit->pit_state_e;

  if (state_s != NULL && state_s < state_e) {
    char const *impl_s, *impl_e;
    pdb_iterator_text impl_pit;

    err = pdb_iterator_util_thaw(pdb, &state_s, state_e, "%{(bytes)}", &impl_s,
                                 &impl_e);
    if (err != 0) goto err;

    /* My position is the one in the masquerading iterator.
     */
    impl_pit.pit_position_s = pit->pit_position_s;
    impl_pit.pit_position_e = pit->pit_position_e;

    /* Set and state come from "impl".
     */
    impl_pit.pit_set_s = impl_s;
    impl_pit.pit_set_e = graphd_unparenthesized_curchr(impl_s, impl_e, '/');
    if (impl_pit.pit_set_e == NULL) {
      impl_pit.pit_set_e = impl_e;
      impl_pit.pit_state_e = impl_pit.pit_state_s = NULL;
    } else {
      impl_pit.pit_state_s = impl_pit.pit_set_e + 1;
      impl_pit.pit_state_e = impl_e;
    }

    /* OK, now thaw *that* into *it_out.
     */
    err =
        graphd_iterator_thaw(graphd, &impl_pit, pib, 0, loglevel, it_out, NULL);
    if (err != 0) {
      cl_log(cl, loglevel,
             "or_thaw_masquerade: can't thaw "
             "masquerade state \"%.*s\"",
             (int)(pit->pit_state_e - pit->pit_state_s), pit->pit_state_s);
    } else {
      cl_assert(cl, *it_out != NULL);

      *has_position = true;

      if ((*it_out)->it_type != &or_iterator_type) {
        char buf[200];
        cl_log(cl, CL_LEVEL_DEBUG,
               "or_thaw_masquerade: "
               "evolved into a non-OR %s",
               pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
        return 0;
      }

      /*  Remember the masquerade, even though
       *  we didn't end up using it.
       */
      gio = (*it_out)->it_theory;
      cl_assert(cl, gio != NULL);
      cl_assert(cl, gio->gio_cm != NULL);

      gio->gio_masquerade = cm_malcpy(gio->gio_cm, mas_s, mas_e - mas_s);
      if (gio->gio_masquerade == NULL) {
        err = ENOMEM;
        goto err;
      }

      return 0;
    }
  }

  sub_pit.pit_set_s = mas_s;
  sub_pit.pit_set_e = mas_e;
  sub_pit.pit_position_s = sub_pit.pit_position_e = NULL;
  sub_pit.pit_state_s = sub_pit.pit_state_e = NULL;

  err = graphd_iterator_thaw(graphd, &sub_pit, pib, 0, loglevel, it_out, NULL);
  if (err != 0) return err;

  if ((*it_out)->it_type != &or_iterator_type) {
    cl_log(cl, loglevel,
           "graphd_iterator_or_thaw:"
           "subiterator \"%.*s\" doesn't evaluate to an "
           "or iterator (cursor format change?)",
           (int)(sub_pit.pit_set_e - sub_pit.pit_set_s), sub_pit.pit_set_s);

    if (pit->pit_position_s == NULL ||
        pit->pit_position_s == pit->pit_position_e)

      /* Ah well, doesn't matter.
       */
      return 0;

    err = EINTR;
    goto err;
  }
  return 0;

err:
  pdb_iterator_destroy(pdb, it_out);
  return err ? err : GRAPHD_ERR_LEXICAL;
}

/**
 * @brief Reconstitute a frozen iterator
 *
 * @param graphd	module handle
 * @param s		beginning of stored form
 * @param e		end of stored form
 * @param forward	run low to high?
 * @param it_out	rebuild the iterator here.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_or_thaw_loc(graphd_handle *graphd,
                                pdb_iterator_text const *pit,
                                pdb_iterator_base *pib, cl_loglevel loglevel,
                                pdb_iterator **it_out, char const *file,
                                int line) {
  pdb_handle *pdb = graphd->g_pdb;
  graphd_request *greq;
  bool forward;
  graphd_iterator_or *gio;
  size_t oc_n, i;
  unsigned long long low, high, estimate_n, upper_bound;
  int err;
  bool has_statistics = false, has_position = false;
  cl_handle *cl = pdb_log(pdb);
  pdb_budget check_cost, next_cost, find_cost;
  char const *ord_s, *ord_e;
  char const *set_s, *set_e, *state_s, *state_e;
  char const *s, *e;
  pdb_iterator_account *acc = NULL;
  pdb_id oc_off = PDB_ID_NONE;

  *it_out = NULL;

  if ((upper_bound = pdb_primitive_n(pdb)) == 0)
    return pdb_iterator_null_create(pdb, it_out);
  greq = pdb_iterator_base_lookup(pdb, pib, "graphd.request");
  if (greq == NULL) return EINVAL;

  if (graphd_request_timer_check(greq)) return GRAPHD_ERR_TOO_HARD;

  set_s = pit->pit_set_s;
  set_e = pit->pit_set_e;

  state_s = pit->pit_state_s;
  state_e = pit->pit_state_e;

  if (set_s < set_e && *set_s == '(') {
    char const *mas_s, *mas_e;

    err = pdb_iterator_util_thaw(pdb, &set_s, set_e, "%{(bytes)}", &mas_s,
                                 &mas_e);
    if (err != 0) return err;

    err = or_thaw_masquerade(graphd, mas_s, mas_e, pit, pib, &has_position,
                             loglevel, it_out);
    if (err != 0) goto err;

    /*  The state of a masqueraded or-iterator is the
     *  unmasqueraded or-iterator.
     *
     *  If we could use that, we've already done so;
     *  if not, it's useless now.
     */
    state_s = state_e = NULL;

    if ((*it_out)->it_type != &or_iterator_type) return 0;

    gio = (*it_out)->it_theory;
    gio->gio_thaw = 1;
  } else {
    err = pdb_iterator_util_thaw(pdb, &set_s, set_e,
                                 "%{forward}%{low[-high]}:"
                                 "%zu%{orderingbytes}%{account}%{extensions}:",
                                 &forward, &low, &high, &oc_n, &ord_s, &ord_e,
                                 pib, &acc, (pdb_iterator_property *)NULL);
    if (err != 0) goto err;

    if (oc_n >= 2 * (set_e - set_s) || oc_n < 2) goto err;

    err =
        graphd_iterator_or_create_loc(greq, oc_n, forward, it_out, file, line);
    if (err != 0) return err;

    if (ord_s != NULL) {
      char const *o =
          graphd_iterator_ordering_internalize(graphd, pib, ord_s, ord_e);
      pdb_iterator_ordering_set(pdb, *it_out, o);
    }

    pdb_iterator_account_set(pdb, *it_out, acc);
    if ((*it_out)->it_type != &or_iterator_type) return 0;

    gio = (*it_out)->it_theory;
    gio->gio_thaw = 1;

    for (i = 0; i < oc_n; i++) {
      /*  Make a combined subpit from the set subiterator
       *  and an optional fragment of the state.
       */
      err = or_thaw_subcondition(*it_out, &set_s, set_e, &state_s, state_e, pib,
                                 loglevel);
      if (err != 0) goto err;
    }
    has_position = true;
  }

  s = pit->pit_position_s;
  e = pit->pit_position_e;

  if (s != NULL && s < e) {
    pdb_id last_id = PDB_ID_NONE, resume_id = PDB_ID_NONE;
    bool eof = false;

    err = graphd_iterator_util_thaw_position(pdb, &s, e, loglevel, &eof,
                                             &last_id, &resume_id);
    if (err != 0) goto err;

    err = pdb_iterator_util_thaw(pdb, &s, e, "%{extensions}%{end}",
                                 (pdb_iterator_property *)NULL);
    if (err != 0) goto err;

    if (!has_position) {
      /*  We've lost state and need to catch up.
       */
      gio->gio_eof = eof;
      gio->gio_id = PDB_ID_NONE;
      gio->gio_resume_id = (resume_id == PDB_ID_NONE ? last_id : resume_id);
    } else {
      /*  We've exactly recovered the state; we only
       *  need to catch up if we were in the process
       *  of catching up when we froze.
       */
      gio->gio_eof = eof;
      gio->gio_id = last_id;
      gio->gio_resume_id = resume_id;
    }
  }

  if (state_s != NULL && state_s < state_e) {
    err = pdb_iterator_util_thaw(pdb, &state_s, state_e, ":%{id}", &oc_off);
    if (err != 0) goto err;

    if (state_s < state_e) {
      has_statistics = true;
      err = pdb_iterator_util_thaw(
          pdb, &state_s, state_e, ":%{budget}:%{next[+find]}:%llu", &check_cost,
          &next_cost, &find_cost, &estimate_n);
      if (err != 0) goto err;
    }
    err = pdb_iterator_util_thaw(pdb, &state_s, state_e, "%{extensions}%{end}",
                                 (pdb_iterator_property *)NULL);
    if (err != 0) goto err;
  }

  if (has_statistics) {
    bool sorted = true;
    graphd_or_subcondition const *oc;
    size_t i;

    pdb_iterator_check_cost_set(pdb, *it_out, check_cost);
    pdb_iterator_next_cost_set(pdb, *it_out, next_cost);
    pdb_iterator_find_cost_set(pdb, *it_out, find_cost);
    pdb_iterator_n_set(pdb, *it_out, estimate_n);

    /*  We're sorted if all our subiterators are.
     */
    for (i = gio->gio_n, oc = gio->gio_oc; i--; oc++)
      sorted &= !!pdb_iterator_sorted(pdb, oc->oc_it);

    pdb_iterator_sorted_set(pdb, *it_out, sorted);

    /*  If we are sorted and have an ordering,
     *  then our ordering is in sort-order.
     */
    if (pdb_iterator_ordering(pdb, *it_out) != NULL)
      pdb_iterator_ordered_set(pdb, *it_out, sorted);

    pdb_iterator_statistics_done_set(pdb, *it_out);
  }
  if ((err = graphd_iterator_or_create_commit(*it_out)) != 0) {
    cl_log_errno(cl, loglevel, "graphd_iterator_or_create_commit", err,
                 "from cursor %.*s", (int)(pit->pit_set_e - pit->pit_set_s),
                 pit->pit_set_s);
    pdb_iterator_destroy(pdb, it_out);
    return err;
  }

  /*  Restore the position in a "check".
   */
  if ((*it_out)->it_type == &or_iterator_type && oc_off != PDB_ID_NONE) {
    gio = (*it_out)->it_theory;
    if (oc_off <= gio->gio_n) gio->gio_this_oc = gio->gio_oc + oc_off;
  }

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_iterator_or_thaw: %p", (void *)*it_out);
  return 0;

err:
  cl_log(cl, loglevel, "graphd_iterator_or_thaw: can't parse \"%.*s\"",
         (int)(pit->pit_set_e - pit->pit_set_s), pit->pit_set_s);
  pdb_iterator_destroy(pdb, it_out);

  return err ? err : GRAPHD_ERR_LEXICAL;
}

/**
 * @brief Are all my subiterators "VIP" iterators with the same type?
 *
 *	If they are, we can get rid of a "type" constraint
 * 	sharing an "and" iterator with this "or".
 *
 * @param pdb		module handle
 * @param it		beginning of stored form
 * @param type_id_out	the type of the VIP iterators
 *
 * @return true if yes (and *type_id_out is assigned the type),
 *	false otherwise.
 */
bool graphd_iterator_or_is_vip_type(pdb_handle *pdb, pdb_iterator *it,
                                    pdb_id *type_id_out) {
  pdb_id type_id = PDB_ID_NONE;
  graphd_iterator_or *gio;
  graphd_or_subcondition *oc;
  size_t n;

  if (it->it_type != &or_iterator_type) return false;

  gio = it->it_theory;
  if (gio->gio_n < 1) return false;

  for (n = gio->gio_n, oc = gio->gio_oc; n--; oc++) {
    if (!graphd_iterator_vip_is_instance(pdb, oc->oc_it)) return false;

    if (type_id == PDB_ID_NONE)
      type_id = graphd_iterator_vip_type_id(pdb, oc->oc_it);
    else if (type_id != graphd_iterator_vip_type_id(pdb, oc->oc_it))
      return false;
  }
  *type_id_out = type_id;
  return true;
}

/**
 * @brief Set a string that this iterator disguises itself as.
 *
 *  The disguise is used when freezing this iterator.
 *  In the disguise, the first pair of :: has low-high
 *  inserted into it on freezing.
 *
 * @param it	some iterator, hopefully an "or"
 * @param mas	masquerade string
 *
 * @return true if yes (and *type_id_out is assigned the type),
 *	false otherwise.
 */
int graphd_iterator_or_set_masquerade(pdb_iterator *it, char const *mas) {
  graphd_iterator_or *gio;
  char *mas_dup;

  it = it->it_original;
  if (it->it_type != &or_iterator_type) return GRAPHD_ERR_NO;

  gio = it->it_theory;
  if ((mas_dup = cm_strmalcpy(gio->gio_cm, mas)) == NULL)
    return errno ? errno : ENOMEM;
  if (gio->gio_masquerade != NULL) cm_free(gio->gio_cm, gio->gio_masquerade);
  gio->gio_masquerade = mas_dup;

  return 0;
}

/**
 * @brief Get a specific subiterator.
 *
 * @param pdb		module handle
 * @param it		iterator we're asking about -
 *			not necessarily an OR iterator, but could be.
 * @param i		index of subiterator we want.
 * @param sub_out	out: the subiterator iterator at that position
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_or_get_subconstraint(pdb_handle *pdb, pdb_iterator *it,
                                         size_t i, pdb_iterator **sub_out) {
  graphd_iterator_or *gio;

  if (it->it_type != &or_iterator_type) return GRAPHD_ERR_NO;

  gio = it->it_theory;
  if (i >= gio->gio_n) return GRAPHD_ERR_NO;

  *sub_out = gio->gio_oc[i].oc_it;
  return 0;
}

/**
 * @brief Is this an or iterator?  How many subiterators does it have?
 *
 * @param pdb		module handle
 * @param it		iterator we're asking about -
 *			not necessarily an AND iterator, but could be.
 * @param n_out		out: number of subiterators
 *
 * @return 0 on success, a nonzero error code on error.
 */
bool graphd_iterator_or_is_instance(pdb_handle *pdb, pdb_iterator *it,
                                    size_t *n_out) {
  graphd_iterator_or *gio;

  if (it->it_type != &or_iterator_type) return false;

  gio = it->it_theory;
  if (n_out != NULL) *n_out = gio->gio_n;

  return true;
}
