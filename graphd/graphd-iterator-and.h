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
#ifndef GRAPHD_ITERATOR_AND_H
#define GRAPHD_ITERATOR_AND_H

/*  Set the ps_call_state to this to start with a find
 *  in graphd-iterator-and-run.c
 */
#define GRAPHD_ITERATOR_AND_RUN_FIND_START 98
#define GRAPHD_ITERATOR_AND_RUN_NEXT_CATCH_UP_START 99

/*
 *  When finding out which subcondition makes the best iterator,
 *  don't stop until one hits the end or finds this many.
 */
#define GRAPHD_AND_CONTEST_GOAL 5

/*  Some of the subconstraints don't complete their statistics
 *  prior to our call.  When determining our own cost estimates,
 *  we need to make assumptions about their cost ("expensive")
 *  and what it'll buy us ("little").
 */
#define UNKNOWN_CHECK_COST 5000  /* high */
#define UNKNOWN_CHECK_CHANCE 0.9 /* almost no reduction */

/*  How much production cost are we going to spend on turning
 *  into a fixed array?
 */
#define GRAPHD_AND_PRODUCTION_COST_FIXED_MAX 5000

/*  How large a fixed array are we willing to produce?
 */
#define GRAPHD_AND_N_FIXED_MAX 200

/*  Maximum cost we're willing to spend to produce and check the
 *  contents of the easiest available producer during create-commit.
 */
#define GRAPHD_AND_PREEVALUATE_COST_MAX (1024 * 10)

/*  Magic number to identify GIA state.  (Go Ramones!)
 */
#define GRAPHD_AND_MAGIC 0x01020304
#define GRAPHD_IS_AND(cl, gia) \
  cl_assert(cl, (gia)->gia_magic == GRAPHD_AND_MAGIC)

/*  During the contest, how much do we prefer sorted producers
 *  over unsorted ones?
 */

/*  One state in the "and" iterator, enough to execute
 *  check, next, or find.
 */
typedef struct and_process_state {
#define GRAPHD_AND_PROCESS_STATE_MAGIC (0xfee7)
#define GRAPHD_AND_IS_PROCESS_STATE(cl, ps) \
  cl_assert(cl, (ps)->ps_magic == GRAPHD_AND_PROCESS_STATE_MAGIC)
  unsigned short ps_magic;

  /*  The value returned by the most recent call to
   *  next() or on-or-after(), or PDB_ID_NONE at the start.
   */
  pdb_id ps_id;

  /*  The value most recently produced by the producer.
   */
  pdb_id ps_producer_id;

  /*  If the producer doesn't like hopping around with
   *  on-or-after (its traversal cost is high), but we
   *  learned an on-or-after id for cheap from one of the
   *  sorted checkers, it is stored here, and pdb_iterator_next()
   *  gets called on the producer until the producer returns
   *  or goes past the on-or-after ID.
   *
   *  If unset, the value is PDB_ID_NONE.
   */
  pdb_id ps_next_find_resume_id;

  /*  A call state for "graphd_iterator_and_run".
   */
  unsigned short ps_run_call_state;

  /*  The number of IDs the contest's producer produced.
   *  If and_next is called as part of the statistics contest,
   *  this count, compared to the number of elements
   *  that actually make it through all the tests,
   *  will help the contest figure out how many elements
   *  to expect, overall.
   */
  unsigned long long ps_run_produced_n;

  /*  Cumulative cost across multiple phases of an and_next().
   */
  pdb_budget ps_run_cost;

  /*  The ID that "find" was first called with.
   */
  pdb_id ps_find_id;

  /*  When checking against iterators, the index of the
   *  iterator we're currently checking.  Mapped through
   *  sc->sc_order[] to get the real index.
   */
  size_t ps_check_i;

  /*  A copy of gia->gia_check_order from the last time we started
   *  checking something.
   */
  size_t *ps_check_order;

  /*  Its version number.  If different from the one in gia,
   *  we should resort.
   */
  unsigned int ps_check_order_version;

  /*  From the most recent check, remember a range between low
   *  and high that we know has nothing in it.
   */
  pdb_id ps_check_exclude_low;
  pdb_id ps_check_exclude_high;

  /*  Have we hit the end?
   */
  bool ps_eof;

  /* gia_n iterators.
   */
  pdb_iterator **ps_it;

  /*  The number of entries in ps_it and ps_check_order.
   */
  size_t ps_n;

} and_process_state;

/*  One subcondition in an "and" iterator.
 *  Only in the original.
 */
typedef struct graphd_subcondition {
  /*  Subiterator.
   *
   *  During the contest: Dormant; used only for printing, cloning.
   *  After the contest:  Producer in the producer's sc.
   */
  pdb_iterator *sc_it;

  /*  Contest participant process state.
   */
  and_process_state sc_contest_ps;

  /*  During the contest, how much budget has this iterator used?
   */
  pdb_budget sc_contest_cost;

  /*  During the contest, results from this producer.
   */
  pdb_id sc_contest_id[GRAPHD_AND_CONTEST_GOAL];
  size_t sc_contest_id_n;

  /*  Coroutine state for the and_iterator_statistics_work on
   *  this producer.
   */
  int sc_contest_state;

  /*  If set to false, this subcondition doesn't compete
   *  for producership in the statistics contest.
   */
  unsigned int sc_compete : 1;

} graphd_subcondition;

typedef struct graphd_iterator_and {
  unsigned long gia_magic;

  graphd_handle *gia_graphd;
  graphd_request *gia_greq;
  pdb_handle *gia_pdb;
  cm_handle *gia_cm;
  cl_handle *gia_cl;

  /* In the original only, the subiterators.
   */
  graphd_subcondition *gia_sc;
  size_t gia_m;
  size_t gia_n;

  graphd_direction gia_direction;

  /*  (Original only.) Statistics about actual cost, actual number
   *  of elements produced and checked.
   */
  unsigned long long gia_n_produced;
  unsigned long long gia_n_checked;
  unsigned long long gia_total_cost_produce;
  unsigned long long gia_total_cost_check;
  unsigned long long gia_total_cost_statistics;

  /*  (Original only.) Which order to try statistics contestants in.
   *  If you have a fast producer, you want it closer to the beginning
   *  of the ordering.
   */
  size_t *gia_contest_order;

  /*  (Original only.)  The iterator's best guess as to the optimal
   *  order in which to consult subiterators.
   */
  size_t *gia_check_order;

  /*  (Original only.) If set, new information has become known
   *	about the subconstraints since the last time
   *	gia->gia_check_order was compiled.
   */
  unsigned int gia_resort : 1;

  /*  (Original only.) The version number of this check order.
   *  Updated after each sort.
   */
  unsigned int gia_check_order_version;

  /*  Used by code in graphd-iterator-and-check.c.
   */
  graphd_and_slow_check_state *gia_scs;

  /*  (Original only.)  After the contest: index of the chosen
   *  producer within gia_sc.
   */
  size_t gia_producer;

  /*  (After a thaw in the original only.)  If not -1,
   *  let this producer win the contest.
   */
  int gia_producer_hint;

  /*  (Original only.)  During the contest: We slowly ramp up
   *  the budget allocation per subproducer.
   *  This variable keeps track of where we are
   *  in that - 10, 100, 1000, or 10000.
   */
  unsigned int gia_contest_max_turn;

  /*  (Original only.)  If non-zero, the maximum per-contestant budget.
   *  Set as soon as the first contestant completes.
   */
  pdb_budget gia_contest_max_budget;

  /*  (Original only.)  Don't run AND statistics if we don't have a
   *  budget of at least 1$ per participant.  Until then,
   *  silently consume the input.
   */
  pdb_budget gia_contest_to_save;

  /*  Set if we're thawing; makes commit be more gentle.
   */
  unsigned int gia_thaw : 1;

  /*  If not PDB_ID_NONE: after unthawing, walk past this before
   *  returning/caching anything (for next).
   */
  pdb_id gia_resume_id;

  /*  The most recently returned value, or PDB_ID_NONE.
   */
  pdb_id gia_id;

  /* Subiterator state used in next, on-or-after, check.
   */
  and_process_state gia_ps;

  /*  has graphd_iterator_create_commit() been called?
   */
  unsigned int gia_committed : 1;

  /*  After the statistics are done, have we attempted
   *  a final step of seeing who everybody is and
   *  replacing ourselves with a faster version?
   */
  unsigned int gia_evolved : 1;

  /*  In an original only: a cache of pre-evaluated items, and
   *  how much they cost to generate.
   */
  graphd_iterator_cache *gia_cache;

  /*  In all clones: what would the current position in the cache be?
   */
  size_t gia_cache_offset;

  /*  Is that current position actually valid?
   *  This may be lost if we on-or-after outside the cached range.
   */
  unsigned int gia_cache_offset_valid : 1;

  /*  The process state for the and_next that produces another
   *  element of the cache.  Moved from the contest winner.
   */
  and_process_state gia_cache_ps;

  /*  Incremented after each freeze.
   */
  unsigned long gia_original_version;

  /*  The first offset that another ID pulled out of gia_cache_ps
   *  might fit in in the cache.
   */
  size_t gia_cache_ps_offset;
  unsigned int gia_context_pagesize_valid : 1;
  unsigned long long gia_context_pagesize;

  unsigned int gia_context_setsize_cached : 1;
  unsigned int gia_context_setsize_valid : 1;
  unsigned long long gia_context_setsize;

} graphd_iterator_and;

#define ogia_nocheck(it) ((graphd_iterator_and *)((it)->it_original->it_theory))

#define ogia(it)                                                               \
  ((ogia_nocheck(it)->gia_magic == GRAPHD_AND_MAGIC)                           \
       ? ogia_nocheck(it)                                                      \
       : (cl_notreached(                                                       \
              ((graphd_iterator_and *)((it)->it_theory))->gia_cl,              \
              "AND iterator %p has an original (%p) that is not an AND", (it), \
              (it)->it_original),                                              \
          (graphd_iterator_and *)NULL))

/* graphd-iterator-and.c */

int graphd_iterator_and_access(pdb_handle *pdb, pdb_iterator *it,
                               pdb_budget *budget_inout, float research);

/* graphd-iterator-and-check.c */

int graphd_iterator_and_check(pdb_handle *const _pdb, pdb_iterator *const _it,
                              pdb_id const _id,
                              pdb_budget *const _budget_inout);

void graphd_iterator_and_slow_check_finish(pdb_handle *const _pdb,
                                           pdb_iterator *const _it);

int graphd_iterator_and_check_sort(pdb_iterator *);
int graphd_iterator_and_check_sort_refresh(pdb_iterator *it,
                                           and_process_state *ps);
void graphd_iterator_and_check_delete_subcondition(pdb_iterator *it, size_t i);

int graphd_iterator_and_check_thaw_slow(graphd_iterator_and *_gia,
                                        char const **_s_ptr, char const *_e,
                                        pdb_iterator_base *_pib,
                                        cl_loglevel _loglevel);

int graphd_iterator_and_check_freeze_slow(graphd_iterator_and *_gia,
                                          cm_buffer *_buf);

/* graphd-iterator-and-freeze.c */

int graphd_iterator_and_freeze(pdb_handle *_pdb, pdb_iterator *_it,
                               unsigned int _flags, cm_buffer *_buf);

/* graphd-iterator-and-run.c */

int graphd_iterator_and_run(pdb_iterator *it, size_t producer,
                            and_process_state *ps, pdb_budget *budget_inout);

int graphd_iterator_and_find_loc(pdb_handle *pdb, pdb_iterator *it,
                                 pdb_id id_in, pdb_id *id_out,
                                 pdb_budget *budget_inout, char const *file,
                                 int line);

/* graphd-iterator-and-optimize.c */

int graphd_iterator_and_evolve(pdb_handle *pdb, pdb_iterator *it);

int graphd_iterator_and_optimize(graphd_handle *g, pdb_iterator *it);

/* graphd-iterator-and-process-state.c */

void graphd_iterator_and_process_state_clear(and_process_state *ps);

void graphd_iterator_and_process_state_finish(graphd_iterator_and *gia,
                                              and_process_state *ps);

void graphd_iterator_and_process_state_delete_subcondition(
    pdb_iterator *it, and_process_state *ps, size_t i);

int graphd_iterator_and_process_state_clone(pdb_handle *pdb, pdb_iterator *it,
                                            and_process_state const *src,
                                            and_process_state *dst);

int graphd_iterator_and_process_state_initialize(pdb_handle *pdb,
                                                 pdb_iterator *it,
                                                 and_process_state *ps);

/* graphd-iterator-and-statistics.c */

int graphd_iterator_and_statistics(pdb_handle *const _pdb,
                                   pdb_iterator *const _it,
                                   pdb_budget *const _budget_inout);

pdb_budget graphd_iterator_and_calculate_check_cost(
    pdb_iterator const *const it, graphd_iterator_and const *const gia);

#endif /* GRAPHD_ITERATOR_AND_H */
