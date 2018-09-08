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

static double step_size(pdb_handle const *pdb, pdb_iterator const *it,
                        unsigned long long upper_bound) {
  if (pdb_iterator_n_valid(pdb, it)) {
    unsigned long long bre;

    bre = (it->it_high == PDB_ITERATOR_HIGH_ANY ? upper_bound : it->it_high) -
          it->it_low;

    if (pdb_iterator_n(pdb, it) <= 1) return (double)bre;

    return (double)bre / pdb_iterator_n(pdb, it);
  }

  return 1.0;
}

/**
 * @brief Given a producer and some checkers, get the next value.
 *
 *  This is used both for the and_iterator_next()
 *  implementation and for the internal contest phase in
 *  the statistics.
 *
 * @param it		The and-iterator
 * @param producer	Index of the producer.
 * @param ps		process-state local to the particular
 *			statistics/and_iterator_next/../...
 * @param budget_inout	budget
 *
 * @return 0 on success, a nonzero error code on failure.
 * @return PDB_ERR_MORE if we ran out of time.
 */
int graphd_iterator_and_run(pdb_iterator *const it, size_t const producer,
                            and_process_state *const ps,
                            pdb_budget *const budget_inout) {
  graphd_iterator_and *gia = it->it_theory;
  cl_handle *cl = gia->gia_cl;
  pdb_handle *pdb = gia->gia_pdb;
  bool changed;
  int err = 0;
  pdb_iterator *c_it = NULL;
  pdb_budget budget_in = *budget_inout;
  char buf[200];
  bool checker_likes_find;
  size_t check_i;

  cl_log(cl, CL_LEVEL_DEBUG,
         "graphd_iterator_and_run(it=%p, ps=%p, ps_id=%llx, resume_id=%llx, "
         "call_state=%d, check_i=%zu, producer=#%d, "
         "budget $%lld, gia_n %zu, ogia_n %zu, ps_n %zu)",
         (void *)it, (void *)ps, (unsigned long long)ps->ps_id,
         (unsigned long long)ps->ps_next_find_resume_id,
         (int)ps->ps_run_call_state, ps->ps_check_i, (int)producer,
         *budget_inout, gia->gia_n, ogia(it)->gia_n, ps->ps_n);

  cl_enter(cl, CL_LEVEL_VERBOSE,
           "(it=%p, call_state=%d, check_i=%zu, producer=#%d, "
           "budget $%lld, gia_n %zu, ogia_n %zu, ps_n %zu, sabotage %p, %lu)",
           (void *)it, (int)ps->ps_run_call_state, ps->ps_check_i,
           (int)producer, *budget_inout, gia->gia_n, ogia(it)->gia_n, ps->ps_n,
           gia->gia_graphd->g_sabotage,
           gia->gia_graphd->g_sabotage
               ? gia->gia_graphd->g_sabotage->gs_countdown
               : 0);

  switch (ps->ps_run_call_state) {
    default:
      cl_notreached(cl,
                    "graphd_iterator_and_run: "
                    "unexpected call state %d",
                    ps->ps_run_call_state);

    case GRAPHD_ITERATOR_AND_RUN_NEXT_CATCH_UP_START:

      /*  Preprocessing: resynchronize the producer --
       *  that is, move it onto ps_next_find_resume_id;
       *  *then* do a next.
       *
       *  It's possible that ps_next_find_resume_id
       *  resulted from a find in one of the checkers -
       *  count a producer find that jumps past it
       *  as if it were a find and a next.
       */
      if (ps->ps_it == NULL) {
        err = graphd_iterator_and_process_state_initialize(pdb, it, ps);
        if (err != 0) return err;
      }
      cl_assert(cl, ps->ps_it != NULL);

      ps->ps_run_cost = 0;
      pdb_iterator_call_reset(pdb, ps->ps_it[producer]);

      if (ps->ps_next_find_resume_id == PDB_ID_NONE) {
        err = pdb_iterator_reset(pdb, ps->ps_it[producer]);
        if (err != 0) return err;
      } else if (pdb_iterator_sorted(pdb, ps->ps_it[producer]) &&
                 pdb_iterator_statistics_done(pdb, ps->ps_it[producer]) &&
                 pdb_iterator_find_cost(pdb, ps->ps_it[producer]) <
                     (pdb_iterator_n(pdb, ps->ps_it[producer]) *
                      pdb_iterator_next_cost(pdb, ps->ps_it[producer]))) {
        /*  Resumption via a direct "find" on
         *  the producer.
         */
        pdb_id id_found;
        pdb_iterator *p_it;

        p_it = ps->ps_it[producer];
        pdb_iterator_call_reset(pdb, p_it);

        case 9:
          p_it = ps->ps_it[producer];
          err = pdb_iterator_find(pdb, p_it, ps->ps_next_find_resume_id,
                                  &id_found, budget_inout);
          if (err == PDB_ERR_MORE) {
            ps->ps_run_call_state = 9;
            goto suspend;
          }
          if (err != 0) {
            /*  This is possible if the ID
             *  didn't originate with this
             *  producer, and this producer
             *  just bumped into its end.
             */
            char buf[200];
            cl_log_errno(cl, err == PDB_ERR_NO ? CL_LEVEL_FAIL : CL_LEVEL_ERROR,
                         "pdb_iterator_find", err, "id=%llx, iterator=%s",
                         (unsigned long long)ps->ps_next_find_resume_id,
                         pdb_iterator_to_string(pdb, p_it, buf, sizeof buf));
            goto done;
          }
          ps->ps_id = id_found;

          /*  If we overshot, count this as the producer
           *  producing a new candidate.
           */
          if (id_found != ps->ps_next_find_resume_id) {
            cl_assert(cl, pdb_iterator_forward(pdb, p_it)
                              ? id_found > ps->ps_next_find_resume_id
                              : id_found < ps->ps_next_find_resume_id);

            ps->ps_next_find_resume_id = PDB_ID_NONE;
            goto have_producer_next_result;
          }
      } else {
        /*  Resumption via "next" calls on just the producer,
         *  until we see our desired ID.
         */
        pdb_id id_found;
        do {
          pdb_iterator *p_it;
          if (*budget_inout < 0) {
            err = PDB_ERR_MORE;
            ps->ps_run_call_state = 10;
            goto suspend;
          }

          p_it = ps->ps_it[producer];
          pdb_iterator_call_reset(pdb, p_it);

          case 10:
            p_it = ps->ps_it[producer];
            err = pdb_iterator_next(pdb, p_it, &id_found, budget_inout);
            if (err != 0) {
              if (err == PDB_ERR_MORE) {
                ps->ps_run_call_state = 10;
                goto suspend;
              }
              ps->ps_next_find_resume_id = PDB_ID_NONE;
              ps->ps_run_call_state = 0;

              goto done;
            }

            if (pdb_iterator_sorted(pdb, p_it) &&
                pdb_iterator_sorted_valid(pdb, p_it) &&
                (pdb_iterator_forward(pdb, p_it)
                     ? id_found > ps->ps_next_find_resume_id
                     : id_found < ps->ps_next_find_resume_id)) {
              /*  We overshot, as in the find case.
               */
              ps->ps_id = id_found;
              ps->ps_next_find_resume_id = PDB_ID_NONE;
              ps->ps_run_call_state = 0;

              goto have_producer_next_result;
            }

            /*  If we're not sorted, we can't tell
             *  whether we overshoot - but in that
             *  case, the checkers aren't allowed to
             *  find past their check-id, and thus
             *  any ps_id must actually have originated
             *  with the producer.
             */

        } while (id_found != ps->ps_next_find_resume_id);
      }

      ps->ps_run_call_state = 0;
      ps->ps_next_find_resume_id = PDB_ID_NONE;

    /* Fall through */

    /*  This entry point, 0, is for initial entry only.
     *  For resumption as if via 0, use 7 instead.
     */
    case 0:
      if (ps->ps_eof) {
        cl_leave(cl, CL_LEVEL_VERBOSE, "eof (stored)");
        return GRAPHD_ERR_NO;
      }
      if (ps->ps_it == NULL) {
        err = graphd_iterator_and_process_state_initialize(pdb, it, ps);
        if (err != 0) return err;

        cl_log(cl, CL_LEVEL_DEBUG,
               "graphd_iterator_and_run, after and_process_state_initialize: "
               "it=%p, ps=%p, ps_id=%llx, resume_id=%llx, call_state=%d, "
               "check_i=%zu, producer=#%d, "
               "budget $%lld, gia_n %zu, ogia_n %zu, ps_n %zu",
               (void *)it, (void *)ps, (unsigned long long)ps->ps_id,
               (unsigned long long)ps->ps_next_find_resume_id,
               (int)ps->ps_run_call_state, ps->ps_check_i, (int)producer,
               *budget_inout, gia->gia_n, ogia(it)->gia_n, ps->ps_n);
      }
      cl_assert(cl, ps->ps_it != NULL);
      ps->ps_run_cost = 0;
      ps->ps_next_find_resume_id = PDB_ID_NONE;

      do /* ... while (err == GRAPHD_ERR_NO) */
      {
        case 7:
          cl_assert(cl, pdb_iterator_has_position(pdb, ps->ps_it[producer]));
          pdb_iterator_call_reset(pdb, ps->ps_it[producer]);
        case 1:
          ps->ps_run_call_state = 0;
          cl_assert(cl, ps->ps_it != NULL);
          PDB_IS_ITERATOR(cl, ps->ps_it[producer]);
          cl_assert(cl, pdb_iterator_has_position(pdb, ps->ps_it[producer]));

          /*  Get the next candidate from this producer.
           *  One of two ways:
           *
           *  - If a previous checker used "find" to go forward
           *    to an ID, maybe we can use "find" to position
           *    on or after it.
           *
           *  - Otherwise, either there was no previous
           *    candidate, or the last failure was a simple
           *    check, and we just use "next".
           */

          /*  Cost to the producer with find: 1 find cost.
           *  Cost with nexts: distance between the new
           *	point and the current point,
           *	divided by the average next step width,
           *	times the next cost.
           */
          cl_log(cl, CL_LEVEL_VERBOSE,
                 "graphd_iterator_and_run: find or next? "
                 "producer sorted? %s; find-id %llx; resume-id %llx; stats "
                 "done %s; fc=%lld; primitive n: %llu; total %llu; nc=%lld",
                 pdb_iterator_sorted(pdb, ps->ps_it[producer]) ? "yes" : "no",
                 (unsigned long long)ps->ps_next_find_resume_id,
                 (unsigned long long)ps->ps_producer_id,
                 pdb_iterator_statistics_done(pdb, ps->ps_it[producer]) ? "yes"
                                                                        : "no",
                 (long long)pdb_iterator_find_cost(pdb, ps->ps_it[producer]),
                 (unsigned long long)pdb_primitive_n(pdb),
                 (unsigned long long)pdb_iterator_n(pdb, ps->ps_it[producer]),
                 (long long)pdb_iterator_next_cost(pdb, ps->ps_it[producer]));

          if (pdb_iterator_sorted(pdb, ps->ps_it[producer]) &&
              ps->ps_next_find_resume_id != PDB_ID_NONE &&
              ps->ps_producer_id != PDB_ID_NONE &&
              pdb_iterator_statistics_done(pdb, ps->ps_it[producer]) &&
              pdb_iterator_find_cost(pdb, ps->ps_it[producer]) <
                  ((double)(ps->ps_next_find_resume_id > ps->ps_producer_id
                                ? ps->ps_next_find_resume_id -
                                      ps->ps_producer_id
                                : ps->ps_producer_id -
                                      ps->ps_next_find_resume_id) /
                   ((double)pdb_iterator_spread(pdb, ps->ps_it[producer]) /
                    pdb_iterator_n(pdb, ps->ps_it[producer]))) *
                      pdb_iterator_next_cost(pdb, ps->ps_it[producer])) {
            pdb_id id_found;
            pdb_iterator *p_it;

            /* This producer likes find for this jump. */

            ps->ps_id = ps->ps_next_find_resume_id;

            case GRAPHD_ITERATOR_AND_RUN_FIND_START:
              p_it = ps->ps_it[producer];
              pdb_iterator_call_reset(pdb, p_it);
            case 2:
              cl_assert(cl, ps->ps_it != NULL);
              p_it = ps->ps_it[producer];
              PDB_IS_ITERATOR(cl, p_it);

              err = pdb_iterator_find(pdb, p_it, ps->ps_id, &id_found,
                                      budget_inout);
              if (err == PDB_ERR_MORE) {
                ps->ps_run_call_state = 2;
                goto suspend;
              }
              if (err != 0) goto done;

              if (ps->ps_next_find_resume_id != PDB_ID_NONE &&
                  id_found != ps->ps_next_find_resume_id) {
                if (pdb_iterator_forward(pdb, p_it)
                        ? (id_found > ps->ps_next_find_resume_id)
                        : (id_found < ps->ps_next_find_resume_id)) {
                  ps->ps_next_find_resume_id = PDB_ID_NONE;
                  ps->ps_id = id_found;

                  goto have_producer_next_result;
                }
                cl_notreached(cl,
                              "graphd_iterator_and_run: "
                              "producer %s jumped past "
                              "ps_next_find_resume_id %llx, "
                              "landing on %llx instead.",
                              pdb_iterator_to_string(pdb, ps->ps_it[producer],
                                                     buf, sizeof buf),
                              (unsigned long long)ps->ps_next_find_resume_id,
                              (unsigned long long)id_found);
              }
              ps->ps_id = id_found;
          } else {
            pdb_iterator *p_it;
            ;

            p_it = ps->ps_it[producer];
            pdb_iterator_call_reset(pdb, p_it);

            /*  Just use "next".
             */
            case 3:
              p_it = ps->ps_it[producer];
              err = pdb_iterator_next(pdb, p_it, &ps->ps_id, budget_inout);
              if (err != 0) {
                if (err != PDB_ERR_MORE) goto done;

                ps->ps_run_call_state = 3;
                goto suspend;
              }

              if (ps->ps_next_find_resume_id != PDB_ID_NONE &&
                  ps->ps_next_find_resume_id != ps->ps_id) {
                /*  Did we overshoot?
                 */
                if (pdb_iterator_sorted(pbd, ps->ps_it[producer]) &&
                    (pdb_iterator_forward(pdb, ps->ps_it[producer])
                         ? ps->ps_id > ps->ps_next_find_resume_id
                         : ps->ps_id < ps->ps_next_find_resume_id)) {
                  ps->ps_next_find_resume_id = PDB_ID_NONE;
                  goto have_producer_next_result;
                }
                cl_log(cl, CL_LEVEL_VERBOSE,
                       "graphd_iterator_and_run: "
                       "ignore %llx; still "
                       "waiting to go past %llx",
                       (unsigned long long)ps->ps_id,
                       (unsigned long long)ps->ps_next_find_resume_id);

                if (GRAPHD_SABOTAGE(gia->gia_graphd, *budget_inout <= 0)) {
                  pdb_iterator_call_reset(pdb, ps->ps_it[producer]);
                  ps->ps_run_call_state = 3;
                  goto suspend;
                }
                err = GRAPHD_ERR_NO;
                continue;
              }
          }

        have_producer_next_result:

          ps->ps_next_find_resume_id = PDB_ID_NONE;
          ps->ps_producer_id = ps->ps_id;

          /*  Count this as the producer producing something.
           *
           *  If we're running as part of the initial contest,
           *  this count, compared to the number of elements
           *  that actually make it through all the tests,
           *  will help the contest figure out how many elements
           *  to expect, overall.
           */
          ps->ps_run_produced_n++;

          cl_log(cl, CL_LEVEL_VERBOSE,
                 "graphd_iterator_and_run: "
                 "producer #%d made %llx (attempt #%llu)",
                 (int)producer, (unsigned long long)ps->ps_id,
                 (unsigned long long)ps->ps_run_produced_n);

          if (ps->ps_id < it->it_low || ps->ps_id >= it->it_high) {
            cl_log(cl, CL_LEVEL_VERBOSE,
                   "graphd_iterator_and_run: "
                   "value %lld outside "
                   "of low/high boundaries %lld..%lld",
                   (long long)ps->ps_id, (long long)it->it_low,
                   (long long)it->it_high - 1);

            if (pdb_iterator_sorted(pdb, ps->ps_it[producer]) &&
                (pdb_iterator_forward(pdb, it) ? ps->ps_id >= it->it_high
                                               : ps->ps_id < it->it_low)) {
              err = GRAPHD_ERR_NO;
              goto done;
            }

            /*  If our producer is sorted, and we're not
             *  currently chasing after something - maybe
             *  we can use "find" to position on-or-after
             *  the initial boundary?
             */
            if (pdb_iterator_sorted(pdb, ps->ps_it[producer]) &&
                ps->ps_next_find_resume_id == PDB_ID_NONE) {
              ps->ps_next_find_resume_id =
                  pdb_iterator_forward(pdb, it) ? it->it_low : it->it_high - 1;
            }
            if (GRAPHD_SABOTAGE(gia->gia_graphd, *budget_inout <= 0)) {
              ps->ps_run_call_state = 7;
              goto suspend;
            }
            err = GRAPHD_ERR_NO;
            continue;
          }

          /*  Check the candidate ps->ps_id against all
           *  the iterators that didn't produce it.
           */
          err = graphd_iterator_and_check_sort_refresh(it, ps);
          if (err != 0) goto done;

          /*  Whenever we resume into the middle of this
           *  loop, we actually do need to retest the end
           *  condition, ps->ps_check_i >= ps->ps_n, because
           *  ps_n can have been decremented while removing
           *  a subiterator during optimization.
           *  (We're not really sure *which* ps is involved
           *  here.)
           */
          for (ps->ps_check_i = 0; ps->ps_check_i < ps->ps_n;
               ps->ps_check_i++) {
            pdb_id id_found;

            cl_assert(cl, ps->ps_check_order != NULL);

            check_i = ps->ps_check_order[ps->ps_check_i];

            if (GRAPHD_SABOTAGE(gia->gia_graphd, *budget_inout < 0)) {
              ps->ps_run_call_state = 5;
              goto suspend;

              case 5:
                ps->ps_run_call_state = 0;
                if (ps->ps_check_i >= ps->ps_n) break;
                check_i = ps->ps_check_order[ps->ps_check_i];
            }

            /*  This checker is the producer?
             *  If that's true, we don't need to check.
             */
            if (check_i == producer) continue;

            c_it = ps->ps_it[check_i];
            pdb_iterator_call_reset(pdb, c_it);

            if (!pdb_iterator_sorted(pdb, c_it) ||
                !pdb_iterator_sorted(pdb, ps->ps_it[producer]) ||
                !pdb_iterator_statistics_done(pdb, c_it))
              checker_likes_find = false;
            else if (pdb_iterator_n(pdb, c_it) == 0)
              checker_likes_find = true;
            else {
              double find_cost_per_point, check_cost_per_point;
              double c_step, p_step, two_find_step;
              long long step_i_can_use;
              long long upper_bound = pdb_primitive_n(pdb);

              /*  Each "find" slides us on average stepsize/2
               *  across the iterator's numerical breadth.
               *
               *  So, two pairs of checker and producer finds
               *  together get us past p_step + c_step IDs.
               */
              c_step = step_size(pdb, c_it, upper_bound);
              p_step = step_size(pdb, ps->ps_it[producer], upper_bound);
              two_find_step = c_step + p_step;

              step_i_can_use =
                  (pdb_iterator_forward(pdb, it)
                       ? (it->it_high == PDB_ITERATOR_HIGH_ANY ? upper_bound
                                                               : it->it_high) -
                             ps->ps_id
                       : ps->ps_id - it->it_low);
              if (step_i_can_use < 1) step_i_can_use = 0.00001;

              if (c_step > step_i_can_use) c_step = step_i_can_use;

              if (p_step > step_i_can_use) p_step = step_i_can_use;

              if (two_find_step > step_i_can_use)
                two_find_step = step_i_can_use;

              find_cost_per_point =
                  ((double)(pdb_iterator_find_cost(pdb, c_it) +
                            pdb_iterator_find_cost(pdb, ps->ps_it[producer])) *
                   2.0) /
                  two_find_step;

              /*  Each "next+check" step gets us past
               *  p_step IDs.
               */
              check_cost_per_point =
                  (double)(pdb_iterator_next_cost(pdb, ps->ps_it[producer]) +
                           pdb_iterator_check_cost(pdb, c_it)) /
                  p_step;

              checker_likes_find = find_cost_per_point < check_cost_per_point;

              cl_log(
                  cl, CL_LEVEL_VERBOSE,
                  "graphd_iterator_and_run: "
                  "subiterator %s: find cost %.3f, "
                  "(c.fc=%lld + p.fc=%lld)*2"
                  "/(p_step=%.3f+c_step=%.3f;step_i_can_use=%lld);"
                  "check_cost %.3f (nc=%lld + cc=%lld)/p_step=%.3f",
                  pdb_iterator_to_string(pdb, c_it, buf, sizeof buf),
                  find_cost_per_point,
                  (long long)pdb_iterator_find_cost(pdb, c_it),
                  (long long)pdb_iterator_find_cost(pdb, ps->ps_it[producer]),
                  p_step, c_step, step_i_can_use,

                  check_cost_per_point,
                  (long long)pdb_iterator_next_cost(pdb, ps->ps_it[producer]),
                  (long long)pdb_iterator_check_cost(pdb, c_it), p_step);
            }

            if (!checker_likes_find) {
              /* Perform a check.
               */
              cl_assert(cl, ps->ps_check_i < ps->ps_n);
              cl_assert(cl, ps->ps_check_order != NULL);
              check_i = ps->ps_check_order[ps->ps_check_i];
              c_it = ps->ps_it[check_i];
              pdb_iterator_call_reset(pdb, c_it);

              case 4:
                ps->ps_run_call_state = 0;
                if (ps->ps_check_i >= ps->ps_n) {
                  err = 0;
                  break;
                }
                check_i = ps->ps_check_order[ps->ps_check_i];
                c_it = ps->ps_it[check_i];
                PDB_IS_ITERATOR(cl, c_it);

                cl_log(cl, CL_LEVEL_VERBOSE,
                       "check %llx against "
                       "iterator #%d (producer is %d), %s ($%lld)",
                       (unsigned long long)ps->ps_id, (int)check_i,
                       (int)producer,
                       pdb_iterator_to_string(pdb, c_it, buf, sizeof buf),
                       *budget_inout);

                err = pdb_iterator_check(pdb, c_it, ps->ps_id, budget_inout);
                if (err != 0) {
                  if (err == PDB_ERR_MORE) {
                    ps->ps_run_call_state = 4;
                    goto suspend;
                  }
                  if (err != GRAPHD_ERR_NO) goto unexpected_check_error;

                  cl_log(cl, CL_LEVEL_VERBOSE,
                         "graphd_iterator_and_run: check #%zu "
                         "(%s) fails: %llx: %s",
                         ps->ps_check_i,
                         pdb_iterator_to_string(pdb, c_it, buf, sizeof buf),
                         (unsigned long long)ps->ps_id, graphd_strerror(err));
                  break;
                }
                continue;
            }

            /*  Perform a find.
             */
            check_i = ps->ps_check_order[ps->ps_check_i];
            c_it = ps->ps_it[check_i];
            pdb_iterator_call_reset(pdb, c_it);

            if (GRAPHD_SABOTAGE(gia->gia_graphd, *budget_inout < 0)) {
              ps->ps_run_call_state = 6;
              goto suspend;
              case 6:
                ps->ps_run_call_state = 0;
                if (ps->ps_check_i >= ps->ps_n) {
                  err = 0;
                  break;
                }
            }

            check_i = ps->ps_check_order[ps->ps_check_i];
            c_it = ps->ps_it[check_i];
            changed = false;

            cl_log(cl, CL_LEVEL_VERBOSE, "find %llx in iterator #%zu, %s",
                   (unsigned long long)ps->ps_id, check_i,
                   pdb_iterator_to_string(pdb, c_it, buf, sizeof buf));

            err = pdb_iterator_find(pdb, c_it, ps->ps_id, &id_found,
                                    budget_inout);
            if (err != 0) {
              if (err != PDB_ERR_MORE) goto done;
              ps->ps_run_call_state = 6;
              goto suspend;
            }
            cl_assert(cl, id_found <= ADDB_U5_MAX);

            /*  Not changing the ID is like passing
             *  pdb_iterator_check() -- just move on to
             *  the next condition.
             */
            if (ps->ps_id == id_found) continue;

            /*  Tell the producer where to resume.
             */
            ps->ps_next_find_resume_id = ps->ps_id = id_found;
            err = GRAPHD_ERR_NO;

            break;
          }

          cl_assert(cl, err != 0 || ps->ps_check_i >= ps->ps_n);

          if (err == GRAPHD_ERR_NO &&
              GRAPHD_SABOTAGE(gia->gia_graphd, *budget_inout <= 0)) {
            ps->ps_run_call_state = 7;
            goto suspend;
          }

      } while (err == GRAPHD_ERR_NO);
  }

done:
  ps->ps_run_cost += budget_in - *budget_inout;
  if (err != 0) {
    if (err == GRAPHD_ERR_NO) ps->ps_eof = true;

    cl_leave(cl, CL_LEVEL_VERBOSE, "%s: %s ($%lld)",
             pdb_iterator_to_string(pdb, it, buf, sizeof buf),
             err == GRAPHD_ERR_NO ? "done" : graphd_strerror(err),
             (long long)(budget_in - *budget_inout));
  } else
    cl_leave(cl, CL_LEVEL_VERBOSE, "%s: %llu ($%lld)",
             pdb_iterator_to_string(pdb, it, buf, sizeof buf),
             (unsigned long long)ps->ps_id,
             (long long)(budget_in - *budget_inout));
  return err;

unexpected_check_error:
  cl_assert(cl, c_it != NULL);
  cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_check", err,
               "iterator=%s, id=%llu",
               pdb_iterator_to_string(pdb, c_it, buf, sizeof buf),
               (unsigned long long)ps->ps_id);
  cl_leave(cl, CL_LEVEL_VERBOSE, "unexpected error: %s", graphd_strerror(err));
  return err;

suspend:
  ps->ps_run_cost += budget_in - *budget_inout;
  cl_leave(cl, CL_LEVEL_VERBOSE, "resume %hd ($%lld)", ps->ps_run_call_state,
           (long long)(budget_in - *budget_inout));
  return PDB_ERR_MORE;
}

/*  A "find" is like a next with a slightly different
 *  starting point.
 */
int graphd_iterator_and_find_loc(pdb_handle *pdb, pdb_iterator *it,
                                 pdb_id id_in, pdb_id *id_out,
                                 pdb_budget *budget_inout, char const *file,
                                 int line) {
  graphd_iterator_and *const gia = it->it_theory;
  and_process_state *const ps = &gia->gia_ps;
  cl_handle *const cl = gia->gia_cl;
  pdb_budget budget_in = *budget_inout;
  int err;

  /* Come back when there's budget!
   */
  if (GRAPHD_SABOTAGE(gia->gia_graphd, *budget_inout < 0)) return PDB_ERR_MORE;

  pdb_rxs_push(pdb, "FIND %p and %llx state=%d+%d [%s:%d]", (void *)it,
               (unsigned long long)id_in, it->it_call_state,
               ps->ps_run_call_state, file, line);

  cl_assert(cl, pdb_iterator_sorted(pdb, it));

  /*  We no longer care about the resume ID - with the start
   *  of the find, our previous position is irrelevant.
   */
  gia->gia_resume_id = PDB_ID_NONE;
  gia->gia_id = PDB_ID_NONE;
  gia->gia_ps.ps_eof = false;

  err = graphd_iterator_and_access(pdb, it, budget_inout, 1.0);
  if (err != GRAPHD_ERR_ALREADY) {
    if (err == 0) {
      pdb_rxs_pop(pdb, "FIND %p and %llx redirect ($%lld)", (void *)it,
                  (unsigned long long)id_in,
                  (long long)(budget_in - *budget_inout));

      return pdb_iterator_find_loc(pdb, it, id_in, id_out, budget_inout, file,
                                   line);
    }

    if (err == PDB_ERR_MORE)
      pdb_rxs_pop(pdb,
                  "FIND %p and %llx suspend; "
                  "state=%d ($%lld)",
                  (void *)it, (unsigned long long)id_in, it->it_call_state,
                  (long long)(budget_in - *budget_inout));
    else
      pdb_rxs_pop(pdb, "FIND %p and %llx error %s ($%lld)", (void *)it,
                  (unsigned long long)id_in, graphd_strerror(err),
                  (long long)(budget_in - *budget_inout));
    goto err;
  }

  cl_assert(cl, pdb_iterator_statistics_done(pdb, it));

  if (it->it_call_state == 0) {
    ps->ps_id = id_in;
    ps->ps_eof = false;

    /*  Do we know for sure that we're out of range?
     */
    if (pdb_iterator_forward(pdb, it) ? ps->ps_id >= it->it_high
                                      : ps->ps_id < it->it_low) {
      err = GRAPHD_ERR_NO;
      goto done;
    }

    /*  Is the next value cached?
     */
    gia->gia_cache_offset_valid = false;
    if ((err = graphd_iterator_cache_search(
             pdb, it, ogia(it)->gia_cache, &ps->ps_id,
             &gia->gia_cache_offset)) != PDB_ERR_MORE) {
      if (err == 0) {
        /*  Go past the value we're returning.
         */
        gia->gia_cache_offset_valid = true;
        gia->gia_cache_offset++;

        *budget_inout -= graphd_iterator_cache_cost(ogia(it)->gia_cache);
      }
      goto done;
    }

    /*  Mark that our cache position is invalid.
     */
    gia->gia_cache_offset_valid = false;
    ps->ps_run_call_state = GRAPHD_ITERATOR_AND_RUN_FIND_START;
    it->it_call_state = 1;

    /*  Make sure that we have an iterator state
     *  to actually walk around with.
     */
    err = graphd_iterator_and_process_state_initialize(pdb, it, ps);
    if (err != 0) goto done;
    cl_assert(cl, ps->ps_it != NULL);
  }

  cl_assert(cl, ps->ps_it != NULL);
  err = graphd_iterator_and_run(it, ogia(it)->gia_producer, ps, budget_inout);
done:
  if (err == PDB_ERR_MORE) {
    pdb_rxs_pop(pdb, "FIND %p and %llx suspend; state=%d+%d ($%lld)",
                (void *)it, (unsigned long long)id_in, it->it_call_state,
                ps->ps_run_call_state, (long long)(budget_in - *budget_inout));
    goto err;
  }

  it->it_call_state = 0;
  if (err == 0) {
    gia->gia_id = *id_out = ps->ps_id;
    pdb_rxs_pop(pdb, "FIND %p and %llx -> %llx ($%lld)", (void *)it,
                (unsigned long long)id_in, (unsigned long long)*id_out,
                (long long)(budget_in - *budget_inout));
  } else {
    ps->ps_id = PDB_ID_NONE;
    if (err == GRAPHD_ERR_NO) {
      ps->ps_eof = true;
      pdb_rxs_pop(pdb, "FIND %p and %llx EOF ($%lld)", (void *)it,
                  (unsigned long long)id_in,
                  (long long)(budget_in - *budget_inout));
    } else
      pdb_rxs_pop(pdb, "FIND %p and %llx: error %s ($%lld)", (void *)it,
                  (unsigned long long)id_in, graphd_strerror(err),
                  (long long)(budget_in - *budget_inout));
  }
err:
  pdb_iterator_account_charge_budget(pdb, it, find);
  return err;
}
