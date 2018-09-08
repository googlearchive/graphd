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
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

static unsigned long long graphd_timeval_to_micros(
    struct timeval const *const t) {
  return (unsigned long long)t->tv_sec * 1000000ull + t->tv_usec;
}

int graphd_runtime_statistics_get(graphd_request *greq,
                                  graphd_runtime_statistics *st) {
  struct rusage ru;
  struct timeval tv;
  graphd_handle *graphd;

  if (greq == NULL) return EINVAL;

  graphd = graphd_request_graphd(greq);

  memset(st, 0, sizeof(*st));

  if (getrusage(RUSAGE_SELF, &ru) == 0) {
    st->grts_system_micros = graphd_timeval_to_micros(&ru.ru_stime);
    st->grts_user_micros = graphd_timeval_to_micros(&ru.ru_utime);
    st->grts_minflt = ru.ru_minflt;
    st->grts_majflt = ru.ru_majflt;
  }
  if (gettimeofday(&tv, NULL) == 0) {
    st->grts_endtoend_micros = st->grts_wall_micros =
        graphd_timeval_to_micros(&tv);
  }

  if (graphd->g_diary_cl != NULL) {
    cl_log(graphd->g_diary_cl, CL_LEVEL_DETAIL,
           "SYSTIME=tr=%llu ts=%llu tu=%llu pr=%llu pf=%llu",
           (unsigned long long)st->grts_wall_micros / 1000ull,
           (unsigned long long)st->grts_system_micros / 1000ull,
           (unsigned long long)st->grts_user_micros / 1000ull,
           (unsigned long long)st->grts_minflt,
           (unsigned long long)st->grts_majflt);
  } else {
    cl_handle *cl;
    cl = pdb_log(graphd->g_pdb);
    cl_log(cl, CL_LEVEL_INFO, "XXX no diary!");
  }

  st->grts_values_allocated = graphd->g_rts_values_allocated;

  pdb_runtime_statistics_get(graphd->g_pdb, &st->grts_pdb);
  return 0;
}

/**
 * @brief Compute a - b, correctly handling wraparound.
 * @param a	The larger (later) of two runtime statistics samples
 * @param b	The smaller (earlier) of two runtime statistics samples
 * @param c	Out: a - b.
 */
void graphd_runtime_statistics_diff(graphd_request *greq,
                                    graphd_runtime_statistics const *a,
                                    graphd_runtime_statistics const *b,
                                    graphd_runtime_statistics *c) {
  pdb_runtime_statistics_diff(&a->grts_pdb, &b->grts_pdb, &c->grts_pdb);

#define SUB(x) c->x = a->x - b->x

  SUB(grts_system_micros);
  SUB(grts_user_micros);
  SUB(grts_wall_micros);
  SUB(grts_endtoend_micros);
  SUB(grts_minflt);
  SUB(grts_majflt);
  SUB(grts_values_allocated);

#undef SUB
}

/**
 * @brief Compute a + b,
 * @param a	One of the structs to add
 * @param b	Other struct to add.
 * @param c	Out: a + b.
 */
void graphd_runtime_statistics_add(graphd_runtime_statistics const *a,
                                   graphd_runtime_statistics const *b,
                                   graphd_runtime_statistics *c) {
  pdb_runtime_statistics_add(&a->grts_pdb, &b->grts_pdb, &c->grts_pdb);

#define ADD(x) c->x = a->x + b->x

  ADD(grts_system_micros);
  ADD(grts_user_micros);
  ADD(grts_wall_micros);
  ADD(grts_minflt);
  ADD(grts_majflt);
  ADD(grts_values_allocated);

/*  We're leaving the end-to-end micros unchanged.
 *  They're not accumulated per-processing-phase,
 *  but just set once, at the end.
 */
#undef ADD
}

/**
 * @brief Massage statistics for publication.
 * @param a	Input - the true result.
 * @param b	Out - the result users see.
 */
void graphd_runtime_statistics_publish(graphd_runtime_statistics const *a,
                                       graphd_runtime_statistics *b) {
  static int bucket = 0;

  *b = *a;

  b->grts_system_millis = b->grts_system_micros / 1000;
  bucket += b->grts_system_micros % 1000;
  if (bucket >= 1000) {
    b->grts_system_millis++;
    bucket -= 1000;
  }

  b->grts_user_millis = b->grts_user_micros / 1000;
  bucket += b->grts_user_micros % 1000;
  if (bucket >= 1000) {
    b->grts_user_millis++;
    bucket -= 1000;
  }

  b->grts_wall_millis = b->grts_wall_micros / 1000;
  bucket += b->grts_wall_micros % 1000;
  if (bucket >= 1000) {
    b->grts_wall_millis++;
    bucket -= 1000;
  }

  b->grts_endtoend_millis = b->grts_endtoend_micros / 1000;
  bucket += b->grts_endtoend_micros % 1000;
  if (bucket >= 1000) {
    b->grts_endtoend_millis++;
    bucket -= 1000;
  }

  /*  usertime + systemtime <= walltime.  I don't care about
   *  the intermediate results, but if it's not true for the
   *  printed cost output, people are going to wonder.
   *
   *  So, fake it.
   *
   *  (The system time granuarity is more something like hundredth
   *  of a second, so it's not surprising that small-resolution
   *  results sometimes are slightly off.)
   */
  if (b->grts_user_millis + b->grts_system_millis > b->grts_wall_millis)
    b->grts_wall_millis = b->grts_user_millis + b->grts_system_millis;

  /*  Similarly, walltime <= end-to-end time.
   */
  if (b->grts_wall_millis > b->grts_endtoend_millis)
    b->grts_endtoend_millis = b->grts_wall_millis;
}

/**
 * @brief Initialize the set of statistics to the largest set possible.
 * @param r	set to initialize
 */
void graphd_runtime_statistics_max(graphd_runtime_statistics *r) {
  pdb_runtime_statistics_max(&r->grts_pdb);

  /* I'm using ULONG_MAX / 2, rather than just ULONG_MAX,
   * to still detect programmer errors/integer overflows.
   */

  r->grts_system_micros = r->grts_user_micros = r->grts_wall_micros =
      r->grts_endtoend_micros = r->grts_system_millis = r->grts_user_millis =
          r->grts_endtoend_millis = r->grts_wall_millis = r->grts_minflt =
              r->grts_majflt = r->grts_values_allocated =
                  (unsigned long long)-1 / 2;
}

/**
 * @brief Add to <acc> the difference between <before> and now.
 * @param acc		Accumulate the total here.
 * @param before	A starting point measured earlier.
 */
void graphd_runtime_statistics_accumulate(
    graphd_request *greq, graphd_runtime_statistics *acc,
    graphd_runtime_statistics const *before) {
  graphd_runtime_statistics now;
  graphd_runtime_statistics diff;
  cl_handle *cl = graphd_request_cl(greq);

  graphd_runtime_statistics_get(greq, &now);

  if (graphd_runtime_statistics_exceeds(before, &now, NULL)) {
    /*  This typically happens as a result of NTP jitters;
     *  TIME TRAVEL messages from graphd should coincide with
     *  ntp.log messages about adjusting the system clock
     *  more than just by gradually slowing and speeding up
     *  (which shouldn't violate continuity).
     */
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "TIME TRAVEL?  "
           "(Compensating...)  Before: "
           "tu=%llu "  /* time/user			*/
           "ts=%llu "  /* time/system			*/
           "tr=%llu "  /* time/real			*/
           "te=%llu "  /* time/endtoend		*/
           "pr=%llu "  /* page reclaims		*/
           "pf=%llu "  /* page faults			*/
           "dw=%llu "  /* primitive writes 		*/
           "dr=%llu "  /* primitive reads 		*/
           "in=%llu "  /* gmap size reads		*/
           "ir=%llu "  /* gmap reads			*/
           "iw=%llu "  /* gmap writes			*/
           "va=%llu; " /* values allocated		*/

           "now: "
           "tu=%llu " /* time/user			*/
           "ts=%llu " /* time/system			*/
           "tr=%llu " /* time/real			*/
           "te=%llu " /* time/endtoend		*/
           "pr=%llu " /* page reclaims		*/
           "pf=%llu " /* page faults			*/
           "dw=%llu " /* primitive writes 		*/
           "dr=%llu " /* primitive reads 		*/
           "in=%llu " /* gmap size reads		*/
           "ir=%llu " /* gmap reads			*/
           "iw=%llu " /* gmap writes			*/
           "va=%llu", /* values allocated		*/

           before->grts_user_micros,
           before->grts_system_micros, before->grts_wall_micros,
           before->grts_endtoend_micros, before->grts_minflt,
           before->grts_majflt, before->grts_pdb.rts_primitives_written,
           before->grts_pdb.rts_primitives_read,
           before->grts_pdb.rts_index_extents_read,
           before->grts_pdb.rts_index_elements_read,
           before->grts_pdb.rts_index_elements_written,
           before->grts_values_allocated,

           now.grts_user_micros, now.grts_system_micros, now.grts_wall_micros,
           now.grts_endtoend_micros, now.grts_minflt, now.grts_majflt,
           now.grts_pdb.rts_primitives_written,
           now.grts_pdb.rts_primitives_read,
           now.grts_pdb.rts_index_extents_read,
           now.grts_pdb.rts_index_elements_read,
           now.grts_pdb.rts_index_elements_written, now.grts_values_allocated);

    /*  Any value in <now> that is less than <before> gets
     *  raised to <before>.
     */
    graphd_runtime_statistics_limit_below(before, &now);
  }
  graphd_runtime_statistics_diff(greq, &now, before, &diff);
  graphd_runtime_statistics_add(acc, &diff, acc);

  /*  If we're spending a lot of time ("tr"), and it's accounted for
   *  neither as user nor as system time ("tu + ts"), something is
   *  going wrong.  (I.e., we were blocked while working on a request?)
   *  Make a note.
   *
   *  This, too, may be the result of time adjustment jitters.
   */
  if (diff.grts_wall_micros > 100000 /* more than 100 millis */
      &&
      diff.grts_wall_micros >
          5 * (diff.grts_user_micros + diff.grts_system_micros)) {
    cl_handle *const netlog_cl = srv_netlog(graphd_request_srv(greq));
    if (netlog_cl != NULL) {
      cl_log(
          netlog_cl, CL_LEVEL_INFO | GRAPHD_FACILITY_COST,
          "graphd.request.time-lapse: "
          "TID: %s "               /* transaction ID 	*/
          "%s"                     /* address_parts	*/
          "(l)graphd.sesid: %llu " /* local session ID */
          "(l)graphd.reqid: %llu " /* local request ID */
          "graphd.request.cost.delta: "
          "tu=%llu " /* time/user		*/
          "ts=%llu " /* time/system		*/
          "tr=%llu " /* time/real		*/
          "te=%llu " /* time/end-to-end	*/
          "pr=%llu " /* page reclaims	*/
          "pf=%llu " /* page faults		*/
          "dw=%llu " /* primitive writes 	*/
          "dr=%llu " /* primitive reads 	*/
          "in=%llu " /* gmap size reads	*/
          "ir=%llu " /* gmap reads		*/
          "iw=%llu " /* gmap writes		*/
          "va=%llu", /* values allocated	*/

          greq->greq_req.req_display_id ? greq->greq_req.req_display_id : "???",
          greq->greq_req.req_session->ses_netlog_header
              ? greq->greq_req.req_session->ses_netlog_header
              : "",
          greq->greq_req.req_session->ses_id, greq->greq_req.req_id,

          diff.grts_user_micros / 1000ull, diff.grts_system_micros / 1000ull,
          diff.grts_wall_micros / 1000ull, diff.grts_endtoend_micros / 1000ull,
          diff.grts_minflt, diff.grts_majflt,
          diff.grts_pdb.rts_primitives_written,
          diff.grts_pdb.rts_primitives_read,
          diff.grts_pdb.rts_index_extents_read,
          diff.grts_pdb.rts_index_elements_read,
          diff.grts_pdb.rts_index_elements_written, diff.grts_values_allocated);
    }
  }

  /*  End-to-end values just are copied into the accumulator;
   *  they are not added up piece-by-piece.
   */
  acc->grts_endtoend_micros =
      now.grts_endtoend_micros - acc->grts_endtoend_micros_start;
}

/**
 * @brief Does small exceed large in any of its metrics?
 * @param small		Is this so large it exceeds large anywhere?
 * @param large		This normally is larger than small.
 */
bool graphd_runtime_statistics_exceeds(graphd_runtime_statistics const *small,
                                       graphd_runtime_statistics const *large,
                                       graphd_runtime_statistics *report) {
#define TST(x)                                \
  if (small->x > large->x) {                  \
    if (report != NULL) report->x = small->x; \
    return true;                              \
  }

  TST(grts_system_micros);
  TST(grts_user_micros);
  TST(grts_wall_micros);
  TST(grts_endtoend_micros);
  TST(grts_minflt);
  TST(grts_majflt);
  TST(grts_values_allocated);

#undef TST

  return pdb_runtime_statistics_exceeds(&small->grts_pdb, &large->grts_pdb,
                                        report ? &report->grts_pdb : NULL);
}

/**
 * @brief If any part of <large> is less than <lower_limit>,
 *	reset it to <lower_limit>.
 * @param large		This normally is larger than small.
 * @param lower_limit	Is this so large it exceeds large anywhere?
 */
void graphd_runtime_statistics_limit_below(
    graphd_runtime_statistics const *lower_limit,
    graphd_runtime_statistics *large) {
#define LIMIT(x)                  \
  if (large->x >= lower_limit->x) \
    ;                             \
  else                            \
  large->x = lower_limit->x

  LIMIT(grts_system_micros);
  LIMIT(grts_user_micros);
  LIMIT(grts_wall_micros);
  LIMIT(grts_endtoend_micros);
  LIMIT(grts_minflt);
  LIMIT(grts_majflt);
  LIMIT(grts_values_allocated);

#undef LIMIT

  pdb_runtime_statistics_limit_below(&lower_limit->grts_pdb, &large->grts_pdb);
}

/**
 * @brief Limit a runtime allowance structure to another structure
 *
 *   Calculate the component-wise minimum of two structures.
 *
 * @param req 		Limit this structure
 * @param lim		apply these limits.
 */
void graphd_runtime_statistics_limit(graphd_runtime_statistics *req,
                                     graphd_runtime_statistics const *lim) {
#define LIMIT(op) (void)(((req)->op > (lim)->op) && ((req)->op = (lim)->op))

  LIMIT(grts_wall_millis);
  LIMIT(grts_system_millis);
  LIMIT(grts_user_millis);
  LIMIT(grts_minflt);
  LIMIT(grts_majflt);
  LIMIT(grts_values_allocated);

  /* sticking this pdb and cm stuff in here seems a bit
   * slap-dash given all of the trouble not to in the above code. MMP */

  LIMIT(grts_pdb.rts_primitives_read);
  LIMIT(grts_pdb.rts_primitives_written);
  LIMIT(grts_pdb.rts_index_elements_written);
  LIMIT(grts_pdb.rts_index_elements_read);
  LIMIT(grts_pdb.rts_index_extents_read);
}

void graphd_runtime_statistics_start_request(graphd_request *greq) {
  if (!greq->greq_runtime_statistics_started) {
    unsigned long long tmp;

    /*  The end-to-end micros were set when the request first
     *  finished parsing.  But some requests don't come in through
     *  the parser - set their end-to-end time now.
     */
    tmp = greq->greq_runtime_statistics.grts_endtoend_micros;

    memset(&greq->greq_runtime_statistics_accumulated, 0,
           sizeof(graphd_runtime_statistics));

    graphd_runtime_statistics_get(greq, &greq->greq_runtime_statistics);

    /* If we had end-to-end micros from before, restore them.
     * If not, use the current time.
     */
    if (tmp == 0) tmp = greq->greq_runtime_statistics.grts_endtoend_micros;

    greq->greq_runtime_statistics_accumulated.grts_endtoend_micros_start = tmp;

    greq->greq_runtime_statistics_started = true;
  } else {
    graphd_runtime_statistics_get(greq, &greq->greq_runtime_statistics);
  }
}
