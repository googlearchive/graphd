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

#define GRAPHD_AND_CACHE_INLINE_MAX 10

static void and_promote_producer_ordering_into_copies(pdb_handle *pdb,
                                                      pdb_iterator *it) {
  pdb_iterator *p_it;
  graphd_iterator_and *gia = it->it_theory;

  if (pdb_iterator_statistics_done(pdb, it)) {
    if (gia->gia_ps.ps_it != NULL) {
      p_it = gia->gia_ps.ps_it[ogia(it)->gia_producer];
      pdb_iterator_ordering_set(pdb, p_it, pdb_iterator_ordering(pdb, it));
      pdb_iterator_ordered_set(pdb, p_it, pdb_iterator_ordered(pdb, it));
    }

    if (ogia(it)->gia_cache_ps.ps_it != NULL) {
      p_it = ogia(it)->gia_cache_ps.ps_it[ogia(it)->gia_producer];
      pdb_iterator_ordering_set(pdb, p_it, pdb_iterator_ordering(pdb, it));
      pdb_iterator_ordered_set(pdb, p_it, pdb_iterator_ordered(pdb, it));
    }
  }
}

static int and_freeze_process_state(pdb_handle *pdb,
                                    and_process_state const *ps,
                                    cm_buffer *buf) {
  int err;
  size_t i;
  char b1[42], b2[42], b3[42], b4[42], b5[42], b6[42];
  cl_handle *cl = pdb_log(pdb);
  size_t o0 = buf->buf_n;

  if (ps->ps_it == NULL) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "and_freeze_process_state: uninitialized - "
           "saving just id %lld",
           (long long)ps->ps_id);

    /*  Uninitialized.
     */
    return cm_buffer_add_string(
        buf, pdb_id_to_string(pdb, ps->ps_id, b1, sizeof b1));
  }

  cl_enter(cl, CL_LEVEL_VERBOSE, "ps->ps_n=%zu", ps->ps_n);
  err = cm_buffer_sprintf(
      buf, "[ps:%s:%s:%s:%s:%s:%s:%hu:%u:%llu:%lld:%zu:%u:%zu",
      pdb_id_to_string(pdb, ps->ps_id, b1, sizeof b1),
      pdb_id_to_string(pdb, ps->ps_producer_id, b2, sizeof b2),
      pdb_id_to_string(pdb, ps->ps_next_find_resume_id, b3, sizeof b3),
      pdb_id_to_string(pdb, ps->ps_find_id, b4, sizeof b4),
      pdb_id_to_string(pdb, ps->ps_check_exclude_low, b5, sizeof b5),
      pdb_id_to_string(pdb, ps->ps_check_exclude_high, b6, sizeof b6),
      ps->ps_run_call_state, ps->ps_eof, ps->ps_run_produced_n, ps->ps_run_cost,
      ps->ps_check_i, ps->ps_check_order_version, ps->ps_n);
  if (err != 0) goto err;

  for (i = 0; i < ps->ps_n; i++) {
    cl_assert(cl, ps->ps_it[i] != NULL);
    err = cm_buffer_sprintf(buf, "(%zu:", ps->ps_check_order[i]);
    if (err != 0) goto err;

    err = pdb_iterator_freeze(pdb, ps->ps_it[i], PDB_ITERATOR_FREEZE_EVERYTHING,
                              buf);
    if (err != 0) {
      char bx[200];
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_freeze", err, "it=%s",
                   pdb_iterator_to_string(pdb, ps->ps_it[i], bx, sizeof bx));
      goto err;
    }

    err = cm_buffer_add_string(buf, ")");
    if (err != 0) goto err;
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "\"%.*s]\"", (int)(cm_buffer_length(buf) - o0),
           cm_buffer_memory(buf) + o0);
  return cm_buffer_add_string(buf, "]");

err:
  cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
  return err;
}

static int and_skip_process_state(graphd_request *greq, char const **s_ptr,
                                  char const *e) {
  graphd_handle *g = graphd_request_graphd(greq);
  pdb_handle *pdb = g->g_pdb;

  if (*s_ptr < e && **s_ptr != '[') {
    pdb_id dummy;
    return pdb_iterator_util_thaw(pdb, s_ptr, e, "%{id}", &dummy);
  }
  *s_ptr = pdb_unparenthesized(*s_ptr + 1, e, ']');
  if (*s_ptr && *s_ptr < e && **s_ptr == ']') (*s_ptr)++;
  return 0;
}

static int and_thaw_process_state(graphd_request *greq, cm_handle *cm,
                                  char const **s_ptr, char const *e,
                                  cl_loglevel loglevel, and_process_state *ps) {
  graphd_handle *g = graphd_request_graphd(greq);
  pdb_handle *pdb = g->g_pdb;
  cl_handle *cl = graphd_request_cl(greq);
  int err;
  size_t i = 0;
  int eof, run_call_state;
  unsigned long check_order_version;
  char const *s0;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");

  /* Initialize with 0.
   */
  graphd_iterator_and_process_state_clear(ps);

  if (*s_ptr >= e || **s_ptr != '[') {
    /* Uninitialized. */
    graphd_iterator_and_process_state_clear(ps);
    cl_leave(cl, CL_LEVEL_VERBOSE, "empty");
    return pdb_iterator_util_thaw(pdb, s_ptr, e, "%{id}", &ps->ps_id);
  }

  s0 = *s_ptr;
  err = pdb_iterator_util_thaw(
      pdb, s_ptr, e,
      "[ps:%{id}:%{id}:%{id}:%{id}:%{id}:%{id}:%d:%d:%llu:%{budget}:%zu:%lu:%"
      "zu",
      &ps->ps_id, &ps->ps_producer_id, &ps->ps_next_find_resume_id,
      &ps->ps_find_id, &ps->ps_check_exclude_low, &ps->ps_check_exclude_high,
      &run_call_state, &eof, &ps->ps_run_produced_n, &ps->ps_run_cost,
      &ps->ps_check_i, &check_order_version, &ps->ps_n);
  if (err != 0) {
    cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                 "cannot parse \"%.*s\"", (int)(e - s0), s0);
    cl_leave(cl, CL_LEVEL_VERBOSE, "syntax error");
    goto err;
  }

  if (ps->ps_n > e - *s_ptr || ps->ps_check_i > ps->ps_n) {
    cl_log(cl, loglevel,
           "and_thaw_process_state: number values "
           "out of range: ps->ps_n %zu, remaining "
           "bytes of state %zu, check_i %zu",
           ps->ps_n, (size_t)(e - *s_ptr), ps->ps_check_i);
    err = GRAPHD_ERR_SYNTAX;
    goto err;
  }

  ps->ps_run_call_state = run_call_state;
  ps->ps_eof = eof;
  ps->ps_check_order_version = check_order_version;

  ps->ps_check_order = cm_malloc(cm, ps->ps_n * sizeof(*ps->ps_check_order));
  if (ps->ps_check_order == NULL) {
    err = errno ? errno : ENOMEM;
    goto err;
  }

  ps->ps_it = cm_malloc(cm, ps->ps_n * sizeof(*ps->ps_it));
  if (ps->ps_it == NULL) {
    err = errno ? errno : ENOMEM;
    goto err;
  }

  for (i = 0; i < ps->ps_n; i++) {
    char const *sub0;
    char const *sub_s, *sub_e;

    sub0 = *s_ptr;
    err = pdb_iterator_util_thaw(pdb, s_ptr, e, "%{(bytes)}", &sub_s, &sub_e);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                   "expected (...), got %.*s", (int)(e - sub0), sub0);
      goto err;
    }

    sub0 = sub_s;
    err = pdb_iterator_util_thaw(pdb, &sub_s, sub_e, "%zu:",
                                 ps->ps_check_order + i);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                   "expected N:.., got %.*s", (int)(sub_e - sub0), sub0);
      goto err;
    }

    sub0 = sub_s;
    err = graphd_iterator_thaw_bytes(greq, sub_s, sub_e, 0, loglevel,
                                     ps->ps_it + i);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "graphd_iterator_thaw_bytes", err, "%.*s",
                   (int)(sub_e - sub0), sub0);
      goto err;
    }
  }

  /* Optional extensible content.
   */
  err = pdb_iterator_util_thaw(pdb, s_ptr, e, "%{extensions}",
                               (pdb_iterator_property *)NULL);
  if (err != 0) goto err;

  if (*s_ptr < e && **s_ptr == ']')
    ++*s_ptr;
  else {
    cl_log(cl, loglevel,
           "and_thaw_process_state: trailing garbage"
           " in state string (expected closing ']')"
           ": \"%.*s\"",
           (int)(e - *s_ptr), *s_ptr);
    errno = GRAPHD_ERR_LEXICAL;
    goto err;
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "ok; %p callstate=%d, ps_id=%lld, nfr_id=%lld",
           (void *)ps, ps->ps_run_call_state, (long long)ps->ps_id,
           (long long)ps->ps_next_find_resume_id);
  return 0;

err:
  /* Free the subiterators we already unthawed.
   */
  if (ps->ps_it != NULL) {
    while (i > 0) {
      i--;
      pdb_iterator_destroy(g->g_pdb, ps->ps_it + i);
    }
    cm_free(cm, ps->ps_it);
  }
  if (ps->ps_check_order != NULL) cm_free(cm, ps->ps_check_order);

  graphd_iterator_and_process_state_clear(ps);
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
  return err;
}

static int and_thaw_original_cache(graphd_request *greq,
                                   graphd_iterator_and *ogia,
                                   unsigned long version, char const **s_ptr,
                                   char const *e, cl_loglevel loglevel) {
  int err;
  char const *cache_s, *cache_e, *s0, *tmp_s;
  cl_handle *cl = ogia->gia_cl;
  graphd_handle *g = graphd_request_graphd(greq);
  pdb_id new_id;
  size_t old_cache_size;

  s0 = *s_ptr;

  err = pdb_iterator_util_thaw(ogia->gia_pdb, s_ptr, e, "%[]", &cache_s,
                               &cache_e);
  if (err != 0) {
    cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err, "%%[] vs %.*s",
                 (int)(e - *s_ptr), *s_ptr);
    return err;
  }
  cl_log(cl, CL_LEVEL_VERBOSE, "and_thaw_original_cache: cache is \"%.*s\"",
         (int)(cache_e - cache_s), cache_s);

  err = pdb_iterator_util_thaw(ogia->gia_pdb, &cache_s, cache_e, "cache:");
  if (err != 0) {
    *s_ptr = s0;
    cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err, "cache: vs. %.*s",
                 (int)(cache_e - cache_s), cache_s);
    return err;
  }

  old_cache_size = ogia->gia_cache->gic_n;
  err = graphd_iterator_cache_rethaw(ogia->gia_graphd, &cache_s, cache_e,
                                     loglevel, &ogia->gia_cache);

  if (err != 0 && err != GRAPHD_ERR_ALREADY) {
    /*  We wanted a cache, but couldn't find it;
     *  maybe it had been aged out of the storable
     *  cache.
     */
    if (err == GRAPHD_ERR_NO) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_cache_rethaw", err,
                   "%.*s", (int)(cache_e - s0), s0);
    } else
      cl_log_errno(cl, loglevel, "graphd_iterator_cache_rethaw", err, "%.*s",
                   (int)(cache_e - s0), s0);
    return err;
  }

  /*  Go with the cache's process state, instead of our own,
   *  if the cache's ID is further advanced in the cache than
   *  our own.
   */
  if (cache_s < cache_e && *cache_s == ':') cache_s++;

  /*  Not strictly necessary, but heads of a common
   *  CL_LEVEL_FAIL log.
   */
  tmp_s = cache_s;
  if (tmp_s >= cache_e || *tmp_s != '[') return 0;

  err =
      pdb_iterator_util_thaw(g->g_pdb, &tmp_s, cache_e, "[ps:%{id}:", &new_id);

  if (err != 0 || new_id == PDB_ID_NONE ||
      old_cache_size >= ogia->gia_cache->gic_n)
    return 0;

  s0 = cache_s;
  cl_log(cl, CL_LEVEL_VERBOSE,
         "and_thaw_original_cache: replacing "
         "process state %p with new version.",
         (void *)&ogia->gia_cache_ps);

  graphd_iterator_and_process_state_finish(ogia, &ogia->gia_cache_ps);
  err = and_thaw_process_state(greq, ogia->gia_cm, &cache_s, cache_e, loglevel,
                               &ogia->gia_cache_ps);
  if (err != 0) {
    cl_log_errno(cl, loglevel, "and_thaw_process_state", err, "%.*s",
                 (int)(cache_e - s0), s0);
    return err;
  }
  return 0;
}

static int and_freeze_original_cache(graphd_iterator_and const *ogia,
                                     cm_buffer *buf) {
  int err;

  err = cm_buffer_add_string(buf, "[cache:");
  if (err != 0) {
    return err;
  }

  err = graphd_iterator_cache_freeze(ogia->gia_graphd, ogia->gia_cache, buf);
  if (err != 0) {
    cl_log_errno(ogia->gia_cl, CL_LEVEL_FAIL, "graphd_iterator_cache_freeze",
                 err, "unexpected error");
    return err;
  }

  if (!ogia->gia_cache->gic_eof) {
    err = cm_buffer_add_string(buf, ":");
    if (err != 0) {
      cl_log_errno(ogia->gia_cl, CL_LEVEL_FAIL, "cm_buffer_add_string", err,
                   "unexpected error");
      return err;
    }

    err = and_freeze_process_state(ogia->gia_pdb, &ogia->gia_cache_ps, buf);
    if (err != 0) {
      cl_log_errno(ogia->gia_cl, CL_LEVEL_FAIL, "and_freeze_process_state", err,
                   "unexpected error");
      return err;
    }
  }

  err = cm_buffer_add_string(buf, "]");
  if (err) {
    cl_log_errno(ogia->gia_cl, CL_LEVEL_FAIL, "cm_buffer_add_string", err,
                 "unexpected error");
  }
  return err;
}

static int and_statistics_freeze_subcondition(graphd_iterator_and const *ogia,
                                              graphd_subcondition const *sc,
                                              cm_buffer *buf) {
  pdb_handle *pdb = ogia->gia_pdb;
  int err;
  size_t i;
  char const *csep = ":";
  cl_handle *cl = ogia->gia_cl;

  cl_enter(cl, CL_LEVEL_VERBOSE, "%zu of %zu",
           (size_t)(1 + (sc - ogia->gia_sc)), ogia->gia_n);

  err = cm_buffer_add_string(buf, "(");
  if (err != 0) goto err;

  err = and_freeze_process_state(pdb, &sc->sc_contest_ps, buf);
  if (err != 0) goto err;

  err = cm_buffer_sprintf(buf, ":%lld:%d:%llu:%d:%zu",
                          (long long)sc->sc_contest_cost, sc->sc_contest_state,
                          (unsigned long long)(0 * 100000ull), sc->sc_compete,
                          sc->sc_contest_id_n);
  if (err != 0) goto err;

  for (i = 0; i < sc->sc_contest_id_n; i++) {
    char b1[200];
    err = cm_buffer_sprintf(
        buf, "%s%s", csep,
        pdb_id_to_string(pdb, sc->sc_contest_id[i], b1, sizeof b1));
    if (err != 0) goto err;
    csep = ",";
  }

  err = cm_buffer_add_string(buf, ")");
  if (err != 0) goto err;

  cl_leave(cl, CL_LEVEL_VERBOSE, "leave");
  return 0;

err:
  cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
  return err;
}

static int and_statistics_thaw_subcondition(graphd_request *greq,
                                            graphd_iterator_and *ogia,
                                            unsigned long version,
                                            char const **s_ptr, char const *e,
                                            cl_loglevel loglevel,
                                            graphd_subcondition *sc) {
  pdb_handle *pdb = ogia->gia_pdb;
  int err, compete;
  size_t i, id_n;
  char const *sub_s, *sub_s0, *sub_e;
  unsigned long long pref;
  bool ignore;

  err = pdb_iterator_util_thaw(pdb, s_ptr, e, "%{(bytes)}", &sub_s, &sub_e);
  if (err != 0) {
    cl_log_errno(ogia->gia_cl, CL_LEVEL_FAIL, "pdb_iterator_util_thaw", err,
                 "couldn't get subcondition");
    return err;
  }

  sub_s0 = sub_s;

  /*  If this is an older (numerically lower) version of this
   *  iterator's state, we'll scan past it, but won't assign values.
   */
  ignore = version <= ogia->gia_original_version;

  if (ignore)
    err = and_skip_process_state(greq, &sub_s, sub_e);
  else {
    graphd_iterator_and_process_state_finish(ogia, &sc->sc_contest_ps);

    err = and_thaw_process_state(greq, ogia->gia_cm, &sub_s, sub_e, loglevel,
                                 &sc->sc_contest_ps);
    if (err != 0) {
      cl_log_errno(ogia->gia_cl, loglevel, "and_thaw_process_state", err,
                   "couldn't get subcondition's "
                   "contest state");
      return err;
    }
  }
  if (err != 0) {
    cl_log_errno(ogia->gia_cl, loglevel, "and_thaw_process_state", err,
                 "couldn't get subcondition's "
                 "contest state from \"%.*s\"",
                 (int)(sub_e - sub_s0), sub_s0);
    return err;
  }

  if (ignore) {
    pdb_budget budget;
    int state;

    err =
        pdb_iterator_util_thaw(pdb, &sub_s, sub_e, ":%{budget}:%d:%llu:%d:%zu",
                               &budget, &state, &pref, &compete, &id_n);
  } else {
    err = pdb_iterator_util_thaw(
        pdb, &sub_s, sub_e, ":%{budget}:%d:%llu:%d:%zu", &sc->sc_contest_cost,
        &sc->sc_contest_state, &pref, &compete, &id_n);
  }
  if (err != 0) return err;

  /* Optional extensible content.
   */
  err = pdb_iterator_util_thaw(pdb, &sub_s, sub_e, "%{extensions}",
                               (pdb_iterator_property *)NULL);
  if (err != 0) return err;

  if (!ignore && id_n > sc->sc_contest_id_n) sc->sc_contest_id_n = id_n;

  for (i = 0; i < id_n; i++) {
    pdb_id dummy;

    if (sub_s < sub_e && (*sub_s == ':' || *sub_s == ',')) sub_s++;
    err = pdb_iterator_util_thaw(pdb, &sub_s, sub_e, "%{id}",
                                 ignore ? &dummy : sc->sc_contest_id + i);
    if (err != 0) {
      cl_log_errno(ogia->gia_cl, CL_LEVEL_FAIL, "pdb_iterator_util_thaw", err,
                   "couldn't get subcondition's id");
      return err;
    }
  }

  /* End.
   */
  if (sub_s < sub_e && *sub_s == ':') sub_s++;
  err = pdb_iterator_util_thaw(pdb, &sub_s, sub_e, "%$");
  if (err != 0) return err;

  if (!ignore) sc->sc_compete = compete;
  return 0;
}

static int and_statistics_freeze(graphd_iterator_and *ogia, cm_buffer *buf) {
  int err;
  size_t i;

  cl_enter(ogia->gia_cl, CL_LEVEL_VERBOSE, "enter");

  /*	[stat-in-progress:TOSAVE:MAXBUDGET:
   */
  err = cm_buffer_sprintf(buf, "[stat-in-progress:%ld:%ld:",
                          (long)ogia->gia_contest_to_save, (long)0);
  if (err != 0) {
    cl_leave(ogia->gia_cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
    return err;
  }

  for (i = 0; i < ogia->gia_n; i++) {
    /*  (SUBSTAT1)(SUBSTAT2)...(SUBSTATN)
     */
    err = and_statistics_freeze_subcondition(ogia, ogia->gia_sc + i, buf);
    if (err != 0) {
      cl_leave(ogia->gia_cl, CL_LEVEL_VERBOSE, "error: %s",
               graphd_strerror(err));
      return err;
    }
  }
  cl_leave(ogia->gia_cl, CL_LEVEL_VERBOSE, "leave");
  return cm_buffer_add_string(buf, "]");
}

static int and_statistics_thaw(graphd_request *greq, graphd_iterator_and *ogia,
                               unsigned long version, char const **s_ptr,
                               char const *e, cl_loglevel loglevel) {
  int err;
  size_t i;
  unsigned long long budget;
  unsigned long long dummy;

  cl_enter(ogia->gia_cl, CL_LEVEL_VERBOSE, "version %lu, ogia version %lu",
           version, ogia->gia_original_version);

  /*	[stat-in-progress:TOSAVE:MAXBUDGET:
   */
  err = pdb_iterator_util_thaw(ogia->gia_pdb, s_ptr, e,
                               "[stat-in-progress:%{budget}:%{budget}:",
                               &budget, &dummy);
  if (err != 0) {
    cl_leave(ogia->gia_cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
    return err;
  }
  if (version > ogia->gia_original_version) ogia->gia_contest_to_save = budget;

  for (i = 0; i < ogia->gia_n; i++) {
    /*  (SUBSTAT1)(SUBSTAT2)...(SUBSTATN)
     */
    err = and_statistics_thaw_subcondition(greq, ogia, version, s_ptr, e,
                                           loglevel, ogia->gia_sc + i);
    if (err != 0) {
      cl_leave(ogia->gia_cl, CL_LEVEL_VERBOSE, "error: %s",
               graphd_strerror(err));
      return err;
    }
  }
  cl_leave(ogia->gia_cl, CL_LEVEL_VERBOSE, "leave");
  return pdb_iterator_util_thaw(ogia->gia_pdb, s_ptr, e, "]");
}

static pdb_iterator *gia_current_producer(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_and *gia = it->it_theory;
  graphd_iterator_and *ogia = ogia(it);

  if (!pdb_iterator_statistics_done(pdb, it)) return NULL;

  /*  Are we returning data from the cache?
   */
  if (gia->gia_cache_offset_valid) {
    /*  Even if we do know a producer - if we're not
     *  advanced to the end of the cache, the cache
     *  producer's position isn't "current" for us -
     *  it's probably a few IDs ahead.
     */
    if (ogia->gia_cache_ps.ps_it == NULL ||
        gia->gia_cache_offset != ogia->gia_cache->gic_n)
      return NULL;

    return ogia->gia_cache_ps.ps_it[ogia->gia_producer];
  }

  /*  We're not using the cache; if we do have a current
   *  producer, it's our own.
   */
  else if (gia->gia_ps.ps_it != NULL)
    return gia->gia_ps.ps_it[ogia->gia_producer];
  return NULL;
}

/* [~]and:LOW[-HIGH]:N:(sub)(sub)...(sub):...
 *	 	[stat-in-progress:STATISTICS-STATE]
 *	     or	[stat-done:CHECK:NEXT[+FIND]:N:
 *			:PRODUCER
 *			:CACHE-OFFSET:RESUME-ID:CACHE-STATE]
 *		:CALL-STATE:PROCESS-STATE
 */
int graphd_iterator_and_freeze(pdb_handle *pdb, pdb_iterator *it,
                               unsigned int flags, cm_buffer *buf) {
  graphd_iterator_and *gia = it->it_theory;
  cl_handle *cl = gia->gia_cl;
  graphd_iterator_and *ogia;
  size_t b0;
  size_t i;
  int err;
  char ibuf[200];
  char const *sep = "";
  pdb_iterator *p_it;

  b0 = cm_buffer_length(buf);
  if (graphd_request_timer_check(gia->gia_greq)) return GRAPHD_ERR_TOO_HARD;

  cl_enter(
      cl, CL_LEVEL_VERBOSE,
      "(%p, %s, flags=%d, id=%llx, resume_id=%s, call_state %d)", (void *)it,
      pdb_iterator_statistics_done(pdb, it) ? "+stat" : "contest-in-progress",
      flags, (unsigned long long)gia->gia_id,
      pdb_id_to_string(pdb, gia->gia_resume_id, ibuf, sizeof ibuf),
      it->it_call_state);

  /*  If we're structurally different from our original,
   *  freeze the original.  (We can't have been positioned
   *  or we would have evolved to match the original - so
   *  we're not losing anything.)
   */
  if (it->it_original->it_id != it->it_id) {
    char ibuf[200];

    err = pdb_iterator_freeze(pdb, it->it_original, flags, buf);
    cl_leave(
        cl, CL_LEVEL_VERBOSE, "-> original (%s)",
        err ? graphd_strerror(err)
            : pdb_iterator_to_string(pdb, it->it_original, ibuf, sizeof ibuf));
    return err;
  }

  ogia = ogia(it);

  if (flags & PDB_ITERATOR_FREEZE_SET) {
    err = cm_buffer_sprintf(
        buf, "and:%c%llu",
        graphd_iterator_direction_to_char(ogia->gia_direction), it->it_low);
    if (err != 0) goto buffer_error;

    if (it->it_high != PDB_ITERATOR_HIGH_ANY) {
      err = cm_buffer_sprintf(buf, "-%llu", it->it_high);
      if (err != 0) goto buffer_error;
    }

    if ((err = cm_buffer_sprintf(buf, ":%zu:", ogia->gia_n)) != 0)
      goto buffer_error;

    if ((err = pdb_iterator_freeze_ordering(pdb, buf, it)) != 0)
      goto buffer_error;

    if ((err = pdb_iterator_freeze_account(pdb, buf, it)) != 0)
      goto buffer_error;

    if (ogia->gia_context_pagesize_valid) {
      err = cm_buffer_sprintf(buf, "[psz:%llu]",
                              (unsigned long long)ogia->gia_context_pagesize);
      if (err != 0) goto buffer_error;
    }
    if (ogia->gia_context_setsize_valid) {
      err = cm_buffer_sprintf(buf, "[ssz:%llu]",
                              (unsigned long long)ogia->gia_context_setsize);
      if (err != 0) goto buffer_error;
    }
    err = cm_buffer_sprintf(buf, "[ov:%llu]",
                            (unsigned long long)ogia->gia_original_version);
    if (err != 0) goto buffer_error;

    /*  This is incremented every time it gets frozen,
     *  so we know which version of an original is the newest.
     */
    ogia->gia_original_version++;

    /*     	(subiterator1)(subiterator2)...(subiteratorN)
     */
    for (i = 0; i < ogia->gia_n; i++) {
      /*  Regardless of what our flags are, we're
       *  only freezing the "set" component of the
       *  subiterators here.  Their positions and states
       *  will be part of the AND iterator's state.
       */
      if ((err = cm_buffer_add_string(buf, "(")) != 0) goto buffer_error;

      if ((err = pdb_iterator_freeze(pdb, ogia->gia_sc[i].sc_it,
                                     PDB_ITERATOR_FREEZE_SET, buf)) != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_freeze", err, "it=%s",
                     pdb_iterator_to_string(pdb, ogia->gia_sc[i].sc_it, ibuf,
                                            sizeof ibuf));
        cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
        return err;
      }
      if ((err = cm_buffer_add_string(buf, ")")) != 0) goto buffer_error;
    }

    /*  Producer hint.
     */
    if (pdb_iterator_statistics_done(pdb, it)) {
      err = cm_buffer_sprintf(buf, "[pro:%zu]", ogia->gia_producer);
      if (err != 0) goto buffer_error;
    }
    sep = "/";
  }

  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    err = cm_buffer_add_string(buf, sep);
    if (err != 0) goto buffer_error;

    err = graphd_iterator_util_freeze_position(
        pdb, gia->gia_ps.ps_eof, gia->gia_id, gia->gia_resume_id, buf);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_util_freeze_position",
                   err, "unexpected error");
      cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
      return err;
    }

    /*  If there is a producer that is at the current
     *  spot, and it has a position, save that, too.
     *  If we lose the state, knowing our producer's
     *  position alone is likely to drastically speed
     *  up recovery.
     */
    if (pdb_iterator_statistics_done(pdb, it) &&
        (p_it = gia_current_producer(pdb, it)) != NULL &&
        pdb_iterator_has_position(pdb, p_it)) {
      err = cm_buffer_add_string(buf, "[pp:");
      if (err != 0) goto buffer_error;

      err = pdb_iterator_freeze(pdb, p_it, PDB_ITERATOR_FREEZE_POSITION, buf);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_freeze", err, "it=%s",
                     pdb_iterator_to_string(pdb, p_it, ibuf, sizeof ibuf));
        cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
        return err;
      }
      err = cm_buffer_add_string(buf, "]");
      if (err != 0) goto buffer_error;
    }

    sep = "/";
  }

  if (flags & PDB_ITERATOR_FREEZE_STATE) {
    err = cm_buffer_sprintf(buf, "%s", sep);
    if (err != 0) goto buffer_error;

    /*  The positions and internal states of the subiterators.
     *  Just like in the set definition, it's
     *
     *     	(subiterator1)(subiterator2)...(subiteratorN)
     *
     *  Except now only with runtime position and -state.
     */
    for (i = 0; i < ogia->gia_n; i++) {
      if ((err = cm_buffer_add_string(buf, "(")) != 0) goto buffer_error;

      err = pdb_iterator_freeze(
          pdb, ogia->gia_sc[i].sc_it,
          PDB_ITERATOR_FREEZE_POSITION | PDB_ITERATOR_FREEZE_STATE, buf);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_freeze", err, "it=%s",
                     pdb_iterator_to_string(pdb, ogia->gia_sc[i].sc_it, ibuf,
                                            sizeof ibuf));
        cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
        return err;
      }

      if ((err = cm_buffer_add_string(buf, ")")) != 0) goto buffer_error;
    }

    /*  	:[slow-check:...]
     */
    if (gia->gia_scs != NULL) {
      if ((err = cm_buffer_add_string(buf, ":")) != 0) goto buffer_error;

      if ((err = graphd_iterator_and_check_freeze_slow(gia, buf)) != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_and_check_freeze_slow",
                     err, "can't freeze slow-check cache");
        cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
        return err;
      }
    }

    /*  	:[stat-in-progress:...]
     */
    if (!pdb_iterator_statistics_done(pdb, it)) {
      if ((err = cm_buffer_add_string(buf, ":")) != 0) goto buffer_error;

      err = and_statistics_freeze(ogia, buf);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "and_statistics_freeze", err,
                     "unexpected error");
        cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
        return err;
      }
    } else {
      char b1[200];
      pdb_id offset;

      /*  Statistics results.
       *
       *  :CHECK:NEXT
       */
      err = cm_buffer_sprintf(buf, ":%lld:%lld",
                              (long long)pdb_iterator_check_cost(pdb, it),
                              (long long)pdb_iterator_next_cost(pdb, it));
      if (err != 0) goto buffer_error;

      /*  +FIND
       */
      if (pdb_iterator_sorted(pdb, it))
        err = cm_buffer_sprintf(buf, "+%lld",
                                (long long)pdb_iterator_find_cost(pdb, it));
      if (err != 0) goto buffer_error;

      /*  Cache.  (If we don't have statistics, we definitely
       *  don't have a cache.)
       *
       *   :N:PRODUCER:OFFSET:CACHE-PROCESS-STATE
       */
      if (!gia->gia_cache_offset_valid)
        offset = PDB_ID_NONE;
      else if (!pdb_iterator_has_position(pdb, it))
        offset = 0;
      else {
        /* The offset is the clone's alone ("gia"),
         * but the cache data that the offset points
         * to is shared between all clones and
         * hence resides in the original ("ogia").
         */
        offset = gia->gia_cache_offset;
        cl_assert(cl, offset <= ogia->gia_cache->gic_n);
      }

      err = cm_buffer_sprintf(buf, ":%llu:%zu:%s:", pdb_iterator_n(pdb, it),
                              ogia->gia_producer,
                              pdb_id_to_string(pdb, offset, b1, sizeof b1));
      if (err != 0) goto buffer_error;

      err = and_freeze_original_cache(ogia(it), buf);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "and_freeze_original_cache", err,
                     "unexpected error");
        cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
        return err;
      }
    }

    /*
     *	:CALL-STATE:PROCESS-STATE
     */
    err = cm_buffer_sprintf(buf, ":%d:", it->it_call_state);
    if (err != 0) goto buffer_error;

    err = and_freeze_process_state(pdb, &gia->gia_ps, buf);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "and_freeze_process_state", err,
                   "unexpected error");
      cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
      return err;
    }
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "%.*s", (int)(cm_buffer_length(buf) - b0),
           cm_buffer_memory(buf) + b0);
  return 0;

buffer_error:
  cl_log_errno(cl, CL_LEVEL_FAIL, "cm_buffer_add_string/sprintf", err, "it=%s",
               pdb_iterator_to_string(pdb, it, ibuf, sizeof ibuf));
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));

  return err;
}

typedef struct parsed_and {
  char pa_dirchar;
  unsigned long long pa_low;
  unsigned long long pa_high;
  unsigned long long pa_sc_n;
  char const *pa_ord_s;
  char const *pa_ord_e;
  pdb_iterator_account *pa_acc;

  char const *pa_subset_s;
  char const *pa_subset_e;

  char const *pa_substate_s;
  char const *pa_substate_e;

  int pa_producer_hint;

  bool pa_eof;
  pdb_id pa_last_id;
  pdb_id pa_resume_id;

  char const *pa_producer_position_s;
  char const *pa_producer_position_e;

  unsigned long long pa_setsize;
  unsigned int pa_setsize_valid : 1;
  unsigned long long pa_pagesize;
  unsigned int pa_pagesize_valid : 1;

  unsigned long pa_original_version;
  unsigned int pa_original_version_valid : 1;

} parsed_and;

static int and_thaw_parse(graphd_request *greq, cl_loglevel loglevel,
                          pdb_iterator_text const *pit, pdb_iterator_base *pib,
                          parsed_and *pa) {
  graphd_handle *g = graphd_request_graphd(greq);
  pdb_handle *pdb = g->g_pdb;
  cl_handle *cl = graphd_request_cl(greq);
  char const *s, *state_s, *stmp;
  char const *e, *state_e;
  int err;
  size_t i;

  memset(pa, 0, sizeof(*pa));
  pa->pa_last_id = PDB_ID_NONE;
  pa->pa_resume_id = PDB_ID_NONE;

  s = pit->pit_set_s;
  e = pit->pit_set_e;

  state_s = pit->pit_state_s;
  state_e = pit->pit_state_e;

  err = pdb_iterator_util_thaw(
      pdb, &s, e, "%c%{low[-high]}:%llu:%{orderingbytes}%{account}",
      &pa->pa_dirchar, &pa->pa_low, &pa->pa_high, &pa->pa_sc_n, &pa->pa_ord_s,
      &pa->pa_ord_e, pib, &pa->pa_acc, (pdb_iterator_property *)NULL);
  if (err != 0) return err;

  if (e - s > 5 && memcmp(s, "[psz:", 5) == 0) {
    err = pdb_iterator_util_thaw(pdb, &s, e, "[psz:%llu]", &pa->pa_pagesize);
    if (err == 0) pa->pa_pagesize_valid = true;
  }
  if (e - s > 5 && memcmp(s, "[ssz:", 5) == 0) {
    err = pdb_iterator_util_thaw(pdb, &s, e, "[ssz:%llu]", &pa->pa_setsize);
    if (err == 0) pa->pa_setsize_valid = true;
  }
  if (e - s > 5 && memcmp(s, "[ov:", 4) == 0) {
    err = pdb_iterator_util_thaw(pdb, &s, e, "[ov:%lu]",
                                 &pa->pa_original_version);
    if (err == 0) pa->pa_original_version_valid = true;
  }
  err = pdb_iterator_util_thaw(pdb, &s, e, "%{extensions}",
                               (pdb_iterator_property *)NULL);
  if (err != 0) return err;

  pa->pa_subset_s = s;
  pa->pa_substate_s = state_s;

  for (i = 0; i < pa->pa_sc_n; i++) {
    pdb_iterator_text subpit;

    /*  Make a combined subpit from the set subiterator
     *  and an optional fragment of the state.
     */
    err = pdb_iterator_util_thaw(pdb, &s, e, "%{(bytes)}", &subpit.pit_set_s,
                                 &subpit.pit_set_e);
    if (err) {
      cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err, "%.*s",
                   (int)(e - s), s);
      return err;
    }
    if (state_s != NULL && state_s < state_e) {
      /*  (SUBPOS/SUBSTATE)
       */
      err = pdb_iterator_util_thaw(pdb, &state_s, state_e,
                                   "%{(position/state)}", &subpit);
      if (err != 0) {
        cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err, "%.*s",
                     (int)(e - s), s);
        return err;
      }
    }

    cl_assert(cl, subpit.pit_set_s != NULL);
    cl_assert(cl, subpit.pit_set_e != NULL);
  }

  pa->pa_subset_e = s;
  pa->pa_substate_e = state_e;

  /*  Optional producer hint?
   */
  stmp = s;

  if (s >= e || *s != '[' ||
      pdb_iterator_util_thaw(pdb, &stmp, e, "[pro:%d]",
                             &pa->pa_producer_hint) != 0)

    pa->pa_producer_hint = -1;
  else
    s = stmp;

  /*  Position
   */
  pa->pa_resume_id = PDB_ID_NONE;
  pa->pa_last_id = PDB_ID_NONE;
  if ((s = pit->pit_position_s) != NULL && s < (e = pit->pit_position_e)) {
    err = graphd_iterator_util_thaw_position(
        pdb, &s, e, loglevel, &pa->pa_eof, &pa->pa_last_id, &pa->pa_resume_id);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "graphd_iterator_util_thaw_position", err,
                   "can't thaw position?");
      return err;
    }

    /*  Optional producer position hint?
     */
    if (s < e && *s == '[') {
      char const *s1 = s;

      if (pdb_iterator_util_thaw(pdb, &s1, e, "[pp:%{bytes}]",
                                 &pa->pa_producer_position_s,
                                 &pa->pa_producer_position_e) == 0) {
        s = s1;
      }
    }

    err = pdb_iterator_util_thaw(pdb, &s, e, "%{extensions}%{end}",
                                 (pdb_iterator_property *)NULL);
    if (err != 0) return err;
  }

  if (pa->pa_substate_s != NULL && pa->pa_substate_s == pa->pa_substate_e)
    pa->pa_substate_s = pa->pa_substate_e = NULL;

  if (pa->pa_producer_position_s != NULL &&
      pa->pa_producer_position_s == pa->pa_producer_position_e)
    pa->pa_producer_position_s = pa->pa_producer_position_e = NULL;

  return 0;
}

/**
 * @brief Reconstitute a frozen iterator
 *
 * @param graphd	module handle
 * @param pit		iterator's parsed text
 * @param pib		base
 * @param hint	  	optimizer hint
 * @param it_out	rebuild the iterator here.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_and_thaw_loc(graphd_handle *graphd,
                                 pdb_iterator_text const *pit,
                                 pdb_iterator_base *pib,
                                 graphd_iterator_hint hint,
                                 cl_loglevel loglevel, pdb_iterator **it_out,
                                 char const *file, int line) {
  graphd_request *greq;
  pdb_handle *pdb = graphd->g_pdb;
  cl_handle *cl;
  graphd_iterator_and *ogia, *gia;
  char const *state_s, *state_e;
  unsigned long long upper_bound;
  int err = 0;
  size_t i;
  char const *s, *e;
  char const *ordering = NULL;
  parsed_and pa;
  pdb_iterator *and_orig = NULL;

  if ((upper_bound = pdb_primitive_n(pdb)) == 0)
    return pdb_iterator_null_create(pdb, it_out);

  greq = pdb_iterator_base_lookup(pdb, pib, "graphd.request");
  if (greq == NULL) return GRAPHD_ERR_SYNTAX;
  cl = graphd_request_cl(greq);

  if (graphd_request_timer_check(greq)) return GRAPHD_ERR_TOO_HARD;

  err = and_thaw_parse(greq, loglevel, pit, pib, &pa);
  if (err != 0) goto scan_error;

/*  If we can, reconnect with an existing original.
 */
#ifdef READY_FOR_PRIME_TIME
  if (pit->pit_set_s != NULL && pa.pa_original_version_valid &&
      !(hint & GRAPHD_ITERATOR_HINT_HARD_CLONE) &&
      (and_orig = pdb_iterator_by_name_lookup(pdb, pib, pit->pit_set_s,
                                              pit->pit_set_e))) {
    char buf[200];

    if ((err = pdb_iterator_clone(pdb, and_orig, it_out)) != 0) {
      char buf[200];
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "orig=%s",
                   pdb_iterator_to_string(pdb, and_orig, buf, sizeof buf));
      return err;
    }

    /*  Our original has turned into a different type of
     *  iterator?   That means we must have had no position--
     *  otherwise, we'd have turned into that type ourselves.
     */
    if (!graphd_iterator_and_is_instance(pdb, *it_out, NULL, NULL)) {
      char buf[200];

      /* Evolved into something that isn't an "and"?
       */
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_and_thaw: "
             "evolved into something else: %s",
             pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
      return 0;
    }

    ogia = (*it_out)->it_original->it_theory;
    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_and_thaw: "
           "reconnecting from %lu with %p=%s (version %lu)",
           pa.pa_original_version, (void *)and_orig,
           pdb_iterator_to_string(pdb, and_orig, buf, sizeof buf),
           ogia->gia_original_version);
  }
#endif

  s = pa.pa_subset_s;
  e = pa.pa_subset_e;

  state_s = pa.pa_substate_s;
  state_e = pa.pa_substate_e;

  cl_enter(cl, CL_LEVEL_VERBOSE, "set=\"%.*s\", substate=\"%.*s\"",
           pit->pit_set_s ? (int)(pit->pit_set_e - pit->pit_set_s) : 4,
           pit->pit_set_s ? pit->pit_set_s : "null",

           state_s ? (int)(state_e - state_s) : 4, state_s ? state_s : "null");

  if (and_orig == NULL) {
    if (pa.pa_ord_s != NULL) {
      ordering = graphd_iterator_ordering_internalize(graphd, pib, pa.pa_ord_s,
                                                      pa.pa_ord_e);
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_and_thaw_loc: got "
             "ordering %c%s",
             pa.pa_dirchar, ordering ? ordering : "null");
    }
    err = graphd_iterator_and_create_loc(
        greq, pa.pa_sc_n, pa.pa_low, pa.pa_high,
        graphd_iterator_direction_from_char(pa.pa_dirchar), ordering, it_out,
        file, line);
    if (err != 0) goto err;
  }
  pdb_iterator_account_set(pdb, *it_out, pa.pa_acc);
  ogia = (*it_out)->it_original->it_theory;

  /* Turn off aggressive optimizations - we're thawing.
   */
  if (and_orig == NULL) {
    ogia->gia_thaw = !(hint == GRAPHD_ITERATOR_HINT_FIXED);
    ogia->gia_producer_hint = pa.pa_producer_hint;
  }

  /* (SUBSET)(SUBSET)...
   */
  for (i = 0; i < pa.pa_sc_n; i++) {
    pdb_iterator_text subpit;
    pdb_iterator *sub_it;

    memset(&subpit, 0, sizeof subpit);

    /*  Make a combined subpit from the set subiterator
     *  and an optional fragment of the state.
     */
    err = pdb_iterator_util_thaw(pdb, &s, e, "%{(bytes)}", &subpit.pit_set_s,
                                 &subpit.pit_set_e);
    if (err) {
      cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err, "%.*s",
                   (int)(e - s), s);
      goto scan_error;
    }
    cl_assert(cl, subpit.pit_set_s != NULL);
    cl_assert(cl, subpit.pit_set_e != NULL);

    if (state_s == NULL || state_s >= state_e) {
      subpit.pit_state_s = subpit.pit_state_e = NULL;

      if (pa.pa_producer_position_s != NULL && i == pa.pa_producer_hint) {
        subpit.pit_position_s = pa.pa_producer_position_s;
        subpit.pit_position_e = pa.pa_producer_position_e;
      } else {
        subpit.pit_position_s = subpit.pit_position_e = NULL;
      }
    } else {
      /*  (SUBPOS/SUBSTATE)
       */
      err = pdb_iterator_util_thaw(pdb, &state_s, state_e,
                                   "%{(position/state)}", &subpit);
      if (err != 0) {
        cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err, "%.*s",
                     (int)(e - s), s);
        goto scan_error;
      }
    }

    cl_assert(cl, subpit.pit_set_s != NULL);
    cl_assert(cl, subpit.pit_set_e != NULL);

    if (and_orig == NULL) {
      err = graphd_iterator_thaw(graphd, &subpit, pib, hint, loglevel, &sub_it,
                                 NULL);
      if (err != 0) {
        cl_log_errno(cl, loglevel, "graphd_iterator_thaw", err, "%.*s",
                     (int)(subpit.pit_set_e - subpit.pit_set_s),
                     subpit.pit_set_s);
        goto err;
      }
    }

    if (graphd_iterator_and_is_instance(pdb, *it_out, NULL, NULL) &&
        pdb_iterator_sorted(pdb, sub_it) &&
        !pdb_iterator_forward(pdb, *it_out) !=
            !pdb_iterator_forward(pdb, sub_it)) {
      char buf[200];
      cl_log(cl, loglevel,
             "and iterator: cannot add thawed %s \"%s\" "
             "to %s AND!",
             pdb_iterator_forward(pdb, sub_it) ? "forward" : "backward",
             pdb_iterator_to_string(pdb, sub_it, buf, sizeof buf),
             pdb_iterator_forward(pdb, *it_out) ? "forward" : "backward");
      pdb_iterator_destroy(pdb, &sub_it);
      err = GRAPHD_ERR_LEXICAL;

      goto err;
    }

    err = graphd_iterator_and_add_subcondition(graphd, *it_out, &sub_it);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "graphd_iterator_and_add_subcondition", err,
                   "can't add?");
      goto err;
    }
  }

  err = graphd_iterator_and_create_commit(graphd, *it_out);
  if (err != 0) {
    cl_log_errno(cl, loglevel, "graphd_iterator_and_create_commit", err,
                 "can't commit?");
    goto err;
  }

  if (!graphd_iterator_and_is_instance(pdb, *it_out, NULL, NULL)) {
    char buf[200];

    /* Evolved into something that isn't an "and"?
     */
    cl_leave(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_and_thaw: "
             "evolved into something else: %s",
             pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
    return 0;
  }
  gia = (*it_out)->it_theory;
  ogia = (*it_out)->it_original->it_theory;

  /*  Sizes.
   *
   *  If we have a size, and the original doesn't, override it.
   */
  if (pa.pa_setsize_valid && !ogia->gia_context_setsize_valid) {
    ogia->gia_context_setsize_valid = true;
    ogia->gia_context_setsize = pa.pa_setsize;
  }
  if (pa.pa_pagesize_valid && !ogia->gia_context_pagesize_valid) {
    ogia->gia_context_pagesize_valid = true;
    ogia->gia_context_pagesize = pa.pa_pagesize;
  }

  /* We'll set the cache offset to valid if we
   * reacquire our cache.  For now, it's not valid.
   */
  gia->gia_cache_offset_valid = false;
  gia->gia_cache_offset = 0;

  /*  Position
   */
  gia->gia_resume_id = pa.pa_resume_id;

  if (pa.pa_producer_position_s == NULL && pa.pa_producer_position_e == NULL &&
      pa.pa_substate_s == NULL && pa.pa_substate_e == NULL)
    gia->gia_resume_id = pa.pa_last_id;

  /*  State
   */
  if (state_s != NULL && state_s < state_e) {
    int call_state;

    /*  We're positioned just behind the subiterator states.
     *  (They were used while creating the subiterators.)
     */

    /* First, an optional [slow-check:SLOW_CHECK state.
     * Then, if we're still in statistics:
     * [stat-in-progress:STATISTICS]
     * otherwise: :CHECK:NEXT[+FIND]:N:PRODUCER:OFFSET:CACHE-STATE
     */
    if (state_s < state_e && *state_s == ':') state_s++;

    if (state_s < state_e && *state_s == '[' &&
        strncasecmp(state_s, "[slow-check", sizeof("[slow-check") - 1) == 0) {
      /* [slow-check:...]
       */
      err = graphd_iterator_and_check_thaw_slow(gia, &state_s, state_e, pib,
                                                loglevel);
      if (err != 0) {
        cl_log_errno(cl, loglevel, "graphd_iterator_and_check_thaw_slot", err,
                     "%.*s", (int)(state_e - state_s), state_s);
        gia->gia_resume_id = pa.pa_last_id;
        goto state_err;
      }
    }

    if (state_s < state_e && *state_s == ':') state_s++;

    if (state_s < state_e && *state_s == '[' &&
        strncasecmp(state_s, "[stat-in-progress",
                    sizeof("[stat-in-progress") - 1) == 0) {
      /* [stat-in-progress:...]
       */
      err =
          and_statistics_thaw(greq, ogia, and_orig ? pa.pa_original_version : 1,
                              &state_s, state_e, loglevel);
      if (err != 0) {
        cl_log_errno(cl, loglevel, "and_statistics_thaw", err, "%.*s",
                     (int)(state_e - state_s), state_s);
        gia->gia_resume_id = pa.pa_last_id;
        goto state_err;
      }
    } else {
      pdb_budget check_cost, next_cost, find_cost;
      unsigned long long n;
      size_t producer;
      pdb_id off;
      graphd_subcondition *sc;

      /*   CC:NC+FC:N:PRODUCER:CACHEOFF:
       */

      err = pdb_iterator_util_thaw(
          pdb, &state_s, state_e, "%{budget}:%{next[+find]}:%llu:%zu:%{id}:",
          &check_cost, &next_cost, &find_cost, &n, &producer, &off);
      if (err) {
        cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err, "%.*s",
                     (int)(state_e - state_s), state_s);
        goto err;
      }
      gia->gia_cache_offset_valid = (off != PDB_ID_NONE);
      gia->gia_cache_offset = (off == PDB_ID_NONE ? 0 : off);

      gia->gia_producer = producer;
      sc = gia->gia_sc + producer;
      cl_assert(gia->gia_cl, sc->sc_it != NULL);

      pdb_iterator_check_cost_set(pdb, *it_out, check_cost);
      pdb_iterator_next_cost_set(pdb, *it_out, next_cost);
      pdb_iterator_n_set(pdb, *it_out, n);
      pdb_iterator_find_cost_set(pdb, *it_out, find_cost);
      pdb_iterator_sorted_set(pdb, *it_out,
                              pdb_iterator_sorted(pdb, sc->sc_it));

      /*  If we got an ordering early on in the set parse,
       *  and we have statistics, we can conclude that the
       *  ordering is relevant; otherwise, we wouldn't have
       *  bothered to save it.
       */
      pdb_iterator_ordered_set(pdb, *it_out,
                               pdb_iterator_ordering(pdb, *it_out) != NULL);

      /*  If we're ordered, our producer is ordered; tell it.
       */
      if (pdb_iterator_ordered(pdb, *it_out)) {
        char buf[200];
        cl_log(cl, CL_LEVEL_VERBOSE,
               "graphd_iterator_and_thaw_loc: "
               "producer %s is ordered.",
               pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
        pdb_iterator_ordered_set(pdb, sc->sc_it, true);
      }

      pdb_iterator_statistics_done_set(pdb, *it_out);
      ogia->gia_evolved = true;

      /*   ORIGINALCACHE
       */
      err = and_thaw_original_cache(greq, ogia,
                                    and_orig ? pa.pa_original_version : 1,
                                    &state_s, state_e, loglevel);
      if (err != 0) {
        gia->gia_cache_offset = 0;
        gia->gia_cache_offset_valid = false;

        cl_log_errno(cl, err == GRAPHD_ERR_NO ? CL_LEVEL_FAIL : loglevel,
                     "and_thaw_original_cache", err, "%.*s",
                     (int)(state_e - state_s), state_s);
        gia->gia_resume_id = pa.pa_last_id;
        goto state_err;
      }

      if (gia->gia_cache_offset_valid &&
          gia->gia_cache_offset > ogia->gia_cache->gic_n) {
        cl_log(cl, loglevel,
               "graphd_iterator_and_thaw: "
               "cursor has cache offset %lld; "
               "but only %zu items in the cache!",
               (long long)off, ogia->gia_cache->gic_n);
        err = GRAPHD_ERR_SYNTAX;
        gia->gia_cache_offset_valid = false;
        goto state_err;
      }
    }

    if (state_s < state_e && *state_s == ':') state_s++;

    /* :CALL-STATE:PROCESS-STATE
     */
    err = pdb_iterator_util_thaw(pdb, &state_s, state_e, "%d:", &call_state);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err, "%.*s",
                   (int)(state_e - state_s), state_s);
      goto err;
    }
    (*it_out)->it_call_state = call_state;

    err = and_thaw_process_state(greq, gia->gia_cm, &state_s, state_e, loglevel,
                                 &gia->gia_ps);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "and_thaw_process_state", err, "%.*s",
                   (int)(state_e - state_s), state_s);
      gia->gia_resume_id = pa.pa_last_id;
      goto state_err;
    }

    err = pdb_iterator_util_thaw(pdb, &state_s, state_e, "%{extensions}%{end}",
                                 (pdb_iterator_property *)NULL);
    if (err != 0) goto err;

    /*  Since we have a state, and everything about
     *  decoding the state went off without a hitch,
     *  we need not resume or catch up or anything
     *  like that - we're already how we were.
     */
    gia->gia_resume_id = pa.pa_resume_id;

  state_err:
    if (err == GRAPHD_ERR_NO)
      err = 0;
    else if (err != 0)
      goto err;
  }

  and_promote_producer_ordering_into_copies(pdb, *it_out);
  gia->gia_ps.ps_eof = pa.pa_eof;

  /*  If we don't link to an original yet, store
   *  a link to ourselves.
   */
  if (pit->pit_set_s != NULL && !and_orig) {
    err = pdb_iterator_by_name_link(pdb, pib, *it_out, pit->pit_set_s,
                                    pit->pit_set_e);
    if (err != 0) {
      char buf[200];
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_by_name_link", err,
                   "it=%s (ignored)",
                   pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
    }
  }

  cl_leave(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_and_thaw: got iterator %p, "
           "it_id=%d, %s, id=%lld, resume_id=%lld "
           "eof=%d producer_hint=%d",
           *it_out, (int)(*it_out)->it_id,
           pdb_iterator_statistics_done(pdb, *it_out) ? "+stat"
                                                      : "contest-in-progress",
           (long long)gia->gia_id, (long long)gia->gia_resume_id,
           gia->gia_ps.ps_eof, gia->gia_producer_hint);

  return 0;
err:
  pdb_iterator_destroy(pdb, it_out);
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
  return err;

scan_error:
  cl_log(cl, loglevel,
         "graphd_iterator_and_thaw: error "
         "scanning \"%.*s\"/%.*s/%.*s\"",
         (int)(pit->pit_set_e - pit->pit_set_s), pit->pit_set_s,
         pit->pit_position_s ? (int)(pit->pit_position_e - pit->pit_position_s)
                             : 4,
         pit->pit_position_s ? pit->pit_position_s : "null",
         pit->pit_state_s ? (int)(pit->pit_state_e - pit->pit_state_s) : 4,
         pit->pit_state_s ? pit->pit_state_s : "null");
  return GRAPHD_ERR_LEXICAL;
}
