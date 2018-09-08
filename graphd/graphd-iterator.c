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

#include <ctype.h>
#include <errno.h>
#include <limits.h> /* DBL_MAX */
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

GRAPHD_SABOTAGE_DECL;

#define GRAPHD_ITERATOR_FIXED_FAST_INTERSECT_MAX (1024 * 16)

#define IS_LIT(s, e, lit)          \
  ((e) - (s) == sizeof(lit) - 1 && \
   strncasecmp((s), (lit), sizeof(lit) - 1) == 0)

/*  Freeze and rethaw an iterator, creating a clone
 *  that's independent of the original.
 */
int graphd_iterator_hard_clone(graphd_request *greq, pdb_iterator *it,
                               pdb_iterator **it_out) {
  graphd_handle *graphd = graphd_request_graphd(greq);
  pdb_handle *pdb = graphd->g_pdb;
  cm_handle *cm = greq->greq_req.req_cm;
  cl_handle *cl = graphd_request_cl(greq);
  cm_buffer buf;
  int err;
  char ibuf[200];

  cl_enter(cl, CL_LEVEL_VERBOSE, "it=%s",
           pdb_iterator_to_string(pdb, it, ibuf, sizeof ibuf));

  cm_buffer_initialize(&buf, cm);

  err = pdb_iterator_freeze(pdb, it, PDB_ITERATOR_FREEZE_EVERYTHING, &buf);
  if (err != 0) {
    char b1[200];

    cm_buffer_finish(&buf);
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_freeze", err, "it=%s",
                 pdb_iterator_to_string(pdb, it, b1, sizeof b1));
    cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
    return err;
  }
  err = graphd_iterator_thaw_bytes(greq, cm_buffer_memory(&buf),
                                   cm_buffer_memory_end(&buf), 0,
                                   CL_LEVEL_ERROR, it_out);
  if (err != 0)
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_thaw_bytes", err, "%.*s",
                 (int)(cm_buffer_memory_end(&buf) - cm_buffer_memory(&buf)),
                 cm_buffer_memory(&buf));
  cm_buffer_finish(&buf);

  if (pdb_iterator_ordered_valid(pdb, it) && pdb_iterator_ordered(pdb, it)) {
    pdb_iterator_ordered_set(pdb, *it_out, pdb_iterator_ordered(pdb, it));
    pdb_iterator_ordering_set(pdb, *it_out, pdb_iterator_ordering(pdb, it));
  }

  cl_leave(cl, CL_LEVEL_VERBOSE, "%s",
           err ? graphd_strerror(err)
               : pdb_iterator_to_string(pdb, *it_out, ibuf, sizeof ibuf));
  return err;
}

int graphd_iterator_substitute(graphd_request *greq, pdb_iterator *dest,
                               pdb_iterator *source) {
  graphd_handle *graphd = graphd_request_graphd(greq);
  pdb_handle *pdb = graphd->g_pdb;
  pdb_iterator *dup_source = NULL;
  cl_handle *cl = pdb_log(pdb);
  pdb_iterator saved;
  char b1[200], b2[200];
  pdb_iterator *source_clone = NULL;
  pdb_iterator_account *acc = NULL;
  pdb_iterator_chain *source_chain;
  int err = 0;

  PDB_IS_ITERATOR(cl, source);
  PDB_IS_ITERATOR(cl, dest);

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_substitute: replace %p:%s "
         "(orig=%p, ref=%d-%d) with %s (orig=%p, ref=%d-%d)",
         (void *)dest, pdb_iterator_to_string(pdb, dest, b1, sizeof b1),
         (void *)dest->it_original, (int)dest->it_refcount,
         (int)dest->it_clones,
         pdb_iterator_to_string(pdb, source, b2, sizeof b2),
         (void *)source->it_original, (int)source->it_refcount,
         (int)source->it_clones);

  /*  If the destination has clones, the iterator at
   *  <destination> must always be an original.
   *
   *  If the source isn't an original, we have to assign a
   *  hard clone of the source instead.
   */
  if (dest->it_clones && source->it_original != source) {
    pdb_iterator *source_clone;
    char buf[200];
    int err;

    err = graphd_iterator_hard_clone(greq, source, &source_clone);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_hard_clone", err,
                   "couldn't hard-clone %s",
                   pdb_iterator_to_string(pdb, source, buf, sizeof buf));
      return err;
    }
    cl_assert(cl, source_clone->it_original == source_clone);

    err = graphd_iterator_substitute(greq, dest, source_clone);
    if (err != 0) {
      char b2[200];
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_substitute", err,
                   "couldn't substitute %s with %s",
                   pdb_iterator_to_string(pdb, dest, buf, sizeof buf),
                   pdb_iterator_to_string(pdb, source_clone, b2, sizeof b2));

      pdb_iterator_destroy(pdb, &source_clone);
      return err;
    }
    pdb_iterator_destroy(pdb, &source);
    return 0;
  }

  PDB_IS_ITERATOR(cl, source);
  cl_assert(cl, source->it_refcount >= 1);

  /*  Save the account for reassignment, later.
   */
  if ((acc = pdb_iterator_account(pdb, source)) == NULL)
    acc = pdb_iterator_account(pdb, dest);

  /*  "Finish" the destination.  This may free the source
   *  a few times over, as a side effect.
   */
  saved = *dest;
  /*
  pdb_iterator_null_become(pdb, dest);
  */

  pdb_iterator_by_name_unlink(pdb, dest);

  if (dest->it_original != dest) pdb_iterator_unlink_clone(pdb, dest);

  (dest->it_type->itt_finish)(pdb, dest);

  pdb_iterator_chain_out(pdb, dest);
  pdb_iterator_suspend_chain_out(pdb, dest);

  dest->it_refcount = saved.it_refcount;
  dest->it_clones = saved.it_clones;

  cl_assert(cl, source->it_refcount >= 1);

  /*  If the source has references left to it
   *  (other than the one we came in with),
   *  we can't move it,
   *  and will have to move a clone or duplicate instead.
   *
   *  - A clone if the destination has no non-selfrefs
   *
   *  - A duplicate if it does have non-selfrefs and may be
   *    someone's original.
   */
  cl_assert(cl, source->it_refcount >= 1);

  if (source->it_refcount > 1) {
    if (saved.it_clones > 0) {
      /*  Independently duplicate the source
       */
      cm_buffer buf;
      cm_handle *cm;

      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_substitute: "
             "both source and destination "
             "have source %p:%d-%d, dest %p:%d-%d, "
             "outside references.",
             (void *)source, (int)source->it_refcount, (int)source->it_clones,
             (void *)dest, (int)dest->it_refcount, (int)dest->it_clones);

      /*  Make an independent duplicate of the source,
       *  and move that onto the destination.
       */
      cm = pdb_mem(pdb);
      cm_buffer_initialize(&buf, cm);

      err = pdb_iterator_clone(pdb, source, &source_clone);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "it=%s",
                     pdb_iterator_to_string(pdb, source, b1, sizeof b1));
        cm_buffer_finish(&buf);

        goto null_destination;
      }
      err = pdb_iterator_freeze(pdb, source_clone,
                                PDB_ITERATOR_FREEZE_EVERYTHING, &buf);
      if (err != 0) {
        pdb_iterator_destroy(pdb, &source_clone);
        cm_buffer_finish(&buf);
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_freeze", err, "it=%s",
                     pdb_iterator_to_string(pdb, source, b1, sizeof b1));

        goto null_destination;
      }
      pdb_iterator_destroy(pdb, &source_clone);
      err = graphd_iterator_thaw_bytes(
          greq, cm_buffer_memory(&buf), cm_buffer_memory_end(&buf),
          GRAPHD_ITERATOR_HINT_HARD_CLONE, CL_LEVEL_ERROR, &dup_source);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_iterator_thaw_bytes", err,
                     "%.*s",
                     (int)(cm_buffer_memory_end(&buf) - cm_buffer_memory(&buf)),
                     cm_buffer_memory(&buf));
        cm_buffer_finish(&buf);
        goto null_destination;
      }
      cm_buffer_finish(&buf);

      pdb_iterator_destroy(pdb, &source);
      source = dup_source;

      cl_assert(cl, source->it_original == source);
      cl_assert(cl, source->it_refcount == 1);
    } else {
      err = pdb_iterator_clone(pdb, source, &source_clone);
      if (err != 0) goto null_destination;

      PDB_IS_ITERATOR(cl, source);
      PDB_IS_ITERATOR(cl, source_clone);

      /*  Remove the reference from source that we
       *  added.  (Instead, we now own a reference
       *  on the source_clone.)
       */
      pdb_iterator_destroy(pdb, &source);
      source = source_clone;
    }
  }

  /*   There's only one reference to the source,
   *   and we're holding it.
   */
  cl_assert(cl, source->it_refcount == 1);

  PDB_IS_ITERATOR(cl, source);

  pdb_iterator_suspend_save(pdb, source, &source_chain);
  pdb_iterator_chain_out(pdb, source);

  *dest = *source;
  pdb_iterator_chain_in(pdb, dest);
  pdb_iterator_suspend_restore(pdb, dest, source_chain);

  if (source->it_original == source) dest->it_original = dest;

  /* If the destination had references to it,
   * those apply to the replacement, too.
   */
  dest->it_refcount = saved.it_refcount;
  dest->it_clones = saved.it_clones;

  if (source->it_original == dest) {
    /*  Selflinks are not linkcounted,
     *  but remote links are.  If we just
     *  turned a remote link into a selflink,
     *  we need to decrement our reference
     *  count!
     */
    dest->it_refcount--;
    cl_assert(cl, source->it_refcount >= 1);
  }

  PDB_IS_ITERATOR(cl, dest);

  /* Free the leftover physical hull of source.
   */
  source->it_type = NULL;
  pdb_iterator_destroy(pdb, &source);

  pdb_iterator_account_set(pdb, dest, acc);
  return 0;

null_destination:

  /*  The destination can get damaged by unexpected failures in
   *  this call - we've free'ed its state prior to overwriting
   *  it with source, in order to free any links it may hold on
   *  the source and hopefully make it movable.
   *
   *  Let's just turn it into a null iterator, to give it
   *  *something* to be!
   */
  pdb_iterator_null_reinitialize(pdb, dest);

  dest->it_refcount = saved.it_refcount;
  dest->it_clones = saved.it_clones;

  return err;
}

int graphd_iterator_thaw_bytes_loc(graphd_request *greq, char const *s,
                                   char const *e, graphd_iterator_hint hints,
                                   cl_loglevel loglevel, pdb_iterator **it_out,
                                   char const *file, int line) {
  int err;
  pdb_iterator_text pit;

  pdb_iterator_parse(s, e, &pit);
  err = graphd_iterator_thaw_loc(graphd_request_graphd(greq), &pit,
                                 &greq->greq_pib, hints, loglevel, it_out, NULL,
                                 file, line);
  return err;
}

int graphd_iterator_thaw_loc(graphd_handle *g, pdb_iterator_text const *pit,
                             pdb_iterator_base *pib, graphd_iterator_hint hints,
                             cl_loglevel loglevel, pdb_iterator **it_out,
                             graphd_iterator_hint *hints_out, char const *file,
                             int line) {
  char const *col;
  char buf[200];
  int err;
  pdb_handle *pdb = g->g_pdb;
  cl_handle *cl = pdb_log(pdb);
  char const *s = pit->pit_set_s;
  char const *e = pit->pit_set_e;
  pdb_iterator_text pit_mod;
  pdb_iterator_text const *p2 = &pit_mod;

  cl_assert(cl, s != NULL && e != NULL);

  if (hints_out != NULL) *hints_out = 0;

  /* Oof. */
  cl_enter(
      cl, CL_LEVEL_VERBOSE, "\"%.*s%s\" \"%.*s%s\" \"%.*s%s\" [%zu] [%s:%d]",

      s ? (e - s > 100 ? 100 : (int)(e - s)) : 4, s ? s : "null",
      s && e - s > 100 ? "..." : "",

      pit->pit_position_s
          ? (pit->pit_position_e - pit->pit_position_s > 100
                 ? 100
                 : (int)(pit->pit_position_e - pit->pit_position_s))
          : 4,
      pit->pit_position_s ? pit->pit_position_s : "null",
      (pit->pit_position_s && pit->pit_position_e - pit->pit_position_s > 100)
          ? "..."
          : "",

      pit->pit_state_s ? (pit->pit_state_e - pit->pit_state_s > 100
                              ? 100
                              : (int)(pit->pit_state_e - pit->pit_state_s))
                       : 4,
      pit->pit_state_s ? pit->pit_state_s : "null",
      pit->pit_state_s && pit->pit_state_e - pit->pit_state_s > 100 ? "..."
                                                                    : "",

      s ? (size_t)(e - s) : (size_t)0, file, line);

  if ((col = memchr(s, ':', (size_t)(e - s))) == NULL || col - s < 1 ||
      !isascii(*s)) {
    cl_handle *cl = pdb_log(g->g_pdb);
    cl_log(cl, loglevel,
           "graphd_iterator_thaw [from %s:%d]: "
           "expected \"prefix:\", got "
           "\"%.*s\"",
           file, line, (int)(e - s), s);
    cl_leave(cl, CL_LEVEL_VERBOSE, "no prefix");
    return GRAPHD_ERR_LEXICAL;
  }

  pit_mod = *pit;
  pit_mod.pit_set_s = col + 1;

  /*  If there is a local state, retrieve it.
   */
  if (pit_mod.pit_state_s != NULL &&
      pit_mod.pit_state_s < pit_mod.pit_state_e) {
    err = graphd_iterator_state_restore(g, &pit_mod.pit_state_s,
                                        &pit_mod.pit_state_e);
    if (err != 0) {
      /*  We had a coat check ticket for our
       *  cursor, but couldn't retrieve it.
       *  (Maybe it aged out of the cache.)
       *
       *  Oh well.  Just reconstitute our place
       *  from set and position.
       */
      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_iterator_thaw [from %s:%d]: "
             "MISS %.*s (continuing without the local state)",
             file, line, (int)(pit->pit_state_e - pit->pit_state_s),
             pit->pit_state_s);
      pit_mod.pit_state_s = pit_mod.pit_state_e = NULL;
    }
  }

  err = GRAPHD_ERR_LEXICAL;
  switch (tolower(*s)) {
    case 'a':
      if (IS_LIT(s, col, "and")) {
        err = graphd_iterator_and_thaw_loc(g, p2, pib, hints, loglevel, it_out,
                                           file, line);
        goto done;
      }
      break;

    case 'f':
      if (IS_LIT(s, col, "fixed-and")) {
        err = graphd_iterator_and_thaw_loc(g, p2, pib,
                                           hints | GRAPHD_ITERATOR_HINT_FIXED,
                                           loglevel, it_out, file, line);
        if (hints_out != NULL) *hints_out |= GRAPHD_ITERATOR_HINT_FIXED;
        goto done;
      } else if (IS_LIT(s, col, "fixed-isa")) {
        err = graphd_iterator_isa_thaw_loc(g, p2, pib,
                                           hints | GRAPHD_ITERATOR_HINT_FIXED,
                                           loglevel, it_out, file, line);
        if (hints_out != NULL) *hints_out |= GRAPHD_ITERATOR_HINT_FIXED;
        goto done;
      } else if (IS_LIT(s, col, "fixed-linksto")) {
        err = graphd_iterator_linksto_thaw_loc(
            g, p2, pib, hints | GRAPHD_ITERATOR_HINT_FIXED, loglevel, it_out,
            file, line);
        if (hints_out != NULL) *hints_out |= GRAPHD_ITERATOR_HINT_FIXED;
        goto done;
      } else if (IS_LIT(s, col, "fixed")) {
        err = graphd_iterator_fixed_thaw_loc(g, p2, pib, hints, loglevel,
                                             it_out, file, line);
        goto done;
      }
      break;

    case 'i':
      if (IS_LIT(s, col, "isa")) {
        err = graphd_iterator_isa_thaw_loc(g, p2, pib, false, loglevel, it_out,
                                           file, line);
        goto done;
      } else if (IS_LIT(s, col, "islink")) {
        err = graphd_iterator_islink_thaw_loc(g, p2, pib, loglevel, it_out,
                                              file, line);
        goto done;
      }
      break;

    case 'l':
      if (IS_LIT(s, col, "linksto")) {
        err = graphd_iterator_linksto_thaw_loc(g, p2, pib, 0, loglevel, it_out,
                                               file, line);
        goto done;
      }
      break;

    case 'o':
      if (IS_LIT(s, col, "or-linksto")) {
        err = graphd_iterator_linksto_thaw_loc(
            g, p2, pib, GRAPHD_ITERATOR_HINT_OR, loglevel, it_out, file, line);
        goto done;
      } else if (IS_LIT(s, col, "or")) {
        err = graphd_iterator_or_thaw_loc(g, p2, pib, loglevel, it_out, file,
                                          line);
        goto done;
      }
      break;

    case 'p':
      if (IS_LIT(s, col, "prefix")) {
        err = graphd_iterator_prefix_thaw(g, p2, pib, loglevel, it_out);
        goto done;
      }
      break;

    case 's':
      if (IS_LIT(s, col, "sort")) {
        err = graphd_iterator_sort_thaw_loc(g, p2, pib, loglevel, it_out, file,
                                            line);
        goto done;
      }
      break;

    case 'v':
      if (IS_LIT(s, col, "vip")) {
        err = graphd_iterator_vip_thaw(g, p2, pib, loglevel, it_out);
        goto done;
      }
      if (IS_LIT(s, col, "vrange")) {
        err = graphd_iterator_vrange_thaw(g, p2, pib, loglevel, it_out);
        goto done;
      }
      break;

    case 'w':
      if (IS_LIT(s, col, "without")) {
        err = graphd_iterator_without_thaw(g, p2, pib, loglevel, loglevel,
                                           it_out);
        goto done;
      }
  }

  /* We didn't understand the prefix.
   * Pass the original, unmodified pit to pdb for default processing.
   */
  err = pdb_iterator_thaw(g->g_pdb, pit, pib, it_out);
done:
  cl_assert(cl, err || *it_out != NULL);
  if (err != 0)
    cl_log(cl, loglevel,
           "graphd_iterator_thaw_loc %.*s / %.*s / %.*s [from %s:%d]: %s",
           (int)(pit->pit_set_s ? pit->pit_set_e - pit->pit_set_s : 4),
           pit->pit_set_s ? pit->pit_set_s : "null",
           (int)(pit->pit_position_s ? pit->pit_position_e - pit->pit_position_s
                                     : 4),
           pit->pit_position_s ? pit->pit_position_s : "null",
           (int)(pit->pit_state_s ? pit->pit_state_e - pit->pit_state_s : 4),
           pit->pit_state_s ? pit->pit_state_s : "null", file, line,
           graphd_strerror(err));

  cl_leave(cl, CL_LEVEL_VERBOSE, "%s",
           err ? graphd_strerror(err)
               : pdb_iterator_to_string(g->g_pdb, *it_out, buf, sizeof buf));
  return err;
}

/**
 * @brief Scan statistics as part of a thaw.
 *
 *  The input pointer *s_ptr is left on the ':' following the
 *  statistics, if any.
 *
 *	Absent statistics --  ::
 *	Present statistics -- :PRODUCTIONCOST:CHECKCOST:TRAVERSALCOST:N:
 *
 * @param cl 		log errors through this (at level CL_LEVEL_FAIL).
 * @param who		error messages are attributed to this.
 * @param s_ptr		incoming text pointer, initially positioned at
 *				beginning of statistics section; on success,
 *				positioned on the first unused character
 *				(typically, a closing :).
 * @param e		end of all available input
 * @param upper_limit	number of primitives in the system, used
 *				to decode check_chance.
 * @param have_statistics_out 	assigned true or false, depending on
 *				whether statistics were decoded
 * @param check_cost_out	decoded check-cost
 * @param next_cost_out		decoded next_cost
 * @param find_cost_out		decoded traversal cost
 * @param n_out			decoded estimated n.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_thaw_statistics(
    cl_handle *cl, char const *who, char const **s_ptr, char const *e,
    unsigned long long upper_limit, cl_loglevel loglevel,
    bool *have_statistics_out, pdb_budget *check_cost_out,
    pdb_budget *next_cost_out, pdb_budget *find_cost_out,
    unsigned long long *n_out) {
  char const *s = *s_ptr;
  unsigned long long ull;
  int err;

  if (s >= e || *s == ':') {
    *have_statistics_out = false;
    return 0;
  }
  cl_assert(cl, upper_limit > 0);

  *have_statistics_out = true;

  /*  check_cost
   */
  if ((err = pdb_scan_ull(&s, e, &ull)) != 0) {
    cl_log(cl, loglevel,
           "%s: expected "
           "\"check-cost:\", got \"%.*s\": %s",
           who, (int)(e - s), s, graphd_strerror(err));
    return err;
  }
  *check_cost_out = (pdb_budget)ull;
  if (s >= e || *s != ':') {
    cl_log(cl, loglevel,
           "%s: expected "
           "\":production-cost:\", got \"%.*s\"",
           who, (int)(e - s), s);
    return GRAPHD_ERR_LEXICAL;
  }
  s++;

  /*  next_cost
   */
  if ((err = pdb_scan_ull(&s, e, &ull)) != 0) {
    cl_log(cl, loglevel,
           "%s: expected "
           "\"production-cost:\", got \"%.*s\": %s",
           who, (int)(e - s), s, graphd_strerror(err));
    return err;
  }
  *next_cost_out = (pdb_budget)ull;
  if (s >= e || (*s != ':' && *s != '+')) {
    cl_log(cl, loglevel,
           "%s: expected "
           "\"[+traversal-cost]:estimate-n\", "
           "got \"%.*s\"",
           who, (int)(e - s), s);
    return GRAPHD_ERR_LEXICAL;
  }
  s++;

  /*  find_cost
   */
  if (s[-1] == '+') {
    if ((err = pdb_scan_ull(&s, e, &ull)) != 0) {
      cl_log(cl, loglevel,
             "%s: expected "
             "\"traversal-cost:\", got \"%.*s\": %s",
             who, (int)(e - s), s, graphd_strerror(err));
      return err;
    }
    *find_cost_out = (pdb_budget)ull;
    if (s >= e || *s != ':') {
      cl_log(cl, loglevel,
             "%s: expected "
             "\":estimate-n:\", got \"%.*s\"",
             who, (int)(e - s), s);
      return GRAPHD_ERR_LEXICAL;
    }
    s++;
  } else
    *find_cost_out = 0;

  /*  n
   */
  if ((err = pdb_scan_ull(&s, e, n_out)) != 0) {
    cl_log(cl, loglevel,
           "%s: expected "
           "\"estimated-n:\", got \"%.*s\": %s",
           who, (int)(e - s), s, graphd_strerror(err));
    return err;
  }
  if (s < e && *s != ':') {
    cl_log(cl, loglevel,
           "%s: expected "
           "\":\" after estimated-n, got \"%.*s\"",
           who, (int)(e - s), s);
    return GRAPHD_ERR_LEXICAL;
  }
  *s_ptr = s;

  return 0;
}

/**
 * @brief Intersect two iterators, yielding a number of entries
 *  	below a predictable maximum.
 *
 * @param pdb		database handle
 * @param a		first iterator to intersect
 * @param b		second iterator to intersect
 * @param id_out	out: the results
 * @param id_n		out: number of occupied slots
 * @param id_m		in: number of slots in *id_out;
 *
 * @return 0 on success
 * @return GRAPHD_ERR_MORE if one or both iterators didn't
 * 	lend themselves to fast intersects
 * @return other nonzero errors on system error.
 */
int graphd_iterator_intersect(graphd_handle *g, pdb_iterator *a,
                              pdb_iterator *b, unsigned long long low,
                              unsigned long long high, bool forward,
                              bool error_if_null, pdb_budget *budget_inout,
                              pdb_iterator **it_out) {
  pdb_budget budget_in = *budget_inout;
  pdb_handle *pdb = g->g_pdb;
  cl_handle *cl = pdb_log(pdb);
  char b1[200], b2[200];

  pdb_id *a_fixed;
  size_t a_n;

  pdb_id *b_fixed;
  size_t b_n;

  pdb_id *id_s;
  size_t id_n, id_m;

  int err;
  unsigned long long max_n;
  pdb_primitive_summary a_psum, b_psum;
  pdb_iterator_account *acc = NULL;

  cl_enter(cl, CL_LEVEL_VERBOSE, "%s %s ($%lld)",
           pdb_iterator_to_string(pdb, a, b1, sizeof b1),
           pdb_iterator_to_string(pdb, b, b2, sizeof b2),
           (long long)*budget_inout);

  if ((acc = pdb_iterator_account(pdb, a)) == NULL)
    acc = pdb_iterator_account(pdb, b);

  if (GRAPHD_SABOTAGE(g, *budget_inout <= 0)) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "not enough budget");
    return GRAPHD_ERR_MORE;
  }

  *it_out = NULL;
  *budget_inout -= PDB_COST_FUNCTION_CALL;

  if (pdb_iterator_null_is_instance(pdb, a) ||
      pdb_iterator_null_is_instance(pdb, b)) {
  null:
    if (error_if_null) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "no (empty result) ($%lld)",
               budget_in - *budget_inout);
      return GRAPHD_ERR_NO;
    }
    err = pdb_iterator_null_create(pdb, it_out);
    goto done;
  }

  if (high > a->it_high) high = a->it_high;
  if (high > b->it_high) high = b->it_high;
  if (low < a->it_low) low = a->it_low;
  if (low < b->it_low) low = b->it_low;
  if (low >= high) goto null;

  max_n = pdb_iterator_n_valid(pdb, a) ? pdb_iterator_n(pdb, a)
                                       : PDB_ITERATOR_HIGH_ANY;

  if (pdb_iterator_n_valid(pdb, b) && max_n > pdb_iterator_n(pdb, b))
    max_n = pdb_iterator_n(pdb, b);

  if (max_n > high - low) max_n = high - low;

  /*  Create a fixed iterator of the desired size.
   */
  if (max_n > GRAPHD_ITERATOR_FIXED_FAST_INTERSECT_MAX) goto try_vip;

  id_m = max_n;
  err = graphd_iterator_fixed_create(g, id_m, low, high, forward, it_out);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_fixed_create", err,
                 "max_n %llu", max_n);
    cl_leave(cl, CL_LEVEL_VERBOSE, "error in fixed create");
    return err;
  }
  graphd_iterator_fixed_is_instance(pdb, *it_out, &id_s, &id_n);
  id_n = 0;

  if (graphd_iterator_fixed_is_instance(pdb, a, &a_fixed, &a_n) ||
      graphd_iterator_vip_is_fixed_instance(pdb, a, &a_fixed, &a_n)) {
    while (a_n > 0 && a_fixed[0] < low) a_fixed++, a_n--;

    while (a_n > 0 && a_fixed[a_n - 1] >= high) a_n--;

    if (a_n == 0) {
      pdb_iterator_destroy(pdb, it_out);
      goto null;
    }

    if (graphd_iterator_fixed_is_instance(pdb, b, &b_fixed, &b_n) ||
        graphd_iterator_vip_is_fixed_instance(pdb, b, &b_fixed, &b_n)) {
      err = graphd_iterator_fixed_intersect(cl, a_fixed, a_n, b_fixed, b_n,
                                            id_s, &id_n, max_n);
      if (err == 0)
        graphd_iterator_fixed_create_commit_n(*it_out, id_n, true);
      else
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_fixed_intersect", err,
                     "a=%s b=%s", pdb_iterator_to_string(pdb, a, b1, sizeof b1),
                     pdb_iterator_to_string(pdb, b, b2, sizeof b2));
      goto done;
    }
    err = pdb_iterator_fixed_intersect(pdb, b, a_fixed, a_n, id_s, &id_n, id_m);
    if (err != GRAPHD_ERR_MORE) {
      if (err == 0)
        graphd_iterator_fixed_create_commit_n(*it_out, id_n, true);
      else if (err == GRAPHD_ERR_NO)
        goto done;
      else
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_fixed_intersect", err,
                     "a=%s b=%s", pdb_iterator_to_string(pdb, a, b1, sizeof b1),
                     pdb_iterator_to_string(pdb, b, b2, sizeof b2));
      goto done;
    }
  }
  if (graphd_iterator_fixed_is_instance(pdb, b, &b_fixed, &b_n) ||
      graphd_iterator_vip_is_fixed_instance(pdb, b, &b_fixed, &b_n)) {
    while (b_n > 0 && b_fixed[0] < low) b_fixed++, b_n--;
    while (b_n > 0 && b_fixed[b_n - 1] >= high) b_n--;
    if (b_n == 0) {
      pdb_iterator_destroy(pdb, it_out);
      goto null;
    }

    err = pdb_iterator_fixed_intersect(pdb, a, b_fixed, b_n, id_s, &id_n, id_m);
    if (err != GRAPHD_ERR_MORE) {
      if (err == 0)
        graphd_iterator_fixed_create_commit_n(*it_out, id_n, true);
      else
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_fixed_intersect", err,
                     "a=%s b=%s", pdb_iterator_to_string(pdb, a, b1, sizeof b1),
                     pdb_iterator_to_string(pdb, b, b2, sizeof b2));
      goto done;
    }
  }

  /* Fast intersect of two idarrays.
   */
  err = pdb_iterator_intersect(pdb, a, b, low, high, budget_inout, id_s, &id_n,
                               max_n);

  if (err != GRAPHD_ERR_MORE) {
    if (err == 0) {
      if (id_n == 0 && error_if_null) {
        pdb_iterator_destroy(pdb, it_out);
        goto null;
      }
      graphd_iterator_fixed_create_commit_n(*it_out, id_n, /* sorted? */ true);
    } else if (err == GRAPHD_ERR_NO)
      /* PDB says it can't do it. */
      goto done;
    else

      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_fixed_intersect", err,
                   "a=%s b=%s", pdb_iterator_to_string(pdb, a, b1, sizeof b1),
                   pdb_iterator_to_string(pdb, b, b2, sizeof b2));
    goto done;
  }

  /* We just didn't get anywhere with that.
   */
  pdb_iterator_destroy(pdb, it_out);

try_vip:
  if (pdb_iterator_primitive_summary(pdb, a, &a_psum) == 0 &&
      pdb_iterator_primitive_summary(pdb, b, &b_psum) == 0 &&
      b_psum.psum_result == PDB_LINKAGE_N &&
      a_psum.psum_result == PDB_LINKAGE_N) {
    /*  A is a VIP candidate.  B is a type.  That's all they are.
     */
    if (a_psum.psum_complete && b_psum.psum_complete &&
        (a_psum.psum_locked == (1 << PDB_LINKAGE_RIGHT) ||
         a_psum.psum_locked == (1 << PDB_LINKAGE_LEFT)) &&
        b_psum.psum_locked == (1 << PDB_LINKAGE_TYPEGUID) &&
        pdb_iterator_n_valid(pdb, a) && pdb_iterator_n(pdb, a) >= PDB_VIP_MIN) {
      int linkage =
          (a_psum.psum_locked == (1 << PDB_LINKAGE_RIGHT) ? PDB_LINKAGE_RIGHT
                                                          : PDB_LINKAGE_LEFT);

      err = pdb_vip_iterator(pdb, a_psum.psum_guid + linkage, linkage,
                             b_psum.psum_guid + PDB_LINKAGE_TYPEGUID, low, high,
                             forward, false, it_out);
      goto done;
    }

    /*  And vice versa.
     */
    if (b_psum.psum_complete && a_psum.psum_complete &&
        (b_psum.psum_locked == (1 << PDB_LINKAGE_RIGHT) ||
         b_psum.psum_locked == (1 << PDB_LINKAGE_LEFT)) &&
        a_psum.psum_locked == (1 << PDB_LINKAGE_TYPEGUID) &&
        pdb_iterator_n_valid(pdb, b) && pdb_iterator_n(pdb, b) >= PDB_VIP_MIN) {
      int linkage =
          (b_psum.psum_locked == (1 << PDB_LINKAGE_RIGHT) ? PDB_LINKAGE_RIGHT
                                                          : PDB_LINKAGE_LEFT);

      err = pdb_vip_iterator(pdb, b_psum.psum_guid + linkage, linkage,
                             a_psum.psum_guid + PDB_LINKAGE_TYPEGUID, low, high,
                             forward, false, it_out);
      goto done;
    }
  }

  cl_log(cl, CL_LEVEL_DEBUG,
         "graphd_iterator_intersect: "
         "not easy enough (a=%s b=%s)",
         pdb_iterator_to_string(pdb, a, b1, sizeof b1),
         pdb_iterator_to_string(pdb, b, b2, sizeof b2));
  cl_leave(cl, CL_LEVEL_VERBOSE, "($%lld)",
           (long long)(budget_in - *budget_inout));
  return GRAPHD_ERR_MORE;

done:
  if (err == 0) {
    char buf[200];
    pdb_iterator_account_set(pdb, *it_out, acc);
    cl_leave(cl, CL_LEVEL_VERBOSE, "%s ($%lld)",
             pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf),
             (long long)(budget_in - *budget_inout));
    return err;
  } else {
    char b1[200], b2[200];
    pdb_iterator_destroy(pdb, it_out);
    cl_leave(cl, CL_LEVEL_VERBOSE, "%s x %s: %s",
             pdb_iterator_to_string(pdb, a, b1, sizeof b1),
             pdb_iterator_to_string(pdb, b, b2, sizeof b2),
             graphd_strerror(err));
  }
  return err;
}

/**
 * @brief Guess the number of shared values among two tractable iterators
 *
 * @param pdb		database handle
 * @param a		first iterator to intersect
 * @param b		second iterator to intersect
 * @param id_n		out: number of shared IDs.
 * @param budget_inout	in/out: budget
 *
 * @return 0 on success
 * @return GRAPHD_ERR_MORE if one or both iterators didn't
 * 	lend themselves to fast intersects
 * @return other nonzero errors on system error.
 */
int graphd_iterator_quick_intersect_estimate(graphd_handle *g, pdb_iterator *a,
                                             pdb_iterator *b,
                                             pdb_budget *budget_inout,
                                             unsigned long long *n_out) {
  cl_handle *cl = g->g_cl;
  pdb_handle *pdb = g->g_pdb;
  unsigned long long a_n;
  unsigned long long b_n;
  pdb_id id;
  size_t err;

  cl_assert(cl, pdb_iterator_sorted(pdb, a));
  cl_assert(cl, pdb_iterator_sorted_valid(pdb, a));
  cl_assert(cl, pdb_iterator_n_valid(pdb, a));

  cl_assert(cl, pdb_iterator_sorted(pdb, b));
  cl_assert(cl, pdb_iterator_sorted_valid(pdb, b));
  cl_assert(cl, pdb_iterator_n_valid(pdb, b));

  cl_assert(cl, pdb_iterator_forward(pdb, b) == pdb_iterator_forward(pdb, a));

  if ((err = pdb_iterator_reset(pdb, a)) != 0 ||
      (err = pdb_iterator_reset(pdb, b)) != 0)
    return err;

  a_n = pdb_iterator_n(pdb, a);
  b_n = pdb_iterator_n(pdb, b);

  if (a_n > b_n) {
    pdb_iterator *tmp;

    /* Swap a and b.
     */
    tmp = a;
    a = b;
    b = tmp;

    a_n = b_n;
    b_n = pdb_iterator_n(pdb, b);
  }
  *n_out = 0;

  /* B is much larger than A?
   */
  if (b_n > a_n && b_n > a_n * a_n) {
    unsigned long long n_next = 0;

    /* next a, check against b.
     */
    do {
      err = pdb_iterator_next(pdb, a, &id, budget_inout);
      if (err != 0) {
        if (err == PDB_ERR_NO) return 0;
        if (err == PDB_ERR_MORE) break;
        return err;
      }
      n_next++;

      err = pdb_iterator_check(pdb, b, id, budget_inout);
      if (err == 0)
        ++*n_out;
      else if (err == PDB_ERR_MORE)
        break;
      else if (err != PDB_ERR_NO)
        return err;

    } while (*budget_inout >= 0);

    /*  If we arrive here, we ran out of budget.
     *  Extrapolate.
     *
     *       X : *n_out = a_n : n_next.
     */
    if (n_next == 0)
      *n_out = a_n;
    else
      *n_out = (a_n * *n_out) / n_next;
  } else {
    pdb_id last_id = PDB_ID_NONE;
    unsigned long long travelled, range;

  /*  a.next (b.find/a.find)*.
   */
  do_next:
    err = pdb_iterator_next(pdb, a, &id, budget_inout);
    if (err != 0) {
      if (err == PDB_ERR_NO) return 0;
      if (err == PDB_ERR_MORE) goto done;
      return err;
    }

    for (;;) {
      last_id = id;
      err = pdb_iterator_find(pdb, b, last_id, &id, budget_inout);
      if (err == 0) {
        if (id == last_id) {
          ++*n_out;
          goto do_next;
        }
      } else if (err == PDB_ERR_NO)
        return 0;
      else if (err == PDB_ERR_MORE)
        break;
      else
        return err;

      last_id = id;
      err = pdb_iterator_find(pdb, a, last_id, &id, budget_inout);
      if (err == 0) {
        if (last_id == id) {
          ++*n_out;
          goto do_next;
        }
      } else if (err == PDB_ERR_NO)
        return 0;
      else if (err == PDB_ERR_MORE)
        break;
      else
        return err;
    }
  done:
    /*  If we arrive here, we ran out of budget.
     *  Extrapolate.
     *
     *       X : *n_out = range : travelled.
     */
    range = a->it_high - a->it_low;
    if (last_id == PDB_ID_NONE)
      travelled = 0;
    else if (pdb_iterator_forward(pdb, a))
      travelled = last_id - a->it_low;
    else
      travelled = a->it_high - last_id;

    if (travelled == 0)
      *n_out = range;
    else
      *n_out = (range * *n_out) / travelled;
  }
  return 0;
}

/**
 * @brief Freeze an iterator for export.
 *
 *  If the iterator has large private state, that state will be
 *  cached locally.
 *
 * @param g		database handle
 * @param it		freeze this
 * @param buf		into this.
 *
 * @return 0 on success
 * @return GRAPHD_ERR_MORE if one or both iterators didn't lend themselves
 *	to fast intersects
 * @return other nonzero errors on system error.
 */
int graphd_iterator_freeze(graphd_handle *g, pdb_iterator *it, cm_buffer *buf) {
  int err;
  pdb_iterator_text pit;
  cl_handle *cl = g->g_cl;

  /* Freeze using the normal pdb method.
   */
  err = pdb_iterator_freeze(g->g_pdb, it, PDB_ITERATOR_FREEZE_SET |
                                              PDB_ITERATOR_FREEZE_POSITION |
                                              PDB_ITERATOR_FREEZE_STATE,
                            buf);
  if (err != 0) {
    char ibuf[200];
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_freeze", err, "it=\"%s\"",
                 pdb_iterator_to_string(g->g_pdb, it, ibuf, sizeof ibuf));
    return err;
  }

  /* Break up the frozen buffer.
   */
  pdb_iterator_parse(cm_buffer_memory(buf), cm_buffer_memory_end(buf), &pit);

  /* If we have local state, save that in the cache.
   * The cache replaces the local state with a ticket.
   */
  if (pit.pit_state_s != NULL) {
    cl_assert(cl, pit.pit_state_s >= cm_buffer_memory(buf));
    cl_assert(cl, pit.pit_state_s <= cm_buffer_memory_end(buf));

    err = graphd_iterator_state_store(g, buf,
                                      pit.pit_state_s - cm_buffer_memory(buf));
    if (err != 0) {
      char ibuf[200];
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_state_store", err,
                   "it=\"%s\"",
                   pdb_iterator_to_string(g->g_pdb, it, ibuf, sizeof ibuf));
      return err;
    }
  }
  return 0;
}

int graphd_iterator_util_freeze_subiterator(pdb_handle *pdb, pdb_iterator *it,
                                            unsigned int flags,
                                            cm_buffer *buf) {
  int err;

  if (it == NULL) return cm_buffer_add_string(buf, "-");

  if ((err = cm_buffer_add_string(buf, "(")) != 0 ||
      (err = pdb_iterator_freeze(pdb, it, flags, buf)) != 0 ||
      (err = cm_buffer_add_string(buf, ")")) != 0)
    return err;

  return 0;
}

int graphd_iterator_util_thaw_partial_subiterator(
    graphd_handle *g, char const **s_ptr, char const *e, int flags,
    pdb_iterator_text const *pit_in, pdb_iterator_base *pib,
    cl_loglevel loglevel, pdb_iterator **it_out) {
  pdb_iterator_text pit;
  char const *s = *s_ptr;
  char *sub_s, *sub_e;
  int err;

  if (s >= e) {
    cl_log(g->g_cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_util_thaw_subiterator: expected "
           "subiterator, got EOF");
    return GRAPHD_ERR_LEXICAL;
  }
  if (s < e && *s == '-') {
    *s_ptr = s + 1;
    *it_out = NULL;

    return 0;
  }

  if (pit_in != NULL)
    pit = *pit_in;
  else
    pit.pit_set_s = pit.pit_set_e = pit.pit_position_s = pit.pit_position_e =
        pit.pit_state_s = pit.pit_state_e = NULL;

  err =
      pdb_iterator_util_thaw(g->g_pdb, s_ptr, e, "%{(bytes)}", &sub_s, &sub_e);
  if (err != 0) {
    cl_log_errno(g->g_cl, loglevel, "pdb_iterator_util_thaw", err,
                 "can't find () in \"%.*s\"?", (int)(e - s), s);
    return err;
  }
  if (flags & PDB_ITERATOR_FREEZE_SET) {
    pit.pit_set_s = sub_s;
    pit.pit_set_e = pdb_unparenthesized(sub_s, sub_e, '/');
    if (pit.pit_set_e == NULL) pit.pit_set_e = sub_e;

    if ((sub_s = (char *)pit.pit_set_e) < sub_e) sub_s++;
  }
  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    pit.pit_position_s = sub_s;
    pit.pit_position_e = pdb_unparenthesized(sub_s, sub_e, '/');
    if (pit.pit_position_e == NULL) pit.pit_position_e = sub_e;
    if ((sub_s = (char *)pit.pit_position_e) < sub_e) sub_s++;
  }
  if (flags & PDB_ITERATOR_FREEZE_STATE) {
    pit.pit_state_s = sub_s;
    pit.pit_state_e = sub_e;
  }

  err = graphd_iterator_thaw(g, &pit, pib, 0, loglevel, it_out, NULL);
  if (err != 0) {
    cl_log_errno(g->g_cl, loglevel, "graphd_iterator_thaw", err,
                 "failed to thaw \"%.*s\"", (int)(sub_e - sub_s), sub_s);
    return err;
  }
  return 0;
}

int graphd_iterator_util_thaw_subiterator(graphd_handle *g, char const **s_ptr,
                                          char const *e, pdb_iterator_base *pib,
                                          cl_loglevel loglevel,
                                          pdb_iterator **it_out) {
  pdb_iterator_text pit;

  pit.pit_set_s = NULL;
  pit.pit_set_e = NULL;
  pit.pit_position_s = NULL;
  pit.pit_position_e = NULL;
  pit.pit_state_s = NULL;
  pit.pit_state_e = NULL;

  return graphd_iterator_util_thaw_partial_subiterator(
      g, s_ptr, e, PDB_ITERATOR_FREEZE_EVERYTHING, &pit, pib, loglevel, it_out);
}

int graphd_iterator_util_freeze_position(pdb_handle *pdb, bool eof,
                                         pdb_id last_id, pdb_id resume_id,
                                         cm_buffer *buf) {
  char b2[200];

  if (eof) return cm_buffer_add_string(buf, "$");

  if (resume_id != PDB_ID_NONE)
    return cm_buffer_sprintf(buf, "[resume %llu:%s]",
                             (unsigned long long)resume_id,
                             pdb_id_to_string(pdb, last_id, b2, sizeof b2));

  return cm_buffer_add_string(buf,
                              pdb_id_to_string(pdb, last_id, b2, sizeof b2));
}

int graphd_iterator_util_thaw_position(pdb_handle *pdb, char const **s_ptr,
                                       char const *e, cl_loglevel loglevel,
                                       bool *eof, pdb_id *last_id,
                                       pdb_id *resume_id) {
  char const *s = *s_ptr;
  int err = 0;

  if (s == NULL || s >= e) return GRAPHD_ERR_NO;

  if (*s == '$') {
    *last_id = *resume_id = PDB_ID_NONE;
    *eof = true;

    *s_ptr = s + 1;
  } else {
    *eof = false;
    if (*s == '[')
      err = pdb_iterator_util_thaw(pdb, s_ptr, e, "[resume %{id}:%{id}]",
                                   resume_id, last_id);
    else {
      *resume_id = PDB_ID_NONE;
      err = pdb_iterator_util_thaw(pdb, s_ptr, e, "%{id}", last_id);
    }
    if (err != 0) {
      cl_handle *cl = pdb_log(pdb);
      cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                   "expected position; have \"%.*s\"", (int)(e - s), s);
    }
  }
  return err;
}

char const *graphd_iterator_ordering_internalize_request(graphd_request *greq,
                                                         char const *ord_s,
                                                         char const *ord_e) {
  graphd_sort_root sr;
  cl_handle *cl = graphd_request_cl(greq);
  char const *sr_ordering;
  int err;

  /*  In the constraint tree, find the constraint
   *  addressed by this path.
   */
  err = graphd_sort_root_from_string(greq, ord_s, ord_e, &sr);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_sort_root_from_string", err,
                 "couldn't find \"%.*s\"", (int)(ord_e - ord_s), ord_s);
    return NULL;
  }

  sr_ordering = graphd_sort_root_ordering(greq, &sr);
  if (sr_ordering == NULL ||
      strncasecmp(sr_ordering, ord_s, ord_e - ord_s) != 0 ||
      sr_ordering[ord_e - ord_s] != '\0') {
    cl_log(cl, CL_LEVEL_FAIL,
           "graphd_iterator_ordering_lookup: mismatch "
           "between my %s and incoming \"%.*s\"",
           sr_ordering ? sr_ordering : "null", (int)(ord_e - ord_s), ord_s);
    return NULL;
  }
  return sr_ordering;
}

char const *graphd_iterator_ordering_internalize(graphd_handle *g,
                                                 pdb_iterator_base *pib,
                                                 char const *ord_s,
                                                 char const *ord_e) {
  graphd_request *greq;

  greq = pdb_iterator_base_lookup(g->g_pdb, pib, "graphd.request");
  if (greq == NULL) return NULL;
  return graphd_iterator_ordering_internalize_request(greq, ord_s, ord_e);
}

char graphd_iterator_direction_to_char(graphd_direction dir) {
  switch (dir) {
    case GRAPHD_DIRECTION_FORWARD:
      return '+';
    case GRAPHD_DIRECTION_BACKWARD:
      return '-';
    case GRAPHD_DIRECTION_ORDERING:
      return '_';
    default:
      break;
  }
  return '#';
}

graphd_direction graphd_iterator_direction_from_char(int dirchar) {
  switch (dirchar) {
    case '+':
      return GRAPHD_DIRECTION_FORWARD;
    case '-':
      return GRAPHD_DIRECTION_BACKWARD;
    case '_':
      return GRAPHD_DIRECTION_ORDERING;
    default:
      break;
  }
  return GRAPHD_DIRECTION_ANY;
}

/**
 * @brief Set the direction and ordering of a naive iterator
 *
 *  The naive* iterator <it> is either sorted or not.
 *  ("Sorted" means sorted in ascending or descending ID order.)
 *  We're calling from it's environment.  We want <it> to be
 *  sorted because that matches an ordering that we have;
 *  if <it> was actually created in the sort-order indicated
 *  by <direction>, we tag it with its ordering.
 *
 *  ---
 *  * naive:  If <it> were smarter, we'd have passed in direction and
 *     ordering at its create time, and it would manage them itself.
 *
 * @param pdb		handle
 * @param it		iterator
 * @param direction	direction desired of <it>
 * @param ordering	if it had <direction>, what would
 *			its ordering be?
 */
void graphd_iterator_set_direction_ordering(pdb_handle *pdb, pdb_iterator *it,
                                            graphd_direction direction,
                                            char const *ordering) {
  if ((direction == GRAPHD_DIRECTION_FORWARD ||
       direction == GRAPHD_DIRECTION_BACKWARD) &&
      pdb_iterator_sorted_valid(pdb, it) && pdb_iterator_sorted(pdb, it)) {
    cl_handle *cl = pdb_log(pdb);
    char buf[200];

    pdb_iterator_ordered_set(pdb, it, true);
    pdb_iterator_ordering_set(pdb, it, ordering);

    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_set_direction_ordering (%s): %s %d %d",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf),
           pdb_iterator_ordering(pdb, it), pdb_iterator_ordered_valid(pdb, it),
           pdb_iterator_ordered(pdb, it));
  }
}

/*  In the iterator base, save a pointer to <it> as the
 *  original for its set.  If we thaw things later, they'll
 *  clone and parametrize the original over creating completely
 *  new independent operators.
 */
int graphd_iterator_save_original(graphd_handle *g, pdb_iterator_base *pib,
                                  pdb_iterator *it,
                                  pdb_iterator_base **pib_out) {
  cl_handle *cl = g->g_cl;
  cm_buffer set;
  int err;

  cm_buffer_initialize(&set, g->g_cm);
  err = cm_buffer_add_string(&set, "graphd.iterator.");
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "cm_buffer_add_string", err,
                 "can't create set pointer?");
    goto err;
  }

  err = pdb_iterator_freeze(g->g_pdb, it, PDB_ITERATOR_FREEZE_SET, &set);
  if (err != 0) {
    char buf[200];
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_freeze", err,
                 "failed to freeze %s",
                 pdb_iterator_to_string(g->g_pdb, it, buf, sizeof buf));
    goto err;
  }

  err = pdb_iterator_base_set(g->g_pdb, pib, cm_buffer_memory(&set), it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_base_set", err,
                 "failed to set %s", cm_buffer_memory(&set));
    goto err;
  }

  *pib_out = pib;

err:
  cm_buffer_finish(&set);
  return err;
}

/*  Get a pointer to a previously saved original, e.g. for cloning.
 */
int graphd_iterator_get_original(graphd_handle *g, pdb_iterator_base *pib,
                                 pdb_iterator_text const *pit,
                                 pdb_iterator **it_out) {
  cl_handle *cl = g->g_cl;
  cm_buffer set;
  int err;

  cm_buffer_initialize(&set, g->g_cm);
  err = cm_buffer_add_string(&set, "graphd.iterator.");
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "cm_buffer_add_string", err,
                 "can't create set pointer?");
    goto err;
  }

  err = cm_buffer_add_bytes(&set, pit->pit_set_s,
                            pit->pit_set_e - pit->pit_set_s);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "cm_buffer_add_bytes", err,
                 "can't append set \"%.*s\"?",
                 (int)(pit->pit_set_e - pit->pit_set_s), pit->pit_set_s);
    goto err;
  }

  *it_out = pdb_iterator_base_lookup(g->g_pdb, pib, cm_buffer_memory(&set));
  if (*it_out == NULL) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_get_original: cannot "
           "find original for \"%s\" - must be "
           "new.",
           cm_buffer_memory(&set));
    err = GRAPHD_ERR_NO;
    goto err;
  }
err:
  cm_buffer_finish(&set);
  return err;
}

/*  Remove a previously saved original.
 */
int graphd_iterator_remove_saved_original(graphd_handle *g, pdb_iterator *it,
                                          pdb_iterator_base **pib_inout) {
  cl_handle *cl = g->g_cl;
  cm_buffer set;
  int err;

  if (*pib_inout == NULL) return 0;

  cm_buffer_initialize(&set, g->g_cm);
  err = cm_buffer_add_string(&set, "graphd.iterator.");
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "cm_buffer_add_string", err,
                 "can't create set pointer?");
    goto err;
  }

  err = pdb_iterator_freeze(g->g_pdb, it, PDB_ITERATOR_FREEZE_SET, &set);
  if (err != 0) {
    char buf[200];
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_freeze", err,
                 "failed to freeze %s",
                 pdb_iterator_to_string(g->g_pdb, it, buf, sizeof buf));
    goto err;
  }

  err = pdb_iterator_base_delete(g->g_pdb, *pib_inout, cm_buffer_memory(&set));
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_base_set", err,
                 "failed to remove %s", cm_buffer_memory(&set));
    goto err;
  }

err:
  *pib_inout = NULL;
  cm_buffer_finish(&set);
  return err;
}
