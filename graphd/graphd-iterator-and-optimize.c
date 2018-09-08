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

extern const pdb_iterator_type graphd_iterator_and_type;

/*  If the "and" has only one element left,
 *  replace the whole "and" iterator with its first element.
 */
static int and_shrink(pdb_iterator *it) {
  graphd_iterator_and *ogia = it->it_theory;
  graphd_request *greq = ogia->gia_greq;
  pdb_handle *pdb = ogia->gia_pdb;
  cl_handle *cl = ogia->gia_cl;
  pdb_iterator *source;
  int err;
  char const *ordering;
  graphd_direction direction;

  PDB_IS_ITERATOR(cl, it);
  cl_assert(cl, ogia->gia_n == 1);
  cl_assert(cl, it->it_original == it);
  cl_assert(cl, ogia->gia_sc != NULL);

  cl_enter(
      cl, CL_LEVEL_VERBOSE, "input ordering %s %d %d",
      pdb_iterator_ordering(pdb, it) ? pdb_iterator_ordering(pdb, it) : "null",
      pdb_iterator_ordered_valid(pdb, it), pdb_iterator_ordered(pdb, it));

  direction = ogia->gia_direction;
  ordering = pdb_iterator_ordering(pdb, it);

  /*  Replacing the iterator will delete all links
   *  to <ogia->gia_sc->sc_it>.  Since we want to keep
   *  it, we need to make an extra link.
   */
  source = ogia->gia_sc->sc_it;
  pdb_iterator_dup(pdb, source);
  PDB_IS_ITERATOR(cl, source);
  PDB_IS_ITERATOR(cl, it);

  /*  If the "and" was ordered and successfully sorted
   *  or ordered in accordance with its direction, the
   *  subiterator also becomes ordered.
   */
  if (ordering != NULL && pdb_iterator_ordering(pdb, source) == NULL) {
    if (pdb_iterator_sorted_valid(pdb, source) &&
        pdb_iterator_sorted(pdb, source)) {
      if ((direction == GRAPHD_DIRECTION_FORWARD &&
           pdb_iterator_forward(pdb, source)) ||
          (direction == GRAPHD_DIRECTION_BACKWARD &&
           !pdb_iterator_forward(pdb, source))) {
        pdb_iterator_ordering_set(pdb, source, ordering);
        pdb_iterator_ordered_set(pdb, source, true);
      } else {
        pdb_iterator_ordered_set(pdb, source, false);
      }
    } else if (pdb_iterator_ordered_valid(pdb, source) &&
               pdb_iterator_ordered(pdb, source) &&
               direction == GRAPHD_DIRECTION_ORDERING) {
      pdb_iterator_ordering_set(pdb, source, ordering);
      pdb_iterator_ordered_set(pdb, source, true);
    }
  }

  cl_log(cl, CL_LEVEL_VERBOSE, "and_shrink: source ordering %s %d %d",
         pdb_iterator_ordering(pdb, source) ? pdb_iterator_ordering(pdb, source)
                                            : "null",
         pdb_iterator_ordered_valid(pdb, source),
         pdb_iterator_ordered(pdb, source));

  err = graphd_iterator_substitute(greq, it, source);
  if (err != 0) {
    pdb_iterator_destroy(pdb, &source);

    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_substitute", err,
                 "unexpected error");
    cl_leave(cl, CL_LEVEL_VERBOSE, "leave");
    return err;
  }
  PDB_IS_ITERATOR(cl, it);

  cl_leave(
      cl, CL_LEVEL_VERBOSE, "result ordering %s %d %d",
      pdb_iterator_ordering(pdb, it) ? pdb_iterator_ordering(pdb, it) : "null",
      pdb_iterator_ordered_valid(pdb, it), pdb_iterator_ordered(pdb, it));
  return 0;
}

static void and_subcondition_finish(graphd_iterator_and *const ogia,
                                    graphd_subcondition *const sc) {
  cl_handle *cl = ogia->gia_cl;
  cl_enter(cl, CL_LEVEL_VERBOSE, "sc=%p", (void *)sc);

  graphd_iterator_and_process_state_finish(ogia, &sc->sc_contest_ps);
  pdb_iterator_destroy(ogia->gia_pdb, &sc->sc_it);

  cl_leave(cl, CL_LEVEL_VERBOSE, "sc=%p", (void *)sc);
}

/*  Delete the subcondition i.
 */
static void and_delete_subcondition(pdb_iterator *it, size_t i) {
  graphd_iterator_and *ogia = it->it_theory;
  pdb_handle *pdb = ogia->gia_pdb;
  cl_handle *cl = ogia->gia_cl;
  graphd_subcondition *sc;
  char buf[200];
  size_t j;

  cl_enter(cl, CL_LEVEL_VERBOSE, "(i=%lu, %s)", (unsigned long)i,
           pdb_iterator_to_string(pdb, it, buf, sizeof buf));

  cl_assert(cl, pdb_iterator_statistics_done(pdb, it) || !ogia->gia_committed);
  cl_assert(cl, it->it_type == &graphd_iterator_and_type);
  cl_assert(cl, it->it_original == it);
  cl_assert(cl, i < ogia->gia_n);

  /*  Go to a new ID - by deleting the subcondition, this becomes
   *  structurally different from its clones.
   */
  it->it_id = pdb_iterator_new_id(pdb);

  if (it->it_displayname) {
    cm_free(ogia->gia_cm, it->it_displayname);
    it->it_displayname = NULL;
  }

  and_subcondition_finish(ogia, ogia->gia_sc + i);

  GRAPHD_AND_IS_PROCESS_STATE(cl, &ogia->gia_ps);
  GRAPHD_AND_IS_PROCESS_STATE(cl, &ogia->gia_cache_ps);

  graphd_iterator_and_process_state_delete_subcondition(it, &ogia->gia_ps, i);
  graphd_iterator_and_process_state_delete_subcondition(it, &ogia->gia_cache_ps,
                                                        i);
  graphd_iterator_and_check_delete_subcondition(it, i);
  graphd_iterator_and_slow_check_finish(pdb, it);

  if (i < ogia->gia_n - 1)
    memmove(ogia->gia_sc + i, ogia->gia_sc + i + 1,
            sizeof(*ogia->gia_sc) * (ogia->gia_n - (i + 1)));

  for (j = 0, sc = ogia->gia_sc; j < ogia->gia_n - 1; j++, sc++)
    graphd_iterator_and_process_state_delete_subcondition(
        it, &sc->sc_contest_ps, i);

  ogia->gia_n--;
  if (ogia->gia_producer > i) ogia->gia_producer--;

  cl_leave(cl, CL_LEVEL_VERBOSE, "%s",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf));
}

/*
 *  Remove constraints whose primitive summaries are already
 *  contained in those of other primitive summaries.
 *
 *  This is called from the "commit" phase or from within
 *  graphd_iterator_and_evolve().
 */
static int and_combine_psums(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_and *ogia = it->it_theory;
  cl_handle *cl = ogia->gia_cl;
  size_t i;
  int err = 0;
  char buf[200];
  bool any = false;

  cl_enter(cl, CL_LEVEL_VERBOSE, "%s id=%llx",
           pdb_iterator_to_string(ogia->gia_graphd->g_pdb, it, buf, sizeof buf),
           (unsigned long long)it->it_id);
  PDB_IS_ORIGINAL_ITERATOR(cl, it);

  for (i = 0; i < ogia->gia_n; i++) {
    pdb_primitive_summary psum;
    size_t j;
    char buf2[200];
    graphd_subcondition *sc = ogia->gia_sc + i;

    err = pdb_iterator_primitive_summary(pdb, sc->sc_it, &psum);

    cl_log(cl, CL_LEVEL_VERBOSE,
           "and_combine_psum: iterator %zu: %s produces "
           "summary %s",
           i, pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf),
           err ? graphd_strerror(err) : pdb_primitive_summary_to_string(
                                            pdb, &psum, buf2, sizeof buf2));

    if (err != 0 || !psum.psum_complete) continue;

    for (j = 0; j < ogia->gia_n; j++) {
      pdb_primitive_summary ps2;

      if (j != i &&
          pdb_iterator_primitive_summary(pdb, ogia->gia_sc[j].sc_it, &ps2) ==
              0 &&
          pdb_primitive_summary_contains(&psum, &ps2))
        break;
    }
    if (j >= ogia->gia_n) continue;

    /*  Psum is a complete superset of ps2.
     *  It can be removed.
     */
    and_delete_subcondition(it, i);
    i--;
    any = true;
  }

  if (any) {
    if (ogia->gia_n == 1) {
      if ((err = and_shrink(it)) != 0) {
        cl_leave_err(cl, CL_LEVEL_VERBOSE, err, "and_shrink failed");
        return err;
      }
    } else
      it->it_id = pdb_iterator_new_id(pdb);
  }

  cl_leave(cl, CL_LEVEL_VERBOSE, "%s id=%llx",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf),
           (unsigned long long)it->it_id);
  return 0;
}

/*
 *  If there is an "all" iterator in the body (i.e., our constraints
 *  are so terrible that we don't really know anything), and there
 *  is a non-empty PSUM with a primitive result from the subconstraints,
 *  replace the "ALL" with the smallest direct PSUM iterator we can find.
 *
 *  This is called from the "commit" phase only.
 */
static int and_improve_on_all(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_and *ogia = it->it_theory;
  cl_handle *cl = ogia->gia_cl;
  graphd_subcondition *sc, *all_sc = NULL;
  size_t i;
  int err = 0;
  pdb_iterator *best_it = NULL, *this_it = NULL, *best_it_buf = NULL;
  pdb_primitive_summary psum;
  char buf[200], b2[200];
  unsigned long long guess_n[1 << PDB_LINKAGE_N];

  PDB_IS_ORIGINAL_ITERATOR(cl, it);

  for (i = 0, sc = ogia->gia_sc; i < ogia->gia_n; i++, sc++) {
    if (pdb_iterator_all_is_instance(pdb, sc->sc_it)) {
      all_sc = sc;
      break;
    }
  }
  if (i >= ogia->gia_n) return GRAPHD_ERR_ALREADY;

  cl_assert(cl, all_sc != NULL);

  err = pdb_iterator_primitive_summary(pdb, it, &psum);

  cl_enter(cl, CL_LEVEL_VERBOSE, "it=%s, psum=%s",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf),
           pdb_primitive_summary_to_string(pdb, &psum, b2, sizeof b2));

  if (err != 0 || psum.psum_result != PDB_LINKAGE_N || !psum.psum_locked) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "no useful summary");
    return GRAPHD_ERR_ALREADY;
  }

  /*  Mark which psums we already have in our subconstraint set.
   *
   *  Store a pointer to the strongest tractable subiterator
   *  in best_it.
   */
  memset(guess_n, 0, sizeof guess_n);
  for (i = 0, sc = ogia->gia_sc; i < ogia->gia_n; i++, sc++) {
    pdb_primitive_summary sc_psum;

    if (!pdb_iterator_n_valid(pdb, sc->sc_it)) continue;

    err = pdb_iterator_primitive_summary(pdb, sc->sc_it, &sc_psum);
    if (err == 0 && sc_psum.psum_complete && sc_psum.psum_locked) {
      if (sc_psum.psum_locked >= sizeof(guess_n) / sizeof(*guess_n)) continue;

      guess_n[sc_psum.psum_locked] = pdb_iterator_n(pdb, sc->sc_it);
      if (guess_n[sc_psum.psum_locked] == 0) guess_n[sc_psum.psum_locked] = 1;

      if (best_it == NULL ||
          pdb_iterator_n(pdb, best_it) > pdb_iterator_n(pdb, sc->sc_it))
        best_it = sc->sc_it;
    }
  }

  /*  If we found a subiterator whose psum_locked is the same
   *  of the and iterator's psum_locked, we don't need to replace
   *  the "all" - we can just drop it.  There is subiterator
   *  efficient enough to make up for that.
   */
  if (guess_n[psum.psum_locked & ((1 << PDB_LINKAGE_N) - 1)] != 0)

    /* Yes, we have that psum somewhere already.
     */
    goto drop_all;

  /*  Make the smallest VIP or gmap iterator that contains a
   *  superset of this the AND's psum, and isn't already in one
   *  of the other subiterators.
   */
  for (i = 0; i < PDB_LINKAGE_N; i++) {
    bool true_vip;

    if (!(psum.psum_locked & (1 << i))) continue;

    if ((i == PDB_LINKAGE_RIGHT || i == PDB_LINKAGE_LEFT) &&
        (psum.psum_locked & (1 << PDB_LINKAGE_TYPEGUID)) &&
        !guess_n[(1 << i) | (1 << PDB_LINKAGE_TYPEGUID)]) {
      /* Try the VIP iterator.
       */
      err = pdb_vip_linkage_iterator(
          pdb, psum.psum_guid + i, i, psum.psum_guid + PDB_LINKAGE_TYPEGUID,
          it->it_low, it->it_high, it->it_forward,
          /* error-if-null */ false, &this_it, &true_vip);
    } else if (i == PDB_LINKAGE_TYPEGUID &&
               (psum.psum_locked &
                ((1 << PDB_LINKAGE_RIGHT) | (1 << PDB_LINKAGE_LEFT)))) {
      /* We already used this information while visiting
       * the LEFT/RIGHT linkage.
       */
      continue;
    } else if (!guess_n[(1 << i)]) {
      err = pdb_linkage_iterator(pdb, i, psum.psum_guid + i, it->it_low,
                                 it->it_high, it->it_forward,
                                 /* error-if-null */ true, &this_it);
    }

    if (err != 0) {
      pdb_iterator_destroy(pdb, &best_it_buf);
      if (err == GRAPHD_ERR_NO) {
        cl_leave(cl, CL_LEVEL_VERBOSE, "GRAPHD_ERR_NO while creating iterator");
        return pdb_iterator_null_become(pdb, it);
      }
      cl_leave(cl, CL_LEVEL_VERBOSE, "error creating iterator");
      return err;
    }

    /*  Is the iterator we just made better than the
     *  one we already had (if any)?  If yes, move to that
     *  iterator; otherwise, keep the old one.
     */
    cl_assert(cl, this_it != NULL);

    if (best_it == NULL) {
      best_it_buf = best_it = this_it;
    } else if ((pdb_iterator_n_valid(pdb, this_it) &&
                (!pdb_iterator_n_valid(pdb, best_it) ||
                 pdb_iterator_n(pdb, best_it) >
                     pdb_iterator_n(pdb, this_it)))) {
      pdb_iterator_destroy(pdb, &best_it_buf);
      best_it = best_it_buf = this_it;
    } else {
      pdb_iterator_destroy(pdb, &this_it);
    }
  }

  if (best_it == NULL) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "no good replacement");
    return GRAPHD_ERR_ALREADY;
  }

  /*  Do we already have this iterator in our subset?
   */
  if (best_it_buf != NULL) {
    /* No, it's a newcomer.
     */
    pdb_iterator_destroy(pdb, &all_sc->sc_it);
    all_sc->sc_it = best_it;

    best_it_buf = NULL;
  }

drop_all:

  /*  Drop all remaining "all"s.
   */
  for (i = 0; i < ogia->gia_n; i++) {
    if (pdb_iterator_all_is_instance(pdb, ogia->gia_sc[i].sc_it)) {
      char buf[200];

      cl_assert(ogia->gia_cl, ogia->gia_n > 1);
      cl_log(
          cl, CL_LEVEL_VERBOSE, "and_improve_on_all: deleting %s",
          pdb_iterator_to_string(pdb, ogia->gia_sc[i].sc_it, buf, sizeof buf));

      cl_assert(cl, it->it_original == it);
      and_delete_subcondition(it, i);
      i--;
    }
  }

  /*  If that leaves us with only one subiterator,
   *  become that subiterator.
   */
  if (ogia->gia_n == 1) {
    if ((err = and_shrink(it)) != 0) {
      cl_leave_err(cl, CL_LEVEL_VERBOSE, err, "and_shrink failed");
      return err;
    }
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf));
  return 0;
}

static bool and_is_simple_linkage_iterator(pdb_handle *pdb, pdb_iterator *it,
                                           int *linkage_out,
                                           graph_guid *linkage_guid_out) {
  pdb_primitive_summary psum;
  int err;
  int linkage;

  err = pdb_iterator_primitive_summary(pdb, it, &psum);
  if (err != 0 || !psum.psum_complete || psum.psum_result != PDB_LINKAGE_N)
    return false;

  /* Is it one of the three VIP participants?
   */
  if (psum.psum_locked == (1 << (linkage = PDB_LINKAGE_RIGHT)) ||
      psum.psum_locked == (1 << (linkage = PDB_LINKAGE_LEFT)) ||
      psum.psum_locked == (1 << (linkage = PDB_LINKAGE_TYPEGUID))) {
    *linkage_out = linkage;
    *linkage_guid_out = psum.psum_guid[linkage];

    return true;
  }
  return false;
}

/*
 *  Combine "left=x" or "right=x" gmap iterators and
 *  single typeguids into vip operators.  (Or fake
 *  vip operators.)
 *
 *  This is called from the "commit" phase or from within
 *  graphd_iterator_and_evolve().
 */
static int and_combine_vips(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_and *ogia = it->it_theory;
  cl_handle *cl = ogia->gia_cl;
  graphd_subcondition *sc;
  pdb_id type_id = PDB_ID_NONE;
  size_t i, vip_i = ogia->gia_n;
  int err;
  unsigned int have_vip = 0;
  bool changed = false;
  char buf[200];
  pdb_primitive_summary psum;

  cl_enter(cl, CL_LEVEL_VERBOSE, "%s id=%llx",
           pdb_iterator_to_string(ogia->gia_graphd->g_pdb, it, buf, sizeof buf),
           (unsigned long long)it->it_id);
  PDB_IS_ORIGINAL_ITERATOR(cl, it);

  err = pdb_iterator_primitive_summary(pdb, it, &psum);
  if (err != 0 || psum.psum_result != PDB_LINKAGE_N) {
    /* We know nothing! */
    psum.psum_locked = 0;
    psum.psum_complete = false;
  }

  /*  We have VIP ingredients for a typeguid and one of
   *  left or right.
   */
  if (!(psum.psum_locked & (1 << PDB_LINKAGE_TYPEGUID)) ||
      !(psum.psum_locked &
        ((1 << PDB_LINKAGE_RIGHT) | (1 << PDB_LINKAGE_LEFT)))) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "no vip ingredients");
    return 0;
  }

  err = pdb_id_from_guid(pdb, &type_id, psum.psum_guid + PDB_LINKAGE_TYPEGUID);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "typeguid=%s",
                 graph_guid_to_string(psum.psum_guid + PDB_LINKAGE_TYPEGUID,
                                      buf, sizeof buf));
    cl_leave_err(cl, CL_LEVEL_VERBOSE, err, "leave");
    return err;
  }

  for (i = 0, sc = ogia->gia_sc; i < ogia->gia_n; i++, sc++) {
    pdb_primitive_summary sub_psum;

    err = pdb_iterator_primitive_summary(pdb, sc->sc_it, &sub_psum);
    if (err != 0 || sub_psum.psum_result != PDB_LINKAGE_N ||
        !(sub_psum.psum_locked & (1 << PDB_LINKAGE_TYPEGUID)))
      continue;

    /*  We already have a VIP that's mix of typeguid
     *  and left or right.
     */
    have_vip |= sub_psum.psum_locked &
                ((1 << PDB_LINKAGE_LEFT) | (1 << PDB_LINKAGE_RIGHT));
  }
again:
  for (i = 0, sc = ogia->gia_sc; i < ogia->gia_n; i++, sc++) {
    int linkage;
    pdb_iterator *vip;
    graph_guid linkage_guid;
    char b2[200];

    vip = NULL;

    /*  We're looking only at "pure" left, right, or typeguid.
     */
    if (!and_is_simple_linkage_iterator(pdb, sc->sc_it, &linkage,
                                        &linkage_guid))
      continue;

    if (have_vip &&
        ((linkage == PDB_LINKAGE_TYPEGUID) || (have_vip & (1 << linkage)))) {
      /*  Remove this iterator;
       *  it's redundant against
       *  any VIP we already have.
       */
      and_delete_subcondition(it, i);
      goto again;
    }

    /*  We don't have a VIP yet, but we have the ingredients.
     */
    if (linkage == PDB_LINKAGE_TYPEGUID) {
      pdb_id source_id;

      if (!(psum.psum_locked & (1 << (linkage = PDB_LINKAGE_LEFT))))
        linkage = PDB_LINKAGE_RIGHT;

      if (!(psum.psum_locked & (1 << linkage))) {
        cl_notreached(cl,
                      "unexpected "
                      "psum.psum_locked %x",
                      (unsigned int)psum.psum_locked);
      }
      cl_assert(cl, psum.psum_locked & (1 << linkage));
      err = pdb_id_from_guid(pdb, &source_id, psum.psum_guid + linkage);
      if (err != 0) {
        cl_log_errno(
            cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "source=%s",
            graph_guid_to_string(psum.psum_guid + linkage, buf, sizeof buf));
        cl_leave_err(cl, CL_LEVEL_VERBOSE, err, "leave");
        return err;
      }

      /*  Make a VIP iterator out of the endpoint and
       *  the type we got from somewhere else.
       */
      err = graphd_iterator_vip_create(
          ogia->gia_graphd, source_id, linkage, type_id,
          psum.psum_guid + PDB_LINKAGE_TYPEGUID, it->it_low, it->it_high,
          pdb_iterator_forward(pdb, it),
          /* error-if-null */ false, &vip);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_vip_create", err,
                     "iterator=%s",
                     pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
        cl_leave_err(cl, CL_LEVEL_VERBOSE, err, "bye");
        return err;
      }

      have_vip |= (1 << linkage);
    } else {
      pdb_id source_id;

      /*  Get the endpoint's source ID.
       */
      err = pdb_id_from_guid(pdb, &source_id, &linkage_guid);
      if (err != 0) {
        return err;
      }

      /*  Make a VIP iterator out of the endpoint and
       *  the type we got from somewhere else.
       */
      err = graphd_iterator_vip_create(
          ogia->gia_graphd, source_id, linkage, type_id,
          psum.psum_guid + PDB_LINKAGE_TYPEGUID, it->it_low, it->it_high,
          pdb_iterator_forward(pdb, it),
          /* error-if-null */ false, &vip);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_vip_create", err,
                     "iterator=%s",
                     pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
        cl_leave_err(cl, CL_LEVEL_VERBOSE, err, "bye");
        return err;
      }
      have_vip |= (1 << linkage);
    }

    if (vip != NULL) {
      /*  This isn't an identity-preserving replacement.
       *  The VIP iterator is not a refinement of only
       *  the iterator it is replacing.
       *  If there were clones of the old iterator
       *  in other tables, they should not turn into VIPs.
       *  (However, if there are clones of the containing
       *  "and", they *can* change.)
       *
       *  So, we're actually replacing the pointer here,
       *  not just the pointed-to thing.
       */
      cl_log(cl, CL_LEVEL_VERBOSE,
             "and_combine_vips: "
             "replacing %s with vip iterator %s",
             pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf),
             pdb_iterator_to_string(pdb, vip, b2, sizeof b2));

      /* It's empty?
       */
      if (pdb_iterator_n(pdb, vip) == 0) {
        pdb_budget budget = 100;
        pdb_id dummy;

        err = pdb_iterator_next(pdb, vip, &dummy, &budget);
        if (err == GRAPHD_ERR_NO) {
          err = pdb_iterator_null_become(pdb, it);
          pdb_iterator_destroy(pdb, &vip);

          if (err != 0) {
            cl_leave(cl, CL_LEVEL_VERBOSE, "error becoming null");
            return err;
          }
          cl_leave(cl, CL_LEVEL_VERBOSE, "became null");
          return 0;
        }

        /* Well, I guess that didn't work out..
         */
        err = pdb_iterator_reset(pdb, vip);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_reset", err, "it=%s",
                       pdb_iterator_to_string(pdb, vip, buf, sizeof buf));
          pdb_iterator_destroy(pdb, &vip);
          return err;
        }
      }

      /*  Vip gets the same accounting as the guy it replaces.
       */
      if (pdb_iterator_account(pdb, sc->sc_it) != NULL)
        pdb_iterator_account_set(pdb, vip,
                                 pdb_iterator_account(pdb, sc->sc_it));

      pdb_iterator_destroy(pdb, &sc->sc_it);
      sc->sc_it = vip;

      changed = true;
      vip_i = i;
      cl_assert(cl, vip_i < ogia->gia_n);
    }
  }

  /* If anything changed, update our version number.
   */
  if (changed) {
    cl_assert(cl, vip_i < ogia->gia_n);
    it->it_id = pdb_iterator_new_id(pdb);

    /*  The type constraints we no longer need
     *  will be removed by and_combine_psums.
     */
  }

  cl_leave(cl, CL_LEVEL_VERBOSE, "%s id=%llx",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf),
           (unsigned long long)it->it_id);
  return 0;
}

/*
 *  Move iterators about which we know more to the front.
 */
static void and_sort_uninitialized(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_and *ogia = it->it_theory;
  cl_handle *cl = ogia->gia_cl;
  graphd_subcondition *sc;
  size_t i;
  unsigned long long my_n;

  cl_assert(cl, it->it_type == &graphd_iterator_and_type);
  cl_assert(cl, it->it_original == it);

  for (i = 1, sc = ogia->gia_sc + 1; i < ogia->gia_n; i++, sc++) {
    size_t j;

    PDB_IS_ITERATOR(cl, sc->sc_it);

    if (!pdb_iterator_n_valid(pdb, sc->sc_it)) continue;
    my_n = pdb_iterator_n(pdb, sc->sc_it);

    for (j = 0; j < i; j++)
      if (!pdb_iterator_n_valid(pdb, ogia->gia_sc[j].sc_it) ||
          pdb_iterator_n(pdb, ogia->gia_sc[j].sc_it) > my_n)

        break;

    if (j < i) {
      graphd_subcondition sc_tmp;

      /*  Insert sc[i] before sc[j].
       */
      sc_tmp = ogia->gia_sc[i];
      memmove(ogia->gia_sc + j + 1, ogia->gia_sc + j,
              sizeof(*ogia->gia_sc) * (i - j));
      ogia->gia_sc[j] = sc_tmp;
    }
  }

  if (it->it_displayname != NULL) {
    cm_free(ogia->gia_cm, it->it_displayname);
    it->it_displayname = NULL;
  }
}

/**
 * @brief Preevaluate an "and" that's based on a small fixed set.
 *
 * @return PDB_ERR_MORE if that would take too long
 * @return 0 on success
 * @return other errors on unexpected system error.
 */
static int and_become_small_set(graphd_handle *graphd, pdb_iterator *it) {
  graphd_iterator_and *ogia = it->it_theory;
  graphd_request *greq = ogia->gia_greq;
  graphd_subcondition *sc = ogia->gia_sc;
  cl_handle *cl = ogia->gia_cl;
  pdb_handle *pdb = ogia->gia_pdb;
  int err = 0;
  size_t id_n, i;
  pdb_id id, pred_id;
  pdb_iterator *fixed_it = NULL;
  char buf[200];
  unsigned long long best_total = 0, total, best_n = 0;
  graphd_subcondition *best_sc = NULL;
  bool best_likes_find;
  pdb_budget total_budget = GRAPHD_AND_PREEVALUATE_COST_MAX;

  cl_assert(cl, !ogia->gia_thaw);

  /*  All subiterators point in the same direction, right?  Right???
   */
  fixed_it = NULL;
  for (i = 0, sc = ogia->gia_sc; i < ogia->gia_n; i++, sc++) {
    if (!pdb_iterator_sorted(pdb, sc->sc_it) ||
        !pdb_iterator_sorted_valid(pdb, sc->sc_it))
      continue;

    if (fixed_it == NULL)
      fixed_it = sc->sc_it;

    else if (pdb_iterator_forward(pdb, fixed_it) !=
             pdb_iterator_forward(pdb, sc->sc_it)) {
      char buf1[200], buf2[200], buf3[200];

      cl_notreached(cl,
                    "and_become_small_set: %s: subiterator "
                    "%s is sorted in opposite direction from "
                    "%s before it.",
                    pdb_iterator_to_string(pdb, it, buf1, sizeof buf1),
                    pdb_iterator_to_string(pdb, sc->sc_it, buf2, sizeof buf2),
                    pdb_iterator_to_string(pdb, fixed_it, buf3, sizeof buf3));
    }
  }
  fixed_it = NULL;

  /*  Set best_sc to the address of the subconstraint with the
   *  smallest production cost for its full set.
   */
  for (i = ogia->gia_n, sc = ogia->gia_sc; i--; sc++) {
    if (pdb_iterator_n_valid(pdb, sc->sc_it) &&
        pdb_iterator_next_cost_valid(pdb, sc->sc_it)) {
      unsigned long long n;

      /*  If any of the subiterators is empty,
       *  the "and" is empty immediately.
       */
      n = pdb_iterator_n(pdb, sc->sc_it);
      if (n == 0 && pdb_iterator_next_cost(pdb, sc->sc_it) <= 100) {
        pdb_budget budget = 100;
        pdb_id dummy;

        err = pdb_iterator_next(pdb, sc->sc_it, &dummy, &budget);
        if (err == GRAPHD_ERR_NO) {
          /* Cool, we're actually empty.
           */
          return pdb_iterator_null_become(pdb, it);
        } else if (err == 0 || err == PDB_ERR_MORE) {
          if (err == 0) n = 1;

          err = pdb_iterator_reset(pdb, sc->sc_it);
          if (err != 0) {
            cl_log_errno(
                cl, CL_LEVEL_FAIL, "pdb_iterator_reset", err, "it=%s",
                pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
            return err;
          }
        } else {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next/find", err,
                       "id=%llx", (unsigned long long)id);
          return err;
        }
      }

      /*  It costs one more because we need to wait for the
       *  last (empty) one, too.
       */
      total = (1 + n) * pdb_iterator_next_cost(pdb, sc->sc_it);

      if (best_sc == NULL || best_total > total) {
        best_total = total;
        best_sc = sc;
        best_n = n;

        cl_log(cl, CL_LEVEL_VERBOSE,
               "and_become_small_set: looking at n=%lu "
               "total=%llu  estimate from %p/%s",
               (unsigned long)best_n, (unsigned long long)best_total,
               (void *)best_sc->sc_it,
               pdb_iterator_to_string(pdb, best_sc->sc_it, buf, sizeof buf));
      }
    }
  }

  if (best_sc == NULL) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "and_become_small_set: no small set subsets in %s",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    return PDB_ERR_MORE;
  }
  if (best_total > GRAPHD_AND_PREEVALUATE_COST_MAX / 2) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "and_become_small_set: best_total %llu from %s is "
           "too large - defaulting.",
           best_total,
           pdb_iterator_to_string(pdb, best_sc->sc_it, buf, sizeof buf));
    return PDB_ERR_MORE;
  }

  cl_enter(cl, CL_LEVEL_VERBOSE, "best_total is %lu",
           (unsigned long)best_total);

  for (i = ogia->gia_n, sc = ogia->gia_sc; i--; sc++) {
    if (sc == best_sc) continue;

    if (!pdb_iterator_check_cost_valid(pdb, sc->sc_it)) {
      cl_leave(cl, CL_LEVEL_VERBOSE,
               "check cost from %s is not valid - defaulting.",
               pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
      return PDB_ERR_MORE;
    }
    best_total += best_n * pdb_iterator_check_cost(pdb, sc->sc_it);
  }
  if (best_total >= GRAPHD_AND_PREEVALUATE_COST_MAX) {
    cl_leave(cl, CL_LEVEL_VERBOSE,
             "and_become_small_set: "
             "total check cost %llu is too large - defaulting.",
             best_total);
    return PDB_ERR_MORE;
  }

  /*  If we're looking at an intersect of exactly two
   *  sorted GMAPs with a fixed known upper bound,
   *  there's a routine for that.
   */
  if (ogia->gia_n == 2 &&
      (err = graphd_iterator_intersect(
           graphd, ogia->gia_sc[0].sc_it, ogia->gia_sc[1].sc_it, it->it_low,
           it->it_high, pdb_iterator_forward(pdb, it), false, &total_budget,
           &fixed_it)) == 0) {
    err = graphd_iterator_substitute(ogia->gia_greq, it, fixed_it);
    if (err) pdb_iterator_destroy(pdb, &fixed_it);
    cl_leave(cl, CL_LEVEL_VERBOSE, "graphd_iterator_intersect");
    return err;
  }

  /*  We can't use fast GMAP/FIXED  or GMAP/GMAP intersection,
   *  but we can do this using normal find iteration.
   */
  err = graphd_iterator_fixed_create(graphd, (size_t)best_n, it->it_low,
                                     it->it_high, pdb_iterator_forward(pdb, it),
                                     &fixed_it);
  if (err != 0) {
    cl_leave_err(cl, CL_LEVEL_VERBOSE, err,
                 "error in graphd_iterator_fixed_create; "
                 "low=%llx, high=%llx, forward=%d, n=%llu",
                 (unsigned long long)it->it_low,
                 (unsigned long long)it->it_high, (int)it->it_forward, best_n);
    return err;
  }
  pred_id = PDB_ID_NONE;
  id_n = 0;
  best_likes_find = pdb_iterator_sorted(pdb, best_sc->sc_it) &&
                    pdb_iterator_statistics_done(pdb, best_sc->sc_it) &&
                    pdb_iterator_find_cost(pdb, best_sc->sc_it) <=
                        pdb_iterator_check_cost(pdb, best_sc->sc_it) +
                            pdb_iterator_next_cost(pdb, best_sc->sc_it) + 2;
  for (;;) {
  /*  Pull another ID out of the best available producer.
   */
  redo:
    if (pred_id != PDB_ID_NONE) {
      cl_assert(cl, best_likes_find);

      err = pdb_iterator_find(pdb, best_sc->sc_it, pred_id, &id, &total_budget);
      pred_id = PDB_ID_NONE;
    } else {
      err = pdb_iterator_next(pdb, best_sc->sc_it, &id, &total_budget);
    }
    if (err == GRAPHD_ERR_NO)
      break;
    else if (err == PDB_ERR_MORE) {
      cl_log(cl, CL_LEVEL_DEBUG,
             "and_become_small_set: "
             "ran out of time at producer next/find "
             "for \"%s\" ($%lld+) - going back to defaults",
             pdb_iterator_to_string(pdb, best_sc->sc_it, buf, sizeof buf),
             GRAPHD_AND_PREEVALUATE_COST_MAX - total_budget);
      pdb_iterator_call_reset(pdb, best_sc->sc_it);
      goto cancel;
    }
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next/find", err, "id=%llx",
                   (unsigned long long)id);
      goto cancel;
    }

    /*  Check the ID against the other nodes.
     */
    for (i = ogia->gia_n, sc = ogia->gia_sc; i--; sc++) {
      if (sc == best_sc) continue;

      if (pdb_iterator_sorted(pdb, sc->sc_it) && best_n > 1 &&
          best_likes_find && pdb_iterator_statistics_done(pdb, sc->sc_it) &&
          pdb_iterator_find_cost(pdb, sc->sc_it) <=
              pdb_iterator_check_cost(pdb, sc->sc_it) +
                  pdb_iterator_next_cost(pdb, sc->sc_it) + 2) {
        pdb_id id_found;

        err = pdb_iterator_find(pdb, sc->sc_it, id, &id_found, &total_budget);

        if (err == PDB_ERR_MORE) {
          cl_log(cl, CL_LEVEL_DEBUG,
                 "and_become_small_set: "
                 "ran out of time at "
                 "find %llx in \"%s\" ($%lld+)"
                 "- going back to defaults",
                 (unsigned long long)id,
                 pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf),
                 GRAPHD_AND_PREEVALUATE_COST_MAX - total_budget);

          pdb_iterator_call_reset(pdb, sc->sc_it);
          goto cancel;
        }
        if (err != 0) break;
        if (id != id_found) {
          pred_id = id = id_found;
          goto redo;
        }
      } else {
        err = pdb_iterator_check(pdb, sc->sc_it, id, &total_budget);
        if (err != 0) {
          if (err == GRAPHD_ERR_NO) break;

          if (err == PDB_ERR_MORE) {
            cl_log(cl, CL_LEVEL_DEBUG,
                   "and_become_small_set: "
                   "ran out of time at "
                   "check \"%s\" ($%lld+)"
                   "- going back to defaults",
                   pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf),
                   GRAPHD_AND_PREEVALUATE_COST_MAX - total_budget);

            pdb_iterator_call_reset(pdb, sc->sc_it);
            goto cancel;
          }
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_check", err, "id=%llx",
                       (unsigned long long)id);
          goto cancel;
        }
      }
    }

    if (err == GRAPHD_ERR_NO) continue;

    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_check/find", err, "id=%llx",
                   (unsigned long long)id);
      goto cancel;
    }
    if (id_n == best_n) {
      cl_log(cl, CL_LEVEL_FAIL,
             "and_become_small_set: more than "
             "promised %llu id%s in %s",
             (unsigned long long)best_n, best_n == 1 ? "" : "s",
             pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    }
    if (total_budget <= 0) {
      cl_log(cl, CL_LEVEL_FAIL,
             "and_become_small_set: %s is "
             "taking too long ($%lld) (n estimated=%llu, "
             "actual=%llu)",
             pdb_iterator_to_string(pdb, it, buf, sizeof buf),
             GRAPHD_AND_PREEVALUATE_COST_MAX - total_budget,
             (unsigned long long)best_n, (unsigned long long)id_n);
      goto cancel;
    }

    /*  We've got an id that all the subclauses like.
     */
    id_n++;
    err = graphd_iterator_fixed_add_id(fixed_it, id);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_fixed_add_id", err,
                   "id=%llx", (unsigned long long)id);
      goto cancel;
    }
  }
  graphd_iterator_fixed_create_commit(fixed_it);

  err = graphd_iterator_substitute(greq, it, fixed_it);
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
  /* Reset the iterators.
   */
  for (i = ogia->gia_n, sc = ogia->gia_sc; i--; sc++) {
    int e;
    e = pdb_iterator_reset(pdb, sc->sc_it);
    if (e != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_reset", err, "it=%s",
                   pdb_iterator_to_string(pdb, sc->sc_it, buf, sizeof buf));
      if (err == 0) err = e;
    }
  }
  pdb_iterator_destroy(pdb, &fixed_it);

  cl_leave(cl, CL_LEVEL_VERBOSE, "%s",
           err ? graphd_strerror(err) : "too complicated");
  return err ? err : PDB_ERR_MORE;
}

/*
 *  Delete "all" iterators that we threw in as a safeguard against
 *  bad explicit constraint iterators, but don't turn out to need.
 *
 *  If this goes wrong (and returns an error code), the "it"
 *  iterator may be destroyed in the processes.
 */
static int and_delete_spurious_all(pdb_handle *pdb, pdb_iterator *it) {
  size_t i;
  graphd_iterator_and *ogia = it->it_theory;
  int err = 0;
  bool any = false;
  cl_handle *cl = pdb_log(pdb);

  PDB_IS_ITERATOR(cl, it);
  cl_assert(cl, it->it_type == &graphd_iterator_and_type);
  cl_assert(cl, pdb_iterator_statistics_done(pdb, it));
  cl_assert(cl, it->it_original == it);
  cl = ogia->gia_cl;

  /*  Delete an "all" that isn't the producer.
   */
  for (i = 0; i < ogia->gia_n; i++) {
    PDB_IS_ITERATOR(cl, it);

    if (i != ogia->gia_producer &&
        pdb_iterator_all_is_instance(pdb, ogia->gia_sc[i].sc_it)) {
      char buf[200];

      cl_assert(ogia->gia_cl, ogia->gia_n > 1);
      cl_log(
          cl, CL_LEVEL_VERBOSE, "and_delete_spurious_all: deleting %s",
          pdb_iterator_to_string(pdb, ogia->gia_sc[i].sc_it, buf, sizeof buf));

      cl_assert(cl, it->it_original == it);
      and_delete_subcondition(it, i);
      i--;
      any = true;
    }
  }

  /*  If that led to there just being one subiterator in
   *  this "all", replace the "all" with that subiterator.
   */
  cl_assert(cl, ogia->gia_n > 0);
  if (ogia->gia_n == 1) {
    PDB_IS_ITERATOR(cl, it);
    err = and_shrink(it);
    PDB_IS_ITERATOR(cl, it);
  }

  else if (any) {
    /*  If we deleted anything (but didn't shrink to 0),
     *  version this iterator's id so our clones know
     *  to reclone themselves.
     */
    it->it_id = pdb_iterator_new_id(pdb);
    PDB_IS_ITERATOR(cl, it);
  }

  PDB_IS_ITERATOR(cl, it);
  return err;
}

/*
 *  Given the states of the subiterators, perform optimizations that
 *  can possibly change - even replace - the and-iterator itself.
 *
 *  Invoked before "next" or "find".
 *
 * @return GRAPHD_ERR_ALREADY if nothing changed.
 * @return 0	if something changed; invalidate saved state.
 * @return nonzero error codes on error.
 */
int graphd_iterator_and_evolve(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_and *gia = it->it_theory;
  cl_handle *cl = gia->gia_cl;
  char buf[200];
  size_t i;
  int err;
  long long id_entry = it->it_id;

  if (gia->gia_evolved) return GRAPHD_ERR_ALREADY;
  gia->gia_evolved = true;

  cl_enter(cl, CL_LEVEL_VERBOSE, "%s",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf));

  /*  If we're not an original:
   *   - if our original hasn't evolved yet, evolve it;
   *   - then copy it.
   */
  if (it->it_original != it) {
    /*  If our original has changed, but we haven't yet,
     *  change along with the original.
     */

    /*  Make sure the original is evolved.
     */
    if (it->it_original->it_type == &graphd_iterator_and_type) {
      err = graphd_iterator_and_evolve(pdb, it->it_original);
      if (err != 0 && err != GRAPHD_ERR_ALREADY) return err;
    }

    /*  Get in sync with the original.
     */
    err = pdb_iterator_refresh(pdb, it);
    if (err != 0 && err != GRAPHD_ERR_ALREADY) return err;

    /*  If we're still an "and" iterator at this point,
     *  get our producer clue and ordering from our original.
     */
    if (it->it_type == &graphd_iterator_and_type) {
      gia = it->it_theory;

      cm_free(gia->gia_cm, it->it_displayname);
      it->it_displayname = NULL;

      err = graphd_iterator_and_check_sort_refresh(it, &gia->gia_ps);
    }
    goto done;
  }

  cl_assert(cl, pdb_iterator_statistics_done(pdb, it));

  err = and_combine_vips(pdb, it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "and_combine_psums", err, "%s",
                 pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    goto done;
  }
  if (it->it_type != &graphd_iterator_and_type || it->it_theory != gia)
    goto done;

  err = and_combine_psums(pdb, it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "and_combine_psums", err, "%s",
                 pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    goto done;
  }
  if (it->it_type != &graphd_iterator_and_type || it->it_theory != gia)
    goto done;

  /*  If there's just one subcondition, become that
   *  single subcondition.
   */
  if (gia->gia_n == 1) {
    cl_leave(
        cl, CL_LEVEL_VERBOSE, "turn into %s",
        pdb_iterator_to_string(pdb, gia->gia_sc[0].sc_it, buf, sizeof buf));
    return and_shrink(it);
  }

  /*  If there's a null subcondition, become NULL.
   */
  for (i = 0; i < gia->gia_n; i++)
    if (pdb_iterator_null_is_instance(pdb, gia->gia_sc[i].sc_it)) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "turn into null");
      return pdb_iterator_null_become(pdb, it);
    }

  /*  If we have statistics, and we have a non-all producer,
   *  remove any "all" producers.
   */
  if ((err = and_delete_spurious_all(pdb, it)) != 0 ||
      it->it_type != &graphd_iterator_and_type || it->it_theory != gia)
    goto done;

  err = graphd_iterator_and_check_sort(it);

done:
  if (it->it_type == &graphd_iterator_and_type) {
    gia = it->it_theory;

    cm_free(gia->gia_cm, it->it_displayname);
    it->it_displayname = NULL;
  }
  if (err == 0 && it->it_id == id_entry) err = GRAPHD_ERR_ALREADY;

  cl_leave(cl, CL_LEVEL_VERBOSE, "%s",
           err == GRAPHD_ERR_ALREADY
               ? "nothing changed"
               : (err ? strerror(err)
                      : pdb_iterator_to_string(pdb, it, buf, sizeof buf)));
  return err;
}

/*  @brief Do the preparation we do prior to iterator access.
 *
 *  This is called from next, check, and find.
 *
 *  @param pdb		handle
 *  @param it 		an and-iterator
 *  @param budget_inout	time available
 *  @param research	how much of the budget can we spend on research?
 *			1.0=everything, 0.0=nothing.
 *
 *  @return GRAPHD_ERR_ALREADY means, "go ahead, it's prepared'
 *  @return 0 means "I did something, redirect me."
 *  @return other error codes for system errors.
 *
 */
int graphd_iterator_and_access(pdb_handle *pdb, pdb_iterator *it,
                               pdb_budget *budget_inout, float research) {
  graphd_iterator_and *gia = it->it_theory;
  graphd_handle *g = gia->gia_graphd;
  cl_handle *cl = pdb_log(pdb);
  int err;
  unsigned long long entry_id = it->it_id;

  PDB_IS_ITERATOR(cl, it);
  cl_assert(cl, it->it_type == &graphd_iterator_and_type);

  if ((err = pdb_iterator_refresh(pdb, it)) != PDB_ERR_ALREADY) return err;

  PDB_IS_ITERATOR(cl, it);
  if (!pdb_iterator_statistics_done(pdb, it)) {
    /*  Do some statistics.
     */
    pdb_budget research_budget;

    research_budget = *budget_inout * research;
    if (GRAPHD_SABOTAGE(g, research_budget <= 0)) return PDB_ERR_MORE;

    *budget_inout -= research_budget;

    PDB_IS_ITERATOR(cl, it);
    err = pdb_iterator_statistics(pdb, it, &research_budget);
    *budget_inout += research_budget;

    /*  Result here is 0 (statistics are done)
     *  or PDB_ERR_MORE (need more time)
     *  or an unspecified system error.
     */
    cl_assert(cl, err != PDB_ERR_ALREADY);

    /* Did something change that would require
     * updating a clone?
     */
    if (err != 0 || entry_id != it->it_id) return err;
  }

  if (it->it_original->it_original != it->it_original) {
    char buf[200];
    cl_log(cl, CL_LEVEL_FATAL,
           "it %p, it->it_original %p, it->it_original->it_original %p",
           (void *)it, it->it_original, it->it_original->it_original);
    cl_log(cl, CL_LEVEL_FATAL, "it %s",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    cl_log(cl, CL_LEVEL_FATAL, "it->it_original %s",
           pdb_iterator_to_string(pdb, it->it_original, buf, sizeof buf));
    cl_log(cl, CL_LEVEL_FATAL, "it %s",
           pdb_iterator_to_string(pdb, it->it_original->it_original, buf,
                                  sizeof buf));
  }
  PDB_IS_ITERATOR(cl, it);
  cl_assert(cl, it->it_type == &graphd_iterator_and_type);

  gia = it->it_theory;
  if (!gia->gia_evolved) {
    err = graphd_iterator_and_evolve(pdb, it);
    if (err != GRAPHD_ERR_ALREADY) return err;
  }
  cl_assert(cl, pdb_iterator_statistics_done(pdb, it) && gia->gia_evolved);

  return GRAPHD_ERR_ALREADY;
}

int graphd_iterator_and_optimize(graphd_handle *graphd, pdb_iterator *it) {
  int err;
  pdb_handle *pdb = graphd->g_pdb;
  graphd_iterator_and *ogia = it->it_theory;
  cl_handle *cl = ogia->gia_cl;
  char buf[200];
  size_t i;
  unsigned long long upper_bound;

  /* Pull the container's low/high boundaries to the
   * smallest subiterator boundaries.
   */
  for (i = 0; i < ogia->gia_n; i++) {
    pdb_iterator *sub;

    sub = ogia->gia_sc[i].sc_it;
    if (sub->it_low > it->it_low) {
      cl_log(cl, CL_LEVEL_VERBOSE, "raising AND low %llx to %s low %llx",
             it->it_low, pdb_iterator_to_string(pdb, sub, buf, sizeof buf),
             sub->it_low);
      it->it_low = sub->it_low;
    }
    if (sub->it_high < it->it_high) {
      cl_log(cl, CL_LEVEL_VERBOSE, "lowering AND high %lld to %s high %lld",
             it->it_high, pdb_iterator_to_string(pdb, sub, buf, sizeof buf),
             sub->it_high);
      it->it_high = sub->it_high;
    }
  }

  /*  If we have anything at all that is well-sorted
   *  and has tractable costs, we'll prefer that over
   *  an "all" iterator.  In that case, remove the "all"
   *  iterator.
   *
   *  If we have a linksto iterator, it has a built-in
   *  attempt to use an "all" iterator or something
   *  similar - if we find one of those, also throw out
   *  the parent "all".
   */
  upper_bound = pdb_primitive_n(pdb);
  upper_bound =
      (it->it_high == PDB_ITERATOR_HIGH_ANY ? upper_bound : it->it_high) -
      it->it_low;

  for (i = 0; i < ogia->gia_n; i++) {
    pdb_iterator *sub;
    sub = ogia->gia_sc[i].sc_it;

    if (graphd_iterator_linksto_is_instance(pdb, sub, NULL, NULL) &&
        ogia->gia_n == 2) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_and_create_commit: "
             "removing any instances of \"all\" "
             "because I've found a linksto "
             "iterator: %s",
             pdb_iterator_to_string(pdb, sub, buf, sizeof buf));
      break;
    }

    if (pdb_iterator_n_valid(pdb, sub) &&
        pdb_iterator_n(pdb, sub) < upper_bound &&
        pdb_iterator_check_cost_valid(pdb, sub) &&
        pdb_iterator_next_cost_valid(pdb, sub) &&
        ((pdb_iterator_next_cost(pdb, sub) * pdb_iterator_n(pdb, sub)) <
         (pdb_iterator_check_cost(pdb, sub) * upper_bound))) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_and_create_commit: "
             "removing any instances of \"all\" "
             "because I've found something "
             "better: %s, with n=%llu (vs %llu), "
             "nc=%llu, cc=%llu, lo=%llx, hi=%llx",
             pdb_iterator_to_string(pdb, sub, buf, sizeof buf),
             (unsigned long long)pdb_iterator_n(pdb, sub),
             (unsigned long long)upper_bound,
             (unsigned long long)pdb_iterator_next_cost(pdb, sub),
             (unsigned long long)pdb_iterator_check_cost(pdb, sub),
             (unsigned long long)sub->it_low, (unsigned long long)sub->it_high);
      break;
    }
  }
  if (i < ogia->gia_n)
    for (i = 0; i < ogia->gia_n; i++)
      if (ogia->gia_n > 1 &&
          pdb_iterator_all_is_instance(pdb, ogia->gia_sc[i].sc_it)) {
        cl_assert(cl, it->it_original == it);
        and_delete_subcondition(it, i);
        ogia->gia_producer = 0;
        i--;
      }

  if ((err = and_combine_vips(pdb, it)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "and_combine_vips", err, "%s",
                 pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    return err;
  }
  if (it->it_type != &graphd_iterator_and_type || it->it_theory != ogia)
    return 0;

  if ((err = and_combine_psums(pdb, it)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "and_combine_psums", err, "%s",
                 pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    return err;
  }
  if (it->it_type != &graphd_iterator_and_type || it->it_theory != ogia)
    return 0;

  err = and_improve_on_all(pdb, it);
  if (err != 0 && err != GRAPHD_ERR_ALREADY) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "and_improve_on_all", err, "%s",
                 pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    return err;
  }
  if (it->it_type != &graphd_iterator_and_type || it->it_theory != ogia) {
    return 0;
  }

  if (ogia->gia_n > 1) {
    err = and_become_small_set(graphd, it);
    if (err != GRAPHD_ERR_MORE) {
      return err;
    }

    if (it->it_type != &graphd_iterator_and_type || it->it_theory != ogia) {
      return 0;
    }
  }

  if (ogia->gia_n == 1) {
    if ((err = and_shrink(it)) != 0) {
      cl_log_errno(cl, CL_LEVEL_VERBOSE, "and_shrink", err, "unexpected error");
      return err;
    }
    PDB_IS_ITERATOR(cl, it);
    return 0;
  }

  /*  Do a raw pre-sort based on check-counts from
   *  those subelements that happen to have them.
   *
   *  This should float fixed elements to the start.
   */
  and_sort_uninitialized(pdb, it);
  return 0;
}
