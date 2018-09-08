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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "libcl/cl.h"
#include "libcm/cm.h"

#define RIT_MAGIC 0xf030ab87

#define RIT_MAGIC_CHECK(cl, r) cl_assert((cl), (r)->vr_magic == RIT_MAGIC)

/*
 * Vrange iterators are simple wrappers over another iterator that
 * use tricks to iterate over a range of primitive values.
 *
 * Vrange iterators pass through all of the work to their underlying iterator
 * (which is either an OR iterator, or a FIXED iterator that came from an OR)
 * except in these cases:
 * 	* We override the check function to compare the primitives value
 * directly
 * 	* We wrap the underlying iterator when we freeze
 */

static const pdb_iterator_type graphd_iterator_vrange;

/*  Perform vrange statistics, however long it takes.
 */
static int vrange_emergency_statistics(pdb_handle *pdb, pdb_iterator *it) {
  int err;

  do {
    pdb_budget budget = 999999;
    err = pdb_iterator_statistics(pdb, it, &budget);

  } while (err == PDB_ERR_MORE);
  return err;
}

static const char *graphd_iterator_vrange_to_string(pdb_handle *pdb,
                                                    pdb_iterator *it, char *buf,
                                                    size_t size) {
  graphd_value_range *vr = it->it_theory;
  graphd_value_range *orit;
  cl_handle *cl = graphd_request_cl(vr->vr_greq);
  cm_handle *cm = pdb_mem(pdb);
  char sbuf[200];
  const char *s;

  vr = it->it_theory;
  orit = it->it_original->it_theory;
  RIT_MAGIC_CHECK(cl, vr);
  RIT_MAGIC_CHECK(cl, orit);

  if (vr->vr_internal_and) {
    s = pdb_iterator_to_string(pdb, vr->vr_internal_and, sbuf, sizeof sbuf);
  } else {
    s = "everything";
  }
  snprintf(buf, size, "%svrange[%s]('%.*s'-'%.*s')AND(%s):%llx..%llx",
           it->it_forward ? "" : "~", vr->vr_cmp->cmp_name,
           (int)(orit->vr_lo_e - orit->vr_lo_s), orit->vr_lo_s,
           (int)(orit->vr_hi_e - orit->vr_hi_s), orit->vr_hi_s, s, it->it_low,
           it->it_high);
  it->it_displayname = cm_strmalcpy(cm, buf);
  return buf;
}

static int graphd_iterator_vrange_statistics(pdb_handle *pdb, pdb_iterator *it,
                                             pdb_budget *budget_inout) {
  char buf[200];
  int err;
  graphd_value_range *vr = it->it_theory;
  cl_handle *cl = graphd_request_cl(vr->vr_greq);
  unsigned long long total_ids;
  pdb_budget next_cost;

  RIT_MAGIC_CHECK(cl, vr);

  if (pdb_iterator_statistics_done(pdb, it)) return GRAPHD_ERR_ALREADY;

  /*
   * Get the statistics for our internal and if we have one
   */
  if (vr->vr_internal_and) {
    if (!pdb_iterator_statistics_done(pdb, vr->vr_internal_and)) {
      err = pdb_iterator_statistics(pdb, vr->vr_internal_and, budget_inout);
      if (err) return err;
    }
  }

  /*
   * Ask the vrange comparator to calculate statistics for us.
   */
  next_cost = PDB_COST_HMAP_ELEMENT;
  err = vr->vr_cmp->cmp_vrange_statistics(vr->vr_greq, vr, vr + 1, &total_ids,
                                          &next_cost, budget_inout);
  if (err == PDB_ERR_MORE) return PDB_ERR_MORE;

  if (vr->vr_internal_and != NULL) {
    /*
     * We want the next cost to reflect the amortized cost
     * to getting new IDs out of the intersection between
     * the internal_and iterator and the bin hmap.  The
     * number of steps required is:
     *  next_cost * N * log2(M), where M is the size of the larger
     *  set and M is the size of the smaller set, and C is some
     *  constant.  (C = next_cost).
     *
     *  log2(M) represents the search cost of the M set, in
     *  otherwords pdb_iterator_find_cost().  We wish to amortize
     *  the total intersect cost over every next call so we get to
     *  divide by N. We end up with:
     *  next_cost * N * find_cost(M) / N = next_cost*find_cost(M).
     */
    pdb_iterator_next_cost_set(
        pdb, it, next_cost * pdb_iterator_find_cost(pdb, vr->vr_internal_and));
  } else {
    pdb_iterator_next_cost_set(pdb, it, next_cost);
  }

  /*
   * Our best guess for n is the lesser of the interators
   * in our intersection
   */
  if (vr->vr_internal_and &&
      (pdb_iterator_n(pdb, vr->vr_internal_and) < total_ids)) {
    pdb_iterator_n_set(pdb, it, pdb_iterator_n(pdb, vr->vr_internal_and));
  } else {
    pdb_iterator_n_set(pdb, it, total_ids);
  }

  pdb_iterator_check_cost_set(pdb, it, PDB_COST_PRIMITIVE);

  /*
   * XXX Sometimes.. If we're really lucky it might be sorted
   * We need a way for cmp_vrange_statistics to tell us that
   * it is only going to return one iterator that happens to be
   * ID sorted.
   */
  pdb_iterator_sorted_set(pdb, it, false);
  pdb_iterator_statistics_done_set(pdb, it);

  cl_log(pdb_log(pdb), CL_LEVEL_DEBUG,
         "PDB STAT for %s: n=%llu cc=%llu; "
         "nc=%llu; fc=%llu; %ssorted; %s%s",
         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         (unsigned long long)pdb_iterator_n(pdb, it),
         (unsigned long long)pdb_iterator_check_cost(pdb, it),
         (unsigned long long)pdb_iterator_next_cost(pdb, it),
         (unsigned long long)pdb_iterator_find_cost(pdb, it),
         pdb_iterator_sorted(pdb, it) ? "" : "un",
         pdb_iterator_ordered(pdb, it) ? "o=" : "unordered",
         pdb_iterator_ordering(pdb, it) != NULL
             ? pdb_iterator_ordering(pdb, it)
             : (pdb_iterator_ordered(pdb, it) ? "null" : ""));

  return 0;
}

static int graphd_iterator_vrange_reset(pdb_handle *pdb, pdb_iterator *it) {
  graphd_value_range *vr = it->it_theory;
  cl_handle *cl = graphd_request_cl(vr->vr_greq);
  int err;
  char buf[200];

  cl_log(cl, CL_LEVEL_SPEW, "graphd_iterator_vrange_reset: reset %p", it);

  RIT_MAGIC_CHECK(pdb_log(pdb), vr);
  it->it_has_position = true;

  vr->vr_cvit_last_id_out = PDB_ID_NONE;
  vr->vr_last_id_out = PDB_ID_NONE;

  /*
   * Make the vrange iterator reset its position
   */
  err = vr->vr_cmp->cmp_vrange_start(vr->vr_greq, vr, vr + 1);
  if (err) {
    cl_log_errno(pdb_log(pdb), CL_LEVEL_ERROR, "vrange_cmp_start", err,
                 "%s: can't reset vrange state",
                 pdb_iterator_to_string(pdb, it, buf, sizeof(buf)));
    return err;
  }

  /* XXX this is pretty bad.  If we just built our first
   * "and" iterator in e.g. statistics, we really don't
   *  want to destroy it here!
   */

  /*
   * Get rid of our current cvit iterator
   */
  pdb_iterator_destroy(pdb, &vr->vr_cvit);
  pdb_iterator_destroy(pdb, &vr->vr_internal_bin);

  if (vr->vr_internal_and) {
    err = pdb_iterator_reset(pdb, vr->vr_internal_and);
    if (err) {
      cl_log_errno(
          pdb_log(pdb), CL_LEVEL_ERROR, "pdb_iterator_reset", err,
          "%s: can't reset",
          pdb_iterator_to_string(pdb, vr->vr_internal_and, buf, sizeof(buf)));
      return err;
    }
  }
  vr->vr_internal_bin = NULL;
  vr->vr_cvit = NULL;
  vr->vr_eof = false;

  return err;
}

static graphd_value_range *vrange_alloc(cm_handle *cm, size_t state_size,
                                        char const *lo_s, char const *lo_e,
                                        char const *hi_s, char const *hi_e) {
  graphd_value_range *vr;
  size_t lo_need, hi_need;

  lo_need = lo_s == NULL ? 0 : (lo_e - lo_s) + 1;
  hi_need = hi_s == NULL ? 0 : (hi_e - hi_s) + 1;

  vr = cm_malloc(cm, sizeof(*vr) + state_size + lo_need + hi_need);
  if (vr == NULL) return NULL;
  memset(vr, 0, sizeof(*vr) + state_size);

  if (lo_need > 0) {
    vr->vr_lo_s = memcpy((char *)(vr + 1) + state_size, lo_s, lo_need - 1);
    vr->vr_lo_e = vr->vr_lo_s + lo_need - 1;
    *(char *)vr->vr_lo_e = '\0';
  } else {
    vr->vr_lo_s = NULL;
    vr->vr_lo_e = NULL;
  }

  if (hi_need > 0) {
    vr->vr_hi_s =
        memcpy((char *)(vr + 1) + state_size + lo_need, hi_s, hi_need - 1);
    vr->vr_hi_e = vr->vr_hi_s + hi_need - 1;
    *(char *)vr->vr_hi_e = '\0';
  } else {
    vr->vr_hi_s = NULL;
    vr->vr_hi_e = NULL;
  }
  return vr;
}

static int graphd_iterator_vrange_clone(pdb_handle *pdb, pdb_iterator *it,
                                        pdb_iterator **it_out) {
  int err;
  graphd_value_range *vr = it->it_theory;
  cm_handle *cm = pdb_mem(pdb);
  cl_handle *cl = graphd_request_cl(vr->vr_greq);
  graphd_value_range *new_rit;
  char buf[200];

  pdb_iterator *new_cvit = NULL;
  pdb_iterator *new_internal_and = NULL;
  pdb_iterator *new_internal_bin = NULL;

  cl_log(cl, CL_LEVEL_SPEW, "graphd_iterator_vrange_clone[%p]: %s", it,
         pdb_iterator_to_string(pdb, it, buf, sizeof buf));

  vr = it->it_theory;
  RIT_MAGIC_CHECK(pdb_log(pdb), vr);

  /*
   * Make a new graphd_value_range with enough room for our stuff and the
   * comparator state.
   */
  new_rit = vrange_alloc(cm, vr->vr_cmp_state_size, vr->vr_lo_s, vr->vr_lo_e,
                         vr->vr_hi_s, vr->vr_hi_e);
  if (new_rit == NULL) return ENOMEM;

  /*
   * Clone the current subiterator if we have one
   */
  if (vr->vr_cvit) {
    err = pdb_iterator_clone(pdb, vr->vr_cvit, &new_cvit);
    if (err) {
      char buf1[200];
      char buf2[200];
      cl_log_errno(
          pdb_log(pdb), CL_LEVEL_ERROR, "pdb_iterator_clone", err,
          "Can't clone cvit %s under %s",
          pdb_iterator_to_string(pdb, it, buf1, sizeof(buf1)),
          pdb_iterator_to_string(pdb, vr->vr_cvit, buf2, sizeof(buf2)));
      cm_free(cm, new_rit);
      return err;
    }
  } else
    new_cvit = NULL;

  if (vr->vr_internal_bin) {
    err = pdb_iterator_clone(pdb, vr->vr_internal_bin, &new_internal_bin);

    if (err) {
      char buf1[200];
      char buf2[200];
      cl_log_errno(
          pdb_log(pdb), CL_LEVEL_ERROR, "pdb_iterator_clone", err,
          "Can't clone internal_bin %s under %s",
          pdb_iterator_to_string(pdb, vr->vr_internal_bin, buf1, sizeof(buf1)),
          pdb_iterator_to_string(pdb, vr->vr_cvit, buf2, sizeof(buf2)));

      pdb_iterator_destroy(pdb, &new_cvit);
      cm_free(cm, new_rit);
      return err;
    }
  } else
    new_internal_bin = NULL;

  if (vr->vr_internal_and) {
    err = pdb_iterator_clone(pdb, vr->vr_internal_and, &new_internal_and);

    if (err) {
      char buf1[200];
      char buf2[200];
      cl_log_errno(
          pdb_log(pdb), CL_LEVEL_ERROR, "pdb_iterator_clone", err,
          "Can't clone internal_and %s under %s",
          pdb_iterator_to_string(pdb, vr->vr_internal_and, buf1, sizeof(buf1)),
          pdb_iterator_to_string(pdb, vr->vr_cvit, buf2, sizeof(buf2)));

      pdb_iterator_destroy(pdb, &new_cvit);
      pdb_iterator_destroy(pdb, &new_internal_bin);
      cm_free(cm, new_rit);
      return err;
    }
  } else
    new_internal_and = NULL;

  /*
   * Make our it clone
   */
  err = pdb_iterator_make_clone(pdb, it->it_original, it_out);
  if (err) {
    pdb_iterator_destroy(pdb, &new_cvit);
    pdb_iterator_destroy(pdb, &new_internal_and);
    pdb_iterator_destroy(pdb, &new_internal_bin);

    cm_free(cm, new_rit);
    return err;
  }

  /*
   * Copy all of the interesting fields in graphd_value_range
   */
  new_rit->vr_magic = RIT_MAGIC;

  /*
   * Clone keeps the string pointers of the original.
   * We know those won't go away until the original is destroyed which
   * won't happen until every clone is destroyed
   */
  new_rit->vr_cmp = vr->vr_cmp;
  new_rit->vr_last_id_out = vr->vr_last_id_out;
  new_rit->vr_cvit_last_id_out = vr->vr_cvit_last_id_out;
  new_rit->vr_cmp_state_size = vr->vr_cmp_state_size;
  new_rit->vr_eof = vr->vr_eof;
  new_rit->vr_greq = vr->vr_greq;
  new_rit->vr_valueforward = vr->vr_valueforward;

  new_rit->vr_internal_and = new_internal_and;
  new_rit->vr_internal_bin = new_internal_bin;

  /*
   * Copy the comparator_next_it state
   */
  memcpy(new_rit + 1, vr + 1, vr->vr_cmp_state_size);

  new_rit->vr_cvit = new_cvit;
  (*it_out)->it_theory = new_rit;

  /*
   * If the original didn't have a position, reset ourselves.
   */
  if (!pdb_iterator_has_position(pdb, it)) {
    err = pdb_iterator_reset(pdb, *it_out);
    if (err) {
      char buf[200];
      cl_log_errno(cl, CL_LEVEL_ERROR, "pdb_iterator_reset", err,
                   "Can't reset %s",
                   pdb_iterator_to_string(pdb, it, buf, sizeof buf));
      pdb_iterator_destroy(pdb, it_out);
      return err;
    }
  }

  return 0;
}

static void graphd_iterator_vrange_finish(pdb_handle *pdb, pdb_iterator *it) {
  graphd_value_range *vr = it->it_theory;
  cm_handle *cm = pdb_mem(pdb);

  vr = it->it_theory;

  RIT_MAGIC_CHECK(pdb_log(pdb), vr);

  pdb_iterator_destroy(pdb, &vr->vr_internal_and);
  pdb_iterator_destroy(pdb, &vr->vr_cvit);
  pdb_iterator_destroy(pdb, &vr->vr_internal_bin);

  /*
   * vr_lo and vr_hi are only kept by the original
   */
  if (it == it->it_original) {
    cl_assert(pdb_log(pdb), it->it_clones == 0);

    vr->vr_lo_s = NULL;
    vr->vr_hi_s = NULL;
  }
  it->it_magic = 0;
  vr->vr_magic = 0;
  it->it_type = NULL;
  it->it_original = NULL;

  cm_free(cm, it->it_displayname);
  cm_free(cm, vr);

  it->it_theory = NULL;
}

/*
 * try to combine vr->vr_internal_bin and vr->vr_internal_and into
 * vr->vr_cvit.
 *
 * If thats impossible, just clone vr->vr_internal_bin into vr_cvit
 */
static int vrange_construct_cvit(pdb_handle *pdb, pdb_iterator *it,
                                 graphd_value_range *vr,
                                 pdb_budget *budget_inout) {
  cl_handle *cl = graphd_request_cl(vr->vr_greq);
  graphd_handle *graphd = graphd_request_graphd(vr->vr_greq);
  int err;
  char ebuf1[200];
  char ebuf2[200];
  pdb_iterator *and_it, *sub_it;

  and_it = sub_it = NULL;

  /*  Try to intersect the iterator we just got
   *  with a sorted set that was handed to us at create time.
   *  That set is probably a VIP.
   */
  if (vr->vr_internal_and == NULL) {
    err = pdb_iterator_clone(pdb, vr->vr_internal_bin, &vr->vr_cvit);
    if (err != 0)
      cl_log_errno(cl, CL_LEVEL_ERROR, "pdb_iterator_clone", err,
                   "Can't clone iterator %s",
                   pdb_iterator_to_string(pdb, vr->vr_internal_bin, ebuf1,
                                          sizeof ebuf1));
    else
      cl_assert(cl, vr->vr_cvit != NULL);

    return err;
  }

  /*  Create an "and" iterator.
   */
  err = graphd_iterator_and_create(vr->vr_greq, 2, it->it_low, it->it_high,
                                   GRAPHD_DIRECTION_FORWARD, NULL, &and_it);
  if (err != 0) {
    cl_log_errno(
        cl, CL_LEVEL_ERROR, "pdb_iterator_clone", err,
        "Can't clone iterator %s",
        pdb_iterator_to_string(pdb, vr->vr_internal_bin, ebuf1, sizeof ebuf1));
    goto err;
  }

  /*  Clone the bin iterator and add it to the "and".
   */
  err = pdb_iterator_clone(pdb, vr->vr_internal_bin, &sub_it);
  if (err != 0) {
    cl_log_errno(
        cl, CL_LEVEL_ERROR, "pdb_iterator_clone", err,
        "Can't clone iterator %s",
        pdb_iterator_to_string(pdb, vr->vr_internal_bin, ebuf1, sizeof ebuf1));
    goto err;
  }
  err = graphd_iterator_and_add_subcondition(graphd, and_it, &sub_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_iterator_and_add_subcondition",
                 err, "and_it=%s, sub_it=%s",
                 pdb_iterator_to_string(pdb, and_it, ebuf1, sizeof ebuf1),
                 pdb_iterator_to_string(pdb, sub_it, ebuf1, sizeof ebuf2));
    goto err;
  }
  pdb_iterator_destroy(pdb, &sub_it);

  /*  Clone the other filter and add it to the "and".
   */
  err = pdb_iterator_clone(pdb, vr->vr_internal_and, &sub_it);
  if (err != 0) {
    cl_log_errno(
        cl, CL_LEVEL_ERROR, "pdb_iterator_clone", err, "it=%s",
        pdb_iterator_to_string(pdb, vr->vr_internal_and, ebuf1, sizeof ebuf1));
    goto err;
  }
  err = graphd_iterator_and_add_subcondition(graphd, and_it, &sub_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "grapdh_iterator_and_add_subcondition",
                 err, "sub=%s, and=%s",
                 pdb_iterator_to_string(pdb, and_it, ebuf1, sizeof ebuf1),
                 pdb_iterator_to_string(pdb, sub_it, ebuf2, sizeof ebuf2));
    goto err;
  }
  pdb_iterator_destroy(pdb, &sub_it);

  /*  Finish building the "and".
   */
  err = graphd_iterator_and_create_commit(graphd, and_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "grapdh_iterator_and_add_subcondition",
                 err, "sub=%s, and=%s",
                 pdb_iterator_to_string(pdb, and_it, ebuf1, sizeof ebuf1),
                 pdb_iterator_to_string(pdb, sub_it, ebuf2, sizeof ebuf2));
    goto err;
  }
  vr->vr_cvit = and_it;
  return 0;

err:
  pdb_iterator_destroy(pdb, &sub_it);
  pdb_iterator_destroy(pdb, &and_it);

  return err;
}

/*
 * Advance the comparator bin sequence and calculate a new binned iterator
 * in vr->vr_cvit.
 *
 */
static int vrange_get_cvit(pdb_handle *pdb, pdb_iterator *it,
                           graphd_value_range *vr, pdb_budget *budget_inout) {
  char buf[200];
  int err;
  pdb_iterator *new_cvit;
  cl_handle *cl = pdb_log(pdb);

  vr->vr_cvit_last_id_out = PDB_ID_NONE;

  cl_assert(cl, vr->vr_cvit == NULL);
  cl_assert(cl, vr->vr_internal_bin == NULL);

  /*
   * Get a new iterator from our comparator function
   */
  err = vr->vr_cmp->cmp_vrange_it_next(vr->vr_greq, vr, vr + 1, it->it_low,
                                       it->it_high, &new_cvit, budget_inout);

  if (err == PDB_ERR_MORE) return PDB_ERR_MORE;

  /*
   * We're actually at the end of this thing.
   */
  else if (err == GRAPHD_ERR_NO) {
    cl_log(cl, CL_LEVEL_SPEW,
           "vrange_get_cvit: [%s] cmp_vrange_it_next returns"
           " GRAPHD_ERR_NO",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf));

    vr->vr_cvit_last_id_out = PDB_ID_NONE;
    vr->vr_eof = true;

    pdb_iterator_destroy(pdb, &vr->vr_cvit);

    return GRAPHD_ERR_NO;
  } else if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "cmp_vrange_it_next", err, "it=%s",
                 pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    return err;
  }

  /*
   * We need to track this special for freeze/thaw because
   * vr_cvit may be overridden with an intersection iterator,
   * below.
   */
  vr->vr_internal_bin = new_cvit;

  err = vrange_construct_cvit(pdb, it, vr, budget_inout);
  if (err)
    pdb_iterator_destroy(pdb, &vr->vr_internal_bin);
  else
    cl_assert(cl, vr->vr_cvit != NULL);
  return err;
}

/*
 * Check to see if an ID matches this value range by looking up the
 * primitive ahd using the comparators compare function
 */
static int vrange_check_value(pdb_handle *pdb, pdb_iterator *it, pdb_id id,
                              pdb_budget *budget_inout) {
  graphd_value_range *vr = it->it_theory;
  graphd_value_range *orit = it->it_original->it_theory;
  cl_handle *cl = graphd_request_cl(vr->vr_greq);
  pdb_primitive pr;
  int err, res1, res2 = -2;
  const char *str_s, *str_e;

  RIT_MAGIC_CHECK(cl, vr);
  RIT_MAGIC_CHECK(cl, orit);

  *budget_inout -= PDB_COST_PRIMITIVE;

  err = pdb_id_read(pdb, id, &pr);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "pdb_id_read", err,
                 "Unable to read primitive: %llx", (unsigned long long)id);
    return err;
  }

  if (pdb_primitive_value_get_size(&pr) == 0) {
    str_s = str_e = NULL;
  } else {
    str_s = pdb_primitive_value_get_memory(&pr);
    str_e = str_s + pdb_primitive_value_get_size(&pr) - 1;
  }

  res1 = vr->vr_cmp->cmp_sort_compare(vr->vr_greq, str_s, str_e, orit->vr_lo_s,
                                      orit->vr_lo_e);
  if (res1 >= (vr->vr_lo_strict ? 1 : 0)) {
    res2 = vr->vr_cmp->cmp_sort_compare(vr->vr_greq, orit->vr_hi_s,
                                        orit->vr_hi_e, str_s, str_e);
    if (res2 >= (vr->vr_hi_strict ? 1 : 0)) {
      cl_log(cl, CL_LEVEL_ULTRA, "vrange_check_value '%.*s': [%llx] in range",
             (int)(str_e - str_s), str_s, (unsigned long long)id);
      pdb_primitive_finish(pdb, &pr);
      return 0;
    }
  }
  cl_log(cl, CL_LEVEL_ULTRA,
         "vrange_check_value '%.*s': [%llx] OUTSIDE range '%.*s'-'%.*s' (res1: "
         "%d, res2: %d)",
         (int)(str_e - str_s), str_s, (unsigned long long)id,
         (int)(vr->vr_lo_e - vr->vr_lo_s), vr->vr_lo_s,
         (int)(vr->vr_hi_e - vr->vr_hi_s), vr->vr_hi_s, res1, res2);
  pdb_primitive_finish(pdb, &pr);
  return GRAPHD_ERR_NO;
}

/*
 * Check a primitive against both the value range and any internal
 * constraints
 */
static int graphd_iterator_vrange_check(pdb_handle *pdb, pdb_iterator *it,
                                        pdb_id id, pdb_budget *budget_inout) {
  graphd_value_range *vr;
  int err;
  pdb_budget budget_in = *budget_inout;

  vr = it->it_theory;
  RIT_MAGIC_CHECK(pdb_log(pdb), vr);

  if (vr->vr_internal_and == NULL)
    err = vrange_check_value(pdb, it, id, budget_inout);

  /*  Do the cheaper of the two checks first.
   */
  else if (pdb_iterator_check_cost(pdb, vr->vr_internal_and) >
           PDB_COST_PRIMITIVE) {
    err = vrange_check_value(pdb, it, id, budget_inout);
    if (err == 0)
      err = pdb_iterator_check(pdb, vr->vr_internal_and, id, budget_inout);
  } else {
    err = pdb_iterator_check(pdb, vr->vr_internal_and, id, budget_inout);
    if (err == 0) err = vrange_check_value(pdb, it, id, budget_inout);
  }
  pdb_rxs_log(
      pdb, "CHECK %p vrange %llx: %s ($%lld)", (void *)it,
      (unsigned long long)id,
      err == GRAPHD_ERR_NO ? "no" : err == 0 ? "yes" : graphd_strerror(err),
      budget_in - *budget_inout);

  pdb_iterator_account_charge_budget(pdb, it, check);
  return err;
}

static int graphd_iterator_vrange_next_loc(pdb_handle *pdb, pdb_iterator *it,
                                           pdb_id *pdb_id_out,
                                           pdb_budget *budget_inout,
                                           const char *file, int line) {
  graphd_value_range *vr = it->it_theory;
  cl_handle *cl = graphd_request_cl(vr->vr_greq);
  pdb_budget budget_in = *budget_inout;
  pdb_id old;
  int err;

  vr = it->it_theory;
  RIT_MAGIC_CHECK(cl, vr);
  old = *pdb_id_out;

  if (vr->vr_eof) return GRAPHD_ERR_NO;

  pdb_rxs_push(pdb, "NEXT %p vrange", (void *)it);

  /*
   * Keep trying iterator from vrange_it_next until we finally get an
   * iterator that returns a value OR vrange_it_next tells us that we're
   * actually out of data.
   */

  err = 0;
  *pdb_id_out = PDB_ID_NONE;

  do {
    /*  If we don't have a per-bin iterator,
     *  make one.
     */
    if (vr->vr_cvit == NULL || err == GRAPHD_ERR_NO) {
      if (vr->vr_cvit) pdb_iterator_destroy(pdb, &vr->vr_cvit);

      if (vr->vr_internal_bin) pdb_iterator_destroy(pdb, &vr->vr_internal_bin);

      err = vrange_get_cvit(pdb, it, vr, budget_inout);
      if (err != 0) goto err;
    }

    /*
     * Get another value from the per-bin iterator.
     */
    cl_assert(cl, vr->vr_cvit != NULL);
    err = pdb_iterator_next(pdb, vr->vr_cvit, pdb_id_out, budget_inout);

    if (err == GRAPHD_ERR_NO) {
      pdb_iterator_destroy(pdb, &vr->vr_cvit);
      pdb_iterator_destroy(pdb, &vr->vr_internal_bin);

      if (*budget_inout <= 0) {
        err = PDB_ERR_MORE;
        goto err;
      }
    } else if (err != 0)
      goto err;

  } while (err == GRAPHD_ERR_NO);

  cl_assert(cl, err == 0);
  cl_assert(cl, *pdb_id_out != PDB_ID_NONE);

  vr->vr_last_id_out = *pdb_id_out;
  vr->vr_cvit_last_id_out = *pdb_id_out;

  pdb_rxs_pop(pdb, "NEXT %p vrange %llx ($%lld)", (void *)it,
              (unsigned long long)*pdb_id_out,
              (long long)(budget_in - *budget_inout));
  pdb_iterator_account_charge_budget(pdb, it, next);
  return 0;

err:
  if (err == PDB_ERR_MORE)
    pdb_rxs_pop(pdb, "NEXT %p vrange suspend ($%lld)", (void *)it,
                (long long)(budget_in - *budget_inout));
  else if (err == GRAPHD_ERR_NO)
    pdb_rxs_pop(pdb, "NEXT %p vrange done ($%lld)", (void *)it,
                (long long)(budget_in - *budget_inout));
  else
    pdb_rxs_pop(pdb, "NEXT %p vrange error: %s ($%lld)", (void *)it,
                graphd_strerror(err), (long long)(budget_in - *budget_inout));

  pdb_iterator_account_charge_budget(pdb, it, next);
  return err;
}

/*
 * Create a new ranged value iterator
 */
int graphd_iterator_vrange_create(
    graphd_request *greq, const char *lo_s, const char *lo_e, bool lo_strict,
    const char *hi_s, const char *hi_e, bool hi_strict, unsigned long long low,
    unsigned long long high, bool value_forward,
    const graphd_comparator *cmp_type, const char *ordering,
    pdb_iterator *internal_and, pdb_iterator **it_out)

{
  cl_handle *cl = graphd_request_cl(greq);
  pdb_handle *pdb = graphd_request_graphd(greq)->g_pdb;
  cm_handle *cm = pdb_mem(pdb);
  int err;
  graphd_value_range *vr;
  pdb_iterator *it;
  size_t state_size;

  cl_assert(cl, cmp_type);

  if (!(cmp_type->cmp_vrange_size && cmp_type->cmp_vrange_start &&
        cmp_type->cmp_vrange_it_next && cmp_type->cmp_vrange_statistics &&
        cmp_type->cmp_vrange_seek && cmp_type->cmp_vrange_thaw &&
        cmp_type->cmp_vrange_freeze)) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_vrange_create: "
           "%s fails to define at least one vrange function. "
           " can't use binning for this inequality",
           cmp_type->cmp_name);

    return ENOTSUP;
  }

  cl_log(
      cl, CL_LEVEL_DEBUG,
      "graphd_iterator_vrange_create: value range '%.*s' to '%.*s' with cmp=%s",
      (int)(lo_e - lo_s), lo_s, (int)(hi_e - hi_s), hi_s, cmp_type->cmp_name);

  if (high > pdb_primitive_n(pdb)) {
    high = pdb_primitive_n(pdb);
  }

  if (internal_and) {
    if (pdb_iterator_all_is_instance(pdb, internal_and) ||
        !pdb_iterator_sorted(pdb, internal_and) ||
        !pdb_iterator_statistics_done(pdb, internal_and)) {
      char buf[200];

      pdb_iterator_destroy(pdb, &internal_and);
      internal_and = NULL;

      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_vrange_create: %s is not"
             " useful",
             pdb_iterator_to_string(pdb, internal_and, buf, sizeof buf));
    }
  }
  /*
   * the it_theory pointer holds and graphd_value_range and an arbitrary amount
   * of comparator specific data glued to the end of the vr
   */

  state_size = cmp_type->cmp_vrange_size(greq, lo_s, lo_e, hi_s, hi_e);
  vr = vrange_alloc(cm, state_size, lo_s, lo_e, hi_s, hi_e);
  if (!vr) {
    return ENOMEM;
  }
  it = cm_malloc(cm, sizeof *it);
  if (!it) {
    cm_free(cm, vr);
    return ENOMEM;
  }
  pdb_iterator_make(pdb, it, low, high, true);

  /*
   * Setup our iterator and iterator theory
   */
  it->it_type = &graphd_iterator_vrange;
  it->it_theory = vr;

  vr->vr_cmp_state_size = state_size;
  vr->vr_cmp = cmp_type;
  vr->vr_lo_strict = lo_strict;
  vr->vr_hi_strict = hi_strict;

  vr->vr_magic = RIT_MAGIC;

  vr->vr_last_id_out = PDB_ID_NONE;
  vr->vr_cvit_last_id_out = PDB_ID_NONE;
  vr->vr_cvit = NULL;
  vr->vr_greq = greq;
  vr->vr_valueforward = value_forward;
  vr->vr_internal_and = internal_and;
  vr->vr_internal_bin = NULL;

  *it_out = it;

  if (ordering != NULL && (cmp_type->cmp_value_in_range != NULL)) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_vrange_create: ordered according to \"%s\"",
           ordering);
    pdb_iterator_ordering_set(pdb, it, ordering);
    pdb_iterator_ordered_set(pdb, it, true);
  } else {
    cl_log(cl, CL_LEVEL_VERBOSE, "graphd_iterator_vrange_create: not ordered.");
    pdb_iterator_ordered_set(pdb, it, false);
  }

  err = vr->vr_cmp->cmp_vrange_start(vr->vr_greq, vr, vr + 1);

  if (err == GRAPHD_ERR_NO) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_vrange_create: cmp_vrange_start "
           "claims no IDs in set. Returning a NULL iterator");

    /*
     * cmp_vrange_start told us that it won't return anything.
     */
    pdb_iterator_destroy(pdb, it_out);
    return pdb_iterator_null_create(pdb, it_out);

  } else if (err == ENOTSUP) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "vrange start for comparator %s can't index '%.*s' - '%.*s'",
           graphd_comparator_to_string(cmp_type), (int)(lo_e - lo_s), lo_s,
           (int)(hi_e - hi_s), hi_s);
    pdb_iterator_destroy(pdb, it_out);
    return ENOTSUP;
  }

  else if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "pdb_iterator_vrange_start", err,
                 "Can't prepare vrange iterator");
    pdb_iterator_destroy(pdb, it_out);

    return err;
  }

  vr->vr_cvit = NULL;
  vr->vr_eof = false;
  it->it_has_position = true;

  return 0;
}

/*
 * Thaw the set of a vrange iterator.
 * After calling this function *it_out should be an unpositioned vrange
 * iterator that has the same set and ordering as the original
 */
static int vrange_thaw_set(pdb_handle *pdb, graphd_request *greq,
                           pdb_iterator_base *pib, const char *s, const char *e,
                           cl_loglevel loglevel, pdb_iterator **it_out)

{
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = pdb_mem(pdb);
  unsigned long long low, high;
  const char *cmp_s, *cmp_e;
  const char *lo_s, *lo_e;
  const char *hi_s, *hi_e;
  pdb_iterator *it, *and_subit;
  const char *decoded_lo_s, *decoded_lo_e;
  cm_buffer decoded_lo, decoded_hi;
  const char *ordering;
  bool valueforward;
  const graphd_comparator *cmp;
  int err;
  pdb_iterator_account *acc = NULL;

  cl_log(cl, CL_LEVEL_SPEW, "vrange_thaw_set: '%.*s'", (int)(e - s), s);
  err = pdb_iterator_util_thaw(
      pdb, &s, e, "%{low[-high]}:%{bytes}:%{bytes}-%{bytes}:%{forward}:", &low,
      &high, &cmp_s, &cmp_e, &lo_s, &lo_e, &hi_s, &hi_e, &valueforward);
  if (err) {
    cl_log(cl, loglevel, "vrange_thaw_set: cannot parse: '%.*s'", (int)(e - s),
           s);
    return GRAPHD_ERR_LEXICAL;
  }

  cmp = graphd_comparator_from_string(cmp_s, cmp_e);
  if (!cmp) {
    cl_log(cl, loglevel,
           "graphd_iterator_vrange_reconstruct: no comparator "
           "named: '%.*s'",
           (int)(cmp_e - cmp_s), cmp_s);
    return GRAPHD_ERR_LEXICAL;
  }

  and_subit = NULL;
  cm_buffer_initialize(&decoded_lo, cm);
  cm_buffer_initialize(&decoded_hi, cm);

  /*
   * * in the lo position means "empty string"
   */
  if (lo_s != NULL && lo_e == lo_s + 1 && *lo_s == '*') {
    decoded_lo_s = "";
    decoded_lo_e = decoded_lo_s;
  } else {
    err = pdb_xx_decode(pdb, lo_s, lo_e, &decoded_lo);
    if (err) {
      cl_log_errno(cl, loglevel, "pdb_xx_decode", err,
                   "Can't decode lo value from '%.*s'", (int)(lo_e - lo_s),
                   lo_s);
      goto err;
    }
    decoded_lo_s = cm_buffer_memory(&decoded_lo);
    decoded_lo_e = cm_buffer_memory_end(&decoded_lo);
  }

  err = pdb_xx_decode(pdb, hi_s, hi_e, &decoded_hi);
  if (err) {
    cl_log_errno(cl, loglevel, "pdb_xx_decode", err,
                 "Can't decode hi value from '%.*s'", (int)(hi_e - hi_s), hi_s);
    goto err;
  }
  if (e - s >= 2 && s[0] == '(' && s[1] == ')') {
    and_subit = NULL;
    cl_log(cl, CL_LEVEL_SPEW, "vrange_thaw_set: null suband");
    s += 2;
  } else {
    err = graphd_iterator_util_thaw_subiterator(graphd_request_graphd(greq), &s,
                                                e, pib, loglevel, &and_subit);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "graphd_iterator_util_thaw_subiterator", err,
                   "can't parse subiterator '%.*s", (int)(e - s), s);
      goto err;
    }
  }

  err = pdb_iterator_util_thaw(
      pdb, &s, e, ":%{ordering}%{account}%{extensions}", pib, &ordering, pib,
      &acc, (pdb_iterator_property *)NULL);
  if (err != 0) {
    cl_log_errno(cl, loglevel, "graphd_iterator_vrange_create", err,
                 "Can't parse optional ordering in '%.*s", (int)(e - s), s);
    goto err;
  }

  err = graphd_iterator_vrange_create(
      greq, decoded_lo_s, decoded_lo_e, false, cm_buffer_memory(&decoded_hi),
      cm_buffer_memory_end(&decoded_hi), false, low, high, valueforward, cmp,
      ordering, and_subit, &it);

  if (err) {
    char buf[200];
    cl_log_errno(
        cl, loglevel, "graphd_iterator_vrange_create", err,
        "Can't make vrange iterator: (%.*s):'%.*s' - '%.*s'"
        " from %llx to %llx with subiterator %s",
        (int)(cmp_e - cmp_s), cmp_s, (int)(decoded_lo_e - decoded_lo_s),
        decoded_lo_s, (int)cm_buffer_length(&decoded_hi),
        cm_buffer_memory(&decoded_hi), low, high,
        and_subit ? pdb_iterator_to_string(pdb, and_subit, buf, sizeof buf)
                  : "null");
    goto err;
  }

  pdb_iterator_account_set(pdb, it, acc);

  cm_buffer_finish(&decoded_lo);
  cm_buffer_finish(&decoded_hi);

  *it_out = it;
  return 0;

err:
  cm_buffer_finish(&decoded_lo);
  cm_buffer_finish(&decoded_hi);

  pdb_iterator_destroy(pdb, &and_subit);
  return err;
}

/*
 * Thaw the state for a vrange iterator.
 * This uses the cursor state text and the vr_cvit_last_id_out and
 * vr_last_id_out fields to reconstruct vr_cvit and vr_internal_bin.
 * After calling this function, it should be the same as before it was
 * frozen.
 */
static int vrange_thaw_state(pdb_handle *pdb, graphd_request *greq,
                             const char *s, const char *e,
                             pdb_iterator_base *pib, cl_loglevel loglevel,
                             pdb_iterator *it) {
  unsigned long long stats_next_cost;
  unsigned long long stats_n;
  const char *cmp_state_s, *cmp_state_e;
  cl_handle *cl = graphd_request_cl(greq);
  int err;
  graphd_value_range *vr;
  pdb_id id;
  char buf[200];

  vr = it->it_theory;
  RIT_MAGIC_CHECK(cl, vr);

  cl_log(cl, CL_LEVEL_VERBOSE, "pdb_iterator_thaw_state: '%.*s'", (int)(e - s),
         s);

  err = pdb_iterator_util_thaw(pdb, &s, e, "%llu:%llu:%llu:%{(bytes)}:",
                               &vr->vr_cvit_last_id_out, &stats_n,
                               &stats_next_cost, &cmp_state_s, &cmp_state_e);

  if (err) {
    cl_log(cl, loglevel, "unable to extract data from %.*s", (int)(e - s), s);
    return GRAPHD_ERR_LEXICAL;
  }

  if (s == e) {
    vr->vr_internal_bin = NULL;

    cl_log(cl, CL_LEVEL_SPEW, "vrange_thaw_state: not in a bin yet");
  } else {
    char buf[200];
    err = graphd_iterator_util_thaw_subiterator(graphd_request_graphd(greq), &s,
                                                e, pib, loglevel,
                                                &(vr->vr_internal_bin));

    if (err) {
      cl_log(cl, loglevel, "Can't extract iterator from %.*s", (int)(e - s), s);
      return GRAPHD_ERR_LEXICAL;
    }
    cl_log(cl, CL_LEVEL_SPEW, "vrange_thaw_state: current bin is: %s",
           pdb_iterator_to_string(pdb, vr->vr_internal_bin, buf, sizeof buf));
  }

  err = vr->vr_cmp->cmp_vrange_thaw(greq, vr, vr + 1, cmp_state_s, cmp_state_e);

  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "cmp_vrange_thaw", err,
                 "Can't thaw a vlid vrange state out of '%.*s'",
                 (int)(cmp_state_e - cmp_state_s), cmp_state_s);

    pdb_iterator_destroy(pdb, &(vr->vr_internal_bin));
    return GRAPHD_ERR_LEXICAL;
  }

  if (stats_n != (unsigned long long)-1) {
    pdb_iterator_next_cost_set(pdb, it, stats_next_cost);
    pdb_iterator_n_set(pdb, it, stats_n);
    pdb_iterator_check_cost_set(pdb, it, PDB_COST_PRIMITIVE);
    pdb_iterator_sorted_set(pdb, it, false);
    pdb_iterator_statistics_done_set(pdb, it);
  }
  if (!vr->vr_internal_bin) {
    vr->vr_cvit = NULL;
    return 0;
  }

  /*
   * We have a current bin. Reconstruct cvit by intersecting it
   * with our AND if and fast-forward to the right place.
   */
  err = vrange_construct_cvit(pdb, it, vr, NULL);
  if (err) {
    char buf1[200];
    char buf2[200];
    cl_log_errno(
        cl, loglevel, "vrange_construct_cvit", err,
        "unable to reconstruct cvit for "
        "%s vs %s",
        pdb_iterator_to_string(pdb, vr->vr_internal_and, buf1, sizeof buf1),
        pdb_iterator_to_string(pdb, vr->vr_internal_bin, buf2, sizeof buf2));

    return err;
  }

  id = vr->vr_cvit_last_id_out;

  /*
   * Did we get anything out of this bin yet?
   */
  if (id != PDB_ID_NONE) {
    do {
      pdb_budget budget;
      budget = 1000000;
      err = pdb_iterator_statistics(pdb, vr->vr_cvit, &budget);
    } while (err == GRAPHD_ERR_MORE);

    if (err) {
      char buf[200];

      cl_log_errno(cl, CL_LEVEL_ERROR, "pdb_iterator_statistics", err,
                   "Unexpected error while gathering statistics for %s",
                   pdb_iterator_to_string(pdb, vr->vr_cvit, buf, sizeof buf));

      pdb_iterator_destroy(pdb, &vr->vr_cvit);
      return err;
    }

    err = pdb_iterator_find_nonstep(pdb, vr->vr_cvit, id, &id);

    if (err && err != GRAPHD_ERR_NO) {
      cl_log_errno(cl, loglevel, "pdb_iterator_find_nonstep", err,
                   "Can't find %llx in %s",
                   (unsigned long long)vr->vr_last_id_out,
                   pdb_iterator_to_string(pdb, vr->vr_cvit, buf, sizeof buf));

      pdb_iterator_destroy(pdb, &it);
      return err;
    }
  } else {
    err = pdb_iterator_reset(pdb, vr->vr_cvit);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_ERROR, "pdb_iterator_reset", err,
                   "can't reset %s",
                   pdb_iterator_to_string(pdb, vr->vr_cvit, buf, sizeof buf));
      return err;
    }
  }
  return 0;
}

/*
 * Reconstruct the vrange state using ONLY the vr->vr_last_id_out field.
 * It is possible for this function to skep backwards over empty bins or
 * otherwise loose state (although not so much as to return the same ID twice).
 */
static int vrange_recreate_state(pdb_handle *pdb, graphd_request *greq,
                                 pdb_iterator *it) {
  graphd_value_range *vr = it->it_theory;
  cl_handle *cl = graphd_request_cl(greq);
  char buf[200];
  pdb_primitive pr;
  pdb_id id, id_found;
  int err;

  RIT_MAGIC_CHECK(cl, vr);
  id = vr->vr_last_id_out;

  cl_log(cl, CL_LEVEL_VERBOSE,
         "vrange_recreate_state: No state for this vrange iterator"
         " recreating from position %llx",
         (unsigned long long)id);
  do {
    pdb_budget budget = 100000;
    err = graphd_iterator_vrange_statistics(pdb, it, &budget);

  } while (err == PDB_ERR_MORE);

  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_iterator_vrange_statistics", err,
                 "unexpected error redoing statistics for %s",
                 pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    return err;
  }

  if (id == PDB_ID_NONE) {
    /* We didn't pull anything out of this vrange yet.
     */
    err = pdb_iterator_reset(pdb, it);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_ERROR, "pdb_iterator_reset", err,
                   "cannot reset %s",
                   pdb_iterator_to_string(pdb, it, buf, sizeof buf));
      return err;
    }
    return 0;
  }

  /*
   * Grab the last primitive we could look at and ask the comparator
   * to calculate the bin we should be in.
   */
  err = pdb_id_read(pdb, id, &pr);

  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "pdb_id_read", err,
                 "Can't read primitive: %llx", (unsigned long long)id);
    return err;
  }

  if (pdb_primitive_value_get_size(&pr) == 0) {
    err = vr->vr_cmp->cmp_vrange_seek(greq, vr, vr + 1, NULL, NULL, id,
                                      it->it_low, it->it_high,
                                      &(vr->vr_internal_bin));

    if (err) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "cmp_vrange_seek", err,
                   "vrange seek for %s won't seek to "
                   " (nil) (id:%llx)",
                   vr->vr_cmp->cmp_name, (unsigned long long)id);
      pdb_primitive_finish(pdb, &pr);
      return err;
    }
    cl_assert(cl, vr->vr_internal_bin);

  } else {
    err = vr->vr_cmp->cmp_vrange_seek(
        greq, vr, vr + 1, pdb_primitive_value_get_memory(&pr),
        pdb_primitive_value_get_memory(&pr) +
            pdb_primitive_value_get_size(&pr) - 1,
        id, it->it_low, it->it_high, &(vr->vr_internal_bin));

    if (err) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "cmp_vrange_seek", err,
                   "vrange seek %s won't seek to "
                   "'%s' (id: %llx)",
                   vr->vr_cmp->cmp_name, pdb_primitive_value_get_memory(&pr),
                   (unsigned long long)id);
      pdb_primitive_finish(pdb, &pr);
      return err;
    }
  }
  pdb_primitive_finish(pdb, &pr);

  err = vrange_construct_cvit(pdb, it, vr, NULL);
  /*
   * construct_cvit should log all its errors
   */
  if (err) return err;

  do {
    pdb_budget budget;
    budget = 1000000;
    err = pdb_iterator_statistics(pdb, vr->vr_cvit, &budget);
  } while (err == GRAPHD_ERR_MORE);

  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "pdb_iterator_statistics", err,
                 "Unexpected error while gathering statistics for %s",
                 pdb_iterator_to_string(pdb, vr->vr_cvit, buf, sizeof buf));

    pdb_iterator_destroy(pdb, &vr->vr_cvit);
    return err;
  }

  /*
   * Intersections of sorted things should come back sorted
   */
  cl_assert(cl, pdb_iterator_sorted(pdb, vr->vr_cvit));

  err = pdb_iterator_find_nonstep(pdb, vr->vr_cvit, id, &id_found);

  if (err == GRAPHD_ERR_NO || id != id_found) {
    /*
     * If id isn't in cvit at this point, it means that ID came
     * from a different bin.  This is true immediatly after
     * switching to a new bin until we produce an ID from that
     * bin.
     *
     * So, we must be at the start of the bin.
     */

    err = pdb_iterator_reset(pdb, vr->vr_cvit);
    if (err) {
      char buf[200];
      cl_log_errno(cl, CL_LEVEL_ERROR, "pdb_iterator_reset", err,
                   "Can't reset %s",
                   pdb_iterator_to_string(pdb, it, buf, sizeof buf));
      pdb_iterator_destroy(pdb, &vr->vr_cvit);
      return err;
    }

  } else if (err) {
    char buf1[200];
    char buf2[200];
    cl_log_errno(cl, CL_LEVEL_ERROR, "pdb_iterator_find_nonstep", err,
                 "%s Can't find id %llx in %s",
                 pdb_iterator_to_string(pdb, it, buf1, sizeof buf1),
                 (unsigned long long)id,
                 pdb_iterator_to_string(pdb, vr->vr_cvit, buf2, sizeof buf2));

    pdb_iterator_destroy(pdb, &vr->vr_cvit);
    return err;
  }

  cl_assert(cl, !err);
  return 0;
}

int graphd_iterator_vrange_thaw(graphd_handle *g, pdb_iterator_text const *pit,
                                pdb_iterator_base *pib, cl_loglevel loglevel,
                                pdb_iterator **it_out)

{
  int err;
  char buf[200];
  pdb_iterator *it;
  pdb_id last_id_out;
  pdb_handle *pdb = g->g_pdb;
  const char *s, *e;
  bool eof;
  graphd_value_range *vr;
  graphd_request *greq;
  cl_handle *cl;

  greq = pdb_iterator_base_lookup(pdb, pib, "graphd.request");
  if (greq == NULL) {
    cl_log(pdb_log(pdb), CL_LEVEL_ERROR,
           "Can't get a greq structure for this cursor");
    return EINVAL;
  }
  cl = graphd_request_cl(greq);
  cl_log(cl, CL_LEVEL_SPEW, "graphd_iterator_vrange_thaw: '%.*s/%.*s/%.*s'",
         (int)(pit->pit_set_e - pit->pit_set_s), pit->pit_set_s,
         (int)(pit->pit_position_e - pit->pit_position_s), pit->pit_position_s,
         (int)(pit->pit_state_e - pit->pit_state_s), pit->pit_state_s);
  /*
   * Get the set
   */
  err = vrange_thaw_set(pdb, greq, pib, pit->pit_set_s, pit->pit_set_e,
                        loglevel, &it);
  if (err) return err;

  vr = it->it_theory;

  RIT_MAGIC_CHECK(cl, vr);

  /*
   * Now, get the position
   */
  s = pit->pit_position_s;
  e = pit->pit_position_e;

  if (s == NULL || s == e) {
    vr->vr_eof = false;
    vr->vr_last_id_out = PDB_ID_NONE;
  } else {
    err = pdb_iterator_util_thaw(pdb, &s, e, "%{eof/id}", &eof, &last_id_out);
    if (err) {
      cl_log(cl, loglevel,
             "graphd_iterator_vrange_thaw: "
             " Can't parse eof/integer out of %.*s",
             (int)(e - s), s);
      pdb_iterator_destroy(pdb, &it);
      return err;
    }
    cl_log(cl, CL_LEVEL_SPEW,
           "graphd_iterator_vrange_thaw: "
           " position: %.*s yields eof: %s position: %llx",
           (int)(pit->pit_position_e - pit->pit_position_s),
           pit->pit_position_s, eof ? "true" : "false",
           (unsigned long long)last_id_out);

    vr->vr_eof = eof;
    vr->vr_last_id_out = last_id_out;
  }

  if (pit->pit_state_s == pit->pit_state_e) {
    /*
     * Lost our state. recreate it
     */
    err = vrange_recreate_state(pdb, greq, it);
    if (err) {
      pdb_iterator_destroy(pdb, &it);
      return err;
    }
  } else {
    err = vrange_thaw_state(pdb, greq, pit->pit_state_s, pit->pit_state_e, pib,
                            loglevel, it);

    if (err == GRAPHD_ERR_LEXICAL) {
      /*  Probably an old version. Try to recreate the
       *  state instead.
       */
      cl_log(cl, CL_LEVEL_INFO,
             "graphd_iterator_vrange_thaw:"
             " state strings %.*s is invalid. Trying"
             " to recreate state the hard way",
             (int)(pit->pit_state_e - pit->pit_state_s), pit->pit_state_s);

      err = vrange_recreate_state(pdb, greq, it);
    }
    if (err) {
      pdb_iterator_destroy(pdb, &it);
      return err;
    }
  }

  *it_out = it;
  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_vrange_thaw: "
         "successfully remade iterator %s at %llx",
         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         (unsigned long long)vr->vr_last_id_out);

  return 0;
}

/*
 * Freeze a value range iterator. Our format looks like
 * vrange:low-high:lo_value-high_value(subiterator)/sub-position/sub-state
 *
 * format:
 * vrange:low-high:{comparator}:{low_value}-{high_value}:{value_forward}:
 *  ({internal_and}):{ordering}
 * / {last_id_out}
 * /
 * {cvit_last_id_out},{n}:{next_cost}:({internal_vrange_state}):({internal_bin})
 *
 */
static int graphd_iterator_vrange_freeze(pdb_handle *pdb, pdb_iterator *it,
                                         unsigned int flags, cm_buffer *buf) {
  int err;
  const char *sep;
  graphd_value_range *vr = it->it_theory;
  graphd_value_range *orit;
  cl_handle *cl = graphd_request_cl(vr->vr_greq);

  RIT_MAGIC_CHECK(cl, vr);

  if (it->it_original->it_id != it->it_id)
    return graphd_iterator_vrange_freeze(pdb, it->it_original, flags, buf);

  orit = it->it_original->it_theory;
  RIT_MAGIC_CHECK(cl, orit);

  sep = "";

  /*  We can't deal with vrange iterators frozen before
   *  their statistics phase.  If we haven't yet done that,
   *  do it now.
   */
  if (!pdb_iterator_statistics_done(pdb, it)) {
    err = vrange_emergency_statistics(pdb, it);
    if (err != 0) return err;
  }

  if (flags & PDB_ITERATOR_FREEZE_SET) {
    err = pdb_iterator_freeze_intro(buf, it, "vrange");
    if (err) return err;

    err = cm_buffer_sprintf(buf, ":%s:", vr->vr_cmp->cmp_name);
    if (err) return err;

    if (orit->vr_lo_s && orit->vr_lo_s == orit->vr_lo_e) {
      err = cm_buffer_add_bytes(buf, "*", 1);
    } else {
      err =
          pdb_xx_encode(pdb, orit->vr_lo_s, orit->vr_lo_e - orit->vr_lo_s, buf);
    }
    if (err) return err;

    err = cm_buffer_add_bytes(buf, "-", 1);
    if (err) return err;

    err = pdb_xx_encode(pdb, orit->vr_hi_s, orit->vr_hi_e - orit->vr_hi_s, buf);
    if (err) return err;

    err = cm_buffer_sprintf(buf, ":%s:", (vr->vr_valueforward ? "" : "~"));

    if (err) return err;

    /* XXX WRONG
     */
    if (vr->vr_internal_and) {
      err = graphd_iterator_util_freeze_subiterator(
          pdb, vr->vr_internal_and, PDB_ITERATOR_FREEZE_SET, buf);

      if (err) return err;
    } else {
      err = cm_buffer_add_bytes(buf, "()", 2);
      if (err) return err;
    }
    cm_buffer_add_bytes(buf, ":", 1);

    err = pdb_iterator_freeze_ordering(pdb, buf, it);
    if (err) return err;

    err = pdb_iterator_freeze_account(pdb, buf, it);
    if (err) return err;

    sep = "/";
  }

  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    err = cm_buffer_add_bytes(buf, sep, strlen(sep));
    if (err) return err;

    if (vr->vr_eof) {
      cm_buffer_add_bytes(buf, "$", 1);
    } else {
      cm_buffer_sprintf(buf, "%llu", (unsigned long long)vr->vr_last_id_out);
    }
    sep = "/";
  }

  if ((flags & PDB_ITERATOR_FREEZE_STATE)) {
    err = cm_buffer_add_bytes(buf, sep, strlen(sep));
    if (err) return err;

    err = cm_buffer_sprintf(
        buf, "%llu:%llu:%llu:(", (unsigned long long)vr->vr_cvit_last_id_out,
        pdb_iterator_n(pdb, it), pdb_iterator_next_cost(pdb, it));

    if (err) return err;

    err = vr->vr_cmp->cmp_vrange_freeze(vr->vr_greq, vr, vr + 1, buf);

    if (err) return err;

    cm_buffer_add_bytes(buf, "):", 2);

    if (vr->vr_internal_bin) {
      err = graphd_iterator_util_freeze_subiterator(
          pdb, vr->vr_internal_bin,
          PDB_ITERATOR_FREEZE_SET | PDB_ITERATOR_FREEZE_POSITION |
              PDB_ITERATOR_FREEZE_STATE,
          buf);
      if (err) return err;
    }
  }
  return 0;
}

static int graphd_iterator_vrange_beyond(pdb_handle *pdb, pdb_iterator *it,
                                         const char *s, const char *e,
                                         bool *string_in_range) {
  int err;
  graphd_value_range *vr = it->it_theory;
  cl_handle *cl = graphd_request_cl(vr->vr_greq);

  cl_assert(cl, it->it_type == &graphd_iterator_vrange);
  vr = it->it_theory;

  RIT_MAGIC_CHECK(cl, vr);

  /*
   * Not all comparators need to export this function. However if
   * this function isn't exported we must not have an ordering
   * and beyond must never be called.
   */
  cl_assert(cl, vr->vr_cmp->cmp_value_in_range);

  err = vr->vr_cmp->cmp_value_in_range(vr->vr_greq, vr, vr + 1, s, e,
                                       string_in_range);

  cl_log(cl, CL_LEVEL_VERBOSE, "value in range over '%.*s' returns %s: %s",
         (int)(e - s), s, *string_in_range ? "true" : "false", strerror(err));

  return err;
}

bool graphd_vrange_forward(graphd_request *greq, graphd_value_range *vr) {
  RIT_MAGIC_CHECK(graphd_request_cl(greq), vr);
  return (vr->vr_valueforward);
}

/*
 * Primitive summaries for a vrange iterator.
 * If we have a internal_and, inherit its summary but mark that we know
 * something it doesn't.  Otherwise, return nothing.
 */
static int graphd_iterator_vrange_psum(pdb_handle *pdb, pdb_iterator *it,
                                       pdb_primitive_summary *psum_out) {
  graphd_value_range *vr = it->it_theory;
  cl_handle *cl = graphd_request_cl(vr->vr_greq);
  int err;

  vr = it->it_theory;
  RIT_MAGIC_CHECK(cl, vr);

  if (vr->vr_internal_and) {
    err = pdb_iterator_primitive_summary(pdb, vr->vr_internal_and, psum_out);

    if (err) return err;

    psum_out->psum_complete = false;
  } else {
    psum_out->psum_locked = 0;
    psum_out->psum_complete = false;
    psum_out->psum_result = PDB_LINKAGE_N;
  }
  return 0;
}

static const pdb_iterator_type graphd_iterator_vrange = {
    "vrange",
    graphd_iterator_vrange_finish,
    graphd_iterator_vrange_reset,
    graphd_iterator_vrange_clone,
    graphd_iterator_vrange_freeze,
    graphd_iterator_vrange_to_string,

    graphd_iterator_vrange_next_loc,
    NULL, /* No find */
    graphd_iterator_vrange_check,
    graphd_iterator_vrange_statistics,

    NULL, /* idarray	  */
    graphd_iterator_vrange_psum,
    graphd_iterator_vrange_beyond,
    NULL, /* range-estimate */
    NULL, /* restrict	  */

    NULL, /* suspend 	  */
    NULL  /* unsuspend 	  */
};
