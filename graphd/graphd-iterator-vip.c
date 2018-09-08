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

/*  VIP -- a very thin wrapper around another iterator.
 *
 *	The subiterator represents the intersection between
 *	left=X or right=X (X is the "source") and typeguid=Y.
 *
 *	The wrapper stores the GUID and local IDs that were
 * 	involved in creating the subiterator, so that a calling
 *  	e.g. and-iterator can later retrieve and reuse the type
 *	information.
 */

/*  How many samples do we test to figure out the average fan-out?
 */
#define GRAPHD_LINKSTO_N_SAMPLES 5

typedef struct graphd_iterator_vip {
  pdb_handle *vip_pdb;
  cm_handle *vip_cm;
  cl_handle *vip_cl;

  pdb_iterator *vip_sub;
  graph_guid vip_type_guid;
  pdb_id vip_type_id;
  int vip_linkage;
  pdb_id vip_source_id;
  graph_guid vip_source_guid;

  /*  Most recently returned position.
   */
  pdb_id vip_id;
  unsigned int vip_eof : 1;
  unsigned int vip_source_guid_valid : 1;

} graphd_iterator_vip;

static int vip_iterator_find_loc(pdb_handle *pdb, pdb_iterator *it,
                                 pdb_id id_in, pdb_id *id_out,
                                 pdb_budget *budget_inout, char const *file,
                                 int line) {
  graphd_iterator_vip *vip = it->it_theory;
  pdb_budget budget_in = *budget_inout;
  int err;

  err = pdb_iterator_find_loc(pdb, vip->vip_sub, id_in, id_out, budget_inout,
                              file, line);
  if (err == 0)
    vip->vip_id = *id_out;
  else if (err == GRAPHD_ERR_NO)
    vip->vip_eof = true;

  if (err == 0)
    pdb_rxs_log(pdb, "FIND %p vip %llx -> %llx ($%lld)", (void *)it,
                (unsigned long long)id_in, (unsigned long long)*id_out,
                (long long)(budget_in - *budget_inout));
  else
    pdb_rxs_log(pdb, "FIND %p vip %llx %s ($%lld)", (void *)it,
                (unsigned long long)id_in,
                err == PDB_ERR_NO ? "eof" : (err == PDB_ERR_MORE
                                                 ? "suspended"
                                                 : graphd_strerror(err)),
                (long long)(budget_in - *budget_inout));

  pdb_iterator_account_charge_budget(pdb, it, find);
  return err;
}

static int vip_iterator_check(pdb_handle *pdb, pdb_iterator *it,
                              pdb_id check_id, pdb_budget *budget_inout) {
  graphd_iterator_vip *vip = it->it_theory;
  int err;
  addb_id found_id;
  pdb_budget budget_in = *budget_inout;

  if (check_id < it->it_low || check_id >= it->it_high) {
    *budget_inout -= PDB_COST_FUNCTION_CALL;
    err = GRAPHD_ERR_NO;
  }

  /*  If it's cheaper to just read the primitive
   *  and look, do that, rather than checking against the
   *  VIP's hmap.
   */
  else if (pdb_iterator_check_cost_valid(pdb, vip->vip_sub) &&
           (pdb_iterator_check_cost(pdb, vip->vip_sub) <=
            PDB_COST_FUNCTION_CALL + PDB_COST_PRIMITIVE))

    err = pdb_iterator_check(pdb, vip->vip_sub, check_id, budget_inout);
  else {
    graph_guid guid;
    pdb_primitive pr;

    if ((err = pdb_id_read(pdb, check_id, &pr)) != 0) {
      cl_log_errno(vip->vip_cl, CL_LEVEL_ERROR, "pdb_id_read", err, "id=%lld",
                   (long long)check_id);
      goto err;
    }

    *budget_inout -= PDB_COST_PRIMITIVE;

    if (!pdb_primitive_has_linkage(&pr, vip->vip_linkage) ||
        !pdb_primitive_has_linkage(&pr, PDB_LINKAGE_TYPEGUID)) {
    not_this_primitive:
      err = PDB_ERR_NO;
      pdb_primitive_finish(pdb, &pr);
      goto err;
    }

    pdb_primitive_linkage_get(&pr, vip->vip_linkage, guid);
    err = pdb_id_from_guid(pdb, &found_id, &guid);
    if (err != 0) {
      char buf[200];
      cl_log_errno(vip->vip_cl, CL_LEVEL_ERROR, "pdb_id_from_guid", err,
                   "guid=%s", graph_guid_to_string(&guid, buf, sizeof buf));
      pdb_primitive_finish(pdb, &pr);
      goto err;
    }
    if (found_id != vip->vip_source_id) goto not_this_primitive;

    pdb_primitive_linkage_get(&pr, PDB_LINKAGE_TYPEGUID, guid);
    if (!GRAPH_GUID_EQ(vip->vip_type_guid, guid)) goto not_this_primitive;

    pdb_primitive_finish(pdb, &pr);
  }

err:
  pdb_rxs_log(
      pdb, "CHECK %p vip %llx %s ($%lld)", (void *)it,
      (unsigned long long)check_id,
      err == GRAPHD_ERR_NO ? "no" : (err == 0 ? "yes" : graphd_strerror(err)),
      (long long)(budget_in - *budget_inout));
  pdb_iterator_account_charge_budget(pdb, it, check);
  return err;
}

static int vip_iterator_statistics(pdb_handle *pdb, pdb_iterator *it,
                                   pdb_budget *budget_inout) {
  graphd_iterator_vip *vip = it->it_theory;

  cl_notreached(vip->vip_cl,
                "unexpected call to "
                "vip_iterator_statistics(%p) (it->it_statistics_done=%d)",
                (void *)it, pdb_iterator_statistics_done(pdb, it));

  /* NOTREACHED */
  return EINVAL;
}

static int vip_iterator_next_loc(pdb_handle *pdb, pdb_iterator *it,
                                 pdb_id *id_out, pdb_budget *budget_inout,
                                 char const *file, int line) {
  graphd_iterator_vip *vip = it->it_theory;
  pdb_budget budget_in = *budget_inout;
  int err;

  if (vip->vip_eof) {
    pdb_rxs_log(pdb, "NEXT %p vip done [cached] ($%lld)", (void *)it,
                (long long)(budget_in - *budget_inout));
    err = GRAPHD_ERR_NO;
  } else {
    err = pdb_iterator_next_loc(pdb, vip->vip_sub, id_out, budget_inout, file,
                                line);
    if (err == 0) {
      vip->vip_id = *id_out;
      pdb_rxs_log(pdb, "NEXT %p vip %llx ($%lld)", (void *)it,
                  (unsigned long long)*id_out,
                  (long long)(budget_in - *budget_inout));
    } else {
      vip->vip_id = PDB_ID_NONE;
      if (err == PDB_ERR_NO) vip->vip_eof = true;

      pdb_rxs_log(pdb, "NEXT %p vip %s ($%lld)", (void *)it,
                  (err == PDB_ERR_NO ? "eof" : (err == PDB_ERR_MORE
                                                    ? "suspended"
                                                    : graphd_strerror(err))),
                  (long long)(budget_in - *budget_inout));
    }
  }
  pdb_iterator_account_charge_budget(pdb, it, next);
  return err;
}

static int vip_iterator_idarray(pdb_handle *pdb, pdb_iterator *it,
                                addb_idarray **idarray_out,
                                unsigned long long *s_out,
                                unsigned long long *e_out) {
  graphd_iterator_vip *vip = it->it_theory;

  return pdb_iterator_idarray(pdb, vip->vip_sub, idarray_out, s_out, e_out);
}

static int vip_iterator_reset(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_vip *vip = it->it_theory;

  vip->vip_id = PDB_ID_NONE;
  vip->vip_eof = false;

  return pdb_iterator_reset(pdb, vip->vip_sub);
}

/*  vip:source:linkage:type:<hmap>
 */
static int vip_iterator_freeze(pdb_handle *pdb, pdb_iterator *it,
                               unsigned int flags, cm_buffer *buf) {
  graphd_iterator_vip *vip = it->it_theory;
  int err;
  char const *sep = "";
  char b1[200], b2[200];

  if (flags & PDB_ITERATOR_FREEZE_SET) {
    err = pdb_iterator_freeze_intro(buf, it, "vip");
    if (err != 0) return err;

    err = cm_buffer_sprintf(
        buf, ":%.1s+%s->%s", pdb_linkage_to_string(vip->vip_linkage),
        graph_guid_to_string(&vip->vip_type_guid, b1, sizeof b1),
        pdb_id_to_string(pdb, vip->vip_source_id, b2, sizeof b2));
    if (err != 0) return err;

    err = pdb_iterator_freeze_account(pdb, buf, it);
    if (err != 0) return err;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    err = cm_buffer_sprintf(
        buf, "%s%s", sep,
        pdb_iterator_has_position(pdb, vip->vip_sub)
            ? (vip->vip_eof ? "$"
                            : pdb_id_to_string(pdb, vip->vip_id, b2, sizeof b2))
            : "-");
    if (err != 0) return err;
    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_STATE) {
    /*  Our runtime state is the full state of
     *  the subiterator.
     */
    err = cm_buffer_add_string(buf, sep);
    if (err != 0) return err;

    err = graphd_iterator_util_freeze_subiterator(
        pdb, vip->vip_sub, PDB_ITERATOR_FREEZE_EVERYTHING, buf);
    if (err != 0) return err;
  }

  return 0;
}

static int vip_iterator_clone(pdb_handle *pdb, pdb_iterator *it,
                              pdb_iterator **it_out) {
  pdb_iterator *it_orig = it->it_original;
  graphd_iterator_vip *vip = it->it_theory;
  graphd_iterator_vip *vip_out;
  int err;

  PDB_IS_ITERATOR(vip->vip_cl, it);
  PDB_IS_ORIGINAL_ITERATOR(vip->vip_cl, it->it_original);

  if (it->it_id != it_orig->it_id)
    return pdb_iterator_clone(pdb, it_orig, it_out);
  *it_out = NULL;

  vip_out = cm_malcpy(vip->vip_cm, vip, sizeof(*vip));
  if (vip_out == NULL) return errno ? errno : ENOMEM;

  err = pdb_iterator_clone(pdb, vip->vip_sub, &vip_out->vip_sub);
  if (err != 0) {
    cm_free(vip->vip_cm, vip_out);
    return err;
  }
  if ((err = pdb_iterator_make_clone(pdb, it->it_original, it_out)) != 0) {
    pdb_iterator_destroy(pdb, &vip_out->vip_sub);
    cm_free(vip->vip_cm, vip_out);

    return err;
  }
  (*it_out)->it_theory = vip_out;
  (*it_out)->it_has_position = true;

  if (!pdb_iterator_has_position(pdb, it)) {
    err = pdb_iterator_reset(pdb, *it_out);
    if (err != 0) {
      pdb_iterator_destroy(pdb, it_out);
      return err;
    }
  }
  return 0;
}

static void vip_iterator_finish(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_vip *vip = it->it_theory;
  if (vip != NULL) {
    cl_cover(vip->vip_cl);

    pdb_iterator_destroy(pdb, &vip->vip_sub);
    cm_free(vip->vip_cm, it->it_displayname);
    it->it_displayname = NULL;

    cm_free(vip->vip_cm, vip);
    it->it_theory = NULL;
  }
  it->it_type = NULL;
  it->it_magic = 0;
}

static char const *vip_iterator_to_string(pdb_handle *pdb, pdb_iterator *it,
                                          char *buf, size_t size) {
  graphd_iterator_vip *vip = it->it_theory;
  char sub[200];

  snprintf(buf, size, "%svip(%s=%llx;%llx):%s",
           pdb_iterator_forward(pdb, it) ? "" : "~",
           pdb_linkage_to_string(vip->vip_linkage),
           (unsigned long long)vip->vip_source_id,
           (unsigned long long)vip->vip_type_id,
           pdb_iterator_to_string(pdb, vip->vip_sub, sub, sizeof sub));
  return buf;
}

/**
 * @brief Return the primitive summary for a VIP iterator.
 *
 * @param pdb		module handle
 * @param it		a vip iterator
 * @param psum_out	out: summary
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int vip_iterator_primitive_summary(pdb_handle *pdb, pdb_iterator *it,
                                          pdb_primitive_summary *psum_out) {
  int err;
  graphd_iterator_vip *vip = it->it_theory;

  if (!vip->vip_source_guid_valid) {
    err = pdb_id_to_guid(pdb, vip->vip_source_id, &vip->vip_source_guid);
    if (err != 0) {
      cl_log_errno(vip->vip_cl, CL_LEVEL_ERROR, "pdb_id_to_guid", err,
                   "vip->vip_source_id=%lld", (long long)vip->vip_source_id);
      return err;
    }
    vip->vip_source_guid_valid = true;
  }
  psum_out->psum_guid[vip->vip_linkage] = vip->vip_source_guid;
  psum_out->psum_guid[PDB_LINKAGE_TYPEGUID] = vip->vip_type_guid;

  psum_out->psum_locked = (1 << PDB_LINKAGE_TYPEGUID) | (1 << vip->vip_linkage);
  psum_out->psum_result = PDB_LINKAGE_N;
  psum_out->psum_complete = true;

  return 0;
}

/**
 *  @brief Are we done with this?
 */
static int vip_iterator_beyond(pdb_handle *pdb, pdb_iterator *it, char const *s,
                               char const *e, bool *beyond_out) {
  graphd_iterator_vip *vip = it->it_theory;
  int err;
  char buf[200];

  /*  If the VIP iterator is ordered, so is its contained iterator.
   *  Both have the same ordering.
   */
  pdb_iterator_ordered_set(pdb, vip->vip_sub, true);
  pdb_iterator_ordering_set(pdb, vip->vip_sub, pdb_iterator_ordering(pdb, it));

  err = pdb_iterator_beyond(pdb, vip->vip_sub, s, e, beyond_out);

  cl_log(vip->vip_cl, CL_LEVEL_VERBOSE, "vip_iterator_beyond: %s: %s",
         pdb_iterator_to_string(pdb, vip->vip_sub, buf, sizeof buf),
         err ? graphd_strerror(err) : *beyond_out
                                          ? "yes, we're done"
                                          : "no, we can still go below that");

  return err;
}

static int vip_iterator_range_estimate(pdb_handle *pdb, pdb_iterator *it,
                                       pdb_range_estimate *range) {
  graphd_iterator_vip *vip = it->it_theory;
  return pdb_iterator_range_estimate(pdb, vip->vip_sub, range);
}

static const pdb_iterator_type graphd_iterator_vip_type = {
    "vip",
    vip_iterator_finish,
    vip_iterator_reset,
    vip_iterator_clone,
    vip_iterator_freeze,
    vip_iterator_to_string,

    vip_iterator_next_loc,
    vip_iterator_find_loc,
    vip_iterator_check,
    vip_iterator_statistics,

    vip_iterator_idarray,
    vip_iterator_primitive_summary,
    vip_iterator_beyond,
    vip_iterator_range_estimate,
    NULL, /* restrict */

    NULL, /* suspend */
    NULL  /* unsuspend */
};

/**
 * @brief Assemble a VIP iterator.
 *
 *  The new iterator L is derived from another iterator S.
 *  The primitives in L point to the primitives in S with their
 *  linkage pointer.
 *
 *  The subconstraint sub is implicitly moved into the new
 *  iterator and must not be referenced by clones.
 *
 * @param graphd	server for whom we're doing this
 * @param sub		iterator to be wrapped
 * @param source_id	endpoint local ID
 * @param linkage	endpoint position relative to the link IDs
 * @param type_id	local ID of the type
 * @param type_guid	GUID of the type
 * @param low		first local ID in the result range
 * @param high		first local ID not in the result range.
 * @param forward	sort low to high?
 * @param it_out	store resulting iterator here.
 *
 * @return NULL on allocation error, otherwise a freshly minted
 *  	vip structure.
 */
static int vip_assemble(graphd_handle *graphd, pdb_iterator *sub,
                        pdb_id source_id, int linkage, pdb_id type_id,
                        graph_guid const *type_guid, pdb_iterator **it_out) {
  pdb_handle *pdb = graphd->g_pdb;
  cm_handle *cm = pdb_mem(pdb);
  graphd_iterator_vip *vip;
  char buf[200];
  int err;

  *it_out = NULL;

  cl_assert(graphd->g_cl, sub != NULL);

  if ((vip = cm_zalloc(cm, sizeof(*vip))) == NULL ||
      (*it_out = cm_zalloc(cm, sizeof(**it_out))) == NULL) {
    err = errno ? errno : ENOMEM;
    if (vip != NULL) cm_free(cm, vip);
    *it_out = NULL;
    return err;
  }

  vip->vip_pdb = pdb;
  vip->vip_cl = pdb_log(graphd->g_pdb);
  vip->vip_cm = cm;
  vip->vip_sub = sub;
  vip->vip_source_id = source_id;
  vip->vip_linkage = linkage;
  vip->vip_type_guid = *type_guid;
  vip->vip_type_id = type_id;

  pdb_iterator_make(pdb, *it_out, sub->it_low, sub->it_high, sub->it_forward);

  (*it_out)->it_theory = vip;
  (*it_out)->it_type = &graphd_iterator_vip_type;

  pdb_iterator_sorted_set(pdb, *it_out, true);
  pdb_iterator_find_cost_set(pdb, *it_out, pdb_iterator_find_cost(pdb, sub));

  if (pdb_iterator_check_cost(pdb, sub) >
      PDB_COST_FUNCTION_CALL + PDB_COST_PRIMITIVE)
    pdb_iterator_check_cost_set(pdb, *it_out,
                                PDB_COST_FUNCTION_CALL + PDB_COST_PRIMITIVE);
  else
    pdb_iterator_check_cost_set(pdb, *it_out,
                                pdb_iterator_check_cost(pdb, sub));

  pdb_iterator_n_set(pdb, *it_out, pdb_iterator_n(pdb, sub));
  pdb_iterator_next_cost_set(pdb, *it_out, pdb_iterator_next_cost(pdb, sub));
  pdb_iterator_statistics_done_set(pdb, *it_out);

  cl_log(vip->vip_cl, CL_LEVEL_VERBOSE | PDB_FACILITY_ITERATOR,
         "PDB STAT for %s: n=%llu cc=%lld "
         "nc=%lld fc=%lld sorted %llx..%llx (incl)",

         pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf),
         (unsigned long long)pdb_iterator_n(pdb, *it_out),
         (long long)pdb_iterator_check_cost(pdb, *it_out),
         (long long)pdb_iterator_next_cost(pdb, *it_out),
         (long long)pdb_iterator_find_cost(pdb, *it_out),
         (unsigned long long)(*it_out)->it_low,
         (unsigned long long)((*it_out)->it_high - 1));

  return 0;
}

/**
 * @brief Create a "vip" iterator structure around an existing iterator.
 *
 *  The caller has done most of the work, and produced an sub
 *  iterator, null iterator, or fixed iterator iterator that
 *  values can be pulled out of.
 *
 *  This iterator only decorates that subiterator with a structure
 *  that holds information about where the substructure comes from,
 *  so that a containing AND iterator can extract that information
 *  later.
 *
 * @param graphd	server for whom we're doing this
 * @param sub		subiterator, moves into the new iterator
 * @param source_id	endpoint ID as seen from the links
 * @param linkage	endpoint's position seen from the links, PDB_LINKAGE_..
 * @param low		low end of result spectrum (included)
 * @param high		high end of result spectrum (excluded)
 * @param it_out	out: the newly constructed iterator.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int vip_wrap(graphd_handle *graphd, pdb_iterator **sub, pdb_id source_id,
                    int linkage, pdb_id type_id, pdb_iterator **it_out) {
  pdb_handle *pdb = graphd->g_pdb;
  graph_guid type_guid;
  int err;

  *it_out = NULL;

  /* Null in, null out.
   */
  if (pdb_iterator_null_is_instance(graphd->g_pdb, *sub)) {
    *it_out = *sub;
    *sub = NULL;

    return 0;
  }

  err = pdb_id_to_guid(pdb, type_id, &type_guid);
  if (err != 0) {
    return err;
  }
  err = vip_assemble(graphd, *sub, source_id, linkage, type_id, &type_guid,
                     it_out);
  if (err != 0) {
    *it_out = NULL;
    return err;
  }
  *sub = NULL;
  return 0;
}

/**
 * @brief Desequentialize a vip iterator
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_vip_thaw(graphd_handle *g, pdb_iterator_text const *pit,
                             pdb_iterator_base *pib, cl_loglevel loglevel,
                             pdb_iterator **it_out) {
  pdb_handle *pdb = g->g_pdb;
  cl_handle *cl = pdb_log(pdb);
  pdb_iterator *sub = NULL;
  char const *s, *e;
  int err;
  unsigned long long low, high;
  bool forward, eof = false, have_state = false;
  graph_guid type_guid;
  pdb_id source_id, pos_id = PDB_ID_NONE, type_id;
  int linkage;
  graphd_iterator_vip *vip;
  pdb_iterator_account *acc = NULL;

  s = pit->pit_set_s;
  e = pit->pit_set_e;

  err = pdb_iterator_util_thaw(
      pdb, &s, e,
      "%{forward}%{low[-high]}:"
      "%{linkage}+%{guid}->%{id}%{account}%{extensions}%{end}",
      &forward, &low, &high, &linkage, &type_guid, &source_id, pib, &acc,
      (pdb_iterator_property *)NULL);
  if (err != 0) return err;

  if ((s = pit->pit_position_s) != NULL && s < (e = pit->pit_position_e)) {
    err = pdb_iterator_util_thaw(pdb, &s, e, "%{eof/id}%{extensions}%{end}",
                                 &eof, &pos_id, (pdb_iterator_property *)NULL);
    if (err != 0) return err;
  }

  /* Will be cached:the ID that the type translates to.
   */
  err = pdb_id_from_guid(pdb, &type_id, &type_guid);
  if (err != 0) {
    cl_log(cl, loglevel,
           "graphd_iterator_vip_thaw: cannot convert "
           "%llx to a GUID: %s",
           (long long)type_id, graphd_strerror(err));
    return err;
  }

  if ((s = pit->pit_state_s) != NULL && s < (e = pit->pit_state_e)) {
    err = graphd_iterator_util_thaw_subiterator(g, &s, e, pib, loglevel, &sub);
    if (err != 0) return err;

    have_state = true;

    cl_assert(cl, sub != NULL);
    err = vip_wrap(g, &sub, source_id, linkage, type_id, it_out);
    if (err != 0) {
      pdb_iterator_destroy(pdb, &sub);
      return err;
    }

    err = pdb_iterator_util_thaw(pdb, &s, e, "%{extensions}%{end}",
                                 (pdb_iterator_property *)NULL);
    if (err != 0) {
      pdb_iterator_destroy(pdb, &sub);
      return err;
    }

  } else {
    /*  We don't have a detailed copy of the subiterator;
     *  ah well, just recreate it.
     */
    err = graphd_iterator_vip_create(g, source_id, linkage, type_id, &type_guid,
                                     low, high, forward,
                                     /* error-if-null */ false, it_out);
    if (err != 0) return err;
  }

  pdb_iterator_account_set(pdb, *it_out, acc);
  vip = (*it_out)->it_theory;

  if ((s = pit->pit_position_s) != NULL && s < (e = pit->pit_position_e)) {
    err = pdb_iterator_util_thaw(pdb, &s, e, "%{eof/id}%{extensions}%{end}",
                                 &eof, &pos_id, (pdb_iterator_property *)NULL);
    if (err != 0) {
      pdb_iterator_destroy(pdb, it_out);
      return err;
    }
    vip->vip_id = pos_id;
    vip->vip_eof = eof;
  }

  /*  If we have a position, but didn't have a state,
   *  we need to position on the state.
   *
   *  That's alright, we can do this quickly because
   *  our subiterator is tractable (a fixed or an hmap).
   */
  if (pos_id != PDB_ID_NONE && !have_state) {
    pdb_budget big_budget = 1000;

    err = pdb_iterator_find(pdb, vip->vip_sub, pos_id, &pos_id, &big_budget);
    if (err == GRAPHD_ERR_NO)
      vip->vip_eof = true;
    else if (err == 0)
      vip->vip_id = pos_id;
    else {
      char buf[200];
      pdb_iterator_destroy(pdb, it_out);
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_find", err, "it=%s",
                   pdb_iterator_to_string(pdb, vip->vip_sub, buf, sizeof buf));
      return err;
    }
  }
  return err;
}

bool graphd_iterator_vip_is_instance(pdb_handle *pdb, pdb_iterator *it) {
  return it->it_type == &graphd_iterator_vip_type;
}

int graphd_iterator_vip_linkage(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_vip *vip = it->it_theory;
  return vip->vip_linkage;
}

pdb_id graphd_iterator_vip_type_id(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_vip *vip = it->it_theory;
  return vip->vip_type_id;
}

pdb_id graphd_iterator_vip_source_id(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_vip *vip = it->it_theory;
  return vip->vip_source_id;
}

/**
 * @brief Build a VIP iterator or its simulation.
 *
 *  If the endpoint doesn't have enough fan-in to be a VIP node,
 *  evaluate the intersection between type and fan-in right away.
 *
 *  If the VIP iterator is null, it is *not* wrapped into
 *  a VIP shell, but just returned as a null.  (The assumption is
 *  that the VIP iterator may be in an AND with others, and the
 *  sooner we find the nulls, the better.)
 *
 * @param graphd	opaque database module handle
 * @param source_id	endpoint that it's the iterator for
 * @param linkage	endpoint's linkage relative to links
 * @param type_id	local id of the typeguid
 * @param type_guid	typeguid
 * @param low		low end of produced id range, included
 * @param high		high end of produced id range, excluded
 * @param forward	run low to high?
 * @param it_out	return iterator here.
 */

int graphd_iterator_vip_create(graphd_handle *graphd, pdb_id source_id,
                               int linkage, pdb_id type_id,
                               graph_guid const *type_guid,
                               unsigned long long low, unsigned long long high,
                               bool forward, bool error_if_null,
                               pdb_iterator **it_out) {
  pdb_handle *pdb = graphd->g_pdb;
  cl_handle *cl = pdb_log(pdb);
  pdb_iterator *vip_it = NULL;
  bool true_vip = false;
  int err;
  char buf[200];
  pdb_budget budget = 1024 * 16;

  cl_enter(cl, CL_LEVEL_VERBOSE, "%s(%llx) + %s in [%llx..[%lld%s",
           pdb_linkage_to_string(linkage), (unsigned long long)source_id,
           graph_guid_to_string(type_guid, buf, sizeof buf), low, high,
           forward ? "" : ", backwards");

  if (type_id == PDB_ID_NONE ||
      (linkage != PDB_LINKAGE_LEFT && linkage != PDB_LINKAGE_RIGHT)) {
    err = pdb_linkage_id_iterator(graphd->g_pdb, linkage, source_id, low, high,
                                  forward, error_if_null, it_out);

    cl_leave(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_vip_create: "
             "pure gmap");
    return err;
  }

  /*  Get an iterator for a superset of the links
   *  the caller is asking for.
   */
  err =
      pdb_vip_linkage_id_iterator(pdb, source_id, linkage, type_guid, low, high,
                                  forward, error_if_null, &vip_it, &true_vip);
  if (err != 0) {
    cl_leave_err(cl, CL_LEVEL_VERBOSE, err,
                 "pdb_vip_linkage_id_iterator fails");
    if (err == GRAPHD_ERR_NO)
      return error_if_null ? GRAPHD_ERR_NO
                           : pdb_iterator_null_create(pdb, it_out);
    return err;
  }

  if (pdb_iterator_null_is_instance(pdb, vip_it)) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "null");
    *it_out = vip_it;
    return 0;
  }

  if (!true_vip) {
    pdb_iterator *type_it;

    /*  We got something other than NULL, but it doesn't
     *  take the type into account.  So, do the intersect
     *  right now.
     */
    err = pdb_linkage_id_iterator(pdb, PDB_LINKAGE_TYPEGUID, type_id, low, high,
                                  forward, error_if_null, &type_it);
    if (err != 0) {
      pdb_iterator_destroy(pdb, &vip_it);
      cl_leave_err(cl, CL_LEVEL_VERBOSE, err,
                   "pdb_linkage_id_iterator, type_id=%llx",
                   (unsigned long long)type_id);
      return err == GRAPHD_ERR_NO && !error_if_null
                 ? pdb_iterator_null_create(pdb, it_out)
                 : err;
    }
    if (pdb_iterator_null_is_instance(pdb, type_it)) {
      pdb_iterator_destroy(pdb, &vip_it);

      cl_leave_err(cl, CL_LEVEL_VERBOSE, err,
                   "pdb_linkage_id_iterator, null iterator "
                   "for instances of type_id=%llx, null %p",
                   (unsigned long long)type_id, (void *)type_it);
      if (error_if_null) {
        pdb_iterator_destroy(pdb, &type_it);
        return GRAPHD_ERR_NO;
      }
      *it_out = type_it;
      return 0;
    }

    err = graphd_iterator_intersect(graphd, vip_it, type_it, low, high, forward,
                                    error_if_null, &budget, it_out);
    if (err != 0) {
      char buf2[200];
      pdb_id id[PDB_VIP_MIN];
      size_t id_n = 0;

      if (error_if_null && err == GRAPHD_ERR_NO) {
        pdb_iterator_destroy(pdb, &type_it);
        pdb_iterator_destroy(pdb, &vip_it);
        *it_out = NULL;

        cl_leave(cl, CL_LEVEL_VERBOSE, "no (empty result set)");
        return GRAPHD_ERR_NO;
      }

      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_iterator_vip_create: "
             "didn't get a genuine VIP link, "
             "but can't get a fast intersect between "
             "%s and %s, either: %s",
             pdb_iterator_to_string(pdb, type_it, buf, sizeof buf),
             pdb_iterator_to_string(pdb, vip_it, buf2, sizeof buf2),
             graphd_strerror(err));

      /* Shortcut didn't work.
         Slow version.
       */
      id_n = 0;
      for (;;) {
        pdb_id sample, found_id;

        err = pdb_iterator_next_nonstep(pdb, vip_it, &sample);
        if (err != 0) break;

        for (;;) {
          err = pdb_iterator_find_nonstep(pdb, type_it, sample, &found_id);
          if (err != 0 || found_id == sample) break;

          sample = found_id;
          err = pdb_iterator_find_nonstep(pdb, vip_it, sample, &found_id);
          if (err != 0 || found_id == sample) break;
        }
        if (err) break;

        cl_assert(cl, id_n < sizeof(id) / sizeof(*id));
        id[id_n++] = sample;
      }
      cl_assert(cl, err != 0);
      if (err != GRAPHD_ERR_NO) {
        cl_leave_err(cl, CL_LEVEL_FAIL, err, "type_id=%llx",
                     (unsigned long long)type_id);

        pdb_iterator_destroy(pdb, &vip_it);
        pdb_iterator_destroy(pdb, &type_it);

        return err;
      }

      err = graphd_iterator_fixed_create_array(graphd, id, id_n, low, high,
                                               forward, it_out);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_fixed_create_array",
                     err, "n=%zu", id_n);
        cl_leave(cl, CL_LEVEL_VERBOSE, "leave");
        return err;
      }
    }
    pdb_iterator_destroy(pdb, &vip_it);
    pdb_iterator_destroy(pdb, &type_it);

    vip_it = *it_out;
    *it_out = NULL;
  }
  err = vip_wrap(graphd, &vip_it, source_id, linkage, type_id, it_out);
  if (err != 0) pdb_iterator_destroy(pdb, &vip_it);

  cl_leave_err(cl, CL_LEVEL_VERBOSE, err, "%p", *it_out);
  return err;
}

bool graphd_iterator_vip_is_fixed_instance(pdb_handle *pdb, pdb_iterator *it,
                                           pdb_id **values_out, size_t *n_out) {
  if (it->it_type == &graphd_iterator_vip_type) {
    graphd_iterator_vip *vip = it->it_theory;

    return graphd_iterator_fixed_is_instance(pdb, vip->vip_sub, values_out,
                                             n_out);
  }
  return false;
}
