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
#include <limits.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

/*  This IDSET iterator relies on someone else to create it, define
 *  its psum and freeze format, and to do most of the thawing.
 *
 *  It manages boundaries and the idset itself.
 */

static int idset_recover(pdb_iterator *it, pdb_budget *budget_inout);

/* The idset original's data.
 */
#define oids(it) ((graphd_iterator_idset *)(it)->it_original->it_theory)

typedef struct graphd_iterator_idset {
  cm_handle *ids_cm;
  cl_handle *ids_cl;
  pdb_handle *ids_pdb;
  graphd_handle *ids_graphd;

  /*  In the original only: the primitive summary for
   *  this iterator.
   */
  pdb_primitive_summary ids_psum;

  /*  In the original only: the IDs that make up the
   *  contents in this iterator.
   */
  graph_idset *ids_set;

  /*  Current or desired position.
   */
  graph_idset_position ids_pos;

  /*  A string we print as our set if someone
   *  asks us to freeze.
   */
  char const *ids_frozen_set;

  /*  Callback for recovering.
   *  Results:
   * 	0               -> we've got our set and location back.
   *      GRAPHD_ERR_MORE -> more budget is needed
   *      other errors    -> unexpected failure.
   */
  void *ids_recover_callback_data;
  int (*ids_recover_callback)(void *data, graphd_handle *g, graph_idset **,
                              pdb_budget *);

  /*  And after recovery, reset.
   */
  unsigned int ids_recover_reset : 1;

  /*  Callback for finishing (also frees the idset
   *  resource, if any.)
   */
  void *ids_finish_callback_data;
  void (*ids_finish_callback)(void *data, graphd_handle *g, graph_idset *);

} graphd_iterator_idset;

static int idset_iterator_find_loc(pdb_handle *pdb, pdb_iterator *it,
                                   pdb_id id_in, pdb_id *id_out,
                                   pdb_budget *budget_inout, char const *file,
                                   int line) {
  graphd_iterator_idset *ids = it->it_theory;
  pdb_budget budget_in = *budget_inout;
  unsigned long long ull;
  int err = 0;
  pdb_id id;

  if ((err = idset_recover(it, budget_inout)) != 0) goto err;

  *budget_inout -= pdb_iterator_find_cost(pdb, it);

  id = id_in;
  if (pdb_iterator_forward(pdb, it)) {
    if (id_in < it->it_low) id = it->it_low;
  } else {
    if (id_in >= it->it_high) id = it->it_high - 1;
  }

  /* Find exactly what we're looking for?
   */
  if (graph_idset_locate(oids(it)->ids_set, id, &ids->ids_pos)) {
    *id_out = id;

    pdb_rxs_log(pdb, "FIND %p idset %llx -> %llx ($%lld)", (void *)it,
                (unsigned long long)id_in, (unsigned long long)*id_out,
                (long long)(budget_in - *budget_inout));
    pdb_iterator_account_charge_budget(pdb, it, find);
    return 0;
  }

  /* No?  Go to the next.
   */
  if (pdb_iterator_forward(pdb, it)) {
    if (graph_idset_next(oids(it)->ids_set, &ull, &ids->ids_pos)) {
      if (ull >= it->it_high)
        err = GRAPHD_ERR_NO;
      else
        *id_out = ull;
      goto err;
    }
  } else {
    if (graph_idset_prev(oids(it)->ids_set, &ull, &ids->ids_pos)) {
      if (ull < it->it_low)
        err = GRAPHD_ERR_NO;
      else
        *id_out = ull;
      goto err;
    }
  }
  err = GRAPHD_ERR_NO;

err:
  if (err == 0)
    pdb_rxs_log(pdb, "FIND %p idset %llx %llx ($%lld)", (void *)it,
                (unsigned long long)id_in, (unsigned long long)*id_out,
                (long long)(budget_in - *budget_inout));
  else
    pdb_rxs_log(pdb, "FIND %p idset %llx %s ($%lld)", (void *)it,
                (unsigned long long)id_in,
                (err == GRAPHD_ERR_NO ? "eof" : err == GRAPHD_ERR_MORE
                                                    ? "suspended"
                                                    : graphd_strerror(err)),
                (long long)(budget_in - *budget_inout));

  pdb_iterator_account_charge_budget(pdb, it, find);
  return err;
}

static int idset_iterator_next_loc(pdb_handle *pdb, pdb_iterator *it,
                                   pdb_id *id_out, pdb_budget *budget_inout,
                                   char const *file, int line) {
  graphd_iterator_idset *ids = it->it_theory;
  pdb_budget budget_in = *budget_inout;
  unsigned long long ull;
  int err;

  if ((err = idset_recover(it, budget_inout)) != 0) goto err;

  *budget_inout -= PDB_COST_FUNCTION_CALL;

  if (pdb_iterator_forward(pdb, it)
          ? graph_idset_next(oids(it)->ids_set, &ull, &ids->ids_pos)
          : graph_idset_prev(oids(it)->ids_set, &ull, &ids->ids_pos)) {
    *id_out = ull;
    err = 0;
  } else
    err = GRAPHD_ERR_NO;

err:
  if (err == 0)
    pdb_rxs_log(pdb, "NEXT %p idset %llx ($%lld)", (void *)it,
                (unsigned long long)*id_out,
                (long long)(budget_in - *budget_inout));
  else if (err == GRAPHD_ERR_NO)
    pdb_rxs_log(pdb, "NEXT %p idset eof ($%lld)", (void *)it,
                (long long)(budget_in - *budget_inout));
  else
    pdb_rxs_log(pdb, "NEXT %p idset %s ($%lld)", (void *)it,
                (err == GRAPHD_ERR_MORE ? "suspend" : graphd_strerror(err)),
                (long long)(budget_in - *budget_inout));
  pdb_iterator_account_charge_budget(pdb, it, next);
  return err;
}

static int idset_iterator_check(pdb_handle *pdb, pdb_iterator *it, pdb_id id,
                                pdb_budget *budget_inout) {
  pdb_budget budget_in = *budget_inout;
  int err;

  if ((err = idset_recover(it, budget_inout)) != 0) goto err;

  *budget_inout -= pdb_iterator_check_cost(pdb, it);

  if (id < it->it_low || id >= it->it_high) {
    err = GRAPHD_ERR_NO;
    goto err;
  }

  err = graph_idset_check(oids(it)->ids_set, id) ? 0 : GRAPHD_ERR_NO;
err:
  pdb_rxs_log(
      pdb, "CHECK %p idset %llx %s ($%lld)", (void *)it, (unsigned long long)id,
      err == 0 ? "yes"
               : (err == GRAPHD_ERR_NO ? "no" : (err == GRAPHD_ERR_MORE
                                                     ? "suspend"
                                                     : graphd_strerror(err))),
      (unsigned long long)(budget_in - *budget_inout));
  pdb_iterator_account_charge_budget(pdb, it, check);
  return err;
}

static int idset_iterator_statistics(pdb_handle *pdb, pdb_iterator *it,
                                     pdb_budget *budget_inout) {
  graphd_iterator_idset *ids = it->it_theory;
  cl_handle *cl = ids->ids_cl;
  graph_idset_position ipos;
  char buf[200];
  int err;

  if ((err = idset_recover(it, budget_inout)) != 0) return err;

  *budget_inout -= PDB_COST_FUNCTION_CALL;

  graph_idset_next_reset(oids(it)->ids_set, &ipos);
  pdb_iterator_n_set(pdb, it,
                     graph_idset_offset(oids(it)->ids_set, &ipos, it->it_high));
  pdb_iterator_statistics_done_set(pdb, it);

  cl_log(cl, CL_LEVEL_DEBUG,
         "PDB STAT for %s: n=%llu cc=%llu nc=%llu fc=%llu%s%s",
         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         (unsigned long long)pdb_iterator_n(pdb, it),
         (unsigned long long)pdb_iterator_check_cost(pdb, it),
         (unsigned long long)pdb_iterator_next_cost(pdb, it),
         (unsigned long long)pdb_iterator_find_cost(pdb, it),
         pdb_iterator_ordered(pdb, it) ? ", o=" : "",
         pdb_iterator_ordered(pdb, it) ? pdb_iterator_ordering(pdb, it) : "");
  return 0;
}

static int idset_iterator_freeze(pdb_handle *pdb, pdb_iterator *it,
                                 unsigned int flags, cm_buffer *buf) {
  graphd_iterator_idset *ids = it->it_theory;
  cl_handle *cl = ids->ids_cl;
  int err = 0;
  char const *sep = "";
  char ibuf[200];

  if (flags & PDB_ITERATOR_FREEZE_SET) {
    err = cm_buffer_add_string(buf, oids(it)->ids_frozen_set);
    if (err != 0) goto buffer_error;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    err = cm_buffer_sprintf(buf, "%s%llu:%zu", sep, ids->ids_pos.gip_ull,
                            ids->ids_pos.gip_size);
    if (err != 0) goto buffer_error;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_STATE) {
    err = cm_buffer_sprintf(buf, "%s", sep);
    if (err != 0) goto buffer_error;
  }
  return 0;

buffer_error:
  cl_log_errno(cl, CL_LEVEL_FAIL, "cm_buffer_add_string/sprintf", err, "it=%s",
               pdb_iterator_to_string(pdb, it, ibuf, sizeof ibuf));
  return err;
}

static int idset_iterator_reset(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_idset *ids = it->it_theory;

  if (oids(it)->ids_recover_callback != NULL) {
    /*  We haven't recovered yet.  Plan a reset
     *  after the recovery, but don't execute it
     *  yet.
     */
    ids->ids_recover_reset = true;
    return 0;
  }

  if (pdb_iterator_forward(pdb, it)) {
    if (it->it_low != 0)
      (void)graph_idset_locate(ids->ids_set, it->it_low, &ids->ids_pos);
    else
      graph_idset_next_reset(oids(it)->ids_set, &ids->ids_pos);
  } else {
    if (it->it_high != PDB_ITERATOR_HIGH_ANY)
      (void)graph_idset_locate(ids->ids_set, it->it_high, &ids->ids_pos);
    else
      graph_idset_prev_reset(oids(it)->ids_set, &ids->ids_pos);
  }
  ids->ids_recover_reset = false;
  return 0;
}

static int idset_iterator_clone(pdb_handle *pdb, pdb_iterator *it,
                                pdb_iterator **it_out) {
  pdb_iterator *it_orig = it->it_original;
  graphd_iterator_idset *ids_out = NULL, *ids = it->it_theory;
  int err;

  PDB_IS_ITERATOR(ids->ids_cl, it);
  PDB_IS_ORIGINAL_ITERATOR(ids->ids_cl, it_orig);

  *it_out = NULL;
  cl_assert(ids->ids_cl, it_orig->it_n > 0);

  ids_out = cm_malloc(ids->ids_cm, sizeof(*ids));
  if (ids_out == NULL) return errno ? errno : ENOMEM;

  *ids_out = *ids;
  ids_out->ids_set = NULL;

  if ((err = pdb_iterator_make_clone(pdb, it_orig, it_out)) != 0) {
    cm_free(ids->ids_cm, ids_out);
    return err;
  }

  (*it_out)->it_theory = ids_out;
  (*it_out)->it_has_position = true;

  if (oids(it)->ids_recover_callback != NULL &&
      !pdb_iterator_has_position(pdb, it))
    ids_out->ids_recover_reset = true;

  return 0;
}

static void idset_iterator_finish(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_idset *ids = it->it_theory;

  if (ids != NULL) {
    /* Original only */
    if (ids->ids_set != NULL) {
      (*ids->ids_finish_callback)(ids->ids_finish_callback_data,
                                  ids->ids_graphd, ids->ids_set);

      /*  Remove the link we added during create.
       */
      graph_idset_free(ids->ids_set);
      ids->ids_set = NULL;
    }
    cm_free(ids->ids_cm, it->it_displayname);
    it->it_displayname = NULL;

    cm_free(ids->ids_cm, ids);
    it->it_theory = NULL;
  }
  it->it_type = NULL;
  it->it_magic = 0;
}

static char const *idset_iterator_to_string(pdb_handle *pdb, pdb_iterator *it,
                                            char *buf, size_t size) {
  char pbuf[200];
  graphd_iterator_idset const *ids = it->it_theory;

  snprintf(buf, size, "idset(%s)", pdb_primitive_summary_to_string(
                                       pdb, &ids->ids_psum, pbuf, sizeof pbuf));
  return buf;
}

/**
 * @brief Return the primitive summary for a idset iterator.
 *
 * @param pdb		module handle
 * @param it		a idset iterator
 * @param psum_out	out: summary
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int idset_iterator_primitive_summary(pdb_handle *pdb, pdb_iterator *it,
                                            pdb_primitive_summary *psum_out) {
  graphd_iterator_idset const *ids = it->it_theory;

  /*  Defer to the original.  It may have a different type.
   */
  if (it->it_original != it)
    return pdb_iterator_primitive_summary(pdb, it->it_original, psum_out);

  *psum_out = ids->ids_psum;
  return 0;
}

/**
 * @brief Has this iterator progressed beyond this value?
 *
 * @param pdb		module handle
 * @param it		iterator we're asking about
 * @param s		start of comparison value
 * @param e		end of comparison value
 * @param beyond_out	out: true if the most recently returned
 *			ID from this iterator was greater than
 *			(or, if it runs backward, smaller than)
 *			the parameter ID.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int idset_iterator_beyond(pdb_handle *pdb, pdb_iterator *it,
                                 char const *s, char const *e,
                                 bool *beyond_out) {
  char buf[200];
  pdb_id id;
  unsigned long long last_id;
  graphd_iterator_idset *ids = it->it_theory;
  graph_idset_position ipos;

  if (e - s != sizeof(id)) {
    *beyond_out = false;
    cl_log(ids->ids_cl, CL_LEVEL_ERROR,
           "idset_iterator_beyond: unexpected "
           "value size (%zu bytes; expected %zu)",
           (size_t)(e - s), sizeof(id));
    return EINVAL;
  }

  ipos = ids->ids_pos;

  /*  Going one step into the opposite direction from
   *  what our iterator direction is will either yield
   *  an error (if we hit a boundary) or the previously
   *  returned ID.
   *
   *  This is disregarding high/low boundaries, which
   *  I think is okay at this point - if it's out of
   *  range, we wouldn't have returned it anyway.
   */
  if (pdb_iterator_forward(pdb, it)) {
    if (!graph_idset_prev(ids->ids_set, &last_id, &ipos)) {
      cl_log(ids->ids_cl, CL_LEVEL_VERBOSE,
             "idset_iterator_beyond: "
             "still at the beginning");
    }
  } else {
    if (!graph_idset_next(ids->ids_set, &last_id, &ipos)) {
      cl_log(ids->ids_cl, CL_LEVEL_VERBOSE,
             "idset_iterator_beyond: "
             "still at the end");
    }
  }
  memcpy(&id, s, sizeof(id));

  *beyond_out = (pdb_iterator_forward(pdb, it) ? id < last_id : id > last_id);

  cl_log(ids->ids_cl, CL_LEVEL_VERBOSE,
         "idset_iterator_beyond: "
         "%llx vs. last_id %llx in %s: %s",
         (unsigned long long)id, (unsigned long long)last_id,
         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         *beyond_out ? "yes" : "no");
  return 0;
}

static const pdb_iterator_type idset_iterator_type = {
    "idset",

    idset_iterator_finish,
    idset_iterator_reset,
    idset_iterator_clone,
    idset_iterator_freeze,
    idset_iterator_to_string,

    idset_iterator_next_loc,
    idset_iterator_find_loc,
    idset_iterator_check,
    idset_iterator_statistics,

    NULL,
    idset_iterator_primitive_summary,
    idset_iterator_beyond,
    NULL, /* range-estimate */
    NULL, /* restrict */

    NULL, /* suspend */
    NULL  /* unsuspend */
};

/**
 * @brief Create an iterator that dispenses a idset set of indices.
 *
 * @param g		server for whom we're doing this
 * @param n		number of returned values.
 * @param low		lowest included value
 * @param high		highest value that isn't included
 * @param forward	true if we'll be iterating from low to high.
 *
 * @return NULL on allocation error, otherwise a freshly minted
 *  	and structure.
 */
int graphd_iterator_idset_create_loc(
    graphd_handle *g, unsigned long long low, unsigned long long high,
    bool forward, graph_idset *set, char const *frozen_set,
    pdb_primitive_summary const *psum,
    int (*recover_callback)(void *, graphd_handle *, graph_idset **,
                            pdb_budget *),
    void *recover_callback_data,
    void (*finish_callback)(void *, graphd_handle *, graph_idset *),
    void *finish_callback_data, pdb_iterator **it_out, char const *file,
    int line) {
  size_t frozen_set_n = strlen(frozen_set);
  pdb_handle *pdb = g->g_pdb;
  cm_handle *cm = pdb_mem(pdb);
  cl_handle *cl = g->g_cl;
  graphd_iterator_idset *ids;
  graph_idset_position ipos;

  cl_assert(cl, recover_callback != NULL || set != NULL);

  if (set != NULL) {
    unsigned long long ull;

    if (!graph_idset_locate(set, low, &ipos)) {
      /*  <low> isn't in the set - move it up
       *  to the next thing that *is*.
       */
      if (!graph_idset_next(set, &ull, &ipos) || ull >= high)
        return pdb_iterator_null_create(g->g_pdb, it_out);
      low = ull;
    }

    if (high != PDB_ITERATOR_HIGH_ANY)
      (void)graph_idset_locate(set, high, &ipos);
    else
      graph_idset_prev_reset(set, &ipos);

    if (!graph_idset_prev(set, &ull, &ipos) || ull < low)
      return pdb_iterator_null_create(g->g_pdb, it_out);

    /*  The first ID not in the set.
     */
    high = ull + 1;
  }

  *it_out = cm->cm_realloc_loc(cm, NULL, sizeof(**it_out), file, line);
  if (*it_out == NULL) return errno ? errno : ENOMEM;

  if ((ids = cm_zalloc(cm, sizeof(*ids) + frozen_set_n + 1)) == NULL) {
    int err = errno ? errno : ENOMEM;

    cm_free(cm, *it_out);
    *it_out = NULL;

    return err;
  }

  ids->ids_cm = cm;
  ids->ids_cl = cl;
  ids->ids_pdb = pdb;
  ids->ids_graphd = g;

  ids->ids_frozen_set = (char *)(ids + 1);
  memcpy((char *)(ids + 1), frozen_set, frozen_set_n);
  ids->ids_set = set;
  ids->ids_finish_callback = finish_callback;
  ids->ids_finish_callback_data = finish_callback_data;
  ids->ids_recover_callback = recover_callback;
  ids->ids_recover_callback_data = recover_callback_data;

  if (psum != NULL) ids->ids_psum = *psum;

  pdb_iterator_make_loc(pdb, *it_out, low, high, forward, file, line);

  if (set != NULL) {
    /* Go to the beginning, and measure the distance to the end.
     */
    (void)graph_idset_locate(set, low, &ids->ids_pos);
    pdb_iterator_n_set(pdb, *it_out,
                       graph_idset_offset(set, &ids->ids_pos, high));

    /* If we're iterating backwards, move to the end or beyond it.
     */
    if (!pdb_iterator_forward(pdb, *it_out))
      (void)graph_idset_locate(set, high, &ids->ids_pos);
  }

  /*  Add our own link to the idset.
   */
  graph_idset_link(ids->ids_set);

  (*it_out)->it_theory = ids;
  (*it_out)->it_type = &idset_iterator_type;

  pdb_iterator_sorted_set(pdb, *it_out, true);
  pdb_iterator_next_cost_set(pdb, *it_out, PDB_COST_FUNCTION_CALL);
  pdb_iterator_check_cost_set(pdb, *it_out, PDB_COST_FUNCTION_CALL);
  pdb_iterator_find_cost_set(pdb, *it_out, PDB_COST_FUNCTION_CALL);

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_idset_create: it %p, ids %p, "
         "space for %lu in [%lld..[%lld%s",
         (void *)*it_out, (void *)ids, (unsigned long)set->gi_n, low, high,
         forward ? "" : ", backwards");
  return 0;
}

/**
 * @brief Reconstitute a frozen iterator
 *
 * @param graphd	module handle
 * @param it		iterator to thaw
 * @param pit 		text form
 * @param pib 		unused
 * @param it_out	rebuild the iterator here.
 * @param file		caller's source file
 * @param int		caller's source line
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_idset_position_thaw_loc(graphd_handle *graphd,
                                            pdb_iterator *it,
                                            pdb_iterator_text const *pit,
                                            cl_loglevel loglevel,
                                            char const *file, int line) {
  graphd_iterator_idset *ids = it->it_theory;
  pdb_handle *pdb = graphd->g_pdb;
  cl_handle *cl = graphd->g_cl;
  int err = 0;
  char const *s, *e;

  s = pit->pit_position_s;
  e = pit->pit_position_e;
  if (s != NULL && e != NULL) {
    unsigned long long llu1, llu2;

    err = pdb_iterator_util_thaw(pdb, &s, e, "%llu:%llu", &llu1, &llu2);
    if (err != 0) {
      cl_log(cl, loglevel,
             "graphd_iterator_idset_position_thaw_loc: "
             "can't thaw position \"%.*s\" [from %s:%d]",
             (int)(pit->pit_position_e - pit->pit_position_s),
             pit->pit_position_s, file, line);
      return err;
    }

    ids->ids_pos.gip_ull = llu1;
    ids->ids_pos.gip_size = llu2;
  } else {
    if (ids->ids_recover_callback == NULL) {
      if (pdb_iterator_forward(pdb, it))
        graph_idset_next_reset(ids->ids_set, &ids->ids_pos);
      else
        graph_idset_prev_reset(ids->ids_set, &ids->ids_pos);
    } else
      ids->ids_recover_reset = true;
  }
  return 0;
}

static int idset_recover(pdb_iterator *it, pdb_budget *budget_inout) {
  graphd_iterator_idset *oids, *ids;
  int err;

  if (it->it_original->it_type != &idset_iterator_type) return 0;

  oids = it->it_original->it_theory;
  if (oids->ids_recover_callback != NULL) {
    err = (*oids->ids_recover_callback)(oids->ids_recover_callback_data,
                                        oids->ids_graphd, &oids->ids_set,
                                        budget_inout);
    if (err != 0) return err;
    oids->ids_recover_callback = NULL;
    oids->ids_recover_callback_data = NULL;
  }

  ids = it->it_theory;
  if (ids->ids_recover_reset) {
    err = pdb_iterator_reset(oids->ids_pdb, it);
    if (err != 0) return err;
  }
  return 0;
}
