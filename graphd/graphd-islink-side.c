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
#include "graphd/graphd-islink.h"

#include <errno.h>

/*  A side is part of the type state.
 *
 *  While a type is analysing itself, it keeps
 *  temporary results for each side.
 */

void graphd_islink_side_finish(graphd_handle* g, graphd_islink_side* side,
                               int result_linkage, pdb_id type_id) {
  if (side->side_idset != NULL) {
    graphd_islink_side_count* sc;

    graph_idset_free(side->side_idset);
    side->side_idset = NULL;

    sc = NULL;
    while ((sc = cm_hnext(&side->side_count, graphd_islink_side_count, sc)) !=
           NULL)
      if (sc->sc_idset != NULL) graph_idset_free(sc->sc_idset);
    cm_hashfinish(&side->side_count);
  }
}

int graphd_islink_side_initialize(graphd_handle* g, graphd_islink_side* side) {
  int err;

  side->side_idset = graph_idset_tile_create(g->g_graph);
  if (side->side_idset == NULL) {
    err = errno ? errno : ENOMEM;
    cl_log_errno(g->g_cl, CL_LEVEL_FAIL, "graph_idset_tile_create", errno,
                 "failed to allocate tile set");
    return err;
  }

  err = cm_hashinit(g->g_cm, &side->side_count,
                    sizeof(graphd_islink_side_count), 1024);
  if (err != 0) {
    cl_log_errno(g->g_cl, CL_LEVEL_FAIL, "cm_hashinit", errno,
                 "failed to allocate hashtable set");
    graph_idset_free(side->side_idset);
    side->side_idset = NULL;
    return err;
  }

  return 0;
}

/*  Given a side count slot, return the endpoint ID it's
 *  counting instances of.
 */
pdb_id graphd_islink_side_count_id(graphd_islink_side const* side,
                                   graphd_islink_side_count const* sc) {
  pdb_id id;

  if (sc == NULL) return PDB_ID_NONE;

  memcpy(&id, cm_hmem(&side->side_count, graphd_islink_side_count, sc),
         sizeof id);
  return id;
}

/*  Catch up with a group.
 *
 *  We're being fed all instances of a typeguid in ascending order.
 *  Up to <pr_id>, inclusive, we haven't bothered tracking those whose
 *  endpoint is <endpoint_id>.
 *
 *  We just decided that the IDs that point to this endpoint are forming
 *  a group, and that we *would* like to track them.  We'll add other IDs
 *  we hear about to the group centrally, but we need to catch up with
 *  the ones we didn't keep.
 */
static int islink_group_initialize(graphd_handle* g, pdb_id type_id,
                                   int fixed_endpoint_linkage,
                                   pdb_id fixed_endpoint_id, pdb_id last_id,
                                   graph_idset** idset_out) {
  cl_handle* cl = g->g_cl;
  pdb_handle* pdb = g->g_pdb;
  int err;
  int variable_endpoint_linkage;
  bool true_vip;
  pdb_iterator* vip_it;
  char buf[200];
  graph_idset* idset = NULL;
  graph_guid type_guid;

  variable_endpoint_linkage =
      (fixed_endpoint_linkage == PDB_LINKAGE_RIGHT ? PDB_LINKAGE_LEFT
                                                   : PDB_LINKAGE_RIGHT);

  cl_log(cl, CL_LEVEL_VERBOSE,
         "islink_group_initialize type=%llx, %s endpoint=%llx, last_id=%llx",
         (unsigned long long)type_id,
         pdb_linkage_to_string(fixed_endpoint_linkage),
         (unsigned long long)fixed_endpoint_id, (unsigned long long)last_id);

  err = pdb_id_to_guid(pdb, type_id, &type_guid);
  if (err != 0) return err;

  /* Look up the VIP iterator for the endpoint
   * and type.
   */
  err = pdb_vip_linkage_id_iterator(
      pdb, fixed_endpoint_id, fixed_endpoint_linkage, &type_guid,
      PDB_ITERATOR_LOW_ANY, PDB_ITERATOR_HIGH_ANY,
      /* forward	 */ true,
      /* error if null */ true, &vip_it, &true_vip);
  if (err != 0) {
    cl_log_errno(
        cl, CL_LEVEL_FAIL, "pdb_vip_linkage_id_iterator", err,
        "%s=%llx,type=%llx", pdb_linkage_to_string(fixed_endpoint_linkage),
        (unsigned long long)fixed_endpoint_id, (unsigned long long)type_id);
    return err;
  }

  /* Create an empty set.
   */
  idset = graph_idset_tile_create(g->g_graph);
  if (idset == NULL) {
    err = errno ? errno : ENOMEM;
    pdb_iterator_destroy(pdb, &vip_it);

    cl_log_errno(g->g_cl, CL_LEVEL_FAIL, "graph_idset_tile_create", errno,
                 "failed to allocate tile set");
    return err;
  }

  for (;;) {
    pdb_budget budget = 9999999;
    pdb_id id;
    pdb_primitive pr;
    pdb_id variable_endpoint_id;
    graph_guid variable_endpoint_guid;

    /* Pull another ID out of the iterator.
     */
    if ((err = pdb_iterator_next(pdb, vip_it, &id, &budget)) != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "it=%s",
                   pdb_iterator_to_string(pdb, vip_it, buf, sizeof buf));
      pdb_iterator_destroy(pdb, &vip_it);
      graph_idset_free(idset);

      return err;
    }

    /* We're done once we hit "last_id".
     */
    cl_assert(cl, id <= last_id);
    if (id >= last_id) break;

    /* Read the corresponding primitive.
     */
    err = pdb_id_read(pdb, id, &pr);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "it=%s",
                   pdb_iterator_to_string(pdb, vip_it, buf, sizeof buf));
      pdb_iterator_destroy(pdb, &vip_it);
      graph_idset_free(idset);

      return err;
    }

    /*  Get the other (non-fixed) endpoint of the
     *  primitive; that one will become a member of the group.
     */
    if (!pdb_primitive_has_linkage(&pr, variable_endpoint_linkage)) {
      pdb_primitive_finish(pdb, &pr);
      continue;
    }

    pdb_primitive_linkage_get(&pr, variable_endpoint_linkage,
                              variable_endpoint_guid);
    pdb_primitive_finish(pdb, &pr);

    /*  Convert the endpoint GUID into an ID.
     */
    err = pdb_id_from_guid(pdb, &variable_endpoint_id, &variable_endpoint_guid);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "it=%s",
                   pdb_iterator_to_string(pdb, vip_it, buf, sizeof buf));

      graph_idset_free(idset);
      pdb_iterator_destroy(pdb, &vip_it);

      return err;
    }

    /*  Add the ID to the set.
     */
    err = graph_idset_insert(idset, variable_endpoint_id);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graph_idset_insert", err, "id=%llx",
                   (unsigned long long)variable_endpoint_id);

      pdb_iterator_destroy(pdb, &vip_it);
      graph_idset_free(idset);

      return err;
    }
  }
  pdb_iterator_destroy(pdb, &vip_it);
  *idset_out = idset;

  return 0;
}

/* Add an ID to the side of a type.
 */
int graphd_islink_side_add(graphd_handle* g, graphd_islink_side* side,
                           int linkage, pdb_id side_id, pdb_id type_id,
                           pdb_id other_id, pdb_id pr_id) {
  cl_handle* cl = g->g_cl;
  int err;

  if (!graph_idset_check(side->side_idset, side_id)) {
    if (side->side_idset->gi_n >= GRAPHD_ISLINK_INTERESTING_MAX) {
      graphd_islink_side_finish(g, side, linkage, type_id);
      side->side_vast = true;

      cl_log(cl, CL_LEVEL_VERBOSE, "graphd_islink_side_add: vast %s(%llx)",
             pdb_linkage_to_string(linkage), (unsigned long long)type_id);
    } else {
      err = graph_idset_insert(side->side_idset, side_id);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graph_idset_add", err, "side_id=%llx",
                     (unsigned long long)side_id);
        return err;
      }
    }
  } else {
    graphd_islink_side_count* sc;

    /*  Allocate or access its counter.
     */
    sc = cm_hnew(&side->side_count, graphd_islink_side_count, &side_id,
                 sizeof(side_id));
    if (sc == NULL) {
      err = errno ? errno : ENOMEM;
      cl_log_errno(cl, CL_LEVEL_FAIL, "cm_hnew", err, "side_id=%llx",
                   (unsigned long long)side_id);
      return err;
    }

    /*  The first time, we count both the increment
     *  and the pre-existing idset entry.
     */
    if (sc->sc_count == 0)
      sc->sc_count = 2;
    else
      sc->sc_count++;

    if (sc->sc_count < GRAPHD_ISLINK_INTERESTING_MIN) return 0;

    if (sc->sc_count > GRAPHD_ISLINK_INTERESTING_MAX) {
      if (sc->sc_idset == NULL) return 0;

      /*  There are so many instance sides pointing
       *  to this particular endpoint, with so many
       *  IDs on the other side, that caching them
       *  is pretty pointless - you could probably just
       *  pick an instance at random and find one.
       *
       *  So, let's not bother.
       */
      if (sc->sc_idset->gi_n >= GRAPHD_ISLINK_INTERESTING_MAX) {
        graph_idset_free(sc->sc_idset);
        sc->sc_idset = NULL;

        return 0;
      }
    }

    /*  There are so many instances' sides pointing
     *  to this particular endpoint that we should
     *  place all those that do in a group.
     */
    if (sc->sc_idset == NULL) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "idset==NULL; count >= %llu; "
             "create a new group",
             (unsigned long long)sc->sc_count);

      /*  We're new to this; count up to here.
       */
      err = islink_group_initialize(g, type_id, linkage, side_id, pr_id,
                                    &sc->sc_idset);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "islink_group_initialize", err,
                     "side_id=%llx", (unsigned long long)side_id);
        return err;
      }
    }

    cl_assert(cl, sc->sc_idset != NULL);
    err = graph_idset_insert(sc->sc_idset, other_id);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graph_idset_insert", err,
                   "other_id=%llx", (unsigned long long)other_id);
      return err;
    }
  }
  return 0;
}

/*  Finish collecting the side of a type for the
 *  first time.
 *
 *  Later, we'll incrementally update these.
 */
int graphd_islink_side_complete(graphd_handle* g, graphd_islink_side* side,
                                int result_linkage, pdb_id type_id) {
  graphd_islink_key key;
  graphd_islink_side_count* sc;
  int err;

  /*  If we are vast, we don't store anything about ourselves.
   *  There were just too many (unique) results to do anything
   *  meaningful here.
   */
  if (side->side_vast) return 0;

  /*  Opportunistically store our side set.
   *
   *  If it's small, it'll be difficult to construct because the
   *  endpoints will need finding.
   *
   *  If it's large, it'll be difficult to construct because,
   *  well, it's large.
   */
  graphd_islink_key_make(g, result_linkage, type_id, PDB_ID_NONE, &key);
  err = graphd_islink_group_create(g, &key, side->side_idset);
  if (err != 0) return err;

  side->side_group = true;

  /*  Store those incoming cones that became
   *  large enough to grow their own sets.
   */
  sc = NULL;
  while ((sc = cm_hnext(&side->side_count, graphd_islink_side_count, sc)) !=
         NULL) {
    if (sc->sc_idset == NULL) {
      cl_log(g->g_cl, CL_LEVEL_VERBOSE,
             "graphd_islink_side_complete: %s(type=%llx) "
             "= %llx: count=%llu, too small.",
             pdb_linkage_to_string(result_linkage), (unsigned long long)type_id,
             (unsigned long long)graphd_islink_side_count_id(side, sc),
             (unsigned long long)sc->sc_count);
      continue;
    }

    cl_log(g->g_cl, CL_LEVEL_VERBOSE,
           "graphd_islink_side_complete: %s(type=%llx) "
           "= %llx: count=%llu, n=%llu, creating group.",
           pdb_linkage_to_string(result_linkage), (unsigned long long)type_id,
           (unsigned long long)graphd_islink_side_count_id(side, sc),
           (unsigned long long)sc->sc_count,
           (unsigned long long)sc->sc_idset->gi_n);

    graphd_islink_key_make(
        g, result_linkage == PDB_LINKAGE_RIGHT ? PDB_LINKAGE_LEFT
                                               : PDB_LINKAGE_RIGHT,
        type_id, graphd_islink_side_count_id(side, sc), &key);

    err = graphd_islink_group_create(g, &key, sc->sc_idset);
    if (err != 0) return err;
  }
  return 0;
}
