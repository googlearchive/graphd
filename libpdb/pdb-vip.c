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
#include "libpdb/pdbp.h"

#include <errno.h>
#include <stdio.h>

#include "libaddb/addb.h"

/*  The composite key used to retrieve the VIP index array
 */

typedef struct pdb_vip_key {
  addb_u5 vik_id;      /* id being indexed */
  addb_u1 vik_linkage; /* linkage */
  addb_u5 vik_type;    /* id of typeguid */
} pdb_vip_key;

#define PDB_VIK_ID(B__) ADDB_GET_U5((B__)->vik_id)
#define PDB_VIK_ID_SET(B__, V__) ADDB_PUT_U5((B__)->vik_id, (V__))

#define PDB_VIK_TYPE(B__) ADDB_GET_U5((B__)->vik_type)
#define PDB_VIK_TYPE_SET(B__, V__) ADDB_PUT_U5((B__)->vik_type, (V__))

/*  Given a VIP hmap, return its primitive summary.
 */
int pdb_vip_hmap_primitive_summary(pdb_handle* pdb, char const* key,
                                   size_t size,
                                   pdb_primitive_summary* psum_out) {
  pdb_vip_key vik;
  pdb_id source_id, type_id;
  int err;

  cl_assert(pdb->pdb_cl, size == sizeof(vik));

  memcpy(&vik, key, sizeof(vik));

  source_id = PDB_VIK_ID(&vik);
  type_id = PDB_VIK_TYPE(&vik);

  err = pdb_id_to_guid(pdb, source_id, psum_out->psum_guid + vik.vik_linkage);
  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_id_to_guid", err,
                 "vik source_id=%lld", (long long)source_id);
    return err;
  }
  err =
      pdb_id_to_guid(pdb, type_id, psum_out->psum_guid + PDB_LINKAGE_TYPEGUID);
  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_id_to_guid", err,
                 "vik source_id=%lld", (long long)source_id);
    return err;
  }
  psum_out->psum_locked = (1 << PDB_LINKAGE_TYPEGUID) | (1 << vik.vik_linkage);
  psum_out->psum_result = PDB_LINKAGE_N;
  psum_out->psum_complete = true;

  return 0;
}

/*  I'm open to suggestions on this one.  I'm trying to
 *  keep this pretty local --  if the endpoints don't go
 *  all over the place, neither does the hash, so that we
 *  can run with as few partitions as the "to-node" and
 *  "from-node" tables.
 */
static unsigned long long pdb_vip_hash(pdb_id id, int linkage, pdb_id type_id) {
  return ((1ull << 34) - 1) &
         ((unsigned long long)id ^ (linkage ^ 3) ^ type_id);
}

/**
 * @brief How many links are likely to have this endpoint/typeguid combination?
 *
 * @param pdb opaque database pointer, creatd with pdb_create()
 * @param id the local ID of the links' common source.
 * @param linkage PDB_LINKAGE_LEFT or PDB_LINKAGE_RIGHT, depending on whether
 *	the popular primitive is on the right or on the left of the links.
 * @param qualifier the typeguid of the link.
 * @param low The lower bound of the numeric result range
 * @param high The upper bound (first value not included) of the result range.
 * @param upper_bound we're only interested in the result if it's less than
 * 		this many.
 * @param n_out	the number of entries that an iterator for this set
 *	of criteria would return.
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_vip_id_count(pdb_handle* pdb, pdb_id id, int linkage,
                     graph_guid const* qualifier, pdb_id low, pdb_id high,
                     unsigned long long upper_bound,
                     unsigned long long* n_out) {
  pdb_id const type_id = GRAPH_GUID_SERIAL(*qualifier);
  pdb_vip_key vik;

  PDB_VIK_ID_SET(&vik, id);
  vik.vik_linkage = linkage;
  PDB_VIK_TYPE_SET(&vik, type_id);

  return pdb_count_hmap(pdb, pdb->pdb_hmap, pdb_vip_hash(id, linkage, type_id),
                        (char*)&vik, sizeof vik, addb_hmt_vip, low, high,
                        upper_bound, n_out);
}

/**
 * @brief Create an iterator that returns links matching a VIP pattern.
 *
 * @param pdb 		opaque pdb module handle
 * @param source 	the node from which the links radiate
 * @param linkage	linkage of that node relative to the links
 * @param qualifier	the GUID the links have in common
 * @param low 		low end of the link ids, included
 * @param high		high end of the link ids, excluded
 * @param forward	sort low to high?
 * @param it_out 	out: the iterator
 *
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_vip_id_iterator(pdb_handle* pdb, pdb_id source, int linkage,
                        graph_guid const* qualifier, pdb_id low, pdb_id high,
                        bool forward, bool error_if_null,
                        pdb_iterator** it_out) {
  pdb_id const type_id = GRAPH_GUID_SERIAL(*qualifier);
  pdb_vip_key vik;

  PDB_VIK_ID_SET(&vik, source);
  vik.vik_linkage = linkage;
  PDB_VIK_TYPE_SET(&vik, type_id);

  return pdb_iterator_hmap_create(
      pdb, pdb->pdb_hmap, pdb_vip_hash(source, linkage, type_id), (char*)&vik,
      sizeof vik, addb_hmt_vip, low, high, forward, error_if_null, it_out);
}

/**
 * @brief Create an iterator that returns links matching a VIP pattern.
 *
 * @param pdb 		opaque pdb module handle
 * @param node	 	the node from which the links radiate
 * @param linkage	node's position relative to the links
 * @param qualifier	typeguid
 * @param low		low local ID boundary for results
 * @param high		high local ID boundary for results
 * @param forward	sort low to high if possible.
 * @param it_out 	out: the iterator
 *
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_vip_iterator(pdb_handle* pdb, graph_guid const* node, int linkage,
                     graph_guid const* qualifier, pdb_id low, pdb_id high,
                     bool forward, bool error_if_null, pdb_iterator** it_out) {
  pdb_id source;
  int err;
  pdb_id const type_id = GRAPH_GUID_SERIAL(*qualifier);
  pdb_vip_key vik;

  if ((err = pdb_id_from_guid(pdb, &source, node)) != 0) return err;

  PDB_VIK_ID_SET(&vik, source);
  vik.vik_linkage = linkage;
  PDB_VIK_TYPE_SET(&vik, type_id);

  return pdb_iterator_hmap_create(
      pdb, pdb->pdb_hmap, pdb_vip_hash(source, linkage, type_id), (char*)&vik,
      sizeof vik, addb_hmt_vip, low, high, forward, error_if_null, it_out);
}

static int pdb_vip_synchronize_add(pdb_handle* pdb, pdb_id id,
                                   pdb_id endpoint_id, int linkage,
                                   pdb_id type_id) {
  unsigned long long h = pdb_vip_hash(endpoint_id, linkage, type_id);
  pdb_vip_key vik;
  int err;

  PDB_VIK_ID_SET(&vik, endpoint_id);
  vik.vik_linkage = linkage;
  PDB_VIK_TYPE_SET(&vik, type_id);

  cl_enter(pdb->pdb_cl, CL_LEVEL_SPEW, "(%llx.%s(%llx)=%llx)",
           (unsigned long long)id, pdb_linkage_to_string(linkage),
           (unsigned long long)type_id, (unsigned long long)endpoint_id);

  pdb->pdb_runtime_statistics.rts_index_elements_written++;
  err = addb_hmap_add(pdb->pdb_hmap, h, (char*)&vik, sizeof vik, addb_hmt_vip,
                      id);
  if (err)
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_hmap_add", err,
                 "Can't add vip %llx -> %llx", h, (unsigned long long)id);

  cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "%s", err ? pdb_strerror(err) : "ok");
  return err;
}

static int pdb_vip_transition(pdb_handle* pdb, pdb_id endpoint_id,
                              int linkage) {
  addb_gmap* const gm = pdb_linkage_to_gmap(pdb, linkage);
  pdb_iterator* it;
  pdb_id link_id;
  int err;

  cl_enter(pdb->pdb_cl, CL_LEVEL_SPEW, "(*.%s=%llx)",
           pdb_linkage_to_string(linkage), (unsigned long long)endpoint_id);
  cl_assert(pdb->pdb_cl,
            linkage == PDB_LINKAGE_LEFT || linkage == PDB_LINKAGE_RIGHT);

  /*  Get an iterator for the links connected to the
   *  existing endpoints.
   */
  err = pdb_iterator_gmap_create(pdb, gm, linkage, endpoint_id,
                                 PDB_ITERATOR_LOW_ANY, PDB_ITERATOR_HIGH_ANY,
                                 true,
                                 /* error if null */ false, &it);
  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_iterator_gmap_create", err,
                 "gmap %s(%llx)", pdb_linkage_to_string(linkage),
                 (unsigned long long)endpoint_id);
    return err;
  }

  /*  For all links connected to the endpoint...
   */
  while ((err = pdb_iterator_next_nonstep(pdb, it, &link_id)) == 0) {
    pdb_primitive link_pr;

    if ((err = pdb_id_read(pdb, link_id, &link_pr)) != 0) {
      if (err == PDB_ERR_NO) continue;

      cl_leave(pdb->pdb_cl, CL_LEVEL_FAIL,
               "unexpected error from "
               "pdb_id_read: %s",
               pdb_strerror(err));

      pdb_iterator_destroy(pdb, &it);
      return err;
    }

    if (pdb_primitive_has_typeguid(&link_pr)) {
      graph_guid typeguid;
      pdb_id type_id;

      pdb_primitive_typeguid_get(&link_pr, typeguid);
      type_id = GRAPH_GUID_SERIAL(typeguid);
      err =
          pdb_vip_synchronize_add(pdb, link_id, endpoint_id, linkage, type_id);
      if (err != 0) {
        pdb_primitive_finish(pdb, &link_pr);
        pdb_iterator_destroy(pdb, &it);

        cl_leave(pdb->pdb_cl, CL_LEVEL_FAIL,
                 "unexpected error from "
                 "pdb_vip_synchronize_add: %s",
                 pdb_strerror(err));
        return err;
      }
    }
    pdb_primitive_finish(pdb, &link_pr);
  }
  pdb_iterator_destroy(pdb, &it);

  cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "%s",
           err == PDB_ERR_NO ? "done" : pdb_strerror(err));

  return err == PDB_ERR_NO ? 0 : err;
}

static int pdb_vip_sync_linkage(pdb_handle* pdb, pdb_id id,
                                pdb_primitive const* pr, pdb_id type_id,
                                int linkage) {
  char buf[GRAPH_GUID_SIZE];
  graph_guid endpoint_guid;
  pdb_id endpoint_id;
  int err;

  if (!pdb_primitive_has_linkage(pr, linkage)) return 0;

  pdb_primitive_linkage_get(pr, linkage, endpoint_guid);
  err = pdb_id_from_guid(pdb, &endpoint_id, &endpoint_guid);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_vip_sync_linkage", err,
                 "can't get id from %llx.%s=%s", (unsigned long long)id,
                 pdb_linkage_to_string(linkage),
                 graph_guid_to_string(&endpoint_guid, buf, sizeof buf));
    return err;
  }
  return pdb_vip_add(pdb, endpoint_id, linkage, type_id, id);
}

/**
 * @brief Synchronize the "vip" table.
 *
 *  Primitive #id, pr, is being added to the database.
 *  Update the VIP index.
 *
 *  The VIP table maps endpoint (left or right) and typeguid
 *  to GUIDs of links that have them.  They are kept only for
 *  endpoints with more than a certain number of links from
 *  or to them -- very important primitives.
 *
 * @param pdb 	opaque pdb module handle
 * @param id	local ID of the passed-in primitive
 * @param pr 	passed-in primitive
 *
 * @return 0 on success, a nonzero error code on failure.
 */
int pdb_vip_synchronize(pdb_handle* pdb, pdb_id id, pdb_primitive const* pr) {
  pdb_id type_id;
  int err;

  if (pdb_primitive_has_typeguid(pr)) {
    graph_guid typeguid;

    pdb_primitive_typeguid_get(pr, typeguid);
    type_id = GRAPH_GUID_SERIAL(typeguid);
  } else
    type_id = PDB_ID_NONE;

  err = pdb_vip_sync_linkage(pdb, id, pr, type_id, PDB_LINKAGE_RIGHT);
  if (err) return err;

  err = pdb_vip_sync_linkage(pdb, id, pr, type_id, PDB_LINKAGE_LEFT);
  if (err) return err;

  return 0;
}

bool pdb_vip_is_endpoint_id(pdb_handle* pdb, pdb_id endpoint_id, int linkage,
                            graph_guid const* qualifier) {
  unsigned long long n;
  int err =
      pdb_linkage_count_est(pdb, linkage, endpoint_id, PDB_ITERATOR_LOW_ANY,
                            PDB_ITERATOR_HIGH_ANY, PDB_COUNT_UNBOUNDED, &n);

  return err == 0 && n >= PDB_VIP_MIN;
}

/**
 * @brief Like pdb_linkage_count(.., PDB_LINKAGE_RIGHT, ...),
 *  	but take a shortcut if one is possible.
 *
 * @param pdb opaque database pointer, creatd with pdb_create()
 * @param node_id the local ID of the links' common source.
 * @param qualifier	NULL or pointer to a typeguid
 * @param low		lower bound of the result set
 * @param high		upper bound (first value not included) of results
 * @param upper_bound	we only care if it's fewer than this many
 * @param n_out 	out: the number of entries that an iterator for this
 *node's
 *	guid_vip_to_node_set() would return.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_vip_linkage_id_count(pdb_handle* pdb, pdb_id node_id, int linkage,
                             graph_guid const* qualifier, pdb_id low,
                             pdb_id high, unsigned long long upper_bound,
                             unsigned long long* n_out) {
  addb_gmap* const gm = pdb_linkage_to_gmap(pdb, linkage);
  unsigned long long n;
  int err;

  pdb->pdb_runtime_statistics.rts_index_extents_read++;

  err = addb_gmap_array_n(gm, node_id, &n);
  if (err != 0) return err;

  if (n >= PDB_VIP_MIN && qualifier != NULL &&
      !GRAPH_GUID_IS_NULL(*qualifier)) {
    unsigned long long n2;
    int err;

    err = pdb_vip_id_count(pdb, node_id, linkage, qualifier, low, high,
                           upper_bound, &n2);

    if (err != 0) return err;

    if (n2 < n) n = n2;

  } else if (low != PDB_ITERATOR_LOW_ANY || high != PDB_ITERATOR_HIGH_ANY) {
    int err;
    err = pdb_linkage_count_est(pdb, linkage, node_id, low, high, upper_bound,
                                &n);
    if (err != 0) return err;
  }
  *n_out = n;
  return 0;
}

/**
 * @brief Like pdb_vip_linkage_id_count, but with an endpoint GUID.
 *
 * @param pdb opaque database pointer, creatd with pdb_create()
 * @param node_guid the GUID of the links' common source.
 * @param qualifier	NULL or pointer to a typeguid
 * @param low		lower bound of the result set
 * @param high		upper bound (first value not included) of results
 * @param upper_bound	we only care if it's fewer than this many
 * @param n_out 	out: the number of entries that an iterator for this
 *node's
 *	guid_vip_to_node_set() would return.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_vip_linkage_guid_count(pdb_handle* pdb, graph_guid const* node_guid,
                               int linkage, graph_guid const* qualifier,
                               pdb_id low, pdb_id high,
                               unsigned long long upper_bound,
                               unsigned long long* n_out) {
  pdb_id id;
  int err;

  err = pdb_id_from_guid(pdb, &id, node_guid);
  if (err != 0) return err;

  return pdb_vip_linkage_id_count(pdb, id, linkage, qualifier, low, high,
                                  upper_bound, n_out);
}

/**
 * @brief Is this id a VIP?
 *
 * @param pdb 		opaque pdb module handle
 * @param source	the node from which the links radiate
 * @param linkage	direction in which they radiate
 * @param vip_out	set to true or false
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_vip_id(pdb_handle* pdb, pdb_id source, int linkage, bool* vip_out) {
  addb_gmap* gm;
  int err = 0;
  unsigned long long n;

  /* VIP maps exist only for left or right endpoints.
   */
  if (linkage != PDB_LINKAGE_RIGHT && linkage != PDB_LINKAGE_LEFT) {
    *vip_out = false;
    return 0;
  }

  gm = pdb_linkage_to_gmap(pdb, linkage);
  pdb->pdb_runtime_statistics.rts_index_extents_read++;

  if ((err = addb_gmap_array_n(gm, source, &n)) != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_gmap_array_n", err,
                 "unexpected error");
    return err;
  }

  *vip_out = n >= PDB_VIP_MIN;
  return 0;
}

/**
 * @brief Create an iterator that returns links from a node.
 *
 * @param pdb 		opaque pdb module handle
 * @param node	 	the node from which the links radiate
 * @param qualifier	NULL or pointer to a typeguid
 * @param low		lower bound of the result set
 * @param high		upper bound (first value not included) of results
 * @param forward	sort low to high
 * @param error_if_null	if true, don't bother creating a null iterator
 * @param it_out 	out: the iterator
 * @param true_vip_out	out: set to true if actually a VIP
 *
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_vip_linkage_id_iterator(pdb_handle* pdb, pdb_id source, int linkage,
                                graph_guid const* qualifier, pdb_id low,
                                pdb_id high, bool forward, bool error_if_null,
                                pdb_iterator** it_out, bool* true_vip_out) {
  addb_gmap* const gm = pdb_linkage_to_gmap(pdb, linkage);
  int err = 0;
  unsigned long long n;
  char buf[200];
  cl_enter(pdb->pdb_cl, CL_LEVEL_SPEW, "%s=%llx typeguid=%s",
           pdb_linkage_to_string(linkage), (unsigned long long)source,
           graph_guid_to_string(qualifier, buf, sizeof buf));

  if (qualifier != NULL && !GRAPH_GUID_IS_NULL(*qualifier)) {
    pdb->pdb_runtime_statistics.rts_index_extents_read++;

    err = addb_gmap_array_n(gm, source, &n);
    if (err != 0) {
      cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "%s", pdb_strerror(err));
      return err;
    }

    if (n >= PDB_VIP_MIN) {
      pdb_id type_id = GRAPH_GUID_SERIAL(*qualifier);
      pdb_vip_key vik;
      int err;

      PDB_VIK_ID_SET(&vik, source);
      vik.vik_linkage = linkage;
      PDB_VIK_TYPE_SET(&vik, type_id);

      err = pdb_iterator_hmap_create(pdb, pdb->pdb_hmap,
                                     pdb_vip_hash(source, linkage, type_id),
                                     (char*)&vik, sizeof vik, addb_hmt_vip, low,
                                     high, forward, error_if_null, it_out);
      if (err == 0) {
        if (true_vip_out != NULL) *true_vip_out = true;

        cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "native hmap");
        return 0;
      }

      if (!error_if_null)
        cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
               "Can't get a native vip for %llx:%llx %s",
               (unsigned long long)source, (unsigned long long)type_id,
               pdb_strerror(err));
      cl_leave_err(pdb->pdb_cl, CL_LEVEL_SPEW, err, "(vip)");
      return err;
    }
  }

  err = pdb_iterator_gmap_create(pdb, gm, linkage, source, low, high, forward,
                                 error_if_null, it_out);

  cl_leave_err(pdb->pdb_cl, CL_LEVEL_SPEW, err, "%s",
               err ? pdb_strerror(err)
                   : pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
  return err;
}

/**
 * @brief Create an iterator that returns links from a node.
 *
 * @param pdb 		opaque pdb module handle
 * @param node	 	the node from which the links radiate
 * @param qualifier	NULL or pointer to a typeguid
 * @param low		lower bound of the result set
 * @param high		upper bound (first value not included) of results
 * @param forward	sort low to high
 * @param it_out 	out: the iterator
 * @param true_vip_out	out: set to true if actually a VIP
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_vip_linkage_iterator(pdb_handle* pdb, graph_guid const* node,
                             int linkage, graph_guid const* qualifier,
                             pdb_id low, pdb_id high, bool forward,
                             bool error_if_null, pdb_iterator** it_out,
                             bool* true_vip_out) {
  addb_gmap* const gm = pdb_linkage_to_gmap(pdb, linkage);
  pdb_id source;
  int err = 0;

  cl_enter(pdb->pdb_cl, CL_LEVEL_SPEW, "(%s)", pdb_linkage_to_string(linkage));

  if (true_vip_out != NULL) *true_vip_out = false;
  cl_assert(pdb->pdb_cl, gm);
  if ((err = pdb_id_from_guid(pdb, &source, node)) != 0) {
    cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "error in pdb_id_from_guid: %s",
             pdb_strerror(err));
    return err;
  }
  err =
      pdb_vip_linkage_id_iterator(pdb, source, linkage, qualifier, low, high,
                                  forward, error_if_null, it_out, true_vip_out);

  cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "%s",
           err ? pdb_strerror(err)
               : (true_vip_out ? (*true_vip_out ? "created vip" : "faked it")
                               : "ok"));
  return err;
}

/**
 * @brief A new primitive is being added.  Update the index.
 *
 * @param pdb 		opaque pdb module handle
 * @param endpoint_id	endpoint's local id
 * @param linkage	direction of the endpoint
 * @param type_id 	local id of typeguid
 * @param link_id 	local id of the indexed link.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_vip_add(pdb_handle* pdb, pdb_id endpoint_id, int linkage,
                pdb_id type_id, pdb_id link_id) {
  unsigned long long n;
  int err = 0;

  cl_enter(pdb->pdb_cl, CL_LEVEL_SPEW, "(%s=%llx, typeguid=%llx, link=%llx)",
           pdb_linkage_to_string(linkage), (unsigned long long)endpoint_id,
           (unsigned long long)type_id, (unsigned long long)link_id);

  pdb->pdb_runtime_statistics.rts_index_extents_read++;
  err = pdb_linkage_count_est(pdb, linkage, endpoint_id, PDB_ITERATOR_LOW_ANY,
                              PDB_ITERATOR_HIGH_ANY, PDB_COUNT_UNBOUNDED, &n);
  if (err) {
    cl_leave(pdb->pdb_cl, CL_LEVEL_FAIL, "error getting array size: %s",
             pdb_strerror(err));
    return err;
  }
  cl_log(pdb->pdb_cl, CL_LEVEL_SPEW, "testing for vipage: %llx %llu",
         (unsigned long long)endpoint_id, n);
  if (n < PDB_VIP_MIN - 1) {
    cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "only %llu entr%s",
             (unsigned long long)n, n == 1 ? "y" : "ies");
    return 0;
  }

  if (PDB_VIP_MIN - 1 == n) {
    cl_log(pdb->pdb_cl, CL_LEVEL_DETAIL,
           "making new vip table; endpoint: %llx type: %llx link: %llx",
           (unsigned long long)endpoint_id, (unsigned long long)type_id,
           (unsigned long long)link_id);
    err = pdb_vip_transition(pdb, endpoint_id, linkage);

    if (err) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_vip_transition", err,
                   "Failed to make a new vip entry from "
                   "endpoint: %llx, linkage %i",
                   (unsigned long long)endpoint_id, linkage);
    }
  } else if (type_id != PDB_ID_NONE)
    err = pdb_vip_synchronize_add(pdb, link_id, endpoint_id, linkage, type_id);
  cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "%s", err ? pdb_strerror(err) : "done");

  return err;
}
