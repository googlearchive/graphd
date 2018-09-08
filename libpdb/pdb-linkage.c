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

#include <ctype.h>
#include <errno.h>
#include <stdio.h>

#include "libaddb/addb.h"

static char const* const linkage_name[] = {"type", "right", "left", "scope"};

char const* pdb_linkage_to_string(int linkage) {
  if (linkage < 0 || linkage >= 4) return "unknown linkage";
  return linkage_name[linkage];
}

int pdb_linkage_from_string(char const* s, char const* e) {
  if (s < e) switch (tolower(*s)) {
      case 'r':
        return PDB_LINKAGE_RIGHT;
        break;
      case 'l':
        return PDB_LINKAGE_LEFT;
        break;
      case 't':
        return PDB_LINKAGE_TYPEGUID;
        break;
      case 's':
        return PDB_LINKAGE_SCOPE;
        break;
    }

  return PDB_LINKAGE_N;
}

/* Return the GMAP coresponding to the passed linkage.
 *
 * If SCOPE and TYPEGUID are moved to HMAPS this switch will need
 * to move into the callers
 */
addb_gmap* pdb_linkage_to_gmap(pdb_handle* pdb, int linkage) {
  /*  Linkages and index values are identical up to PDB_LINKAGE_N.
   */
  if (linkage >= PDB_LINKAGE_TYPEGUID && linkage <= PDB_LINKAGE_SCOPE)
    return pdb->pdb_indices[linkage].ii_impl.gm;

  cl_notreached(pdb->pdb_cl, "pdb_linkage_to_gmap: bogus linkage %d", linkage);
  return (addb_gmap*)0;
}

/**
 * @brief How many links emerge from this node (bounded ID version)?
 *
 * @param pdb 		opaque database pointer, creatd with pdb_create()
 * @param linkage	which of the four linkage databases are we
 *			talking about?
 * @param source 	the local ID used to index it
 * @param low	 	start counting at this value
 * @param high	 	stop counting at this value
 * @param upper_bound	if more than this many, stop counting.
 * @param n_out		the number of outgoing links, or upper_bound
 *
 * @return
 */
int pdb_linkage_count(pdb_handle* pdb, int linkage, pdb_id source, pdb_id low,
                      pdb_id high, unsigned long long upper_bound,
                      unsigned long long* n_out) {
  addb_gmap* const gm = pdb_linkage_to_gmap(pdb, linkage);
  int err;

  cl_assert(pdb->pdb_cl, gm);

  err = pdb_count_gmap(pdb, gm, source, low, high, upper_bound, n_out);

  if (err)
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_count_gmap", err,
                 "Can't get size estimate for %s:%llx (%llx-%llx)",
                 pdb_linkage_to_string(linkage), (unsigned long long)source,
                 (unsigned long long)low, (unsigned long long)high);

  return err;
}

int pdb_linkage_count_est(pdb_handle* pdb, int linkage, pdb_id source,
                          pdb_id low, pdb_id high,
                          unsigned long long upper_bound,
                          unsigned long long* n_out) {
  int err;
  addb_gmap* const gm = pdb_linkage_to_gmap(pdb, linkage);

  cl_assert(pdb->pdb_cl, gm);

  err = pdb_count_gmap_est(pdb, gm, source, low, high, upper_bound, n_out);

  if (err)
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_count_gmap_est", err,
                 "Can't get size estimate for %s:%llx (%llx-%llx)",
                 pdb_linkage_to_string(linkage), (unsigned long long)source,
                 (unsigned long long)low, (unsigned long long)high);
  return err;
}

/**
 * @brief How many links emerge from this node
 *
 * @param pdb 		opaque database pointer, creatd with pdb_create()
 * @param linkage	which of the four linkage databases are we
 *			talking about?
 * @param source_guid 	the local ID used to index it
 * @param low	 	start counting at this value
 * @param high	 	stop counting at this value
 * @param upper_bound	if more than this many, stop counting.
 * @param n_out		the number of outgoing links, or upper_bound
 *
 * @return
 */
int pdb_linkage_guid_count_est(pdb_handle* pdb, int linkage,
                               graph_guid const* source_guid, pdb_id low,
                               pdb_id high, unsigned long long upper_bound,
                               unsigned long long* n_out) {
  pdb_id id;
  int err;

  err = pdb_id_from_guid(pdb, &id, source_guid);
  if (err != 0) return err;

  return pdb_linkage_count_est(pdb, linkage, id, low, high, upper_bound, n_out);
}

/**
 * @brief Add a type/primitive pair to the reverse type lookup table.
 *
 * @param pdb 		opaque database pointer, creatd with pdb_create()
 * @param linkage	which of the four linkage databases?
 * @param source 	source that maps to an ID set
 * @param id	 	id it maps to.
 *
 * @return 0 no success, otherwise a nonzero error number.
 */
static int pdb_linkage_add(pdb_handle* pdb, int linkage, pdb_id source,
                           pdb_id id) {
  addb_gmap* const gm = pdb_linkage_to_gmap(pdb, linkage);
  int err;

  cl_assert(pdb->pdb_cl, id != PDB_ID_NONE);
  cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
         "pdb_linkage_add: %s %llx -> primitive %llx",
         pdb_linkage_to_string(linkage), (unsigned long long)source,
         (unsigned long long)id);
  cl_cover(pdb->pdb_cl);

  pdb->pdb_runtime_statistics.rts_index_elements_written++;
  err = addb_gmap_add(gm, source, id, 0);
  if (err)
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "addb_gmap_add", err,
                 "pdb_linkage_add: %s %llx -> primitive %llx FAILS",
                 pdb_linkage_to_string(linkage), (unsigned long long)source,
                 (unsigned long long)id);

  return err;
}

/**
 * @brief Create an iterator that returns primitives in a type.
 *
 * @param pdb 		opaque pdb module handle
 * @param linkage 	linkage selector, PDB_LINKAGE_...
 * @param source	the id of the source
 * @param low 		local dateline low, start here
 * @param high 		local dateline high, end below here
 * @param forward	are we iterating low to high?
 * @param error_if_no	return PDB_ERR_NO if empty?
 * @param it_out 	out: the iterator
 *
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_linkage_id_iterator(pdb_handle* pdb, int linkage, pdb_id source,
                            pdb_id low, pdb_id high, bool forward,
                            bool error_if_null, pdb_iterator** it_out) {
  addb_gmap* const gm = pdb_linkage_to_gmap(pdb, linkage);

  cl_assert(pdb->pdb_cl, gm);
  return pdb_iterator_gmap_create(pdb, gm, linkage, source, low, high, forward,
                                  error_if_null, it_out);
}

/**
 * @brief Create an iterator that returns primitives with a certain linkage.
 *
 * @param pdb 		opaque pdb module handle
 * @param linkage 	linkage selector, PDB_LINKAGE_...
 * @param linkage_guid 	the GUID of the common type
 * @param low 		local dateline low, start here
 * @param high 		local dateline high, end below here
 * @param forward	sort order
 * @param error_if_null	if true, we prefer PDB_ERR_NO over a null iterator
 * @param it_out 	out: the iterator
 */
int pdb_linkage_iterator(pdb_handle* pdb, int linkage,
                         graph_guid const* linkage_guid, pdb_id low,
                         pdb_id high, bool forward, bool error_if_null,
                         pdb_iterator** it_out) {
  addb_gmap* const gm = pdb_linkage_to_gmap(pdb, linkage);
  pdb_id source;
  int err;

  cl_assert(pdb->pdb_cl, gm);
  cl_assert(pdb->pdb_cl, PDB_IS_LINKAGE(linkage));
  cl_assert(pdb->pdb_cl, linkage_guid);

  err = pdb_id_from_guid(pdb, &source, linkage_guid);
  if (err) {
    char buf[GRAPH_GUID_SIZE];

    if (err == PDB_ERR_NO)
      return error_if_null ? PDB_ERR_NO : pdb_iterator_null_create(pdb, it_out);

    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err,
                 "pdb_linkage_iterator(%s %s): no ID",
                 pdb_linkage_to_string(linkage),
                 graph_guid_to_string(linkage_guid, buf, sizeof buf));

    return err;
  }

  cl_log(pdb->pdb_cl, CL_LEVEL_SPEW, "pdb_linkage_iterator: * %s->%llx",
         pdb_linkage_to_string(linkage), (unsigned long long)source);

  return pdb_iterator_gmap_create(pdb, gm, linkage, source, low, high, forward,
                                  error_if_null, it_out);
}

/**
 * @brief Synchronize the various linkages
 *
 * @param pdb 		opaque pdb module handle
 * @param linkage 	PDB_LINKAGE_... selector
 * @param source_guid	GUID of the linkage field
 * @param id 		ID of the record that contains the key
 */
int pdb_linkage_synchronize(pdb_handle* pdb, pdb_id id,
                            pdb_primitive const* pr) {
  int linkage;
  int err;

  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++) {
    graph_guid g;
    pdb_id source;

    if (!pdb_primitive_has_linkage(pr, linkage)) continue;

    pdb_primitive_linkage_get(pr, linkage, g);
    cl_assert(pdb->pdb_cl, !GRAPH_GUID_IS_NULL(g));

    err = pdb_id_from_guid(pdb, &source, &g);
    if (err) {
      char buf1[GRAPH_GUID_SIZE];

      cl_log_errno(
          pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err,
          "pdb_linkage_synchronize: cannot resolve %llx.%s=%s to a local id",
          (unsigned long long)id, pdb_linkage_to_string(linkage),
          graph_guid_to_string(&g, buf1, sizeof buf1));
      return err;
    }

    err = pdb_linkage_add(pdb, linkage, source, id);
    if (err) return err;
  }

  return 0;
}
