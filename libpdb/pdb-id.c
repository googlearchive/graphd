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
#include <string.h>

#define ISDIGIT(x) (isascii(x) && isdigit(x))

/* There is a GMAP, pdb_key, that serves two functions:
 *
 *  - it translates external GUIDs to PDB IDs
 *
 *  - it translates local IDs of multi-versioned objects
 *    to local IDs of the version chain's first instance.
 *
 *  An application key has the shape of a GUID, but has a sequence number
 *  of 0.  All GUIDs that are versions of the same application object
 *  share the same application key.
 *
 *  All still true except for the GMAP part.  Mapping is handled by
 *  the HMAP with type addb_hmt_key.
 */

/**
 *  Hash a GUID or application key to a 34-bit value.
 *
 *  We're populating the top 10 bits of the 34-bit hash from the
 *  serial number only, in order to provide some locality, specifically,
 *  in order to not allocate more than one partition for
 *  this table when hashing objects with low (< 2^24) serial numbers.
 */

static unsigned long long pdb_id_guid_hash34(graph_guid const *guid) {
  if (GRAPH_GUID_SERIAL(*guid) == 0) {
    return ((1ull << 34) - 1) & ((0xFFFFFFull & ((GRAPH_GUID_DB(*guid) >> 24) ^
                                                 GRAPH_GUID_DB(*guid))));
  } else {
    return ((1ull << 34) - 1) & (GRAPH_GUID_SERIAL(*guid) ^
                                 (0xFFFFFFull & ((GRAPH_GUID_DB(*guid) >> 24) ^
                                                 GRAPH_GUID_DB(*guid))));
  }
}

/**
 * @brief Given a PDB ID, read the corresponding primitive.
 *
 *  The PDB ID is smaller than the GUID and local to one datastore; it
 *  can be used to index the grpahd istore database file, and obtain
 *  the primitive data in constant time.
 *
 *  This is invoked as pdb_id_read() via a macro declared in pdb.h.
 *
 * @param pdb 	opaque module handle
 * @param id 	primitive id
 * @param id 	out: the primitive data
 * @param file	the file name of the calling code (for reference tracking)
 * @param line	the line number of the calling code (for reference tracking)
 *
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_id_read_loc(pdb_handle *pdb, pdb_id id, pdb_primitive *pr,
                    char const *file, int line) {
  /*  Look up the ID <id>.
   */
  int err;
  char const *errstr;
  char buf[200];

  if (pdb->pdb_primitive == NULL) {
    cl_log(pdb->pdb_cl, CL_LEVEL_DEBUG, "no database loaded yet; initialzing");
    if ((err = pdb_initialize(pdb)) != 0) {
      cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
             "pdb_primitive_read: initialize fails: %s", pdb_xstrerror(err));
      return err;
    }
  }

  pdb->pdb_runtime_statistics.rts_primitives_read++;
  err = addb_istore_read_loc(pdb->pdb_primitive, id, &pr->pr_data, file, line);
  if (err != 0) {
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
           "pdb_id_read_loc: addb_istore_read (%llx) "
           "fails: %s [from %s:%d]",
           (unsigned long long)id, pdb_xstrerror(err), file, line);
    return err;
  } else if ((errstr = pdb_primitive_check(pr, buf, sizeof buf)) != NULL) {
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb_id_read_loc: pdb_primitive_check (%llx) "
           "fails: database corrupt: %s [from %s:%d]",
           (unsigned long long)id, errstr, file, line);
    err = PDB_ERR_DATABASE;
  }

  pr->pr_database_guid = &pdb->pdb_database_guid;

  if (pdb_primitive_has_external_guid(pr)) {
    pdb_primitive_get_external_guid(pr, pr->pr_guid);
  } else {
    graph_guid_from_db_serial(&pr->pr_guid, pdb->pdb_database_id, id);
  }

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "pdb_id_read %llx [from %s:%d]",
         (unsigned long long)id, file, line);
  return err;
}

/**
 * @brief Given a PDB ID, look up the record's GUID.
 *
 *  This just uses the ID to look up the record and return the record's
 *  GUID.  If the calling code needs to obtain the primitive data as well,
 *  it's more efficient to call pdb_id_read() instead.
 *
 *  This is invoked as pdb_id_to_guid() via a macro declared in pdb.h.
 *
 * @param pdb 		opaque module handle
 * @param id 		primitive id
 * @param guid_out 	out: the GUID
 * @param file		the file name of the calling code
 * @param line		the line number of the calling code
 *
 * @return 0 on success, a nonzero error code on error.
 */

int pdb_id_to_guid_loc(pdb_handle *pdb, pdb_id id, graph_guid *guid_out,
                       char const *file, int line) {
  /*  Look up the ID <id>. */
  pdb_primitive pr;
  int err;

  err = pdb_id_read_loc(pdb, id, &pr, file, line);
  if (err == 0) {
    pdb_primitive_guid_get(&pr, *guid_out);
    addb_istore_free(pdb->pdb_primitive, &pr.pr_data);
  }
  return err;
}

/**
 * @brief Given a GUID, get the site-local PDB ID.
 *
 * @param pdb 	opaque module handle
 * @param id 	out: primitive id
 * @param guid 	in: the global GUID
 *
 * @return 0 success (and *id contains the local ID)
 * @return ADDB_ERR_NO if the GUID was neither local nor known
 * @return other nonzero error codes on system error.
 */
int pdb_id_from_guid(pdb_handle *pdb, pdb_id *id_out, graph_guid const *guid) {
  pdb_id h;
  addb_hmap_iterator hm_it;
  int err;

  /* If the GUID is local, just return the embedded serial number.
   */
  if (PDB_GUID_IS_LOCAL(pdb, *guid)) {
    if (GRAPH_GUID_SERIAL(*guid) >= addb_istore_next_id(pdb->pdb_primitive)) {
      char buf[GRAPH_GUID_SIZE];
      addb_id max_id = addb_istore_next_id(pdb->pdb_primitive);

      if (max_id == 0)
        cl_log(pdb->pdb_cl,
               (max_id == GRAPH_GUID_SERIAL(*guid) ? CL_LEVEL_VERBOSE
                                                   : CL_LEVEL_FAIL),
               "pdb_id_from_guid: local GUID %s out "
               "of range",
               graph_guid_to_string(guid, buf, sizeof buf));

      else
        cl_log(pdb->pdb_cl,
               (max_id == GRAPH_GUID_SERIAL(*guid) ? CL_LEVEL_VERBOSE
                                                   : CL_LEVEL_FAIL),
               "pdb_id_from_guid: local GUID %s out "
               "of range -- our maximum allocated ID is %llx",
               graph_guid_to_string(guid, buf, sizeof buf),
               (unsigned long long)max_id - 1);
      return ADDB_ERR_NO;
    }

    *id_out = GRAPH_GUID_SERIAL(*guid);
    return 0;
  }

  /*  If we have a concentric graph translation table,
   *  use that.
   */
  if (pdb->pdb_concentric_map != NULL) {
    graph_guid local_guid;
    graph_grmap_map(pdb->pdb_concentric_map, guid, &local_guid);
    return GRAPH_GUID_SERIAL(local_guid);
  }

  /*  Hash the GUID we're looking up into something we can
   *  index the key table with.
   */
  h = pdb_id_guid_hash34(guid);

  /*  Look up this key in the hmap.  There should be exactly 0 or 1
   *  results.
   */

  *id_out = 0;
  addb_hmap_iterator_initialize(&hm_it);

  err = addb_hmap_iterator_next(pdb->pdb_hmap, h, (char const *)guid,
                                sizeof *guid, addb_hmt_key, &hm_it, id_out);
  if (0 == err) {
    graph_guid key_guid;

    pdb->pdb_runtime_statistics.rts_index_elements_read++;
    err = pdb_id_to_guid(pdb, *id_out, &key_guid);
    if (err) {
      addb_hmap_iterator_finish(&hm_it);
      return err;
    }

    cl_assert(pdb->pdb_cl, GRAPH_GUID_EQ(key_guid, *guid));
    return 0;
  }

  addb_hmap_iterator_finish(&hm_it);
  return err;
}

/**
 * @brief Add a mapping from a GUID to a PDB ID.
 *
 *  This mapping is only added for non-local GUIDs.
 *
 * @param pdb 	opaque module handle
 * @param original_id 	the id to which to map
 * @param guid 	the global GUID or application key from which to map
 *
 * @return 0 success, nonzero error codes on error.
 */
int pdb_id_add_guid(pdb_handle *pdb, pdb_id id, graph_guid const *guid) {
  pdb->pdb_runtime_statistics.rts_index_elements_written++;
  return
      /* XXX binary portability of databases? */
      addb_hmap_add(pdb->pdb_hmap, pdb_id_guid_hash34(guid), (char const *)guid,
                    sizeof *guid, addb_hmt_key, id);
}

/**
 * @brief  Synchronize
 *
 * @param pdb 	opaque pdb module handle
 * @param id	local ID of the passed-in record
 * @param pr 	passed-in record
 */
int pdb_id_synchronize(pdb_handle *pdb, pdb_id id, pdb_primitive const *pr) {
  graph_guid guid;
  unsigned long long h;
  int err;

  pdb_primitive_guid_get(pr, guid);

  if (PDB_GUID_IS_LOCAL(pdb, guid)) return 0;

  h = pdb_id_guid_hash34(&guid);

  pdb->pdb_runtime_statistics.rts_index_elements_written++;
  err = addb_hmap_add(pdb->pdb_hmap, h,
                      /* XXX binary portability of databases? */
                      (char const *)&guid, sizeof guid, addb_hmt_key, id);
  if (err && err != PDB_ERR_EXISTS) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_hmap_add", err,
                 "%llx -> %llx", (unsigned long long)h, (unsigned long long)id);
    return err;
  }
  return 0;
}

/**
 * @brief Render a PDB ID in a string.
 *
 * @param pdb	PDB module handle
 * @param id	The ID to render
 * @param buf	Buffer that may be used to render it
 * @param size  number of bytes pointed to by buf.
 *	PDB_ID_SIZE is a good size.
 *
 * @return a pointer to the '\0'-terminated,
 *  rendered ID.
 */
char const *pdb_id_to_string(pdb_handle *pdb, pdb_id id, char *buf,
                             size_t size) {
  if (id == PDB_ID_NONE) return "-";

  snprintf(buf, size, "%llu", (unsigned long long)id);
  return buf;
}

/**
 * @brief Scan a PDB ID from a string.
 *
 * @param pdb	PDB module handle
 * @param id_out	Store the scanned ID here.
 * @param s_ptr		Beginning of unparsed text, modified.
 * @param e  		First inaccessible byte
 *
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_id_from_string(pdb_handle *pdb, pdb_id *id_out, char const **s_ptr,
                       char const *e) {
  char const *s = *s_ptr;
  unsigned long long ull;
  int err;

  if (s >= e) return PDB_ERR_SYNTAX;

  if (*s == '-') {
    *s_ptr = s + 1;
    *id_out = PDB_ID_NONE;

    return 0;
  }

  if (!ISDIGIT(*s)) return PDB_ERR_SYNTAX;

  err = pdb_scan_ull(s_ptr, e, &ull);
  if (err == 0) *id_out = ull;

  return err;
}
