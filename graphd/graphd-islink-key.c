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

/*  An ISLINK KEY is a specification for an islink job or
 *  its results.  They are used to find jobs, for jobs to
 *  remember what they're working on, and to find results
 *  inside the islink cache.
 *
 *  There's alwyas a typeguid.  Usually, there's also an
 *  endpoint linkage ("all left sides of an `is-a'");
 *  sometimes, there's also a fixed opposite endpoint
 *  ("all right sides of an `is-a' with a left side of
 *  `person'").
 *
 *  Islink keys are a subset of primitive summaries.  Some
 *  primitive summaries can translate into islink keys, and
 *  all islink keys translate into primitive summaries.
 */

/*  Make a printable representation of an islink key,
 *  for debugging.
 */
char const* graphd_islink_key_to_string(graphd_islink_key const* key, char* buf,
                                        size_t size) {
  if (key == NULL) return "null";

  if (key->key_result_linkage == PDB_LINKAGE_N) {
    /*  Just a typeguid.
     */
    snprintf(buf, size, "<%llx>", (unsigned long long)key->key_type_id);
  } else if (key->key_endpoint_id == PDB_ID_NONE) {
    /* A linkage and a type.
     */
    snprintf(buf, size, "%s<%llx>",
             pdb_linkage_to_string(key->key_result_linkage),
             (unsigned long long)key->key_type_id);
  } else {
    /* A linkage, type, and endpoint
     */
    snprintf(buf, size, "%s<%llx;%s=%llx>",
             pdb_linkage_to_string(key->key_result_linkage),
             (unsigned long long)key->key_type_id,
             pdb_linkage_to_string(graphd_islink_key_endpoint_linkage(key)),
             (unsigned long long)key->key_endpoint_id);
  }
  return buf;
}

/*  Given the ingredients, make an islink key.
 *
 *  The only thing of value here is zeroing the buffer
 *  before assigning to it - that way, it can be used
 *  to hash.
 */
graphd_islink_key* graphd_islink_key_make(graphd_handle* g, int result_linkage,
                                          pdb_id type_id, pdb_id endpoint_id,
                                          graphd_islink_key* buf) {
  cl_assert(g->g_cl, type_id != PDB_ID_NONE);
  cl_assert(g->g_cl,
            result_linkage < PDB_LINKAGE_N || endpoint_id == PDB_ID_NONE);
  cl_assert(g->g_cl, result_linkage == PDB_LINKAGE_N ||
                         result_linkage == PDB_LINKAGE_RIGHT ||
                         result_linkage == PDB_LINKAGE_LEFT);

  memset(buf, 0, sizeof *buf);

  buf->key_type_id = type_id;
  buf->key_endpoint_id = endpoint_id;
  buf->key_result_linkage = result_linkage;

  return buf;
}

/*  Return the linkage of the left or right endpoint,
 *  if one is encoded in the key.
 *  (If the key does not fix an endpoint, return PDB_LINKAGE_N.)
 */
int graphd_islink_key_endpoint_linkage(graphd_islink_key const* key) {
  if (key->key_endpoint_id == PDB_ID_NONE) return PDB_LINKAGE_N;

  return key->key_result_linkage == PDB_LINKAGE_RIGHT ? PDB_LINKAGE_LEFT
                                                      : PDB_LINKAGE_RIGHT;
}

/* Return the primitive summary of a key.
 */
int graphd_islink_key_psum(graphd_handle* g, graphd_islink_key const* key,
                           pdb_primitive_summary* psum) {
  int err;

  memset(psum, 0, sizeof(*psum));

  psum->psum_locked = 1 << PDB_LINKAGE_TYPEGUID;

  if (key->key_endpoint_id != PDB_ID_NONE) {
    int endpoint_linkage = graphd_islink_key_endpoint_linkage(key);
    psum->psum_locked |= 1 << endpoint_linkage;

    err = pdb_id_to_guid(g->g_pdb, key->key_endpoint_id,
                         psum->psum_guid + endpoint_linkage);
    if (err != 0) return err;
  }
  err = pdb_id_to_guid(g->g_pdb, key->key_type_id,
                       psum->psum_guid + PDB_LINKAGE_TYPEGUID);
  if (err != 0) return err;

  psum->psum_result = key->key_result_linkage;
  psum->psum_complete = true;

  return 0;
}
