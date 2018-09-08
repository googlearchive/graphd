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

/**
 * @brief Given a primitive, get the GUID of its predecessor in its lineage.
 *
 *  The primitives don't store their predecessor's GUID; rather,
 *  they store a pointer to the whole set of family members plus
 *  their position in it.  This allows us to, given any primitive,
 *  quickly jump to the last position in the lineage.
 *  But this also means that looking up a predecessor is a little
 *  more difficult, and involves a few true database lookups.
 *
 * @param pdb		Database module handle.
 * @param pr		The primitive whose previous ID we're interested in.
 * @param prev_out 	Out: GUID of its predecessor.
 *
 * @return 0 on success.
 * @return PDB_ERR_NO if the primitive has no predecessor.
 */
int pdb_primitive_previous_guid(pdb_handle* pdb, pdb_primitive const* pr,
                                graph_guid* prev_out) {
  pdb_id lineage_id;
  pdb_id prev_id;
  unsigned long long generation;
  int err;

  if (!pdb_primitive_has_previous(pr)) return PDB_ERR_NO;

  lineage_id = pdb_primitive_lineage_get(pr);
  generation = pdb_primitive_generation_get(pr);

  cl_assert(pdb->pdb_cl, generation > 0);

  err = addb_hmap_sparse_array_nth(pdb->pdb_hmap, lineage_id, addb_hmt_gen,
                                   generation - 1, &prev_id);
  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_hmap_sparse_array_nth", err,
                 "can't retrieve generation #%llu of %llx", generation - 1,
                 (unsigned long long)lineage_id);
    return err;
  }
  return pdb_id_to_guid(pdb, prev_id, prev_out);
}
