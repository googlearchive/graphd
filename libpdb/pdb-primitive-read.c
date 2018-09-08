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
#include <string.h>

/*  Get primitive data, given a GUID.  Data must be
 *  free'ed using pdb_primitive_finish().
 */

int pdb_primitive_read_loc(pdb_handle* pdb, pdb_guid const* guid,
                           pdb_primitive* pr, char const* file, int line) {
  pdb_id id;
  int err;

  if (GRAPH_GUID_IS_NULL(*guid)) {
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
           "pdb_primitive_read (from %s:%d): null GUID", file, line);
    cl_cover(pdb->pdb_cl);
    return PDB_ERR_NO;
  }

  /* Look it up in the key translation table. */

  if ((err = pdb_id_from_guid(pdb, &id, guid)) != 0) {
    char guid_buf[GRAPH_GUID_SIZE];
    cl_log_errno(
        pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "guid=%s [%s:%d]",
        graph_guid_to_string(guid, guid_buf, sizeof(guid_buf)), file, line);
    cl_cover(pdb->pdb_cl);
    return err;
  }

  return pdb_id_read_loc(pdb, id, pr, file, line);
}
