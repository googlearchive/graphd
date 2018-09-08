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
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


/*  Perform more of the concentric graph initialization.
 *  Return 0 if everything finished okay; PDB_ERR_MORE if more
 *  work is needed (and update the pointer in *state).
 */
int pdb_concentric_initialize(pdb_handle* pdb, pdb_id* state) {
  pdb_id hi = *state + 100 * 1000;
  int err;

  if (hi >= pdb_primitive_n(pdb)) hi = pdb_primitive_n(pdb);

  /*   Already finished?
   */
  if (*state >= hi) return 0;

  while (*state < hi) {
    pdb_primitive pr;
    graph_guid source, dest;

    /*  Read the primitive at <*state>.
     */
    err = pdb_id_read(pdb, *state, &pr);
    if (err != 0) {
      /* Done?
       */
      if (err == PDB_ERR_NO) return 0;

      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_id_read", err, "id=%llu",
                   (unsigned long long)*state);
      return err;
    }

    pdb_primitive_guid_get(&pr, source);

    if (GRAPH_GUID_SERIAL(source) == *state &&
        GRAPH_GUID_DB(source) == pdb->pdb_database_id)
      ;
    else {
      graph_guid_from_db_serial(&dest, pdb->pdb_database_id, *state);

      err = graph_grmap_add_range(pdb->pdb_concentric_map, &source, &dest, 1);
    }
    pdb_primitive_finish(pdb, &pr);

    if (err != 0) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "graph_grmap_add_range", err,
                   "id=%llu", (unsigned long long)*state);
      return err;
    }
  }

  return PDB_ERR_MORE;
}
