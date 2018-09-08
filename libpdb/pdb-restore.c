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


int pdb_restore_avoid_database_id(pdb_handle* pdb, graph_guid const* guid) {
  int err;
  char flat_id[6 + 5];

  if (GRAPH_GUID_DB(*guid) != pdb->pdb_database_id) return 0;

  if (addb_istore_next_id(pdb->pdb_primitive) != 0) return PDB_ERR_EXISTS;

  /* Rewrite the database id into the flat file. */

  pdb->pdb_database_id++;

  pdb_set5(flat_id, 0ull);
  pdb_set6(flat_id + 5, pdb->pdb_database_id);

  err = addb_flat_write(pdb->pdb_header, flat_id, sizeof flat_id);
  if (err != 0) return err;

  return 0;
}

/*  Adopt the database id of <GUID> as the base for compressed IDs
 *  in the database.
 */
int pdb_restore_adopt_database_id(pdb_handle* pdb, graph_guid const* guid) {
  int err;
  char flat_id[6 + 5];

  if (GRAPH_GUID_DB(*guid) == pdb->pdb_database_id) {
    cl_log(pdb->pdb_cl, CL_LEVEL_DEBUG,
           "pdb_restore_adopt_database_id: already have that ID");
    return 0;
  }

  if (addb_istore_next_id(pdb->pdb_primitive) != 0) {
    cl_log(pdb->pdb_cl, CL_LEVEL_DEBUG,
           "pdb_restore_adopt_database_id: already "
           "have %llu ids in the database",
           (unsigned long long)addb_istore_next_id(pdb->pdb_primitive));
    return PDB_ERR_EXISTS;
  }

  /* Rewrite the database id into the flat file. */

  pdb->pdb_database_id = GRAPH_GUID_DB(*guid);
  graph_guid_from_db_serial(&pdb->pdb_database_guid, pdb->pdb_database_id, 0);

  pdb_set5(flat_id, 0ull);
  pdb_set6(flat_id + 5, pdb->pdb_database_id);

  err = addb_flat_write(pdb->pdb_header, flat_id, sizeof flat_id);
  if (err != 0) return err;

  cl_log(pdb->pdb_cl, CL_LEVEL_DEBUG,
         "pdb_restore_adopt_database_id: switched "
         "internal ID to %llu",
         (unsigned long long)pdb->pdb_database_id);
  return 0;
}

/* If the restore is starting from zero, truncate the database
 */
int pdb_restore_prepare(pdb_handle* pdb, pdb_id start) {
  int err;

  if (start != 0) return 0; /* no truncation required */

  err = pdb_checkpoint_mandatory(pdb, true);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_checkpoint_mandatory", err,
                 "Unable to checkpoint prior to restore-from-0");

    return err;
  }
  err = pdb_checkpoint_optional(pdb, 0);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_checkpoint_optional", err,
                 "Unable to checkpoint prior to restore-from-0");

    return err;
  }
  err = pdb_truncate(pdb);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_truncate", err,
                 "Unable to truncate prior to restore-from-0");

    return err;
  }

  err = pdb_initialize_open_databases(pdb);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_initialize_open_databases",
                 err, "Unable to initialize database prior to restore-from-0");

    return err;
  }

  return 0;
}
