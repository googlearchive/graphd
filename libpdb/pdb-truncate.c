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

int pdb_truncate(pdb_handle* pdb) {
  int err = 0;
  int e;
  int i;

  if (pdb->pdb_primitive != NULL) {
    err = addb_istore_truncate(pdb->pdb_primitive, pdb->pdb_primitive_path);
    if (err != 0)
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_istore_truncate", err,
                   "path=%s", pdb->pdb_primitive_path);
    pdb->pdb_primitive = NULL;
  }

  for (i = 0; i < PDB_INDEX_N; i++) {
    pdb_index_instance* const ii = pdb->pdb_indices + i;
    if (ii->ii_impl.any) {
      e = (*ii->ii_type->ixt_truncate)(pdb, ii);
      if (e != 0) {
        cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "ii->ii_type->ixt_truncate", e,
                     "type=%s", ii->ii_type->ixt_name);
        if (err == 0) err = e;
      }
      ii->ii_impl.any = (void*)0;
      ii->ii_stage = PDB_CKS_START;
    }
  }

  e = pdb_primitive_alloc_subscription_call(pdb, PDB_ID_NONE, NULL);
  if (e != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL,
                 "pdb_primitive_alloc_subscription_call", e,
                 "(with null parameters)");
    if (err == 0) err = e;
  }

  return err;
}
