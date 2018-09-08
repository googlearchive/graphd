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
#include "libpdb/pdb.h"
#include "libpdb/pdbp.h"

#include <errno.h>

#include "libaddb/addb-bmap.h"

int pdb_is_versioned(pdb_handle* pdb, pdb_id id, bool* result) {
  int err;

  err = addb_bmap_check(pdb->pdb_versioned, id, result);

  return err;
}

int pdb_versioned_synchronize(pdb_handle* pdb, pdb_id id,
                              pdb_primitive const* pr) {
  pdb_id lineage, last;
  unsigned long long n;
  addb_idarray ida;
  int err;

  if (pdb_primitive_has_generation(pr)) {
    lineage = pdb_primitive_lineage_get(pr);

    err = addb_hmap_sparse_idarray(pdb->pdb_hmap, lineage, addb_hmt_gen, &ida);
    if (err) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "addb_hmap_sparse_idarray", err,
                   "Can't get lineage idarray (%llu)"
                   " which is versioned by %llx",
                   (unsigned long long)lineage, (unsigned long long)id);
      return PDB_ERR_DATABASE;
    }

    n = addb_idarray_n(&ida);
    if (n < 2) {
      cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
             "Corrupt hmap! id %llx is versioned but"
             " has only %llu ids in its version hmap",
             (unsigned long long)id, (unsigned long long)n);
      addb_idarray_finish(&ida);
      return PDB_ERR_DATABASE;
    }
    while (n > 0) {
      n--;
      addb_idarray_read1(&ida, n, &last);
      if (last < id) goto done;
    }

    addb_idarray_finish(&ida);
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "Didn't find any IDs less than %llx"
           " in version hmap for %llu",
           (unsigned long long)id, (unsigned long long)last);
    return PDB_ERR_DATABASE;

  done:
    addb_idarray_finish(&ida);

    /* 	This should, in general, be equivalent to:
               err = addb_hmap_sparse_array_nth(
                    pdb->pdb_hmap,
                    lineage,
                    addb_hmt_gen,
                    n - 2,
                    &last);
            but it's more resilient against changing the way
            IDs are added to the lineage hmap.
    */

    if (err) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "addb_hmap_sparse_array_nth",
                   err, "Can't get previous version of %llx",
                   (unsigned long long)(id));
      return err;
    }

    cl_assert(pdb->pdb_cl, last < pdb_primitive_n(pdb));

    cl_log(pdb->pdb_cl, CL_LEVEL_DEBUG,
           "pdb_versioned_synchronize: "
           "Marking %llx as a version of %llx",
           (unsigned long long)(last), (unsigned long long)(id));

    err = addb_bmap_set(pdb->pdb_versioned, last);
    if (err) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "addb_bmap_set", err,
                   "pdb_versioned_synchronize: can't mark %llx"
                   " as versioned by %llx",
                   (unsigned long long)last, (unsigned long long)id);
      return err;
    }

    return 0;
  }
  return 0;
}
