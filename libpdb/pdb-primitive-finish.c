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

/*  Allocate primitive data of a certain size; return it and a GUID.
 */

void pdb_primitive_finish_loc(pdb_handle* pdb, pdb_primitive* pr,
                              char const* file, int line) {
  if (pdb != NULL && pr != NULL && pr->pr_data.data_type != ADDB_DATA_NONE) {
    char buf[200];
    cl_log(pdb->pdb_cl, CL_LEVEL_ULTRA, "pdb_primitive_finish %s [for %s:%d]",
           pdb_primitive_to_string(pr, buf, sizeof buf), file, line);

    addb_istore_free_loc(pdb->pdb_primitive, &pr->pr_data, file, line);
    cl_assert(pdb->pdb_cl, pr->pr_data.data_type == ADDB_DATA_NONE);
  }
}
