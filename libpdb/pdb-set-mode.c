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
#include <stdlib.h>
#include <string.h>

#include "libcl/cl.h"
#include "libcm/cm.h"

int pdb_set_mode(pdb_handle* pdb, int mode) {
  if (mode & ~(PDB_MODE_READ | PDB_MODE_WRITE)) {
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb_set_mode: unexpected mode %x [%s:%d]", mode, __FILE__,
           __LINE__);
    return EINVAL;
  }
  if (pdb->pdb_primitive != NULL) {
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb_set_mode: mode already initialized [%s:%d]", __FILE__,
           __LINE__);
    return PDB_ERR_ALREADY;
  }
  pdb->pdb_mode = mode;
  return 0;
}
