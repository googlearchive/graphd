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
#include <string.h>

#include "libcm/cm.h"

int pdb_set_path(pdb_handle* pdb, char const* path) {
  char* path_dup;

  if (pdb == NULL) return EINVAL;

  if (path == NULL)
    path_dup = NULL;
  else if ((path_dup = cm_strmalcpy(pdb->pdb_cm, path)) == NULL)
    return ENOMEM;

  if (pdb->pdb_path) cm_free(pdb->pdb_cm, pdb->pdb_path);
  pdb->pdb_path = path_dup;

  return 0;
}
