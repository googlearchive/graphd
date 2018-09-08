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

#include <stdlib.h>
#include <string.h>

#include "libcl/cl.h"
#include "libcm/cm.h"

/**
 * @brief Return the memory handle that a PDB allocates through.
 * @param pdb	NULL or a PDB handle
 * @return NULL or the pdb's cm_handle object.
 */
cm_handle* pdb_mem(pdb_handle* pdb) { return pdb ? pdb->pdb_cm : NULL; }
