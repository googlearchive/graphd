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

const cl_facility pdb_facilities[] = {{"cost", PDB_FACILITY_COST},
                                      {"iterator", PDB_FACILITY_ITERATOR},
                                      {NULL, 0, addb_facilities},
                                      {0}};

pdb_handle* pdb_create(cm_handle* cm, cl_handle* cl, int version) {
  size_t i;

  pdb_handle* pdb = cm_talloc(cm, pdb_handle, 1);
  if (!pdb) return NULL;

  memset(pdb, 0, sizeof(*pdb));

  pdb->pdb_cm = cm;
  pdb->pdb_cl = cl;
  pdb->pdb_database_id = (unsigned long long)-1;

  pdb->pdb_graph = NULL;
  pdb->pdb_addb = NULL;
  pdb->pdb_path = NULL;
  pdb->pdb_lockfile_path = NULL;
  pdb->pdb_primitive_path = NULL;
  pdb->pdb_primitive = NULL;
  pdb->pdb_header_path = NULL;
  pdb->pdb_header = NULL;
  pdb->pdb_version = version;
  pdb->pdb_disk_available = true;
  pdb->pdb_primitive_alloc_head = pdb->pdb_primitive_alloc_tail = NULL;
  pdb->pdb_iterator_chain_buf.pic_head = pdb->pdb_iterator_chain_buf.pic_tail =
      NULL;
  pdb->pdb_iterator_chain = &pdb->pdb_iterator_chain_buf;

  for (i = 0; i < PDB_INDEX_N; i++) {
    pdb->pdb_indices[i].ii_type = (pdb_index_type*)0;
    pdb->pdb_indices[i].ii_path = (char*)0;
    pdb->pdb_indices[i].ii_stage = PDB_CKS_START;
    pdb->pdb_indices[i].ii_impl.any = (void*)0;
  }
  pdb->pdb_indices[PDB_INDEX_TYPEGUID].ii_type = &pdb_index_gmap;
  pdb->pdb_indices[PDB_INDEX_LEFT].ii_type = &pdb_index_gmap;
  pdb->pdb_indices[PDB_INDEX_RIGHT].ii_type = &pdb_index_gmap;
  pdb->pdb_indices[PDB_INDEX_SCOPE].ii_type = &pdb_index_gmap;
  pdb->pdb_indices[PDB_INDEX_HMAP].ii_type = &pdb_index_hmap;
  pdb->pdb_indices[PDB_INDEX_PREFIX].ii_type = &pdb_index_bmap;
  pdb->pdb_indices[PDB_INDEX_DEAD].ii_type = &pdb_index_bmap;

  /*   Make sure that we initialized all types.
   */
  for (i = 0; i < PDB_INDEX_N; i++) cl_assert(cl, pdb->pdb_indices[i].ii_type);

  return pdb;
}
