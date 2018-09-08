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

static int pdb_iterator_smap_check(pdb_handle *_pdb, pdb_iterator *_it,
                                   pdb_id _id, pdb_budget *_cost) {
  return PDB_ERR_NO;
}

static int pdb_iterator_smap_next_loc(pdb_handle *pdb, pdb_iterator *it_inout,
                                      pdb_id *id_out, pdb_budget *cost_inout,
                                      char const *file, int line) {
  return PDB_ERR_NO;
}

static int pdb_iterator_smap_find_loc(pdb_handle *pdb, pdb_iterator *it_inout,
                                      pdb_id id_in, pdb_id *id_out,
                                      pdb_budget *cost, char const *file,
                                      int line) {
  return PDB_ERR_NO;
}

static int pdb_iterator_smap_statistics(pdb_handle *pdb, pdb_iterator *it,
                                        pdb_budget *cost) {
  return PDB_ERR_NO;
}

static char const *pdb_iterator_smap_to_string(pdb_handle *_pdb,
                                               pdb_iterator *_it, char *_buf,
                                               size_t _size) {
  return 0;
}

static void pdb_iterator_smap_finish(pdb_handle *_pdb, pdb_iterator *_it) {}

static int pdb_iterator_smap_reset(pdb_handle *_pdb, pdb_iterator *_it) {
  return PDB_ERR_NO;
}

static int pdb_iterator_smap_clone(pdb_handle *_pdb, pdb_iterator *_clone_in,
                                   pdb_iterator **_clone_out) {
  return PDB_ERR_NO;
}

static int pdb_iterator_smap_freeze(pdb_handle *_pdb, pdb_iterator *_it,
                                    unsigned int _flags, cm_buffer *_buf) {
  return PDB_ERR_NO;
}

static int pdb_iterator_smap_idarray(pdb_handle *_pdb, pdb_iterator *_it,
                                     addb_idarray **_ida_out,
                                     unsigned long long *_s_out,
                                     unsigned long long *_e_out) {
  return PDB_ERR_NO;
}

static int pdb_iterator_smap_primitive_summary(
    pdb_handle *_pdb, pdb_iterator *_it, pdb_primitive_summary *_psum_out) {
  return PDB_ERR_NO;
}

static int pdb_iterator_smap_beyond(pdb_handle *_pdb, pdb_iterator *_it,
                                    char const *_s, char const *_e,
                                    bool *_beyond_out) {
  return PDB_ERR_NO;
}

const pdb_iterator_type pdb_iterator_smap_type = {
    "smap",
    pdb_iterator_smap_finish,
    pdb_iterator_smap_reset,
    pdb_iterator_smap_clone,
    pdb_iterator_smap_freeze,
    pdb_iterator_smap_to_string,

    pdb_iterator_smap_next_loc,
    pdb_iterator_smap_find_loc,
    pdb_iterator_smap_check,
    pdb_iterator_smap_statistics,

    pdb_iterator_smap_idarray,
    pdb_iterator_smap_primitive_summary,
    pdb_iterator_smap_beyond,

    /* range-estimate */ NULL,

    /* restrict */ NULL,

    /* suspend */ NULL,
    /* unsuspend  */ NULL};
