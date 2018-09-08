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
#include "graphd/graphd.h"

#include <errno.h>
#include <stdio.h>
#include <stdbool.h>

void graphd_bad_cache_initialize(graphd_bad_cache* bc) {
  size_t i;
  for (i = 0; i < GRAPHD_BAD_CACHE_N; i++) bc->bc_id[i] = PDB_ID_NONE;
  bc->bc_n = 0;
}

bool graphd_bad_cache_member(graphd_bad_cache const* bc, pdb_id id) {
  size_t i;
  for (i = 0; i < GRAPHD_BAD_CACHE_N; i++)
    if (bc->bc_id[i] == id) return true;
  return false;
}

void graphd_bad_cache_add(graphd_bad_cache* bc, pdb_id id) {
  bc->bc_id[bc->bc_n] = id;
  bc->bc_n = (bc->bc_n + 1) % GRAPHD_BAD_CACHE_N;
}
