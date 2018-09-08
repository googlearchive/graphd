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
#include "graphd/graphd-hash.h"

#include <ctype.h>
#include <errno.h>
#include <stdlib.h>

void graphd_dateline_expire(graphd_handle* g) {
  if (g->g_dateline != NULL) {
    graph_dateline_destroy(g->g_dateline);
    g->g_dateline = NULL;
  }
}

graph_dateline* graphd_dateline(graphd_handle* g) {
  if (g->g_dateline == NULL) {
    cm_handle* cm = cm_c();
    graph_dateline* dl;
    int err;

    if ((dl = graph_dateline_create(cm)) == NULL) return NULL;

    err = graph_dateline_add(&dl, pdb_database_id(g->g_pdb),
                             pdb_primitive_n(g->g_pdb), g->g_instance_id);
    if (err != 0) {
      graph_dateline_destroy(dl);
      return NULL;
    }
    g->g_dateline = dl;
  }
  return graph_dateline_dup(g->g_dateline);
}

pdb_id graphd_dateline_low(graphd_handle const* g,
                           graphd_constraint const* con) {
  graph_dateline const* dl;
  unsigned long long ull;

  if (g == NULL || con == NULL ||
      (dl = con->con_dateline.dateline_min) == NULL ||
      graph_dateline_get(dl, pdb_database_id(g->g_pdb), &ull))

    return PDB_ITERATOR_LOW_ANY;
  return ull;
}

pdb_id graphd_dateline_high(graphd_handle const* g,
                            graphd_constraint const* con) {
  graph_dateline const* dl;
  unsigned long long ull;

  if (g == NULL || con == NULL ||
      (dl = con->con_dateline.dateline_max) == NULL ||
      graph_dateline_get(dl, pdb_database_id(g->g_pdb), &ull))

    return PDB_ITERATOR_HIGH_ANY;
  return ull;
}

void graphd_dateline_constraint_hash(
    cl_handle* const cl, graphd_dateline_constraint const* const condat,
    unsigned long* const hash_inout) {
  GRAPHD_HASH_BIT(*hash_inout, condat->dateline_min != 0);
  if (condat->dateline_min != 0) {
    unsigned long long dh;
    dh = graph_dateline_hash(condat->dateline_min);
    GRAPHD_HASH_VALUE(*hash_inout, (dh >> (sizeof(dh) / 2)) ^ dh);
  }

  GRAPHD_HASH_BIT(*hash_inout, condat->dateline_max != 0);
  if (condat->dateline_max != 0) {
    unsigned long long dh;
    dh = graph_dateline_hash(condat->dateline_max);
    GRAPHD_HASH_VALUE(*hash_inout, (dh >> (sizeof(dh) / 2)) ^ dh);
  }
}
