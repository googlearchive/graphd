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
#ifndef GRAPHP_H
#define GRAPHP_H

#include "libgraph/graph.h"

#include "libcm/cm.h"
#include "libcl/cl.h"

struct graph_handle {
  cl_handle *graph_cl;
  cm_handle *graph_cm;
};

int graph_ull_from_hexstring(unsigned long long *, char const *, char const *);

typedef struct graph_grmap_range {
  unsigned long long range_low;
  unsigned long long range_high;
  long long range_offset;
  unsigned long long range_dbid;

} graph_grmap_range;

struct graph_grmap_table {
  /*  How many of these slots are occupied?
   */
  size_t tab_n;

  /*  Open-ended array of slots.
   */
  graph_grmap_range tab_data[1];
};

/* graphd-grmap.c */

void graph_grmap_invariant_loc(graph_grmap const *const grm, char const *file,
                               int line);

graph_grmap_dbid_slot *graph_grmap_dbid_lookup_id(graph_grmap const *grm,
                                                  unsigned long long dbid);

graph_grmap_dbid_slot *graph_grmap_dbid_lookup(graph_grmap const *grm,
                                               graph_guid const *guid);

/* graphd-grmap-table.c */

void graph_grmap_table_delete(graph_grmap *grm, graph_grmap_dbid_slot *dis,
                              size_t i);

bool graph_grmap_table_next_overlap(
    graph_grmap const *grm, graph_grmap_dbid_slot const *dis,
    unsigned long long *lo, unsigned long long hi, bool *found_out,
    unsigned long long *lo_out, unsigned long long *hi_out, size_t *loc_out);

bool graph_grmap_table_lookup(graph_grmap const *grm,
                              graph_grmap_dbid_slot const *dis,
                              unsigned long long i, size_t *loc_out);

int graph_grmap_table_insert(graph_grmap *grm, graph_grmap_dbid_slot *tab,
                             size_t i, unsigned long long low);

int graph_grmap_table_split(graph_grmap *grm, graph_grmap_dbid_slot *dis,
                            size_t i);

/* graph-grmap-range.c */

void graph_grmap_range_repack(graph_grmap *grm, graph_grmap_dbid_slot *dis,
                              size_t tab_i, size_t i);

bool graph_grmap_range_adjacent(graph_grmap_range const *const a,
                                graph_grmap_range const *const b);

void graph_grmap_range_delete(graph_grmap *grm, graph_grmap_table *tab,
                              size_t i);

int graph_grmap_range_insert(graph_grmap *grm, graph_grmap_table *tab, size_t i,
                             unsigned long long low, unsigned long long high,
                             unsigned long long dbid, long long offset);

bool graph_grmap_range_lookup(graph_grmap const *grm,
                              graph_grmap_table const *tab,
                              unsigned long long i, size_t *loc_out);

bool graph_grmap_range_next_overlap(
    graph_grmap const *grm, graph_grmap_table const *tab,
    unsigned long long *lo, unsigned long long hi, bool *found_out,
    unsigned long long *lo_out, unsigned long long *hi_out, size_t *loc_out);

#endif /* GRAPHP_H */
