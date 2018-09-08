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
#include <time.h>
#include <errno.h>
#include <ctype.h>
#include <stdio.h>

#include "libgraph/graphp.h"

/*  Initialize a traversal over all mappings.
 */
void graph_grmap_next_initialize(graph_grmap const* grm,
                                 graph_grmap_next_state* state) {
  cl_cover(grm->grm_graph->graph_cl);
  memset(state, 0, sizeof(*state));
}

/*  Return the next mapping.
 */
bool graph_grmap_next(graph_grmap const* grm, graph_grmap_next_state* state,
                      graph_guid* source, graph_guid* destination,
                      unsigned long long* n_out) {
  graph_grmap_table* tab;
  graph_grmap_dbid_slot* dis;
  graph_grmap_range* range;

  if (state->grn_dis_i >= grm->grm_n) {
    cl_cover(grm->grm_graph->graph_cl);
    return false;
  }

  dis = grm->grm_dbid + state->grn_dis_i;
  tab = dis->dis_table[state->grn_tab_i].ts_table;
  range = tab->tab_data + state->grn_range_i;

  /*  Copy into the output.
   */
  graph_guid_from_db_serial(source, dis->dis_dbid, range->range_low);
  graph_guid_from_db_serial(destination, range->range_dbid,
                            range->range_low + range->range_offset);
  *n_out = range->range_high - range->range_low;

  /*  Increment.
   */
  state->grn_range_i++;
  if (state->grn_range_i >= tab->tab_n) {
    cl_cover(grm->grm_graph->graph_cl);

    /* Carry (1) range -> table.
     */
    state->grn_range_i = 0;
    state->grn_tab_i++;

    if (state->grn_tab_i >= dis->dis_n) {
      cl_cover(grm->grm_graph->graph_cl);

      /* Carry (2) table -> dbid.
       */
      state->grn_range_i = 0;
      state->grn_tab_i = 0;
      state->grn_dis_i++;
    }
  }
  return true;
}

/*  Begin iterating over the mappings for the DBID
 *  of <source>.
 */
void graph_grmap_next_dbid_initialize(graph_grmap const* grm,
                                      graph_guid const* source,
                                      graph_grmap_next_state* state) {
  graph_grmap_dbid_slot* dis;

  cl_cover(grm->grm_graph->graph_cl);
  memset(state, 0, sizeof(*state));

  dis = graph_grmap_dbid_lookup(grm, source);
  state->grn_dis_i = dis == NULL ? grm->grm_n : dis - grm->grm_dbid;
}

/*  Return the next mapping for the DBID that
 *  graph_grmap_next_dbid_initialize was called
 *  with.
 */
bool graph_grmap_next_dbid(graph_grmap const* grm,
                           graph_grmap_next_state* state, graph_guid* source,
                           graph_guid* destination, unsigned long long* n_out) {
  graph_grmap_table* tab;
  graph_grmap_dbid_slot* dis;
  graph_grmap_range* range;

  if (state->grn_dis_i >= grm->grm_n) {
    cl_cover(grm->grm_graph->graph_cl);
    return false;
  }

  dis = grm->grm_dbid + state->grn_dis_i;
  if (state->grn_tab_i >= dis->dis_n) {
    cl_cover(grm->grm_graph->graph_cl);
    return false;
  }

  tab = dis->dis_table[state->grn_tab_i].ts_table;
  range = tab->tab_data + state->grn_range_i;

  /*  Copy into the output.
   */
  graph_guid_from_db_serial(source, dis->dis_dbid, range->range_low);
  graph_guid_from_db_serial(destination, range->range_dbid,
                            range->range_low + range->range_offset);
  *n_out = range->range_high - range->range_low;

  /*  Increment.
   */
  state->grn_range_i++;
  if (state->grn_range_i >= tab->tab_n) {
    /* Carry (1) range -> table.
     */
    state->grn_range_i = 0;
    state->grn_tab_i++;

    cl_cover(grm->grm_graph->graph_cl);

    /* Do not carry into the next dbid.
     */
  }
  return true;
}
