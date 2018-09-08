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

enum grw_state {
  GRAPH_GRMAP_WRITE_INITIAL = 0,
  GRAPH_GRMAP_WRITE_DBID,
  GRAPH_GRMAP_WRITE_TABLE,
  GRAPH_GRMAP_WRITE_RANGE

};
#define APPEND_LITERAL(w, lit)       \
  do {                               \
    memcpy(w, lit, sizeof(lit) - 1); \
    w += sizeof(lit) - 1;            \
  } while (0)

void graph_grmap_write_initialize(graph_grmap const* grm,
                                  graph_grmap_write_state* state) {
  cl_cover(grm->grm_graph->graph_cl);
  memset(state, 0, sizeof(*state));
}

/**
 * @brief Format the next chunk of textual grmap output into hte
 * 	buffer *s...e.  *s...e must be at least 128 bytes large.
 */
int graph_grmap_write_next(graph_grmap const* grm, char** s,
                           char const* const e,
                           graph_grmap_write_state* state) {
  char* w;
  graph_grmap_range const* range;
  graph_grmap_table const* tab;

  cl_assert(grm->grm_graph->graph_cl, e - *s >= 128);

  w = *s;
  while (e - w >= 128) {
    switch (state->grw_state) {
      case GRAPH_GRMAP_WRITE_INITIAL:
        cl_cover(grm->grm_graph->graph_cl);
        APPEND_LITERAL(w, "grmap {\n");
        state->grw_state = GRAPH_GRMAP_WRITE_DBID;
        break;

      case GRAPH_GRMAP_WRITE_DBID:
        if (state->grw_dis_i >= grm->grm_n) {
          cl_cover(grm->grm_graph->graph_cl);
          APPEND_LITERAL(w, "}\n");
          *w = '\0';

          /* Done. */
          *s = w;

          state->grw_range_i = 0;
          state->grw_tab_i = 0;
          state->grw_dis_i = 0;

          return GRAPH_ERR_DONE;
        }

        cl_cover(grm->grm_graph->graph_cl);
        snprintf(w, e - w, "    %llx {\n",
                 grm->grm_dbid[state->grw_dis_i].dis_dbid);
        w += strlen(w);
        state->grw_state = GRAPH_GRMAP_WRITE_TABLE;
        break;

      case GRAPH_GRMAP_WRITE_TABLE:
        if (state->grw_tab_i >= grm->grm_dbid[state->grw_dis_i].dis_n) {
          APPEND_LITERAL(w, "    }\n");

          cl_cover(grm->grm_graph->graph_cl);
          state->grw_tab_i = 0;
          state->grw_dis_i++;
          state->grw_state = GRAPH_GRMAP_WRITE_DBID;
          break;
        }
        cl_cover(grm->grm_graph->graph_cl);
        state->grw_state = GRAPH_GRMAP_WRITE_RANGE;
      /* fall through */

      case GRAPH_GRMAP_WRITE_RANGE:
        tab = grm->grm_dbid[state->grw_dis_i]
                  .dis_table[state->grw_tab_i]
                  .ts_table;
        if (state->grw_range_i >= tab->tab_n) {
          cl_cover(grm->grm_graph->graph_cl);

          state->grw_state = GRAPH_GRMAP_WRITE_TABLE;
          state->grw_tab_i++;
          state->grw_range_i = 0;
          break;
        }
        cl_cover(grm->grm_graph->graph_cl);
        range = tab->tab_data + state->grw_range_i++;
        snprintf(w, e - w, "\t%llx-%llx: %llx %c%llx\n", range->range_low,
                 range->range_high, range->range_dbid,
                 range->range_offset < 0 ? '-' : '+',
                 range->range_offset < 0 ? -range->range_offset
                                         : range->range_offset);
        w += strlen(w);
        break;
    }
  }

  cl_cover(grm->grm_graph->graph_cl);
  *w = '\0';
  *s = w;

  return 0;
}
