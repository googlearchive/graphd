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

/**
 * @brief Are a and b equal?
 */
bool graph_grmap_equal(graph_grmap const* a, graph_grmap const* b) {
  graph_grmap_dbid_slot const* dis;
  size_t i;

  if (a->grm_n != b->grm_n) {
    cl_cover(a->grm_graph->graph_cl);
    return false;
  }

  for (i = 0, dis = a->grm_dbid; i < a->grm_n; i++, dis++) {
    graph_grmap_next_state a_state;
    graph_grmap_next_state b_state;
    graph_guid guid;

    graph_guid_from_db_serial(&guid, dis->dis_dbid, 0);

    graph_grmap_next_dbid_initialize(a, &guid, &a_state);
    graph_grmap_next_dbid_initialize(b, &guid, &b_state);

    for (;;) {
      graph_guid a_source, a_dest, b_source, b_dest;
      unsigned long long a_n, b_n;

      if (!graph_grmap_next_dbid(a, &a_state, &a_source, &a_dest, &a_n)) {
        if (!graph_grmap_next_dbid(b, &b_state, &b_source, &b_dest, &b_n)) {
          cl_cover(a->grm_graph->graph_cl);
          break;
        }
        cl_cover(a->grm_graph->graph_cl);
        return false;
      }
      if (!graph_grmap_next_dbid(b, &b_state, &b_source, &b_dest, &b_n)) {
        cl_cover(a->grm_graph->graph_cl);
        return false;
      }

      if (a_n != b_n || !GRAPH_GUID_EQ(a_source, b_source) ||
          !GRAPH_GUID_EQ(a_dest, b_dest)) {
        cl_cover(a->grm_graph->graph_cl);
        return false;
      }
    }
  }

  cl_cover(a->grm_graph->graph_cl);
  return true;
}
