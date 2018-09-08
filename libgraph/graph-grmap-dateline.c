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

#include "libgraph/graphp.h"

/* Given a grmap, return a dateline that describes
 *	for each database ID,
 *	the first GUID *not* present in the range.
 */
graph_dateline* graph_grmap_dateline(graph_grmap const* grm) {
  cl_handle* cl;
  size_t db_i;
  graph_grmap_dbid_slot* dis;
  graph_dateline* dl;

  if (grm == NULL) return NULL;

  cl = grm->grm_graph->graph_cl;
  dl = graph_dateline_create(grm->grm_graph->graph_cm);
  if (dl == NULL) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graph_dateline_create",
                 errno ? errno : ENOMEM, "cannot allocate dateline!");
    return dl;
  }

  for (db_i = 0, dis = grm->grm_dbid; db_i < grm->grm_n; db_i++, dis++) {
    unsigned long long hi;
    graph_grmap_table const* tab;
    int err;

    if (dis->dis_n == 0)
      hi = 0;
    else {
      tab = dis->dis_table[dis->dis_n - 1].ts_table;
      cl_assert(cl, tab->tab_n > 0);
      hi = tab->tab_data[tab->tab_n - 1].range_high;
    }
    err = graph_dateline_add(&dl, dis->dis_dbid, hi, NULL);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graph_dateline_add",
                   errno ? errno : ENOMEM, "dbid=%llu, n=%llu",
                   (unsigned long long)dis->dis_dbid, (unsigned long long)hi);
      graph_dateline_destroy(dl);
      errno = err;

      return NULL;
    }
  }
  return dl;
}
