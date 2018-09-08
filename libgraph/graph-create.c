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
#include <string.h>

#include "libgraph/graphp.h"

/**
 * @brief Create a new graph module handle.
 * @param cm Memory allocator module handle
 * @param cl Logging module handle.
 * @return NULL on allocation error, otherwise a graph module handle.
 */
graph_handle* graph_create(struct cm_handle* cm, struct cl_handle* cl) {
  graph_handle* graph;

  if (!(graph = cm_talloc(cm, graph_handle, 1))) return NULL;
  memset(graph, 0, sizeof(*graph));

  graph->graph_cm = cm;
  graph->graph_cl = cl;

  cl_cover(cl);

  return graph;
}
