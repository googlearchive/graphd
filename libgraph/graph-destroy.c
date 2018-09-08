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
#include "libgraph/graphp.h"

/**
 * @brief Destroy a graph module handle.
 * @param graph NULL or a graph module handle allocated with graph_create().
 */
void graph_destroy(graph_handle *graph) {
  if (!graph) return;
  cl_cover(graph->graph_cl);

  cm_free(graph->graph_cm, graph);
}
