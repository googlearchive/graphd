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
#include "libgraph/graph.h"

char const* graph_strerror(int err) {
  switch (err) {
    case GRAPH_ERR_DONE:
      return "done";
    case GRAPH_ERR_LEXICAL:
      return "lexical error";
    case GRAPH_ERR_SEMANTICS:
      return "semantics error";
    case GRAPH_ERR_NO:
      return "no";
    case GRAPH_ERR_INSTANCE_ID_MISMATCH:
      return "instance-id mismatch";
    case GRAPH_ERR_RANGE_OVERLAP:
      return "GUID map range overlaps with a "
             "different, conflicting, mapping";
    case GRAPH_ERR_USED:
      return "this parameter can only be set "
             "in an unused table object.";
    default:
      break;
  }
  return NULL;
}
