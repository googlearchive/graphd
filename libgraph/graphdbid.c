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
#include <stdio.h>
#include <string.h>

int main(int argc, char** argv) {
  int i;

  for (i = 1; i < argc; i++) {
    graph_guid g;

    if (graph_guid_from_string(&g, argv[i], argv[i] + strlen(argv[i])) != 0)
      fprintf(stderr, "\"%s\" is not a valid GUID\n", argv[i]);
    else
      printf("%llu\n", GRAPH_GUID_DB(g));
  }

  return 0;
}
