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

int graphd_backend_initialize(graphd_handle* g) {
  cl_handle* cl = pdb_log(g->g_pdb);

  g->g_backend_write = NULL;
  g->g_backend_update = NULL;
  g->g_backend_argv = NULL;
}

int graphd_backend_finish(graphd_handle* g) {
  cl_handle* cl = pdb_log(g->g_pdb);
  return 0;
}

int graphd_backend_open(graphd_handle* g, graphd_config* gcf, cm_handle* cm,
                        cl_handle* cl) {
  return 0;
}
