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

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sysexits.h>

#include "libsrv/srv.h"

int graphd_predictable_option_set(void* data, srv_handle* srv, cm_handle* cm,
                                  int opt, char const* opt_arg) {
  graphd_handle* g = data;

  g->g_predictable = 1;

  return 0;
}

int graphd_predictable_option_configure(void* data, srv_handle* srv,
                                        void* config_data,
                                        srv_config* srv_config_data) {
  graphd_handle* g = data;

  if (g->g_predictable) g->g_now = 0;

  return 0;
}
