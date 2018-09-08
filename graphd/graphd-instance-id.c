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

#include <string.h>
#include <sysexits.h>
#include <unistd.h>

/*
 * Read and configure instance IDs from the graphd.conf file
 */

int graphd_instance_id_config_read(void* data, srv_handle* srv,
                                   void* config_data, srv_config* srv_cf,
                                   char** s, char const* e) {
  graphd_config* gcf = config_data;
  char* iid;
  iid = srv_config_read_string(srv_cf, srv_log(srv), "instance-id", s, e);

  if (!iid) exit(EX_USAGE);

  if (strlen(iid) > 31) {
    cl_log(srv_log(srv), CL_LEVEL_OPERATOR_ERROR,
           "instance ID may not be longer than 31 bytes");
    exit(EX_USAGE);
  }

  strcpy(gcf->gcf_instance_id, iid);

  return 0;
}

int graphd_instance_id_config_open(void* data, srv_handle* srv,
                                   void* config_data, srv_config* srv_cf) {
  graphd_handle* g = data;
  graphd_config* gcf = config_data;

  /*
   * If the instance-id has already been set by the -I flag, don't
   * use the value in the configuration.
   */
  if (g->g_instance_id[0]) return 0;

  strcpy(g->g_instance_id, gcf->gcf_instance_id);
  return 0;
}
