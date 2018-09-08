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
#include <stdio.h>    /* EOF */
#include <sysexits.h> /* EX_OSERR */

#include "libsrv/srv.h"

#define IS_LIT(lit, s, e) \
  ((e) - (s) == sizeof lit - 1 && !strncasecmp(lit, s, sizeof lit - 1))

/**
 * @brief Set an option as configured.  (Method.)
 *
 *  This is a method of the generic libsrv parameter mechanism,
 *  passed in via a srv_config_parameter[] structure declared in graphd.c.
 *
 * @param data		opaque application data handle (i.e., graphd)
 * @param srv 		generic libsrv handle
 * @param config_data	opaque application config data (i.e., graphd_config)
 * @param srv_cf	generic libsrv parameters
 *
 * @return 0 on success, a nonzero errno on error.
 */

int graphd_smp_leader_config_read(void* data, srv_handle* srv,
                                  void* config_data, srv_config* srv_cf,
                                  char** s, char const* e) {
  cl_handle* cl = srv_log(srv);
  graphd_config* gcf = config_data;

  gcf->gcf_smp_leader =
      srv_config_read_string(srv_cf, cl, "SMP leader socket", s, e);

  if (gcf->gcf_smp_leader == NULL || strlen(gcf->gcf_smp_leader) < 4) {
    cl_cover(cl);
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "configuration file %s, line %d: "
           "Invalid socket address for the leader "
           "process to use.",
           srv_config_file_name(srv_cf), srv_config_line_number(srv_cf, e));
    return GRAPHD_ERR_SYNTAX;
  }
  return 0;
}

int graphd_smp_leader_config_open(void* data, srv_handle* srv,
                                  void* config_data, srv_config* srv_cf) {
  graphd_handle* g = data;
  graphd_config* gcf = config_data;
  cl_handle* cl = srv_log(srv);

  cl_assert(cl, g != NULL);
  cl_assert(cl, config_data != NULL);

  cl_cover(cl);

  if (srv_smp_processes(srv) <= 1)
    g->g_smp_leader_address = NULL;
  else {
    if (g->g_leader_address_arg != NULL)
      g->g_smp_leader_address = cm_strmalcpy(g->g_cm, g->g_leader_address_arg);
    else if (gcf->gcf_smp_leader != NULL)
      g->g_smp_leader_address = cm_strmalcpy(g->g_cm, gcf->gcf_smp_leader);
    else
      g->g_smp_leader_address = cm_sprintf(
          g->g_cm, "unix://graphd-smp-socket.%u", (unsigned int)getpid());
    if (g->g_smp_leader_address == NULL) return errno ? errno : ENOMEM;
  }
  return 0;
}

int graphd_smp_leader_option_set(void* data, srv_handle* srv, cm_handle* cm,
                                 int opt, char const* opt_arg) {
  graphd_handle* g = data;

  if (g->g_leader_address_arg) {
    fprintf(stderr, "%s: duplicate leader address %s, original: %s",
            srv_program_name(srv), g->g_leader_address_arg, opt_arg);
    exit(EX_OSERR);
  }

  g->g_leader_address_arg = opt_arg;

  return 0;
}
