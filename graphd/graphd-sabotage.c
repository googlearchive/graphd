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
#include "graphd/graphd-sabotage.h"

#include <errno.h>
#include <stdio.h>
#include <sysexits.h>

static int graphd_sabotage_scan(graphd_handle* g, cm_handle* cm,
                                char const* arg) {
  unsigned long lu;
  graphd_sabotage_handle* gs;
  char const* col;
  cl_loglevel loglevel = CL_LEVEL_ERROR;
  bool deadbeef = false;
  int target = 10;
  int err;
  char const* e;

  if (arg[0] == '0' && arg[1] == 'x') {
    deadbeef = true;
    arg += 2;
  }
  col = strchr(arg, ':');
  if (col != NULL) {
    err = cl_loglevel_from_string(arg, col, NULL, &loglevel);
    if (err != 0) return err;

    arg = col + 1;
  }

  col = strchr(arg, '/');
  if (col != NULL)
    if (sscanf(col + 1, "%d", &target) != 1) return GRAPHD_ERR_LEXICAL;

  if (target < 0 || target > 255) {
    fprintf(stderr, "%s: target must be between 0 and 255 (got: %d)\n",
            srv_program_name(g->g_srv), target);
    exit(EX_USAGE);
  }

  if (sscanf(arg, "%lu", &lu) != 1) return GRAPHD_ERR_LEXICAL;

  e = arg + strlen(arg);

  if ((gs = cm_zalloc(cm, sizeof(*gs))) == NULL) return ENOMEM;

  gs->gs_cycle = (e[-1] == '+');
  if (gs->gs_cycle) gs->gs_increment = (e[-2] == '+');

  gs->gs_countdown = gs->gs_countdown_initial = lu;
  gs->gs_cl = g->g_cl; /* Usually NULL at this time. */
  gs->gs_target = target;
  gs->gs_loglevel = loglevel;
  gs->gs_deadbeef = deadbeef;

  g->g_sabotage = gs;

  return 0;
}

void graphd_sabotage_report(graphd_sabotage_handle* gs, char const* file,
                            int line, char const* func, char const* cond,
                            int local_count) {
  cl_log(gs->gs_cl, gs->gs_loglevel,
         "SABOTAGE[%lu]: %s() [%s:%d; %d of %d] pretends %s",
         gs->gs_countdown_total += gs->gs_countdown_initial, func, file, line,
         local_count, (int)gs->gs_target, cond);

  if (gs->gs_cycle) {
    if (gs->gs_increment) gs->gs_countdown_initial++;
    gs->gs_countdown = gs->gs_countdown_initial;
  }
}

/**
 * @brief Parse an option from the command line.  (Method.)
 *
 *  This is a method of the generic libsrv parameter mechanism,
 *  passed in via a srv_option[] structure declared in graphd.c.
 *
 * @param data		opaque application data handle (i.e., graphd)
 * @param srv 		generic libsrv handle
 * @param cm		allocate through this
 * @param opt		command line option ('d')
 * @param optarg	option's parameter
 *
 * @return 0 on success, a nonzero errno on error.
 */
int graphd_sabotage_option_set(void* data, srv_handle* srv, cm_handle* cm,
                               int opt, char const* opt_arg) {
  graphd_handle* g = data;
  int err;

  if (g->g_sabotage != NULL) {
    fprintf(stderr, "%s: duplicate sabotage option %s", srv_program_name(srv),
            opt_arg);
    exit(EX_USAGE);
  }
  g->g_srv = srv;
  switch (err = graphd_sabotage_scan(g, cm, opt_arg)) {
    case GRAPHD_ERR_LEXICAL:
    case GRAPHD_ERR_SEMANTICS:
      fprintf(stderr, "%s: expected sabotage level:counter[+], got \"%s\"\n",
              srv_program_name(srv), opt_arg);
      exit(EX_USAGE);

    case ENOMEM:
      fprintf(stderr,
              "%s: failed to allocate sabotage "
              "counter \"%s\" - out of memory?!\n",
              srv_program_name(srv), opt_arg);
      exit(EX_OSERR);

    case 0:
      break;

    default:
      fprintf(stderr,
              "%s: unexpected error from sabotage "
              "parser for \"%s\": %s\n",
              srv_program_name(srv), opt_arg, graphd_strerror(err));
      exit(EX_OSERR);
  }
  return 0;
}

int graphd_sabotage_option_configure(void* data, srv_handle* srv,
                                     void* config_data,
                                     srv_config* srv_config_data) {
  graphd_handle* const g = data;
  graphd_config* const gcf = config_data;

  if (g->g_sabotage != NULL) {
    gcf->gcf_sabotage_cf.gsc_loglevel = g->g_sabotage->gs_loglevel;
    gcf->gcf_sabotage_cf.gsc_countdown_initial =
        g->g_sabotage->gs_countdown_initial;
    gcf->gcf_sabotage_cf.gsc_cycle = g->g_sabotage->gs_cycle;
    gcf->gcf_sabotage_cf.gsc_increment = g->g_sabotage->gs_increment;
    gcf->gcf_sabotage_cf.gsc_deadbeef = g->g_sabotage->gs_deadbeef;
    gcf->gcf_sabotage_cf.gsc_target = g->g_sabotage->gs_target;
  }
  return 0;
}

void graphd_sabotage_initialize(graphd_sabotage_handle* gs, cl_handle* cl) {
  gs->gs_cl = cl;
}
