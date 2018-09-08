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
/*
 * All things begin.
 *
 */

#include "graphd/graphd.h"
#include "graphd/graphd-version.h"

#include <stdio.h>
#include <sysexits.h>
#include <string.h>
#include <errno.h>

#include "libaddb/addb.h"
#include "libcl/cl.h"
#include "libcm/cm.h"
#include "libes/es.h"
#include "libgraph/graph.h"
#include "libpdb/pdb.h"
#include "libsrv/srv.h"

#ifndef GRAPHD_VERSION
#define GRAPHD_VERSION "0.1.10"
#endif

#ifndef GRAPHD_DEFAULT_PORT
#define GRAPHD_DEFAULT_PORT 8100
#endif

#ifndef CM_FACILITY_MEMORY
#define CM_FACILITY_MEMORY 0
#endif

static int graphd_noverify_option_set(void* data, srv_handle* srv,
                                      cm_handle* cm, int opt,
                                      char const* opt_arg) {
  graphd_handle* g = data;
  g->g_verify = false;
  return 0;
}

static int graphd_force_option_set(void* data, srv_handle* srv, cm_handle* cm,
                                   int opt, char const* opt_arg) {
  graphd_handle* g = data;
  g->g_force = true;
  return 0;
}

static int graphd_database_exists_option_set(void* data, srv_handle* srv,
                                             cm_handle* cm, int opt,
                                             char const* opt_arg) {
  graphd_handle* g = data;
  g->g_database_must_exist = true;
  return 0;
}

static int graphd_delay_replica_write_option_set(void* data, srv_handle* srv,
                                                 cm_handle* cm, int opt,
                                                 char const* opt_arg) {
  graphd_handle* g = data;
  size_t delay_secs;

  if (sscanf(opt_arg, "%zu", &delay_secs) != 1 || delay_secs <= 0) {
    fprintf(stderr,
            "graphd: expected "
            "positive number with -Z, got \"%s\"\n",
            opt_arg);
    exit(EX_USAGE);
  }
  g->g_should_delay_replica_writes = true;
  g->g_delay_replica_writes_secs = delay_secs;
  return 0;
}

static int graphd_freeze_option_set(void* data, srv_handle* srv, cm_handle* cm,
                                    int opt, char const* opt_arg) {
  graphd_handle* g = data;
  size_t freeze_factor;

  if (sscanf(opt_arg, "%zu", &freeze_factor) != 1 || freeze_factor <= 0) {
    fprintf(stderr,
            "graphd: expected "
            "positive number with -e, got \"%s\"\n",
            opt_arg);
    exit(EX_USAGE);
  }
  g->g_freeze = freeze_factor;
  return 0;
}

static int graphd_instance_option_set(void* data, srv_handle* srv,
                                      cm_handle* cm, int opt,
                                      const char* opt_arg) {
  graphd_handle* g = data;
  if (!opt_arg || (strlen(opt_arg) > 31)) {
    fprintf(stderr, "graphd: expected a short string with -I");
    exit(EX_USAGE);
  }

  if (!graph_dateline_instance_verify(opt_arg, opt_arg + strlen(opt_arg))) {
    fprintf(stderr,
            "graphd: instance id may contain only "
            "[A-Za-z0-9], and must be between 1 and %d "
            "characters long.",
            GRAPH_INSTANCE_ID_SIZE);
    exit(EX_USAGE);
  }
  strcpy(g->g_instance_id, opt_arg);
  return 0;
}

static int graphd_test_option_set(void* data, srv_handle* srv, cm_handle* cm,
                                  int opt, const char* arg) {
  graphd_handle* g = data;

  /* Parse a comma-separated list of options.
   */
  while (*arg != '\0') {
    char const* e = strchr(arg, ',');
    if (e == NULL) e = arg + strlen(arg);

#define IS_LIT(lit, s, e) \
  ((e) - (s) == sizeof(lit) - 1 && !strncasecmp(s, lit, sizeof(lit) - 1))

    if (arg < e) {
      if (IS_LIT("sleep-write", arg, e))
        g->g_test_sleep_write = true;
      else if (IS_LIT("sleep-forever-write", arg, e))
        g->g_test_sleep_forever_write = true;
      else {
        fprintf(stderr,
                "graphd: unexpected argument "
                "\"%.*s\" with -J - supported arguments:\n",
                (int)(e - arg), arg);
        fprintf(stderr,
                "\tsleep-write     sleep 1 second for each write command.\n"
                "\tsleep-forever   block indefinitely on write\n");
        exit(EX_USAGE);
      }
    }
    if (*e == ',')
      arg = e + 1;
    else
      break;
  }
  return 0;
}

static const srv_option graphd_srv_options[] = {
    {"a", "  -a               (aiiiieee) don't verify at startup\n",
     graphd_noverify_option_set, NULL},
    {"b", "  -b               (Boring) make server predictable\n",
     graphd_predictable_option_set, graphd_predictable_option_configure},
    {"C", "  -C               (continue) force graphd to start\n",
     graphd_force_option_set, NULL},
    {"D", "  -D               graphd fails without a database dir\n",
     graphd_database_exists_option_set, NULL},
    {"d:", "  -d directory     use database in <directory>\n",
     graphd_database_option_set, graphd_database_option_configure},
    {"e:", "  -e factor        freeze every <factor> chances\n",
     graphd_freeze_option_set, NULL},
    {"I:", "  -I identifier    assume instance id <identifier>\n",
     graphd_instance_option_set, NULL},
    {"J:", "  -J pattern       execute test behavior <pattern>\n",
     graphd_test_option_set, NULL},
    {"K:",
     "  -K pattern       Specify max RAM sizing parameter when initializing a "
     "new database\n",
     graphd_database_total_memory_set, NULL},
    {"M:", "  -M address       force address as write-master \n",
     graphd_write_master_option_set, NULL},
    {"r:",
     "  -r address       run as replica server (connection required for "
     "startup)\n",
     graphd_replica_option_set_required, graphd_replica_option_configure},
    {"R:",
     "  -R address       run as replica server (connection not required for "
     "startup)\n",
     graphd_replica_option_set_not_required, graphd_replica_option_configure},
    {"s:", "  -s pattern       sabotage according to <pattern>\n",
     graphd_sabotage_option_set, graphd_sabotage_option_configure},
    {"S", "  -S               start with sync=false\n",
     graphd_nosync_option_set, NULL},
    {"T", "  -T               start with transactional=false\n",
     graphd_notransactional_option_set, NULL},
    {"U:",
     "  -U address       use <address> for internal process communication\n",
     graphd_smp_leader_option_set, NULL},
    {"w", "  -w               print database version number and exit\n", NULL,
     NULL, GRAPHD_FORMAT_VERSION},
    {"Z:", "  -Z               delay the writing back to replicas\n",
     graphd_delay_replica_write_option_set, NULL},

    {NULL} /* sentinel */
};

static const srv_config_parameter graphd_srv_configs[] = {
    {"database", graphd_database_config_read, graphd_database_config_open,
     graphd_database_config_run},
    {"replica", graphd_replica_config_read, graphd_replica_config_open,
     graphd_replica_config_run},
    {"archive", graphd_archive_config_read, graphd_replica_config_open,
     graphd_replica_config_run},
    {"request-size-max", graphd_request_size_max_config_read,
     graphd_request_size_max_config_open},
    {"leader-socket", graphd_smp_leader_config_read,
     graphd_smp_leader_config_open},
    {"cost", graphd_cost_config_read, graphd_cost_config_open},
    {"instance-id", graphd_instance_id_config_read,
     graphd_instance_id_config_open},
    {NULL} /* sentinel */
};

void graphd_set_time(graphd_handle* g) {
  if (!g->g_predictable)
    graph_timestamp_sync(&g->g_now, time((time_t*)NULL));
  else {
    unsigned long long count = g->g_pdb ? pdb_primitive_n(g->g_pdb) : 0;
    graph_timestamp_t ts = GRAPH_TIMESTAMP_MAKE(count / 10000, count % 10000);
    if (g->g_now < ts) g->g_now = ts;
  }
}

static void graphd_pre_dispatch(void* data, srv_handle* srv) {
  (void)srv;
  graphd_set_time((graphd_handle*)data);
}

static const srv_build_version_reference graphd_srv_build_versions[] = {
    {"graphd", graphd_build_version},
    {"srv", srv_build_version},
    {"pdb", pdb_build_version},
    {"addb", addb_build_version},
    {"graph", graph_build_version},
    {"es", es_build_version},
    {"cm", cm_build_version},
    {"cl", cl_build_version},
    {NULL, NULL},
};

const cl_facility graphd_facilities[] = {
    {"memory", CM_FACILITY_MEMORY},
    {"query", GRAPHD_FACILITY_QUERY},
    {"scheduler", GRAPHD_FACILITY_SCHEDULER},
    {"linksto", GRAPHD_FACILITY_LINKSTO},
    {NULL, 0, pdb_facilities},
    {NULL}};

static int graphd_spawn(void* data, srv_handle* srv, pid_t new_pid) {
  graphd_handle* g = data;
  return pdb_spawn(g->g_pdb, new_pid);
}

static char const* graphd_cl_strerror(void* data, int err) {
  (void)data;
  return graphd_strerror(err);
}

static int graphd_startup_complete(void* data, srv_handle* srv) {
  graphd_handle* g = data;
  cl_handle* const cl = srv_log(srv);

  cl_log(cl, CL_LEVEL_VERBOSE, "Called startup complete callback");

  if (!g->g_require_replica_connection_for_startup) {
    if (g->g_smp_processes == 1) {
      /* Since g_smp_processes == 1, graphd_smp_startup() will
       * not be called, so we should call this here.
       *
       * It's ok if we are not in replica/archive mode.
       * graphd_replica_connect() will be a successful no-op
       */
      int err = graphd_replica_connect(g);
      if (err) return err;
    }
  }

  g->g_started = true;
  srv_settle_ok(srv);

  return 0;
}

static int graphd_startup(void* data, srv_handle* srv) {
  graphd_handle* const g = data;
  cl_handle* const cl = srv_log(srv);
  char err_buf[1024];
  bool err_retriable;
  int err;

  cl_assert(cl, g != NULL);
  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_startup");

  g->g_started = false;

  if (g->g_cm == NULL) g->g_cm = srv_mem(srv);

  if (g->g_cl == NULL) g->g_cl = cl;

  cl_set_strerror(g->g_cl, graphd_cl_strerror, NULL);
  if (g->g_graph == NULL) {
    g->g_graph = graph_create(g->g_cm, cl);
    if (g->g_graph == NULL) return ENOMEM;
  }
  g->g_srv = srv;
  g->g_smp_proc_type = GRAPHD_SMP_PROCESS_SINGLE;
  g->g_smp_processes = srv_smp_processes(srv);

  /* Usually, we are complete after this function runs.
     If we need a replica connection for startup (-r),
     then set that here and setup the todo item.
  */
  if (g->g_require_replica_connection_for_startup) {
    g->g_startup_want_replica_connection = true;
    graphd_startup_todo_initialize(&g->g_startup_todo_replica_connection);
    graphd_startup_todo_add(g, &g->g_startup_todo_replica_connection);
  }

  if (g->g_smp_processes > 1) {
    bool transactional_db = pdb_transactional(g->g_pdb);

    /* If you are an smp replica you should not use option Z */
    if (g->g_should_delay_replica_writes) {
      cl_log(cl, CL_LEVEL_ERROR,
             "This is an SMP replica, disabling -Z delay write option");
      g->g_should_delay_replica_writes = false;
      g->g_delay_replica_writes_secs = 0;
    }

    if (transactional_db) {
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "Attempting to start an SMP graph "
             "with a transactional=true database.");
      return GRAPHD_ERR_NOT_SUPPORTED;
    }
    if (!(g->g_access == GRAPHD_ACCESS_REPLICA ||
          g->g_access == GRAPHD_ACCESS_REPLICA_SYNC ||
          g->g_access == GRAPHD_ACCESS_ARCHIVE)) {
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "Attempting to start an SMP graph "
             "on a non-replica configuration.");
      return GRAPHD_ERR_NOT_SUPPORTED;
    }
    /* Add one to the number of configured processes (for the leader doing
     * writes) */
    g->g_smp_processes += 1;
    srv_set_smp_processes(srv, g->g_smp_processes);
  }

  g->g_diary = cl_diary_create(cl);
  if (!g->g_diary)
    cl_log_errno(cl, CL_LEVEL_ERROR, "cl_diary_create", ENOMEM,
                 "Unable to create diary");
  g->g_diary_cl = cl_create();
  if (!g->g_diary_cl)
    cl_log_errno(cl, CL_LEVEL_ERROR, "cl_create", ENOMEM,
                 "Unable to create diary cl");

  if (g->g_diary && g->g_diary_cl) {
    cl_diary(g->g_diary_cl, g->g_diary);
    cl_set_loglevel_full(g->g_diary_cl, CL_LEVEL_VERBOSE);
    srv_set_diary(srv, g->g_diary_cl);
  } else {
    cl_diary_destroy(g->g_diary);
    cl_destroy(g->g_diary_cl);
    g->g_diary = 0;
    g->g_diary_cl = 0;
  }

  graphd_runtime_statistics_max(&g->g_runtime_statistics_allowance);

  err = graphd_access_set_global(g, g->g_access, &err_retriable, err_buf,
                                 sizeof err_buf);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_OPERATOR_ERROR, "graphd_access_set_global", err,
                 "Unable to initialize access mode to %d: %s", g->g_access,
                 err_buf);

    if (!err_retriable) {
      srv_shared_set_restart(g->g_srv, false);
      srv_epitaph_print(g->g_srv, EX_SOFTWARE,
                        "Unable to initialize access mode to \"%s\": %s",
                        graphd_access_global_to_string(g->g_access), err_buf);
      return err;
    }
  }
  err = graphd_iterator_resource_initialize(g);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_iterator_resource_initialize", err,
                 "can't initialize iterator resource hashtable");
    return err;
  }
  err = graphd_islink_initialize(g);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_islink_initialize", err,
                 "can't initialize is-a/linksto cache");
    return err;
  }

  graphd_idle_initialize(g);

  if (g->g_sabotage != NULL) graphd_sabotage_initialize(g->g_sabotage, g->g_cl);

  graphd_startup_todo_check(g);
  return err;
}

static srv_application graphd_srv_application = {
    "graphd",

    graphd_build_version,
    graphd_srv_build_versions,

    graphd_spawn,
    graphd_startup,
    graphd_shutdown,
    graphd_session_shutdown,
    graphd_session_initialize,
    graphd_session_interactive_prompt,
    graphd_request_initialize,
    graphd_request_input,
    graphd_request_run,
    graphd_request_output,
    graphd_sleep,
    graphd_request_finish,
    graphd_pre_dispatch,
    graphd_startup_complete,
    graphd_smp_startup,
    graphd_smp_finish,

    "/var/run/graphd.pid",
    GRAPHD_DEFAULT_PORT,
    "/usr/local/etc/graph.conf",

    graphd_srv_options,

    sizeof(graphd_config),
    graphd_srv_configs,

    sizeof(graphd_session),
    sizeof(graphd_request),

    graphd_facilities};

int main(int argc, char** argv) {
  graphd_handle g[1];

  memset(g, 0, sizeof g);
  graphd_type_initialize(g);

  g->g_dateline_suspended_max = PDB_ID_NONE;
  g->g_verify = true;
  g->g_force = false;
  g->g_database_must_exist = false;

  return srv_main(argc, argv, g, &graphd_srv_application);
}
