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
#include <stdlib.h>
#include <sysexits.h>

static bool graphd_should_write_on_shutdown(graphd_handle* g) {
  if (g->g_smp_processes > 1 && g->g_started) {
    if (g->g_smp_proc_type == GRAPHD_SMP_PROCESS_LEADER) return true;
    return false;
  }
  return true;
}

void graphd_shutdown(void* data, srv_handle* srv) {
  graphd_handle* g = data;

  if (g == NULL) return;

  if (g->g_rep_master_address) srv_address_destroy(g->g_rep_master_address);
  if (g->g_rep_write_address) srv_address_destroy(g->g_rep_write_address);

  /* Delete the checkpoint delay callback, if we have one.
   */
  if (g->g_checkpoint_delay != NULL) {
    srv_delay_destroy(g->g_checkpoint_delay);
    g->g_checkpoint_delay = NULL;
  }

  /*  Delete the replica reconnect delay callback, if we have one.
   */
  if (g->g_rep_reconnect_delay != NULL) {
    srv_delay_destroy(g->g_rep_reconnect_delay);
    g->g_rep_reconnect_delay = NULL;
  }

  /* Remove idle callbacks.
   */
  graphd_idle_finish(g);

  /* Free the is-a/linksto cache
   */
  graphd_islink_finish(g);

  /* Make sure database details are written out to disk. */
  if (g->g_pdb != NULL && graphd_should_write_on_shutdown(g)) {
    int result = 0, err;
    char const* result_func = NULL;
    char path_buf[200];
    char const* path;

    if ((path = pdb_database_path(g->g_pdb)) != NULL)
      snprintf(path_buf, sizeof path_buf, "%s", path);
    else
      strcpy(path_buf, "???");

    err = pdb_checkpoint_mandatory(g->g_pdb, true);
    if (err == GRAPHD_ERR_ALREADY) err = 0;
    if (err != 0 && result == 0) {
      result_func = "pdb_checkpoint_mandatory";
      result = err;
    }

    /* We do checkpoint optional twice, once to finish up an
     * in-progress checkpoint and the second time to get ourselves
     * caught up from wherever the in-progress checkpoint left off.
     */
    err = pdb_checkpoint_optional(g->g_pdb, 0);
    if (err && !result) {
      result_func = "pdb_checkpoint_optional(1)";
      result = err;
    }

    err = pdb_checkpoint_optional(g->g_pdb, 0);
    if (err && !result) {
      result_func = "pdb_checkpoint_optional(2)";
      result = err;
    }

    err = pdb_destroy(g->g_pdb);
    if (err != 0 && result == 0) {
      result_func = "pdb_destroy(2)";
      result = err;
    }

    g->g_pdb = NULL;

    cl_cover(srv_log(srv));
    if (result != 0)
      srv_epitaph_print(srv, EX_SOFTWARE,
                        "unexpected error from %s while closing "
                        "database \"%s\": %s - check logfile "
                        "for details",
                        result_func ? result_func : "(unknown)", path_buf,
                        strerror(result));
  }
  if (g->g_dateline != NULL) {
    graph_dateline_destroy(g->g_dateline);
    g->g_dateline = NULL;
  }

  /* Free the iterator resource stack.
   */
  graphd_iterator_resource_finish(g);

  /* Free the interface ID.
   */
  if (g->g_interface_id != NULL) {
    cm_free(srv_mem(srv), g->g_interface_id);
    g->g_interface_id = NULL;
  }

  /*  Free SMP data structures.
   */
  if (g->g_smp_leader_address != NULL &&
      g->g_smp_proc_type == GRAPHD_SMP_PROCESS_LEADER) {
    cm_free(g->g_cm, g->g_smp_leader_address);
    g->g_smp_leader_address = NULL;
  }

  graph_destroy(g->g_graph);
  g->g_graph = NULL;
}
