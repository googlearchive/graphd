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

/**
 * @brief Checkpoint graphd's indices (nonessential).
 *
 * @param g		the graphd state.
 */
int graphd_checkpoint_optional(graphd_handle* g) {
  pdb_msclock_t now = pdb_msclock(g->g_pdb);
  int err = pdb_checkpoint_optional(g->g_pdb, now + 100);
  if (err) {
    if (err == PDB_ERR_MORE) g->g_checkpoint_state = GRAPHD_CHECKPOINT_PENDING;
    return err;
  }

  g->g_checkpoint_state = GRAPHD_CHECKPOINT_CURRENT;

  return 0;
}

/* Do some work on a checkpoint if one is in progress
 */
int graphd_checkpoint_work(graphd_handle* g) {
  if (GRAPHD_CHECKPOINT_PENDING != g->g_checkpoint_state) return 0;

  return graphd_checkpoint_optional(g);
}

/**
 * @brief Roll back to before an accident.
 *
 *  The passed-in horizon must be the most recent one.
 *
 * @param g		the graphd module handle.
 * @param horizon	roll back to before this.
 */
int graphd_checkpoint_rollback(graphd_handle* g, unsigned long long horizon) {
  int err;

  /*  Reset the type bootstrap GUIDs, in case the
   *  assignments were part of writing a type system.
   *
   *  The next successful write will reassign to them.
   */
  GRAPH_GUID_MAKE_NULL(g->g_namespace_bootstrap);
  GRAPH_GUID_MAKE_NULL(g->g_attribute_has_key);
  GRAPH_GUID_MAKE_NULL(g->g_namespace_root);
  GRAPH_GUID_MAKE_NULL(g->g_core_scope);

  err = pdb_checkpoint_rollback(g->g_pdb, horizon);
  if (err) return err;

  g->g_checkpoint_state = GRAPHD_CHECKPOINT_CURRENT;

  return 0;
}
