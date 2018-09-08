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
#include <errno.h>
#include <stdio.h>
#include <stdbool.h>

#define GRAPHD_CHECK_CACHE_MAX 1024

typedef struct {
  unsigned int ccs_value : 1;
  unsigned int ccs_use : 31;

} graphd_check_cache_slot;

int graphd_check_cache_initialize(graphd_handle *g, graphd_check_cache *cc) {
  cc->cc_initialized = false;
  return 0;
}

/*  Free resources associated with the cache.
 */
void graphd_check_cache_finish(graphd_handle *g, graphd_check_cache *cc) {
  if (cc->cc_initialized) {
    cm_hashfinish(&cc->cc_hash);
    cc->cc_initialized = false;
  }
}

/*  Add the fact that an id is or isn't present to the cache.
 */
int graphd_check_cache_add(graphd_handle *g, graphd_check_cache *cc, pdb_id id,
                           bool is_present) {
  graphd_check_cache_slot *ccs;

  cl_log(g->g_cl, CL_LEVEL_VERBOSE,
         "graphd_check_cache_add cc=%p id=%llx val=%s", (void *)cc,
         (unsigned long long)id, is_present ? "true" : "false");

  if (!cc->cc_initialized ||
      cm_hashnelems(&cc->cc_hash) >= GRAPHD_CHECK_CACHE_MAX)
    return 0;

  ccs = cm_hnew(&cc->cc_hash, graphd_check_cache_slot, &id, sizeof id);
  if (ccs == NULL) return ENOMEM;

  /*  If we already know this value, its cached presence value
   *  must match what we're returning.
   */
  if (ccs->ccs_use > 0)
    cl_assert(g->g_cl, !ccs->ccs_value == !is_present);
  else {
    ccs->ccs_value = is_present;
    ccs->ccs_use = 1;
  }
  return 0;
}

/*  Test for presence of an ID in the cache.
 *  Return GRAPHD_ERR_NO if it's not in the cache,
 *  otherwise 0, and set *is_present_out to
 *  the boolean presence value of the id in the cache.
 */
int graphd_check_cache_test(graphd_handle *g, graphd_check_cache *cc, pdb_id id,
                            bool *is_present_out) {
  graphd_check_cache_slot *ccs;

  if (!cc->cc_initialized) {
    int err = cm_hashinit(g->g_cm, &cc->cc_hash,
                          sizeof(graphd_check_cache_slot), 256);
    if (err != 0) return err;

    cc->cc_initialized = true;
    return GRAPHD_ERR_NO;
  }

  ccs = cm_haccess(&cc->cc_hash, graphd_check_cache_slot, &id, sizeof id);
  if (ccs == NULL) return GRAPHD_ERR_NO;

  ccs->ccs_use++;
  *is_present_out = ccs->ccs_value;

  return 0;
}
