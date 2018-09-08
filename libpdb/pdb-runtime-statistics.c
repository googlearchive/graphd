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
#include "libpdb/pdbp.h"

#include <errno.h>

int pdb_runtime_statistics_get(pdb_handle* pdb,
                               pdb_runtime_statistics* stat_out) {
  if (pdb == NULL) return EINVAL;

  *stat_out = pdb->pdb_runtime_statistics;
  return 0;
}

/**
 * @brief Compute a - b, correctly handling wraparound.
 * @param a	The larger (later) of two runtime statistics samples
 * @param b	The smaller (earlier) of two runtime statistics samples
 * @param c	Out: a - b.
 */
void pdb_runtime_statistics_diff(pdb_runtime_statistics const* a,
                                 pdb_runtime_statistics const* b,
                                 pdb_runtime_statistics* c) {
#define SUB(x) c->x = a->x - b->x

  SUB(rts_primitives_written);
  SUB(rts_primitives_read);

  SUB(rts_index_extents_read);
  SUB(rts_index_elements_read);
  SUB(rts_index_elements_written);

#undef SUB
}

/**
 * @brief Compute a + b, correctly handling wraparound (by doing nothing)
 * @param a	A runtime statistics sample
 * @param b	Another runtime statistics sample
 * @param c	Out: a + b.
 */
void pdb_runtime_statistics_add(pdb_runtime_statistics const* a,
                                pdb_runtime_statistics const* b,
                                pdb_runtime_statistics* c) {
#define ADD(x) c->x = a->x + b->x

  ADD(rts_primitives_written);
  ADD(rts_primitives_read);

  ADD(rts_index_extents_read);
  ADD(rts_index_elements_read);
  ADD(rts_index_elements_written);

#undef ADD
}

/**
 * @brief Assign maximum statistics to a set.
 * @param r	Set to be initialized
 */
void pdb_runtime_statistics_max(pdb_runtime_statistics* r) {
  r->rts_primitives_written = r->rts_primitives_read =
      r->rts_index_extents_read = r->rts_index_elements_read =
          r->rts_index_elements_written = (unsigned long long)-1 / 2;
}

/**
 * @brief Is small > large in any of small's members?
 * @param small	A runtime statistics set we expect to be <= large.
 * @param large	A runtime statistics set we don't want to exceed.
 * @param report NULL or an output parameter to which only those
 *		members of small that are too large are assigned.
 * @return true if small is too large, false if everything is alright
 */
bool pdb_runtime_statistics_exceeds(pdb_runtime_statistics const* small,
                                    pdb_runtime_statistics const* large,
                                    pdb_runtime_statistics* report) {
#define TST(x)                                \
  if (small->x > large->x) {                  \
    if (report != NULL) report->x = small->x; \
    return true;                              \
  }

  TST(rts_primitives_written);
  TST(rts_primitives_read);

  TST(rts_index_extents_read);
  TST(rts_index_elements_read);
  TST(rts_index_elements_written);

#undef TST

  return false;
}

/**
 * @brief Is small > large in any of small's members?
 * @param small	A runtime statistics set we expect to be <= large.
 * @param large	A runtime statistics set we don't want to exceed.
 * @param report NULL or an output parameter to which only those
 *		members of small that are too large are assigned.
 * @return true if small is too large, false if everything is alright
 */
void pdb_runtime_statistics_limit_below(
    pdb_runtime_statistics const* limit_below, pdb_runtime_statistics* large) {
#define LIMIT(x)                  \
  if (large->x >= limit_below->x) \
    ;                             \
  else                            \
  large->x = limit_below->x

  LIMIT(rts_primitives_written);
  LIMIT(rts_primitives_read);

  LIMIT(rts_index_extents_read);
  LIMIT(rts_index_elements_read);
  LIMIT(rts_index_elements_written);

#undef LIMIT
}
