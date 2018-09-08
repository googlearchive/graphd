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
#include "libcm/cm.h"
#include <string.h>

/**
 * @brief Compute a - b, correctly handling wraparound.
 *
 *  In non-cumulative cases where diff doesn't make sense,
 *  this code doesn't compute a-b, but instead just copies
 *  the newer (current) state.
 *
 * @param a     The larger (newer, later) of two runtime statistics samples
 * @param b     The smaller (earlier) of two runtime statistics samples
 * @param c     Out: a - b.
 */
void cm_runtime_statistics_diff(cm_runtime_statistics const* a,
                                cm_runtime_statistics const* b,
                                cm_runtime_statistics* c) {
#define SUB(x) c->x = a->x - b->x
#define SET(x) c->x = a->x

  SET(cmrts_num_fragments);
  SET(cmrts_max_fragments);

  SET(cmrts_size);
  SET(cmrts_max_size);

  SUB(cmrts_total_size);
  SUB(cmrts_total_allocs);

#undef SUB
}

/**
 * @brief Compute a + b, correctly handling wraparound (by doing nothing)
 *
 *  In non-cumulative cases where this doesn't make sense,
 *  it simply copies the new state <b> into the accumulator <c>.
 *
 * @param a     A runtime statistics sample
 * @param b     Another runtime statistics sample
 * @param c     Out: a + b.
 */
void cm_runtime_statistics_add(cm_runtime_statistics const* a,
                               cm_runtime_statistics const* b,
                               cm_runtime_statistics* c) {
#define ADD(x) c->x = a->x + b->x

#undef SET
#define SET(x) c->x = b->x

  SET(cmrts_num_fragments);
  SET(cmrts_max_fragments);

  SET(cmrts_size);
  SET(cmrts_max_size);

  ADD(cmrts_total_size);
  ADD(cmrts_total_allocs);

#undef ADD
}

/**
 * @brief Assign maximum statistics to a set.
 * @param r     Set to be initialized
 */
void cm_runtime_statistics_max(cm_runtime_statistics* r) {
  r->cmrts_num_fragments = r->cmrts_max_fragments = r->cmrts_size =
      r->cmrts_max_size = r->cmrts_total_size = r->cmrts_total_allocs =
          (unsigned long long)-1 / 2;
}

/**
 * @brief Is small > large in any of small's members?
 * @param small A runtime statistics set we expect to be <= large.
 * @param large A runtime statistics set we don't want to exceed.
 * @param report NULL or an output parameter to which only those
 *		members of small that are too large are assigned.
 * @return true if small is too large, false if everything is alright
 */
bool cm_runtime_statistics_exceeds(cm_runtime_statistics const* small,
                                   cm_runtime_statistics const* large,
                                   cm_runtime_statistics* report) {
#define TST(x)                                \
  if (small->x > large->x) {                  \
    if (report != NULL) report->x = small->x; \
    return true;                              \
  }

  /* num_fragments and size are never greater than max_fragments or
   * max_size, so don't bother testing them */

  /*	TST(cmrts_num_fragments); */
  TST(cmrts_max_fragments);

  /*	TST(cmrts_size); */
  TST(cmrts_max_size);

  /* i am concerned that in a heap with a really long lifetime
   * cmrts_total_size could overflow at some point. a long long
   * is a staggeringly large number, but nonetheless, it could
   * happen and then there would be confusion. you have been
   * warned. MMP */

  /*	TST(cmrts_total_size); */
  TST(cmrts_total_allocs);

#undef TST

  return false;
}

/**
 * @brief Limit a larger number to at least a smaller one.
 * @param large A runtime statistics set
 * @param limit_below A runtime statistics set we expect to be <= large.
 */
void cm_runtime_statistics_limit_below(cm_runtime_statistics const* limit_below,
                                       cm_runtime_statistics* large) {
#define LIMIT(x)                  \
  if (large->x >= limit_below->x) \
    ;                             \
  else                            \
  large->x = limit_below->x

  LIMIT(cmrts_max_fragments);
  LIMIT(cmrts_max_size);
  LIMIT(cmrts_total_allocs);

#undef LIMIT
}
