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

#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

/*  The "iterator state" is the long-winded part of a cursor
 *  that doesn't help specify what the set is or where in it
 *  we are - it just helps us _get_ there quickly.
 *
 *  We cache iterator states and publish a unique reference to
 *  the cached state in the iterator.  If the iterator state
 *  expires from the cache, the cursor code knows how to recover
 *  the information - it may just take a little longer.
 */

typedef struct graphd_iterator_state {
  graphd_storable gis_storable;
  cm_handle *gis_cm;
  size_t gis_n;
  char gis_s[1]; /* open-ended */

} graphd_iterator_state;

static void gis_storable_destroy(void *data) {
  graphd_iterator_state *gis = data;
  cm_handle *cm = gis->gis_cm;

  cm_free(cm, gis);
}

static bool gis_storable_equal(void const *A, void const *B) {
  graphd_iterator_state const *a, *b;

  a = (graphd_iterator_state const *)A;
  b = (graphd_iterator_state const *)B;

  return A == B ||
         (a->gis_n == b->gis_n &&
          (a->gis_n == 0 ||
           memcmp(a->gis_s, b->gis_s, sizeof(*a->gis_s) * a->gis_n) == 0));
}

static unsigned long gis_storable_hash(void const *data) {
  graphd_iterator_state const *gis = data;
  unsigned long hash = 0;
  size_t i;

  for (i = 0; i < gis->gis_n; i++) hash = (hash * 33) ^ gis->gis_s[i];
  return hash ^ gis->gis_n;
}

static const graphd_storable_type gis_storable_type = {
    "iterator state", gis_storable_destroy, gis_storable_equal,
    gis_storable_hash};

/*  If the local part of the subiterator in the buffer <buf> is
 *  longer than its ticket would be (and longer than some fixed
 *  minimum), replace its literal with an itstate cache ticket.
 */
int graphd_iterator_state_store(graphd_handle *g, cm_buffer *buf,
                                size_t offset) {
  int err;
  cl_handle *cl = g->g_cl;
  cm_handle *cm = pdb_mem(g->g_pdb);
  char sb[GRAPHD_ITERATOR_RESOURCE_STAMP_SIZE];
  size_t data_n;
  graphd_iterator_state *gis;

  /*  If the iterator state isn't all that large, don't
   *  cache it, just leave it alone.
   */
  cl_assert(cl, offset <= cm_buffer_length(buf));

  data_n = cm_buffer_length(buf) - offset;
  if (data_n <= 1 + GRAPHD_ITERATOR_RESOURCE_STAMP_SIZE) return 0;

  /*  Create a new storable around this.
   */
  gis = cm_malloc(cm, data_n + sizeof(*gis));
  if (gis == NULL) return ENOMEM;

  memset(gis, 0, sizeof(*gis));
  gis->gis_storable.gs_size = data_n + sizeof(*gis);

  gis->gis_storable.gs_type = &gis_storable_type;
  gis->gis_storable.gs_linkcount = 1;
  gis->gis_storable.gs_stored = 0;
  gis->gis_cm = cm;
  memcpy(gis->gis_s, cm_buffer_memory(buf) + offset, data_n);
  gis->gis_n = data_n;

  err = graphd_iterator_resource_store(g, &gis->gis_storable, sb, sizeof sb);
  if (err != 0) {
    cm_free(cm, gis);
    return err;
  }

  /* If graphd_iterator_resource_store() took a link,
   * the linkcount now drops from 2->1.
   * Otherwise, the record already existed, and we're
   * freeing a spurious copy.
   */
  graphd_storable_unlink(gis);

  /* Append the tag to the buffer instead of the
   * long state.
   */
  buf->buf_n = offset;
  return cm_buffer_sprintf(buf, "@%s", sb);
}

/*  Given an iterator state or ticket, get the iterator state.
 *
 *  return values:
 *
 *	0 	- OK, here's your iterator state
 *	GRAPHD_ERR_NO	- sorry, fend for yourself.
 *
 *  All iterators must be able to deal with an empty or
 *  invalid state, as long as their set specification and/or
 *  position are present.
 *
 *  The iterator state is in fairly volatile memory - it
 *  must be parsed immediately, with no intervening resource
 *  allocations.
 */
int graphd_iterator_state_restore(graphd_handle *g, char const **state_s,
                                  char const **state_e) {
  graphd_iterator_state *gis;
  char const *s0;

  /*  Tickets are stored with an @ prefix.
   */
  if (*state_s == NULL || *state_s >= *state_e || **state_s != '@') return 0;

  s0 = ++*state_s;
  gis = graphd_iterator_resource_thaw(g, state_s, *state_e, &gis_storable_type);
  if (gis == NULL) {
    cl_log(g->g_cl, CL_LEVEL_FAIL,
           "graphd_iterator_state_restore: "
           "MISS \"%.*s\"",
           (int)(*state_e - s0), s0);
    return ENOMEM;
  }

  *state_s = gis->gis_s;
  *state_e = gis->gis_s + gis->gis_n;

  graphd_storable_unlink(&gis->gis_storable);

  cl_log(g->g_cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_state_restore: "
         "graphd_iterator_resource_thaw returns %p",
         (void *)gis);
  return 0;
}
