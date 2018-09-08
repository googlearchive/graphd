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
#include "graphd/graphd-iterator-isa.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

/*  5 bytes for position -> id;
 *  16 for a hashtable slot that maps id -> position.
 */
#define ISA_SLOT_SIZE (16 + 5)

typedef unsigned char uc5[5];

struct graphd_iterator_isa_storable {
  graphd_storable is_storable;

  /*  Context for allocation of base and its referers.
   */
  graphd_handle *is_g;

  /*  All the IDs we know are in the set, so far.
   */
  graph_idset *is_ids;

  /*  Convert offset to 5-byte ID.
   */
  unsigned char *is_offset_to_id;
  size_t is_offset_to_id_n;
  size_t is_offset_to_id_m;

  unsigned int is_eof : 1;
};

#define get5(in)                                               \
  ((unsigned long long)((unsigned char *)(in))[0] << (4 * 8) | \
   (unsigned long)((unsigned char *)(in))[1] << (3 * 8) |      \
   (unsigned long)((unsigned char *)(in))[2] << (2 * 8) |      \
   (unsigned int)((unsigned char *)(in))[3] << (1 * 8) |       \
   ((unsigned char *)(in))[4])

#define put5(out, val)                                              \
  (((unsigned char *)(out))[0] =                                    \
       (unsigned char)((unsigned long long)(val) >> (4 * 8)),       \
   ((unsigned char *)(out))[1] =                                    \
       (unsigned char)((unsigned long)(val) >> (3 * 8)),            \
   ((unsigned char *)(out))[2] =                                    \
       (unsigned char)((unsigned long)(val) >> (2 * 8)),            \
   ((unsigned char *)(out))[3] = (unsigned char)((val) >> (1 * 8)), \
   ((unsigned char *)(out))[4] = (unsigned char)(val))

static unsigned long isa_storable_hash(void const *data) {
  graphd_iterator_isa_storable const *is = data;
  return (unsigned long)(intptr_t)is;
}

static void isa_storable_destroy(void *data) {
  graphd_iterator_isa_storable *is = data;
  cm_handle *cm = is->is_g->g_cm;

  cl_log(is->is_g->g_cl, CL_LEVEL_VERBOSE,
         "isa_storable_destroy is=%p, idset=%p", (void *)is,
         (void *)is->is_ids);

  graph_idset_free(is->is_ids);
  if (is->is_offset_to_id_m > 0) {
    cm_free(cm, is->is_offset_to_id);
    is->is_offset_to_id = NULL;
    is->is_offset_to_id_m = 0;
  }
  cm_free(cm, is);
}

static bool isa_storable_equal(void const *A, void const *B) { return A == B; }

static const graphd_storable_type isa_storable_type = {
    "is-a duplicate detector & cache", isa_storable_destroy, isa_storable_equal,
    isa_storable_hash};

bool graphd_iterator_isa_storable_complete(graphd_iterator_isa_storable *is) {
  return is->is_eof;
}

/*  Is there a cache element # (ordinal) POSITION?  If yes,
 *  what ID does it evaluate to?
 */
bool graphd_iterator_isa_storable_offset_to_id(graphd_iterator_isa_storable *is,
                                               size_t position,
                                               pdb_id *id_out) {
  unsigned char const *r;

  if (position >= is->is_offset_to_id_n / 5) {
    *id_out = PDB_ID_NONE;
    return false;
  }

  r = is->is_offset_to_id + position * 5;
  *id_out = get5(r);

  return true;
}

/*  Is ID in the cache?
 */
bool graphd_iterator_isa_storable_check(graphd_iterator_isa_storable const *is,
                                        pdb_id id) {
  return graph_idset_check(is->is_ids, (unsigned long long)id);
}

/*  How many unique pairs of (POSITION, ID) are there
 *  in this base?
 */
size_t graphd_iterator_isa_storable_nelems(
    graphd_iterator_isa_storable const *is) {
  if (is == NULL) return 0;

  return is->is_offset_to_id_n / 5;
}

/*  What's the range of the ids in the cache?
 */
void graphd_iterator_isa_storable_range(graphd_iterator_isa_storable const *is,
                                        pdb_range_estimate *range, size_t off) {
  unsigned char const *r, *e;

  if (is == 0 || off >= is->is_offset_to_id_n / 5) {
    range->range_n_exact = range->range_n_max = range->range_low =
        range->range_high = 0;

    return;
  }

  range->range_low = PDB_ITERATOR_HIGH_ANY;
  range->range_high = PDB_ITERATOR_LOW_ANY;

  r = is->is_offset_to_id + off * 5;
  e = is->is_offset_to_id + is->is_offset_to_id_n;

  range->range_n_exact = range->range_n_max = (is->is_offset_to_id_n / 5) - off;

  for (; r + 5 <= e; r += 5) {
    pdb_id const id = get5(r);

    if (id < range->range_low) range->range_low = id;
    if (id >= range->range_high) range->range_high = id + 1;
  }
  range->range_low_rising = range->range_high_falling = 0;

  return;
}

/*  Create a fresh isa-storable
 *  You will allocate in CM and log through CL.
 *
 *  A successful call transfers one reference to the caller.
 */
graphd_iterator_isa_storable *graphd_iterator_isa_storable_alloc(
    graphd_handle *g) {
  graphd_iterator_isa_storable *is;

  is = cm_malloc(g->g_cm, sizeof(*is));
  if (is == NULL) return NULL;

  memset(is, 0, sizeof(*is));

  is->is_ids = graph_idset_tile_create(g->g_graph);
  if (is->is_ids == NULL) {
    cm_free(g->g_cm, is);
    return NULL;
  }

  cl_log(g->g_cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_isa_storable_alloc is=%p, idset=%p", (void *)is,
         (void *)is->is_ids);

  is->is_storable.gs_linkcount = 1;
  is->is_storable.gs_type = &isa_storable_type;
  is->is_storable.gs_size = sizeof(*is);

  is->is_g = g;
  return is;
}

/*  Hello, I am an is-a iterator at (ordinal) POSITION.
 *  I'm going to return ID.  Please cache that.
 */
int graphd_iterator_isa_storable_add(graphd_handle *g,
                                     graphd_iterator_isa_storable *is,
                                     size_t position, pdb_id id) {
  int err;
  unsigned long long n_before;

  if (position * 5 > is->is_offset_to_id_n) return PDB_ERR_MORE;

  /*  Already there?
   */
  if (position * 5 < is->is_offset_to_id_n) {
    pdb_id my_id;

    cl_assert(is->is_g->g_cl,
              graphd_iterator_isa_storable_offset_to_id(is, position, &my_id) &&
                  my_id == id);
    return 0;
  }

  if (is->is_offset_to_id_n + 5 > is->is_offset_to_id_m) {
    /* Grow the offset-to-id array.
     */
    unsigned char *tmp;

    tmp = cm_realloc(is->is_g->g_cm, is->is_offset_to_id,
                     is->is_offset_to_id_m + 64 * 1024);
    if (tmp == NULL) return ENOMEM;
    is->is_offset_to_id_m += 64 * 1024;
    is->is_offset_to_id = tmp;
    graphd_storable_size_add(g, is, 64 * 1024);
  }

  n_before = is->is_ids->gi_n;

  err = graph_idset_insert(is->is_ids, id);
  if (err != 0) return err;

  /*  Already in the set; no need to expand.
   */
  if (n_before == is->is_ids->gi_n) return 0;

  /*   Remember the offset in *r, and the value in offset_to_id.
   */
  put5(is->is_offset_to_id + is->is_offset_to_id_n, id);
  is->is_offset_to_id_n += 5;

  /*  The new idset entry takes up about 8 bytes.  We've already
   *  charged for the offset-to-id array when we allocated it.
   */

  cl_log(g->g_cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_isa_storable_add(%p [%zu] := %llx)", (void *)is,
         (is->is_offset_to_id_n - 5) / 5, (unsigned long long)id);
  return 0;
}

/* Return NULL or a fresh reference to the saved state.
 */
graphd_iterator_isa_storable *graphd_iterator_isa_storable_thaw(
    graphd_handle *g, char const **s_ptr, char const *e) {
  return graphd_iterator_resource_thaw(g, s_ptr, e, &isa_storable_type);
}

bool graphd_iterator_isa_storable_id_to_offset(
    graphd_iterator_isa_storable const *is, pdb_id id, size_t *offset_out) {
  unsigned char buf[6];
  unsigned char const *r, *e;

  if (id == PDB_ID_NONE) {
    *offset_out = 0;
    return true;
  }

  if (!graph_idset_check(is->is_ids, id)) return false;

  put5(buf, id);

  /*  Slow lineaer search.  We're trying to never do thi.
   */
  e = is->is_offset_to_id + is->is_offset_to_id_n;
  for (r = is->is_offset_to_id; r < e; r += 5) {
    if (*r == buf[0] && r[1] == buf[1] && r[2] == buf[2] && r[3] == buf[3] &&
        r[4] == buf[4]) {
      *offset_out = (r - is->is_offset_to_id) / 5;
      return true;
    }
  }

  cl_notreached(is->is_g->g_cl,
                "idset and to-offset array in "
                "conflict?");
  return false;
}

/*  Returns:
 *	0 after adding a new ID to the cache.
 *      PDB_ERR_MORE after running out of budget
 * 	GRAPHD_ERR_NO after running out of IDs.
 */
int graphd_iterator_isa_storable_run(graphd_handle *g, pdb_iterator *it,
                                     pdb_iterator *sub, int linkage,
                                     graphd_iterator_isa_storable *is,
                                     pdb_budget *budget_inout) {
  int err;
  pdb_id id;
  cl_handle *const cl = g->g_cl;
  char buf[200];
  pdb_budget budget_in = *budget_inout;
  unsigned long long cache_size = is->is_offset_to_id_n;

  cl_assert(cl, is != NULL);

  if (is->is_eof) return GRAPHD_ERR_NO;

  while (*budget_inout >= 0) {
    err = graphd_iterator_isa_run_next(g, it, sub, linkage, NULL, &id,
                                       budget_inout, false);
    if (err != 0) {
      if (err == GRAPHD_ERR_NO) {
        is->is_eof = true;
        cl_log(cl, CL_LEVEL_VERBOSE,
               "graphd_iterator_isa_storable_run: "
               "done ($%lld)",
               (long long)(budget_in - *budget_inout));
      } else if (err == PDB_ERR_MORE)
        cl_log(cl, CL_LEVEL_VERBOSE,
               "graphd_iterator_isa_storable_run: "
               "suspended in "
               "graphd_iterator_isa_run_next ($%lld)",
               (long long)(budget_in - *budget_inout));
      else
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_isa_run_next", err,
                     "unexpected error; sub=%s",
                     pdb_iterator_to_string(g->g_pdb, sub, buf, sizeof buf));
      return err;
    }

    cl_assert(cl, id >= it->it_low && id < it->it_high);

    /*  Add the new ID to the cache.
     */
    err = graphd_iterator_isa_storable_add(
        g, is, graphd_iterator_isa_storable_nelems(is), id);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_isa_storable_add", err,
                   "id=%llx", (unsigned long long)id);
      return err;
    }

    /*  If that made the cache larger, we're done.
     *  Otherwise, this was a duplicate, and we need
     *  to try to read another one.
     */
    if (is->is_offset_to_id_n > cache_size) return 0;

    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_isa_storable_run: "
           "id=%llx is a duplicate",
           (unsigned long long)id);
  }

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_isa_storable_run: "
         "suspended in main loop ($%lld)",
         (long long)(budget_in - *budget_inout));
  return PDB_ERR_MORE;
}
