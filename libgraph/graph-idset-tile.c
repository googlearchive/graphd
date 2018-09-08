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
#include <errno.h>
#include <stdio.h>

#include "libgraph/graphp.h"

#define IDT_TILE_MAX 1024

/* Use of graph_idset_pos:
 *
 *	gip_ull  -- the tile index
 *      gip_size -- index within a tile.
 */
typedef unsigned long long tile_index_t;
typedef size_t id_index_t;

#define gip_tile_i gip_ull
#define gip_id_i gip_size

#define CHECK(idt)                \
  cl_assert(                      \
      (idt)->idt_graph->graph_cl, \
      idt->idt_tile_n < 1 || idt->idt_tile[idt->idt_tile_n - 1]->t_n != 0)

typedef struct tile {
  unsigned long long t_data[IDT_TILE_MAX];

  /* How many slots of this tile are actually occupied?
   * (At most: IDT_TILE_MAX)
   */
  size_t t_n;

} tile;

typedef struct graph_idset_tile {
  /* Generic part */

  graph_idset_type const *idt_type;
  graph_handle *idt_graph;
  unsigned long long idt_n;
  unsigned long idt_linkcount;

  /* Specific part */

  tile **idt_tile;
  size_t idt_tile_m;
  size_t idt_tile_n;

} graph_idset_tile;

static bool tile_for_id(graph_idset_tile const *idt, unsigned long long id,
                        unsigned long long *tile_i_out) {
  tile *const *s, *const *e;

  if (idt->idt_tile_n <= 0) {
    *tile_i_out = 0;
    return false;
  }

  s = idt->idt_tile;
  e = idt->idt_tile + idt->idt_tile_n;

  while (s < e) {
    tile *const *middle = s + (e - s) / 2;

    cl_assert(idt->idt_graph->graph_cl, (*middle)->t_n != 0);

    if ((*middle)->t_data[0] > id)
      e = middle;

    else if ((*middle)->t_data[(*middle)->t_n - 1] < id)
      s = middle + 1;
    else {
      *tile_i_out = middle - idt->idt_tile;
      return true;
    }
  }

  *tile_i_out = e - idt->idt_tile;
  if (*tile_i_out >= idt->idt_tile_n) --*tile_i_out;
  return true;
}

static bool id_in_tile(tile const *t, unsigned long long id,
                       id_index_t *off_out) {
  unsigned long long const *s, *e;

  if (t == NULL) {
    *off_out = 0;
    return false;
  }

  s = t->t_data;
  e = t->t_data + t->t_n;

  while (s < e) {
    unsigned long long const *middle = s + (e - s) / 2;

    if (*middle > id)
      e = middle;

    else if (*middle < id)
      s = middle + 1;
    else {
      *off_out = middle - t->t_data;
      return true;
    }
  }
  *off_out = e - t->t_data;

  return *off_out < t->t_n && *e == id;
}

static int tile_alloc(graph_idset_tile *idt, tile_index_t tile_i) {
  tile *t;

  cl_assert(idt->idt_graph->graph_cl, tile_i <= idt->idt_tile_n);
  CHECK(idt);

  /* Grow idt's tile array by one.
   */
  if (idt->idt_tile_n >= idt->idt_tile_m) {
    tile **tmp;

    tmp = cm_realloc(idt->idt_graph->graph_cm, idt->idt_tile,
                     (idt->idt_tile_m + 1024) * sizeof(*idt->idt_tile));
    if (tmp == NULL) return errno ? errno : ENOMEM;

    idt->idt_tile_m += 1024;
    idt->idt_tile = tmp;
  }

  t = cm_malloc(idt->idt_graph->graph_cm, sizeof(*t));
  if (t == NULL) return errno ? errno : ENOMEM;
  t->t_n = 0;

  /*  Move the tail of the dispatch array out of the way.
   */
  if (tile_i < idt->idt_tile_n)
    memmove(idt->idt_tile + tile_i + 1, idt->idt_tile + tile_i,
            sizeof(*idt->idt_tile) * (idt->idt_tile_n - tile_i));

  /*  Store t in the dispatch table.
   */
  idt->idt_tile_n++;
  idt->idt_tile[tile_i] = t;

  cl_assert(idt->idt_graph->graph_cl, tile_i < idt->idt_tile_n);
  cl_assert(idt->idt_graph->graph_cl,
            tile_i == 0 || idt->idt_tile[tile_i - 1]->t_n != 0);
  cl_assert(idt->idt_graph->graph_cl, tile_i == idt->idt_tile_n - 1 ||
                                          idt->idt_tile[tile_i + 1]->t_n != 0);

  return 0;
}

/*  Make sure that there is at least one free slot in idt[tile][slot].
 */
static int tile_make_room(graph_idset_tile *idt, tile_index_t *tile_i_inout,
                          id_index_t *id_i_inout) {
  tile *t = idt->idt_tile[*tile_i_inout];
  tile *t2;
  int err;

  CHECK(idt);

  if (t->t_n < IDT_TILE_MAX) return 0;

  cl_assert(idt->idt_graph->graph_cl, t->t_n == IDT_TILE_MAX);
  cl_assert(idt->idt_graph->graph_cl, *tile_i_inout < idt->idt_tile_n);

  err = tile_alloc(idt, *tile_i_inout + 1);
  if (err != 0) return err;

  t2 = idt->idt_tile[*tile_i_inout + 1];

  cl_assert(idt->idt_graph->graph_cl, t2 != NULL);
  cl_assert(idt->idt_graph->graph_cl, t2->t_n == 0);
  cl_assert(idt->idt_graph->graph_cl, t->t_n == IDT_TILE_MAX);

  /*  If *id_i_inout is at the very end, just start
   *  a new tile.  Otherwise, split along the median.
   */
  if (*id_i_inout == t->t_n) {
    *id_i_inout = 0;
    ++*tile_i_inout;

  } else {
    t2->t_n = IDT_TILE_MAX / 2;
    t->t_n -= t2->t_n;

    memcpy(t2->t_data, t->t_data + t->t_n, sizeof(*t2->t_data) * t2->t_n);

    /*  If the target lives in the second tile,
     *  adjust the indices.
     *
     *  If the target lies between the two tiles,
     *  favor appending to the first.  (Appending
     *  at the end is cheaper.)
     */
    if (*id_i_inout > t->t_n) {
      *id_i_inout -= t->t_n;
      ++*tile_i_inout;
    }
  }

  cl_assert(idt->idt_graph->graph_cl, *tile_i_inout < idt->idt_tile_n);
  cl_assert(idt->idt_graph->graph_cl,
            *id_i_inout <= idt->idt_tile[*tile_i_inout]->t_n);
  cl_assert(idt->idt_graph->graph_cl,
            tile_i_inout == 0 || *tile_i_inout == 0 ||
                idt->idt_tile[*tile_i_inout - 1]->t_n > 0);

  return 0;
}

static void graph_idset_tile_next_reset(graph_idset *gi,
                                        graph_idset_position *pos) {
  pos->gip_id_i = 0;
  pos->gip_tile_i = 0;
}

static bool graph_idset_tile_next(graph_idset *gi, unsigned long long *id_out,
                                  graph_idset_position *gip) {
  graph_idset_tile const *idt = (graph_idset_tile const *)gi;

  CHECK(idt);

  while (gip->gip_tile_i < idt->idt_tile_n) {
    tile const *t = idt->idt_tile[gip->gip_tile_i];

    if (gip->gip_id_i < t->t_n) {
      *id_out = t->t_data[gip->gip_id_i++];
      return true;
    }
    gip->gip_tile_i++;
    gip->gip_id_i = 0;
  }
  return false;
}

static void graph_idset_tile_prev_reset(graph_idset *gi,
                                        graph_idset_position *pos) {
  graph_idset_tile const *idt = (graph_idset_tile const *)gi;

  /* At the end = at the first entry of a nonexistent tile.
   */
  pos->gip_tile_i = idt->idt_tile_n;
  pos->gip_id_i = 0;
}

static bool graph_idset_tile_prev(graph_idset *gi, unsigned long long *id_out,
                                  graph_idset_position *gip) {
  graph_idset_tile const *idt = (graph_idset_tile const *)gi;

  CHECK(idt);

  if (gip->gip_tile_i > idt->idt_tile_n) {
    gip->gip_tile_i = idt->idt_tile_n;
    gip->gip_id_i = 0;
  }

  for (;;) {
    tile const *t;

    if (gip->gip_tile_i < idt->idt_tile_n) {
      t = idt->idt_tile[gip->gip_tile_i];
      if (gip->gip_id_i > t->t_n) gip->gip_id_i = t->t_n;

      if (gip->gip_id_i > 0) {
        *id_out = t->t_data[--(gip->gip_id_i)];
        return true;
      }
    }

    if (gip->gip_tile_i == 0) break;

    cl_assert(idt->idt_graph->graph_cl, gip->gip_tile_i > 0);
    cl_assert(idt->idt_graph->graph_cl, gip->gip_tile_i <= idt->idt_tile_n);

    gip->gip_tile_i--;
    t = idt->idt_tile[gip->gip_tile_i];
    gip->gip_id_i = t->t_n;
  }
  return false;
}

static bool graph_idset_tile_locate(graph_idset *gi, unsigned long long val,
                                    graph_idset_position *pos) {
  graph_idset_tile const *idt = (graph_idset_tile *)gi;

  CHECK(idt);

  /* Empty?
   */
  if (idt->idt_tile_n == 0) {
    pos->gip_tile_i = 0;
    pos->gip_id_i = 0;

    return false;
  }
  tile_for_id(idt, val, &pos->gip_tile_i);
  return id_in_tile(idt->idt_tile[pos->gip_tile_i], val, &pos->gip_id_i);
}

static bool graph_idset_tile_check(graph_idset *gi, unsigned long long val) {
  graph_idset_tile const *idt = (graph_idset_tile *)gi;
  tile_index_t tile_i;
  id_index_t id_i;

  if (idt->idt_tile_n == 0) return false;

  CHECK(idt);

  tile_for_id(idt, val, &tile_i);
  return id_in_tile(idt->idt_tile[tile_i], val, &id_i);
}

static void graph_idset_tile_free(graph_idset *idset) {
  graph_idset_tile *idt = (graph_idset_tile *)idset;

  if (idt != NULL) {
    tile_index_t off;

    if (idt->idt_tile != NULL) {
      for (off = 0; off < idt->idt_tile_n; off++)
        cm_free(idt->idt_graph->graph_cm, idt->idt_tile[off]);

      cm_free(idt->idt_graph->graph_cm, idt->idt_tile);
    }
    cm_free(idt->idt_graph->graph_cm, idt);
  }
}

static int graph_idset_tile_insert(graph_idset *gi, unsigned long long val) {
  graph_idset_tile *idt = (graph_idset_tile *)gi;
  tile_index_t tile_i;
  tile *t;
  int err;

  CHECK(idt);

  if (idt->idt_tile_n == 0) {
    if ((err = tile_alloc(idt, 0)) != 0) {
      cl_log(gi->gi_graph->graph_cl, CL_LEVEL_VERBOSE,
             "graph_insert_tile_insert fails: "
             "error from tile_alloc - %s!",
             strerror(err));
      CHECK(idt);
      return err;
    }

    t = idt->idt_tile[0];
    cl_assert(idt->idt_graph->graph_cl, t->t_n == 0);
    t->t_data[t->t_n++] = val;
    CHECK(idt);
  } else {
    id_index_t id_i;

    tile_for_id(idt, val, &tile_i);

    cl_assert(idt->idt_graph->graph_cl, tile_i < idt->idt_tile_n);
    if (id_in_tile(idt->idt_tile[tile_i], val, &id_i)) {
      cl_log(gi->gi_graph->graph_cl, CL_LEVEL_VERBOSE,
             "graph_insert_tile_insert: "
             "id_in_tile confirms %llu in tile %lld at %lld",
             val, (long long)tile_i, (long long)id_i);
      CHECK(idt);
      return 0;
    }
    if (idt->idt_tile[tile_i]->t_n >= IDT_TILE_MAX) {
      err = tile_make_room(idt, &tile_i, &id_i);
      if (err != 0) {
        cl_log(gi->gi_graph->graph_cl, CL_LEVEL_FAIL,
               "graph_insert_tile_insert: "
               "tile_make_room fails: %s",
               strerror(err));
        CHECK(idt);
        return err;
      }
    }

    cl_assert(idt->idt_graph->graph_cl, tile_i < idt->idt_tile_n);
    t = idt->idt_tile[tile_i];

    /* Insert the ID at offset <id_i>.
     */
    cl_assert(idt->idt_graph->graph_cl, id_i <= t->t_n);
    cl_assert(idt->idt_graph->graph_cl, id_i < IDT_TILE_MAX);
    if (id_i < t->t_n)
      memmove(t->t_data + id_i + 1, t->t_data + id_i,
              (t->t_n - id_i) * sizeof(*t->t_data));

    t->t_data[id_i] = val;
    t->t_n++;

    cl_assert(idt->idt_graph->graph_cl, id_i < t->t_n);
  }

  /*  Increment the published overall count.
   */
  idt->idt_n++;

  CHECK(idt);

  cl_assert(gi->gi_graph->graph_cl,
            idt->idt_tile[idt->idt_tile_n - 1]->t_n > 0);
  cl_log(gi->gi_graph->graph_cl, CL_LEVEL_VERBOSE,
         "graph_insert_tile_insert: %llu (of %lld)", val,
         (long long)idt->idt_n);
  return 0;
}

static long long idt_offset(graph_idset_tile *idt,
                            graph_idset_position const *a,
                            graph_idset_position const *b) {
  tile_index_t tile_i;
  long long total = 0;

  /* A <= B */

  cl_assert(idt->idt_graph->graph_cl, a->gip_tile_i <= b->gip_tile_i);

  /*  At least two ways of doing this calculation: tiles between
   *  A and B, or everything minus tiles *not* included.
   */
  if ((1 + b->gip_tile_i - a->gip_tile_i) < idt->idt_tile_n / 2) {
    if (a->gip_tile_i == b->gip_tile_i) return b->gip_id_i - a->gip_id_i;

    /* Count entries after b, to the end of b's tile.
     */
    total += b->gip_id_i;

    /*  Count entries from the beginning of a's
     *  tile, up to a's id.
     */
    if (a->gip_tile_i < idt->idt_tile_n)
      total += idt->idt_tile[a->gip_tile_i]->t_n - a->gip_id_i;

    /*  Count entries in the (fully included) tiles
     *  between a's and b's tile.
     */
    for (tile_i = a->gip_tile_i + 1; tile_i < b->gip_tile_i; tile_i++)
      total += idt->idt_tile[tile_i]->t_n;
  } else {
    /* Count the number of elements *not* in the target array.
     */
    if (a->gip_tile_i > 0) /* Entries in tiles before a's tile. */
      for (tile_i = 0; tile_i < a->gip_tile_i; tile_i++)
        total += idt->idt_tile[tile_i]->t_n;

    /* Entries right in front of A */
    total += a->gip_id_i;

    if (b->gip_tile_i < idt->idt_tile_n) /* The entries after B in B's tile. */
      total += idt->idt_tile[b->gip_tile_i]->t_n - (1 + b->gip_id_i);

    /* Entries in tiles after B's tile. */
    for (tile_i = b->gip_tile_i + 1; tile_i < idt->idt_tile_n; tile_i++)
      total += idt->idt_tile[tile_i]->t_n;

    /* And subtract that from the total to yield the number in
     * the target array.
     */
    total = idt->idt_n - total;
  }
  return total;
}

/*  return position(val) - gip, that is, how far forward
 *  you'd have to go from your current position at gip to
 *  stand on top of val, if it existed.
 *
 *  Negative numbers mean you'd have to go backwards.
 */
static long long graph_idset_tile_offset(graph_idset *idset,
                                         graph_idset_position *gip,
                                         unsigned long long val) {
  graph_idset_tile *idt = (graph_idset_tile *)idset;
  graph_idset_position gip2;

  /* Find val (or its insertion point) in the set.
   */
  graph_idset_tile_locate(idset, val, &gip2);

  if (gip2.gip_tile_i == gip->gip_tile_i)
    return gip2.gip_id_i - gip->gip_id_i;
  else if (gip2.gip_tile_i > gip->gip_tile_i)
    return idt_offset(idt, gip, &gip2);
  else
    return -idt_offset(idt, &gip2, gip);
}

static const graph_idset_type graph_idset_tile_type[1] = {
    {graph_idset_tile_insert, graph_idset_tile_check, graph_idset_tile_locate,
     graph_idset_tile_next, graph_idset_tile_next_reset, graph_idset_tile_prev,
     graph_idset_tile_prev_reset, graph_idset_tile_offset,
     graph_idset_tile_free}};

graph_idset *graph_idset_tile_create(graph_handle *g) {
  graph_idset_tile *idt;

  idt = cm_malloc(g->graph_cm, sizeof(*idt));
  if (idt == NULL) return NULL;

  idt->idt_linkcount = 1;
  idt->idt_n = 0;
  idt->idt_tile_m = 0;
  idt->idt_tile_n = 0;
  idt->idt_tile = NULL;
  idt->idt_type = graph_idset_tile_type;
  idt->idt_graph = g;

  return (graph_idset *)idt;
}
