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

#define GRAPHD_ITERATOR_CACHE_INLINE_N 5

static void gic_storable_destroy(void *data) {
  graphd_iterator_cache *gic = data;
  cm_handle *cm = gic->gic_cm;

  if (gic->gic_m) cm_free(cm, gic->gic_id);
  cm_free(cm, gic);
}

static bool gic_storable_equal(void const *A, void const *B) {
  graphd_iterator_cache const *a, *b;

  /*  While two caches are still growing, they must be
   *  identical to be equal (we can't predict how they'll
   *  continue to grow).  Once they're eof'ed, they are
   *  equal if they contain the same elements.
   */
  return A == B ||
         ((a = (graphd_iterator_cache const *)A)->gic_eof &&
          (b = (graphd_iterator_cache const *)B)->gic_eof &&
          a->gic_n == b->gic_n &&
          (a->gic_n == 0 ||
           memcmp(a->gic_id, b->gic_id, sizeof(*a->gic_id) * a->gic_n) == 0));
}

static unsigned long gic_storable_hash(void const *data) {
  return (intptr_t)data;
}

static const graphd_storable_type gic_storable_type = {
    "iterator cache", gic_storable_destroy, gic_storable_equal,
    gic_storable_hash};

/*  The iterator cache likes holding IDs that were expensive to generate.
 *  The more expensive its contents, and the more its contents are actually
 *  used, the more it holds.
 */

graphd_iterator_cache *graphd_iterator_cache_create(graphd_handle *graphd,
                                                    size_t m) {
  pdb_handle *pdb = graphd->g_pdb;
  cm_handle *cm = pdb_mem(pdb);
  cl_handle *cl = pdb_log(pdb);
  graphd_iterator_cache *gic;

  gic = cm_malloc(cm, sizeof(*gic));
  if (gic == NULL) return NULL;

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_iterator_cache_create %p[%zu]",
         (void *)gic, m);

  memset(gic, 0, sizeof(*gic));
  gic->gic_storable.gs_type = &gic_storable_type;
  gic->gic_storable.gs_linkcount = 1;
  gic->gic_storable.gs_size = sizeof(*gic);

  gic->gic_graphd = graphd;
  gic->gic_cm = cm;
  gic->gic_use_total = 0;
  gic->gic_cost_total = 0;
  gic->gic_cost = 1;
  gic->gic_m = m;
  gic->gic_n = 0;
  gic->gic_eof = false;

  if (m == 0)
    gic->gic_id = NULL;
  else {
    gic->gic_id = cm_malloc(cm, m * sizeof(*gic->gic_id));
    if (gic->gic_id == NULL) {
      cm_free(cm, gic);
      return NULL;
    }
  }
  return gic;
}

void graphd_iterator_cache_destroy(graphd_iterator_cache *gic) {
  if (gic != NULL) {
    if (gic->gic_storable.gs_linkcount <= 1)
      gic_storable_destroy(gic);
    else
      gic->gic_storable.gs_linkcount--;
  }
}

void graphd_iterator_cache_dup(graphd_iterator_cache *gic) {
  gic->gic_storable.gs_linkcount++;
}

int graphd_iterator_cache_add(graphd_iterator_cache *gic, pdb_id id,
                              pdb_budget id_cost) {
  if (gic->gic_n > 0 && gic->gic_id[gic->gic_n - 1] == id) return 0;

  if (gic->gic_n >= gic->gic_m) {
    cm_handle *cm = gic->gic_cm;
    size_t need;
    pdb_id *tmp;

    /*  Should we grow?
     *
     *  We should grow if the benefits outweigh the cost.
     *  When this type of cache is involved, that's pretty
     *  much always.
     */
    if (gic->gic_m >= 64 * 1024)
      need = gic->gic_m + 64 * 1024;
    else
      need = gic->gic_m ? gic->gic_m * 2 : 8;

    tmp = cm_realloc(cm, gic->gic_id, need * sizeof(*tmp));
    if (tmp == NULL) return ENOMEM;
    gic->gic_id = tmp;
    gic->gic_m = need;
  }

  gic->gic_cost_total += id_cost;

  gic->gic_id[gic->gic_n++] = id;

  gic->gic_cost = gic->gic_cost_total / gic->gic_n;
  if (gic->gic_cost == 0) gic->gic_cost = 1;

  graphd_storable_size_add(gic->gic_graphd, gic, sizeof(*gic->gic_id));

  gic->gic_eof = false;
  return 0;
}

void graphd_iterator_cache_eof(graphd_iterator_cache *gic) {
  gic->gic_eof = true;
}

/**
 * @brief Find the ID closest to a given index in the cache.
 *
 * @param pdb	database we're doing that for
 * @param it	iterator we're part of
 * @param gic	cache descriptor
 * @param id_inout	in: ID to search for; out: closest nearby
 * @param off_out	out: offset of found ID.
 *
 * @return 0 if the ID or closest nearby ID was found.
 * @return GRAPHD_ERR_MORE if the ID or a nearby ID *may* be part of
 *	the result set, but the cache doesn't have the answer.
 */
int graphd_iterator_cache_search(pdb_handle *pdb, pdb_iterator *it,
                                 graphd_iterator_cache *gic, pdb_id *id_inout,
                                 size_t *off_out) {
  cl_handle *cl = pdb_log(pdb);

  cl_assert(cl, gic != NULL);

  if (gic->gic_n == 0) return gic->gic_eof ? GRAPHD_ERR_NO : GRAPHD_ERR_MORE;

  if (!pdb_iterator_sorted(pdb, it)) {
    size_t i;

    /*  Linear search.
     */
    for (i = 0; i < gic->gic_n; i++)
      if (gic->gic_id[i] == *id_inout) {
        *off_out = i;
        return 0;
      }
    return gic->gic_eof ? GRAPHD_ERR_NO : GRAPHD_ERR_MORE;
  }

  if (pdb_iterator_forward(pdb, it)) {
    /*  Find *id_inout or the next larger id.
     */
    if (*id_inout > gic->gic_id[gic->gic_n - 1])
      return gic->gic_eof ? GRAPHD_ERR_NO : GRAPHD_ERR_MORE;

    if (*id_inout <= gic->gic_id[0]) {
      *off_out = 0;
      *id_inout = gic->gic_id[0];

      return 0;
    }

    /*  Find the same or larger.
     */

    {
      size_t end = gic->gic_n;
      size_t start = 0;
      size_t off;
      unsigned long long endval = *id_inout;
      pdb_id cache_id;

      for (;;) {
        off = start + (end - start) / 2;
        cache_id = gic->gic_id[off];

        if (cache_id < *id_inout)
          start = ++off;

        else if (cache_id > *id_inout) {
          end = off;
          endval = cache_id;
        } else
          break;

        if (start >= end) {
          cache_id = endval;
          break;
        }
      }
      *id_inout = cache_id;
      *off_out = off;
    }
    return 0;
  } else {
    /*  Backwards -- find *id_inout or the next smaller id.
     *  Higher gic->gic_n means lower id values.
     */
    if (*id_inout < gic->gic_id[gic->gic_n - 1])
      return gic->gic_eof ? GRAPHD_ERR_NO : GRAPHD_ERR_MORE;

    else if (*id_inout >= gic->gic_id[0]) {
      *off_out = 0;
      *id_inout = gic->gic_id[0];

      return 0;
    }

    /*  Find the same or smaller.
     */

    {
      size_t end = gic->gic_n;
      size_t start = 0;
      size_t off;
      unsigned long long endval = *id_inout;
      pdb_id cache_id;

      for (;;) {
        off = start + (end - start) / 2;
        cache_id = gic->gic_id[off];

        if (cache_id > *id_inout) {
          start = ++off;
        } else if (cache_id < *id_inout) {
          end = off;
          endval = cache_id;
        } else
          break;

        if (start >= end) {
          cache_id = endval;
          break;
        }
      }
      *id_inout = cache_id;
      *off_out = off;
    }
  }
  return 0;
}

/**
 * @brief Is an ID in the cache?
 *
 * @param pdb	database we're doing that for
 * @param it	iterator we're part of
 * @param gic	cache descriptor
 * @param id	ID to check
 *
 * @return 0 if the ID is in the cache.
 * @return GRAPHD_ERR_NO if the ID would be in the cache, but isn't.
 * @return GRAPHD_ERR_MORE if the ID or a nearby ID *may* be part of
 *	the result set, but the cache doesn't know whether it is.
 */
int graphd_iterator_cache_check(pdb_handle *pdb, pdb_iterator *it,
                                graphd_iterator_cache *gic, pdb_id id) {
  pdb_id id_found = id;
  size_t off = 0;
  int err;

  err = graphd_iterator_cache_search(pdb, it, gic, &id_found, &off);
  if (!pdb_iterator_sorted(pdb, it)) return err;

  if (err == 0)
    if (off < gic->gic_n) return id == id_found ? 0 : GRAPHD_ERR_NO;

  return gic->gic_eof ? GRAPHD_ERR_NO : GRAPHD_ERR_MORE;
}

/**
 * @brief Read an ID from the cache.
 *
 * @param gic		cache descriptor
 * @param offset	Get the ID for this offset.
 * @param id_out	Assign the ID to this location.
 * @param budget_inout	Subtract the ID's cost from here.
 *
 * @return 0 if the ID is in the cache.
 * @return GRAPHD_ERR_NO if the ID would be in the cache, but isn't.
 * @return GRAPHD_ERR_MORE if the ID or a nearby ID *may* be part of
 *	the result set, but the cache doesn't know whether it is.
 */
int graphd_iterator_cache_index(graphd_iterator_cache *gic, size_t offset,
                                pdb_id *id_out, pdb_budget *budget_inout) {
  if (offset >= gic->gic_n)
    return gic->gic_eof ? GRAPHD_ERR_NO : GRAPHD_ERR_MORE;

  *id_out = gic->gic_id[offset];
  *budget_inout -= gic->gic_cost;

  gic->gic_use_total += gic->gic_cost - 1;
  return 0;
}
/**
 * @brief We read an id.  How much did that cost?
 *
 * @param gic	cache descriptor
 * @return the cost of the ID
 */
pdb_budget graphd_iterator_cache_cost(graphd_iterator_cache *gic) {
  gic->gic_use_total += gic->gic_cost;
  return gic->gic_cost;
}

int graphd_iterator_cache_freeze(graphd_handle *g, graphd_iterator_cache *gic,
                                 cm_buffer *buf) {
  int err;
  char sb[GRAPHD_ITERATOR_RESOURCE_STAMP_SIZE];

  err = cm_buffer_sprintf(buf, "%zu:%lld:%lld:%lld:%d:", gic->gic_n,
                          gic->gic_cost, gic->gic_cost_total,
                          gic->gic_use_total, (int)gic->gic_eof);
  if (err != 0) return err;

  if (gic->gic_n <= GRAPHD_ITERATOR_CACHE_INLINE_N) {
    size_t i;
    char const *sep = "";

    /* Just inline them.
     */
    for (i = 0; i < gic->gic_n; i++) {
      err = cm_buffer_sprintf(buf, "%s%llu", sep,
                              (unsigned long long)gic->gic_id[i]);
      if (err != 0) return err;
      sep = ",";
    }
    return 0;
  }
  err = graphd_iterator_resource_store(g, &gic->gic_storable, sb, sizeof sb);
  if (err != 0) {
    cl_log_errno(g->g_cl, CL_LEVEL_FAIL, "graphd_iterator_cache_freeze", err,
                 "can't freeze %zu bytes?",
                 (size_t)gic->gic_n * sizeof(*gic->gic_id));
    return err;
  }

  return cm_buffer_sprintf(buf, "@%s", sb);
}

/*  Thaw a cache.
 *
 *  On return, if *gic_out is NULL, there was an error, and
 *  the returned error code indicates the error.
 *
 *  If *gic_out is non-NULL, there either was no error, and
 *  the cache is valid - return value 0 - or there was a recoverable
 *  error, and the cache has been restored empty.
 */
int graphd_iterator_cache_thaw(graphd_handle *g, char const **s_ptr,
                               char const *e, cl_loglevel loglevel,
                               graphd_iterator_cache **gic_out) {
  graphd_iterator_cache *gic;
  int err;
  char const *r = *s_ptr;
  cl_handle *cl = g->g_cl;
  int my_eof;
  char const *s0 = *s_ptr;
  size_t gic_n;
  pdb_budget gic_cost, gic_cost_total, gic_use_total;

  *gic_out = NULL;

  if (r >= e) {
    *gic_out = graphd_iterator_cache_create(g, 0);
    return *gic_out == NULL ? ENOMEM : GRAPHD_ERR_NO;
  }

  err = pdb_iterator_util_thaw(
      g->g_pdb, &r, e, "%zu:%{budget}:%{budget}:%{budget}:%d:", &gic_n,
      &gic_cost, &gic_cost_total, &gic_use_total, &my_eof);
  if (err != 0) return err;

  if (r >= e || *r != '@') {
    size_t i;

    /*  Inlined values.
     */
    gic = *gic_out = graphd_iterator_cache_create(g, gic_n);
    if (gic == NULL) return ENOMEM;

    gic->gic_cost = gic_cost;
    gic->gic_cost_total = gic_cost_total;
    gic->gic_use_total = gic_use_total;
    gic->gic_eof = !!my_eof;

    for (i = 0; i < gic->gic_n; i++) {
      pdb_id id;

      if (r < e && *r == ',') r++;

      err = pdb_iterator_util_thaw(g->g_pdb, &r, e, "%{id}", &id);
      if (err != 0) goto err;

      err = graphd_iterator_cache_add(gic, id, gic->gic_cost);
      if (err != 0) goto err;
    }
    *s_ptr = r;
  } else /* Cache may be in iterator resource store. */
  {
    *s_ptr = ++r;

    gic = *gic_out =
        graphd_iterator_resource_thaw(g, s_ptr, e, &gic_storable_type);
    if (gic == NULL) {
      cl_log(cl, loglevel,
             "graphd_iterator_cache_thaw: MISS can't "
             "find cache from \"%.*s\"",
             (int)(e - s0), s0);

      /* Valid parse, but not found  */
      *s_ptr = r;

      gic = *gic_out = graphd_iterator_cache_create(g, gic_n);
      if (gic == NULL) return ENOMEM;

      gic->gic_cost = gic_cost;
      gic->gic_cost_total = gic_cost_total;
      gic->gic_use_total = gic_use_total;
      gic->gic_eof = !!my_eof;

      return GRAPHD_ERR_NO;
    }

    cl_log(cl, loglevel,
           "graphd_iterator_cache_thaw: "
           "HIT recovered \"%.*s\" (%p)",
           (int)(e - s0), s0, (void *)gic);
  }
  return 0;

err:
  graphd_iterator_cache_destroy(*gic_out);
  *gic_out = NULL;

  return err;
}

/*  We already have an old cache state.  See if we can
 *  add something to it.
 *
 *  Return GRAPHD_ERR_ALREADY if everything's okay but we didn't add to
 *  the cache; 0 if we did add to the cache; GRAPHD_ERR_NO if the
 *  state had aged out of the iterator resource cache.
 *
 *  If the caller keeps a producer for the cache, the producer
 *  position becomes invalid with a cache rethaw that returns
 *  anything other than GRAPHD_ERR_NO.  The cache producer needs to
 *  position on the cache end before producing anything new.
 */
int graphd_iterator_cache_rethaw(graphd_handle *g, char const **s_ptr,
                                 char const *e, cl_loglevel loglevel,
                                 graphd_iterator_cache **gic) {
  int err;
  char const *r = *s_ptr;
  cl_handle *cl = g->g_cl;
  int my_eof;
  pdb_budget cost, cost_total, use_total;
  size_t n;

  if (r >= e) return GRAPHD_ERR_NO;

  err = pdb_iterator_util_thaw(g->g_pdb, &r, e,
                               "%zu:%{budget}:%{budget}:%{budget}:%d:", &n,
                               &cost, &cost_total, &use_total, &my_eof);
  if (err != 0) return err;

  if (r >= e || *r != '@') {
    size_t i;
    bool any = false;

    /* Cache contents are inlined.
     */
    for (i = 0; i < n; i++) {
      pdb_id id;

      if (r < e && *r == ',') r++;

      if (i < (*gic)->gic_n)
        while (isascii(*r) && isdigit(*r)) r++;
      else {
        any = true;

        err = pdb_iterator_util_thaw(g->g_pdb, &r, e, "%{id}", &id);
        if (err != 0) {
          cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                       "expected id, got %.*s", (int)(e - r), r);
          return err;
        }
        err = graphd_iterator_cache_add(*gic, id, cost);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_cache_add", err,
                       "id=%llx", (unsigned long long)id);
          return err;
        }
      }
    }

    (*gic)->gic_eof |= my_eof;
    *s_ptr = r;
    if (!any) return GRAPHD_ERR_ALREADY;

  } else /* cache may or may not be in store. */
  {
    graphd_iterator_cache *ogic;

    r++;
    ogic = graphd_iterator_resource_thaw(g, &r, e, &gic_storable_type);
    *s_ptr = r;

    /* If ogic is non-null, we now hold a link to it
     *  (which we must free).
     */
    if (ogic == NULL) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_cache_rethaw: "
             "couldn't thaw cache itself.");
      return GRAPHD_ERR_NO;
    }

    if (ogic == *gic || ogic->gic_n <= (*gic)->gic_n) {
      /*  Everything that guy knows we already know.
       */
      (*gic)->gic_eof |= !!my_eof;
      graphd_iterator_cache_destroy(ogic);

      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_cache_rethaw: "
             "nothing new in the cache.");

      return GRAPHD_ERR_ALREADY;
    }

    /*  We're learning something new.  Go with the
     *  stored cache.
     */
    graphd_iterator_cache_destroy(*gic);
    *gic = ogic;
  }
  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_cache_rethaw: new cache %p, %zu elements",
         (void *)*gic, (*gic)->gic_n);
  return 0;
}
