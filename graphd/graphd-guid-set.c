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
#include "graphd/graphd-hash.h"

#include <limits.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

void graphd_guid_set_initialize(graphd_guid_set *gs) {
  gs->gs_next = NULL;
  gs->gs_guid = gs->gs_buf;
  gs->gs_n = gs->gs_m = 0;
  gs->gs_null = false;
}

bool graphd_guid_set_contains_null(graphd_guid_set const *gs) {
  return gs->gs_null || gs->gs_n == 0;
}

void graphd_guid_set_move(graphd_guid_set *dst, graphd_guid_set *src) {
  *dst = *src;
  if (src->gs_guid == src->gs_buf) dst->gs_guid = dst->gs_buf;
}

/*  Return whether this guid is in this set.
 *
 *  Unlike graphd_guid_set_find(), this function interprets a
 *  zero-length set as matching NULL.
 */
bool graphd_guid_set_match(graphd_guid_set const *gs, graph_guid const *guid) {
  if (guid == NULL) return graphd_guid_set_contains_null(gs);

  if (gs->gs_n == 0) return false;
  return graphd_guid_set_find(gs, guid) < gs->gs_n;
}

/*  Return the index if found, n if not.  NULL lives at 0, if any.
 */
size_t graphd_guid_set_find(graphd_guid_set const *gs, graph_guid const *guid) {
  size_t i;

  if (guid == NULL) return graphd_guid_set_contains_null(gs) ? 0 : gs->gs_n;

  for (i = 0; i < gs->gs_n; i++)
    if (GRAPH_GUID_EQ(gs->gs_guid[i], *guid)) break;
  return i;
}

/* Returns true if found, false if not
 *
 * To delete the "null" from a set, use guid=NULL.
 */
bool graphd_guid_set_delete(graphd_guid_set *gs, graph_guid const *guid) {
  size_t i;

  if (guid == NULL) {
    if (gs->gs_null) {
      gs->gs_null = false;
      return true;
    }
    return false;
  }

  if ((i = graphd_guid_set_find(gs, guid)) >= gs->gs_n) return false;

  if (i < gs->gs_n - 1)
    memmove(gs->gs_guid + i, gs->gs_guid + i + 1,
            (gs->gs_n - (i + 1)) * sizeof(*guid));
  gs->gs_n--;
  return true;
}

/**
 * @brief Add a GUID to a guid constraint.
 *
 * @param greq  	request we're working for
 * @param gs 	constraint to add to.
 * @param guid		NULL or GUID to add
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_guid_set_add(graphd_request *greq, graphd_guid_set *gs,
                        graph_guid const *guid) {
  graph_guid *tmp;
  cm_handle *cm = greq->greq_req.req_cm;
  cl_handle *cl = graphd_request_cl(greq);
  size_t i;

  cl_assert(cl, gs != NULL);

  if (guid == NULL) {
    cl_log(cl, CL_LEVEL_VERBOSE, "graphd_guid_set_add: null to %p", (void *)gs);
    gs->gs_null = true;
    return 0;
  }

  /*  Most common case: a single GUID.  It lives either
   *  in preallocated storage or in a built-in buffer.
   *
   *  Note that adding a GUID to an empty set yields just
   *  that GUID, even though the empty set is treated as
   *  containing NULL.  Add NULL explicitly if you want
   *  to keep it!
   */
  if (gs->gs_n == 0) {
    if (gs->gs_m == 0) gs->gs_guid = gs->gs_buf;

    gs->gs_guid[0] = *guid;
    gs->gs_n = 1;

    return 0;
  }

  /*  If the GUID already exists in the list, don't store it again.
   */
  if ((i = graphd_guid_set_find(gs, guid)) < gs->gs_n) return 0;

  /*  Grow the dynamic array out of the buffer, if needed.
   */
  if (gs->gs_guid == gs->gs_buf) {
    tmp = cm_malloc(cm, (i + 1) * sizeof(*tmp));
    if (tmp == NULL) {
      cl_log(cl, CL_LEVEL_ERROR,
             "graphd_guid_set_add: "
             "failed to allocate %lu bytes",
             (unsigned long)((i + 1) * sizeof(*tmp)));
      return ENOMEM;
    }
    memcpy(tmp, gs->gs_guid, i * sizeof(*tmp));
    gs->gs_m = i + 1;
    gs->gs_guid = tmp;
  } else {
    if (gs->gs_n >= gs->gs_m) {
      tmp = cm_realloc(cm, gs->gs_guid, (gs->gs_m + 8) * sizeof(*tmp));
      if (tmp == NULL) {
        cl_log(cl, CL_LEVEL_ERROR,
               "graphd_guid_set_add: "
               "failed to allocate %zu bytes",
               (i + 1) * sizeof(*tmp));
        return ENOMEM;
      }
      gs->gs_m += 8;
      gs->gs_guid = tmp;
    }
  }
  gs->gs_guid[gs->gs_n++] = *guid;
  return 0;
}

/**
 * @brief Add the generations of a GUID to a list
 *
 *   The GUIDs aren't sorted or uniq'ed at this point;
 *   that's the caller's job.
 *   It's also the caller's job to make sure that the
 *   gen_i..gen_i+gen_n range is valid.
 *
 * @param greq		request handle
 * @param guid		guid to expand
 * @param gen_i		first generation to use
 * @param gen_n		number of generations after gen_i
 * @param gs		gs to add them to.
 *
 * @return 0 on success, a nonzero error code on (system) error.
 */
int graphd_guid_set_add_generations(graphd_request *greq,
                                    graph_guid const *guid, unsigned long gen_i,
                                    unsigned long gen_n, graphd_guid_set *gs) {
  pdb_handle *pdb = graphd_request_graphd(greq)->g_pdb;
  cl_handle *cl = graphd_request_cl(greq);

  cl_assert(cl, gs != NULL);

  if (guid == NULL) {
    cl_log(cl, CL_LEVEL_VERBOSE, "graphd_guid_set_add: null to %p", (void *)gs);
    gs->gs_null = true;
    return 0;
  }

  cl_assert(cl, guid != NULL);
  for (; gen_n > 0; gen_i++, gen_n--) {
    graph_guid g;
    int err = 0;

    err = pdb_generation_nth(pdb, greq->greq_asof, guid, false /* is-newest */,
                             gen_i, NULL, &g);
    if (err != 0) {
      char buf[GRAPH_GUID_SIZE];
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_generation_nth", err,
                   "GUID=%s, generation=%lu",
                   graph_guid_to_string(guid, buf, sizeof buf),
                   (unsigned long)gen_i);
      return err;
    }

    {
      char buf[200];
      cl_log(cl, CL_LEVEL_VERBOSE, "graphd_guid_set_add: add %s",
             graph_guid_to_string(&g, buf, sizeof buf));
    }
    err = graphd_guid_set_add(greq, gs, &g);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_guid_set_add", err, "i=%lu",
                   gen_i);
      return err;
    }
  }
  return 0;
}

/**
 *  @brief Go from guid ~= GUID / guidset to guid = GUID / guidset,
 *
 *  if gs == &con->con_guid.*, do this under control of
 *  con's generational constraint.  (The other ones are not
 *  affected by con's generational settings.)
 */
int graphd_guid_set_convert_generations(graphd_request *greq,
                                        graphd_constraint *con, bool is_guid,
                                        graphd_guid_set *gs) {
  int err = 0;
  graphd_handle *graphd = graphd_request_graphd(greq);
  graph_guid const *r;
  graph_guid *w;
  graph_guid *new_g = NULL;
  size_t new_m = 0, new_n = 0, i;
  cm_handle *cm = greq->greq_req.req_cm;
  cl_handle *cl = graphd_request_cl(greq);

  if (gs->gs_n == 0) return 0;

  /*  Most common case: we want the newest,
   *  or some other single generation.
   */
  if (is_guid && !con->con_oldest.gencon_valid &&
      (!con->con_newest.gencon_valid ||
       (con->con_newest.gencon_min == con->con_newest.gencon_max))) {
    graph_guid *w;

    /*  Translate the guids to their relevant instances.
     */
    for (i = 0, r = w = gs->gs_guid; i < gs->gs_n; i++, r++) {
      if (GRAPH_GUID_IS_NULL(*r)) {
        /* null -> null */
        *w++ = *r;
        continue;
      }

      err = pdb_generation_nth(graphd->g_pdb, greq->greq_asof, r,
                               true /* is-newest */, con->con_newest.gencon_min,
                               NULL, w);
      if (err == 0)
        w++;
      else if (err != GRAPHD_ERR_NO) {
        char buf[GRAPH_GUID_SIZE];
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_generation_nth", err,
                     "failed to get newest generation of %s",
                     graph_guid_to_string(r, buf, sizeof buf));
        return err;
      }
    }
    if (gs->gs_n == 0) {
      con->con_false = true;
      con->con_error =
          "SEMANTICS no GUIDs in the "
          "request range of versions";
      cl_log(cl, CL_LEVEL_DEBUG, "FALSE: [%s:%d] no GUIDs is requested range",
             __FILE__, __LINE__);
    }
    return 0;
  }

  /*  Partially constrained case.  Each GUID may evaluate
   *  to zero or more GUIDs.
   */
  for (i = 0, r = w = gs->gs_guid; i < gs->gs_n; i++, r++) {
    graph_guid guid;
    pdb_id n, last;
    long long gen_i;
    long long gen_min, gen_max, gen_size;
    size_t need;

    err = pdb_generation_last_n(graphd->g_pdb, greq->greq_asof, r, &last, &n);

    if (err == GRAPHD_ERR_NO) continue;

    if (err != 0) return err;

    /*  If there's no generation table entry,
     *  there's just one generation - the one
     *  we're holding.
     */
    if (n == 0) n = 1;

    gen_min = 0;
    gen_max = ULONG_MAX;

    if (is_guid) {
      if (con->con_newest.gencon_valid) {
        gen_max = (con->con_newest.gencon_min > n - 1
                       ? -1
                       : n - (1 + con->con_newest.gencon_min));

        gen_min = (con->con_newest.gencon_max > n - 1
                       ? -1
                       : n - (1 + con->con_newest.gencon_max));
      }
      if (con->con_oldest.gencon_valid) {
        if (gen_min < con->con_oldest.gencon_min)
          gen_min = con->con_oldest.gencon_min;

        if (gen_max > con->con_oldest.gencon_max)
          gen_max = con->con_oldest.gencon_max;
      }
    }
    if (gen_min < 0) gen_min = 0;
    if (gen_max > n - 1) gen_max = n - 1;
    if (gen_max < gen_min) continue;
    gen_size = 1 + (gen_max - gen_min);

    /*  How many do we already have?
     */
    new_n = w - (new_g ? new_g : gs->gs_guid);

    /*  How many will we need with the expansions
     *  of this one, plus the rest on the list?
     */
    need = new_n + gen_size + (gs->gs_n - (i + 1));
    if (need > (new_g ? new_m : gs->gs_n)) {
      void *tmp;

      tmp = cm_realloc(cm, new_g, need * sizeof(*new_g));
      if (tmp == NULL) {
        if (new_g != NULL) cm_free(cm, new_g);
        cl_log(cl, CL_LEVEL_ERROR,
               "graphd_guid_set_"
               "convert_generations: "
               "failed to allocate %llu "
               "bytes for %llu generations",
               (unsigned long long)(need * sizeof(*new_g)),
               (unsigned long long)need);
        return ENOMEM;
      }
      if (new_g == NULL && new_n > 0)
        memcpy(tmp, gs->gs_guid, new_n * sizeof(*w));
      new_g = tmp;
      w = new_g + new_n;
      new_m = need;
    }

    /* In case we'll overwrite *r, copy *r to guid. */
    guid = *r;
    for (gen_i = gen_min; gen_i <= gen_max; gen_i++) {
      /*  If there's just one generation,
       *  and we like it, there is no
       *  generation table entry; our input
       *  is simply our output.
       */
      if (gen_i == 0 && n == 1) {
        *w++ = guid;
        continue;
      }

      err = pdb_generation_nth(graphd->g_pdb, greq->greq_asof, &guid,
                               false /* is-oldest */, gen_i, NULL, w);
      if (err == 0)
        w++;
      else {
        char buf[GRAPH_GUID_SIZE];
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_constraint_convert_"
               "gs_generations: "
               "failed to get generation "
               "#%llu of %s: %s",
               (unsigned long long)gen_i,
               graph_guid_to_string(&guid, buf, sizeof buf),
               graphd_strerror(err));
      }
    }
  }
  if (new_g == NULL)
    gs->gs_m = gs->gs_n = w - gs->gs_guid;
  else {
    if (gs->gs_guid != gs->gs_buf) cm_free(cm, gs->gs_guid);

    gs->gs_guid = new_g;
    gs->gs_m = gs->gs_n = w - new_g;
  }

  if (gs->gs_n == 0 && !gs->gs_null) {
    con->con_false = true;
    con->con_error =
        "SEMANTICS no GUIDs in the "
        "request range of versions";
    cl_log(cl, CL_LEVEL_DEBUG, "FALSE: [%s:%d] no GUIDs is requested range",
           __FILE__, __LINE__);
  }
  return 0;
}

/**
 *  @brief Replace generational identifiers with their root ancestor
 *
 *    If we do this, we can intersect two match groups by intersecting
 *    their IDs.
 *
 *    For example, if we have a versioning chain 1 <- 2 <- 3,
 *    then
 *		(GUID ~= 2  GUID ~= 3)
 *    normalizes to
 *		(GUID ~= 1  GUID ~= 1)
 *    and then to a single
 *		(GUID ~= 1)
 *
 *  @param greq	the request we're doing this for
 *  @param gs	the set whose IDs the request wants normalized
 *
 *  @return 0 on success, a nonzero error code on error.
 */
int graphd_guid_set_normalize_match(graphd_request *greq, graphd_guid_set *gs) {
  graphd_handle *graphd = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  int err = 0;
  size_t i;

  if (gs->gs_n == 0) return 0;

  for (i = 0; i < gs->gs_n; i++) {
    if (GRAPH_GUID_IS_NULL(gs->gs_guid[i])) continue;

    err = pdb_generation_nth(graphd->g_pdb,
                             /* asof */ NULL,
                             /* in 	*/ gs->gs_guid + i,
                             /* oldest */ false,
                             /* off */ 0,
                             /* id_out */ NULL,
                             /* guid_out */ gs->gs_guid + i);

    if (err != 0 && err != GRAPHD_ERR_NO) {
      char buf[GRAPH_GUID_SIZE];
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_generation_nth", err,
                   "failed to get oldest generation of %s",
                   graph_guid_to_string(gs->gs_guid + i, buf, sizeof buf));
      return err;
    }
  }
  return 0;
}

/**
 * @brief Are these two guid sets equal?
 *
 *  False negatives are okay.  In particular,
 *  GUID sets with the same GUID in the wrong order
 *  are misclassified as unequal.
 *
 * @param cl 	Log through here
 * @param a 	guid set
 * @param b 	another guid set
 *
 * @return true if they're equal, false otherwise.
 */
bool graphd_guid_set_equal(cl_handle *cl, graphd_guid_set const *a,
                           graphd_guid_set const *b) {
  size_t i;

  cl_assert(cl, a != NULL);
  cl_assert(cl, b != NULL);

  /*  One contains null, the other one doesn't?
   */
  if ((a->gs_null || a->gs_n == 0) != (b->gs_null || b->gs_n == 0))
    return false;
  do {
    if (a->gs_n != b->gs_n) return false;

    for (i = 0; i < a->gs_n; i++)
      if (!GRAPH_GUID_EQ(a->gs_guid[i], b->gs_guid[i])) return false;

    if (((a = a->gs_next) == NULL) != ((b = b->gs_next) == NULL)) return false;

  } while (a != NULL);

  if (a->gs_null != b->gs_null) return false;

  return true;
}

/**
 * @brief Hash a guid set
 *
 * @param cl 		Log through here
 * @param gs 		guid set
 * @param hash_inout 	hash accumulator
 */
void graphd_guid_set_hash(cl_handle *const cl, graphd_guid_set const *gs,
                          unsigned long *const hash_inout) {
  size_t i;

  cl_assert(cl, gs != NULL);
  cl_assert(cl, hash_inout != NULL);

  do {
    for (i = 0; i < gs->gs_n; i++)
      GRAPHD_HASH_GUID(*hash_inout, gs->gs_guid[i]);

  } while ((gs = gs->gs_next) != NULL);

  GRAPHD_HASH_BIT(*hash_inout, gs->gs_null);
}

/**
 *  @brief Intersection of two GUID constraint sets.
 *
 *  @param greq		Request for which all this happens
 *  @param con 		Containing constraint (for con_false marking)
 *  @param postpone	Postpone intersect if needed
 *  @param accu 	set to merge into
 *  @param in   	incoming gs to merge
 *
 *  @return 0 on success, otherwise a nonzero error code.
 */
int graphd_guid_set_intersect(graphd_request *greq, graphd_constraint *con,
                              bool postpone, graphd_guid_set *accu,
                              graphd_guid_set *in) {
  cl_handle *cl = graphd_request_cl(greq);
  size_t i, f;
  graph_guid *w;
  graph_guid const *r;
  int res;

  cl_assert(cl, accu != NULL);
  cl_assert(cl, in != NULL);

  if (in->gs_n == 0) {
    if (accu->gs_n > 0) {
      if (accu->gs_null)

        /* The filter was {null}.  The <accu> contains
        *  {null}, so {null} survives as a result.
         */
        accu->gs_n = 0;
      else {
        /* The filter was {null}.  The <accu> does not
         *  contain {null}, therefore the result set
         *  is empty.  (It doesn't even contain null.)
         */
        con->con_false = true;
        accu->gs_n = 0;
        cl_log(cl, CL_LEVEL_DEBUG,
               "FALSE [%s:%d]  intersect non-null "
               "with null",
               __FILE__, __LINE__);
      }
    }
    return 0;
  }

  if (accu->gs_n == 0) {
    cl_assert(cl, in->gs_n != 0);

    /* The <accu> set is {null}.  If and only if the filter
    *  contains {null}, <accu> stays {null}; otherwise the
    *  result set is empty.
     */
    if (!graphd_guid_set_contains_null(in)) {
      con->con_false = true;
      cl_log(cl, CL_LEVEL_DEBUG,
             "FALSE [%s:%d]  intersect null "
             "with non-null",
             __FILE__, __LINE__);
    }
    return 0;
  }

  /*  If postpone is set,
  .*  we can't compute the intersection during parse time;
   *  we have to wait until execution time.
   */
  if (postpone) {
    graphd_guid_set *gs;

    gs = cm_malloc(greq->greq_req.req_cm, sizeof *gs);
    if (gs == NULL) return errno ? errno : ENOMEM;

    graphd_guid_set_initialize(gs);
    graphd_guid_set_move(gs, in);

    gs->gs_next = accu->gs_next;
    accu->gs_next = gs;

    return 0;
  }

  if (in->gs_n > 1)
    qsort(in->gs_guid, in->gs_n, sizeof(*in->gs_guid), graph_guid_compare);

  if (accu->gs_n > 1)
    qsort(accu->gs_guid, accu->gs_n, sizeof(*accu->gs_guid),
          graph_guid_compare);

  i = 0, f = 0;
  r = w = accu->gs_guid;

  /* Intersect
   */
  while (i < accu->gs_n && f < in->gs_n) {
    res = graph_guid_compare(r, in->gs_guid + f);
    while (res > 0) {
      f++;
      if (f >= in->gs_n) goto done;

      res = graph_guid_compare(r, in->gs_guid + f);
    }
    if (res == 0) *w++ = *r;
    r++;
    i++;
  }

done:
  accu->gs_n = w - accu->gs_guid;
  accu->gs_null &= graphd_guid_set_contains_null(in);

  if (accu->gs_n == 0 && !accu->gs_null) {
    cl_log(cl, CL_LEVEL_DEBUG,
           "FALSE [%s:%d] nothing left after proper intersect", __FILE__,
           __LINE__);

    con->con_false = true;
  }
  return 0;
}

/**
 *  @brief Filter a GUID set by lineage.
 *
 *   Compute the result of
 *		guid=(1 2 3) guid~=(4 5)
 *
 *   All those GUIDs in <accu> whose root ancestors
 *   are in <fil> are allowed to stay.
 *
 *  @param greq	Request for which all this happens
 *  @param con 	Containing constraint (for con_false marking)
 *  @param accu set to filter
 *  @param fil  root ancestors of allowed IDs.
 *
 *  @return 0 on success, a nonzero error code on error.
 */
int graphd_guid_set_filter_match(graphd_request *greq, graphd_constraint *con,
                                 graphd_guid_set *accu, graphd_guid_set *fil) {
  graphd_handle *graphd = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  size_t i;
  graph_guid *w;

  cl_assert(cl, accu != NULL);
  cl_assert(cl, fil != NULL);

  cl_log(cl, CL_LEVEL_VERBOSE, "gs_intersect: %zd vs %zd", accu->gs_n,
         fil->gs_n);

  if (accu->gs_n == 0) {
    cl_log(cl, CL_LEVEL_VERBOSE, "graphd_guid_set_filter_match: null %p",
           (void *)accu);
    accu->gs_null = true;
  }

  /*  NULL stays if it's allowed in the filter.
   */
  if (accu->gs_null) {
    if (fil->gs_n > 0 && !fil->gs_null) {
      cl_log(cl, CL_LEVEL_DEBUG,
             "FALSE [%s:%d] =/~ against "
             "a null",
             __FILE__, __LINE__);
      con->con_false = true;
    }
    return 0;
  }

  /*  NULL is the only thing that remains in the
   *  accumulator.
   */
  if (fil->gs_n == 0) {
    if (accu->gs_null) {
      accu->gs_n = 0;
      return 0;
    }

    /* The filter was {null}.  The <accu> does not contain {null},
     * therefore the result set is empty.  (It doesn't even contain
     * null.)
     */
    con->con_false = true;
    cl_log(cl, CL_LEVEL_DEBUG,
           "FALSE [%s:%d] =/~ null against "
           "a non-null",
           __FILE__, __LINE__);

    return 0;
  }
  if (accu->gs_n == 0) {
    cl_assert(cl, fil->gs_n != 0);

    /* The <accu> set is {null}.  If and only if the filter
    *  contains {null}, <accu> stays {null}; otherwise the
    *  result set is empty.
     */
    return graphd_guid_set_contains_null(fil);
  }

  for (i = 0, w = accu->gs_guid; i < accu->gs_n; i++) {
    graph_guid guid;
    int err;

    if (GRAPH_GUID_IS_NULL(accu->gs_guid[i])) {
      *w++ = accu->gs_guid[i];
      continue;
    }

    /*  Normalize accu->gs[i]
     */
    err = pdb_generation_nth(graphd->g_pdb,
                             /* asof */ NULL,
                             /* in 	*/ accu->gs_guid + i,
                             /* oldest */ false,
                             /* off */ 0,
                             /* id_out */ NULL,
                             /* guid_out */ &guid);

    if (err == GRAPHD_ERR_NO) {
      guid = accu->gs_guid[i];
      err = 0;
    }
    if (err != 0) {
      char buf[GRAPH_GUID_SIZE];
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_generation_nth", err,
                   "failed to get oldest generation of %s",
                   graph_guid_to_string(accu->gs_guid + i, buf, sizeof buf));
      return err;
    }
    if (graphd_guid_set_find(fil, &guid) < fil->gs_n) *w++ = accu->gs_guid[i];
  }
  accu->gs_n = w - accu->gs_guid;
  if (fil->gs_n == 0) {
    con->con_false = true;
    cl_log(cl, CL_LEVEL_DEBUG, "FALSE [%s:%d] =/~ no overlap", __FILE__,
           __LINE__);
  }
  return 0;
}

/**
 *  @brief Subtract a set from another set
 *
 *  @param greq	Request for which all this happens
 *  @param con 	Containing constraint (for con_false marking)
 *  @param accu constraint to remove from
 *  @param in   incoming gs to remove
 *
 *  @return false if a previously non-empty list has
 * 	been reduced to an empty one, true otherwise.
 */
bool graphd_guid_set_subtract(graphd_request *greq, graphd_guid_set *accu,
                              graphd_guid_set const *in) {
  cl_handle *cl = graphd_request_cl(greq);
  size_t i;

  cl_assert(cl, accu != NULL);
  cl_assert(cl, in != NULL);

  if (accu->gs_n == 0) accu->gs_null = true;

  if (in->gs_n == 0) {
    /* <in> is {null}.  If <accu> contains null
     * (or is null), and accu has nothing left
     * after the subtraction, the result set is empty.
     */
    accu->gs_null = false;
    return accu->gs_n > 0;
  }
  if (accu->gs_n == 0) {
    /* accu is {null}.  If <in> contains null,
     * the result set is empty (and the call
     * returns false.)
     */
    return !in->gs_null;
  }

  /* Remove guids from <accu> that are in <in>.
   * Return whether the result set is empty after that.
   */
  if (in->gs_null) accu->gs_null = false;

  for (i = 0; i < in->gs_n; i++)
    (void)graphd_guid_set_delete(accu, in->gs_guid + i);

  return accu->gs_n > 0 || accu->gs_null;
}

/**
 *  @brief "OR" of two guid sets
 *
 *  "in" is burned after the call, and will be freed as
 *   part of the request heap.
 *
 *  @param greq	Request for which all this happens
 *  @param con 	Containing constraint (for con_false marking)
 *  @param accu constraint to merge into
 *  @param in   incoming gs to merge
 *
 *  @return 0 on success, otherwise a nonzero error code.
 */
int graphd_guid_set_union(graphd_request *greq, graphd_guid_set *accu,
                          graphd_guid_set *in) {
  int err = 0;
  cl_handle *cl = graphd_request_cl(greq);
  size_t i;

  cl_assert(cl, accu != NULL);
  cl_assert(cl, in != NULL);

  accu->gs_null |= (in->gs_null || in->gs_n == 0);

  if (in->gs_n == 0) return 0;

  if (accu->gs_n == 0) {
    cl_assert(cl, in->gs_n != 0);

    /*  <accu> is {null}.  Move <in> into <accu> and
     *  add {null} to that.
     */
    graphd_guid_set_move(accu, in);
    accu->gs_null = true;

    return 0;
  }

  /* Add <in>'s GUIDs to <accu>.  (The duplicate detection
   * is already done - clumsily - in constraint_add).
   */
  for (i = 0; i < in->gs_n; i++) {
    err = graphd_guid_set_add(greq, accu, in->gs_guid + i);
    if (err != 0) return err;
  }
  return 0;
}

/**
 *  @brief "OR" of two guid sets
 *
 *  "in" is burned after the call, and will be freed as
 *   part of the request heap.
 *
 *  @param greq	Request for which all this happens
 *  @param con 	Containing constraint (for con_false marking)
 *  @param accu constraint to merge into
 *  @param in   incoming gs to merge
 *
 *  @return 0 on success, otherwise a nonzero error code.
 */
void graphd_guid_set_dump(cl_handle *cl, graphd_guid_set const *gs) {
  size_t i;
  char buf[GRAPH_GUID_SIZE];

  if (gs->gs_null) cl_log(cl, CL_LEVEL_VERBOSE, " [flag] null");

  for (i = 0; i < gs->gs_n; i++)
    cl_log(cl, CL_LEVEL_VERBOSE, " [%zu] %s", i,
           graph_guid_to_string(gs->gs_guid + i, buf, sizeof buf));
}
