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
#include <string.h>

/*  How many values will we cache, at most?
 */
#define GRAPHD_PREFIX_CACHE_MAX (1024 * 1024)

static const pdb_iterator_type prefix_iterator_type;

/*  The prefix iterator original keeps a cache of
 *  initial values for all the clones.
 *
 *  The clones share the cache, and only clone the
 *  underlying "or" iterator once they've run out
 *  of cached data.
 */

/*  How do we know where we are?
 */
typedef enum {
  /*  We don't know or care where we are.
   *  (After a "check".)
   */
  GRAPHD_PREFIX_NONE,

  /*  We're in the cache, at position
   *  pre_cache_offset (first is #0).
   */
  GRAPHD_PREFIX_CACHE,

  /*  We're where the "pre_or" iterator
   *  thinks we are.  (If we don't have one,
   *  we're at the very start of the original's
   *  pre_or and should expand the cache.)
   */
  GRAPHD_PREFIX_OR,

  /*  We're at the very end of the dataset.
   */
  GRAPHD_PREFIX_EOF

} graphd_prefix_position;

typedef struct graphd_iterator_prefix {
  graphd_handle *pre_g;
  pdb_handle *pre_pdb;
  graphd_request *pre_greq;
  cm_handle *pre_cm;
  cl_handle *pre_cl;

  /*  The pre-build stage of this "or" iterator.
   *  Once this is NULL, pre_or or pre_cache_iterator
   *  are active.
   */
  pdb_iterator *pre_build_or;
  pdb_prefix_context pre_build_ppc;

  /*  An "or" iterator (or similar) that does
   *  the real work.
   *
   *  In a clone, the pre_or cloning is delayed
    * until it runs out of cached values.
   */
  pdb_iterator *pre_or;

  /*  In the original only, a cache of values and
   *  how much they cost to produce.
   */
  graphd_iterator_cache *pre_cache;

  /*  A clone of pre_or that's used to fill the cache,
   *  only.  Only in the original.
   */
  pdb_iterator *pre_cache_iterator;

  /*  The string we're matching.
   */
  char pre_prefix[6 * 5 + 1];
  size_t pre_prefix_n;
  unsigned long long pre_prefix_hash[2];

  /*  Most recently returned ID, or PDB_ID_NONE at the start.
   */
  pdb_id pre_id;

  /*  In the clones, the current offset into the
   *  cached value table.  Incremented after a next
   *  or on-or-after.
   */
  size_t pre_cache_offset;

  graphd_prefix_position pre_position;

} graphd_iterator_prefix;

static int pre_check_primitive(pdb_handle *pdb, pdb_iterator *it, pdb_id id,
                               pdb_budget *budget_inout) {
  graphd_iterator_prefix *pre = it->it_theory;
  pdb_primitive pr;
  int err;
  char const *value_mem;
  size_t value_size;

  *budget_inout -= PDB_COST_PRIMITIVE + 10;

  if ((err = pdb_id_read(pdb, id, &pr)) != 0) {
    cl_log_errno(pre->pre_cl, CL_LEVEL_FAIL, "pdb_id_read", err, "id=%lld",
                 (long long)id);
    return err;
  }

  if ((value_size = pdb_primitive_value_get_size(&pr)) == 0)
    err = GRAPHD_ERR_NO;
  else {
    value_mem = pdb_primitive_value_get_memory(&pr);
    if (!pdb_word_has_prefix_hash(pdb, pre->pre_prefix_hash, value_mem,
                                  value_mem + value_size))
      err = GRAPHD_ERR_NO;
  }
  pdb_primitive_finish(pdb, &pr);

  cl_log(
      pre->pre_cl, CL_LEVEL_VERBOSE, "pre_check_primitive(\"%s\", %lld): %s",
      pre->pre_prefix, (long long)id,
      err == GRAPHD_ERR_NO ? "no" : (err == 0 ? "yes" : graphd_strerror(err)));
  return err;
}

/** @brief Add a little more to an or.
 *
 *  This must complete with GRAPHD_ERR_ALREADY before pre_or can be touched.
 *
 * @return GRAPHD_ERR_ALREADY	finished, safe to use
 * @return 0 		finished, may have changed structure; redirect
 * @return other nonzero error values on error.
 */
static int pre_make_or(pdb_handle *pdb, pdb_iterator *it, pdb_budget *budget) {
  graphd_iterator_prefix *pre;
  cl_handle *cl;
  int err;
  pdb_iterator *sub = NULL;
  pdb_budget budget_in = *budget;

  if (it->it_type == &prefix_iterator_type &&
      it->it_original->it_type == &prefix_iterator_type &&
      ((graphd_iterator_prefix *)it->it_original->it_theory)->pre_build_or ==
          NULL)
    return GRAPHD_ERR_ALREADY;

  cl = pdb_log(pdb);

  cl_enter(cl, CL_LEVEL_VERBOSE, "budget=$%lld", (long long)*budget);
  if (it->it_type != &prefix_iterator_type) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "not a prefix iterator");
    return 0;
  }

  if (it->it_original->it_type != &prefix_iterator_type) {
    cl_leave(cl, CL_LEVEL_VERBOSE,
             "original is not a prefix iterator; refresh");
    return pdb_iterator_refresh(pdb, it);
  }

  pre = it->it_original->it_theory;
  cl_assert(cl, pre->pre_build_or != NULL);

  cl = pre->pre_cl;

  while (*budget > 0) {
    *budget -= PDB_COST_GMAP_ARRAY;

    err = pdb_prefix_next(&pre->pre_build_ppc, it->it_low, it->it_high,
                          pdb_iterator_forward(pdb, it), &sub);
    if (err != 0) {
      size_t dummy_n;

      if (err != GRAPHD_ERR_NO) {
        cl_leave(cl, CL_LEVEL_VERBOSE,
                 "error in "
                 "pdb_prefix_next: %s",
                 graphd_strerror(err));
        return err;
      }

      /*  Commit the "or" iterator.
       */
      err = graphd_iterator_or_create_commit(pre->pre_build_or);
      if (err != 0) {
        cl_leave(cl, CL_LEVEL_VERBOSE,
                 "error in "
                 "graphd_iterator_or_create_commit: %s",
                 graphd_strerror(err));
        return err;
      }

      if (!graphd_iterator_or_is_instance(pre->pre_pdb, pre->pre_build_or,
                                          &dummy_n)) {
        pdb_iterator *tmp;

        tmp = pre->pre_build_or;
        pre->pre_build_or = NULL;

        err = pdb_iterator_substitute(pdb, it, tmp);
        if (err) pdb_iterator_destroy(pdb, &tmp);

        cl_leave(cl, CL_LEVEL_VERBOSE, "(changed) $%lld",
                 (long long)(budget_in - *budget));
        return err;
      }

      /*  Move the "or" iterator into
       *  general service, now that it's complete.
       */
      pre->pre_or = pre->pre_build_or;
      pre->pre_build_or = NULL;

      /*  Reduce our own boundaries to those of
       *  the "or" iterator.
       */
      if (it->it_high > pre->pre_or->it_high)
        it->it_high = pre->pre_or->it_high;
      if (it->it_low < pre->pre_or->it_low) it->it_low = pre->pre_or->it_low;

      cl_leave(cl, CL_LEVEL_VERBOSE, "done $%lld",
               (long long)(budget_in - *budget));
      return GRAPHD_ERR_ALREADY;
    }
    if (pdb_iterator_null_is_instance(pdb, sub)) {
      pdb_iterator_destroy(pdb, &sub);
      continue;
    }
    err = graphd_iterator_or_add_subcondition(pre->pre_build_or, &sub);
    if (err != 0) {
      pdb_iterator_destroy(pdb, &sub);
      cl_leave(cl, CL_LEVEL_VERBOSE, "error in add-subcondition: %s",
               graphd_strerror(err));
      return err;
    }
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "(resume later) $%lld",
           (long long)(budget_in - *budget));
  return PDB_ERR_MORE;
}

/*  Clone the prefix-original's "or" iterator into a prefix-clone.
 */
static int pre_clone_or(pdb_iterator *it, pdb_budget *budget) {
  graphd_iterator_prefix *pre = it->it_theory;
  graphd_iterator_prefix *opre = it->it_original->it_theory;
  cl_handle *cl = pre->pre_cl;
  pdb_handle *pdb = pre->pre_pdb;
  char buf[200];
  int err;

  if (pre->pre_or != NULL) return 0;

  if (opre->pre_or == NULL) return 0;

  cl_assert(cl, opre->pre_or != NULL);

  err = pdb_iterator_clone(pdb, opre->pre_or, &pre->pre_or);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "it=%s",
                 pdb_iterator_to_string(pdb, opre->pre_or, buf, sizeof buf));
    return err;
  }
  if (budget != NULL) {
    size_t n;

    if (graphd_iterator_or_is_instance(pdb, opre->pre_or, &n))
      *budget -= n;
    else
      *budget -= 1;
  }

  /*  Position the cloned "or"-subiterator on the accessing
   *  prefix clone's current position.
   */
  if (pre->pre_id != PDB_ID_NONE) {
    pdb_id id_found;

    err = pdb_iterator_find_nonstep(pdb, pre->pre_or, pre->pre_id, &id_found);

    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_find_nonstep", err,
                   "it=%s, id=%llx",
                   pdb_iterator_to_string(pdb, pre->pre_or, buf, sizeof buf),
                   (unsigned long long)pre->pre_id);
      return err;
    }
    pre->pre_id = id_found;
  }

  cl_log(cl, CL_LEVEL_VERBOSE, "pre_clone_or(%p:%s); at %lld", (void *)it,
         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         (long long)pre->pre_id);

  return 0;
}

/*  Extend a preallocated cache by another entry.
 * (I.e., fill in one more element in a preallocated array.)
 */
static int pre_cache_add(pdb_iterator *it) {
  graphd_iterator_prefix *opre = it->it_original->it_theory;
  pdb_handle *pdb = opre->pre_pdb;
  int err;
  pdb_budget cost = 0;
  pdb_id id;

  /*  We already have as many as we were going to keep.
   */
  if (graphd_iterator_cache_n(opre->pre_cache) > GRAPHD_PREFIX_CACHE_MAX)
    return GRAPHD_ERR_MORE;

  pdb_rxs_push(pdb, "CACHE-ADD %p pre", (void *)it);

  /* Before calling this, pre_make_or must have run to
   * completion.
   */
  cl_assert(opre->pre_cl, opre->pre_or != NULL);

  if (opre->pre_cache_iterator == NULL) {
    err = pdb_iterator_clone(opre->pre_pdb, opre->pre_or,
                             &opre->pre_cache_iterator);
    if (err != 0) {
      pdb_rxs_pop(pdb, "CACHE-ADD %p pre error: %s", (void *)it,
                  graphd_strerror(err));
      return err;
    }
  }

  /*  Read another element from the underlying
   * "or" iterator into the cache array.
   */
  do {
    pdb_budget budget, before;

    before = budget = 99999999;
    err = pdb_iterator_next(opre->pre_pdb, opre->pre_cache_iterator, &id,
                            &budget);
    cost += before - budget;

  } while (err == PDB_ERR_MORE);

  if (err == 0)
    err = graphd_iterator_cache_add(opre->pre_cache, id, cost);
  else if (err == GRAPHD_ERR_NO) {
    graphd_iterator_cache_eof(opre->pre_cache);
    err = 0;
  } else {
    char buf[200];
    cl_log_errno(
        opre->pre_cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "it=%s",
        pdb_iterator_to_string(opre->pre_pdb, opre->pre_or, buf, sizeof buf));
  }

  if (err == 0)
    pdb_rxs_pop(pdb, "CACHE-ADD %p %llx ($%lld)", (void *)it,
                (unsigned long long)id, (long long)cost);

  else if (err == GRAPHD_ERR_NO)
    pdb_rxs_pop(pdb, "CACHE-ADD %p done ($%lld)", (void *)it, (long long)cost);

  else
    pdb_rxs_pop(pdb, "CACHE-ADD %p %s ($%lld)", (void *)it,
                graphd_strerror(err), (long long)cost);

  return err;
}

static int pre_iterator_find_loc(pdb_handle *pdb, pdb_iterator *it,
                                 pdb_id id_in, pdb_id *id_out,
                                 pdb_budget *budget_inout, char const *file,
                                 int line) {
  graphd_iterator_prefix *pre = it->it_theory;
  graphd_iterator_prefix *opre = it->it_original->it_theory;
  cl_handle *cl = pre->pre_cl;
  int err = 0;
  size_t off;
  pdb_budget budget_in = *budget_inout;
  pdb_id id_found;

  pdb_rxs_push(pdb, "FIND %p pre %llx", (void *)it, (unsigned long long)id_in);

  cl_assert(cl, id_in != PDB_ID_NONE);
  cl_assert(cl, id_in < (1ull << 34));

  switch (it->it_call_state) {
    default:
    case 0:
      pre->pre_id = PDB_ID_NONE;
      pre->pre_position = GRAPHD_PREFIX_NONE;

      err = pre_make_or(pdb, it, budget_inout);
      if (err != GRAPHD_ERR_ALREADY) {
        char buf[200];
        if (err == 0) {
          pdb_rxs_pop(pdb, "FIND %p pre %llx redirect ($%lld)", (void *)it,
                      (unsigned long long)id_in,
                      (long long)(budget_in - *budget_inout));
          pdb_iterator_account_charge_budget(pdb, it, find);

          return pdb_iterator_find_loc(pdb, it, id_in, id_out, budget_inout,
                                       file, line);
        }
        cl_log_errno(cl, CL_LEVEL_FAIL, "pre_make_or", err, "it=%s",
                     pdb_iterator_to_string(pdb, it, buf, sizeof buf));
        goto unexpected_error;
      }

      /*  Do we have everything we need already in
       *  the cache?
       */
      id_found = id_in;
      err = graphd_iterator_cache_search(pdb, it, opre->pre_cache, &id_found,
                                         &off);
      if (err == GRAPHD_ERR_MORE) {
        /*  No.  Expand the cache by one and try again;
         *  maybe we're just going forward.
         */
        err = pre_cache_add(it);
        if (err == 0 || err == GRAPHD_ERR_NO) {
          id_found = id_in;
          err = graphd_iterator_cache_search(pdb, it, opre->pre_cache,
                                             &id_found, &off);
        }
      }

      if (err != GRAPHD_ERR_MORE)
        /* Charge for the cached entry or EOF
         */
        *budget_inout -= graphd_iterator_cache_cost(opre->pre_cache);
      if (err == 0) {
        pre->pre_cache_offset = off + 1;
        pre->pre_position = GRAPHD_PREFIX_CACHE;
        break;
      } else if (err != GRAPHD_ERR_MORE)
        break;

      /*  Let the "or" do the real work.
       */
      pre->pre_position = GRAPHD_PREFIX_OR;

      /*  If we don't have one left over,
       *  clone an "or" iterator that we can use
       *  for positioning.
       */
      if (pre->pre_or == NULL && (err = pre_clone_or(it, budget_inout)) != 0)
        goto unexpected_error;

      cl_assert(cl, pre->pre_or != NULL);

    case 1:
      err = pdb_iterator_find_loc(pdb, pre->pre_or, id_in, &id_found,
                                  budget_inout, file, line);
      if (err == PDB_ERR_MORE) {
        it->it_call_state = 1;
        pdb_rxs_pop(pdb, "FIND %p pre %llx suspend ($%lld)", (void *)it,
                    (unsigned long long)id_in,
                    (long long)(budget_in - *budget_inout));
        goto err;
      }
      break;
  }

  if (err == 0) {
    pdb_rxs_pop(pdb, "FIND %p pre %llx %llx ($%lld)", (void *)it,
                (unsigned long long)id_in, (unsigned long long)*id_out,
                (long long)(budget_in - *budget_inout));

    pre->pre_id = *id_out = id_found;
  } else if (err == GRAPHD_ERR_NO) {
    pre->pre_position = GRAPHD_PREFIX_EOF;
    pre->pre_id = PDB_ID_NONE;

    pdb_rxs_pop(pdb, "FIND %p pre %llx done ($%lld)", (void *)it,
                (unsigned long long)id_in,
                (long long)(budget_in - *budget_inout));
  } else {
  unexpected_error:
    pdb_rxs_pop(pdb, "FIND %p pre %llx error: %s ($%lld)", (void *)it,
                (unsigned long long)id_in, graphd_strerror(err),
                (long long)(budget_in - *budget_inout));
  }
err:
  pdb_iterator_account_charge_budget(pdb, it, find);
  return err;
}

static int pre_iterator_check(pdb_handle *pdb, pdb_iterator *it, pdb_id id,
                              pdb_budget *budget_inout) {
  graphd_iterator_prefix *pre = it->it_theory;
  graphd_iterator_prefix *opre = it->it_original->it_theory;
  int err;
  pdb_budget budget_in = *budget_inout;

  pre->pre_id = PDB_ID_NONE;
  pre->pre_position = GRAPHD_PREFIX_NONE;

  /*  Don't bother creating a pre_or iterator if we don't
   *  have to -- but if we happen to have one, try using
   *  the cache.
   */
  if (pre->pre_or != NULL) {
    /*  Would this be in the cache if it existed?
     */
    err = graphd_iterator_cache_check(pdb, it, opre->pre_cache, id);
    if (err == PDB_ERR_MORE) {
      /*  Cache doesn't know.  Expand it, then
       *  ask the expanded cache.
       */
      err = pre_cache_add(it);
      if (err != 0 && err != GRAPHD_ERR_NO) {
        char buf[200];
        cl_log_errno(pre->pre_cl, CL_LEVEL_FAIL, "pre_cache_add", err, "it=%s",
                     pdb_iterator_to_string(pdb, it, buf, sizeof buf));
        goto err;
      }

      err = graphd_iterator_cache_check(pdb, it, opre->pre_cache, id);
    }
    if (err != PDB_ERR_MORE) {
      /* Cached values are half price. */
      *budget_inout -= pdb_iterator_check_cost(pdb, it) / 2;
      cl_log(pre->pre_cl, CL_LEVEL_VERBOSE,
             "pre_iterator_check(\"%s\", %lld):"
             " %s [cached] ($%lld)",
             pre->pre_prefix, (long long)id,
             err == GRAPHD_ERR_NO ? "no"
                                  : (err == 0 ? "yes" : graphd_strerror(err)),
             (long long)(budget_in - *budget_inout));
      goto err;
    }
  }

  err = pre_check_primitive(pdb, it, id, budget_inout);

  pdb_rxs_log(
      pdb, "CHECK %p pre %llx %s ($%lld)", (void *)it, (unsigned long long)id,
      err == GRAPHD_ERR_NO ? "no" : (err == 0 ? "yes" : graphd_strerror(err)),
      (long long)(budget_in - *budget_inout));

err:
  pdb_iterator_account_charge_budget(pdb, it, check);
  return err;
}

static int pre_iterator_statistics(pdb_handle *pdb, pdb_iterator *it,
                                   pdb_budget *budget_inout) {
  graphd_iterator_prefix *pre = it->it_theory;
  cl_handle *cl = pre->pre_cl;
  int err;

  cl_assert(cl, it->it_original == it);

  /* Phase 1: wait for the "or" construction to complete.
   */
  err = pre_make_or(pdb, it, budget_inout);
  if (err != GRAPHD_ERR_ALREADY) {
    if (err == 0) return pdb_iterator_statistics(pdb, it, budget_inout);
    return err;
  }
  cl_assert(cl, pre->pre_or != NULL);

  /* Phase 2: run statistics on the "or" we just built.
   */
  err = pdb_iterator_statistics(pdb, pre->pre_or, budget_inout);
  if (pdb_iterator_statistics_done(pdb, pre->pre_or)) {
    char buf[200];

    pdb_iterator_statistics_copy(pdb, it, pre->pre_or);

    /* Except for check, which is cheaper. */
    pdb_iterator_check_cost_set(pdb, it, PDB_COST_PRIMITIVE + 10);

    pdb_iterator_statistics_done_set(pdb, it);

    pdb_rxs_log(pdb,
                "STAT %p pre %s n=%llu "
                "cc=%llu; nc=%llu; fc=%llu; %ssorted",
                (void *)it, pdb_iterator_to_string(pdb, it, buf, sizeof buf),
                (unsigned long long)pdb_iterator_n(pdb, it),
                (unsigned long long)pdb_iterator_check_cost(pdb, it),
                (unsigned long long)pdb_iterator_next_cost(pdb, it),
                (unsigned long long)pdb_iterator_find_cost(pdb, it),
                pdb_iterator_sorted(pdb, it) ? "" : "un");

    pdb_prefix_statistics_store(pdb, it, pre->pre_prefix,
                                pre->pre_prefix + strlen(pre->pre_prefix));
  }
  return err;
}

static int pre_iterator_next_loc(pdb_handle *pdb, pdb_iterator *it,
                                 pdb_id *id_out, pdb_budget *budget_inout,
                                 char const *file, int line) {
  graphd_iterator_prefix *pre = it->it_theory;
  graphd_iterator_prefix *opre = it->it_original->it_theory;
  cl_handle *cl = pre->pre_cl;
  int err = 0;
  char buf[200];
  pdb_budget budget_in = *budget_inout;

  pdb_rxs_push(pdb, "NEXT %p pre", (void *)it);

  cl_assert(cl, pre->pre_position != GRAPHD_PREFIX_NONE);
  switch (it->it_call_state) {
    default:
    case 0:
      if (pre->pre_position == GRAPHD_PREFIX_EOF) {
        err = GRAPHD_ERR_NO;
        break;
      }

      /* Wait for the "or" construction to complete.
       */
      err = pre_make_or(pdb, it, budget_inout);
      if (err != GRAPHD_ERR_ALREADY) {
        if (err == 0) {
          pdb_rxs_pop(pdb, "NEXT %p pre redirect ($%lld)", (void *)it,
                      (long long)(budget_in - *budget_inout));

          /* May have changed structure; redirect.
           */
          pdb_iterator_account_charge_budget(pdb, it, next);

          return pdb_iterator_next_loc(pdb, it, id_out, budget_inout, file,
                                       line);
        }
        cl_log_errno(cl, CL_LEVEL_FAIL, "pre_make_or", err, "it=%s",
                     pdb_iterator_to_string(pdb, it, buf, sizeof buf));
        break;
      }
      cl_assert(cl, opre->pre_or != NULL);

      if (pre->pre_position == GRAPHD_PREFIX_CACHE) {
        /*  If we're at the very end of the cache, try
         *  topping it off.
         */
        if (pre->pre_cache_offset == graphd_iterator_cache_n(opre->pre_cache)) {
          err = pre_cache_add(it);
          if (err != 0 && err != GRAPHD_ERR_MORE) break;
        }

        /*  Is the value cached?
         */
        err = graphd_iterator_cache_index(
            opre->pre_cache, pre->pre_cache_offset, id_out, budget_inout);
        if (err != PDB_ERR_MORE) {
          if (err == 0) pre->pre_cache_offset++;
          break;
        }
      }

      /*  Make sure we have an "or" iterator that implements
       *  the prefix combination.
       */
      if (pre->pre_or == NULL) {
        err = pre_clone_or(it, budget_inout);
        if (err != 0) break;
      }

    /* Use the "or" iterator to generate the next value.
     */
    case 1:
      err = pdb_iterator_next_loc(pdb, pre->pre_or, id_out, budget_inout, file,
                                  line);
      if (err == PDB_ERR_MORE) {
        it->it_call_state = 1;
        pdb_rxs_pop(pdb, "NEXT %p pre suspend ($%lld)", (void *)it,
                    (budget_in - *budget_inout));
        goto err;
      }
      break;

  } /* switch (it->it_call_state) */

  if (err != 0) {
    if (err == GRAPHD_ERR_NO) {
      pre->pre_position = GRAPHD_PREFIX_EOF;
      pdb_rxs_pop(pdb, "NEXT %p pre done ($%lld)", (void *)it,
                  (budget_in - *budget_inout));
    } else
      pdb_rxs_pop(pdb, "NEXT %p pre error: %s ($%lld)", (void *)it,
                  graphd_strerror(err), (budget_in - *budget_inout));
  } else {
    pdb_rxs_pop(pdb, "NEXT %p pre %llx ($%lld)", (void *)it,
                (unsigned long long)*id_out, (budget_in - *budget_inout));
    pre->pre_id = *id_out;
  }

err:
  pdb_iterator_account_charge_budget(pdb, it, next);
  return err;
}

static int pre_iterator_reset(pdb_handle *pdb, pdb_iterator *it) {
  char buf[200];
  graphd_iterator_prefix *pre = it->it_theory;

  pre->pre_id = PDB_ID_NONE;
  pre->pre_cache_offset = 0;
  pre->pre_position = GRAPHD_PREFIX_CACHE;

  cl_log(pre->pre_cl, CL_LEVEL_VERBOSE, "pre_iterator_reset %s",
         pdb_iterator_to_string(pdb, it, buf, sizeof buf));

  return 0;
}

/*  prefix:[~]LOW[-HIGH]:TEXT
 *	/ID
 *	/[statistics]
 */
static int pre_iterator_freeze(pdb_handle *pdb, pdb_iterator *it,
                               unsigned int flags, cm_buffer *buf) {
  graphd_iterator_prefix *pre = it->it_theory;
  int err = 0;
  char pbuf[200];
  char const *sep = "";

  if (flags & PDB_ITERATOR_FREEZE_SET) {
    cl_assert(pre->pre_cl, pre->pre_prefix_n <= (5 * 6));
    graphd_escape(pre->pre_cl, pre->pre_prefix,
                  pre->pre_prefix + pre->pre_prefix_n, pbuf,
                  pbuf + sizeof pbuf);

    err = pdb_iterator_freeze_intro(buf, it, "prefix");
    if (err != 0) return err;

    err = pdb_iterator_freeze_account(pdb, buf, it);
    if (err != 0) return err;

    err = cm_buffer_sprintf(buf, ":%s", pbuf);
    if (err != 0) return err;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    err = cm_buffer_add_string(buf, sep);
    if (err != 0) return err;

    err = graphd_iterator_util_freeze_position(
        pdb, pre->pre_position == GRAPHD_PREFIX_EOF, pre->pre_id, PDB_ID_NONE,
        buf);
    if (err != 0) return err;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_STATE) {
    err = cm_buffer_add_string(buf, sep);
    if (err != 0) return err;

    if (pdb_iterator_statistics_done(pdb, it)) {
      err = cm_buffer_sprintf(
          buf, "[st:%llu:%llu:%llu]",
          (unsigned long long)pdb_iterator_n(pdb, it),
          (unsigned long long)pdb_iterator_next_cost(pdb, it),
          (unsigned long long)pdb_iterator_find_cost(pdb, it));
      if (err != 0) return err;
    }
  }
  return err;
}

static int pre_iterator_clone(pdb_handle *pdb, pdb_iterator *it,
                              pdb_iterator **it_out) {
  pdb_iterator *it_orig = it->it_original;
  graphd_iterator_prefix *pre = it->it_theory;
  graphd_iterator_prefix *pre_out;
  int err;

  PDB_IS_ITERATOR(pre->pre_cl, it);
  PDB_IS_ORIGINAL_ITERATOR(pre->pre_cl, it->it_original);

  /*  Prefix iterators do not evolve.
   */
  cl_assert(pre->pre_cl, it_orig->it_type == it->it_type);

  *it_out = NULL;

  pre_out = cm_malcpy(pre->pre_cm, pre, sizeof(*pre));
  if (pre_out == NULL) return errno ? errno : ENOMEM;

  pre_out->pre_build_or = NULL;
  pre_out->pre_or = NULL;
  pre_out->pre_cache_iterator = NULL;

  /*  Clones have a null cache; they use the
   *  original's cache instead.
   */
  pre_out->pre_cache = NULL;

  if ((err = pdb_iterator_make_clone(pdb, it_orig, it_out)) != 0) {
    cm_free(pre->pre_cm, pre_out);
    return err;
  }
  (*it_out)->it_theory = pre_out;
  (*it_out)->it_has_position = true;

  pdb_rxs_log(pdb, "CLONE %p pre %p", (void *)it, (void *)*it_out);
  return 0;
}

static void pre_iterator_finish(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_prefix *pre = it->it_theory;

  if (pre != NULL) {
    pdb_iterator_destroy(pdb, &pre->pre_build_or);
    pdb_iterator_destroy(pdb, &pre->pre_or);
    pdb_iterator_destroy(pdb, &pre->pre_cache_iterator);

    if (pre->pre_cache != NULL) {
      graphd_iterator_cache_destroy(pre->pre_cache);
      pre->pre_cache = NULL;
    }

    cm_free(pre->pre_cm, it->it_displayname);
    it->it_displayname = NULL;

    cm_free(pre->pre_cm, pre);
    it->it_theory = NULL;
  }
  it->it_type = NULL;
  it->it_magic = 0;
}

static char const *pre_iterator_to_string(pdb_handle *pdb, pdb_iterator *it,
                                          char *buf, size_t size) {
  graphd_iterator_prefix *pre = it->it_theory;
  char sub[200];

  graphd_escape(pre->pre_cl, pre->pre_prefix,
                pre->pre_prefix + pre->pre_prefix_n, sub, sub + sizeof sub);
  snprintf(buf, size, "%sprefix(%s)", pdb_iterator_forward(pdb, it) ? "" : "~",
           sub);
  return buf;
}

static const pdb_iterator_type prefix_iterator_type = {
    "prefix",
    pre_iterator_finish,
    pre_iterator_reset,
    pre_iterator_clone,
    pre_iterator_freeze,
    pre_iterator_to_string,

    pre_iterator_next_loc,
    pre_iterator_find_loc,
    pre_iterator_check,
    pre_iterator_statistics,

    NULL, /* idarray */
    NULL, /* primitive-summary*/
    NULL, /* beyond */
    NULL, /* estimate */
    NULL, /* restrict */

    NULL, /* suspend */
    NULL  /* unsuspend */
};

/**
 * @brief Make an "or" iterator from the prefix completions of a short string.
 *
 * @param graphd	opaque module handle
 * @param s		start of the prefix string
 * @param e		end of the prefix string (first byte not included)
 * @param low		low end of result set
 * @param high		first id not included in result set
 * @param forward	are we iterating low to high?
 * @param it_out	create an iterator here.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int prefix_make(graphd_request *greq, char const *s, char const *e,
                       unsigned long long low, unsigned long long high,
                       bool forward, pdb_iterator **it_out) {
  graphd_handle *graphd = graphd_request_graphd(greq);
  graphd_iterator_prefix *pre;
  pdb_handle *pdb = graphd->g_pdb;
  cl_handle *cl = pdb_log(pdb);
  cm_handle *cm = pdb_mem(pdb);
  graphd_iterator_cache *gic;
  size_t len;

  int err;
  pdb_budget budget = 5000;

  len = pdb_word_utf8len(pdb, s, e);
  if (len == 0) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "prefix_map: length s..e \"%.*s\" is 0: returning all", (int)(e - s),
           s);

    return pdb_iterator_all_create(pdb, low, high, forward, it_out);
  } else if (len >= 5)

    /*  For length >= 5, PDB makes no difference between
     *  prefixes and words (i.e., "intercourse" and "intercept"
     *  get hashed to the same series of buckets.)
     */
    return pdb_iterator_word_create(pdb, s, e, low, high, forward,
                                    /* error-if-null */ false, it_out);

  *it_out = NULL;
  if ((pre = cm_zalloc(cm, sizeof(*pre))) == NULL ||
      (*it_out = cm_malloc(cm, sizeof(**it_out))) == NULL ||
      (gic = graphd_iterator_cache_create(graphd, 1024)) == NULL) {
    err = errno ? errno : ENOMEM;
    cm_free(cm, pre);
    cm_free(cm, *it_out);

    return err;
  }
  pdb_iterator_make(pdb, *it_out, low, high, forward);
  (*it_out)->it_theory = pre;
  (*it_out)->it_type = &prefix_iterator_type;

  pre->pre_g = graphd;
  pre->pre_pdb = pdb;
  pre->pre_cm = cm;
  pre->pre_cl = cl;
  pre->pre_greq = greq;
  pre->pre_cache = gic;
  pre->pre_position = GRAPHD_PREFIX_CACHE;
  pre->pre_id = PDB_ID_NONE;
  pre->pre_cache_iterator = NULL;
  pdb_prefix_initialize(pdb, s, e, &pre->pre_build_ppc);
  pdb_iterator_sorted_set(pdb, *it_out, true);
  pdb_iterator_check_cost_set(pdb, *it_out, PDB_COST_PRIMITIVE + 10);

  memcpy(pre->pre_prefix, s, e - s);
  pre->pre_prefix[e - s] = '\0';
  pre->pre_prefix_n = e - s;
  pdb_word_has_prefix_hash_compile(pdb, pre->pre_prefix_hash, s, e);

  /*  If we cached statistics, read them from PDB's cache.
   */
  (void)pdb_prefix_statistics_load(pdb, *it_out, pre->pre_prefix,
                                   pre->pre_prefix + pre->pre_prefix_n);

  /*  Create an "or" iterator.  It'll implement this virtual
   *  prefix iterator as an "or" of the individual prefix
   *  completions and words we get from pdb.
   */
  err = graphd_iterator_or_create(greq, 0, forward, &pre->pre_build_or);
  if (err != 0) return err;

  /*  Spend a little bit of budget exploring this iterator -
   *  but if it takes too long, leave it for later.
   */
  budget = 5000;
  err = pre_make_or(pdb, *it_out, &budget);
  if (err != GRAPHD_ERR_MORE && err != GRAPHD_ERR_ALREADY)
    /*  The iterator changed shape - it's not safe
     *  to touch our local variables at this point.
     */
    return err;

  pdb_rxs_log(pdb, "CREATE %p pre", (void *)*it_out);
  return 0;
}

/**
 * @brief Make an "or" iterator from the prefix completions of a short string.
 *
 * @param graphd	opaque module handle
 * @param s		start of the prefix string
 * @param e		end of the prefix string (first byte not included)
 * @param low		low end of result set
 * @param high		first id not included in result set
 * @param forward	are we iterating low to high?
 * @param it_out	create an iterator here.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_prefix_create(graphd_request *greq, char const *s,
                                  char const *e, unsigned long long low,
                                  unsigned long long high,
                                  graphd_direction direction,
                                  pdb_iterator **it_out) {
  graphd_handle *graphd = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  char buf[200];
  bool forward = direction != GRAPHD_DIRECTION_BACKWARD;
  int err;

  cl_assert(cl, GRAPHD_DIRECTION_VALID(direction));
  cl_enter(cl, CL_LEVEL_VERBOSE, "(\"%.*s\", %lld-%lld, %s)", (int)(e - s), s,
           (long long)low, (long long)high, forward ? "forward" : "backward");

  err = prefix_make(greq, s, e, low, high, forward, it_out);

  cl_leave(
      cl, CL_LEVEL_VERBOSE, "%s",
      err ? graphd_strerror(err)
          : pdb_iterator_to_string(graphd->g_pdb, *it_out, buf, sizeof buf));
  return err;
}

/**
 * @brief Thaw a prefix iterator.
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_prefix_thaw(graphd_handle *graphd,
                                pdb_iterator_text const *pit,
                                pdb_iterator_base *pib, cl_loglevel loglevel,
                                pdb_iterator **it_out) {
  unsigned long long low, high;
  int err;
  graphd_request *greq;
  pdb_handle *pdb = graphd->g_pdb;
  cl_handle *cl;
  char buf[200 + sizeof("prefix::") + 3 * 5 * 6];
  char const *prefix_s, *prefix_e;
  bool forward;
  pdb_id resume_id = PDB_ID_NONE, last_id = PDB_ID_NONE;
  char const *s, *e;
  graphd_iterator_prefix *pre;
  pdb_iterator_account *acc = NULL;
  bool eof = false;

  greq = pdb_iterator_base_lookup(pdb, pib, "graphd.request");
  if (greq == NULL) return GRAPHD_ERR_NO;

  cl = graphd_request_cl(greq);

  cl_enter(cl, CL_LEVEL_VERBOSE, "\"%.*s\"",
           (int)(pit->pit_set_e - pit->pit_set_s), pit->pit_set_s);

  /* Prefix
   */
  s = pit->pit_set_s;
  e = pit->pit_set_e;

  err = pdb_iterator_util_thaw(
      pdb, &s, e, "%{forward}%{low[-high]}%{account}%{extensions}:%s", &forward,
      &low, &high, pib, &acc, (pdb_iterator_property *)NULL, &prefix_s,
      &prefix_e);
  if (err != 0) {
    cl_log(cl, loglevel,
           "graphd_iterator_prefix_thaw: "
           "parser error in \"%.*s\" (expected prefix:..)",
           (int)(e - s), s);
    cl_leave(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_prefix_thaw: "
             "parser error in \"%.*s\" (expected prefix:..)",
             (int)(e - s), s);
  }
  prefix_e = graphd_unescape(cl, prefix_s, prefix_e, buf, buf + sizeof(buf));
  prefix_s = buf;

  /*  Position.
   */
  if ((s = pit->pit_position_s) != NULL && (s < (e = pit->pit_position_e))) {
    err = graphd_iterator_util_thaw_position(pdb, &s, e, loglevel, &eof,
                                             &last_id, &resume_id);
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
      return err;
    }

    err = pdb_iterator_util_thaw(pdb, &s, e, "%{extensions}%{end}",
                                 (pdb_iterator_property *)NULL);
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
      return err;
    }
  }

  err = prefix_make(greq, prefix_s, prefix_e, low, high, forward, it_out);
  if (err != 0) {
    cl_leave_err(cl, CL_LEVEL_VERBOSE, err, "syntax error");
    return err;
  }

  pdb_iterator_account_set(pdb, *it_out, acc);

  /*  State - statistics, if we had some.
   */
  if ((s = pit->pit_state_s) != NULL && (s < (e = pit->pit_state_e))) {
    char const *s0 = s;

    if (s < e) {
      unsigned long long ull;
      pdb_budget nc, fc;

      if (pdb_iterator_util_thaw(pdb, &s, e, "[st:%llu:%{budget}:%{budget}]",
                                 &ull, &nc, &fc) == 0) {
        pdb_iterator_n_set(pdb, *it_out, ull);
        pdb_iterator_next_cost_set(pdb, *it_out, nc);
        pdb_iterator_find_cost_set(pdb, *it_out, fc);
        pdb_iterator_forward_set(pdb, *it_out, forward);

        /*  Check cost is constant - a little more
         *  than a primitive read.
         */
        pdb_iterator_check_cost_set(pdb, *it_out, PDB_COST_PRIMITIVE + 10);

        pdb_iterator_statistics_done_set(pdb, *it_out);
      } else
        s = s0;
    }
    err = pdb_iterator_util_thaw(pdb, &s, e, "%{extensions}%{end}",
                                 (pdb_iterator_property *)NULL);
    if (err != 0) {
      pdb_iterator_destroy(pdb, it_out);
      cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
      return err;
    }
  }

  if ((*it_out)->it_type != &prefix_iterator_type) {
    cl_leave(cl, CL_LEVEL_VERBOSE,
             "evolved into something other than a prefix");
    return 0;
  }
  pre = (*it_out)->it_theory;
  if (eof) {
    pre->pre_position = GRAPHD_PREFIX_EOF;
    pre->pre_id = PDB_ID_NONE;
  } else {
    pdb_budget high = 999999;
    int err;

    if (last_id != PDB_ID_NONE) {
      pdb_id id;
      bool changed;

      while ((err = pdb_iterator_statistics(pdb, *it_out, &high)) ==
             PDB_ERR_MORE) {
        cl_log(cl, CL_LEVEL_INFO,
               "graphd_iterator_prefix_thaw: while "
               "rebuilding \"%.*s/%.*s\", statistics "
               "take a long time.",
               (int)(pit->pit_set_e - pit->pit_set_s), pit->pit_set_s,
               (int)(pit->pit_position_e - pit->pit_position_s),
               pit->pit_position_s);
        high = 999999;
      }
      if (err != 0) {
        cl_log_errno(cl, loglevel, "pdb_iterator_statistics", err, "it=%s",
                     pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));

        goto err;
      }

      changed = false;

      while ((err = pdb_iterator_find(pdb, *it_out, last_id, &id, &high)) ==
             PDB_ERR_MORE) {
        cl_log(cl, CL_LEVEL_INFO,
               "graphd_iterator_prefix_thaw: while "
               "rebuilding \"%.*s/%.*s\", find takes "
               "a long time.",
               (int)(pit->pit_set_e - pit->pit_set_s), pit->pit_set_s,
               (int)(pit->pit_position_e - pit->pit_position_s),
               pit->pit_position_s);
        high = 999999;
      }
      if (err != 0) {
        cl_log_errno(cl, loglevel, "pdb_iterator_find", err, "it=%s, id=%llx",
                     pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf),
                     (unsigned long long)last_id);
        goto err;
      }
      if (id != last_id) {
        cl_log(cl, loglevel,
               "graphd_iterator_prefix_thaw: "
               "find can't find %llx, positioning "
               "on %llx instead?",
               (unsigned long long)last_id, (unsigned long long)id);
        err = GRAPHD_ERR_NO;
        goto err;
      }
    }
    pre->pre_id = last_id;
  }

  /*  If we have a statistics cache or an OR with statistics,
   *  copy those stats.
   */
  if (pdb_prefix_statistics_load(pdb, *it_out, pre->pre_prefix,
                                 pre->pre_prefix + strlen(pre->pre_prefix)) !=
      0) {
    if (pre->pre_or != NULL && pdb_iterator_statistics_done(pdb, pre->pre_or)) {
      pdb_iterator_statistics_copy(pdb, *it_out, pre->pre_or);
      /* Except for check, which is cheaper. */
      pdb_iterator_check_cost_set(pdb, *it_out, PDB_COST_PRIMITIVE + 10);
      pdb_iterator_statistics_done_set(pdb, *it_out);
    }
  }

  cl_leave(cl, CL_LEVEL_VERBOSE, "done (it=%p)", (void *)*it_out);
  return 0;

err:
  pdb_iterator_destroy(pdb, it_out);
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));

  return err;
}

/**
 * @brief Get data about an iterator.
 *
 * @param pdb		opaque module handle, created with pdb_create()
 * @param it		some iterator
 * @param s_out		out: start of prefix.
 * @param e_out		out: end of prefix
 *
 * @return true if this is a prefix iterator (and the prefix is filled in),
 * 	false otherwise.
 */
int graphd_iterator_prefix_is_instance(pdb_handle *pdb, pdb_iterator *it,
                                       char const **s_out, char const **e_out) {
  graphd_iterator_prefix *pre;

  if (it->it_type != &prefix_iterator_type) return false;

  pre = it->it_theory;
  if (s_out != NULL) *s_out = pre->pre_prefix;
  if (e_out != NULL) *e_out = pre->pre_prefix + pre->pre_prefix_n;

  return true;
}

/**
 * @brief Get data about an iterator.
 *
 * @param pdb		opaque module handle, created with pdb_create()
 * @param it		some iterator
 * @param s_out		out: start of prefix.
 * @param e_out		out: end of prefix
 *
 * @return true if this is a prefix iterator (and the prefix is filled in),
 * 	false otherwise.
 */
int graphd_iterator_prefix_or(pdb_handle *pdb, pdb_iterator *it,
                              pdb_iterator **sub_out) {
  graphd_iterator_prefix *opre;

  if (it->it_type != &prefix_iterator_type) return GRAPHD_ERR_NO;

  opre = it->it_original->it_theory;
  *sub_out = opre->pre_or;

  return 0;
}
