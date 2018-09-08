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
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

/* How many results before we give up an intersect attempt?
 */
#define GRAPHD_ITERATOR_FIXED_FAST_INTERSECT_MAX (1024 * 32)
#define GRAPHD_ITERATOR_FIXED_CACHE_MIN 10

/* The fixed original's data.
 */
#define ofix(it) ((graphd_iterator_fixed *)(it)->it_original->it_theory)

typedef struct graphd_iterator_fixed_base {
  /*  In the original only - a ``storable'' handle for
   *  use with the iterator resource cache.
   */
  graphd_storable fb_storable;

  cm_handle *fb_cm;
  cl_handle *fb_cl;
  pdb_handle *fb_pdb;
  graphd_handle *fb_graphd;

  /*  In the original only: the IDs that make up the
   *  contents in this iterator.
   *
   *  If it's just one ID, it's buffered implicitly.
   */
  pdb_id *fb_id;
  pdb_id fb_id_buf[1];
  size_t fb_m;
  size_t fb_n; /* same as it_n. */

  /*  In original only: was an ID added to the iterator
   *  in the construction phase that wasn't in order?  If
   *  yes, the iterator needs sorting.
   */
  bool fb_sort_me;

  /*  In the original only:
   *  When freezing, masquerade as this rather than
   *  iterating over all the subiterators.
   *
   *  low and high are injected into the first :: in the string.
   */
  char *fb_masquerade;

} graphd_iterator_fixed_base;

typedef struct graphd_iterator_fixed {
  graphd_iterator_fixed_base *fix_base;
  size_t fix_i;
  unsigned int fix_committed : 1;

} graphd_iterator_fixed;

static int fixed_wrap_loc(graphd_handle *g, unsigned long long low,
                          unsigned long long high, bool forward,
                          graphd_iterator_fixed *fix, pdb_iterator **it_out,
                          char const *file, int line);

static void fixed_storable_destroy(void *data) {
  graphd_iterator_fixed_base *fb = data;
  cl_handle *cl = fb->fb_cl;

  cl_enter(cl, CL_LEVEL_VERBOSE, "fb=%p", (void *)fb);

  if (fb->fb_m > 0 && fb->fb_id != NULL && fb->fb_id != fb->fb_id_buf)
    cm_free(fb->fb_cm, fb->fb_id);

  cm_free(fb->fb_cm, fb->fb_masquerade);
  cm_free(fb->fb_cm, fb);

  cl_leave(cl, CL_LEVEL_VERBOSE, "done");
}

static bool fixed_storable_equal(void const *A, void const *B) {
  graphd_iterator_fixed_base const *a = A;
  graphd_iterator_fixed_base const *b = B;

  if (a == b) return true;

  return a->fb_n == b->fb_n &&
         memcmp(a->fb_id, b->fb_id, sizeof(*a->fb_id) * a->fb_n) == 0;
}

static unsigned long fixed_storable_hash(void const *data) {
  graphd_iterator_fixed_base const *fb = data;
  unsigned long hash = 0;
  size_t i;

  for (i = 0; i < fb->fb_n; i++) hash = (hash * 33) ^ fb->fb_id[i];
  return hash ^ fb->fb_n;
}

static struct graphd_storable_type const fixed_storable_type = {
    "fixed iterator data", fixed_storable_destroy, fixed_storable_equal,
    fixed_storable_hash

};

static graphd_iterator_fixed_base *fixed_base_make(graphd_handle *g,
                                                   size_t nelems) {
  cm_handle *cm = pdb_mem(g->g_pdb);
  cl_handle *cl = pdb_log(g->g_pdb);
  graphd_iterator_fixed_base *fb;

  if ((fb = cm_zalloc(cm, sizeof(*fb))) == NULL) return NULL;

  fb->fb_storable.gs_type = &fixed_storable_type;
  fb->fb_storable.gs_linkcount = 1;
  fb->fb_storable.gs_size = sizeof(*fb);
  fb->fb_n = 0;

  if (nelems <= 1) {
    fb->fb_id = fb->fb_id_buf;
    fb->fb_m = 1;
  } else {
    fb->fb_id = cm_malloc(cm, nelems * sizeof(*fb->fb_id));
    if (fb->fb_id == NULL) {
      cm_free(cm, fb);
      return NULL;
    }
    fb->fb_m = nelems;
  }
  fb->fb_cm = cm;
  fb->fb_cl = cl;
  fb->fb_pdb = g->g_pdb;
  fb->fb_graphd = g;

  return fb;
}

static graphd_iterator_fixed *fixed_make(graphd_handle *g,
                                         graphd_iterator_fixed_base *fb) {
  cm_handle *cm = pdb_mem(g->g_pdb);
  graphd_iterator_fixed *fix;

  if (fb == NULL) return NULL;

  if ((fix = cm_zalloc(cm, sizeof(*fix))) == NULL) return NULL;

  graphd_storable_link(fb);
  fix->fix_base = fb;

  return fix;
}

static void fixed_destroy(graphd_iterator_fixed *fix) {
  cm_handle *cm = fix->fix_base->fb_cm;
  cl_handle *cl = fix->fix_base->fb_cl;

  cl_log(cl, CL_LEVEL_VERBOSE, "fixed_destroy %p->%p[%zu -> %zu]", (void *)fix,
         (void *)fix->fix_base, (size_t)fix->fix_base->fb_storable.gs_linkcount,
         (size_t)fix->fix_base->fb_storable.gs_linkcount - 1);

  if (fix->fix_base != NULL)
    graphd_storable_unlink(&fix->fix_base->fb_storable);

  cm_free(cm, fix);
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
 * @return PDB_ERR_MORE if the ID or a nearby ID *may* be part of
 *	the result set, but the cache doesn't have the answer.
 */
static int fixed_search(pdb_handle *pdb, pdb_iterator *it, pdb_id *id_inout,
                        size_t *off_out) {
  pdb_id const *fix_id = ofix(it)->fix_base->fb_id;
  pdb_id id = *id_inout;

  /*  Find *id_inout or the next larger id.
   */
  if (it->it_n == 0 || id > fix_id[it->it_n - 1]) return GRAPHD_ERR_NO;

  if (id <= fix_id[0]) {
    *off_out = 0;
    *id_inout = fix_id[0];

    return 0;
  }

  /*  Find the same or larger.
   */

  {
    size_t end = it->it_n;
    size_t start = 0;
    size_t off;
    unsigned long long endval = id;
    pdb_id my_id;

    for (;;) {
      off = start + (end - start) / 2;
      my_id = fix_id[off];

      if (my_id < id)
        start = ++off;

      else if (my_id > id) {
        end = off;
        endval = my_id;
      } else
        break;

      if (start >= end) {
        my_id = endval;
        break;
      }
    }
    *id_inout = my_id;
    *off_out = off;
  }
  return 0;
}

static int fixed_iterator_find_loc(pdb_handle *pdb, pdb_iterator *it,
                                   pdb_id id_in, pdb_id *id_out,
                                   pdb_budget *budget_inout, char const *file,
                                   int line) {
  pdb_budget budget_in = *budget_inout;
  graphd_iterator_fixed *fix = it->it_theory;
  pdb_id my_id, found_id;
  size_t i;
  int err;

  *budget_inout -= pdb_iterator_check_cost(pdb, it);
  if (it->it_forward) {
    if (id_in >= it->it_high) {
      fix->fix_i = pdb_iterator_n(pdb, it);
      err = GRAPHD_ERR_NO;
      goto err;
    }

    my_id = (id_in < it->it_low ? it->it_low : id_in);
    err = fixed_search(pdb, it, &my_id, &fix->fix_i);
    if (err == 0) *id_out = my_id;
  } else {
    /* Backwards -- on or below.
     */
    if (id_in < it->it_low) {
      fix->fix_i = pdb_iterator_n(pdb, it);
      err = GRAPHD_ERR_NO;
      goto err;
    }

    found_id = my_id = (id_in >= it->it_high ? it->it_high - 1 : id_in);
    err = fixed_search(pdb, it, &found_id, &i);
    if (err == 0) {
      fix->fix_i = (it->it_n - 1) - i;
      if (found_id == my_id)
        *id_out = my_id;
      else {
        fix->fix_i++;
        if (i == 0) {
          err = GRAPHD_ERR_NO;
          goto err;
        }

        /* We need the next smaller. */

        *id_out = ofix(it)->fix_base->fb_id[i - 1];
      }
    }
  }

err:
  if (err == 0) {
    fix->fix_i++;
    pdb_rxs_log(pdb, "FIND %p fixed %llx %llx ($%lld)", (void *)it,
                (unsigned long long)id_in, (unsigned long long)*id_out,
                (long long)(budget_in - *budget_inout));
  } else if (err == GRAPHD_ERR_NO) {
    pdb_rxs_log(pdb, "FIND %p fixed %llx eof ($%lld)", (void *)it,
                (unsigned long long)id_in,
                (long long)(budget_in - *budget_inout));
  }

  pdb_iterator_account_charge_budget(pdb, it, find);
  return err;
}

static int fixed_iterator_next_loc(pdb_handle *pdb, pdb_iterator *it,
                                   pdb_id *id_out, pdb_budget *cost_inout,
                                   char const *file, int line) {
  graphd_iterator_fixed *fix = it->it_theory;
  size_t i;

  *cost_inout -= PDB_COST_FUNCTION_CALL;
  pdb_iterator_account_charge(pdb, it, next, 1, PDB_COST_FUNCTION_CALL);

  if (fix->fix_i >= it->it_n) {
    pdb_rxs_log(pdb, "NEXT %p fixed EOF ($%lld)", (void *)it,
                (long long)PDB_COST_FUNCTION_CALL);
    return GRAPHD_ERR_NO;
  }

  i = fix->fix_i++;
  if (!it->it_forward) i = it->it_n - (i + 1);
  *id_out = ofix(it)->fix_base->fb_id[i];

  pdb_rxs_log(pdb, "NEXT %p fixed %llx ($%lld)", (void *)it,
              (unsigned long long)*id_out, (long long)PDB_COST_FUNCTION_CALL);
  return 0;
}

static int fixed_iterator_check(pdb_handle *pdb, pdb_iterator *it, pdb_id id,
                                pdb_budget *cost_inout) {
  graphd_iterator_fixed *fix = it->it_theory;
  size_t i;
  pdb_id my_id;
  int err;

  *cost_inout -= pdb_iterator_check_cost(pdb, it);
  pdb_iterator_account_charge(pdb, it, check, 1, PDB_COST_FUNCTION_CALL);

  if (id < it->it_low || id >= it->it_high) {
    cl_log(fix->fix_base->fb_cl, CL_LEVEL_SPEW,
           "fixed_iterator_check: rejecting %llx; "
           "it's outside the range of [%llx...[%llx",
           (unsigned long long)id, (unsigned long long)it->it_low,
           (unsigned long long)it->it_high);
    pdb_rxs_log(pdb, "CHECK %p fixed %llx no ($%u)", (void *)it,
                (unsigned long long)id, (unsigned int)PDB_COST_FUNCTION_CALL);
    return GRAPHD_ERR_NO;
  }

  my_id = id;
  err = fixed_search(pdb, it, &my_id, &i);
  if (err != 0 || my_id != id) {
    if (err == 0 || err == GRAPHD_ERR_NO)
      pdb_rxs_log(pdb, "CHECK %p fixed %llx no ($%u)", (void *)it,
                  (unsigned long long)id, (unsigned int)PDB_COST_FUNCTION_CALL);

    cl_log(fix->fix_base->fb_cl, CL_LEVEL_SPEW,
           "fixed_iterator_check: %llx: no.", (unsigned long long)id);
    return err ? err : GRAPHD_ERR_NO;
  }
  pdb_rxs_log(pdb, "CHECK %p fixed %llx yes ($%u)", (void *)it,
              (unsigned long long)id, (unsigned int)PDB_COST_FUNCTION_CALL);
  return 0;
}

static int fixed_iterator_thaw_local_state(graphd_handle *g, char const **s_ptr,
                                           char const *e,
                                           graphd_iterator_fixed **fix_out,
                                           bool *forward_out) {
  pdb_handle *pdb = g->g_pdb;
  cl_handle *cl = g->g_cl;
  graphd_iterator_fixed *fix;
  unsigned long long n;
  pdb_id prev_id;
  int err = 0;
  size_t i;
  char const *s = *s_ptr;
  char flag;

  /*  In case of an error later, leave
   *  output variables in a defined empty
   *  state.
   */
  *fix_out = NULL;
  *forward_out = true;

  if (s < e && *s == '~') {
    s++;
    *forward_out = false;
  }

  /*  iterator resource tag?
   */
  if (s < e && *s == '[') {
    char const *stamp_s, *stamp_e;
    graphd_iterator_fixed_base *fb = NULL;

    if (e - s >= 7 && strncasecmp(s, "[cache:", 7) == 0) {
      err =
          pdb_iterator_util_thaw(pdb, &s, e, "[cache:@%s]", &stamp_s, &stamp_e);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_VERBOSE, "pdb_iterator_util_thaw", err,
                     "error while thawing cache "
                     "stamp \"%.*s\"",
                     (int)(e - (s + 1)), s + 1);
        return err;
      }

      /* Cached. */
      fb = graphd_iterator_resource_thaw(g, &stamp_s, stamp_e,
                                         &fixed_storable_type);
      if (fb == NULL) {
        cl_log(cl, CL_LEVEL_DEBUG,
               "fixed_iterator_thaw_local_state: "
               "MISS \"%.*s\"",
               (int)(stamp_e - stamp_s), stamp_s);
        *s_ptr = s;
        return GRAPHD_ERR_NO;
      }
    }
    fix = fixed_make(g, fb);

    /* If fixed_make succeeded, it took a link to fb;
     * we're now freeing the one we got back from thaw.
     */
    graphd_storable_unlink(fb);

    if (fix == NULL) {
      graphd_storable_unlink(&fb->fb_storable);
      return ENOMEM;
    }
  } else {
    graphd_iterator_fixed_base *fb;

    /*  Inlined values.
     */
    err = pdb_iterator_util_thaw(pdb, &s, e, "%llu%{extensions}:", &n,
                                 (pdb_iterator_property *)NULL);
    if (err != 0) {
      *s_ptr = s;
      return err;
    }

    fix = fixed_make(g, fb = fixed_base_make(g, n));
    graphd_storable_unlink(&fb->fb_storable);

    if (fix == NULL) return ENOMEM;

    prev_id = 0;
    flag = 0;
    for (i = 0; i < n; i++) {
      err =
          pdb_iterator_util_thaw(pdb, &s, e, "%{id}", fix->fix_base->fb_id + i);
      if (err != 0) {
        fixed_storable_destroy(fix);
        *s_ptr = s;
        return err;
      }
      if (flag == '+') fix->fix_base->fb_id[i] += prev_id;
      prev_id = fix->fix_base->fb_id[i];

      if (s < e && (*s == ',' || *s == '+')) flag = *s++;
    }
    fix->fix_base->fb_n = n;

    cl_log(cl, CL_LEVEL_DEBUG, "%s:%d: storable size of %p is %zu", __FILE__,
           __LINE__, (void *)fix->fix_base, fix->fix_base->fb_storable.gs_size);
    graphd_storable_size_add(g, fix->fix_base,
                             n * sizeof(*fix->fix_base->fb_id));
    cl_log(cl, CL_LEVEL_DEBUG, "%s:%d: storable size of %p is %zu", __FILE__,
           __LINE__, (void *)fix->fix_base, fix->fix_base->fb_storable.gs_size);
  }
  *fix_out = fix;
  *s_ptr = s;

  return 0;
}

static int fixed_iterator_freeze_masquerade_local_state(pdb_handle *pdb,
                                                        pdb_iterator *it,
                                                        cm_buffer *buf) {
  graphd_iterator_fixed *fix = it->it_theory;
  cl_handle *cl = fix->fix_base->fb_cl;
  int err = 0;
  size_t i;
  char const *sep = "";
  pdb_id *fix_id = ofix(it)->fix_base->fb_id;

  cl_assert(cl, it->it_theory != NULL);
  cl_assert(cl, ofix(it)->fix_base->fb_masquerade != NULL);

  if (!pdb_iterator_forward(pdb, it)) {
    err = cm_buffer_add_string(buf, "~");
    if (err != 0) return err;
  }

  if (pdb_iterator_n(pdb, it) >= GRAPHD_ITERATOR_FIXED_CACHE_MIN) {
    char sb[GRAPHD_ITERATOR_RESOURCE_STAMP_SIZE];

    /*  Just save the binary cache.
     */
    err = graphd_iterator_resource_store(ofix(it)->fix_base->fb_graphd,
                                         &ofix(it)->fix_base->fb_storable, sb,
                                         sizeof sb);
    if (err != 0) return err;

    if (strcmp(sb, "x") == 0) {
      cl_log(cl, CL_LEVEL_DEBUG,
             "fixed_iterator_freeze_masquerade_local_state: "
             "failed to freeze %llu entries.  Fix base mine %p ofix %p, size "
             "%zu/%zu\n",
             (unsigned long long)pdb_iterator_n(pdb, it), (void *)fix->fix_base,
             (void *)ofix(it)->fix_base, fix->fix_base->fb_storable.gs_size,
             ofix(it)->fix_base->fb_storable.gs_size);
    }
    return cm_buffer_sprintf(buf, "[cache:@%s]", sb);
  }

  err = cm_buffer_sprintf(buf, "%llu:", pdb_iterator_n(pdb, it));
  if (err != 0) return err;
  for (i = 0; i < pdb_iterator_n(pdb, it); i++) {
    err = cm_buffer_sprintf(buf, "%s%llu", sep, (unsigned long long)fix_id[i]);
    if (err != 0) return err;
    sep = ",";
  }
  return 0;
}

static int fixed_iterator_freeze(pdb_handle *pdb, pdb_iterator *it,
                                 unsigned int flags, cm_buffer *buf) {
  graphd_iterator_fixed *fix = it->it_theory;
  cl_handle *cl = ofix(it)->fix_base->fb_cl;
  int err = 0;
  size_t i;
  char const *sep = "";
  pdb_id const *fix_id = ofix(it)->fix_base->fb_id;
  char ibuf[200];
  size_t off = buf->buf_n;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");

  if (flags & PDB_ITERATOR_FREEZE_SET) {
    err = cm_buffer_add_string(buf, "fixed:");
    if (err != 0) goto buffer_error;

    if (fix->fix_base->fb_masquerade != NULL) {
      if ((err = cm_buffer_sprintf(buf, "(%s)",
                                   fix->fix_base->fb_masquerade)) != 0)
        goto buffer_error;
    } else {
      unsigned long prev_id;
      char const *sep = "";

      if ((err = cm_buffer_sprintf(buf, "%s%llu:",
                                   pdb_iterator_forward(pdb, it) ? "" : "~",
                                   pdb_iterator_n(pdb, it))) != 0)
        goto buffer_error;

      prev_id = 0;

      /* Grow as much as we're going to need.
       */
      snprintf(ibuf, sizeof ibuf, "%llu,",
               (unsigned long long)fix_id[it->it_n - 1]);
      err = cm_buffer_alloc(buf, strlen(ibuf) * it->it_n);
      if (err != 0) goto buffer_error;

      for (i = 0; i < it->it_n; i++) {
        err = cm_buffer_sprintf(buf, "%s%llu", sep,
                                (unsigned long long)fix_id[i]);
        if (err != 0) goto buffer_error;
        sep = ",";
        prev_id = fix_id[i];
      }
    }

    err = pdb_iterator_freeze_account(pdb, buf, it);
    if (err != 0) goto buffer_error;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    err = cm_buffer_sprintf(buf, "%s%zu", sep, fix->fix_i);
    if (err != 0) goto buffer_error;
    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_STATE) {
    err = cm_buffer_sprintf(buf, "%s", sep);
    if (err != 0) goto buffer_error;

    /*  If we have a masquerade, the expansion of the
     *  masquerade into fixed values is our local state -
     *  we can live without it, but it's faster if we
     *  have it.
     */
    if (fix->fix_base->fb_masquerade != NULL) {
      size_t off = cm_buffer_length(buf);

      err = fixed_iterator_freeze_masquerade_local_state(pdb, it, buf);
      if (err != 0) goto buffer_error;

      cl_log(cl, CL_LEVEL_VERBOSE,
             "fixed_iterator_freeze: "
             "got local state \"%s\"",
             cm_buffer_memory(buf) + off);
    }
  }
  if (err)
    cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
  else
    cl_leave(cl, CL_LEVEL_VERBOSE, "%.*s",
             (int)(cm_buffer_memory_end(buf) - (buf->buf_s + off)),
             buf->buf_s + off);
  return 0;

buffer_error:
  cl_log_errno(cl, CL_LEVEL_FAIL, "cm_buffer_add_string/sprintf", err, "it=%s",
               pdb_iterator_to_string(pdb, it, ibuf, sizeof ibuf));
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
  return err;
}

static int fixed_iterator_reset(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_fixed *fix = it->it_theory;

  fix->fix_i = 0;
  return 0;
}

static int fixed_iterator_clone(pdb_handle *pdb, pdb_iterator *it,
                                pdb_iterator **it_out) {
  pdb_iterator *it_orig = it->it_original;
  graphd_iterator_fixed *fix_out = NULL, *fix = it->it_theory;
  int err;

  PDB_IS_ITERATOR(fix->fix_base->fb_cl, it);
  PDB_IS_ORIGINAL_ITERATOR(fix->fix_base->fb_cl, it_orig);

  *it_out = NULL;
  cl_assert(fix->fix_base->fb_cl, it_orig->it_n > 0);

  fix_out = cm_malloc(fix->fix_base->fb_cm, sizeof(*fix));
  if (fix_out == NULL) {
    return errno ? errno : ENOMEM;
  }

  *fix_out = *fix;
  if (!pdb_iterator_has_position(pdb, it)) fix_out->fix_i = 0;

  if ((err = pdb_iterator_make_clone(pdb, it_orig, it_out)) != 0) {
    cm_free(fix->fix_base->fb_cm, fix_out);
    return err;
  }

  graphd_storable_link(&fix_out->fix_base->fb_storable);

  (*it_out)->it_theory = fix_out;
  (*it_out)->it_has_position = true;

  return 0;
}

static void fixed_iterator_finish(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_fixed *fix = it->it_theory;

  if (fix != NULL) {
    cm_handle *cm = fix->fix_base->fb_cm;

    fixed_destroy(fix);

    cm_free(cm, it->it_displayname);
    it->it_displayname = NULL;
    it->it_theory = NULL;
  }
  it->it_type = NULL;
  it->it_magic = 0;
}

static char const *fixed_iterator_to_string(pdb_handle *pdb, pdb_iterator *it,
                                            char *buf, size_t size) {
  pdb_id const *fix_id = ofix(it)->fix_base->fb_id;
  char b2[200], b3[200];

  b2[0] = b3[0] = '\0';
  if (it->it_n == 0) return "fixed[]";

  if (it->it_n > 1) {
    snprintf(b2, sizeof b2, ", %llx",
             (unsigned long long)fix_id[it->it_forward ? 1 : it->it_n - 2]);
    if (it->it_n > 2)
      snprintf(b3, sizeof b3, ", %llx",
               (unsigned long long)fix_id[it->it_forward ? 2 : it->it_n - 2]);
  }

  snprintf(buf, size, "%sfixed[%d: %llx%s%s%s]", it->it_forward ? "" : "~",
           (int)it->it_n,
           (unsigned long long)
               fix_id[pdb_iterator_forward(pdb, it) ? 0 : it->it_n - 1],
           b2, b3, it->it_n > 3 ? ", ..." : "");
  return buf;
}

/**
 * @brief Return the primitive summary for a fixed iterator.
 *
 * @param pdb		module handle
 * @param it		a fixed iterator
 * @param psum_out	out: summary
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int fixed_iterator_primitive_summary(pdb_handle *pdb, pdb_iterator *it,
                                            pdb_primitive_summary *psum_out) {
  /*  Defer to the original.  It may have a different type.
   */
  if (it->it_original != it)
    return pdb_iterator_primitive_summary(pdb, it->it_original, psum_out);

  psum_out->psum_locked = 0;
  psum_out->psum_result = PDB_LINKAGE_N;
  psum_out->psum_complete = false;

  return 0;
}

/**
 * @brief Has this iterator progressed beyond this value?
 *
 * @param pdb		module handle
 * @param it		iterator we're asking about
 * @param s		start of comparison value
 * @param e		end of comparison value
 * @param beyond_out	out: true if the most recently returned
 *			ID from this iterator was greater than
 *			(or, if it runs backward, smaller than)
 *			the parameter ID.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int fixed_iterator_beyond(pdb_handle *pdb, pdb_iterator *it,
                                 char const *s, char const *e,
                                 bool *beyond_out) {
  char buf[200];
  size_t off;
  pdb_id id, last_id;
  graphd_iterator_fixed *fix = it->it_theory;

  if (e - s != sizeof(id)) {
    *beyond_out = false;
    cl_log(fix->fix_base->fb_cl, CL_LEVEL_ERROR,
           "fixed_iterator_beyond: unexpected "
           "value size (%zu bytes; expected %zu)",
           (size_t)(e - s), sizeof(id));
    return GRAPHD_ERR_LEXICAL;
  }

  if (fix->fix_i >= it->it_n) {
    cl_log(fix->fix_base->fb_cl, CL_LEVEL_VERBOSE,
           "fixed_iterator_beyond: "
           "still at the beginning");
    *beyond_out = false;
    return 0;
  }
  memcpy(&id, s, sizeof(id));

  off = fix->fix_i - 1;
  if (!it->it_forward) off = it->it_n - (off + 1);
  last_id = fix->fix_base->fb_id[off];

  *beyond_out = (pdb_iterator_forward(pdb, it) ? id < last_id : id > last_id);

  cl_log(fix->fix_base->fb_cl, CL_LEVEL_VERBOSE,
         "fixed_iterator_beyond: "
         "%llx vs. last_id %llx in %s: %s",
         (unsigned long long)id, (unsigned long long)last_id,
         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         *beyond_out ? "yes" : "no");
  return 0;
}

static const pdb_iterator_type fixed_iterator_type = {
    "fixed",

    fixed_iterator_finish,
    fixed_iterator_reset,
    fixed_iterator_clone,
    fixed_iterator_freeze,
    fixed_iterator_to_string,

    fixed_iterator_next_loc,
    fixed_iterator_find_loc,
    fixed_iterator_check,
    pdb_iterator_util_statistics_none,

    NULL, /* idarray */
    fixed_iterator_primitive_summary,
    fixed_iterator_beyond,
    NULL, /* range-estimate */
    NULL, /* restrict */

    NULL, /* suspend */
    NULL  /* unsuspend */
};

/**
 * @brief Create an iterator that dispenses a fixed set of indices.
 *
 * @param g		server for whom we're doing this
 * @param n		number of returned values.
 * @param low		lowest included value
 * @param high		highest value that isn't included
 * @param forward	true if we'll be iterating from low to high.
 * @param it_out	assign the new iterator to this
 * @param file		calling code's filename
 * @param line		calling code's line
 *
 * @return ENOMEM on allocation error, 0 otherwise.
 */
static int fixed_wrap_loc(graphd_handle *g, unsigned long long low,
                          unsigned long long high, bool forward,
                          graphd_iterator_fixed *fix, pdb_iterator **it_out,
                          char const *file, int line) {
  cm_handle *cm = pdb_mem(g->g_pdb);

  *it_out = cm->cm_realloc_loc(cm, NULL, sizeof(**it_out), file, line);
  if (*it_out == NULL) return errno ? errno : ENOMEM;

  pdb_iterator_make_loc(g->g_pdb, *it_out, low, high, forward, file, line);

  (*it_out)->it_n = fix->fix_base->fb_n;
  (*it_out)->it_theory = fix;
  (*it_out)->it_type = &fixed_iterator_type;

  pdb_iterator_sorted_set(pdb, *it_out, true);

  return 0;
}

/**
 * @brief Create an iterator that dispenses a fixed set of indices.
 *
 * @param g		server for whom we're doing this
 * @param n		number of returned values.
 * @param low		lowest included value
 * @param high		highest value that isn't included
 * @param forward	true if we'll be iterating from low to high.
 * @param it_out	assign the new iterator to this
 * @param file		calling code's filename
 * @param line		calling code's line
 *
 * @return ENOMEM on allocation error, 0 otherwise.
 */
int graphd_iterator_fixed_create_loc(graphd_handle *g, size_t n,
                                     unsigned long long low,
                                     unsigned long long high, bool forward,
                                     pdb_iterator **it_out, char const *file,
                                     int line) {
  cm_handle *cm = pdb_mem(g->g_pdb);
  cl_handle *cl = pdb_log(g->g_pdb);
  graphd_iterator_fixed *fix;
  graphd_iterator_fixed_base *fb;
  int err;

  fix = fixed_make(g, fb = fixed_base_make(g, n));
  graphd_storable_unlink(&fb->fb_storable);

  if (fix == NULL) {
    int err = errno ? errno : ENOMEM;

    cm_free(cm, *it_out);
    *it_out = NULL;

    return err;
  }

  err = fixed_wrap_loc(g, low, high, forward, fix, it_out, file, line);
  if (err != 0) {
    fixed_destroy(fix);
    return err;
  }

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_fixed_create: it %p, fix %p, "
         "space for %lu in [%lld..[%lld%s [from %s:%d]",
         (void *)*it_out, (void *)fix, (unsigned long)n, low, high,
         forward ? "" : ", backwards", file, line);

  return 0;
}

/**
 * @brief Add a pdb_id to a fixed iterator.
 *
 *  The iterator's array of IDs is grown if needed.
 *  The array is kept sorted.
 *
 *  Duplicate insertions are ignored (but do not fail).
 *  Insertions out of the low..high range are also
 *  silently ignored.
 *
 * @param it	"fixed" iterator to add to.
 * @param id	ID to add.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_fixed_add_id(pdb_iterator *it, pdb_id id) {
  graphd_iterator_fixed *fix = it->it_theory;

  cl_assert(fix->fix_base->fb_cl, it == it->it_original);

  if (id < it->it_low || id >= it->it_high) {
    cl_log(fix->fix_base->fb_cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_fixed_add_id: ignoring %llx; "
           "it's outside the range of %llx...%llx",
           (unsigned long long)id, (unsigned long long)it->it_low,
           (unsigned long long)it->it_high);
    return 0;
  }

  if (it->it_n >= fix->fix_base->fb_m) {
    pdb_id *tmp;

    if (fix->fix_base->fb_m == 1 &&
        fix->fix_base->fb_id == fix->fix_base->fb_id_buf) {
      tmp = cm_malloc(fix->fix_base->fb_cm, (fix->fix_base->fb_m + 128) *
                                                sizeof(*fix->fix_base->fb_id));
      if (tmp != NULL) *tmp = *fix->fix_base->fb_id;
    } else {
      tmp = cm_realloc(
          fix->fix_base->fb_cm, fix->fix_base->fb_id,
          (fix->fix_base->fb_m + 128) * sizeof(*fix->fix_base->fb_id));
    }
    if (tmp == NULL) return ENOMEM;

    fix->fix_base->fb_id = tmp;
    fix->fix_base->fb_m += 128;
  }

  cl_assert(fix->fix_base->fb_cl, it->it_n < fix->fix_base->fb_m);
  if (it->it_n > 0 && fix->fix_base->fb_id[it->it_n - 1] >= id) {
    if (fix->fix_base->fb_id[it->it_n - 1] == id) return 0;
    fix->fix_base->fb_sort_me = true;
  }
  fix->fix_base->fb_id[it->it_n++] = id;

  return 0;
}

static int fix_id_compar(void const *A, void const *B) {
  return *(unsigned long long const *)A < *(unsigned long long const *)B
             ? -1
             : *(unsigned long long const *)A > *(unsigned long long const *)B;
}

static void fixed_optimize(pdb_iterator *it) {
  graphd_iterator_fixed *fix = it->it_theory;
  pdb_handle *pdb = fix->fix_base->fb_pdb;
  unsigned long long upper_bound = pdb_primitive_n(fix->fix_base->fb_pdb);
  size_t m;
  pdb_id *id_s, *id_e;

  if (it->it_n == 0 || upper_bound == 0) {
    pdb_iterator_null_become(pdb, it);
    return;
  }

  /*  Sort and unique the indices, cutting down
   *  the array in the process.
   */
  if (it->it_n > 1 && fix->fix_base->fb_sort_me) {
    pdb_id const *r, *r_e;
    pdb_id *w;

    qsort(fix->fix_base->fb_id, it->it_n, sizeof(*fix->fix_base->fb_id),
          fix_id_compar);

    r_e = fix->fix_base->fb_id + it->it_n;

    /*  Move w to the first ID that is identical to its
     *  predecessor.
     */
    for (w = fix->fix_base->fb_id + 1; w < r_e && w[-1] != w[0]; w++)
      ;
    if (w < r_e) {
      for (r = w; r < r_e; r++)
        if (*r != w[-1]) *w++ = *r;
      it->it_n = w - fix->fix_base->fb_id;
    }
  }

  /* Cut start and end according to low, high
   */
  id_s = fix->fix_base->fb_id;
  id_e = id_s + it->it_n;

  while (id_s < id_e) {
    if (*id_s >= it->it_low) break;
    id_s++;
  }
  if (it->it_high != PDB_ITERATOR_HIGH_ANY)
    while (id_e > id_s) {
      if (id_e[-1] < it->it_high) break;
      id_e--;
    }
  if (id_s == id_e) {
    pdb_iterator_null_become(pdb, it);
    return;
  }
  if (id_s < id_e && id_s != fix->fix_base->fb_id)
    memmove(fix->fix_base->fb_id, id_s, (char *)id_e - (char *)id_s);
  it->it_n = id_e - id_s;

  /*  Assign low, high according to actual values.
   */
  it->it_low = fix->fix_base->fb_id[0];
  it->it_high = fix->fix_base->fb_id[it->it_n - 1] + 1;

  for (m = 0; it->it_n > (1 << m); m++)
    ;

  /* Duplicate the n for the graphd_storable data. */
  fix->fix_base->fb_n = it->it_n;

  graphd_storable_size_set(
      fix->fix_base->fb_graphd, fix->fix_base,
      sizeof(*fix->fix_base) + it->it_n * sizeof(*fix->fix_base->fb_id));

  pdb_iterator_n_set(pdb, it, it->it_n);
  pdb_iterator_next_cost_set(pdb, it, PDB_COST_FUNCTION_CALL);
  pdb_iterator_check_cost_set(pdb, it, 1 + m / 10);
  pdb_iterator_find_cost_set(pdb, it, pdb_iterator_check_cost(pdb, it));
  pdb_iterator_statistics_done_set(pdb, it);
}

void graphd_iterator_fixed_create_commit(pdb_iterator *it) {
  graphd_iterator_fixed *fix = it->it_theory;

  cl_assert(fix->fix_base->fb_cl, it == it->it_original);
  cl_assert(fix->fix_base->fb_cl, !fix->fix_committed);

  fix->fix_committed = true;
  fixed_optimize(it);
}

void graphd_iterator_fixed_create_commit_n(pdb_iterator *it, size_t n,
                                           bool sorted) {
  graphd_iterator_fixed *fix = it->it_theory;
  pdb_handle *pdb = fix->fix_base->fb_pdb;

  if ((it->it_n += n) == 0) {
    pdb_iterator_null_become(pdb, it);
    return;
  }
  fix->fix_base->fb_sort_me |= !sorted;
  graphd_iterator_fixed_create_commit(it);
}

/**
 * @brief Create an iterator that dispenses a fixed set of indices.
 *
 * @param g		server for whom we're doing this
 * @param array		array of indexes; need not be sorted, faster if it is
 * @param array_n	number of elements in array
 * @param low		lowest value included in results; values below
 *				are dropped, even if they're in array.
 * @param high		PDB_ITERATOR_HIGH_ANY or lowest value *not*
 *				included in results; values on or above
 *				are dropped, even if they're in array
 * @param forward	true if results are ordered low through high
 * @param it_out	assign the iterator to here
 *
 * @return NULL on allocation error, otherwise a freshly minted
 *  	and structure.
 */
int graphd_iterator_fixed_create_array_loc(graphd_handle *g,
                                           pdb_id const *array, size_t array_n,
                                           unsigned long long low,
                                           unsigned long long high,
                                           bool forward, pdb_iterator **it_out,
                                           char const *file, int line) {
  int err;
  pdb_id const *array_e, *array_r;
  graphd_iterator_fixed *fix;

  if (array_n == 0) return pdb_iterator_null_create(g->g_pdb, it_out);

  err = graphd_iterator_fixed_create_loc(g, array_n, low, high, forward, it_out,
                                         file, line);
  if (err != 0) return err;

  array_r = array;
  array_e = array + array_n - 1;

  while (array_r < array_e && array_r[0] < array_r[1]) array_r++;

  fix = (*it_out)->it_theory;
  fix->fix_base->fb_sort_me = (array_r != array_e);

  memcpy(fix->fix_base->fb_id, array, array_n * sizeof(*array));
  (*it_out)->it_n = array_n;

  graphd_iterator_fixed_create_commit(*it_out);

  return 0;
}

/**
 * @brief Create an iterator that dispenses a fixed set of indices.
 *
 * @param g		server for whom we're doing this
 * @param array		array of GUIDs whose local indexes we want;
 *			need not be sorted, faster if it is
 * @param array_n	number of elements in array
 * @param low		lowest value included in results; values below
 *				are dropped, even if they're in array.
 * @param high		PDB_ITERATOR_HIGH_ANY or lowest value *not*
 *				included in results; values on or above
 *				are dropped, even if they're in array
 * @param forward	true if results are ordered low through high
 * @param it_out	assign the iterator to here
 *
 *
 * @return NULL on allocation error, otherwise a freshly minted
 *  	and structure.
 */
int graphd_iterator_fixed_create_guid_array(
    graphd_handle *g, graph_guid const *array, size_t array_n,
    unsigned long long low, unsigned long long high, bool forward,
    pdb_iterator **it_out) {
  int err;
  cl_handle *cl = pdb_log(g->g_pdb);

  cl_enter(cl, CL_LEVEL_SPEW, "enter");
  if (array_n == 0) {
    cl_leave(cl, CL_LEVEL_SPEW, "null");
    return pdb_iterator_null_create(g->g_pdb, it_out);
  }

  err = graphd_iterator_fixed_create(g, array_n, low, high, forward, it_out);
  if (err != 0) {
    cl_leave(cl, CL_LEVEL_SPEW, "unexpected error from fixed_create: %s",
             graphd_strerror(err));
    return err;
  }
  for (; array_n-- > 0; array++) {
    pdb_id id;

    err = pdb_id_from_guid(g->g_pdb, &id, array);

    if (err == GRAPHD_ERR_NO) continue;
    if (err != 0) {
      char buf[GRAPH_GUID_SIZE];

      pdb_iterator_destroy(g->g_pdb, it_out);
      cl_leave(cl, CL_LEVEL_SPEW,
               "unexpected error from pdb_id_from_guid(%s): "
               "%s",
               graph_guid_to_string(array, buf, sizeof buf),
               graphd_strerror(err));
      return err;
    }
    err = graphd_iterator_fixed_add_id(*it_out, id);
    if (err != 0) return err;
  }
  PDB_IS_ITERATOR(cl, *it_out);
  cl_leave(cl, CL_LEVEL_SPEW, "leave");

  graphd_iterator_fixed_create_commit(*it_out);
  return 0;
}

/**
 * @brief Set the position in an unfrozen iterator
 *
 *  This is called by modules that use masquerading
 *  to regenerate the contents of a fixed iterator.
 *
 * @param pdb		module handle
 * @param it		fixed iterator
 * @param off		position to set
 *
 * @return GRAPHD_ERR_NO if the number is out of range, or the
 *	iterator isn't a fixed iterator.
 * @return 0 on success
 */
int graphd_iterator_fixed_set_offset(pdb_handle *pdb, pdb_iterator *it,
                                     unsigned long long off) {
  graphd_iterator_fixed *fix;

  if (it->it_type != &fixed_iterator_type) {
    cl_handle *cl = pdb_log(pdb);
    char buf[200];

    cl_log(cl, CL_LEVEL_FAIL,
           "graphd_iterator_fixed_set_offset: not "
           "a fixed iterator! (%s)",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    return GRAPHD_ERR_NO;
  }

  fix = it->it_theory;
  if (off > pdb_iterator_n(pdb, it)) {
    cl_log(fix->fix_base->fb_cl, CL_LEVEL_FAIL,
           "graphd_iterator_fixed_set_offset: value %llu "
           "out of range (max: %llu)",
           off, (unsigned long long)pdb_iterator_n(pdb, it));
    return GRAPHD_ERR_NO;
  }
  fix->fix_i = off;

  cl_log(fix->fix_base->fb_cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_fixed_set_offset "
         "(%p): %zu of %zu (%lld)",
         (void *)it, fix->fix_i, (size_t)pdb_iterator_n(pdb, it),
         fix->fix_i >= pdb_iterator_n(pdb, it)
             ? -1ll
             : (long long)fix->fix_base->fb_id[fix->fix_i]);

  return 0;
}

/**
 * @brief Reconstitute a frozen iterator
 *
 * @param graphd	module handle
 * @param pit 		text form
 * @param pib 		unused
 * @param it_out	rebuild the iterator here.
 * @param file		caller's source file
 * @param int		caller's source line
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_fixed_thaw_loc(graphd_handle *graphd,
                                   pdb_iterator_text const *pit,
                                   pdb_iterator_base *pib,
                                   graphd_iterator_hint hints,
                                   cl_loglevel loglevel, pdb_iterator **it_out,
                                   char const *file, int line) {
  pdb_handle *pdb = graphd->g_pdb;
  cl_handle *cl = graphd->g_cl;
  unsigned long long n, off;
  size_t i;
  int err = 0;
  pdb_iterator_text sub_pit;
  char const *s, *e;
  char const *ord_s = NULL, *ord_e, *ordering = NULL;
  bool forward;
  char const *state_s, *state_e;
  graphd_iterator_fixed *fix = NULL;
  char const *mas_s, *mas_e;
  pdb_iterator_account *acc = NULL;
  bool needs_optimize = true;
  bool has_offset = false;

  s = pit->pit_set_s;
  e = pit->pit_set_e;
  cl_assert(cl, s && e);
  state_s = pit->pit_state_s;
  state_e = pit->pit_state_e;
  *it_out = NULL;

  /*  Is there a masquerade string?
   */
  if (s != NULL && s < e && *s == '(' && e[-1] == ')') {
    mas_s = s + 1;
    mas_e = e - 1;
  } else
    mas_s = mas_e = NULL;

  /*  Do we have a state?
   */
  if (state_s != NULL && state_s < state_e) {
    bool forward;

    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_fixed_thaw: "
           "local state \"%.*s\"",
           (int)(state_e - state_s), state_s);

    err = fixed_iterator_thaw_local_state(graphd, &state_s, state_e, &fix,
                                          &forward);

    if (err == 0) {
      cl_assert(cl, fix != NULL);
      err = fixed_wrap_loc(graphd, fix->fix_base->fb_id[0],
                           fix->fix_base->fb_id[fix->fix_base->fb_n - 1] + 1,
                           forward, fix, it_out, file, line);
      if (err != 0) {
        fixed_storable_destroy(fix);
        return err;
      }
      pdb_iterator_n_set(pdb, *it_out, fix->fix_base->fb_n);
      goto set_position;
    }
  }

  /*  We couldn't just reaccess or scan the "fix" state;
   *  derive it from the set instead, creating a new fixed base
   *  as necessary.
   */
  if (s != NULL && s < e && *s == '(') {
    graphd_iterator_hint fixed_thaw_hint = 0;

    err = pdb_iterator_util_thaw(pdb, &s, e, "%{(bytes)}", &sub_pit.pit_set_s,
                                 &sub_pit.pit_set_e);
    if (err != 0) return err;

    sub_pit.pit_position_s = sub_pit.pit_position_e = NULL;
    sub_pit.pit_state_s = sub_pit.pit_state_e = NULL;

    err = graphd_iterator_thaw(graphd, &sub_pit, pib, 0, loglevel, it_out,
                               &fixed_thaw_hint);
    if (err != 0) {
      cl_log_errno(
          cl, CL_LEVEL_FAIL, "graphd_iterator_thaw", err, "sub_pit=\"%.*s\"",
          (int)(sub_pit.pit_set_e - sub_pit.pit_set_s), sub_pit.pit_set_s);
      goto scan_error;
    }

    if ((*it_out)->it_type != &fixed_iterator_type) {
      char *mas;
      graphd_request *greq;
      pdb_iterator *my_fixed;

      greq = pdb_iterator_base_lookup(graphd->g_pdb, pib, "graphd.request");
      if (greq == NULL) {
        err = errno ? errno : EINVAL;
        cl_log_errno(cl, loglevel, "pdb_iterator_base_lookup", err,
                     "failed to look up request "
                     "context");
        pdb_iterator_destroy(pdb, it_out);
        return err;
      }

      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_iterator_fixed_thaw [%s:%d]:"
             "subiterator \"%.*s\" doesn't evaluate to a "
             "fixed iterator (cursor format change?) "
             "(recovering...)",
             file, line, (int)(sub_pit.pit_set_e - sub_pit.pit_set_s),
             sub_pit.pit_set_s);

      /*  Force it.
       */
      mas = cm_substr(graphd->g_cm, mas_s, mas_e);
      if (mas == NULL) {
        pdb_iterator_destroy(pdb, it_out);
        return ENOMEM;
      }

      err = graphd_iterator_fixed_create_from_iterator(greq, *it_out, mas,
                                                       &my_fixed);

      if (err == GRAPHD_ERR_TOO_HARD) {
        /* Go ahead and just try to use the non-fixed version of
         * this iterator
         */
        if ((err = pdb_iterator_reset(pdb, *it_out)) != 0) {
          return err;
        }
        return 0;
      }

      cm_free(graphd->g_cm, mas);
      pdb_iterator_destroy(pdb, it_out);

      if (err != 0) return err;
      *it_out = my_fixed;
      needs_optimize = false;
    }
  } else {
    unsigned long long prev_id;

    err = pdb_iterator_util_thaw(pdb, &s, e, "%{forward}%llu:", &forward, &n);
    if (err != 0) {
      cl_log(cl, CL_LEVEL_VERBOSE, "%s:%d", __FILE__, __LINE__);
      goto scan_error;
    }

    err = graphd_iterator_fixed_create_loc(graphd, n, 0, PDB_ITERATOR_HIGH_ANY,
                                           forward, it_out, file, line);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "graphd_iterator_fixed_create", err, "%llu",
                   n);
      return err;
    }

    fix = (*it_out)->it_theory;
    prev_id = 0;
    for (i = 0; i < n; i++) {
      unsigned long long id;
      char flag;

      while (s < e && (*s == ':' || *s == ',' || *s == '+')) s++;
      flag = s[-1];

      err = pdb_iterator_util_thaw(pdb, &s, e, "%llu", &id);
      if (err != 0) {
        pdb_iterator_destroy(pdb, it_out);
        cl_log(cl, loglevel,
               "graphd_iterator_fixed_thaw: "
               "expected numbers, got \"%.*s\"",
               (int)(e - s), s);
        return err;
      }
      if (flag == '+') id += prev_id;

      err = graphd_iterator_fixed_add_id(*it_out, id);
      if (err != 0) {
        cl_log_errno(cl, loglevel, "graphd_iterator_fixed_add_id", err,
                     "id=%lld", id);
        return err;
      }
      prev_id = id;
    }
    err = pdb_iterator_util_thaw(
        pdb, &s, e, "%{orderingbytes}%{account}%{extensions}%{end}", &ord_s,
        &ord_e, pib, &acc, (pdb_iterator_property *)NULL);
    if (err != 0) {
      pdb_iterator_destroy(pdb, it_out);
      return GRAPHD_ERR_LEXICAL;
    }

    pdb_iterator_account_set(pdb, *it_out, acc);

    if (ord_s != NULL) {
      ordering =
          graphd_iterator_ordering_internalize(graphd, pib, ord_s, ord_e);
      if (ordering != NULL) {
        pdb_iterator_ordering_set(pdb, *it_out, ordering);
        pdb_iterator_ordered_set(pdb, *it_out, true);
      } else {
        pdb_iterator_ordered_set(pdb, *it_out, false);
      }
    }
  }

set_position:
  fix = (*it_out)->it_theory;
  cl_assert(cl, fix != NULL);

  if (mas_s != NULL && fix->fix_base->fb_masquerade == NULL) {
    fix->fix_base->fb_masquerade = cm_substr(graphd->g_cm, mas_s, mas_e);
    if (fix->fix_base->fb_masquerade == NULL) {
      pdb_iterator_destroy(pdb, it_out);
      return ENOMEM;
    }
  }

  if (pit->pit_position_s != NULL &&
      pit->pit_position_s < pit->pit_position_e) {
    char const *s = pit->pit_position_s;
    char const *e = pit->pit_position_e;

    err = pdb_iterator_util_thaw(pdb, &s, e, "%llu%{extensions}%{end}", &off,
                                 (pdb_iterator_property *)NULL);
    if (err) {
      cl_log(cl, CL_LEVEL_VERBOSE, "%s:%d", __FILE__, __LINE__);
      goto scan_error;
    }
    has_offset = true;
  }

  if (state_s != NULL && state_s < state_e) {
    err = pdb_iterator_util_thaw(pdb, &state_s, state_e, "%{extensions}%{end}",
                                 (pdb_iterator_property *)NULL);
    if (err) {
      cl_log(cl, CL_LEVEL_VERBOSE, "%s:%d - state is %.*s", __FILE__, __LINE__,
             (int)(pit->pit_state_e - pit->pit_state_s), pit->pit_state_s);
      goto scan_error;
    }
  }

  if (needs_optimize) fixed_optimize(*it_out);

  if (has_offset) {
    err = graphd_iterator_fixed_set_offset(pdb, *it_out, off);
    if (err != 0) {
      char buf[200];
      cl_log_errno(cl, loglevel, "graphd_iterator_fixed_set_offset", err,
                   "off=%llu; it=%s", off,
                   pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
      goto scan_error;
    }
  }

  cl_assert(cl, (*it_out)->it_has_position);

  return 0;

scan_error:
  /* Destroy a patial result, if any.
   */
  pdb_iterator_destroy(pdb, it_out);

  cl_log(cl, loglevel, "graphd_iterator_fixed_thaw_loc: can't thaw \"%.*s\"",
         (int)(pit->pit_set_e - pit->pit_set_s), pit->pit_set_s);
  return err ? err : GRAPHD_ERR_SEMANTICS;
}

/**
 * @brief Is this a fixed iterator?  What are its values?
 *
 * @param pdb		module handle
 * @param it		iterator caller is asking about
 * @param values_out	if yes, store a pointer to the value array here.
 * @param n_out		if yes, store number of values here.
 *
 * @return 0 on success, a nonzero error code on error.
 */
bool graphd_iterator_fixed_is_instance(pdb_handle *pdb, pdb_iterator *it,
                                       pdb_id **values_out, size_t *n_out) {
  if (it->it_type == &fixed_iterator_type) {
    graphd_iterator_fixed const *fix = it->it_theory;

    if (values_out != NULL) *values_out = fix->fix_base->fb_id;
    if (n_out != NULL) *n_out = it->it_n;

    return true;
  }
  return false;
}

/**
 * @brief Intersect two sorted arrays of IDs.
 *
 *  The arrays are sorted in ascending order.
 *
 * @param a_base	beginning of first array
 * @param a_n		number of elements in the first array
 * @param b_base	beginning of second array
 * @param b_n		number of elements in the second array
 * @param id_inout	append intersection elements to this
 * @param id_n		append here and increment
 * @param id_m		maximum number of slots available
 *
 * @return 0 	 -- success
 * @return GRAPHD_ERR_MORE -- more than id_m elements became available.
 */
int graphd_iterator_fixed_intersect(cl_handle *cl,

                                    pdb_id *a_base, size_t a_n,

                                    pdb_id *b_base, size_t b_n,

                                    pdb_id *id_inout, size_t *id_n,
                                    size_t id_m) {
  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_fixed_intersect %p[%zu] "
         "vs. %p[%zu]; space for %zu (of which %zu are taken)",
         (void *)a_base, a_n, (void *)b_base, b_n, id_m, *id_n);

  for (;;) /* Tail recursion at the end of this loop. */
  {
    unsigned long long b_off, a_off;
    addb_gmap_id a_id, b_id;

    if (b_n < a_n) {
      /* B is smaller.  Swap a and b.
       */
      size_t tmp_n = b_n;
      pdb_id *tmp_base = b_base;

      b_n = a_n;
      b_base = a_base;

      a_n = tmp_n;
      a_base = tmp_base;
    }

    /*  A is smaller.   Are we out of things to intersect?
     */
    if (a_n <= 0) break;

    cl_assert(cl, a_base != NULL);
    cl_assert(cl, b_base != NULL);

    /*  The middle value in a's range.
     */
    a_off = a_n / 2;
    cl_assert(cl, a_off < a_n);

    a_id = a_base[a_off];

    cl_log(cl, CL_LEVEL_VERBOSE, "a[%llu] = %llu", a_off,
           (unsigned long long)a_id);

    /*  Project the middle value into b.
     */
    {
      size_t b_end = b_n;
      size_t b_start = 0;
      unsigned long long endval = a_id;

      for (;;) {
        b_off = b_start + (b_end - b_start) / 2;
        b_id = b_base[b_off];

        if (b_id < a_id)
          b_start = ++b_off;

        else if (b_id > a_id) {
          b_end = b_off;
          endval = b_id;
        } else {
          break;
        }

        if (b_start >= b_end) {
          b_id = endval;
          break;
        }
      }
    }

    /*  Recursion: (1) The entries up to b_off.
     */
    if (b_off > 0 && a_off > 0) {
      int err = graphd_iterator_fixed_intersect(cl, a_base, a_off, b_base,
                                                b_off, id_inout, id_n, id_m);
      if (err != 0) return err;
    }

    /*  The middle element
     */
    if (b_off < b_n && b_id == a_id) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_fixed_intersect "
             "found %llu at a=%llu, b=%llu",
             (unsigned long long)a_id, a_off, b_off);

      if (*id_n >= id_m) {
        return GRAPHD_ERR_MORE;
      }

      id_inout[(*id_n)++] = a_id;
      b_off++;
    } else
      cl_log(cl, CL_LEVEL_VERBOSE,
             "iterator_fixed_intersect: "
             "middle for a_id %llu is a=%llu, "
             "b=%llu",
             (unsigned long long)a_id, a_off, b_off);

    /* Recursion: (2) The entries after a_off (tail ~)
     */
    b_base += b_off;
    b_n -= b_off;

    a_base += a_off + 1;
    a_n -= a_off + 1;
  }
  return 0;
}

/**
 * @brief Set a string that this iterator disguises itself as.
 *
 *  The disguise is used inside the "set" part of the iterator.
 *
 * @param it	some iterator, hopefully a fixed iterator
 * @param mas	masquerade string
 *
 * @return true if yes (and *type_id_out is assigned the type),
 *	false otherwise.
 */
int graphd_iterator_fixed_set_masquerade(pdb_iterator *it, char const *mas) {
  graphd_iterator_fixed *fix;
  char *mas_dup;

  it = it->it_original;
  if (it->it_type != &fixed_iterator_type) return GRAPHD_ERR_NO;

  fix = it->it_theory;
  if ((mas_dup = cm_strmalcpy(fix->fix_base->fb_cm, mas)) == NULL)
    return errno ? errno : ENOMEM;

  if (fix->fix_base->fb_masquerade != NULL)
    cm_free(fix->fix_base->fb_cm, fix->fix_base->fb_masquerade);
  fix->fix_base->fb_masquerade = mas_dup;

  return 0;
}

/*  Given some other iterator, pull out its contents and turn them
 *  into a fixed iterator.
 *
 *  This can be used to force any iterator into fixed shape, even if
 *  it normally wouldn't optimize into a fixed iterator.
 *
 *  (Why would an iterator turn fixed in one case, and not in another?
 *   - changes in optimization strategy between releases
 *   - changes in optimization strategy due to expanding indexes;
 *     an index that was clearly small before may have had something
 *     added to it that now lifts it just outside the easily optimizable set.)
 *
 * @param greq		request we're doing this for
 * @apram it		the iterator we'd like to turn fixed
 * @param mas		masquerade string for that fixed iterator
 * @param it_out	out: store the new iterator here.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_fixed_create_from_iterator(graphd_request *greq,
                                               pdb_iterator *it,
                                               char const *mas,
                                               pdb_iterator **it_out) {
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  pdb_handle *pdb = graphd_request_graphd(greq)->g_pdb;
  pdb_id id;
  pdb_id id_buf[1024], *id_ptr = id_buf;
  size_t id_n = 0, id_m = sizeof(id_buf) / sizeof(*id_buf);
  int err;
  char buf[200];

  if ((err = pdb_iterator_reset(pdb, it)) != 0) {
    return err;
  }

  for (;;) {
    pdb_budget budget = 999999;

    do {
      err = pdb_iterator_next(pdb, it, &id, &budget);
      if (it->it_original->it_type == &fixed_iterator_type) break;
    } while (err == PDB_ERR_MORE && budget > 0);

    if (budget <= 0) return GRAPHD_ERR_TOO_HARD;

    if (it->it_original->it_type == &fixed_iterator_type) {
      pdb_iterator_call_reset(pdb, it);
      pdb_iterator_reset(pdb, it);

      err = pdb_iterator_clone(pdb, it, it_out);
      if (err == 0 && mas != NULL) {
        err = graphd_iterator_fixed_set_masquerade(*it_out, mas);
        if (err != 0) pdb_iterator_destroy(pdb, it_out);
      }
      goto err;
    }

    if (err == PDB_ERR_NO) break;
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "it=%s",
                   pdb_iterator_to_string(pdb, it, buf, sizeof buf));
      goto err;
    }
    if (id_n >= id_m) {
      pdb_id *tmp;
      if (id_ptr == id_buf) {
        tmp = cm_malloc(cm, sizeof(*id_ptr) * (id_m + 16 * 1024));
        if (tmp == NULL) {
          err = errno ? errno : ENOMEM;
          goto err;
        }
        memcpy(tmp, id_buf, sizeof(id_buf));
      } else {
        tmp = cm_realloc(cm, id_ptr, sizeof(*id_ptr) * id_m + (16 * 1024));
        if (tmp == NULL) {
          err = errno ? errno : ENOMEM;
          goto err;
        }
      }
      id_ptr = tmp;
      id_m += 16 * 1024;
    }
    id_ptr[id_n++] = id;
  }

  /*  Now that we have the array of IDs, create
   *  an iterator around it.
   */
  err = graphd_iterator_fixed_create_array(
      graphd_request_graphd(greq), id_ptr, id_n, it->it_low, it->it_high,
      pdb_iterator_forward(pdb, it), it_out);
  if (err == 0 && mas != NULL) {
    err = graphd_iterator_fixed_set_masquerade(*it_out, mas);
    if (err != 0) pdb_iterator_destroy(pdb, it_out);
  }

/* FALL THROUGH */
err:
  if (id_ptr != id_buf) cm_free(cm, id_ptr);
  return err;
}
