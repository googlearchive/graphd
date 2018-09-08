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

#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


static cm_list_offsets const pdb_iterator_offsets =
    CM_LIST_OFFSET_INIT(pdb_iterator, it_next, it_prev);

#define IS_LIT(s, e, lit)          \
  ((e) - (s) == sizeof(lit) - 1 && \
   strncasecmp((s), (lit), sizeof(lit) - 1) == 0)

static unsigned int bits(unsigned long long x) {
  unsigned int b = 0;
  while (x) {
    x >>= 1;
    b++;
  }
  return b;
}

/*  The "spread" of an iterator is one more than the distance
 *  between its lowest and highest possible value.
 */
unsigned long long pdb_iterator_spread(pdb_handle *pdb,
                                       pdb_iterator const *it) {
  unsigned long long high;

  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  if ((high = it->it_high) == PDB_ITERATOR_HIGH_ANY)
    high = pdb_primitive_n(pdb);

  return high > it->it_low ? high - it->it_low : 0;
}

/**
 * @brief Free an iterator "in place", without affecting the links to it.
 *	This is what should happen prior to a substitution.
 *
 * @param pdb	module handle
 * @param it	itertor to wipe out
 */
void pdb_iterator_dup(pdb_handle *pdb, pdb_iterator *it) {
  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  if (it != NULL) it->it_refcount++;
}

/**
 * @brief Unlink a clone from its original.
 *
 *  After the unlink,  the clone's "original" pointer is set to NULL.
 *  If this was the last link to the original, the original is destroyed.
 *
 *  This function is only called for clones.
 *
 * @param pdb	module handle
 * @param it	clone to be unlinked
 */
void pdb_iterator_unlink_clone(pdb_handle *pdb, pdb_iterator *it) {
  pdb_iterator *o;
  cl_handle *const cl = pdb->pdb_cl;

  cl_assert(cl, it != NULL);
  cl_assert(cl, it->it_clones == 0);
  o = it->it_original;

  cl_assert(cl, o != it);
  cl_assert(cl, o != NULL);
  cl_assert(cl, o->it_clones > 0);
  cl_assert(cl, o->it_refcount > 0);

  o->it_clones--;
  o->it_refcount--;

  if (o->it_refcount == 0) {
    o->it_refcount++;
    pdb_iterator_destroy(pdb, &o);
  }
  it->it_original = NULL;
}

void pdb_iterator_destroy(pdb_handle *pdb, pdb_iterator **it_ptr) {
  pdb_iterator *it;
  cl_handle *cl = pdb->pdb_cl;

  if (it_ptr == NULL || (it = *it_ptr) == NULL) return;
  /*
          cl_log(cl, CL_LEVEL_VERBOSE,
                  "pdb_iterator_destroy it %p (o=%p r=%d-%d; magic %lx)",
                  (void *)it,
                  (void *)it->it_original,
                  (int)it->it_refcount,
                  (int)it->it_clones,
                  (unsigned long)it->it_magic);
  */
  pdb_iterator_by_name_unlink(pdb, it);

  if (it->it_type != NULL) {
    PDB_IS_ITERATOR(pdb->pdb_cl, it);

    /*
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
    "pdb_iterator_destroy original %p (original %p)",
    (void *)it->it_original,
    (void *)it->it_original->it_original);
    */
    cl_assert(cl, it->it_refcount > 0);

    if (it->it_refcount-- <= 1) {
      if (it->it_original != it) pdb_iterator_unlink_clone(pdb, it);

      pdb_iterator_suspend_chain_out(pdb, it);
      pdb_iterator_chain_out(pdb, it);

      (it->it_type->itt_finish)(pdb, it);

      cm_free(pdb->pdb_cm, it);
    }
  } else {
    /*
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "pdb_iterator_destroy: plain free");
    */
    cm_free(pdb->pdb_cm, it);
  }
  *it_ptr = NULL;
}

pdb_budget pdb_iterator_bsearch_cost(unsigned long long n,
                                     unsigned long long n_per_tile,
                                     pdb_budget array_cost,
                                     pdb_budget element_cost) {
  if (n == 0) return PDB_COST_FUNCTION_CALL;

  if (n_per_tile < n)
    return array_cost * bits(n / n_per_tile) + element_cost * bits(n_per_tile);
  else
    return array_cost + element_cost * bits(n);
}

int pdb_iterator_next_nonstep_loc(pdb_handle *pdb, pdb_iterator *it,
                                  pdb_id *id_out, char const *file, int line) {
  int err;

  do {
    pdb_budget budget = PDB_COST_HIGH;

    err = pdb_iterator_next_loc(pdb, it, id_out, &budget, file, line);

  } while (err == PDB_ERR_MORE);

  return err;
}

int pdb_iterator_find_nonstep_loc(pdb_handle *pdb, pdb_iterator *it,
                                  pdb_id id_in, pdb_id *id_out,
                                  char const *file, int line) {
  int err;

  do {
    pdb_budget budget = PDB_COST_HIGH;

    err = pdb_iterator_find_loc(pdb, it, id_in, id_out, &budget, file, line);

  } while (err == PDB_ERR_MORE);

  return err;
}

int pdb_iterator_check_nonstep(pdb_handle *pdb, pdb_iterator *it, pdb_id id) {
  int err;

  do {
    pdb_budget budget = PDB_COST_HIGH;
    err = pdb_iterator_check(pdb, it, id, &budget);

  } while (err == PDB_ERR_MORE);

  return err;
}

/**
 * @brief Utility: initialize empty iterator variables.
 *
 * @param pdb		module handle
 * @param it_out	the iterator to initialize
 * @param low		low end of value range
 * @param high		high end of value range
 * @param forward	true if the iterator runs from
 *		  	low to high ids, false otherwise
 */
void pdb_iterator_make_loc(pdb_handle *pdb, pdb_iterator *it_out, pdb_id low,
                           pdb_id high, bool forward, char const *file,
                           int line) {
  memset(it_out, 0, sizeof(*it_out));

  it_out->it_magic = PDB_ITERATOR_MAGIC;
  it_out->it_n = (unsigned long long)-1;
  it_out->it_low = low;
  it_out->it_high = high;
  it_out->it_has_position = true;
  it_out->it_forward = forward;

  it_out->it_check_cost = PDB_COST_HIGH;
  it_out->it_next_cost = PDB_COST_HIGH;
  it_out->it_find_cost = PDB_COST_HIGH;
  it_out->it_sorted = false;

  it_out->it_original = it_out;
  it_out->it_refcount = 1;
  it_out->it_clones = 0;
  it_out->it_displayname = NULL;
  it_out->it_id = pdb_iterator_new_id(pdb);
  it_out->it_file = file;
  it_out->it_line = line;
  it_out->it_next = NULL;
  it_out->it_prev = NULL;

  pdb_iterator_chain_in(pdb, it_out);
}

void pdb_iterator_chain_finish(pdb_handle *const pdb,
                               pdb_iterator_chain *const chain,
                               char const *const name) {
  pdb_iterator *it;

  if (chain->pic_head == NULL) return;

  for (it = chain->pic_head; it != NULL; it = it->it_next)
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "%s: leftover iterator: %p [from: %s:%d]", name, (void *)it,
           it->it_file, it->it_line);

  cl_notreached(pdb->pdb_cl,
                "pdb_iterator_chain_finish: leftover iterators from %s", name);
}

void pdb_iterator_chain_clear(pdb_handle *pdb, pdb_iterator_chain *chain) {
  cl_assert(pdb->pdb_cl, chain != NULL);
  if (pdb->pdb_iterator_chain == chain)
    pdb->pdb_iterator_chain = &pdb->pdb_iterator_chain_buf;
}

void pdb_iterator_chain_set(pdb_handle *pdb, pdb_iterator_chain *chain) {
  cl_assert(pdb->pdb_cl, chain != NULL);
  pdb->pdb_iterator_chain = chain;
}

void pdb_iterator_chain_out(pdb_handle *pdb, pdb_iterator *it) {
  pdb_iterator_chain *it_chain;

  cl_assert(pdb->pdb_cl, it != NULL);
  cl_assert(pdb->pdb_cl, it->it_file != NULL);
  cl_assert(pdb->pdb_cl, it->it_chain != NULL);

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "chain out %p [%s:%d]", (void *)it,
         it->it_file, it->it_line);

  it_chain = it->it_chain;

  if (it->it_prev == NULL) cl_assert(pdb->pdb_cl, it_chain->pic_head == it);
  if (it->it_next == NULL) cl_assert(pdb->pdb_cl, it_chain->pic_tail == it);
  if (it->it_suspended) it->it_chain->pic_n_suspended--;

  cm_list_remove(pdb_iterator, pdb_iterator_offsets, &it_chain->pic_head,
                 &it_chain->pic_tail, it);
  it->it_next = NULL;
  it->it_prev = NULL;
  it->it_chain = NULL;
}

void pdb_iterator_chain_in(pdb_handle *pdb, pdb_iterator *it) {
  cl_assert(pdb->pdb_cl, it != NULL);
  cl_assert(pdb->pdb_cl, it->it_file != NULL);

  if (pdb->pdb_iterator_chain->pic_head == it)
    cl_notreached(pdb->pdb_cl,
                  "pdb_iterator_chain_in: "
                  "%p [%s:%d] is already the head of the chain!",
                  (void *)it, it->it_file, it->it_line);
  if (pdb->pdb_iterator_chain->pic_tail == it)
    cl_notreached(pdb->pdb_cl,
                  "pdb_iterator_chain_in: "
                  "%p [%s:%d] is already the tail of the chain!?",
                  (void *)it, it->it_file, it->it_line);

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "chain in %p [%s:%d]", (void *)it,
         it->it_file, it->it_line);
  cm_list_enqueue(pdb_iterator, pdb_iterator_offsets,
                  &pdb->pdb_iterator_chain->pic_head,
                  &pdb->pdb_iterator_chain->pic_tail, it);
  it->it_chain = pdb->pdb_iterator_chain;

  if (it->it_suspended) it->it_chain->pic_n_suspended++;
}

/**
 * @brief Replace an iterator without severing its clone/original ties.
 *
 *  The source of the move must not have non-selfrefs.  If it does,
 *  it is cloned.
 *
 *  Clones that point to the destination will, after the move, point
 *  to the replacement.  The replacement will still have a linkcount
 *  including the inherited clones.
 *
 * @param pdb		module handle
 * @param dest		the location that the old and new iterator occupy
 * @param source 	the iterator that will move into the old's location.
 */
int pdb_iterator_substitute(pdb_handle *pdb, pdb_iterator *dest,
                            pdb_iterator *source) {
  pdb_iterator saved;
  pdb_iterator *source_clone = NULL;
  cl_handle *const cl = pdb->pdb_cl;
  pdb_iterator_account *ia;
  pdb_iterator_chain *source_chain;

  /*
          cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
                  "pdb_iterator_substitute: replace %p:%s (orig=%p, ref=%d-%d)
     with %s (orig=%p, ref=%d-%d)",
                  (void *)dest,
                  pdb_iterator_to_string(pdb, dest, b1, sizeof b1),
                  (void *)dest->it_original, (int)dest->it_refcount,
     (int)dest->it_clones,
                  pdb_iterator_to_string(pdb, source, b2, sizeof b2),
                  (void *)source->it_original, (int)source->it_refcount,
     (int)source->it_clones
                  );
  */
  PDB_IS_ITERATOR(pdb->pdb_cl, source);

  cl_assert(cl, source->it_refcount >= 1);

  /*  What account will the result have?  By default,
   *  that of the source; but if the source had none,
   *  and the destination had one, let's go with the
   *  destination.
   */
  ia = source->it_account;
  if (ia == NULL) ia = dest->it_account;

  /*  "Finish" the destination.  This may free source
   *  a few times over, as a side effect, but we'll
   *  still have the link that we walked in with.
   */
  saved = *dest;
  pdb_iterator_by_name_unlink(pdb, dest);

  if (dest->it_type != NULL) {
    if (saved.it_original != dest) pdb_iterator_unlink_clone(pdb, dest);

    dest->it_type->itt_finish(pdb, dest);
    dest->it_type = NULL;

    pdb_iterator_suspend_chain_out(pdb, dest);
    pdb_iterator_chain_out(pdb, dest);

    cl_assert(cl, source->it_refcount >= 1);
  }

  /*  If the source has references left to it
   *  (other than the one we added),
   *  we can't move it,
   *  and will have to move a clone or duplicate instead.
   *
   *  - A clone if the destination itself has no clones.
   */
  cl_assert(cl, source->it_refcount >= 1);
  PDB_IS_ITERATOR(pdb->pdb_cl, source);

  if (source->it_refcount > 1) {
    int err;

    /* dest does not have clones.
     */
    cl_assert(cl, !dest->it_type || dest->it_clones == 0);

    err = pdb_iterator_clone(pdb, source, &source_clone);
    if (err != 0) return err;

    PDB_IS_ITERATOR(pdb->pdb_cl, source);
    PDB_IS_ITERATOR(pdb->pdb_cl, source_clone);

    pdb_iterator_destroy(pdb, &source);
    source = source_clone;
    PDB_IS_ITERATOR(pdb->pdb_cl, source);
  }

  pdb_iterator_suspend_save(pdb, source, &source_chain);
  pdb_iterator_chain_out(pdb, source);

  *dest = *source;

  pdb_iterator_chain_in(pdb, dest);
  pdb_iterator_suspend_restore(pdb, dest, source_chain);

  /* If the destination had references to it,
   * those apply to the replacement, too.
   */
  if (saved.it_type != NULL) {
    dest->it_refcount = saved.it_refcount;
    dest->it_clones = saved.it_clones;
  }

  /* Move the by-name reference to the new location.
   */
  if (dest->it_by_name != NULL) {
    dest->it_by_name->is_it = dest;
    source->it_by_name = NULL;
  }

  if (source->it_original == source)
    dest->it_original = dest;

  else if (source->it_original == dest) {
    /*  Selflinks are not linkcounted,
     *  but remote links are.  If we just
     *  turned a remote link into a selflink,
     *  we need to decrement our reference
     *  count!
     */
    dest->it_refcount--;
    cl_assert(cl, source->it_refcount >= 1);
  }

  PDB_IS_ITERATOR(pdb->pdb_cl, dest);
  /*
  if (source != NULL) cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
  "pdb_iterator_substitute: destroying source %p (orig=%p, ref=%d-%d) (may be
  wiped)",
  (void *)source,
  (void *)source->it_original,
  (int)source->it_refcount,
  (int)source->it_clones);
  */
  /* Free the leftover physical hull of source.
   */
  source->it_type = NULL;

  pdb_iterator_destroy(pdb, &source);

  /*  Restore (perhaps) the destination account.
   */
  pdb_iterator_account_set(pdb, dest, ia);

  return 0;
}

/**
 * @brief Refresh a pointer to a clone with a more
 * 	 accurate version of its original.
 *
 * @param pdb		module handle
 * @param it_ptr	clone to be refreshed
 */
int pdb_iterator_refresh_pointer(pdb_handle *pdb, pdb_iterator **it_ptr) {
  char buf[200];
  int err;
  pdb_iterator *new_clone;

  PDB_IS_ITERATOR(pdb->pdb_cl, *it_ptr);

  if ((*it_ptr)->it_id == (*it_ptr)->it_original->it_id) return PDB_ERR_ALREADY;

  err = pdb_iterator_clone(pdb, (*it_ptr)->it_original, &new_clone);
  if (err != 0) {
    cl_log_errno(
        pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err,
        "failed to clone %s",
        pdb_iterator_to_string(pdb, (*it_ptr)->it_original, buf, sizeof buf));
    return err;
  }
  PDB_IS_ITERATOR(pdb->pdb_cl, new_clone);
  pdb_iterator_destroy(pdb, it_ptr);
  *it_ptr = new_clone;

  return 0;
}

/**
 * @brief Refresh a clone with a more accurate version of its original.
 *
 * @param pdb		module handle
 * @param it		clone to be refreshed
 */
int pdb_iterator_refresh(pdb_handle *pdb, pdb_iterator *it) {
  char buf[200];
  int err;
  pdb_iterator *new_clone;

  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  if (it->it_id == it->it_original->it_id) return PDB_ERR_ALREADY;

  err = pdb_iterator_clone(pdb, it->it_original, &new_clone);
  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err,
                 "failed to clone %s",
                 pdb_iterator_to_string(pdb, it->it_original, buf, sizeof buf));
    return err;
  }
  PDB_IS_ITERATOR(pdb->pdb_cl, new_clone);

  err = pdb_iterator_substitute(pdb, it, new_clone);
  cl_assert(pdb->pdb_cl, err == 0);

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "pdb_iterator_refresh: refreshed %p from %p:%s", (void *)it,
         it->it_original,
         pdb_iterator_to_string(pdb, it->it_original, buf, sizeof buf));

  return 0;
}

static int pdb_iterator_initialize_clone_loc(pdb_handle *pdb,
                                             pdb_iterator *original_in,
                                             pdb_iterator *clone_out,
                                             char const *file, int line) {
  PDB_IS_ITERATOR(pdb->pdb_cl, original_in);
  PDB_IS_ORIGINAL_ITERATOR(pdb->pdb_cl, original_in);

  cl_assert(pdb->pdb_cl, clone_out != NULL);
  cl_assert(pdb->pdb_cl, original_in->it_original == original_in);
  cl_assert(pdb->pdb_cl, original_in->it_refcount >= 1);

  memcpy(clone_out, original_in, sizeof(*clone_out));

  clone_out->it_displayname = NULL;
  clone_out->it_refcount = 1;
  clone_out->it_clones = 0;
  clone_out->it_call_state = 0;
  clone_out->it_file = file;
  clone_out->it_line = line;
  clone_out->it_by_name = NULL;
  clone_out->it_chain = NULL;
  clone_out->it_next = NULL;
  clone_out->it_prev = NULL;
  clone_out->it_suspend_prev = NULL;
  clone_out->it_suspend_next = NULL;
  clone_out->it_suspended = false;

  pdb_iterator_chain_in(pdb, clone_out);
  if (pdb_iterator_suspend_is_chained_in(pdb, original_in))
    pdb_iterator_suspend_chain_in(pdb, clone_out);

  original_in->it_refcount++;
  original_in->it_clones++;

  cl_assert(pdb->pdb_cl, clone_out->it_original == original_in);

  return 0;
}

int pdb_iterator_make_clone_loc(pdb_handle *pdb, pdb_iterator *original_in,
                                pdb_iterator **clone_out, char const *file,
                                int line) {
  *clone_out = pdb->pdb_cm->cm_realloc_loc(pdb->pdb_cm, NULL,
                                           sizeof(**clone_out), file, line);
  if (*clone_out == NULL) {
    int err = errno ? errno : ENOMEM;
    char buf[200];

    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb->pdb_cm->cm_realloc_loc", err,
                 "iterator %s, %zu bytes [from %s:%d]",
                 pdb_iterator_to_string(pdb, original_in, buf, sizeof buf),
                 sizeof(**clone_out), file, line);
    return err;
  }

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "pdb_iterator_make_clone: %p -> %p",
         original_in, (void *)*clone_out);

  pdb_iterator_initialize_clone_loc(pdb, original_in, *clone_out, file, line);
  return 0;
}

int pdb_iterator_new_id(pdb_handle *pdb) { return pdb->pdb_iterator_id++; }

/**
 * @brief initialize an empty iterator.
 *
 *  Once an iterator has been initialized, it is safe to redundantly
 *  free it with pdb_iterator_all_finish().
 *  If iterated over, it will return no records.
 *
 * @param it_out	the iterator to initialize
 */
void pdb_iterator_initialize(pdb_iterator *it_out) {
  memset(it_out, 0, sizeof(*it_out));
  it_out->it_type = NULL;
}

int pdb_iterator_thaw(pdb_handle *pdb, pdb_iterator_text const *pit,
                      pdb_iterator_base *pib, pdb_iterator **it_out) {
  char const *col;
  char const *s = pit->pit_set_s;
  char const *e = pit->pit_set_e;
  pdb_iterator_text p;

  if ((col = memchr(s, ':', e - s)) == NULL || col - s < 1 || !isascii(*s)) {
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
           "pdb_iterator_thaw: expected \"prefix:\", got "
           "\"%.*s\"",
           (int)(e - s), s);
    return PDB_ERR_SYNTAX;
  }
  p = *pit;
  p.pit_set_s = col + 1;

  switch (*s) {
    case 'a':
      if (IS_LIT(s, col, "all"))
        return pdb_iterator_all_thaw(pdb, &p, pib, it_out);
      break;
    case 'b':
      if (IS_LIT(s, col, "bgmap"))
        return pdb_iterator_bgmap_thaw(pdb, &p, pib, it_out);
      break;
    case 'g':
      if (IS_LIT(s, col, "gmap"))
        return pdb_iterator_gmap_thaw(pdb, &p, pib, it_out);
      break;

    case 'h':
      if (IS_LIT(s, col, "hmap"))
        return pdb_iterator_hmap_thaw(pdb, &p, pib, it_out);
      break;

    case 'n':
      if (IS_LIT(s, col, "null"))
        return pdb_iterator_null_thaw(pdb, &p, pib, it_out);
      break;
  }
  cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
         "pdb_iterator_thaw: unrecognized prefix \"%.*s:\"", (int)(col - s), s);
  return PDB_ERR_SYNTAX;
}

static void shrink_boundary_offsets(pdb_iterator *it, addb_idarray *id,
                                    pdb_id low, pdb_id high,
                                    unsigned long long *s,
                                    unsigned long long *e) {
  int err;

  if (low > it->it_low) {
    unsigned long long new_s;
    pdb_id found;

    err = addb_idarray_search(id, *s, *e, low, &new_s, &found);
    if (err == 0) *s = new_s;
  }

  if (high < it->it_high) {
    unsigned long long new_e;
    pdb_id found;

    err = addb_idarray_search(id, *s, *e, high, &new_e, &found);
    if (err == 0) *e = new_e;
  }
}

/**
 * @brief Intersect two iterators, yielding a number of entries
 *  	below a predictable maximum.
 *
 * @param pdb		database handle
 * @param a		first iterator to intersect
 * @param b		second iterator to intersect
 * @param budget_inout	in/out: budget
 * @param id_out	out: the results
 * @param n_out		out: number of occupied slots
 * @param m		in: number of slots in *id_out
 *
 * @return 0 on success
 * @return PDB_ERR_MORE if one or both iterators didn't lend
 * 	themselves to fast intersects
 * @return other nonzero errors on system error.
 */
int pdb_iterator_intersect_loc(pdb_handle *pdb, pdb_iterator *a,
                               pdb_iterator *b, pdb_id low, pdb_id high,
                               pdb_budget *budget_inout, pdb_id *id_out,
                               size_t *n_out, size_t m, char const *file,
                               int line) {
  addb_idarray *a_id;
  unsigned long long a_s;
  unsigned long long a_e;

  addb_idarray *b_id;
  unsigned long long b_s;
  unsigned long long b_e;

  int err;
  char buff[200];

  if (budget_inout != NULL) *budget_inout -= PDB_COST_FUNCTION_CALL;

  if (pdb_iterator_null_is_instance(pdb, a) ||
      pdb_iterator_null_is_instance(pdb, b)) {
    *n_out = 0;
    return 0;
  }

  /*  Shrink low, high to the operator boundaries.
   */
  if (low < a->it_low) low = a->it_low;
  if (low < b->it_low) low = b->it_low;
  if (high > a->it_high) high = a->it_high;
  if (high > b->it_high) high = b->it_high;

  if (pdb_iterator_bgmap_is_instance(pdb, b, PDB_LINKAGE_ANY) &&
      pdb_iterator_gmap_is_instance(pdb, a, PDB_LINKAGE_ANY)) {
    err = pdb_iterator_idarray(pdb, a, &a_id, &a_s, &a_e);
    if (err) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_iterator_idarray", err,
                   "iterator %s says its a gmap but won't "
                   "give me an idarray",
                   pdb_iterator_to_string(pdb, a, buff, sizeof buff));
      return err;
    }

    if (budget_inout != NULL)
      *budget_inout -= pdb_iterator_n(pdb, a) * pdb_iterator_check_cost(pdb, b);

    return pdb_iterator_bgmap_idarray_intersect(pdb, b, a_id, low, high, id_out,
                                                n_out, m);
  }

  if (pdb_iterator_bgmap_is_instance(pdb, a, PDB_LINKAGE_ANY) &&
      pdb_iterator_gmap_is_instance(pdb, b, PDB_LINKAGE_ANY)) {
    err = pdb_iterator_idarray(pdb, b, &b_id, &b_s, &b_e);
    if (err) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_iterator_idarray", err,
                   "iterator %s says its a gmap but won't "
                   "give me an idarray",
                   pdb_iterator_to_string(pdb, b, buff, sizeof buff));
      return err;
    }

    if (budget_inout != NULL)
      *budget_inout -= pdb_iterator_n(pdb, b) * pdb_iterator_check_cost(pdb, a);

    return pdb_iterator_bgmap_idarray_intersect(pdb, a, b_id, low, high, id_out,
                                                n_out, m);
  }

  err = pdb_iterator_idarray(pdb, a, &a_id, &a_s, &a_e);
  if (err == 0) {
    shrink_boundary_offsets(a, a_id, low, high, &a_s, &a_e);

    err = pdb_iterator_idarray(pdb, b, &b_id, &b_s, &b_e);
    if (err == 0) {
      double a_n = a_e - a_s;
      double b_n = b_e - b_s;

      *n_out = 0;

      if (budget_inout != NULL)
        *budget_inout -= (a_n > b_n) ? b_n * log(a_n) : a_n * log(b_n);

      return addb_idarray_intersect(pdb->pdb_addb, a_id, a_s, a_e, b_id, b_s,
                                    b_e, id_out, n_out, m);
    }
    if (err != PDB_ERR_NO) return err;
  }

  return PDB_ERR_MORE;
}

/**
 * @brief Intersect an iterator and a fixed set of indices.
 *
 * @param pdb 		opaque pdb module handle
 * @param a		one iterator
 * @param b_id		a fixed set of indices
 * @param b_n		number of indices pointed to by b_id.
 * @param id_out	out: a list of ids
 * @param n_out		out: # of occupied slots in that array
 * @param m		in: # of slots in the array
 *
 * @return 0 on success, a nonzero error code on error.
 * @return PDB_ERR_MORE if there is no fast solution.
 */
int pdb_iterator_fixed_intersect_loc(pdb_handle *pdb, pdb_iterator *a,
                                     pdb_id *b_id, size_t b_n, pdb_id *id_out,
                                     size_t *n_out, size_t m, char const *file,
                                     int line) {
  addb_idarray *a_ida;
  unsigned long long a_s, a_e;
  int err;

  if (b_n == 0 || a == NULL) {
  empty:
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_fixed_intersect: "
           "null result");
    *n_out = 0;
    return 0;
  }

  /*  Adjust the fixed array to accomodate the boundaries
   *  of the iterator.
   */
  while (b_n > 0 && a->it_low > *b_id) {
    b_id++;
    if (!--b_n) goto empty;
  }
  while (b_n > 0 && a->it_high <= b_id[b_n - 1])
    if (!--b_n) goto empty;

  err = pdb_iterator_idarray(pdb, a, &a_ida, &a_s, &a_e);
  if (err == 0) {
    if (a_s >= a_e) goto empty;
    *n_out = 0;
    return addb_idarray_fixed_intersect(pdb->pdb_addb, a_ida, a_s, a_e, b_id,
                                        b_n, id_out, n_out, m);
  }

  /* The iterator didn't have an idarray.  Is it a bitmap?
   */
  err = pdb_iterator_bgmap_fixed_intersect(pdb, a, b_id, b_n, id_out, n_out, m);
  if (err != PDB_ERR_NOT_SUPPORTED) return err;

  return PDB_ERR_MORE;
}

/*
 * Is it an interator for the gmap 'linkage'? This checks for both
 * a gmap or bgmap representation.
 */

/* Callers should probably look at the primitive summary instead. --ju
 */
bool pdb_iterator_xgmap_is_instance(pdb_handle *pdb, pdb_iterator *it,
                                    int linkage) {
  return pdb_iterator_gmap_is_instance(pdb, it, linkage) ||
         pdb_iterator_bgmap_is_instance(pdb, it, linkage);
}

int pdb_iterator_freeze_intro(cm_buffer *buf, pdb_iterator *it,
                              char const *name) {
  char const *fwd = it->it_forward ? "" : "~";

  return it->it_high == PDB_ITERATOR_HIGH_ANY
             ? cm_buffer_sprintf(buf, "%s:%s%llu", name, fwd, it->it_low)
             : cm_buffer_sprintf(buf, "%s:%s%llu-%llu", name, fwd, it->it_low,
                                 it->it_high);
}

int pdb_iterator_freeze_ordering(pdb_handle *pdb, cm_buffer *buf,
                                 pdb_iterator *it) {
  if (it != NULL && buf != NULL && pdb_iterator_ordering(pdb, it) != NULL &&
      (!pdb_iterator_statistics_done(pdb, it) ||
       (pdb_iterator_ordered_valid(pdb, it) && pdb_iterator_ordered(pdb, it))))

    return cm_buffer_sprintf(buf, "[o:%s]", pdb_iterator_ordering(pdb, it));

  return 0;
}

int pdb_iterator_freeze_account(pdb_handle *pdb, cm_buffer *buf,
                                pdb_iterator *it) {
  if (it != NULL && buf != NULL && it->it_account != NULL)

    return cm_buffer_sprintf(buf, "[a:%zu]", it->it_account->ia_id);

  return 0;
}

int pdb_iterator_freeze_statistics(pdb_handle *pdb, cm_buffer *buf,
                                   pdb_iterator *it) {
  if (!pdb_iterator_statistics_done(pdb, it))
    return cm_buffer_add_string(buf, "-");

  else if (pdb_iterator_find_cost(pdb, it))
    return cm_buffer_sprintf(buf, "%lld:%lld+%lld:%lld",
                             (long long)pdb_iterator_check_cost(pdb, it),
                             (long long)pdb_iterator_next_cost(pdb, it),
                             (long long)pdb_iterator_find_cost(pdb, it),
                             (long long)pdb_iterator_n(pdb, it));
  else
    return cm_buffer_sprintf(buf, "%lld:%lld:%lld",
                             (long long)pdb_iterator_check_cost(pdb, it),
                             (long long)pdb_iterator_next_cost(pdb, it),
                             (long long)pdb_iterator_n(pdb, it));
}

/**
 * @brief If it's easy to return a single ID for this iterator, do so.
 *
 *  The call has the side effect of resetting the iterator to the
 *  start of its range.
 *
 * @param pdb		handle
 * @param it		iterator in question
 * @param id_out	out: the id.
 *
 * @return 0 if the iterator is a single ID, and knows it;
 * 	PDB_ERR_MORE if we odn't know enough to tell quickly
 * 	PDB_ERR_TOO_MANY if it's more than one
 *	PDB_ERR_NO if it's empty
 *	other nonzero error codes on unexpected error.
 */
int pdb_iterator_single_id(pdb_handle *pdb, pdb_iterator *it, pdb_id *id_out) {
  pdb_id id;
  pdb_budget budget = 50;
  int err, reset_err;
  char buf[200];

  if (!pdb_iterator_n_valid(pdb, it) ||
      !pdb_iterator_next_cost_valid(pdb, it) ||
      pdb_iterator_next_cost(pdb, it) >= budget) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_single_id (%s): too complicated",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    return PDB_ERR_MORE;
  }

  if (pdb_iterator_n(pdb, it) > 1) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_single_id (%s): n = %llu",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf),
           (unsigned long long)pdb_iterator_n(pdb, it));
    return PDB_ERR_TOO_MANY;
  }
  if ((err = pdb_iterator_reset(pdb, it)) != 0 ||
      (err = pdb_iterator_next(pdb, it, id_out, &budget)) != 0) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_single_id (%s): reset/next fails: %s",
           pdb_iterator_to_string(pdb, it, buf, sizeof buf),
           pdb_xstrerror(err));
    return err;
  }

  err = pdb_iterator_next(pdb, it, &id, &budget);

  /*  Whatever the outcome of the ``next'' call,
   *  unconditionally reset the iterator we're
   *  operating on.
   */
  reset_err = pdb_iterator_reset(pdb, it);
  if (reset_err != 0) {
    /*  That reset really should never fail.
     */
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_iterator_reset", reset_err,
                 "it=%s (next err=%s)",
                 pdb_iterator_to_string(pdb, it, buf, sizeof buf),
                 strerror(err));
    return reset_err;
  }

  /*  We want the second ``next'' to have failed with
   *  PDB_ERR_NO. If it did, we're a single-element iterator.
   */
  if (err != PDB_ERR_NO) {
    if (err == 0)
      cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
             "pdb_iterator_single_id (%s): more than "
             "one element",
             pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    else
      /* E.g., PDB_ERR_MORE if it unexpectedly
       * takes too long to determine if there
       * was a second ID.
       */
      cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
             "pdb_iterator_single_id (%s): unexpected "
             "error in second \"next\" call: %s",
             pdb_iterator_to_string(pdb, it, buf, sizeof buf), strerror(err));

    return (err == 0) ? PDB_ERR_TOO_MANY : err;
  }

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "pdb_iterator_single_id (%s): ok (%llx)",
         pdb_iterator_to_string(pdb, it, buf, sizeof buf), (long long)*id_out);
  return 0;
}

int pdb_iterator_scan_forward_low_high(cl_handle *cl, char const *who,
                                       char const **s_ptr, char const *e,
                                       bool *forward_out,
                                       unsigned long long *low_out,
                                       unsigned long long *high_out) {
  char const *s0 = *s_ptr;
  int err;

  *forward_out = true;
  if (*s_ptr < e && **s_ptr == '~') {
    *forward_out = false;
    ++*s_ptr;
  }

  if ((err = pdb_scan_ull(s_ptr, e, low_out)) != 0) {
    cl_log(cl, CL_LEVEL_FAIL, "%s: expected \"low[-high]:\", got \"%.*s\": %s",
           who, (int)(e - s0), s0, strerror(err));
    return err;
  }
  *high_out = PDB_ITERATOR_HIGH_ANY;
  if (*s_ptr < e && **s_ptr == '-') {
    (*s_ptr)++;
    if ((err = pdb_scan_ull(s_ptr, e, high_out)) != 0) {
      cl_log(cl, CL_LEVEL_FAIL, "%s: expected \"low-high\", got \"%.*s\": %s",
             who, (int)(e - s0), s0, strerror(err));
      return err;
    }
  }
  if (*s_ptr >= e || **s_ptr != ':') {
    cl_log(cl, CL_LEVEL_FAIL,
           "%s: trailing text after  \"low[-high]\": \"%.*s\"", who,
           (int)(e - s0), s0);
    return PDB_ERR_SYNTAX;
  }
  ++*s_ptr;

  return 0;
}

char const *pdb_unparenthesized(char const *s, char const *e, int ch) {
  unsigned int nparen = 0;
  bool in_string = false, escaped = false;

  while (s < e && (*s != ch || nparen != 0 || in_string)) {
    if (in_string) {
      if (escaped)
        escaped = false;
      else if (*s == '"')
        in_string = false;
      else
        escaped = (*s == '\\');
      s++;
    } else
      switch (*s++) {
        case '[':
        case '(':
          nparen++;
          break;
        case ']':
        case ')':
          nparen--;
          break;
        case '"':
          in_string = true;
          break;
        default:
          break;
      }
  }
  return s;
}

void pdb_iterator_parse(char const *s, char const *e, pdb_iterator_text *pit) {
  pit->pit_set_s = s;

  pit->pit_position_s = pit->pit_position_e = pit->pit_state_s =
      pit->pit_state_e = NULL;

  if ((pit->pit_set_e = pdb_unparenthesized(s, e, '/')) < e) {
    pit->pit_position_s = pit->pit_set_e + 1;
    pit->pit_position_e = pdb_unparenthesized(pit->pit_position_s, e, '/');
    if (pit->pit_position_e < e) {
      pit->pit_state_s = pit->pit_position_e + 1;
      pit->pit_state_e = e;
    }
  }
}

/*  Get the next (..) or [..] from a text.
 *
 *  @param s	beginning of the text
 *  @param e	end of text (pointer just past last char)
 *  @param seps pair of separators, e.g. "()" or "[]".
 *  @param out_s 	out: beginning of parenthesized fragment.
 *  @param out_e 	out: end of parentesized fragment
 *
 *  @return true if another fragment has been loaded into <pit>;
 *  false if we ran out.
 */
bool pdb_iterator_parse_next(char const *s, char const *e, char const *seps,
                             char const **out_s, char const **out_e) {
  if (*out_s == NULL) *out_e = s;

  *out_s = pdb_unparenthesized(*out_e, e, seps[0]);
  if (*out_s >= e) return false;

  *out_e = pdb_unparenthesized(*out_s + 1, e, seps[1]);
  return true;
}

/*  We're dealing with two structurally isomorphic trees {set} and {posstate}.
 *  {possttate} may be missing.
 *
 *  If present, elements of {posstate} have the form {pos/state}.
 *  If {posstate} is missing, NULLs are pulled from it.
 *
 *  Return true if another fragment has been loaded into <pit>;
 *  false if we ran out.
 */
bool pdb_iterator_parse_parallel_next(char const *set_s, char const *set_e,
                                      char const *posstate_s,
                                      char const *posstate_e, char const *seps,
                                      pdb_iterator_text *pit) {
  if (pit->pit_set_s == NULL) {
    pit->pit_set_e = set_s;
    pit->pit_position_e = posstate_s;
  }

  pit->pit_set_s = pdb_unparenthesized(pit->pit_set_e, set_e, seps[0]);
  if (pit->pit_set_s >= set_e) return false;
  pit->pit_set_e = pdb_unparenthesized(pit->pit_set_s + 1, set_e, seps[1]);

  if (posstate_s == NULL) {
    pit->pit_state_e = pit->pit_state_s = pit->pit_position_e =
        pit->pit_position_s = NULL;

    return true;
  }

  pit->pit_position_s =
      pdb_unparenthesized(pit->pit_position_s, posstate_e, seps[0]);
  if (pit->pit_position_s >= posstate_e) return false;

  pit->pit_state_e =
      pdb_unparenthesized(pit->pit_position_s + 1, posstate_e, seps[1]);
  pit->pit_position_e =
      pdb_unparenthesized(pit->pit_position_s, pit->pit_state_e, '/');

  pit->pit_state_s =
      pit->pit_position_e + (pit->pit_position_e < pit->pit_state_e);
  return true;
}

int pdb_iterator_base_initialize(pdb_handle *pdb, cm_handle *cm,
                                 pdb_iterator_base *pib) {
  int err;

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "pdb_iterator_base_initialize %p, cm=%p", (void *)pib, (void *)cm);

  pib->pib_cm = cm;

  err = cm_hashinit(cm, &pib->pib_by_name, sizeof(pdb_iterator_by_name), 16);
  if (err != 0) return err;

  return cm_hashinit(cm, &pib->pib_hash, sizeof(void *), 16);
}

void pdb_iterator_base_finish(pdb_handle *pdb, pdb_iterator_base *pib) {
  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "pdb_iterator_base_finish %p, cm=%p",
         (void *)pib, pib->pib_cm);
  cl_assert(pdb->pdb_cl, pib->pib_hash.h_cm == pib->pib_cm);
  cm_hashfinish(&pib->pib_hash);
}

void *pdb_iterator_base_lookup(pdb_handle *pdb, pdb_iterator_base *pib,
                               char const *name) {
  void **slot;

  slot = cm_haccess(&pib->pib_hash, void *, name, strlen(name));
  if (slot == NULL) return NULL;
  return *slot;
}

int pdb_iterator_base_delete(pdb_handle *pdb, pdb_iterator_base *pib,
                             char const *name) {
  void **slot;

  slot = cm_haccess(&pib->pib_hash, void *, name, strlen(name));
  if (slot == NULL) return PDB_ERR_NO;

  cm_hdelete(&pib->pib_hash, void *, slot);
  return 0;
}

int pdb_iterator_base_set(pdb_handle *pdb, pdb_iterator_base *pib,
                          char const *name, void *ptr) {
  void **slot;

  slot = cm_hnew(&pib->pib_hash, void *, name, strlen(name));
  if (slot == NULL) return errno ? errno : ENOMEM;
  *slot = ptr;
  return 0;
}

pdb_iterator_account *pdb_iterator_base_account_lookup(
    pdb_handle *pdb, pdb_iterator_base const *pib, size_t number) {
  if (pib->pib_account_resolve_callback == NULL) return NULL;

  return (*pib->pib_account_resolve_callback)(
      pib->pib_account_resolve_callback_data, pib, number);
}

void pdb_iterator_base_set_account_resolver(
    pdb_handle *pdb, pdb_iterator_base *pib,
    pdb_iterator_base_account_resolver *callback, void *callback_data) {
  (void)pdb;

  pib->pib_account_resolve_callback = callback;
  pib->pib_account_resolve_callback_data = callback_data;
}

void pdb_iterator_statistics_copy(pdb_handle *pdb, pdb_iterator *dst,
                                  pdb_iterator const *src) {
  if (src == NULL || dst == NULL) return;

  (void)pdb;

  dst = dst->it_original;
  src = src->it_original;

  dst->it_ordering = src->it_ordering;
  dst->it_ordered = src->it_ordered;
  dst->it_ordered_valid = src->it_ordered_valid;

  dst->it_next_cost = src->it_next_cost;
  dst->it_next_cost_valid = src->it_next_cost_valid;

  dst->it_check_cost = src->it_check_cost;
  dst->it_check_cost_valid = src->it_check_cost_valid;

  dst->it_find_cost = src->it_find_cost;
  dst->it_find_cost_valid = src->it_find_cost_valid;

  dst->it_sorted = src->it_sorted;
  dst->it_sorted_valid = src->it_sorted_valid;

  dst->it_forward = src->it_forward;

  dst->it_n_valid = src->it_n_valid;
  dst->it_n = src->it_n;

  dst->it_statistics_done = src->it_statistics_done;
}

int pdb_iterator_range_estimate_default(pdb_handle *pdb, pdb_iterator *it,
                                        pdb_range_estimate *range) {
  (void)pdb;

  range->range_low = it->it_low;
  range->range_high = it->it_high;

  range->range_n_exact = PDB_COUNT_UNBOUNDED;
  range->range_n_max =
      (it->it_high == PDB_ITERATOR_HIGH_ANY ? PDB_COUNT_UNBOUNDED
                                            : it->it_high - it->it_low);

  range->range_low_rising = false;
  range->range_high_falling = false;

  if (pdb_iterator_sorted(pdb, it)) {
    if (pdb_iterator_forward(pdb, it))
      range->range_low_rising = true;
    else
      range->range_high_falling = true;
  }
  return 0;
}

int pdb_iterator_restrict_default(pdb_handle *pdb, pdb_iterator *it,
                                  pdb_primitive_summary const *psum,
                                  pdb_iterator **it_out) {
  pdb_primitive_summary it_psum;
  int err;

  err = pdb_iterator_primitive_summary(pdb, it, &it_psum);
  if (err != 0) return PDB_ERR_ALREADY;

  return pdb_primitive_summary_allows(psum, &it_psum) ? PDB_ERR_ALREADY
                                                      : PDB_ERR_NO;
}
