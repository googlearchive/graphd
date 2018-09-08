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
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

GRAPHD_SABOTAGE_DECL;

/*  Is-a -- an iterator over things that are pointed to by values
 *  	from another iterator.
 *
 *	  [ our results ]<--[ subiterator ]
 *
 * 	The results are usually not sorted.
 *
 *  	We may have to keep state in order to make
 * 	sure that we don't return duplicates.
 */

/*  How many samples do we test to figure out the average
 *  number of pointers from the sub-entries to results?
 */
#define GRAPHD_ISA_N_SAMPLES 5

/*  If the subiterator is simple and has fewer than this many
 *  elements, evaluate it at creation time.
 */
#define GRAPHD_ISA_INLINE_N_THRESHOLD 300

/*  If the subiterator is simple and getting another element costs
 *  less than this, evaluate it at creation time.
 */
#define GRAPHD_ISA_INLINE_COST_THRESHOLD 50

/*  Maximum we're willing to spend on inlining.
 */
#define GRAPHD_ISA_INLINE_BUDGET_TOTAL \
  (GRAPHD_ISA_INLINE_N_THRESHOLD * GRAPHD_ISA_INLINE_COST_THRESHOLD)

/*  If the hashtable grows larger than this, and we have the option
 *  of doing sorted interesects, do sorted intersects instead of hashing.
 */
#define GRAPHD_STORABLE_HUGE(hint) \
  (1024 * (hint & GRAPHD_ITERATOR_ISA_HINT_CURSOR ? 512 : 2 * 1024))

/*  If a thawed iterator doesn't contain a hint, we treat it as this:
 */
#define GRAPHD_ITERATOR_ISA_HINT_DEFAULT GRAPHD_ITERATOR_ISA_HINT_CURSOR

/**
 * @brief A structure that manages the weeding out of duplicates.
 *
 *	The primitives we pull out of our subiterator are all unique,
 * 	but their linkage entries can point to the same primitive over and over.
 *
 *	Nevertheless, one primitive must only be returned once.
 *
 * 	There are two ways of doing this: a fast way that takes up memory,
 *	and a slow way that requires the subiterator to be sorted.
 */
typedef struct isa_duplicate_test {
  /**
   * @brief The current position in our cache of returned values.
   *	(The offset of the next value that will be returned;
   *	0 initially)
   */
  size_t dt_storable_position;

  /**
   * @brief The slow duplicate check uses the following components.
   *	This is a set of primitives whose linkage-entry
   *	points to the ID we're trying to return.
   */
  pdb_iterator *dt_fanin;

  /* @brief A clone of the sorted subiterator to intersect with.
   */
  pdb_iterator *dt_sub;

  /**
   * @brief Transaction state; used when interrupting
   *	and resuming the duplicate check.
   */
  int dt_state;

  /**
   * @brief The first possible source ID.  (The ID we're actually
   *	trying to return is at the other side of the source's
   * 	linkage pointer.)  The slow check tries to set the dt_id
   *	to the first intersection between dt_fanin and the
   *	subiterator.
   */
  pdb_id dt_id;

  /**
   * @brief In playing on-or-after against each other, the number
   *	of iterators that have left dt_id unchanged.  Once it
   *	hits two, we're done.
   */
  int dt_n_ok;

  enum {
    ISA_DT_METHOD_UNSPECIFIED = 0,
    ISA_DT_METHOD_STORABLE = 1,
    ISA_DT_METHOD_INTERSECT = 2
  } dt_method;

} isa_duplicate_test;

#define GRAPHD_ISA_MAGIC 0x08316558
#define GRAPHD_IS_ISA(cl, isa) \
  cl_assert(cl, (isa)->isa_magic == GRAPHD_ISA_MAGIC)

static const pdb_iterator_type isa_type;

/**
 * @brief Internal state for an is-a operator.
 */
typedef struct graphd_iterator_isa {
  unsigned long isa_magic;

  /**
   * @brief Containing graphd.
   */
  graphd_handle *isa_graphd;

  /**
   * @brief pdb's cm_handle.  Allocate and free through this.
   */
  cm_handle *isa_cm;

  /**
   * @brief pdb's cl_handle.  Log through this.
   */
  cl_handle *isa_cl;

  /**
   * @brief The linkage, one of PDB_LINKAGE...
   *
   *	Specifies *which* of the pointers in the primitives
   * 	returned by the subiterator points to the primitives
   *	returned by this iterator.
   */
  int isa_linkage;

  /**
   * @brief Subiterator.  Its primitives point to the ones
   *	returned by this iterator.
   */
  pdb_iterator *isa_sub;

  /**
   * @brief Subiterator linkage GUIDs.
   */
  pdb_primitive_summary isa_sub_psum;

  /**
   * @brief Cached linkage guids, converted to IDs.
   */
  pdb_id isa_sub_psum_id[PDB_LINKAGE_N];

  /**
   * @brief While working on a "next" or "check" call, the
    * 	id most recently returned by the subiterator.
   */
  pdb_id isa_sub_source;

  /**
   * @brief Are we positioned correctly?
   *	If true, the subiterator isa_sub is where it should be.
   *	If false, the sub-iterator needs an on-or-after
   * 	on isa_sub_source to get to the position <isa_sub_source>.
   */
  unsigned int isa_sub_has_position : 1;

  /**
   * @brief While working on a "check" call, the set of all
   *	linkage-pointers that point to the entry we're trying
   *	to check.
   */
  pdb_iterator *isa_fanin;

  /**
   * @brief Duplicate test for use while iterating over the
   * 	result set.
   */
  isa_duplicate_test isa_dup;

  /**
   * @brief Transaction state for sequential calls to
   *	statistics.
   */
  int isa_statistics_state;

  /**
   * @brief Clone of the sub-iterator for statistics.
   * 	Statistics can interleave with calls to "next" or "check",
   *	and we don't want to mess up the state for those.
   */
  pdb_iterator *isa_statistics_sub;

  /**
   * @brief Statistics-only: Sample results we're pulling out of
   *	the subiterator, then follow the linkage pointer
   *	(if there is one).
   *
   *      We keep them until the end of the statistics phase to
   *	be able to turn into a fixed iterator if there turn out
   * 	to be fewer than GRAPHD_ISA_N_SAMPLES.
   */
  pdb_id isa_sub_id[GRAPHD_ISA_N_SAMPLES];

  /**
   * @brief Number of sample-results we've found so far.
   */
  size_t isa_statistics_id_n;

  /**
   * @brief Number of results we got from the sub-iterator.
   */
  size_t isa_sub_id_trial_n;

  /**
   * If this is != PDB_ID_NONE, we have to loop until "next" returns
   * <isa_resume_id> before returning the next id.
   */
  pdb_id isa_resume_id;

  /**
   *  If this is != PDB_ID_NONE, we have to loop until there are at least
   *  that many elements in the cache.
   */
  pdb_id isa_resume_position;

  /*  The most recently returned id; becomes isa_resume_id after a thaw.
   */
  pdb_id isa_last_id;

  /*  Have we reached EOF yet?
   */
  unsigned int isa_eof : 1;

  /**
   * @brief If true, this iterator was thawed from a cursor.
   *	We should settle in for the long haul.
   */
  unsigned int isa_thawed : 1;

  /**
   * @brief Subprocess state: a temporary ID used in next or
   *	statistics that hasn't been dup-checked yet.
   */
  pdb_id isa_next_tmp;

  graphd_direction isa_direction;

  /* @brief Original only: which ids have been checked against
   *  	this iterator, and what was their result?
   */
  graphd_check_cache isa_ccache;

  /**
   * @brief Original only in an ISA that uses STORABLE: the storable
    * 	cache and a subiterator we use to feed it.
   */
  graphd_iterator_isa_storable *isa_cache;
  pdb_iterator *isa_cache_sub;

  /** @brief flags passed in at create-time.
   */
  graphd_iterator_isa_hint isa_hint;

} graphd_iterator_isa;

#define oisa_nocheck(it) ((graphd_iterator_isa *)(it)->it_original->it_theory)

#define oisa(it)                                                               \
  ((oisa_nocheck(it)->isa_magic == GRAPHD_ISA_MAGIC)                           \
       ? oisa_nocheck(it)                                                      \
       : (cl_notreached(                                                       \
              ((graphd_iterator_isa *)((it)->it_theory))->isa_cl,              \
              "ISA iterator %p has an original (%p) that is not an ISA", (it), \
              (it)->it_original),                                              \
          (graphd_iterator_isa *)NULL))

/*  The code below features three restartable functions.
 *
 *  If a restartable function returns PDB_ERR_MORE, the iterator call state
 *  will be set to values that causes the call to resume where it left off,
 *  if called again.
 *
 *  The iteration state always consists of:
 *  	- the sub-iterator-state ("its_substate") with a
 *		deallocator function.
 *	- i (the index into the subconstraint array)
 *	- a numeric "state" used to switch (...) to the location
 * 	  we returned from.
 *
 *  The RESUME_STATE(..) macro is a "case:" target to the
 *  initial switch.
 */
#undef RESUME_STATE
#define RESUME_STATE(it, st) \
  case st:                   \
    (it)->it_call_state = 0;

#undef LEAVE_SAVE_STATE
#define LEAVE_SAVE_STATE(it, st) \
  return ((it)->it_call_state = (st), cl_leave_suspend(cl, st), PDB_ERR_MORE)

#define SAVE_STATE_GOTO(it, st) \
  do {                          \
    (it)->it_call_state = (st); \
    goto suspend;               \
  } while (0)

#define cl_leave_suspend(cl, st)                                        \
  cl_leave(cl, CL_LEVEL_VERBOSE, "suspend [%s:%d; state=%d]", __FILE__, \
           __LINE__, (int)(st))

static void isa_cache_destroy(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_isa *isa = it->it_theory;
  graphd_handle *g = isa->isa_graphd;
  cl_handle *cl = g->g_cl;

  cl_assert(cl, it == it->it_original);

  cl_log(cl, CL_LEVEL_VERBOSE, "isa_cache_destroy isa=%p, isa->isa_cache=%p",
         (void *)isa, isa->isa_cache);

  if (isa->isa_cache != NULL) {
    graphd_storable_unlink(isa->isa_cache);
    isa->isa_cache = NULL;
  }
  pdb_iterator_destroy(pdb, &isa->isa_cache_sub);
}

static int isa_cache_create(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_isa *isa = it->it_theory;
  graphd_handle *g = isa->isa_graphd;
  cl_handle *cl = g->g_cl;
  int err;
  char buf[200];

  cl_assert(cl, it == it->it_original);

  if (isa->isa_cache != NULL) return 0;

  isa->isa_cache = graphd_iterator_isa_storable_alloc(g);
  if (isa->isa_cache == NULL) {
    err = errno ? errno : ENOMEM;
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_isa_storable_alloc", err,
                 "unexpected error");
    return err;
  }

  if (isa->isa_cache_sub != NULL) {
    err = pdb_iterator_reset(pdb, isa->isa_cache_sub);
    if (err != 0) {
      cl_log_errno(
          cl, CL_LEVEL_FAIL, "pdb_iterator_reset", err, "it=%s",
          pdb_iterator_to_string(pdb, isa->isa_cache_sub, buf, sizeof buf));

      pdb_iterator_destroy(pdb, &isa->isa_cache_sub);
      graphd_storable_destroy(isa->isa_cache);
      isa->isa_cache = NULL;

      return err;
    }
  }

  cl_log(cl, CL_LEVEL_VERBOSE,
         "isa_cache_create isa=%p, isa->isa_cache=%p, isa->isa_cache_sub=%p",
         (void *)isa, isa->isa_cache, (void *)isa->isa_cache_sub);
  return 0;
}

/**
 *  @brief Pull a value out of an IS-A iterator, without checking
 *	for overlap.
 *
 *  Returns:
 *	0 after adding an ID.
 *      PDB_ERR_MORE after running out of budget
 * 	GRAPHD_ERR_NO after running out of IDs.
 */
int graphd_iterator_isa_run_next(graphd_handle *g, pdb_iterator *it,
                                 pdb_iterator *sub, int linkage,
                                 size_t *sub_trials, pdb_id *id_out,
                                 pdb_budget *budget_inout, bool log_rxs) {
  int err;
  pdb_id id;
  cl_handle *const cl = g->g_cl;
  char buf[200];
  pdb_budget budget_in = *budget_inout;
  pdb_budget budget_in_rxs = *budget_inout;

  while (*budget_inout >= 0) {
    pdb_primitive pr;
    graph_guid guid;

    err = pdb_iterator_next(g->g_pdb, sub, &id, budget_inout);
    if (err != 0) {
      if (err == GRAPHD_ERR_NO) {
        cl_log(cl, CL_LEVEL_VERBOSE,
               "graphd_iterator_isa_run_next: "
               "done ($%lld)",
               (long long)(*budget_inout - budget_in));
      } else if (err == PDB_ERR_MORE)
        cl_log(cl, CL_LEVEL_VERBOSE,
               "graphd_iterator_isa_run_next: "
               "suspended in subiterator next ($%lld)",
               (long long)(budget_in - *budget_inout));
      else
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "sub=%s",
                     pdb_iterator_to_string(g->g_pdb, sub, buf, sizeof buf));
      return err;
    }

    /*  If the caller wants to, keep track of the number
     *  of completed "next" calls in the subiterator.
     *
     *  This helps in the statistics phase.
     */
    if (sub_trials != NULL) ++*sub_trials;

    if (id < it->it_low) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_isa_run_next: %llx is < low %llx",
             (unsigned long long)id, (unsigned long long)it->it_low);
      continue;
    }

    err = pdb_id_read(g->g_pdb, id, &pr);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err, "id=%llx",
                   (unsigned long long)id);
      if (err == GRAPHD_ERR_NO) continue;
      return err;
    }

    *budget_inout -= PDB_COST_PRIMITIVE;
    if (!pdb_primitive_has_linkage(&pr, linkage)) {
      pdb_primitive_finish(g->g_pdb, &pr);
      if (log_rxs) {
        cl_log(cl, CL_LEVEL_DEBUG, "RXS: ISA: %llx skip (no linkage) ($%lld)",
               (unsigned long long)id, budget_in_rxs - *budget_inout);
        budget_in_rxs = *budget_inout;
      }
      continue;
    }
    pdb_primitive_linkage_get(&pr, linkage, guid);
    pdb_primitive_finish(g->g_pdb, &pr);

    err = pdb_id_from_guid(g->g_pdb, &id, &guid);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "guid=%s",
                   graph_guid_to_string(&guid, buf, sizeof buf));
      if (err == GRAPHD_ERR_NO) continue;
      return err;
    }

    if (id < it->it_low || id >= it->it_high) {
      if (log_rxs) {
        cl_log(cl, CL_LEVEL_DEBUG,
               "RXS: ISA: %llx skip (result out of range) ($%lld)",
               (unsigned long long)id, budget_in_rxs - *budget_inout);
        budget_in_rxs = *budget_inout;
      }
      continue;
    }

    *id_out = id;
    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_iterator_isa_run_next: "
           "add %llx ($%lld)",
           (unsigned long long)id, (long long)(budget_in - *budget_inout));
    return 0;
  }

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_isa_run_next: "
         "suspended in main loop ($%lld)",
         (long long)(budget_in - *budget_inout));
  return PDB_ERR_MORE;
}

/**
 * @brief Return whether this iterator is capable of
 *  	 using ISA_DT_METHOD_INTERSECT.
 */
static bool isa_dup_can_switch_to_intersect(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_isa *isa = it->it_theory;

  return pdb_iterator_statistics_done(pdb, it) && isa->isa_sub != NULL &&
         pdb_iterator_statistics_done(pdb, isa->isa_sub) &&
         pdb_iterator_sorted(pdb, isa->isa_sub);
}

/**
 * @brief Return the total cost we assign to checking returns for
 *  	overlap in this round using intersects.
 */
static pdb_budget isa_dup_intersect_cost(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_isa *isa = it->it_theory;

  return pdb_iterator_find_cost(pdb, isa->isa_sub) + PDB_COST_GMAP_ARRAY +
         6 * PDB_COST_GMAP_ELEMENT;
}

/**
 * @brief Return the total cost we assign to checking returns for
 *  	overlap in this round using a hashtable.
 */
static pdb_budget isa_dup_storable_cost(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_isa *isa = it->it_theory;
  cl_handle *cl = isa->isa_cl;
  pdb_budget total = 0;
  size_t n, size;

  cl_assert(cl, pdb_iterator_check_cost_valid(pdb, it));
  cl_assert(cl, pdb_iterator_sorted_valid(pdb, isa->isa_sub));
  cl_assert(cl, pdb_iterator_sorted(pdb, isa->isa_sub));

  /*  n := the number of slots in the hashtable.
   *
   *  The hashtable takes 5 bytes per 8-bit slot - 4 byte for
   *  the slot address, 1 byte for the slot contents.
   *
   *  When thawing and restoring, the system saves at most
   *  GRAPHD_ITERATOR_RESOURCE_MAX btyes.
   */
  if (oisa(it)->isa_cache == NULL) {
    n = 0;
    size = 0;
  } else {
    n = graphd_iterator_isa_storable_nelems(oisa(it)->isa_cache);
    size = graphd_storable_size(oisa(it)->isa_cache);
  }
  if (size > GRAPHD_ITERATOR_RESOURCE_MAX) {
    /*  If the hashtable is too large to fit in the cache,
     *  it'll _never_ be successfully saved and restored.
     *  At that point, our cost for saving and restoring is
     *  that of N "next" calls.
     */
    total += n * pdb_iterator_next_cost(pdb, it);
  } else {
    /*  The hashtable has a cost for thawing and restoring,
     *  but it's relatively small.
     */
    total += n / (64 * 1024);
  }

/*  There's a chance that no save/restore ever happens in the
 *  lifetime of this cursor.  The chance is lower if it has
 *  happened before.
 */
#define CHANCE_OF_SPONTANEOUS_FREEZE_THAW 0.10

  if (!isa->isa_thawed) total *= CHANCE_OF_SPONTANEOUS_FREEZE_THAW;

  total += pdb_iterator_check_cost(pdb, it);
  return total;
}

/**
 * @brief Switch from hashtable to intersect mode.
 */
static void isa_dup_storable_switch_to_intersect(pdb_handle *pdb,
                                                 pdb_iterator *it) {
  graphd_iterator_isa *isa = it->it_theory;
  cl_handle *cl = isa->isa_cl;
  isa_duplicate_test *dt = &isa->isa_dup;
  char buf[200];

  cl_assert(cl, pdb_iterator_sorted_valid(pdb, isa->isa_sub));
  cl_assert(cl, pdb_iterator_sorted(pdb, isa->isa_sub));
  cl_assert(cl, it->it_original == it);
  cl_assert(cl, dt->dt_method == ISA_DT_METHOD_STORABLE);

  cl_log(cl, CL_LEVEL_VERBOSE, "isa_dup_storable_switch_to_intersect: %s",
         pdb_iterator_to_string(pdb, it, buf, sizeof buf));

  pdb_iterator_destroy(pdb, &oisa(it)->isa_cache_sub);
  if (oisa(it)->isa_cache != NULL) {
    graphd_storable_unlink(oisa(it)->isa_cache);
    oisa(it)->isa_cache = NULL;
  }

  dt->dt_state = 0;
  dt->dt_method = ISA_DT_METHOD_INTERSECT;
}

static int isa_sub_primitive_summary(pdb_handle *pdb, pdb_iterator *it) {
  int err;
  graphd_iterator_isa *isa = it->it_theory;
  int linkage;
  char buf[200];
  cl_handle *cl = isa->isa_cl;

  err = pdb_iterator_primitive_summary(pdb, isa->isa_sub, &isa->isa_sub_psum);
  if (err == GRAPHD_ERR_NO ||
      (err == 0 && isa->isa_sub_psum.psum_result != PDB_LINKAGE_N)) {
    /* Correct, but useless. */
    isa->isa_sub_psum.psum_locked = 0;
    return 0;
  } else if (err != 0) {
    cl_log_errno(isa->isa_cl, CL_LEVEL_FAIL, "pdb_iterator_primitive_summary",
                 err, "it=%s",
                 pdb_iterator_to_string(pdb, isa->isa_sub, buf, sizeof buf));
    return err;
  }
  {
    cl_log(cl, CL_LEVEL_VERBOSE, "isa_sub_primitive_summary: subiterator %s",
           pdb_iterator_to_string(pdb, isa->isa_sub, buf, sizeof buf));
  }
  {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "isa_sub_primitive_summary: primitive summary %s",
           pdb_primitive_summary_to_string(pdb, &isa->isa_sub_psum, buf,
                                           sizeof buf));
  }

  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++) {
    if (!(isa->isa_sub_psum.psum_locked & (1 << linkage)))
      isa->isa_sub_psum_id[linkage] = PDB_ID_NONE;
    else {
      err = pdb_id_from_guid(pdb, isa->isa_sub_psum_id + linkage,
                             isa->isa_sub_psum.psum_guid + linkage);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "guid=%s",
                     graph_guid_to_string(isa->isa_sub_psum.psum_guid + linkage,
                                          buf, sizeof buf));
        return err;
      }
    }
  }
  return 0;
}

static int isa_freeze_set(pdb_handle *pdb, unsigned long long low,
                          unsigned long long high, graphd_direction direction,
                          char const *ordering, pdb_iterator_account const *acc,
                          pdb_iterator *sub_it, int linkage,
                          graph_guid *type_guid,
                          graphd_iterator_isa_hint isa_hint, cm_buffer *buf) {
  char ibuf[200];
  int err;
  unsigned long long n;
  char dir[2];

  dir[1] = '\0';
  if ((direction == GRAPHD_DIRECTION_FORWARD && ordering == NULL) ||
      direction == GRAPHD_DIRECTION_ANY)
    dir[0] = '\0';
  else if (direction == GRAPHD_DIRECTION_BACKWARD)
    dir[0] = '~';
  else
    dir[0] = graphd_iterator_direction_to_char(direction);

  /*   isa: [~] LOW [-HIGH] : LINKAGE[+TYPEGUID]<-(SUB)
   */
  if ((n = pdb_primitive_n(pdb)) == 0)
    return cm_buffer_add_string(buf, "null:");

  err = (high == PDB_ITERATOR_HIGH_ANY
             ? cm_buffer_sprintf(buf, "isa:%s%llu:", dir, low)
             : cm_buffer_sprintf(buf, "isa:%s%llu-%llu:", dir, low, high));
  if (err != 0) return err;

  err = cm_buffer_sprintf(buf, "%.1s", pdb_linkage_to_string(linkage));
  if (err != 0) return err;

  if (type_guid != NULL && !GRAPH_GUID_IS_NULL(*type_guid)) {
    err = cm_buffer_sprintf(buf, "+%s",
                            graph_guid_to_string(type_guid, ibuf, sizeof ibuf));
    if (err != 0) return err;
  }

  err = cm_buffer_add_string(buf, "<-(");
  if (err != 0) return err;

  err = pdb_iterator_freeze(pdb, sub_it, PDB_ITERATOR_FREEZE_SET, buf);
  if (err != 0) return err;

  err = cm_buffer_add_string(buf, ")");
  if (err != 0) return err;

  if (ordering != NULL) {
    err = cm_buffer_sprintf(buf, "[o:%s]", ordering);
    if (err != 0) return err;
  }
  if (acc != NULL) {
    err = cm_buffer_sprintf(buf, "[a:%zu]", acc->ia_id);
    if (err != 0) return err;
  }
  if ((isa_hint & ~GRAPHD_ITERATOR_ISA_HINT_OPTIMIZE) !=
      GRAPHD_ITERATOR_ISA_HINT_DEFAULT) {
    err = cm_buffer_sprintf(buf, "[hint:%lu]", (unsigned long)isa_hint);
    if (err != 0) return err;
  }
  return 0;
}

/*  We're turning into a fixed iterator.  If the fixed iterator
 *  is very long, it may be easier to just remember how to generate
 *  it, and not remember the IDs in the iterator.
 */
static int isa_set_fixed_masquerade(pdb_handle *pdb, pdb_iterator *fix_it,
                                    int linkage, pdb_iterator *sub_it) {
  cm_buffer mq;
  cm_handle *cm = pdb_mem(pdb);
  int err;
  pdb_id *values_dummy;
  size_t n_dummy;

  /* Don't bother if it's small. */
  if (pdb_iterator_n(pdb, fix_it) <= 5) return 0;

  /* Don't bother if the subiterator is a fixed iterator,
   * too - the cursor will only get longer!
   */
  if (graphd_iterator_fixed_is_instance(pdb, sub_it, &values_dummy, &n_dummy))
    return 0;

  /*  Freeze our set definition.
   */
  cm_buffer_initialize(&mq, cm);
  if ((err = cm_buffer_add_string(&mq, "fixed-")) != 0 ||
      (err = isa_freeze_set(pdb, fix_it->it_low, fix_it->it_high,
                            pdb_iterator_forward(pdb, fix_it)
                                ? GRAPHD_DIRECTION_FORWARD
                                : GRAPHD_DIRECTION_BACKWARD,
                            pdb_iterator_ordering(pdb, fix_it),
                            fix_it->it_account, sub_it, linkage, NULL,
                            /* hint */ 0, &mq)) != 0) {
    cm_buffer_finish(&mq);
    return err;
  }
  err = graphd_iterator_fixed_set_masquerade(fix_it, cm_buffer_memory(&mq));

  /* GRAPHD_ERR_NO from graphd_iterator_fixed_set_masquerade means:
   * "I'm not an 'fixed'-iterator!"
   *  That's okay, we'll just take the unmasqueraded freeze, then.
   */
  if (err == GRAPHD_ERR_NO) err = 0;

  cm_buffer_finish(&mq);
  return err;
}

static int isa_become_small_set(graphd_handle *g, int linkage,
                                pdb_iterator *sub, unsigned long long low,
                                unsigned long long high,
                                graphd_direction direction,
                                char const *ordering, pdb_iterator **it_out) {
  int err;
  pdb_handle *pdb = g->g_pdb;
  cl_handle *cl = g->g_cl;
  pdb_budget budget = GRAPHD_ISA_INLINE_BUDGET_TOTAL;
  pdb_primitive pr;
  pdb_id *w;
  graph_guid guid;
  char buf[200];
  pdb_id sub_ids[GRAPHD_ISA_INLINE_BUDGET_TOTAL / PDB_COST_PRIMITIVE + 1];
  pdb_iterator *sub_clone;

  /*  Clone a working copy of the subiterator.
   */
  err = pdb_iterator_clone(pdb, sub, &sub_clone);
  if (err != 0) return err;

  w = sub_ids;
  while (w < sub_ids + sizeof(sub_ids) / sizeof(*sub_ids)) {
    if (budget <= PDB_COST_PRIMITIVE) {
      err = PDB_ERR_MORE;
      break;
    }
    budget -= PDB_COST_PRIMITIVE;

    /*  Read an ID from the subiterator.
     *  This will return GRAPHD_ERR_NO when we're done.
     */
    err = pdb_iterator_next(pdb, sub_clone, w, &budget);
    if (err != 0) {
      if (err == GRAPHD_ERR_NO || err == PDB_ERR_MORE) break;

      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next_nonstep", err,
                   "error reading from %s",
                   pdb_iterator_to_string(pdb, sub_clone, buf, sizeof buf));
      pdb_iterator_destroy(pdb, &sub_clone);
      return err;
    }

    /*  Get the primitive for the ID.
     */
    if ((err = pdb_id_read(pdb, *w, &pr)) == GRAPHD_ERR_NO) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err,
                   "can't read primitive for %llx "
                   "(ignored)",
                   (unsigned long long)*w);
      continue;
    } else if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err,
                   "cannot read subprimitive %llx", (unsigned long long)*w);
      pdb_iterator_destroy(pdb, &sub_clone);
      return err;
    }

    /*  Get the linkage GUID from the primitive.
     */
    if (!pdb_primitive_has_linkage(&pr, linkage)) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "pdb_iterator_next_nonstep: %llx "
             "doesn't have our linkage",
             (unsigned long long)*w);

      /*  No error; some primitives just
       *  don't have our linkage.  Skip them.
       */
      pdb_primitive_finish(pdb, &pr);
      continue;
    }

    pdb_primitive_linkage_get(&pr, linkage, guid);
    pdb_primitive_finish(pdb, &pr);

    /*  Convert the linkage GUID into a local ID.
     */
    err = pdb_id_from_guid(pdb, w, &guid);
    if (err == GRAPHD_ERR_NO) {
      cl_log(cl, CL_LEVEL_FAIL,
             "isa_become_small_set: "
             "cannot resolve guid %s for <-%s"
             "(%llx) as a local ID. (Skipped.)",
             graph_guid_to_string(&guid, buf, sizeof buf),
             pdb_linkage_to_string(linkage), (unsigned long long)*w);
      continue;
    } else if (err) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "guid=%s",
                   graph_guid_to_string(&guid, buf, sizeof buf));
      pdb_iterator_destroy(pdb, &sub_clone);
      return err;
    }

    if (*w >= low && *w < high)
      w++;
    else
      cl_log(cl, CL_LEVEL_VERBOSE,
             "isa_make: result %llx out "
             "of desired range %llx..%llx",
             (unsigned long long)*w, (unsigned long long)low,
             (unsigned long long)(high - 1));
  }

  cl_assert(cl, (char *)w <= (char *)sub_ids + sizeof(sub_ids));
  cl_assert(cl, err == GRAPHD_ERR_NO || err == PDB_ERR_MORE || err == 0);

  pdb_iterator_destroy(pdb, &sub_clone);

  if (err == GRAPHD_ERR_NO) {
    /*  We ran out of IDs from our iterator -
     *  the desired case.
     */
    err = graphd_iterator_fixed_create_array(
        g, sub_ids, (size_t)(w - sub_ids), low, high,
        direction != GRAPHD_DIRECTION_BACKWARD, it_out);
    if (err != 0) return err;

    /*  The fixed iterator is sorted.
     *  If we're ordered ourselves, without
     *  going by a subiterator, the optimized
     *  version inherits the ordering.
     */
    if (direction != GRAPHD_DIRECTION_ORDERING && ordering != NULL)
      pdb_iterator_ordering_set(pdb, *it_out, ordering);

    /*  If we're so large we're unwieldy, tell
     *  the fixed iterator to masquerade.
     */
    if (w - sub_ids >= 5) {
      err = isa_set_fixed_masquerade(pdb, *it_out, linkage, sub);
      if (err != 0) {
        pdb_iterator_destroy(pdb, it_out);
        return err;
      }
    }
    return err;
  }

  cl_assert(cl, err == 0 || err == PDB_ERR_MORE);

  /*  Mumble.  We were hoping, but weren't guaranteed,
   *  that the iterator's guess would be accurate.
   *  But this one's larger than it thought, or takes
   *  longer than we thought.
   *  Do this the slow way.
   */
  cl_log(cl, CL_LEVEL_FAIL,
         "isa_become_small_set: "
         "more than %zu sub_ids from %s?",
         sizeof(sub_ids) / sizeof(*sub_ids),
         pdb_iterator_to_string(pdb, sub, buf, sizeof buf));
  return PDB_ERR_MORE;
}

/**
 * @brief If we were to build a VIP map, how many entries
 * 	would be in it?
 *
 *  We know a typeguid and two end points.  We can
 *  form up to two VIP iterators with this - left+type  or
 *  right+type.  If we'd pick the one with <linkage>, how
 *  many entries are in it?
 *
 *  (The calling code will then pick the smaller VIP iterator.)
 *
 * @param pdb		PDB handle
 * @param it		containing isa iterator
 * @param linkage_id	the id the linkage pointer points to
 * @param linkage	which linkage pointer are we talkign about?
 * @param type_guid	type GUID
 * @param n_out		out: how many entries?
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int isa_check_vip_n(pdb_handle *pdb, pdb_iterator *it, pdb_id linkage_id,
                           int linkage, graph_guid *type_guid,
                           unsigned long long *n_out) {
  graphd_iterator_isa *isa = it->it_theory;
  bool is_vip;
  int err;

  err = pdb_vip_id(pdb, linkage_id, linkage, &is_vip);
  if (err != 0) {
    return err;
  }
  if (!is_vip) return 0;

  return pdb_vip_id_count(pdb, linkage_id, linkage, type_guid,
                          isa->isa_sub->it_low, isa->isa_sub->it_high,
                          PDB_COUNT_UNBOUNDED, n_out);
}

/**
 * @brief Create a fan-in iterator.
 *
 * 	We're an isa-iterator.  We're holding a single ID, maybe one
 *	we're trying to return or are trying to check.
 *
 *	Create an iterator for the "fan-in" of that ID -- that is,
 *	for all primitives that point to that ID with their
 *	linkage-pointers.  (The "linkage" is something specific
 *	fixed at the time of the iterator's creation, e.g.,
 *	"type" or "left" or "scope".)
 *
 *	Use VIP iterators whenever possible.  If the linkage is a
 *	type, and the sub-iterator has a left or right side in it,
 *	try to turn those into a VIP iterator.  If the linkage is
 *	a left or right, and the subiterator has a type somewhere
 *	in it, same thing.
 *
 *   	The boundaries for the fan-in are the boundaries of the
 *	subiterator.  (The expectation is that the fan-in will
 *	be cut against the subiterator.)
 *
 * @param pdb		containing module
 * @param it		the isa-iterator
 * @param sub_it	sub-iterator to use.  (Might be the
 *			fixed sub-iterator, might be the statistics
 *			copy of the subiterator)
 * @param source_id 	source of the fan-in.  We're looking for
 *			lists of primitives that point to this.
 * @param low		lowest id included
 * @param high		one more than highest id included
 * @param fanin_out	out: the fanin iterator.
 *
 * @return 0 on success
 * @return GRAPHD_ERR_NO in some cases if there's no fan-in (and the
 *	id should be rejected if checking.)
 * @return other nonzero error codes on error.
 */
static int isa_fanin_create(pdb_handle *pdb, pdb_iterator *it,
                            pdb_iterator *sub_it, pdb_id source_id,
                            unsigned long long low, unsigned long long high,
                            pdb_iterator **fanin_out) {
  graphd_iterator_isa *isa = it->it_theory;
  cl_handle *cl = isa->isa_cl;
  bool forward;
  char buf[200];
  int err;

  /*  It's important that the fanin and the sub_it
   *  have the same direction.  (Or on-or-after will
   *  just jitter back and forth between the same two values!)
   *  Since we're creating fanin, we'll just copy the direction
   *  from the sub_it.
   */
  forward = pdb_iterator_forward(pdb, sub_it);

  /*  If the iterator below knows anything about its
   *  linkage IDs, get that knowledge.
   */
  if (!pdb_iterator_statistics_done(pdb, it)) {
    err = isa_sub_primitive_summary(pdb, it);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "isa_sub_psum", err, "it=%s",
                   pdb_iterator_to_string(pdb, sub_it, buf, sizeof buf));
      return err;
    }
  }

  if (low < sub_it->it_low) low = sub_it->it_low;

  if (high == PDB_ITERATOR_HIGH_ANY || high > sub_it->it_high)
    high = sub_it->it_high;

  if ((isa->isa_sub_psum.psum_locked & (1 << PDB_LINKAGE_TYPEGUID)) &&
      (isa->isa_linkage == PDB_LINKAGE_LEFT ||
       isa->isa_linkage == PDB_LINKAGE_RIGHT)) {
    bool is_vip;

    err = pdb_vip_id(pdb, source_id, isa->isa_linkage, &is_vip);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_SPEW, "pdb_vip_id", err, "id=%llx",
                   (unsigned long long)source_id);
      return err;
    }
    if (!is_vip) goto normal;

    err = pdb_vip_id_iterator(
        pdb, source_id, isa->isa_linkage,
        isa->isa_sub_psum.psum_guid + PDB_LINKAGE_TYPEGUID, low, high, forward,
        /* error-if-null */ true, fanin_out);
    if (err == GRAPHD_ERR_NO) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "isa_fanin_create: "
             "pdb_vip_id_iterator says no");
      return err;
    } else if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_vip_id_iterator", err,
                   "%s=%llx, type=%s", pdb_linkage_to_string(isa->isa_linkage),
                   (unsigned long long)source_id,
                   graph_guid_to_string(
                       isa->isa_sub_psum.psum_guid + PDB_LINKAGE_TYPEGUID, buf,
                       sizeof buf));
      return err;
    }
  } else if (isa->isa_linkage == PDB_LINKAGE_TYPEGUID &&
             (isa->isa_sub_psum.psum_locked &
              ((1 << PDB_LINKAGE_LEFT) | (1 << PDB_LINKAGE_RIGHT)))) {
    graph_guid type_guid;
    unsigned long long left_n, right_n;
    int the_linkage;
    pdb_id the_id;

    left_n = (unsigned long long)-1;
    right_n = (unsigned long long)-1;

    err = pdb_id_to_guid(pdb, source_id, &type_guid);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_to_guid", err, "type-id=%llx",
                   (unsigned long long)source_id);
      return err;
    }

    if (isa->isa_sub_psum.psum_locked & (1 << PDB_LINKAGE_RIGHT)) {
      err = isa_check_vip_n(pdb, it, source_id, PDB_LINKAGE_RIGHT, &type_guid,
                            &right_n);
      if (err != 0) {
        cl_leave(cl, CL_LEVEL_SPEW, "error from pdb_isa_check_vip_n: %s",
                 graphd_strerror(err));
        return err;
      }
    }
    if (isa->isa_sub_psum.psum_locked & (1 << PDB_LINKAGE_LEFT)) {
      err = isa_check_vip_n(pdb, it, source_id, PDB_LINKAGE_LEFT, &type_guid,
                            &left_n);
      if (err != 0) {
        cl_leave(cl, CL_LEVEL_SPEW, "error from pdb_isa_check_vip_n: %s",
                 graphd_strerror(err));
        return err;
      }
    }
    if (left_n < right_n)
      the_linkage = PDB_LINKAGE_LEFT;
    else if (right_n == (unsigned long long)-1)
      goto normal;
    else
      the_linkage = PDB_LINKAGE_RIGHT;
    the_id = isa->isa_sub_psum_id[the_linkage];

    err = pdb_vip_id_iterator(pdb, the_id, the_linkage, &type_guid, low, high,
                              forward,
                              /* error_if_null */ true, fanin_out);
    if (err == GRAPHD_ERR_NO) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "isa_fanin_create: "
             "pdb_vip_id_iterator says no");
      return err;
    } else if (err != 0) {
      cl_log_errno(
          cl, CL_LEVEL_FAIL, "pdb_vip_id_iterator", err, "guid=%s, %s=%llx",
          graph_guid_to_string(&type_guid, buf, sizeof buf),
          pdb_linkage_to_string(the_linkage), (unsigned long long)the_id);
      return err;
    }
  } else {
  normal:
    err = pdb_linkage_id_iterator(pdb, isa->isa_linkage, source_id, low, high,
                                  forward,
                                  /* error if null */ true, fanin_out);
    if (err != 0) {
      if (err != GRAPHD_ERR_NO)
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_linkage_id_iterator", err,
                     "%s(%llx)", pdb_linkage_to_string(isa->isa_linkage),
                     (unsigned long long)source_id);
      return err;
    }
  }

  /*  The fan-in iterator inherits our account.
   */
  pdb_iterator_account_set(pdb, *fanin_out, pdb_iterator_account(pdb, it));

  cl_log(cl, CL_LEVEL_VERBOSE, "isa_fanin_create: %s",
         pdb_iterator_to_string(pdb, *fanin_out, buf, sizeof buf));
  return 0;
}

static void isa_dup_finish(pdb_handle *pdb, isa_duplicate_test *dt) {
  pdb_iterator_destroy(pdb, &dt->dt_fanin);
  pdb_iterator_destroy(pdb, &dt->dt_sub);

  dt->dt_state = 0;
}

static int isa_dup_clear(isa_duplicate_test *dt) {
  dt->dt_fanin = NULL;
  dt->dt_sub = NULL;
  dt->dt_state = 0;
  dt->dt_n_ok = 0;
  dt->dt_method = ISA_DT_METHOD_UNSPECIFIED;
  dt->dt_id = PDB_ID_NONE;
  dt->dt_storable_position = 0;

  return 0;
}

static int isa_dup_dup(pdb_handle *pdb, isa_duplicate_test const *in,
                       isa_duplicate_test *out) {
  int err;
  cl_handle *cl = pdb_log(pdb);

  isa_dup_clear(out);

  switch (out->dt_method = in->dt_method) {
    case ISA_DT_METHOD_UNSPECIFIED:
      return 0;

    case ISA_DT_METHOD_STORABLE:
      out->dt_storable_position = in->dt_storable_position;
      return 0;

    case ISA_DT_METHOD_INTERSECT:
      if (in->dt_fanin != NULL) {
        err = pdb_iterator_clone(pdb, in->dt_fanin, &out->dt_fanin);
        if (err != 0) return err;
      }
      if (in->dt_sub != NULL) {
        err = pdb_iterator_clone(pdb, in->dt_sub, &out->dt_sub);
        if (err != 0) return err;

        cl_assert(cl, pdb_iterator_statistics_done(pdb, in->dt_sub));
        cl_assert(cl, pdb_iterator_sorted(pdb, in->dt_sub));
        cl_assert(cl, pdb_iterator_statistics_done(pdb, out->dt_sub));
        cl_assert(cl, pdb_iterator_sorted(pdb, out->dt_sub));
      }
      out->dt_state = in->dt_state;
      out->dt_id = in->dt_id;
      out->dt_n_ok = in->dt_n_ok;
      break;

    default:
      cl_notreached(pdb_log(pdb), "unexpected dt_method %d", in->dt_method);
  }
  return 0;
}

static int isa_dup_freeze(graphd_handle *g, pdb_iterator *it,
                          isa_duplicate_test *dt, cm_buffer *buf) {
  char sb[GRAPHD_ITERATOR_RESOURCE_STAMP_SIZE];
  int err;
  pdb_handle *pdb = g->g_pdb;
  cl_handle *cl = oisa(it)->isa_cl;

  if (dt->dt_method == ISA_DT_METHOD_UNSPECIFIED ||
      (dt->dt_method == ISA_DT_METHOD_INTERSECT && dt->dt_state == 0) ||
      (dt->dt_method == ISA_DT_METHOD_STORABLE &&
       (oisa(it)->isa_cache == NULL ||
        graphd_iterator_isa_storable_nelems(oisa(it)->isa_cache) == 0)))
    return cm_buffer_add_string(buf, "-");

  if (dt->dt_method == ISA_DT_METHOD_STORABLE) {
    /*  [sdup:(SUBPOS/SUBSTATE)@STORABLE]
     */
    if (oisa(it)->isa_cache_sub != NULL) {
      char buf[200];
      cl_log(cl, CL_LEVEL_VERBOSE,
             "isa_dup_freeze: freezing isa_cache_sub %p (%s)",
             (void *)oisa(it)->isa_cache_sub,
             pdb_iterator_to_string(pdb, oisa(it)->isa_cache_sub, buf,
                                    sizeof buf));
      cl_assert(oisa(it)->isa_cl,
                pdb_iterator_statistics_done(pdb, oisa(it)->isa_cache_sub));
    }

    if ((err = cm_buffer_sprintf(buf, "[sd:")) != 0 ||
        (err = graphd_iterator_util_freeze_subiterator(
             pdb, oisa(it)->isa_cache_sub, PDB_ITERATOR_FREEZE_EVERYTHING,
             buf)) != 0 ||
        (err = graphd_iterator_resource_store(
             g, (graphd_storable *)oisa(it)->isa_cache, sb, sizeof sb)) != 0 ||
        (err = cm_buffer_sprintf(buf, "@%s]", sb)) != 0)
      return err;

    return 0;
  }

  /*  We're intersecting, and we're in the middle of one
   *  such intersection.
   */
  cl_assert(g->g_cl, dt->dt_method == ISA_DT_METHOD_INTERSECT);

  err = cm_buffer_sprintf(buf, "[dup:%d:%d:%s:", dt->dt_state, dt->dt_n_ok,
                          pdb_id_to_string(pdb, dt->dt_id, sb, sizeof sb));
  if (err != 0) return err;

  err = graphd_iterator_util_freeze_subiterator(
      pdb, dt->dt_fanin, PDB_ITERATOR_FREEZE_EVERYTHING, buf);
  if (err != 0) return err;

  err = graphd_iterator_util_freeze_subiterator(
      pdb, dt->dt_sub, PDB_ITERATOR_FREEZE_EVERYTHING, buf);
  if (err != 0) return err;

  return cm_buffer_add_string(buf, "]");
}

static void isa_dup_pick_method(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_isa *const isa = it->it_theory;
  cl_handle *const cl = isa->isa_cl;
  char buf[200];
  unsigned long long n;

  if (oisa(it)->isa_dup.dt_method != ISA_DT_METHOD_UNSPECIFIED) return;

  /*  Use INTERSECT if
   *	- the subiterator is sorted
   *	- our result set is large
   *
   *  Otherwise, use STORAGE.
   */

  /*  If our subiterator is sorted and reasonably fast,
   *  but we don't know our size yet, take a wild guess -
   *  read a few of its values and see what the overlap is.
   */
  if (pdb_iterator_n_valid(pdb, it))
    n = pdb_iterator_n(pdb, it);
  else
    n = 1;

  /*  We know the subiterator is sorted; we don't know
   *  how big we're going to be, but we *might* be huge.
   */
  if (pdb_iterator_sorted_valid(pdb, oisa(it)->isa_sub) &&
      pdb_iterator_sorted(pdb, oisa(it)->isa_sub) &&
      !pdb_iterator_n_valid(pdb, it) &&
      pdb_iterator_next_cost_valid(pdb, oisa(it)->isa_sub) &&
      pdb_iterator_next_cost(pdb, oisa(it)->isa_sub) < 100 &&
      pdb_iterator_n_valid(pdb, oisa(it)->isa_sub) &&
      pdb_iterator_n(pdb, oisa(it)->isa_sub) >
          GRAPHD_STORABLE_HUGE(oisa(it)->isa_hint)) {
    pdb_id ar[100];
    size_t ar_n = 0, i;
    size_t sub_n = 0;
    pdb_budget budget = 1000;
    pdb_iterator *sub_it;
    int err;

    /*  Clone the subiterator.
     */
    err = pdb_iterator_clone(pdb, oisa(it)->isa_sub, &sub_it);
    if (err != 0)
      cl_log_errno(
          cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "it=%s",
          pdb_iterator_to_string(pdb, oisa(it)->isa_sub, buf, sizeof buf));
    else {
      while (budget > 0 && ar_n < sizeof(ar) / sizeof(*ar)) {
        pdb_primitive pr;
        pdb_id sub_id, id;
        graph_guid guid;

        /* Pull another value from the subiterator.
         */
        err = pdb_iterator_next(pdb, sub_it, &sub_id, &budget);
        if (err != 0) break;
        sub_n++;

        /*  If it's out of range, throw it out.
         */
        if (sub_id < it->it_low) continue;

        /*  Get the corresponding primitive.
         */
        err = pdb_id_read(pdb, sub_id, &pr);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err, "sub_id=%llu",
                       (unsigned long long)sub_id);
          continue;
        }

        /*  Go from the primitive to its linkage.
         */
        if (!pdb_primitive_has_linkage(&pr, isa->isa_linkage)) {
          pdb_primitive_finish(pdb, &pr);
          continue;
        }
        pdb_primitive_linkage_get(&pr, isa->isa_linkage, guid);
        pdb_primitive_finish(pdb, &pr);

        err = pdb_id_from_guid(pdb, &id, &guid);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "guid=%s",
                       graph_guid_to_string(&guid, buf, sizeof buf));
          continue;
        }

        /*  If the result is out of range,
         *  throw it out.
         */
        if (id < it->it_low || id >= it->it_high) continue;

        /*  Store this if it's new.
         */
        for (i = 0; i < ar_n; i++)
          if (ar[i] == id) break;
        if (i >= ar_n) ar[ar_n++] = id;
      }

      /*  We've read sub_n IDs from the subiterator, and
       *  produced ar_n results in response.
       */
      pdb_iterator_destroy(pdb, &sub_it);

      if (ar_n == 0) ar_n = 1;
      if (sub_n == 0) sub_n = 1;

      n = (pdb_iterator_n(pdb, oisa(it)->isa_sub) * ar_n) / sub_n;
    }
  }

  oisa(it)->isa_dup.dt_method =
      (pdb_iterator_sorted_valid(pdb, oisa(it)->isa_sub) &&
       pdb_iterator_sorted(pdb, oisa(it)->isa_sub) &&
       (pdb_iterator_n_valid(pdb, it) ? pdb_iterator_n(pdb, it) : n) >=
           GRAPHD_STORABLE_HUGE(oisa(it)->isa_hint))
          ? ISA_DT_METHOD_INTERSECT
          : ISA_DT_METHOD_STORABLE;

  cl_log(cl, CL_LEVEL_VERBOSE,
         "isa %s: dup method %s (sub sorted valid? %d, sub sorted? %d,  n "
         "valid? %d, n? %llu, sub n? %llu)",
         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         oisa(it)->isa_dup.dt_method == ISA_DT_METHOD_INTERSECT ? "intersect"
                                                                : "storable",
         pdb_iterator_sorted_valid(pdb, oisa(it)->isa_sub),
         pdb_iterator_sorted(pdb, oisa(it)->isa_sub),
         pdb_iterator_n_valid(pdb, it),
         (unsigned long long)pdb_iterator_n(pdb, it),
         (unsigned long long)pdb_iterator_n(pdb, oisa(it)->isa_sub));
}

/*  Thaw the duplicate detector state.  Possible outcomes:
 *
 *	GRAPHD_ERR_NO	-- oops, I dropped my resource cache
 *		   on the floor.  Please recover.
 *	GRAPHD_ERR_LEXICAL-- this iterator was syntactically broken.
 *	ENOMEM  -- we're out of memory.
 *
 */
static int isa_dup_thaw(graphd_handle *g, pdb_iterator *it, char const **s_ptr,
                        char const *e, pdb_iterator_text *subpit,
                        pdb_iterator_base *pib, cl_loglevel loglevel,
                        isa_duplicate_test *dt) {
  int err;
  char const *s = *s_ptr;
  pdb_handle *pdb = g->g_pdb;
  cl_handle *cl = g->g_cl;

  /*  The duplicate tracker state is
   *  preinitialized with empty.
   */
  if (s == NULL || s >= e || *s == '-') {
    if (s < e && *s == '-') ++*s_ptr;

    dt->dt_method = ISA_DT_METHOD_UNSPECIFIED;
    if (pdb_iterator_statistics_done(pdb, it)) isa_dup_pick_method(pdb, it);

    return 0;
  }

  cl_enter(g->g_cl, CL_LEVEL_VERBOSE, "\"%.*s\"", (int)(e - *s_ptr), *s_ptr);

  if (*s == '@') {
    /*  We don't know this one anymore
     */
    cl_leave(g->g_cl, CL_LEVEL_VERBOSE,
             "don't support @... "
             "anymore; please recover");
    return GRAPHD_ERR_NO;
  } else if (*s == '[' && e - s >= 4 && strncasecmp(s, "[sd:", 4) == 0) {
    char const *s0 = s, *sdup_e;

    *s_ptr += sizeof("[sd:") - 1;

    isa_cache_destroy(pdb, it);

    err = graphd_iterator_util_thaw_subiterator(g, s_ptr, e, pib, loglevel,
                                                &oisa(it)->isa_cache_sub);
    if (err != 0) {
      graphd_storable_unlink(oisa(it)->isa_cache);
      oisa(it)->isa_cache = NULL;

      cl_leave(g->g_cl, CL_LEVEL_FAIL,
               "failed to recover "
               "partial subiterator at \"%.*s\": %s",
               (int)(e - s0), s0, graphd_strerror(err));
      return err;
    }
    if (oisa(it)->isa_cache_sub != NULL) {
      char buf[200];
      cl_log(cl, CL_LEVEL_VERBOSE,
             "isa_dup_thaw: thawed isa_cache_sub is %p (%s)",
             (void *)oisa(it)->isa_cache_sub,
             pdb_iterator_to_string(pdb, oisa(it)->isa_cache_sub, buf,
                                    sizeof buf));
      cl_assert(cl, pdb_iterator_statistics_done(pdb, oisa(it)->isa_cache_sub));
    }

    if ((sdup_e = memchr(*s_ptr, ']', e - *s_ptr)) == NULL) {
      cl_log(g->g_cl, loglevel,
             "isa_dup_thaw: expected "
             "[sdup:(SUB)@HASH], got \"%.*s\"",
             (int)(e - s0), s0);
      cl_leave(g->g_cl, CL_LEVEL_VERBOSE, "missing ]");
      return GRAPHD_ERR_LEXICAL;
    }
    if (*s_ptr < sdup_e && **s_ptr == '@') {
      ++*s_ptr;
      oisa(it)->isa_cache = graphd_iterator_isa_storable_thaw(g, s_ptr, sdup_e);
      if (oisa(it)->isa_cache == NULL) {
        cl_leave(g->g_cl, CL_LEVEL_VERBOSE, "can't get hashtable");
        return GRAPHD_ERR_NO;
      }
      *s_ptr = sdup_e;
    } else {
      err = isa_cache_create(pdb, it);
      if (err != 0) {
        cl_leave(g->g_cl, CL_LEVEL_VERBOSE,
                 "can't allocate fresh hashtable: %s", graphd_strerror(err));
        return err;
      }
    }

    ++*s_ptr;
    dt->dt_method = ISA_DT_METHOD_STORABLE;
  } else if (*s == '[' && e - s >= 6 && strncasecmp(s, "[sdup:", 6) == 0) {
    char const *s0 = s, *sdup_e;

    /* Compatibility with cursors from prevoius release.
     */
    *s_ptr += sizeof("[sdup:") - 1;

    isa_cache_destroy(pdb, it);

    err = graphd_iterator_util_thaw_partial_subiterator(
        g, s_ptr, e, PDB_ITERATOR_FREEZE_POSITION | PDB_ITERATOR_FREEZE_STATE,
        subpit, pib, loglevel, &oisa(it)->isa_cache_sub);
    if (err != 0) {
      graphd_storable_unlink(oisa(it)->isa_cache);
      oisa(it)->isa_cache = NULL;

      cl_leave(g->g_cl, CL_LEVEL_FAIL,
               "failed to recover "
               "partial subiterator at \"%.*s\": %s",
               (int)(e - s0), s0, graphd_strerror(err));
      return err;
    }
    if (oisa(it)->isa_cache_sub != NULL) {
      char buf[200];
      cl_log(cl, CL_LEVEL_VERBOSE,
             "isa_dup_thaw: thawed isa_cache_sub is %p (%s)",
             (void *)oisa(it)->isa_cache_sub,
             pdb_iterator_to_string(pdb, oisa(it)->isa_cache_sub, buf,
                                    sizeof buf));
      cl_assert(cl, pdb_iterator_statistics_done(pdb, oisa(it)->isa_cache_sub));
    }

    if ((sdup_e = memchr(*s_ptr, ']', e - *s_ptr)) == NULL) {
      cl_log(g->g_cl, loglevel,
             "isa_dup_thaw: expected "
             "[sdup:(SUB)@HASH], got \"%.*s\"",
             (int)(e - s0), s0);
      cl_leave(g->g_cl, CL_LEVEL_VERBOSE, "missing ]");
      return GRAPHD_ERR_LEXICAL;
    }
    if (*s_ptr < sdup_e && **s_ptr == '@') {
      ++*s_ptr;
      oisa(it)->isa_cache = graphd_iterator_isa_storable_thaw(g, s_ptr, sdup_e);
      if (oisa(it)->isa_cache == NULL) {
        cl_leave(g->g_cl, CL_LEVEL_VERBOSE, "can't get hashtable");
        return GRAPHD_ERR_NO;
      }
      *s_ptr = sdup_e;
    } else {
      err = isa_cache_create(pdb, it);
      if (err != 0) {
        cl_leave(g->g_cl, CL_LEVEL_VERBOSE,
                 "can't allocate fresh hashtable: %s", graphd_strerror(err));
        return err;
      }
    }

    ++*s_ptr;
    dt->dt_method = ISA_DT_METHOD_STORABLE;
  } else {
    /*  Future extension: anything that doesn't start with
     *  "[dup"
     */
    if (e - *s_ptr < 4 || strncasecmp(*s_ptr, "[dup", 4) != 0) {
      cl_leave(g->g_cl, CL_LEVEL_VERBOSE,
               "don't know what \"%.*s\" means "
               "(future cursor?); dropping state",
               (int)(e - *s_ptr), *s_ptr);
      *s_ptr = e;

      /*  Meaning, "I dropped the state on the floor."
       */
      return GRAPHD_ERR_NO;
    }

    err = pdb_iterator_util_thaw(pdb, s_ptr, e, "[dup:%d:%d:%{id}:",
                                 &dt->dt_state, &dt->dt_n_ok, &dt->dt_id);
    if (err != 0) {
      cl_leave(g->g_cl, CL_LEVEL_VERBOSE, "util thaw fails: %s",
               graphd_strerror(err));
      return err;
    }

    /* scan the two subiterators. */
    err = graphd_iterator_util_thaw_subiterator(g, s_ptr, e, pib, loglevel,
                                                &dt->dt_fanin);
    if (err != 0) {
      cl_leave(g->g_cl, CL_LEVEL_VERBOSE, "util thaw fails: %s",
               graphd_strerror(err));
      return err;
    }

    err = graphd_iterator_util_thaw_subiterator(g, s_ptr, e, pib, loglevel,
                                                &dt->dt_sub);
    if (err != 0) {
      cl_leave(g->g_cl, CL_LEVEL_VERBOSE, "util thaw fails: %s",
               graphd_strerror(err));
      return err;
    }
    if (*s_ptr < e && **s_ptr == ']') ++*s_ptr;

    cl_assert(g->g_cl, pdb_iterator_statistics_done(pdb, dt->dt_sub));
    dt->dt_method = ISA_DT_METHOD_INTERSECT;
  }

  cl_leave(g->g_cl, CL_LEVEL_VERBOSE, "done");
  return 0;
}

/* Has this iterator returned "id" since its last reset?
 */
static int isa_dup_test_intersect(pdb_handle *pdb, pdb_iterator *it,
                                  pdb_iterator *sub_it, isa_duplicate_test *dt,
                                  pdb_id id, pdb_id source_id,
                                  pdb_budget *budget_inout,
                                  bool *is_duplicate_out, char const *file,
                                  int line) {
  graphd_iterator_isa *isa = it->it_theory;
  cl_handle *cl = isa->isa_cl;
  pdb_budget budget_in = *budget_inout;
  int err;
  char buf[200];
  pdb_budget cost_a, cost_b, cost_ab;
  unsigned long long trials_a, trials_b;
  unsigned long long upper_bound;
  unsigned long long smaller_n, sub_n, fanin_n;

  cl_assert(isa->isa_cl, id != PDB_ID_NONE);
  cl_assert(isa->isa_cl, id != source_id);
  cl_assert(isa->isa_cl, id < pdb_primitive_n(pdb));

  cl_assert(cl, pdb_iterator_statistics_done(pdb, isa->isa_sub));
  cl_assert(cl, pdb_iterator_sorted(pdb, isa->isa_sub));

  *is_duplicate_out = false;

  /*  We're thawed and derived from a sorted iterator, and there
   *  have been so many elements that we don't keep our state
   *  in a hashtable anymore.
   *
   *  Intersect
   *
   *	- the fan-in to the result id along our linkage
   *	 (possibly including a vip ingredient from the sorted iterator).
   *
   *	- a duplicate of the sorted iterator.
   *
   *  with a high boundary of the result's source id and
   *  a low boundary of the result.
   *
   *  If the intersection is empty, return OK.
   *  Otherwise, return NO.
   */

  cl_assert(cl, dt != NULL);
  switch (dt->dt_state) {
    default:
      cl_notreached(cl,
                    "isa_dup_test_intersect: "
                    "unexpected dt->dt_state %d",
                    dt->dt_state);
    /* NOTREACHED */

    case 0:
      /*  If needed, make a fresh clone of
       *  the subiterator.
       */
      if (dt->dt_sub == NULL || dt->dt_sub->it_id != sub_it->it_id) {
        cl_assert(cl, pdb_iterator_statistics_done(pdb, isa->isa_sub));
        cl_assert(cl, pdb_iterator_sorted(pdb, isa->isa_sub));

        pdb_iterator_destroy(pdb, &dt->dt_sub);
        err = pdb_iterator_clone(pdb, isa->isa_sub, &dt->dt_sub);
        if (err != 0) {
          cl_log_errno(
              cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "it=%s",
              pdb_iterator_to_string(pdb, isa->isa_sub, buf, sizeof buf));
          return err;
        }
        cl_assert(cl, pdb_iterator_statistics_done(pdb, dt->dt_sub));
        cl_assert(cl, pdb_iterator_sorted(pdb, dt->dt_sub));
      }
      PDB_IS_ITERATOR(cl, dt->dt_sub);
      cl_assert(cl, pdb_iterator_sorted(pdb, dt->dt_sub));

      pdb_iterator_destroy(pdb, &dt->dt_fanin);

      if (pdb_iterator_forward(pdb, dt->dt_sub))
        err = isa_fanin_create(pdb, it, dt->dt_sub, id,
                               /* low */ 0,
                               /* high */ source_id, &dt->dt_fanin);
      else
        err = isa_fanin_create(pdb, it, dt->dt_sub, id,
                               /* low */ source_id + 1,
                               /* high */ PDB_ITERATOR_HIGH_ANY, &dt->dt_fanin);
      if (err != 0) {
        pdb_iterator_destroy(pdb, &dt->dt_fanin);

        if (err == PDB_ERR_NO) {
          *is_duplicate_out = false;
          goto done;
        }

        pdb_iterator_destroy(pdb, &dt->dt_sub);
        cl_log_errno(cl, CL_LEVEL_FAIL, "isa_fanin_create", err, "id=%llx",
                     (unsigned long long)id);
        return err;
      }
      cl_assert(cl, dt->dt_fanin != NULL);
      if (pdb_iterator_null_is_instance(pdb, dt->dt_fanin)) {
        cl_log(cl, CL_LEVEL_VERBOSE,
               "isa_dup_test_intersect: false (null fanin)");
        *is_duplicate_out = false;
        goto done;
      }

      /*  We know the fanin always knows its highest or
       *  lowest ID because we created it, and it's always
       *  either a VIP iterator or a gmap.
       */
      if (pdb_iterator_forward(pdb, dt->dt_sub))
        dt->dt_id = dt->dt_fanin->it_low;
      else
        dt->dt_id = dt->dt_fanin->it_high - 1;

      dt->dt_n_ok = 0;

      /*  We know what our fan-in looks like, but we don't know
       *  our subiterators.  Depending on what it is, it may or
       *  may not be a good idea to run on-or-after checks against it.
       *
       *  (a)  call NEXT on the fan-in, CHECK on the individuals
       *  (b)  call NEXT on the sub iterator, CHECK the fan-in.
       *  (ab) FIND/FIND the two arrays against each other
       */
      upper_bound = pdb_iterator_spread(pdb, dt->dt_sub);
      if (upper_bound == 0) upper_bound = 1;
      sub_n = pdb_iterator_n_valid(pdb, dt->dt_sub)
                  ? pdb_iterator_n(pdb, dt->dt_sub)
                  : upper_bound;
      if (sub_n < 1) sub_n = 1;

      fanin_n = pdb_iterator_n_valid(pdb, dt->dt_fanin)
                    ? pdb_iterator_n(pdb, dt->dt_fanin)
                    : pdb_iterator_spread(pdb, dt->dt_fanin);
      if (fanin_n > upper_bound) fanin_n = upper_bound;
      if (fanin_n < 1) fanin_n = 1;

      /*  (A) call NEXT on the fan-in, CHECK on the subiterator.
       */
      trials_a = fanin_n;
      if (trials_a > upper_bound / sub_n) trials_a = upper_bound / sub_n;

      cost_a = (pdb_iterator_next_cost(pdb, dt->dt_fanin) +
                pdb_iterator_check_cost(pdb, dt->dt_sub)) *
               trials_a;

      /*  (B) call NEXT on the subiterator, CHECK on the fan-in.
       */
      trials_b = sub_n;
      if (trials_b > upper_bound / fanin_n) trials_b = upper_bound / fanin_n;
      cost_b = (pdb_iterator_next_cost(pdb, dt->dt_sub) +
                pdb_iterator_check_cost(pdb, dt->dt_fanin)) *
               trials_b;

      /* (C) Assumption: we have to find/find our way through
       *     half the data
       */
      smaller_n = fanin_n;
      if (smaller_n > sub_n) smaller_n = sub_n;

      cl_assert(cl, pdb_iterator_sorted(pdb, dt->dt_sub));
      cost_ab = (pdb_iterator_find_cost(pdb, dt->dt_fanin) +
                 pdb_iterator_find_cost(pdb, dt->dt_sub)) *
                0.5 * smaller_n;

      if (cost_a < cost_ab && cost_a < cost_b) {
        /*  Walk the fan-in with NEXT,
         *  checking it with CHECK against the
         *  subiterator.
         */
        for (;;) {
          case 11:
            err =
                pdb_iterator_next(pdb, dt->dt_fanin, &dt->dt_id, budget_inout);
            dt->dt_state = 0;

            if (err != 0) {
              /*  Running out of IDs?
               *  (This is the expected case.)
               */
              if (err == PDB_ERR_NO) {
                *is_duplicate_out = false;
                goto done;
              }

              /* Running out of time?
               */
              if (err == PDB_ERR_MORE) {
                dt->dt_state = 11;
                return err;
              }

              /*  All other errors are unexpected.
               */
              cl_log_errno(
                  cl, CL_LEVEL_FAIL, "pdb_iterator_next", err,
                  "id=%llx, fanin=%s", (unsigned long long)dt->dt_id,
                  pdb_iterator_to_string(pdb, dt->dt_fanin, buf, sizeof buf));
              return err;
            }
          case 12:
            err = pdb_iterator_check(pdb, dt->dt_sub, dt->dt_id, budget_inout);
            dt->dt_state = 0;

            /*  The subiterator signed off on it?
             *  -> duplicate.
             */
            if (err == 0) break;

            /*  We ran out of time?
             */
            if (err == PDB_ERR_MORE) {
              dt->dt_state = 12;
              return err;
            }

            /*  Some unexpected error?
             */
            if (err != GRAPHD_ERR_NO) {
              cl_log_errno(
                  cl, CL_LEVEL_FAIL, "pdb_iterator_check", err,
                  "id=%llx, sub_it=%s", (unsigned long long)dt->dt_id,
                  pdb_iterator_to_string(pdb, dt->dt_sub, buf, sizeof buf));
              return err;
            }

            /*  The subiterator didn't like it; we need to
             *  keep going.  Did we run out of time?
             */
            if (GRAPHD_SABOTAGE(isa->isa_graphd, *budget_inout <= 0)) {
              dt->dt_state = 13;
              return PDB_ERR_MORE;
              case 13:
                dt->dt_state = 0;
            }
        }
      } else if (cost_b < cost_ab && cost_b < cost_a) {
        /*  Walk the subiterator with NEXT,
         *  checking it with CHECK against the fan-in.
         */
        err = pdb_iterator_reset(pdb, dt->dt_sub);
        if (err != 0) return err;
        for (;;) {
          case 21:
            err = pdb_iterator_next(pdb, dt->dt_sub, &dt->dt_id, budget_inout);
            dt->dt_state = 0;

            if (err != 0) {
              /*  We ran out of IDs?
               */
              if (err == PDB_ERR_NO) {
                *is_duplicate_out = false;
                goto done;
              }

              /*  We ran out of time?
               */
              if (err == PDB_ERR_MORE) {
                dt->dt_state = 21;
                return err;
              }

              /*  Everything else is unexpected.
               */
              cl_log_errno(
                  cl, CL_LEVEL_FAIL, "pdb_iterator_next", err,
                  "id=%llx, sub=%s", (unsigned long long)dt->dt_id,
                  pdb_iterator_to_string(pdb, dt->dt_sub, buf, sizeof buf));
              return err;
            }

          case 22:
            err =
                pdb_iterator_check(pdb, dt->dt_fanin, dt->dt_id, budget_inout);
            if (err == PDB_ERR_MORE) {
              dt->dt_state = 22;
              return err;
            }
            dt->dt_state = 0;

            /*  The fanin signed off on it?
             */
            if (err == 0) break;

            /*  Some unexpected error?
             */
            else if (err != GRAPHD_ERR_NO) {
              cl_log_errno(
                  cl, CL_LEVEL_FAIL, "pdb_iterator_check", err,
                  "id=%llx, fanin=%s", (unsigned long long)dt->dt_id,
                  pdb_iterator_to_string(pdb, dt->dt_fanin, buf, sizeof buf));
              return err;
            }

            /*  The fanin didn't sign off on it;
             *  we need to keep going.  Did we run
             *  out of time?
             */
            else if (GRAPHD_SABOTAGE(isa->isa_graphd, *budget_inout <= 0)) {
              dt->dt_state = 23;
              return PDB_ERR_MORE;
              case 23:
                dt->dt_state = 0;
            }
        }
      } else {
        for (;;) {
          pdb_id id_found;
          case 1:
            err = pdb_iterator_find(pdb, dt->dt_fanin, dt->dt_id, &id_found,
                                    budget_inout);
            dt->dt_state = 0;

            if (err != 0) {
              /*  We ran out of IDs?
               */
              if (err == PDB_ERR_NO) {
                *is_duplicate_out = false;
                goto done;
              }

              /*  We ran out of time?
               */
              if (err == PDB_ERR_MORE) {
                dt->dt_state = 1;
                return err;
              }

              cl_log_errno(
                  cl, CL_LEVEL_FAIL, "pdb_iterator_find", err,
                  "id=%llx, fanin=%s", (unsigned long long)dt->dt_id,
                  pdb_iterator_to_string(pdb, dt->dt_fanin, buf, sizeof buf));
              return err;
            }
            if (id_found != dt->dt_id) {
              dt->dt_n_ok = 1;
              dt->dt_id = id_found;
            } else {
              if (dt->dt_n_ok++ == 1) break;
            }
          case 2:
            err = pdb_iterator_find(pdb, dt->dt_sub, dt->dt_id, &id_found,
                                    budget_inout);
            dt->dt_state = 0;

            if (err != 0) {
              /*  We ran out of IDs?
               */
              if (err == PDB_ERR_NO) {
                *is_duplicate_out = false;
                goto done;
              }

              /*   We ran out of time?
               */
              if (err == PDB_ERR_MORE) {
                dt->dt_state = 2;
                return err;
              }

              cl_log_errno(
                  cl, CL_LEVEL_FAIL, "pdb_iterator_find", err,
                  "id=%llx, sub_it=%s", (unsigned long long)dt->dt_id,
                  pdb_iterator_to_string(pdb, dt->dt_sub, buf, sizeof buf));
              return err;
            }

            if (id_found != dt->dt_id) {
              dt->dt_n_ok = 1;
              dt->dt_id = id_found;
            } else if (dt->dt_n_ok++ == 1)
              break;

            if (GRAPHD_SABOTAGE(isa->isa_graphd, *budget_inout <= 0)) {
              dt->dt_state = 3;
              return PDB_ERR_MORE;
              case 3:
                dt->dt_state = 0;
            }
        }

        /*  When we arrive here, both iterators
         *  have signed off on their first overlapping
         *  id, dt->dt_id.   Which means that
         *  this one's a duplicate.
         */
        cl_assert(cl, dt->dt_n_ok == 2);
      }

      /*  When we arrive here, dt->dt_id is the
       *  first overlap between fan-in and
       *  subiterator.
       */
      *is_duplicate_out = true;
      break;
  }

done:
  pdb_iterator_destroy(pdb, &dt->dt_fanin);
  dt->dt_state = 0;

  cl_log(cl, CL_LEVEL_VERBOSE, "isa_dup_test_intersect: %lld is %s ($%lld)",
         (long long)id, *is_duplicate_out ? "a duplicate" : "new",
         budget_in - *budget_inout);
  return 0;
}

static int isa_find_loc(pdb_handle *pdb, pdb_iterator *it, pdb_id id_in,
                        pdb_id *id_out, pdb_budget *budget_inout,
                        char const *file, int line) {
  graphd_iterator_isa *isa = it->it_theory;
  cl_handle *cl = isa->isa_cl;
  char buf[200];
  int err;

  if ((err = pdb_iterator_refresh(pdb, it)) == PDB_ERR_ALREADY)
    cl_notreached(cl,
                  "isa_find_loc: "
                  "it=%p:%s, sorted? %d, sorted-valid? %d [called from %s:%d]",
                  (void *)it, pdb_iterator_to_string(pdb, it, buf, sizeof buf),
                  pdb_iterator_sorted(pdb, it),
                  pdb_iterator_sorted_valid(pdb, it), file, line);

  if (err == 0)
    return pdb_iterator_find_loc(pdb, it, id_in, id_out, budget_inout, file,
                                 line);
  return err;
}

static int isa_reset(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_isa *isa = it->it_theory;
  char buf[200];
  int err;

  pdb_rxs_log(pdb, "RESET %p isa", (void *)it);

  /* Reset our position in the duplicate tracker.
   */
  pdb_iterator_destroy(pdb, &isa->isa_dup.dt_fanin);
  pdb_iterator_destroy(pdb, &isa->isa_dup.dt_sub);

  isa->isa_dup.dt_storable_position = 0;
  isa->isa_dup.dt_id = PDB_ID_NONE;
  isa->isa_dup.dt_state = 0;
  isa->isa_dup.dt_n_ok = 0;

  /*  Reset the subiterator.
   */
  err = pdb_iterator_reset(pdb, isa->isa_sub);
  if (err != 0) {
    cl_log_errno(isa->isa_cl, CL_LEVEL_FAIL, "pdb_iterator_reset", err,
                 "while resetting %s",
                 pdb_iterator_to_string(pdb, isa->isa_sub, buf, sizeof buf));
    return err;
  }

  isa->isa_next_tmp = PDB_ID_NONE;
  isa->isa_sub_source = PDB_ID_NONE;
  isa->isa_last_id = PDB_ID_NONE;
  isa->isa_sub_has_position = true;
  isa->isa_eof = false;

  pdb_iterator_call_reset(pdb, it);

  return err;
}

static int isa_statistics_freeze(pdb_handle *pdb, pdb_iterator *it,
                                 cm_buffer *buf) {
  int err;

  /*  Statistics.
   */
  if (pdb_iterator_statistics_done(pdb, it)) {
    err = pdb_iterator_freeze_statistics(pdb, buf, it);
    if (err != 0) return err;
  } else {
    /* Regardless of whom we're freezing, take the statistics
     * state from the original, not from the clone.  (The
     * clone really has no independent statistics going on,
     * it just donates time to the original.)
     */
    graphd_iterator_isa *isa = it->it_original->it_theory;
    char const *csep = ":";
    size_t i;

    cl_assert(isa->isa_cl, it->it_original->it_type == &isa_type);

    err = graphd_iterator_util_freeze_subiterator(
        pdb, isa->isa_statistics_sub, PDB_ITERATOR_FREEZE_EVERYTHING, buf);
    if (err != 0) return err;

    err = cm_buffer_sprintf(buf, ":%d:%zu:%zu", isa->isa_statistics_state,
                            isa->isa_statistics_id_n, isa->isa_sub_id_trial_n);
    if (err != 0) return err;

    for (i = 0; i <= isa->isa_statistics_id_n && i < GRAPHD_ISA_N_SAMPLES;
         i++) {
      err = cm_buffer_sprintf(buf, "%s%llu", csep,
                              (unsigned long long)isa->isa_sub_id[i]);
      if (err != 0) return err;

      csep = ",";
    }
  }
  return 0;
}

static int isa_statistics_thaw(pdb_iterator *it, char const **s_ptr,
                               char const *e, pdb_iterator_base *pib,
                               cl_loglevel loglevel) {
  int err = 0;
  graphd_iterator_isa *isa = it->it_theory;
  pdb_handle *pdb = isa->isa_graphd->g_pdb;
  cl_handle *cl = isa->isa_cl;
  char const *s = *s_ptr;

  /*  Statistics.
   */
  if (s < e && (*s == '(' || *s == '-')) {
    size_t i;

    /*  Still in the middle of the statistics phase.
     */
    cl_assert(cl, isa->isa_statistics_sub == NULL);
    err = graphd_iterator_util_thaw_subiterator(
        isa->isa_graphd, &s, e, pib, loglevel, &isa->isa_statistics_sub);
    if (err != 0) return err;

    err = pdb_iterator_util_thaw(
        pdb, &s, e, ":%d:%zu:%zu:", &isa->isa_statistics_state,
        &isa->isa_statistics_id_n, &isa->isa_sub_id_trial_n);
    if (err != 0) {
      cl_log_errno(isa->isa_cl, CL_LEVEL_FAIL, "pdb_iterator_util_thaw", err,
                   "can't parse statistics section");
      return err;
    }

    if (isa->isa_statistics_id_n > GRAPHD_ISA_N_SAMPLES) {
      isa->isa_statistics_state = 0;
      cl_log(isa->isa_cl, CL_LEVEL_FAIL,
             "isa_statistics_thaw: can "
             "handle at most %d statistics results, "
             "attempt to unthaw %zu?",
             GRAPHD_ISA_N_SAMPLES, isa->isa_statistics_id_n);
      return GRAPHD_ERR_SEMANTICS;
    }
    for (i = 0; i <= isa->isa_statistics_id_n && i < GRAPHD_ISA_N_SAMPLES;
         i++) {
      if (s < e && (*s == ':' || *s == ',')) s++;

      err = pdb_iterator_util_thaw(pdb, &s, e, "%{id}", isa->isa_sub_id + i);
      if (err != 0) {
        cl_log_errno(isa->isa_cl, CL_LEVEL_FAIL, "pdb_iterator_util_thaw", err,
                     "failed to thaw sub-id %d", (int)i);
        isa->isa_statistics_state = 0;

        return err;
      }
    }
    *s_ptr = s;

    isa->isa_dup.dt_method = ISA_DT_METHOD_UNSPECIFIED;
    cl_assert(cl, !pdb_iterator_statistics_done(pdb, it));
  } else {
    pdb_budget nc, fc, cc;
    unsigned long long n;

    err = pdb_iterator_util_thaw(pdb, s_ptr, e, "%{budget}:%{next[+find]}:%llu",
                                 &cc, &nc, &fc, &n);
    if (err != 0) return err;

    pdb_iterator_n_set(pdb, it, n);
    pdb_iterator_check_cost_set(pdb, it, cc);
    pdb_iterator_next_cost_set(pdb, it, nc);
    pdb_iterator_find_cost_set(pdb, it, fc);

    /*  If we have an ordering, interpret that
     *  to imply that we're actually ordered.
     */
    pdb_iterator_ordered_set(pdb, it, pdb_iterator_ordering(pdb, it) != NULL);

    /*  If we're ordered, our subiterator must be, too.
     */
    if (pdb_iterator_ordered(pdb, it))
      pdb_iterator_ordered_set(pdb, isa->isa_sub, true);

    pdb_iterator_statistics_done_set(pdb, it);

    /*  Since we now know our statistics, recalculate
     *  the primitive summary of the subiterator -
     *  that happens at the end of the statistics
     *  process.
     */
    err = isa_sub_primitive_summary(pdb, it);
    if (err != 0) {
      char buf[200];
      cl_log_errno(isa->isa_cl, CL_LEVEL_FAIL, "isa_sub_psum", err, "it=%s",
                   pdb_iterator_to_string(pdb, it, buf, sizeof buf));
      return err;
    }

    isa_dup_pick_method(pdb, it);
    cl_assert(cl, oisa(it)->isa_dup.dt_method != ISA_DT_METHOD_UNSPECIFIED);
  }
  return 0;
}

static int isa_dup_initialize(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_isa *isa = it->it_theory;

  isa_dup_pick_method(pdb, it);
  isa->isa_dup.dt_method = oisa(it)->isa_dup.dt_method;

  if (oisa(it)->isa_dup.dt_method == ISA_DT_METHOD_STORABLE) {
    int err = isa_cache_create(pdb, it->it_original);
    if (err != 0) return err;
  }
  cl_assert(isa->isa_cl,
            oisa(it)->isa_dup.dt_method != ISA_DT_METHOD_UNSPECIFIED);
  return 0;
}

/*  We've finished experimenting; figure out what it all means.
 */
static void isa_statistics_complete(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_isa *isa = it->it_theory;
  cl_handle *cl = isa->isa_cl;

  pdb_id isa_high, max_low, sub_high;
  unsigned long long sub_n, upper_bound, sub_spread, isa_n, cooked_sub_n;
  double average_loss;
  pdb_budget sub_next_cost;
  char buf[200];
  char const *sub_ordering;
  pdb_budget next_cost;

  cl_assert(cl, it->it_original == it);
  cl_assert(cl, isa->isa_sub != NULL);
  cl_assert(cl, pdb_iterator_statistics_done(pdb, isa->isa_sub));
  cl_assert(cl, isa->isa_sub_id_trial_n >= isa->isa_statistics_id_n);

  upper_bound = pdb_iterator_spread(pdb, it);
  cl_assert(cl, upper_bound > 0);

  /*  What's the relation between subiterator results produced
   *  and is-a results produced?
   *
   *  isa_sub_id_trial_n / average_loss = isa->isa_statistics_id_n
   */
  if (isa->isa_statistics_id_n == 0)
    average_loss = 2 * isa->isa_sub_id_trial_n;
  else
    average_loss = (double)isa->isa_sub_id_trial_n / isa->isa_statistics_id_n;
  if (average_loss > upper_bound) average_loss = upper_bound;
  cl_assert(cl, average_loss >= 1);

  /*  How many results does the subiterator plan to return?
   */
  sub_n = pdb_iterator_n(pdb, isa->isa_sub);

  /*  "Loss" is the factor by which the subiterator results
   *  are bigger than the is-a results.  There are two causes
   *  of loss.
   *
   *  One, loss through overlap.  Multiple subiterator
   *  results all point to the same id.
   *
   *  Two, loss through range - the is-a results might be
   *  restricted to a certain range, and we can't predict
   *  where subiterator results will point - we'll have to
   *  examine all of the subiterator range, even though it'll
   *  likely not point into the isa's range.
   *
   *  The test iteration at the beginning of a range
   *  is a particularly poor predictor of the second
   *  kind of loss.  The second kind of loss is more
   *  likely to happen in cases where the first kind
   *  is relatively low - if there's one pointed-to for
   *  one pointer, they're likely to be close in value.
   *
   *    n(isa) : n(sub) = spread(isa) : spread(sub)
   */

  max_low = isa->isa_sub->it_low;
  if (it->it_low > max_low) max_low = it->it_low;

  isa_high = it->it_high;
  if (isa_high == PDB_ITERATOR_HIGH_ANY) isa_high = pdb_primitive_n(pdb) - 1;

  sub_high = isa->isa_sub->it_high;
  if (sub_high == PDB_ITERATOR_HIGH_ANY) sub_high = pdb_primitive_n(pdb);
  sub_spread = sub_high > max_low ? sub_high - max_low : 1;

  /*  If the subiterator and the isa-domain don't overlap, we
   *  don't have to compensate for loss type 2 - our results
   *  already point elsewhere.
   */
  if (isa->isa_sub->it_low >= it->it_high) {
    cooked_sub_n = sub_n;
  } else {
    unsigned long long shared_spread, isa_spread;

    isa_spread = isa_high > max_low ? isa_high - max_low : 1;
    shared_spread = sub_spread < isa_spread ? sub_spread : isa_spread;

    /*
     *  To compensate for loss type II, we use as basis for our
     *  result estimate N not the n of the subiterator, but the
     *  n of the subiterator scaled by the shared spread of the
     *  isa and the subiterator.
     *
     *  cooked_sub_n : shared spread  = sub_n : raw sub spread.
     */
    cooked_sub_n = ((sub_n > sub_spread ? sub_spread : sub_n) * shared_spread) /
                   sub_spread;
    if (cooked_sub_n > shared_spread) cooked_sub_n = shared_spread;
  }

  /*  Loss type I: pretend that whatever we experienced getting
   *  the first five results was typical for the overall process.
   */
  isa_n = cooked_sub_n >= average_loss ? cooked_sub_n / average_loss : 1;
  if (isa_n < GRAPHD_ISA_N_SAMPLES) isa_n = GRAPHD_ISA_N_SAMPLES;

  pdb_iterator_n_set(pdb, it, isa_n);

  /*  next cost:
   *	cost of producing all subiterator values,
   *	times lookup costs for all of them,
   * 	plus cost for keeping the duplicates O(n^2).
   */
  sub_next_cost = pdb_iterator_next_cost(pdb, isa->isa_sub);
  next_cost = ((sub_n > sub_spread ? sub_spread : sub_n) *
               (sub_next_cost + PDB_COST_PRIMITIVE)) /
                  isa_n +
              isa_n / 8000;
  pdb_iterator_next_cost_set(pdb, it, next_cost);

  cl_log(cl, CL_LEVEL_VERBOSE,
         "isa_statistics_complete: trials %llu, loss %g, spread: sub %llu, "
         "max_low %llu, sub hi %llu, isa hi %llu;  n %llu (sub_n: %llu, "
         "sub_spread %llu, cooked sub_n %llu), next_cost %llu (sub_next_cost "
         "%llu)",
         (unsigned long long)isa->isa_sub_id_trial_n, average_loss, sub_spread,
         (unsigned long long)max_low, (unsigned long long)isa->isa_sub->it_high,
         (unsigned long long)it->it_high, isa_n, sub_n, sub_spread,
         cooked_sub_n, (unsigned long long)next_cost,
         (unsigned long long)sub_next_cost);

  /* No find cost - we're not sorted.
   */
  pdb_iterator_find_cost_set(pdb, it, 0);

  /* check cost: This seems counterintuitive, but on average, the
   * 	arbitrary primitive passed into this will have less
   * 	than one other primitive pointing to it, which will
   * 	take an array read and an element read (via the
   * 	inverse index) to check.
   */
  pdb_iterator_check_cost_set(pdb, it,
                              PDB_COST_GMAP_ARRAY + PDB_COST_GMAP_ELEMENT +
                                  pdb_iterator_check_cost(pdb, isa->isa_sub));

  /* ordering: If the subiterator is ordered, and we're
   * 	ordered by the subiterator, we're ordered.
   */
  if (pdb_iterator_ordered_valid(pdb, isa->isa_sub) &&
      pdb_iterator_ordered(pdb, isa->isa_sub) &&
      (sub_ordering = pdb_iterator_ordering(pdb, isa->isa_sub)) != NULL &&
      pdb_iterator_ordering_wants(pdb, it, sub_ordering)) {
    pdb_iterator_ordered_set(pdb, it, true);
  } else {
    if (pdb_iterator_ordering(pdb, it) != NULL)
      cl_log(cl, CL_LEVEL_VERBOSE,
             "isa_statistics: wanted to"
             " be ordered %s, but subiterator %s has "
             "o=%s, o?=%s",
             pdb_iterator_ordering(pdb, it),
             pdb_iterator_to_string(pdb, isa->isa_sub, buf, sizeof buf),
             pdb_iterator_ordering(pdb, isa->isa_sub) == NULL
                 ? "null"
                 : pdb_iterator_ordering(pdb, isa->isa_sub),
             pdb_iterator_ordered_valid(pdb, isa->isa_sub)
                 ? (pdb_iterator_ordered(pdb, isa->isa_sub) ? "true" : "false")
                 : "invalid");

    /*  Clear the ordering - if we get frozen and thawed,
     *  the presence of an ordering in a statistics-completed
     *  iterator will be read to imply that the iterator
     *  actually *is* ordered.  And we're not.
     */
    pdb_iterator_ordered_set(pdb, it, false);
    pdb_iterator_ordering_set(pdb, it, NULL);
  }
}

static int isa_statistics(pdb_handle *pdb, pdb_iterator *it,
                          pdb_budget *budget_inout) {
  graphd_iterator_isa *isa = it->it_theory;
  cl_handle *cl = isa->isa_cl;
  pdb_budget sub_check_cost, budget_in = *budget_inout;
  char buf[200];
  int err;

  cl_assert(cl, it->it_original == it);
  pdb_rxs_push(pdb, "STAT %p isa ($%lld)", (void *)it, (long long)budget_in);

  *budget_inout -= PDB_COST_FUNCTION_CALL;

  for (;;) {
    if (!pdb_iterator_statistics_done(pdb, isa->isa_sub)) {
      err = pdb_iterator_statistics(pdb, isa->isa_sub, budget_inout);
      if (err != 0) {
        if (err != PDB_ERR_MORE)
          cl_log_errno(
              cl, CL_LEVEL_FAIL, "pdb_iterator_statistics", err,
              "subiterator=%s",
              pdb_iterator_to_string(pdb, isa->isa_sub, buf, sizeof buf));
        goto err;
      }
    }

    err = pdb_iterator_refresh_pointer(pdb, &isa->isa_sub);
    if (err == PDB_ERR_ALREADY) {
      err = 0;
      break;
    }
    if (err != 0) {
      if (err != PDB_ERR_MORE)
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_refresh_pointer", err,
                     "subiterator=%s", pdb_iterator_to_string(pdb, isa->isa_sub,
                                                              buf, sizeof buf));
      goto err;
    }
    it->it_id = pdb_iterator_new_id(pdb);
  }

  cl_assert(cl, pdb_iterator_statistics_done(pdb, isa->isa_sub));

  switch (isa->isa_statistics_state) {
    default:
    case 0:
      /*  If the subiterator has reduced to a tractable
       *  set of numbers, become a tractable set of numbers
       *  ourselves.
       */
      if (pdb_iterator_ordering(pdb, it) == NULL &&
          (pdb_iterator_n(pdb, isa->isa_sub) *
           (pdb_iterator_next_cost(pdb, isa->isa_sub) + PDB_COST_PRIMITIVE)) <=
              GRAPHD_ISA_INLINE_BUDGET_TOTAL) {
        pdb_iterator *new_it;
        err = isa_become_small_set(isa->isa_graphd, isa->isa_linkage,
                                   isa->isa_sub, it->it_low, it->it_high,
                                   isa->isa_direction,
                                   pdb_iterator_ordering(pdb, it), &new_it);
        if (err == 0) {
          /*  The new iterator is ordered iff we
           *  are intrinsically ordered.
           */
          if (pdb_iterator_ordering(pdb, it) != NULL &&
              (isa->isa_direction == GRAPHD_DIRECTION_FORWARD ||
               isa->isa_direction == GRAPHD_DIRECTION_BACKWARD)) {
            pdb_iterator_ordered_set(pdb, new_it, true);
            pdb_iterator_ordering_set(pdb, new_it,
                                      pdb_iterator_ordering(pdb, it));
          }
          err = pdb_iterator_substitute(pdb, it, new_it);
          cl_assert(cl, err == 0);

          pdb_rxs_pop(pdb, "STAT %p isa small set ($%lld)", (void *)it,
                      (long long)(budget_in - *budget_inout));
          return 0;
        }
      }

      /*  Use the subiterator's N as a first approximation
       *  of our own.  (We're going to improve on that later.)
       */
      pdb_iterator_n_set(pdb, it, pdb_iterator_n(pdb, isa->isa_sub));

      /*  Refresh the approximation of our check cost as
       *  that of the subiterator, plus one fresh array
       *  lookup.
       */
      sub_check_cost = pdb_iterator_check_cost(pdb, isa->isa_sub);
      pdb_iterator_check_cost_set(
          pdb, it,
          PDB_COST_GMAP_ARRAY + PDB_COST_GMAP_ELEMENT + sub_check_cost);

      /*  Cache the subiterator's primitive summary.
       */
      err = isa_sub_primitive_summary(pdb, it);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "isa_sub_primitive_summary", err,
                     "it=%s", pdb_iterator_to_string(pdb, it, buf, sizeof buf));
        goto err;
      }

      /*  Now that we've got *its* statistics, clone the subiterator.
       *  We're going to use it to produce some test items, and that
       *  mustn't interfere with checks or real production going on
       *  in parallel.
       */

      /*  We may have had a leftover isa_statistics_sub
       *  from a previous half-finished statistics run that we
       *  failed to recover from the iterator resource store.
       *  Usually, this pointer will be NULL, and the next
       *  call will do nothing.
       */
      pdb_iterator_destroy(pdb, &isa->isa_statistics_sub);

      /*  Clone the subiterator.
       */
      err = pdb_iterator_clone(pdb, isa->isa_sub, &isa->isa_statistics_sub);
      if (err != 0) {
        cl_log_errno(
            cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "%s",
            pdb_iterator_to_string(pdb, isa->isa_sub, buf, sizeof buf));
        goto err;
      }

      /*  The clone is just as statistics-done as its original.
       */
      cl_assert(isa->isa_cl,
                pdb_iterator_statistics_done(pdb, isa->isa_statistics_sub));

      /*  Reset the statistics subiterator after cloning -
       *  the original may have been iterated over as part
       *  of a check().
       */
      err = pdb_iterator_reset(pdb, isa->isa_statistics_sub);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_reset", err, "it=%s",
                     pdb_iterator_to_string(pdb, isa->isa_statistics_sub, buf,
                                            sizeof buf));
        goto err;
      }

    /*  estimate our n:
     *	Pull a few values of out the destination;
     *	see what the average loss is (how many do we
     *	pull until we have 5 different candidates?).
     *
     * 	If we run out of subiterator values, great - go
     *  	to the fixed value set.
     */
    case 1:
      while (isa->isa_statistics_id_n < GRAPHD_ISA_N_SAMPLES) {
        pdb_id id;
        size_t i;

        if (*budget_inout < PDB_COST_PRIMITIVE) {
          isa->isa_statistics_state = 1;
          err = PDB_ERR_MORE;
          goto err;
        }

        /* Read one.
         */
        err = graphd_iterator_isa_run_next(
            isa->isa_graphd, it, isa->isa_statistics_sub, isa->isa_linkage,
            &isa->isa_sub_id_trial_n,
            isa->isa_sub_id + isa->isa_statistics_id_n, budget_inout, true);

        if (err == GRAPHD_ERR_NO) {
          pdb_iterator *new_it;

          /* Yay, we're fixed-size. */
          err = graphd_iterator_fixed_create_array(
              isa->isa_graphd, isa->isa_sub_id, isa->isa_statistics_id_n,
              it->it_low, it->it_high, it->it_forward, &new_it);
          if (err != 0) {
            cl_log_errno(cl, CL_LEVEL_FAIL,
                         "graphd_iterator_fixed_create_array", err, "n=%zu",
                         isa->isa_statistics_id_n);
            goto err;
          }

          /*  The new iterator is ordered iff we
           *  are intrinsically ordered.
           */
          if (pdb_iterator_ordering(pdb, it) != NULL &&
              (isa->isa_direction == GRAPHD_DIRECTION_FORWARD ||
               isa->isa_direction == GRAPHD_DIRECTION_BACKWARD)) {
            pdb_iterator_ordered_set(pdb, new_it, true);
            pdb_iterator_ordering_set(pdb, new_it,
                                      pdb_iterator_ordering(pdb, it));
          }
          err = pdb_iterator_substitute(pdb, it, new_it);
          cl_assert(cl, err == 0);

          cl_log(cl, CL_LEVEL_VERBOSE, "redirect to %s ($%lld)",
                 pdb_iterator_to_string(pdb, it, buf, sizeof buf),
                 (long long)(budget_in - *budget_inout));

          pdb_rxs_pop(pdb, "STAT %p isa redirect ($%lld)", (void *)it,
                      (long long)(budget_in - *budget_inout));

          return pdb_iterator_statistics(pdb, it, budget_inout);
        } else if (err != 0) {
          if (err == PDB_ERR_MORE)
            isa->isa_statistics_state = 1;
          else {
            isa->isa_statistics_state = 0;
            cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_isa_run_next", err,
                         "it=%s",
                         pdb_iterator_to_string(pdb, it, buf, sizeof buf));
          }
          goto err;
        }

        isa->isa_next_tmp = id = isa->isa_sub_id[isa->isa_statistics_id_n];

        /*  Have we already seen this result?
         */
        for (i = 0; i < isa->isa_statistics_id_n; i++)
          if (isa->isa_sub_id[i] == id) break;

        if (i < isa->isa_statistics_id_n) {
          /* We've already seen this result.
           */
          cl_log(cl, CL_LEVEL_SPEW,
                 "isa_statistics: "
                 "%llx is a duplicate",
                 (unsigned long long)id);

          if (GRAPHD_SABOTAGE(isa->isa_graphd, *budget_inout <= 0)) {
            /* Resume at the top
             *  of this loop.
             */
            isa->isa_statistics_state = 1;
            err = PDB_ERR_MORE;
            goto err;
          }
          continue;
        }

        /* Accept the id candidate we stored. */
        isa->isa_statistics_id_n++;
      }
      break;
  }

  /*  No restarts after this, just calculation.
   */

  /* Reset the "duplicate detector".
   */
  isa_dup_finish(pdb, &isa->isa_dup);
  isa_dup_clear(&isa->isa_dup);

  /*  Free the statistics subiterator sample.
   */
  pdb_iterator_destroy(pdb, &isa->isa_statistics_sub);
  isa->isa_statistics_state = 0;

  isa_statistics_complete(pdb, it);
  pdb_iterator_statistics_done_set(pdb, it);

  err = isa_dup_initialize(pdb, it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "isa_dup_initialize", err, "it=%s",
                 pdb_iterator_to_string(pdb, it, buf, sizeof buf));
    goto err;
  }
  cl_assert(cl, oisa(it)->isa_dup.dt_method != ISA_DT_METHOD_UNSPECIFIED);

  if (it->it_displayname != NULL) {
    cm_handle *cm = pdb_mem(pdb);
    cm_free(cm, it->it_displayname);
    it->it_displayname = NULL;
  }

  pdb_rxs_pop(
      pdb, "STAT %p isa %s: n=%llu cc=%llu nc=%llu fc=%llu%s%s ($%lld)",
      (void *)it, pdb_iterator_to_string(pdb, it, buf, sizeof buf),
      (unsigned long long)pdb_iterator_n(pdb, it),
      (unsigned long long)pdb_iterator_check_cost(pdb, it),
      (unsigned long long)pdb_iterator_next_cost(pdb, it),
      (unsigned long long)pdb_iterator_find_cost(pdb, it),
      pdb_iterator_ordered(pdb, it) ? ", o=" : "",
      pdb_iterator_ordered(pdb, it) ? pdb_iterator_ordering(pdb, it) : "",
      (long long)(budget_in - *budget_inout));
  return 0;

err:
  pdb_rxs_pop(pdb, "STAT %p isa %s ($%lld)", (void *)it,
              err == PDB_ERR_MORE ? "suspend" : graphd_strerror(err),
              (long long)(budget_in - *budget_inout));
  return err;
}

static int isa_check(pdb_handle *pdb, pdb_iterator *it, pdb_id check_id,
                     pdb_budget *budget_inout) {
  graphd_iterator_isa *const isa = it->it_theory;
  cl_handle *const cl = isa->isa_cl;
  pdb_iterator *const sub = isa->isa_sub;
  pdb_budget const budget_in = *budget_inout;
  char buf[200];
  unsigned long it_id = it->it_id;
  int err = 0;
  bool exists;

#undef func
#define func "isa_check"

  unsigned long long upper_bound;
  unsigned long long high_sub_n, low_sub_n;
  pdb_budget sub_next_cost, sub_check_cost;
  pdb_budget cost_a, cost_b, cost_ab;
  unsigned long long trials_a, trials_b, fanin_n;

  if (GRAPHD_SABOTAGE(isa->isa_graphd, *budget_inout < 0)) return PDB_ERR_MORE;

  pdb_rxs_push(pdb, "CHECK %p isa %llx (state=%d)", (void *)it,
               (unsigned long long)check_id, it->it_call_state);

  *budget_inout -= PDB_COST_FUNCTION_CALL;
  GRAPHD_IS_ISA(cl, isa);
  cl_assert(cl, sub != NULL);

  /*  As long as you're not self-aware, spend a little
   *  time on learning more about yourself.
   */
  if (*budget_inout > 0 && !pdb_iterator_statistics_done(pdb, it)) {
    pdb_budget research_budget;

    if (*budget_inout <= 10)
      research_budget = 1;
    else
      research_budget = *budget_inout / 10;
    *budget_inout -= research_budget;

    err = pdb_iterator_statistics(pdb, it, &research_budget);

    if (research_budget > 0) *budget_inout += research_budget;

    /* If we ended up with PDB_ERR_MORE and
     *  need to spend more time on research,
     *  nevertheless go on and check the
     *  passed-in ID with what we have.
     */
    if (err != 0 && err != PDB_ERR_MORE) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_statistics", err, "it=%s",
                   pdb_iterator_to_string(pdb, it, buf, sizeof buf));
      goto done;
    }
  }

  err = 0;
  if (it_id != it->it_id ||
      (err = pdb_iterator_refresh(pdb, it)) != PDB_ERR_ALREADY) {
    if (err == 0) {
      pdb_rxs_pop(pdb, "CHECK %p isa redirect ($%lld)", (void *)it,
                  (long long)(budget_in - *budget_inout));
      pdb_iterator_account_charge_budget(pdb, it, check);

      return pdb_iterator_check(pdb, it, check_id, budget_inout);
    }
    goto done;
  }

  switch (it->it_call_state) {
    default:
      RESUME_STATE(it, 0)
      GRAPHD_IS_ISA(cl, isa);

      err = graphd_check_cache_test(isa->isa_graphd, &oisa(it)->isa_ccache,
                                    check_id, &exists);
      if (err == 0) {
        *budget_inout -= 1;
        cl_log(cl, CL_LEVEL_VERBOSE, "isa_check: cached result");
        err = exists ? 0 : GRAPHD_ERR_NO;
        goto done_dont_update_caches;
      }
      if (err != GRAPHD_ERR_NO) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_check_cache_test", err,
                     "id=%llx", (unsigned long long)check_id);
        goto done_dont_update_caches;
      }

      /*  If we are doing collision detection via
       *  a storable, we remember every value we've
       *  ever returned.  So, if this value *is* in the
       *  system, we might be able to just check that
       *  quickly.
       *
       *  Unfortunately, negative results don't mean that
       *  it can't be returned in the future...
       */

      cl_assert(cl, sub != NULL);

      if (isa->isa_dup.dt_method == ISA_DT_METHOD_STORABLE) {
        if (graphd_iterator_isa_storable_check(oisa(it)->isa_cache, check_id)) {
          cl_log(cl, CL_LEVEL_VERBOSE,
                 "isa_check: %llx is cached in "
                 "storable",
                 (unsigned long long)check_id);
          err = 0;
          goto done;
        }
        if (graphd_iterator_isa_storable_complete(oisa(it)->isa_cache)) {
          err = GRAPHD_ERR_NO;
          goto done;
        }
      }

      /* (Re)initialize the cached fanin.
       */
      pdb_iterator_destroy(pdb, &isa->isa_fanin);
      err = isa_fanin_create(pdb, it, sub, check_id, 0, PDB_ITERATOR_HIGH_ANY,
                             &isa->isa_fanin);
      if (err != 0) {
        *budget_inout -= PDB_COST_HMAP_ELEMENT;
        goto done;
      }
      *budget_inout -= PDB_COST_GMAP_ARRAY;

      if (pdb_iterator_null_is_instance(pdb, isa->isa_fanin) ||
          isa->isa_fanin->it_low >= sub->it_high ||
          isa->isa_fanin->it_high <= sub->it_low) {
        pdb_iterator_destroy(pdb, &isa->isa_fanin);
        err = GRAPHD_ERR_NO;
        goto done;
      }

      /*  We know our fan-in is efficient, but we don't know
       *  our subiterator.  Depending on what it is, it may or
       *  may not be a good idea to run on-or-after checks
       *  against it.
       *
       *  Possible algorithms:
       *	(a) call NEXT on the fan-in, CHECK on the individuals
       *	(b) call NEXT on the sub iterator, CHECK the fan-in.
       *	(c) ON-OR-AFTER the two arrays against each other
       *	(d) INTERSECT-ANY the two arrays.
       */
      upper_bound = pdb_primitive_n(pdb);
      cl_assert(cl, upper_bound > 0);

      sub_next_cost = pdb_iterator_next_cost_valid(pdb, sub)
                          ? pdb_iterator_next_cost(pdb, sub)
                          : 100000;
      sub_check_cost = pdb_iterator_check_cost_valid(pdb, sub)
                           ? pdb_iterator_check_cost(pdb, sub)
                           : 100000;
      high_sub_n = pdb_iterator_n_valid(pdb, sub) ? pdb_iterator_n(pdb, sub)
                                                  : upper_bound;
      low_sub_n = pdb_iterator_n_valid(pdb, sub) ? pdb_iterator_n(pdb, sub) : 1;
      fanin_n = pdb_iterator_n(pdb, isa->isa_fanin);
      if (fanin_n == 0) fanin_n = 1;

      if (low_sub_n == 0) low_sub_n = 1;

      /*  How many rounds will we run on average?
       *
       *  At worst, we're going to run once for each ID in the
       *  "next" iterator.
       */
      trials_a = fanin_n;

      /*  But if the "check" iterator is very permissive,
       *  odds are an early trial against it will succeed.
       *
       *  Specifically, on average, we're only going to have
       *  to offer upper_bound / iterator_n items before one
       *  gets through.
       *
       *  Of course, if we guessed at the subiterator's sub_n,
       *  we can't make that assumption.
       */
      if (trials_a > upper_bound / low_sub_n)
        trials_a = upper_bound / low_sub_n;

      /*  The average cost of finding a candidate this way:
       *  A next + a check, times how ever many it'll take
       *  to either run out or get accepted.
       */
      cost_a = (pdb_iterator_next_cost(pdb, isa->isa_fanin) + sub_check_cost) *
               trials_a;
      cl_log(cl, CL_LEVEL_VERBOSE,
             "cost_a: %llu = nc=%lld + scc=%lld * trials_a=%lld", cost_a,
             (long long)pdb_iterator_next_cost(pdb, isa->isa_fanin),
             (long long)sub_check_cost, (long long)trials_a);

      /*  How many rounds will we run on average -- same
       *  consideration, with roles switched.
       */
      trials_b = high_sub_n;
      if (trials_b > upper_bound / fanin_n) trials_b = upper_bound / fanin_n;
      cost_b = (sub_next_cost + pdb_iterator_check_cost(pdb, isa->isa_fanin)) *
               trials_b;

      /*  How many rounds will we run on average - what if
       *  we do battling "find"s?
       */
      if (!pdb_iterator_sorted(pdb, sub) ||
          !pdb_iterator_find_cost_valid(pdb, sub) ||
          !pdb_iterator_n_valid(pdb, sub))
        cost_ab = cost_a + cost_b + 1; /* prohibitive */
      else {
        unsigned long long smaller_n;
        unsigned long long n_match_both;

        smaller_n = fanin_n < high_sub_n ? fanin_n : high_sub_n;

        /* n_match_both -- how many IDs in the system
         *  	match both fanin and subiterator (if
         *  	they're independent).
         *
         * upper_bound / n_match_both is how many IDs
         *  I have to guess before guessing one that
         *  matches both.
         */
        n_match_both = (low_sub_n * fanin_n) / upper_bound;
        if (n_match_both > 1 && smaller_n > upper_bound / n_match_both)
          smaller_n = upper_bound / n_match_both;

        cost_ab = (pdb_iterator_find_cost(pdb, isa->isa_fanin) +
                   pdb_iterator_find_cost(pdb, sub)) *
                  0.5 * smaller_n;
      }
      if (cost_ab < cost_a && cost_ab < cost_b) {
        /*  Do mutual on-or-afters, starting with the fan-in.
         */
        pdb_iterator_call_reset(pdb, isa->isa_fanin);
        RESUME_STATE(it, 2)
        err = pdb_iterator_next(pdb, isa->isa_fanin, &isa->isa_sub_source,
                                budget_inout);
        if (err != 0) {
          if (err == PDB_ERR_MORE) SAVE_STATE_GOTO(it, 2);

          goto done;
        }
        RESUME_STATE(it, 5)
        do {
          pdb_id id_found;

          pdb_iterator_call_reset(pdb, sub);

          if (GRAPHD_SABOTAGE(isa->isa_graphd, *budget_inout < 0))
            SAVE_STATE_GOTO(it, 3);
          RESUME_STATE(it, 3)
          err = pdb_iterator_find(pdb, sub, isa->isa_sub_source, &id_found,
                                  budget_inout);
          if (err != 0) {
            if (err == PDB_ERR_MORE) SAVE_STATE_GOTO(it, 3);
            goto done;
          }

          /*  We pulled an item out of the fanin
           *  iterator of check_id, and matched
           *  it against the sub-iterator.
           *  If that worked, we have a match.
           */
          isa->isa_sub_has_position = true;
          if (isa->isa_sub_source == id_found) goto done;

          isa->isa_sub_source = id_found;

          /*  The subiterator changed the ID.  We
           *  need buy-in from the fan-in.
           */
          pdb_iterator_call_reset(pdb, isa->isa_fanin);
          RESUME_STATE(it, 4)
          err = pdb_iterator_find(pdb, isa->isa_fanin, isa->isa_sub_source,
                                  &id_found, budget_inout);
          if (err != 0) {
            if (err == PDB_ERR_MORE) SAVE_STATE_GOTO(it, 4);
            goto done;
          }
          if (id_found == isa->isa_sub_source) goto done;
          isa->isa_sub_source = id_found;

        } while (!GRAPHD_SABOTAGE(isa->isa_graphd, *budget_inout <= 0));
        SAVE_STATE_GOTO(it, 5);
      } else if (cost_a < cost_b) {
        /*  Safe route: pull items out of the fan-in;
         *  check them against the subconstraint.
         *
         *  This is always possbible, regardless
         *  of whether or not the subconstraint
         *  is sorted or has had its statistics done.
         */
        cl_assert(cl, isa->isa_fanin != NULL);
        pdb_iterator_call_reset(pdb, isa->isa_fanin);
        RESUME_STATE(it, 6)
        cl_assert(cl, isa->isa_fanin != NULL);
        err = pdb_iterator_next(pdb, isa->isa_fanin, &isa->isa_sub_source,
                                budget_inout);
        if (err != 0) {
          if (err == PDB_ERR_MORE) SAVE_STATE_GOTO(it, 6);
          goto done;
        }
        cl_assert(cl, isa->isa_sub_source != PDB_ID_NONE);

        /*  Is <isa->isa_sub_source> in the subconstraint?
         */
        RESUME_STATE(it, 9)
        do {
          pdb_iterator_call_reset(pdb, sub);
          RESUME_STATE(it, 7)
          isa->isa_sub_has_position = false;
          cl_assert(cl, isa->isa_sub_source != PDB_ID_NONE);
          err = pdb_iterator_check(pdb, sub, isa->isa_sub_source, budget_inout);
          if (err == PDB_ERR_MORE) SAVE_STATE_GOTO(it, 7);
          if (err != GRAPHD_ERR_NO) goto done;

          cl_assert(cl, isa->isa_fanin != NULL);
          pdb_iterator_call_reset(pdb, isa->isa_fanin);
          RESUME_STATE(it, 8)
          err = pdb_iterator_next(pdb, isa->isa_fanin, &isa->isa_sub_source,
                                  budget_inout);
          if (err != 0) {
            if (err == PDB_ERR_MORE) SAVE_STATE_GOTO(it, 8);
            goto done;
          }
        } while (!GRAPHD_SABOTAGE(isa->isa_graphd, *budget_inout <= 0));

        SAVE_STATE_GOTO(it, 9);
      } else {
        isa->isa_sub_has_position = false;
        if ((err = pdb_iterator_reset(pdb, sub)) != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_reset", err, "sub=%s",
                       pdb_iterator_to_string(pdb, sub, buf, sizeof buf));
          goto done;
        }

        /*  Pull items out of the subconstraint,
         *  and check them against the fan-in.
         */
        pdb_iterator_call_reset(pdb, sub);
        RESUME_STATE(it, 10)
        err = pdb_iterator_next(pdb, sub, &isa->isa_sub_source, budget_inout);
        if (err != 0) {
          if (err == PDB_ERR_MORE) SAVE_STATE_GOTO(it, 10);
          goto done;
        }
        isa->isa_sub_has_position = true;

        /*  Is <isa->isa_sub_source> in the fan-in?
         */
        /*  It's better for us to go via the
         *  sub-iterator than via the fan-in,
         *  but the sub-iterator isn't sorted.
         *
         *  So, all we can do is step-and-check.
         *  ("find" won't help us.)
         */
        RESUME_STATE(it, 13)
        do {
          pdb_iterator_call_reset(pdb, isa->isa_fanin);
          RESUME_STATE(it, 11)
          cl_log(cl, CL_LEVEL_VERBOSE,
                 "isa: check id "
                 "%llx against the fan-in %s",
                 (unsigned long long)isa->isa_sub_source,
                 pdb_iterator_to_string(pdb, isa->isa_fanin, buf, sizeof buf));

          err = pdb_iterator_check(pdb, isa->isa_fanin, isa->isa_sub_source,
                                   budget_inout);
          if (err == PDB_ERR_MORE) SAVE_STATE_GOTO(it, 11);
          if (err != GRAPHD_ERR_NO) goto done;

          cl_log(cl, CL_LEVEL_VERBOSE,
                 "isa: fan-in didn't like id %llx. "
                 "Go to the next.",
                 (unsigned long long)isa->isa_sub_source);

          pdb_iterator_call_reset(pdb, sub);
          RESUME_STATE(it, 12)
          err = pdb_iterator_next(pdb, sub, &isa->isa_sub_source, budget_inout);
          if (err != 0) {
            if (err == PDB_ERR_MORE) SAVE_STATE_GOTO(it, 12);
            goto done;
          }
          isa->isa_sub_has_position = true;

        } while (!GRAPHD_SABOTAGE(isa->isa_graphd, *budget_inout <= 0));

        SAVE_STATE_GOTO(it, 13);
      }
  }

done:
  /*  If we got a definitive result (GRAPHD_ERR_NO or 0),
   *  cache that.
   */
  if (err == GRAPHD_ERR_NO) {
    err = graphd_check_cache_add(isa->isa_graphd, &oisa(it)->isa_ccache,
                                 check_id, false);
    if (err == 0)
      err = GRAPHD_ERR_NO;
    else
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_check_cache_add", err,
                   "id=%lld: false", (long long)check_id);
  } else if (err == 0) {
    err = graphd_check_cache_add(isa->isa_graphd, &oisa(it)->isa_ccache,
                                 check_id, true);
    if (err != 0)
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_check_cache_add", err,
                   "id=%lld: true", (long long)check_id);
  }

done_dont_update_caches:
  pdb_rxs_pop_test(pdb, err, budget_in - *budget_inout, "CHECK %p isa %llx",
                   (void *)it, (unsigned long long)check_id);
  goto err;

suspend:
  err = PDB_ERR_MORE;

  pdb_rxs_pop_test(pdb, err, budget_in - *budget_inout,
                   "CHECK %p isa %llx call-state=%d", (void *)it,
                   (unsigned long long)check_id, it->it_call_state);

err:
  pdb_iterator_account_charge_budget(pdb, it, check);
  return err;
}

static int isa_next_cached(pdb_handle *pdb, pdb_iterator *it, pdb_id *id_out,
                           pdb_budget *budget_inout) {
  pdb_budget budget_in = *budget_inout;
  graphd_iterator_isa *isa = it->it_theory;
  cl_handle *cl = isa->isa_cl;
  int err;
  char id_buf[80];

  cl_enter(cl, CL_LEVEL_VERBOSE, "resume_id=%s; storable_position %zu",
           pdb_id_to_string(pdb, isa->isa_resume_id, id_buf, sizeof id_buf),
           isa->isa_dup.dt_storable_position);

  *budget_inout -= PDB_COST_FUNCTION_CALL;

  /*  Make sure we know the statistics of our isa_sub.
   */
  if (!pdb_iterator_statistics_done(pdb, it)) {
    err = pdb_iterator_statistics(pdb, it, budget_inout);
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "isa_next_cached: statistics ($%lld)",
               (long long)budget_in - *budget_inout);
      return err;
    }
  }

  /* If we don't know the statistics of our isa_cache_sub,
   * clone the isa_sub.
   */
  if (oisa(it)->isa_cache_sub != NULL &&
      !pdb_iterator_statistics_done(pdb, oisa(it)->isa_cache_sub)) {
    pdb_iterator_destroy(pdb, &oisa(it)->isa_cache_sub);
    cl_assert(cl, oisa(it)->isa_cache_sub == NULL);
  }
  if (oisa(it)->isa_cache_sub == NULL) {
    err = pdb_iterator_clone(pdb, oisa(it)->isa_sub, &oisa(it)->isa_cache_sub);
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "isa_next_cached: clone fails: %s",
               graphd_strerror(err));
      return err;
    }
  }

  cl_assert(cl, oisa(it)->isa_cache_sub != NULL);
  cl_assert(cl, pdb_iterator_statistics_done(pdb, oisa(it)->isa_cache_sub));
  cl_assert(cl, oisa(it)->isa_cache != NULL);

  if (isa->isa_resume_id != PDB_ID_NONE) {
    size_t my_position;

    while (isa->isa_resume_id != PDB_ID_NONE &&
           !graphd_iterator_isa_storable_id_to_offset(
               oisa(it)->isa_cache, isa->isa_resume_id, &my_position)) {
      err = graphd_iterator_isa_storable_run(
          isa->isa_graphd, it, oisa(it)->isa_cache_sub, isa->isa_linkage,
          oisa(it)->isa_cache, budget_inout);
      if (err != 0) {
        cl_leave(cl, CL_LEVEL_VERBOSE,
                 "graphd_iterator_isa_storable_run: "
                 "%s ($%lld)",
                 graphd_strerror(err), budget_in - *budget_inout);
        return err;
      }
    }
    isa->isa_dup.dt_storable_position = my_position;
    isa->isa_dup.dt_storable_position++;

    isa->isa_resume_id = PDB_ID_NONE;
  }

  while ((isa->isa_dup.dt_storable_position >=
          graphd_iterator_isa_storable_nelems(oisa(it)->isa_cache)) ||
         (isa->isa_resume_id != PDB_ID_NONE &&
          !graphd_iterator_isa_storable_check(isa->isa_cache,
                                              isa->isa_resume_id))) {
    err = graphd_iterator_isa_storable_run(
        isa->isa_graphd, it, oisa(it)->isa_cache_sub, isa->isa_linkage,
        oisa(it)->isa_cache, budget_inout);
    if (err != 0) {
      if (err == GRAPHD_ERR_NO) isa->isa_eof = true;

      cl_leave(cl, CL_LEVEL_VERBOSE, "isa_next_cached: %s ($%lld)",
               graphd_strerror(err), budget_in - *budget_inout);
      return err;
    }
  }
  isa->isa_resume_id = PDB_ID_NONE;

  if (!graphd_iterator_isa_storable_offset_to_id(
          oisa(it)->isa_cache, isa->isa_dup.dt_storable_position, id_out)) {
    cl_log(cl, CL_LEVEL_ERROR,
           "expected cache to contain a value "
           "at position %llu",
           (unsigned long long)isa->isa_dup.dt_storable_position);
    cl_leave(cl, CL_LEVEL_VERBOSE, "unexpected error");
    return GRAPHD_ERR_NO;
  }

  isa->isa_dup.dt_storable_position++;
  isa->isa_last_id = *id_out;

  cl_leave(cl, CL_LEVEL_VERBOSE, "it=%p position %zu, id=%llx ($%lld)",
           (void *)it, isa->isa_dup.dt_storable_position,
           (unsigned long long)*id_out, (long long)(budget_in - *budget_inout));
  return 0;
}

static int isa_next_intersect(pdb_handle *pdb, pdb_iterator *it, pdb_id *id_out,
                              pdb_budget *budget_inout) {
  graphd_iterator_isa *isa = it->it_theory;
  cl_handle *cl = isa->isa_cl;
  int err;
  graph_guid guid;
  pdb_primitive pr;
  bool is_duplicate;
  char buf[200];

  *budget_inout -= PDB_COST_FUNCTION_CALL;
  if (isa->isa_eof) return GRAPHD_ERR_NO;

  cl_assert(cl, pdb_iterator_statistics_done(pdb, isa->isa_sub));
  cl_assert(cl, pdb_iterator_sorted(pdb, isa->isa_sub));

  cl_enter(cl, CL_LEVEL_VERBOSE, "(it=%p; state=%d; sub_source_id=%s)",
           (void *)it, it->it_call_state,
           pdb_id_to_string(pdb, isa->isa_sub_source, buf, sizeof buf));
  cl_assert(cl, pdb_iterator_statistics_done(pdb, it));

  switch (it->it_call_state) {
    default:
      cl_notreached(cl, "unexpected call state %d", it->it_call_state);

      RESUME_STATE(it, 0)
      for (;;) {
        RESUME_STATE(it, 1)
        if (isa->isa_sub_source != PDB_ID_NONE &&
            pdb_iterator_sorted(pdb, isa->isa_sub) &&
            !isa->isa_sub_has_position) {
          pdb_id id = pdb_iterator_forward(pdb, isa->isa_sub)
                          ? isa->isa_sub_source + 1
                          : isa->isa_sub_source - 1;

          if (!pdb_iterator_forward(pdb, isa->isa_sub) &&
              isa->isa_sub_source == 0)
            err = GRAPHD_ERR_NO;
          else {
            pdb_id id_found;

            cl_log(cl, CL_LEVEL_VERBOSE,
                   "isa_next_intersect: "
                   "catching up to on-or-after %lld",
                   (long long)id);

            err = pdb_iterator_find(pdb, isa->isa_sub, id, &id_found,
                                    budget_inout);
            if (err == 0) {
              isa->isa_sub_has_position = true;
              isa->isa_sub_source = id_found;
            }
          }
        } else {
          /*  Read a new id from the subiterator
           *  into isa->isa_sub_source.
           */
          err = pdb_iterator_next(pdb, isa->isa_sub, &isa->isa_sub_source,
                                  budget_inout);

          if (err == 0) {
            char buf[200];
            cl_log(cl, CL_LEVEL_SPEW,
                   "isa_next_intersect: "
                   "sub iterator %s "
                   "produced %llx",
                   pdb_iterator_to_string(pdb, isa->isa_sub, buf, sizeof buf),
                   (unsigned long long)isa->isa_sub_source);
            cl_assert(isa->isa_cl, isa->isa_sub_source != PDB_ID_NONE);
          } else {
            char buf[200];
            cl_log(cl, CL_LEVEL_SPEW,
                   "isa_next_intersect: sub iterator %s "
                   "returns error: %s",
                   pdb_iterator_to_string(pdb, isa->isa_sub, buf, sizeof buf),
                   graphd_strerror(err));
          }
        }
        if (err != 0) {
          if (err == PDB_ERR_MORE) {
            LEAVE_SAVE_STATE(it, 1);
          }
          goto done;
        }
        if (GRAPHD_SABOTAGE(isa->isa_graphd, *budget_inout <= 0))
          LEAVE_SAVE_STATE(it, 2);
        RESUME_STATE(it, 2)
        *budget_inout -= PDB_COST_PRIMITIVE;

        /*  Read the primitive our subiterator returned.
         */
        cl_assert(isa->isa_cl, isa->isa_sub_source != PDB_ID_NONE);
        err = pdb_id_read(pdb, isa->isa_sub_source, &pr);
        if (err == GRAPHD_ERR_NO) continue;
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err, "id=%lld",
                       (long long)isa->isa_sub_source);
          goto done;
        }

        /*  Go from the primitive to its linkage.
         */
        if (!pdb_primitive_has_linkage(&pr, isa->isa_linkage)) {
          pdb_primitive_finish(pdb, &pr);
          continue;
        }
        pdb_primitive_linkage_get(&pr, isa->isa_linkage, guid);
        pdb_primitive_finish(pdb, &pr);

        /*  Convert the linkage GUID to an ID
         */
        err = pdb_id_from_guid(pdb, &isa->isa_next_tmp, &guid);
        if (err == GRAPHD_ERR_NO)
          continue;

        else if (err != 0)
          goto done;

        cl_assert(cl, isa->isa_next_tmp != PDB_ID_NONE);
        RESUME_STATE(it, 3)
        /* Test the ID for duplicates.
         */
        cl_assert(cl, isa->isa_next_tmp != PDB_ID_NONE);

        PDB_IS_ITERATOR(cl, isa->isa_sub);
        cl_assert(cl, pdb_iterator_statistics_done(pdb, isa->isa_sub));
        cl_assert(cl, pdb_iterator_sorted(pdb, isa->isa_sub));

        if (isa->isa_dup.dt_sub != NULL) {
          PDB_IS_ITERATOR(cl, isa->isa_dup.dt_sub);
          cl_assert(cl, pdb_iterator_sorted(pdb, isa->isa_dup.dt_sub));
        }

        err = isa_dup_test_intersect(pdb, it, isa->isa_sub, &isa->isa_dup,
                                     isa->isa_next_tmp, isa->isa_sub_source,
                                     budget_inout, &is_duplicate, __FILE__,
                                     __LINE__);

        if (err == PDB_ERR_MORE) LEAVE_SAVE_STATE(it, 3);

        if (err != 0) goto done;

        if (!is_duplicate) break;
      }
  }

done:
  if (err == 0) {
    cl_assert(isa->isa_cl, isa->isa_next_tmp != PDB_ID_NONE);
    isa->isa_last_id = *id_out = isa->isa_next_tmp;

    cl_leave(isa->isa_cl, CL_LEVEL_VERBOSE, "NEXT %llx",
             (unsigned long long)isa->isa_next_tmp);
  } else if (err == GRAPHD_ERR_NO) {
    isa->isa_eof = true;
    isa->isa_sub_source = PDB_ID_NONE;
    cl_leave(isa->isa_cl, CL_LEVEL_SPEW, "done");
  } else {
    cl_leave(isa->isa_cl, CL_LEVEL_VERBOSE, "unexpected error: %s",
             graphd_strerror(err));
  }
  return err;
}

static int isa_next_loc(pdb_handle *pdb, pdb_iterator *it, pdb_id *id_out,
                        pdb_budget *budget_inout, char const *file, int line) {
  graphd_iterator_isa *isa = it->it_theory;
  cl_handle *cl = isa->isa_cl;
  pdb_budget budget_in = *budget_inout;
  int err;
  char buf[200];

#undef func
#define func "isa_next_loc"

  pdb_rxs_push(pdb, "NEXT %p isa (state=%d) [%s:%d]", (void *)it,
               it->it_call_state, file, line);

  *budget_inout -= PDB_COST_FUNCTION_CALL;

  if (!pdb_iterator_statistics_done(pdb, it)) {
    err = pdb_iterator_statistics(pdb, it, budget_inout);
    if (err != 0) {
      if (err != PDB_ERR_MORE) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_statistics", err, "it=%s",
                     pdb_iterator_to_string(pdb, it, buf, sizeof buf));
        pdb_rxs_pop(pdb,
                    "NEXT %p isa: "
                    "error in statistics: %s ($%lld)",
                    (void *)it, graphd_strerror(err),
                    (long long)(budget_in - *budget_inout));
      } else {
        pdb_rxs_pop(pdb,
                    "NEXT %p isa: "
                    "suspended in statistics"
                    "; state=%d ($%lld)",
                    (void *)it, it->it_call_state,
                    (long long)(budget_in - *budget_inout));
      }
      goto err;
    }
    cl_assert(cl, pdb_iterator_statistics_done(pdb, it));

    /*  Redirect - this may no longer be an is-a!
     */
    pdb_rxs_pop(pdb, "NEXT %p isa redirect ($%lld)", (void *)it,
                (long long)(budget_in - *budget_inout));

    return pdb_iterator_next_loc(pdb, it, id_out, budget_inout, file, line);
  }

  err = 0;
  if ((err = pdb_iterator_refresh(pdb, it)) != PDB_ERR_ALREADY) {
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_refresh", err, "it=%s",
                   pdb_iterator_to_string(pdb, it, buf, sizeof buf));
      goto unexpected_error;
    }
    pdb_rxs_pop(pdb,
                "NEXT %p isa: redirect after "
                "statistics/refresh ($%lld)",
                (void *)it, (long long)(budget_in - *budget_inout));
    pdb_iterator_account_charge_budget(pdb, it, next);

    return pdb_iterator_next_loc(pdb, it, id_out, budget_inout, file, line);
  }

  cl_assert(cl, pdb_iterator_statistics_done(pdb, it));
  cl_assert(cl, oisa(it)->isa_dup.dt_method != ISA_DT_METHOD_UNSPECIFIED);

  isa = it->it_theory;

  cl_assert(cl, pdb_iterator_statistics_done(pdb, it));
  cl_assert(cl, oisa(it)->isa_dup.dt_method != ISA_DT_METHOD_UNSPECIFIED);
  cl_assert(cl, it->it_id == it->it_original->it_id);

  if (isa->isa_dup.dt_method == ISA_DT_METHOD_STORABLE) {
    err = isa_next_cached(pdb, it, &isa->isa_next_tmp, budget_inout);
    goto done;
  }

  cl_assert(cl, isa->isa_dup.dt_method == ISA_DT_METHOD_INTERSECT);
  switch (it->it_call_state) {
    default:
      RESUME_STATE(it, 0)
      if (isa->isa_resume_id != PDB_ID_NONE &&
          !pdb_iterator_sorted(pdb, isa->isa_sub)) {
        pdb_id tmp_id;

        cl_log(isa->isa_cl, CL_LEVEL_SPEW,
               "isa_next_loc: "
               "catching up to id %llx with %s (%d)",
               (unsigned long long)isa->isa_resume_id,
               pdb_iterator_to_string(pdb, isa->isa_sub, buf, sizeof buf),
               pdb_iterator_sorted(pdb, isa->isa_sub));

        for (;;) {
          case 1:
          case 2:
          case 3:
            if (!pdb_iterator_statistics_done(pdb, isa->isa_sub)) {
              err = pdb_iterator_statistics(pdb, isa->isa_sub, budget_inout);
              if (err != 0) return err;
            }
            cl_assert(cl, pdb_iterator_statistics_done(pdb, isa->isa_sub));
            cl_assert(cl, pdb_iterator_sorted(pdb, isa->isa_sub));

            err = isa_next_intersect(pdb, it, &tmp_id, budget_inout);
            if (err != 0) {
              if (err == PDB_ERR_MORE) {
                pdb_rxs_pop(pdb,
                            "NEXT %p isa: "
                            "suspended in intersect"
                            "; state=%d ($%lld)",
                            (void *)it, it->it_call_state,
                            (long long)(budget_in - *budget_inout));
                goto suspend;
              }
              goto done;
            }

            if (tmp_id == isa->isa_resume_id) break;

            if (GRAPHD_SABOTAGE(isa->isa_graphd, *budget_inout <= 0)) {
              it->it_call_state = 4;
              goto suspend;
              RESUME_STATE(it, 4);
            }
        }
      }
      isa->isa_resume_id = PDB_ID_NONE;

      /*  Read a new id from the subiterator.
       */
      if (0) {
        case 10:
        case 11:
        case 12:
        case 13:
          it->it_call_state -= 10;
      }

      err = isa_next_intersect(pdb, it, id_out, budget_inout);
      if (err == PDB_ERR_MORE) it->it_call_state += 10;
  }

done:
  switch (err) {
    case 0:
      it->it_call_state = 0;
      cl_assert(isa->isa_cl, isa->isa_next_tmp != PDB_ID_NONE);
      isa->isa_last_id = *id_out = isa->isa_next_tmp;
      isa->isa_next_tmp = PDB_ID_NONE;

      pdb_rxs_pop(pdb, "NEXT %p isa %llx ($%lld)", (void *)it,
                  (unsigned long long)*id_out,
                  (long long)(budget_in - *budget_inout));
      break;

    case GRAPHD_ERR_NO:
      isa->isa_eof = true;
      isa->isa_sub_source = PDB_ID_NONE;
      isa->isa_next_tmp = PDB_ID_NONE;
      it->it_call_state = 0;

      pdb_rxs_pop(pdb, "NEXT %p isa: eof ($%lld)", (void *)it,
                  (long long)(budget_in - *budget_inout));
      break;

    suspend:
      err = PDB_ERR_MORE;
    /* FALL THROUGH */

    case PDB_ERR_MORE:
      pdb_rxs_pop(pdb, "NEXT %p isa: suspended state=%d ($%lld)", (void *)it,
                  it->it_call_state, (long long)(budget_in - *budget_inout));
      break;

    default:
    unexpected_error:
      it->it_call_state = 0;
      pdb_rxs_pop(pdb, "NEXT %p isa: unexpected error %s ($%lld)", (void *)it,
                  graphd_strerror(err), (long long)(budget_in - *budget_inout));
      break;
  }
err:
  pdb_iterator_account_charge_budget(pdb, it, next);
  return err;
}

/*
 * isa:[~]LOW[-HIGH]:LINKAGE[+TYPE]<-(SUBSET)
 *	/ RESUMEID SOURCEID / [STATISTICS]:SUBSTATE
 */
static int isa_freeze(pdb_handle *pdb, pdb_iterator *it, unsigned int flags,
                      cm_buffer *buf) {
  graphd_iterator_isa *isa = it->it_theory;
  char b1[200];
  int err = 0;
  char const *sep = "";
  size_t off = buf->buf_n;
  cl_handle *const cl = isa->isa_cl;

  cl_enter(cl, CL_LEVEL_VERBOSE, "it=%p, flags=%u", it, flags);

  if ((err = pdb_iterator_refresh(pdb, it)) == 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "redirect");
    return pdb_iterator_freeze(pdb, it, flags, buf);
  } else if (err != PDB_ERR_ALREADY) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_refresh", err,
                 "Can't refresh isa iterator before freeze");
    cl_leave(cl, CL_LEVEL_VERBOSE, "fail");
    return err;
  }

  if (flags & PDB_ITERATOR_FREEZE_SET) {
    err = isa_freeze_set(pdb, it->it_low, it->it_high, isa->isa_direction,
                         pdb_iterator_ordering(pdb, it),
                         pdb_iterator_account(pdb, it), isa->isa_sub,
                         isa->isa_linkage, NULL, isa->isa_hint, buf);
    if (err != 0) goto err;
    sep = "/";
  }

  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    err = cm_buffer_add_string(buf, sep);
    if (err != 0) goto err;

    err = graphd_iterator_util_freeze_position(
        pdb, isa->isa_eof, isa->isa_last_id, isa->isa_resume_id, buf);
    if (err != 0) goto err;

    /*  Technically, this is state - but it's such tremendously
     *  useful, yet small, state that we keep it in the position.
     */
    err = cm_buffer_sprintf(
        buf, ":%s%s", isa->isa_sub_has_position ? "" : "~",
        pdb_id_to_string(pdb, isa->isa_sub_source, b1, sizeof b1));
    if (err != 0) goto err;

    /*  If we have a position in the isa storable cache,
     *  store that, too.
     */
    if (isa->isa_last_id != PDB_ID_NONE &&
        oisa(it)->isa_dup.dt_method == ISA_DT_METHOD_STORABLE) {
      err =
          cm_buffer_sprintf(buf, "[sp:%zu]", isa->isa_dup.dt_storable_position);
      if (err != 0) goto err;
    }

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_STATE) {
    char b2[200];

    /* Call state, Subiterator state
     */
    err = cm_buffer_sprintf(buf, "%s%d:", sep, it->it_call_state);
    if (err != 0) goto err;

    err = graphd_iterator_util_freeze_subiterator(
        pdb, isa->isa_sub,
        PDB_ITERATOR_FREEZE_POSITION | PDB_ITERATOR_FREEZE_STATE, buf);
    if (err != 0) goto err;

    err = graphd_iterator_util_freeze_subiterator(
        pdb, isa->isa_fanin,
        PDB_ITERATOR_FREEZE_SET | PDB_ITERATOR_FREEZE_POSITION |
            PDB_ITERATOR_FREEZE_STATE,
        buf);
    if (err != 0) goto err;

    err = cm_buffer_add_string(buf, ":");
    if (err != 0) goto err;

    /*  Statistics.
     */
    err = isa_statistics_freeze(pdb, it, buf);
    if (err != 0) goto err;

    /*  If we're in the statistics phase, we need to save
     *  the isa_next_tmp of our *original*, because that's
     *  used to hold temporary statistics state.
     *
     *  Otherwise, we need to save our *own*
     *  isa->isa_next_tmp, because that holds state
     *  used in the "next".
     *
     *  When reconstituting the iterator, it becomes an
     *  original, either with the right "next" state or
     *  with the mid-statistics state.
     *
     *  (Statistics always completes before "next" starts
     *  in any clone.)
     */
    err = cm_buffer_sprintf(
        buf, ":%s:",
        pdb_id_to_string(
            pdb, pdb_iterator_statistics_done(pdb, it)
                     ? isa->isa_next_tmp
                     : ((graphd_iterator_isa *)it->it_original->it_theory)
                           ->isa_next_tmp,
            b2, sizeof b2));
    if (err != 0) goto err;

    /*  Duplicate detection.  Don't save and restore the
     *  hashtable if you don't actually have a position
     *  (because you're being used for checking, not next
     *  or find).
     */
    if (!pdb_iterator_has_position(pdb, it))
      err = cm_buffer_add_string(buf, "-");
    else
      err = isa_dup_freeze(isa->isa_graphd, it, &isa->isa_dup, buf);
    if (err != 0) goto err;
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "%.*s", (int)(buf->buf_n - off),
           buf->buf_s + off);
  return 0;
err:
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
  return err;
}

static int isa_clone(pdb_handle *pdb, pdb_iterator *it, pdb_iterator **it_out) {
  pdb_iterator *it_orig = it->it_original;
  graphd_iterator_isa *isa = it->it_theory;
  cm_handle *cm = isa->isa_cm;
  graphd_iterator_isa *isa_out;
  int err;

  PDB_IS_ITERATOR(isa->isa_cl, it);
  GRAPHD_IS_ISA(pdb_log(pdb), isa);

  /*  If the original iterator has evolved into something
   *  other than an "isa" iterator, clone that iterator
   *  directly and reset it.  If we had a position to save,
   *  we would have already evolved.
   */
  if (it_orig->it_type != it->it_type || it->it_id != it_orig->it_id)
    return pdb_iterator_clone(pdb, it_orig, it_out);

  if (pdb_iterator_statistics_done(pdb, it) &&
      oisa(it)->isa_dup.dt_method == ISA_DT_METHOD_UNSPECIFIED) {
    cl_notreached(isa->isa_cl,
                  "iterator %p, original %p, isa dup method "
                  "is unspecified\n",
                  (void *)it, (void *)it->it_original);
  }

  if (pdb_iterator_statistics_done(pdb, it) &&
      isa->isa_dup.dt_method == ISA_DT_METHOD_UNSPECIFIED)
    isa_dup_initialize(pdb, it);

  *it_out = NULL;
  if ((isa_out = cm_malcpy(cm, isa, sizeof(*isa))) == NULL) {
    return errno ? errno : ENOMEM;
  }

  isa_out->isa_cache_sub = NULL;
  isa_out->isa_cache = NULL;
  isa_out->isa_sub = NULL;
  isa_out->isa_fanin = NULL;
  isa_out->isa_statistics_sub = NULL;
  isa_out->isa_statistics_state = 0;

  err = graphd_check_cache_initialize(isa->isa_graphd, &isa_out->isa_ccache);
  if (err != 0) {
    cm_free(cm, isa_out);
    return err;
  }

  if (isa->isa_sub != NULL) {
    err = pdb_iterator_clone(pdb, isa->isa_sub, &isa_out->isa_sub);
    if (err != 0) {
      cm_free(cm, isa_out);
      return err;
    }
  }
  if (isa->isa_fanin != NULL) {
    err = pdb_iterator_clone(pdb, isa->isa_fanin, &isa_out->isa_fanin);
    if (err != 0) {
      cm_free(cm, isa_out);
      return err;
    }
  }

  err = isa_dup_dup(pdb, &isa->isa_dup, &isa_out->isa_dup);
  if (err != 0) {
    isa_dup_finish(pdb, &isa_out->isa_dup);
    cm_free(cm, isa_out);

    return err;
  }

  if ((err = pdb_iterator_make_clone(pdb, it_orig, it_out)) != 0) {
    pdb_iterator_destroy(pdb, &isa_out->isa_sub);
    pdb_iterator_destroy(pdb, &isa_out->isa_fanin);
    isa_dup_finish(pdb, &isa_out->isa_dup);
    cm_free(isa->isa_cm, isa_out);

    return err;
  }
  (*it_out)->it_theory = isa_out;

  pdb_rxs_log(pdb, "CLONE %p isa %p", (void *)it, *it_out);

  if (pdb_iterator_statistics_done(pdb, it)) {
    cl_assert(isa->isa_cl,
              oisa(it)->isa_dup.dt_method != ISA_DT_METHOD_UNSPECIFIED);
    cl_assert(isa->isa_cl, isa->isa_dup.dt_method != ISA_DT_METHOD_UNSPECIFIED);
    cl_assert(isa->isa_cl,
              isa_out->isa_dup.dt_method != ISA_DT_METHOD_UNSPECIFIED);
    cl_assert(isa->isa_cl, pdb_iterator_statistics_done(pdb, *it_out));
  } else {
    cl_assert(isa->isa_cl, !pdb_iterator_statistics_done(pdb, *it_out));
  }

  if (!pdb_iterator_has_position(pdb, it)) {
    if ((err = pdb_iterator_reset(pdb, *it_out)) != 0) {
      pdb_iterator_destroy(pdb, it_out);
      return err;
    }
  }
  return 0;
}

static void isa_finish(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_isa *isa = it->it_theory;

  if (isa != NULL) {
    cl_cover(isa->isa_cl);

    if (it->it_original == it) isa_cache_destroy(pdb, it);

    graphd_check_cache_finish(isa->isa_graphd, &isa->isa_ccache);

    pdb_iterator_destroy(pdb, &isa->isa_statistics_sub);
    pdb_iterator_destroy(pdb, &isa->isa_fanin);
    pdb_iterator_destroy(pdb, &isa->isa_sub);
    isa_dup_finish(pdb, &isa->isa_dup);

    cm_free(isa->isa_cm, it->it_displayname);
    it->it_displayname = NULL;

    cm_free(isa->isa_cm, isa);
    it->it_theory = NULL;
  }
  it->it_type = NULL;
  it->it_magic = 0;
}

static char const *isa_to_string(pdb_handle *pdb, pdb_iterator *it, char *buf,
                                 size_t size) {
  graphd_iterator_isa *isa = it->it_theory;
  char sub[200];
  char ord[200];

  if (it->it_original->it_id != it->it_id) {
    snprintf(buf, sizeof buf, "isa**%s",
             pdb_iterator_to_string(pdb, it->it_original, sub, sizeof sub));
    return buf;
  }

  *ord = '\0';
  if (pdb_iterator_ordering(pdb, it) != NULL) {
    if (pdb_iterator_ordered_valid(pdb, it))
      snprintf(ord, sizeof ord, "(%so:%s)",
               pdb_iterator_ordered(pdb, it) ? "" : "!",
               pdb_iterator_ordering(pdb, it));
    else
      snprintf(ord, sizeof ord, "(o?:%s)", pdb_iterator_ordering(pdb, it));
  }

  snprintf(buf, size, "%s%sisa%s[<-%.1s: %s%s]", it->it_forward ? "" : "~",
           pdb_iterator_statistics_done(pdb, it) ? "" : "*", ord,
           pdb_linkage_to_string(isa->isa_linkage),
           pdb_iterator_to_string(pdb, isa->isa_sub, sub, sizeof sub),
           oisa(it)->isa_dup.dt_method == ISA_DT_METHOD_STORABLE
               ? " S"
               : (oisa(it)->isa_dup.dt_method == ISA_DT_METHOD_INTERSECT ? " I"
                                                                         : ""));
  return buf;
}

static void isa_propagate_ordering(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_isa *isa = it->it_theory;

  if (pdb_iterator_ordering(pdb, it) == NULL || !pdb_iterator_ordered(pdb, it))
    return;

  if (isa->isa_sub != NULL) {
    pdb_iterator_ordered_set(pdb, isa->isa_sub, true);
    pdb_iterator_ordering_set(pdb, isa->isa_sub,
                              pdb_iterator_ordering(pdb, it));
  }

  if (isa->isa_cache_sub != NULL) {
    pdb_iterator_ordered_set(pdb, isa->isa_cache_sub, true);
    pdb_iterator_ordering_set(pdb, isa->isa_cache_sub,
                              pdb_iterator_ordering(pdb, it));
  }
}

/**
 * @brief Will this iterator ever return a value beyond (in sort order)
 * 	the given value?
 *
 * @param pdb		module handle
 * @param it		iterator we're asking about
 * @param s		start of comparison value
 * @param e		end of comparison value
 * @param beyond_out	out: true if we're safely beyond this value.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int isa_beyond(pdb_handle *pdb, pdb_iterator *it, char const *s,
                      char const *e, bool *beyond_out) {
  graphd_iterator_isa *isa = it->it_theory;
  int err;
  char buf[200];

  /*  Something is out of sync?
   */
  if (!pdb_iterator_statistics_done(pdb, it) ||
      it->it_id != it->it_original->it_id || !pdb_iterator_ordered(pdb, it)) {
    cl_log(isa->isa_cl, CL_LEVEL_VERBOSE, "isa_beyond: %s - returning false",
           !pdb_iterator_statistics_done(pdb, it)
               ? "no statistics yet"
               : (it->it_id != it->it_original->it_id
                      ? "original and instance ids don't match"
                      : "iterator isn't ordered"));

    *beyond_out = false;
    return 0;
  }

  isa_propagate_ordering(pdb, it);

  /*  Pass the request to the source.
   */
  if (isa->isa_dup.dt_method == ISA_DT_METHOD_STORABLE) {
    if (isa->isa_dup.dt_storable_position <
            graphd_iterator_isa_storable_nelems(oisa(it)->isa_cache) ||
        oisa(it)->isa_cache_sub == NULL) {
      *beyond_out = false;
      return 0;
    }
    err = pdb_iterator_beyond(pdb, oisa(it)->isa_cache_sub, s, e, beyond_out);
  } else {
    err = pdb_iterator_beyond(pdb, isa->isa_sub, s, e, beyond_out);
  }
  cl_log(isa->isa_cl, CL_LEVEL_VERBOSE, "isa_beyond: %s: %s",
         pdb_iterator_to_string(pdb, isa->isa_sub, buf, sizeof buf),
         err ? graphd_strerror(err)
             : (*beyond_out ? "we're done" : "no, we can still go below that"));
  return err;
}

static int isa_range_estimate(pdb_handle *pdb, pdb_iterator *it,
                              pdb_range_estimate *range) {
  pdb_range_estimate sub_range;
  graphd_iterator_isa *isa;
  int err;

  if ((err = pdb_iterator_refresh(pdb, it)) == 0)
    return pdb_iterator_range_estimate(pdb, it, range);
  else if (err != PDB_ERR_ALREADY)
    return err;

  pdb_iterator_range_estimate_default(pdb, it, range);

  /*  If we're at the end of the cache, the lower bound
   *  of our subiterator values is a lower bound for
   *  ourselves.  (You can't point to something that
   *  was created after you.)
   */
  range->range_n_max = range->range_n_exact = PDB_COUNT_UNBOUNDED;
  isa = it->it_theory;

  /*  Pass the request to the source.
   */
  if (isa->isa_dup.dt_method == ISA_DT_METHOD_STORABLE) {
    if (isa->isa_dup.dt_storable_position <
            graphd_iterator_isa_storable_nelems(oisa(it)->isa_cache) ||
        oisa(it)->isa_cache_sub == NULL) {
      return 0;
    }
    if (oisa(it)->isa_cache_sub != NULL) {
      pdb_range_estimate cache_range;

      graphd_iterator_isa_storable_range(oisa(it)->isa_cache, &cache_range,
                                         isa->isa_dup.dt_storable_position);

      err =
          pdb_iterator_range_estimate(pdb, oisa(it)->isa_cache_sub, &sub_range);
      if (err != 0) return err;

      if (sub_range.range_low >= sub_range.range_high ||
          sub_range.range_n_exact == 0 || sub_range.range_n_max == 0) {
        *range = cache_range;
        return 0;
      }

      if (cache_range.range_high > sub_range.range_high)
        sub_range.range_high = cache_range.range_high;

      if (sub_range.range_high < range->range_high)
        range->range_high = sub_range.range_high;
    } else {
      return 0;
    }
  } else {
    err = pdb_iterator_range_estimate(pdb, isa->isa_sub, &sub_range);
  }
  if (err != 0) {
    char buf[200];

    if (err != PDB_ERR_NO) return err;

    cl_log(isa->isa_cl, CL_LEVEL_VERBOSE,
           "isa_range_estimate: subiterator %s doesn't "
           "understand the question.",
           pdb_iterator_to_string(pdb, isa->isa_sub, buf, sizeof buf));
  } else {
    /*  The subiterator IDs are always larger than mine.
     *  So, its "high" bounds my own; and if its "high"
     *  gets smaller, mine does, too.
     *
     *  The opposite is not true for the lower bounds.
     */
    range->range_high_falling |= sub_range.range_high_falling;

    if (range->range_high >= sub_range.range_high)
      range->range_high =
          sub_range.range_high == 0 ? 0 : sub_range.range_high - 1;

    range->range_n_max = sub_range.range_n_max;
  }
  cl_log(isa->isa_cl, CL_LEVEL_VERBOSE, "isa_range_estimate: %llx%s...%llx%s",
         range->range_low, range->range_low_rising ? " and rising" : "",
         range->range_high, range->range_high_falling ? " and falling" : "");
  return 0;
}

static const pdb_iterator_type isa_type = {
    "isa",
    isa_finish,
    isa_reset,
    isa_clone,
    isa_freeze,
    isa_to_string,

    isa_next_loc,
    isa_find_loc,
    isa_check,
    isa_statistics,

    NULL, /* idarray */
    NULL, /* primitive-summary */

    isa_beyond,
    isa_range_estimate,
    NULL, /* restrict */

    NULL /* suspend */,
    NULL /* unsuspend */
};

/**
 * @brief Assemble an "isa" iterator structure.
 *
 *  The new iterator L is derived from another iterator S.
 *  The primitives in S point to the primitives in L with their
 *  linkage pointer.
 *
 * @param graphd	server for whom we're doing this
 * @param linkage	linkage that the subiterator results point with.
 * @param sub		pointer to subiterator.  A successful call
 *			zeroes out the pointer and takes possession of
 *			the pointed-to iterator.
 * @param low		low limit of the results (included), or
 *			PDB_ITERATOR_LOW_ANY
 * @param high		high limit of the results (not included),
 *			or PDB_ITERATOR_HIGH_ANY
 * @param direction	forward, backward, ordered?
 * @param ordering	if ordered, how?
 * @param optimize	should I try to optimize this?
 * @param it_out	Assign the new construct to this.
 * @param file		caller's filename
 * @param line		caller's line
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
static int isa_make(graphd_request *greq, int linkage, pdb_iterator **sub,
                    unsigned long long low, unsigned long long high,
                    graphd_direction direction, unsigned int isa_hint,
                    char const *ordering, pdb_iterator **it_out,
                    char const *file, int line) {
  graphd_handle *g = graphd_request_graphd(greq);
  bool forward = direction != GRAPHD_DIRECTION_BACKWARD;
  pdb_handle *pdb = g->g_pdb;
  cl_handle *cl = pdb_log(pdb);
  cm_handle *cm = pdb_mem(pdb);
  graphd_iterator_isa *isa;
  int err = 0;
  size_t sub_ids_n;
  pdb_id type_id = PDB_ID_NONE, end_id = PDB_ID_NONE;
  int end_linkage = 0;
  pdb_primitive_summary psum;

  /*  Try some shortcuts: linkage(null) = null, linkage(A1..AN) = B1..BN
   */
  *it_out = NULL;

  /*  linkage(null)?
   */
  if (pdb_iterator_null_is_instance(pdb, *sub)) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "isa_make: "
           "returning null for <-%s(null)",
           pdb_linkage_to_string(linkage));
    *it_out = *sub;
    *sub = NULL;

    return 0;
  }

  /*  If the subiterator is a fixed vip set, graphd may have
   *  cached our results via the islink cache.
   */
  if ((isa_hint & GRAPHD_ITERATOR_ISA_HINT_OPTIMIZE) &&
      pdb_iterator_primitive_summary(pdb, *sub, &psum) == 0 &&
      psum.psum_result == PDB_LINKAGE_N &&
      (psum.psum_locked & (1 << PDB_LINKAGE_TYPEGUID)) &&
      ((psum.psum_locked & (1 << (end_linkage = PDB_LINKAGE_LEFT)))
           ? linkage == PDB_LINKAGE_RIGHT
           : ((psum.psum_locked & (1 << (end_linkage = PDB_LINKAGE_RIGHT))) &&
              linkage == PDB_LINKAGE_LEFT))) {
    graphd_islink_key key;

    err = pdb_id_from_guid(g->g_pdb, &type_id,
                           psum.psum_guid + PDB_LINKAGE_TYPEGUID);
    if (err != 0) {
      char buf[200];
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "guid=%s",
                   graph_guid_to_string(psum.psum_guid + PDB_LINKAGE_TYPEGUID,
                                        buf, sizeof buf));
      return err;
    }

    err = pdb_id_from_guid(g->g_pdb, &end_id, psum.psum_guid + end_linkage);
    if (err != 0) {
      char buf[200];
      cl_log_errno(
          cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "guid=%s",
          graph_guid_to_string(psum.psum_guid + end_linkage, buf, sizeof buf));
      return err;
    }

    /*  Can we just turn into an islink result set?
     */
    if (psum.psum_complete) {
      err = graphd_iterator_islink_create_loc(
          g, low, high, forward,
          graphd_islink_key_make(g, linkage, type_id, end_id, &key), it_out,
          file, line);
      if (err == 0) return 0;
    }

    /* Oh well. */
    err = 0;
  }

  /*  If it's cheap enough to just evaluate this now and
   *  then work with a fixed set, do that.
   *
   *  "optimize" is false if we already went through this
   *  procedure and are actually in the middle of thawing
   *  a cursor that resulted from a previous path.
   */
  cl_log(cl, CL_LEVEL_VERBOSE,
         "isa_make: optimize %d, statistics done %d, n %llu, next_cost %lld",
         isa_hint & GRAPHD_ITERATOR_ISA_HINT_OPTIMIZE,
         pdb_iterator_statistics_done(pdb, *sub), pdb_iterator_n(pdb, *sub),
         pdb_iterator_next_cost(pdb, *sub));

  if ((isa_hint & GRAPHD_ITERATOR_ISA_HINT_OPTIMIZE) &&
      pdb_iterator_statistics_done(pdb, *sub) &&
      (sub_ids_n = pdb_iterator_n(pdb, *sub)) < GRAPHD_ISA_INLINE_N_THRESHOLD &&
      pdb_iterator_next_cost(pdb, *sub) < GRAPHD_ISA_INLINE_COST_THRESHOLD) {
    err = isa_become_small_set(g, linkage, *sub, low, high, direction, ordering,
                               it_out);
    if (err == 0) {
      pdb_iterator_destroy(pdb, sub);
      return 0;
    }
    if (err != PDB_ERR_MORE) return err;
    err = 0;
  }

  /*  Because my subiterator points to me, my IDs must be
   *  smaller than the subiterator's high.
   *  (Can't link to a primitive that doesn't exist yet.)
   */
  if ((*sub)->it_high != PDB_ITERATOR_HIGH_ANY &&
      (high == PDB_ITERATOR_HIGH_ANY || (*sub)->it_high < high + 1))
    high = (*sub)->it_high - 1;

  if ((isa = cm_zalloc(cm, sizeof(*isa))) == NULL ||
      (*it_out = cm_malloc(cm, sizeof(**it_out))) == NULL) {
    int err = errno ? errno : ENOMEM;
    if (isa != NULL) cm_free(cm, isa);

    cl_log_errno(cl, CL_LEVEL_VERBOSE, "cm_malloc", err,
                 "failed to allocate isa-iterator");
    return err;
  }

  isa_dup_clear(&isa->isa_dup);

  isa->isa_magic = GRAPHD_ISA_MAGIC;
  isa->isa_graphd = g;
  isa->isa_cl = cl;
  isa->isa_cm = cm;
  isa->isa_linkage = linkage;
  isa->isa_direction = direction;

  isa->isa_fanin = NULL;
  isa->isa_statistics_sub = NULL;
  isa->isa_next_tmp = PDB_ID_NONE;
  isa->isa_last_id = PDB_ID_NONE;
  isa->isa_resume_id = PDB_ID_NONE;
  isa->isa_sub_source = PDB_ID_NONE;
  isa->isa_hint = isa_hint;

  pdb_iterator_make_loc(g->g_pdb, *it_out, low, high, forward, file, line);
  pdb_iterator_sorted_set(g->g_pdb, *it_out, false);

  /*  If we wanted to be ordered forwards/backwards, we failed;
   *  clear the indicator.
   *
   *  If we wanted to be ordered by the subiterator, we'll
   *  succeed if we're subiterator-ordered by the end of
   *  the statistics phase.
   */
  if (direction != GRAPHD_DIRECTION_ORDERING) ordering = NULL;

  pdb_iterator_ordering_set(g->g_pdb, *it_out, ordering);

  isa->isa_sub = *sub;
  *sub = NULL;

  (*it_out)->it_theory = isa;
  (*it_out)->it_type = &isa_type;

  err = isa_dup_initialize(pdb, *it_out);
  if (err != 0) {
    pdb_iterator_destroy(pdb, it_out);
    return err;
  }

  err = graphd_check_cache_initialize(g, &isa->isa_ccache);
  if (err != 0) {
    pdb_iterator_destroy(pdb, it_out);
    return err;
  }

  /*  If our subiterator knows its check cost, we can guess
   *  our average.
   */
  if (pdb_iterator_check_cost_valid(pdb, isa->isa_sub))
    pdb_iterator_check_cost_set(pdb, *it_out,
                                PDB_COST_GMAP_ARRAY + PDB_COST_GMAP_ELEMENT +
                                    pdb_iterator_check_cost(pdb, isa->isa_sub));

  GRAPHD_IS_ISA(cl, isa);
  cl_log(cl, CL_LEVEL_VERBOSE,
         "isa_make: it %p, isa %p, sub %p, internal sub %p, [%lld..%lld[",
         (void *)*it_out, (void *)isa, (void *)*sub, isa->isa_sub,
         (long long)(*it_out)->it_low, (long long)(*it_out)->it_high);
  pdb_rxs_log(pdb, "CREATE %p isa", (void *)*it_out);

  return 0;
}

/**
 * @brief Create an "isa" iterator structure.
 *
 *  The new iterator L is derived from another iterator S.
 *  The primitives in S point to the primitives in L with their
 *  linkage pointer.
 *
 * @param greq		request for whom we're doing this
 * @param linkage	linkage that the subiterator results point with.
 * @param sub		pointer to subiterator.  A successful call
 *			zeroes out the pointer and takes possession of
 *			the pointed-to iterator.
 * @param low		low limit of the results (included), or
 *			PDB_ITERATOR_LOW_ANY
 * @param high		high limit of the results (not included),
 *			or PDB_ITERATOR_HIGH_ANY
 * @param forward	sort from low to high, if sorting should happen?
 * @param it_out	Assign the new construct to this.
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
int graphd_iterator_isa_create_loc(
    graphd_request *greq, int linkage, pdb_iterator **sub,
    unsigned long long low, unsigned long long high, graphd_direction direction,
    char const *ordering, graphd_iterator_isa_hint isa_hint,
    pdb_iterator **it_out, char const *file, int line) {
  cl_handle *cl = graphd_request_cl(greq);
  pdb_handle *pdb = graphd_request_graphd(greq)->g_pdb;
  char buf[200];
  int err;

  cl_assert(cl, GRAPHD_DIRECTION_VALID(direction));

  if (ordering != NULL)
    ordering = graphd_iterator_ordering_internalize_request(
        greq, ordering, ordering + strlen(ordering));

  cl_log(cl, CL_LEVEL_VERBOSE, "%.1s[%lld..%lld]<-%s%s%s%s",
         pdb_linkage_to_string(linkage), (long long)low, (long long)high,
         pdb_iterator_to_string(pdb, *sub, buf, sizeof buf),
         ordering ? ", ordering=" : "", ordering ? ordering : "",
         isa_hint & GRAPHD_ITERATOR_ISA_HINT_CURSOR ? ";cursor" : "");

  err = isa_make(greq, linkage, sub, low, high, direction,
                 isa_hint | GRAPHD_ITERATOR_ISA_HINT_OPTIMIZE, ordering, it_out,
                 file, line);
  if (err != 0) return err;

  pdb_iterator_destroy(pdb, sub);

  return 0;
}

/**
 * @brief Reconstitute a frozen isa-iterator
 *
 * [~]LOW[-HIGH]:LINKAGE[+TYPEGUID]<-(SUB)
 * RESUMEID:SOURCEID
 * (SUBSTATE) [statistics]:source:subiterator
 *
 * @param graphd	module handle
 * @param s		beginning of stored form
 * @param e		pointer just past the end of stored form
 * @param forward	no ~ before the name?
 * @param it_out	rebuild the iterator here.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_isa_thaw_loc(graphd_handle *graphd,
                                 pdb_iterator_text const *pit,
                                 pdb_iterator_base *pib,
                                 graphd_iterator_hint hint,
                                 cl_loglevel loglevel, pdb_iterator **it_out,
                                 char const *file, int line) {
  pdb_handle *pdb = graphd->g_pdb;
  pdb_iterator *sub_it = NULL, *stat_it = NULL;
  cl_handle *cl = pdb_log(pdb);
  graphd_iterator_isa *isa;
  graphd_direction direction = GRAPHD_DIRECTION_ANY;

  unsigned long long low, high;
  pdb_id resume_id, source_id, last_id;
  int err, linkage, call_state = 0;
  bool forward, source_has_position = false, eof;

  bool have_storable_position = false;
  size_t storable_position = 0;

  char const *e, *s;
  char const *state_s, *state_e;
  char const *ord_s, *ord_e;
  char const *ordering = NULL;
  graph_guid type_guid;
  pdb_iterator_text subpit;
  graphd_request *greq;
  pdb_iterator_account *acc = NULL;
  char buf[200];
  pdb_iterator_property props[2];
  graphd_iterator_isa_hint isa_hint = GRAPHD_ITERATOR_ISA_HINT_DEFAULT;
  unsigned long lu;
  pdb_iterator *isa_orig = NULL;

  /*
   * SET      := [~]LOW[-HIGH]:LINKAGE[+TYPEGUID]<-(SUB)
   * POSITION := LAST_ID:RESUMEID:SOURCEID
   * STATE    := (SUBSTATE) [statistics]:source:subiterator
   */

  GRAPH_GUID_MAKE_NULL(type_guid);

  /* Initialize it with zero to make a pdb_iterator_destroy() on
   * error harmless.
   */
  *it_out = NULL;

  greq = pdb_iterator_base_lookup(graphd->g_pdb, pib, "graphd.request");
  if (greq == NULL) {
    err = errno ? errno : EINVAL;
    cl_log_errno(cl, loglevel, "pdb_iterator_base_lookup", err,
                 "failed to look up request context");
    goto err;
  }

  if (graphd_request_timer_check(greq)) return GRAPHD_ERR_TOO_HARD;

  /*  SET
   */
  s = pit->pit_set_s;
  e = pit->pit_set_e;
  cl_assert(cl, s != NULL && e != NULL);

  if (s < e &&
      (*s == '#' ||
       graphd_iterator_direction_from_char(*s) != GRAPHD_DIRECTION_ANY)) {
    direction = graphd_iterator_direction_from_char(*s);
    s++;
  }

  err = pdb_iterator_util_thaw(
      pdb, &s, e,
      "%{forward}%{low[-high]}:"
      "%{linkage[+guid]}<-%{(bytes)}%{orderingbytes}%{account}",
      &forward, &low, &high, &linkage, &type_guid, &subpit.pit_set_s,
      &subpit.pit_set_e, &ord_s, &ord_e, pib, &acc,
      (pdb_iterator_property *)NULL);
  if (err != 0) {
    cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                 "could not thaw set");
    return err;
  }

  if (pdb_iterator_util_thaw(pdb, &s, e, "[hint:%lu]", &lu) == 0)
    isa_hint = (graphd_iterator_isa_hint)lu;

  err = pdb_iterator_util_thaw(pdb, &s, e, "%{extensions}%{end}",
                               (pdb_iterator_property *)NULL);
  if (err != 0) {
    cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                 "could not thaw set extensions");
    return err;
  }
  if (!forward) direction = GRAPHD_DIRECTION_BACKWARD;

  resume_id = source_id = last_id = PDB_ID_NONE;
  eof = false;

  if (ord_s != NULL)

    /*  We did get an ordering from the frozen text.
     *  Translate it into a pointer to the same ordering
     *  in the place that it designates.
     */
    ordering = graphd_iterator_ordering_internalize(graphd, pib, ord_s, ord_e);

  /*  If we can, reconnect with an existing original.
   */
  if (pit->pit_set_s != NULL && !(hint & GRAPHD_ITERATOR_HINT_HARD_CLONE) &&
      (isa_orig = pdb_iterator_by_name_lookup(pdb, pib, pit->pit_set_s,
                                              pit->pit_set_e)) != NULL) {
    if ((err = pdb_iterator_clone(pdb, isa_orig, it_out)) != 0) {
      char buf[200];
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "orig=%s",
                   pdb_iterator_to_string(pdb, isa_orig, buf, sizeof buf));
      return err;
    }

    /*  Our original has turned into a different type of
     *  iterator?   That means we must have had no position--
     *  otherwise, we'd have turned into that type ourselves.
     */
    if (!graphd_iterator_isa_is_instance(pdb, *it_out, NULL, NULL)) {
      char buf[200];

      /* Evolved into something that isn't an "isa"?
       */
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_isa_thaw: "
             "evolved into something else: %s",
             pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
      return 0;
    }
  }

  /* POSITION
   */
  if ((s = pit->pit_position_s) != NULL && s < (e = pit->pit_position_e)) {
    err = graphd_iterator_util_thaw_position(pdb, &s, e, loglevel, &eof,
                                             &last_id, &resume_id);
    if (err != 0) return err;

    props[0].pip_name = "sp"; /* storable position */
    props[0].pip_s = props[0].pip_e = NULL;
    props[1].pip_name = NULL; /* sentinel */

    err = pdb_iterator_util_thaw(pdb, &s, e,
                                 ":%{forward}%{id}%{extensions}%{end}",
                                 &source_has_position, &source_id, props);
    if (err != 0) return err;

    /*  Optional storable-position [sp:..]
     */
    if (props[0].pip_s != NULL) {
      unsigned long long ull;
      char const *s = props[0].pip_s;
      char const *e = props[0].pip_e;

      /* We have a isa-storable cache position.  Yay.
       */
      if ((err = pdb_scan_ull(&s, e, &ull)) != 0) {
        cl_log_errno(cl, loglevel, "pdb_scan_ull", err,
                     "could not scan \"%.*s\"",
                     (int)(props[0].pip_e - props[0].pip_s), props[0].pip_s);
        return err;
      }

      storable_position = ull;
      if (storable_position != ull) {
        cl_log(cl, loglevel,
               "graphd_iterator_isa_thaw_loc: "
               "overflow while scanning \"%.*s\" "
               "into a size_t",
               (int)(props[0].pip_e - props[0].pip_s), props[0].pip_s);
        return GRAPHD_ERR_SEMANTICS;
      }
      have_storable_position = true;
    }
  }

  /* STATE (1) - CALL-STATE:SUBITERATOR
   */
  state_s = pit->pit_state_s;
  state_e = pit->pit_state_e;
  if (state_s != NULL && state_s < state_e) {
    /*  [OPT] (SUBPOS/SUBSTATE)
     */
    err = pdb_iterator_util_thaw(pdb, &state_s, state_e,
                                 "%d:%{extensions}%{(bytes)}", &call_state,
                                 (pdb_iterator_property *)NULL,
                                 &subpit.pit_position_s, &subpit.pit_state_e);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                   "could not thaw state");
      return err;
    }

    subpit.pit_position_e = graphd_unparenthesized_curchr(
        subpit.pit_position_s, subpit.pit_state_e, '/');
    if (subpit.pit_position_e == NULL) {
      subpit.pit_position_e = subpit.pit_state_s = subpit.pit_state_e;
    } else {
      subpit.pit_state_s = subpit.pit_position_e + 1;
    }
  } else {
    subpit.pit_position_s = subpit.pit_position_e = NULL;
    subpit.pit_state_s = subpit.pit_state_e = NULL;

    /*  We don't have a state and position for the subiterator -
     *  whatever position it had is lost.
     */
    source_has_position = false;
  }

  if (isa_orig == NULL) {
    /* Create the subiterator.
     */
    err = graphd_iterator_thaw_loc(graphd, &subpit, pib, 0, loglevel, &sub_it,
                                   NULL, file, line);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "graphd_iterator_thaw", err,
                   "failed to thaw subiterator");
      goto err;
    }
    if (hint == GRAPHD_ITERATOR_HINT_FIXED)
      isa_hint |= GRAPHD_ITERATOR_ISA_HINT_OPTIMIZE;

    err = isa_make(greq, linkage, &sub_it, low, high, direction, isa_hint,
                   ordering, it_out, file, line);
    pdb_iterator_destroy(graphd->g_pdb, &sub_it);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "isa_make", err,
                   "could not instantiate %llx..%llx", low, high);
      goto err;
    }
  }

  pdb_iterator_account_set(pdb, *it_out, acc);

  /* Still an is-a iterator? */
  if ((*it_out)->it_type != &isa_type) return 0;

  isa = (*it_out)->it_theory;
  isa->isa_thawed = true;

  /*  STATE (2) - FANIN:STATISTICS
   */
  if (state_s != NULL && state_s < state_e) {
    err = graphd_iterator_util_thaw_subiterator(graphd, &state_s, state_e, pib,
                                                loglevel, &isa->isa_fanin);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "graphd_iterator_util_thaw_subiterator", err,
                   "could not thaw fan-in");
      goto err;
    }
    if (state_s < state_e && *state_s == ':') state_s++;
  }

  if (state_s != NULL && state_s < state_e) {
    err = isa_statistics_thaw(*it_out, &state_s, state_e, pib, loglevel);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "isa_statistics_thaw", err,
                   "could not thaw state");
      goto err;
    }

    if (state_s < state_e && *state_s == ':') state_s++;

    /* Next-tmp and dup state? */

    if (state_s >= state_e) {
      cl_log(cl, loglevel,
             "graphd_iterator_isa_thaw: "
             "short state: expected next_tmp and dup");
      err = GRAPHD_ERR_LEXICAL;
      goto err;
    }

    if ((err = pdb_iterator_util_thaw(pdb, &state_s, state_e, "%{id}:",
                                      &isa->isa_next_tmp)) != 0 ||
        (err = isa_dup_thaw(isa->isa_graphd, *it_out, &state_s, state_e,
                            &subpit, pib, loglevel, &isa->isa_dup)) != 0) {
      if (err == GRAPHD_ERR_NO) goto recover_state;

      cl_log_errno(cl, loglevel, "isa_statistics_thaw", err,
                   "could not thaw next_tmp:dup");
      goto err;
    }

    /*  We got our complete state back.
     *  Meaning, there's no need to resume_id anything -
     *  we're already there. (Unless we were in the middle
     *  of resuming something, in which case we resume
     *  the resuming.)
     */
    isa->isa_resume_id = resume_id;
    isa->isa_last_id = last_id;
    isa->isa_sub_source = source_id;
    isa->isa_sub_has_position = source_has_position;
    isa->isa_eof = eof;

    (*it_out)->it_call_state = call_state;
  } else {
  recover_state:
    if (!pdb_iterator_sorted(pdb, isa->isa_sub)) {
      /*  If we're going to use a hashtable
       *  (because the subiterator isn't sorted),
       *  reset it so we can rebuild the hashtable.
       */
      err = pdb_iterator_reset(pdb, isa->isa_sub);
      if (err != 0) {
        cl_log_errno(
            cl, loglevel, "pdb_iterator_reset", err, "subiterator=%s",
            pdb_iterator_to_string(pdb, isa->isa_sub, buf, sizeof buf));
        goto err;
      }
      isa->isa_sub_source = PDB_ID_NONE;
      isa->isa_sub_has_position = false;
    } else {
      /*  We didn't rescue the subiterator state
       *  itself, but we know where to seek to.
       */
      isa->isa_sub_source = source_id;
      isa->isa_sub_has_position = false;
    }

    if (resume_id != PDB_ID_NONE)
      /*  We were in the middle of resuming to some
       *  ID when we got frozen.  Eeek.  That may
       *  happen occasionally - if it happens a lot,
       *  we may be livelocked.
       */
      isa->isa_resume_id = resume_id;
    else
      isa->isa_resume_id = last_id;

    isa->isa_thawed = true;
    isa->isa_last_id = PDB_ID_NONE;
    isa->isa_statistics_state = 0;
  }

  if (isa->isa_dup.dt_method == ISA_DT_METHOD_STORABLE &&
      isa_dup_can_switch_to_intersect(pdb, *it_out)) {
    pdb_budget budget_intersect, budget_hash;

    budget_intersect = isa_dup_intersect_cost(pdb, *it_out);
    budget_hash = isa_dup_storable_cost(pdb, *it_out);

    if (budget_intersect < budget_hash) {
      cl_log(cl, CL_LEVEL_DEBUG,
             "graphd_iterator_thaw: SWITCH to INTERSECT at "
             "budget_intersect %lld < budget_hash %lld",
             (long long)budget_intersect, (long long)budget_hash);

      isa_dup_storable_switch_to_intersect(pdb, *it_out);
    } else {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_iterator_thaw: intersect budget %lld >= "
             "hash budget %lld",
             (long long)budget_intersect, (long long)budget_hash);
    }
  }

  if (isa->isa_dup.dt_method == ISA_DT_METHOD_STORABLE) {
    if (isa->isa_cache == NULL) {
      /*  We know we're using a storable ID, but couldn't
       *  actually get our storable cache back - initialize
       *  an empty cache in preparation for resuming.
       */
      err = isa_cache_create(pdb, *it_out);
      if (err != 0) goto err;
    } else {
      size_t offset;

      /*  We know our last ID, and we got our
       *  cache back - that means that we can
       *  find our position.
       */
      if (isa->isa_last_id == PDB_ID_NONE)
        isa->isa_dup.dt_storable_position = 0;

      else if (have_storable_position) {
        isa->isa_dup.dt_storable_position = storable_position;
      } else if (graphd_iterator_isa_storable_id_to_offset(
                     isa->isa_cache, isa->isa_last_id, &offset)) {
        cl_log(cl, CL_LEVEL_VERBOSE,
               "graphd_iterator_isa_thaw: "
               "recovered storable position %zu for "
               "id %llx",
               offset, (unsigned long long)isa->isa_last_id);

        /*  Our current position is one *after*
         *  the last id.
         */
        isa->isa_dup.dt_storable_position = offset + 1;
      } else {
        /* Need to recover. */
        isa->isa_resume_id = isa->isa_last_id;
        isa->isa_last_id = PDB_ID_NONE;

        cl_log(cl, CL_LEVEL_VERBOSE,
               "graphd_iterator_isa_thaw: "
               "could not recover storable position "
               "for %llx",
               (unsigned long long)isa->isa_last_id);
      }
    }
  }
  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_isa_thaw: resume_id is %s, "
         "storable_position %zu",
         pdb_id_to_string(pdb, isa->isa_resume_id, buf, sizeof buf),
         (size_t)isa->isa_dup.dt_storable_position);

  return 0;

err:
  pdb_iterator_destroy(pdb, it_out);
  pdb_iterator_destroy(pdb, &stat_it);
  pdb_iterator_destroy(pdb, &sub_it);

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_iterator_isa_thaw: error %s",
         graphd_strerror(err));
  return err;
}

/**
 * @brief Is this an is-a iterator?  Which one?
 *
 * @param pdb		module handle
 * @param it		iterator the caller is asking about
 * @param linkage_out	what's the connection to the subiterator?
 * @param sub_out	what's the subiterator?
 *
 * @return true if this is an is-a iterator, false otherwise
 */
bool graphd_iterator_isa_is_instance(pdb_handle *pdb, pdb_iterator *it,
                                     int *linkage_out, pdb_iterator **sub_out) {
  graphd_iterator_isa *isa;

  if (it->it_type != &isa_type) return false;

  isa = it->it_theory;
  if (sub_out != NULL) *sub_out = isa->isa_sub;

  if (linkage_out != NULL) *linkage_out = isa->isa_linkage;

  return true;
}
