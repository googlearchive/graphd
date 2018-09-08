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
#ifndef PDB_H
#define PDB_H

#include <limits.h> /* LLONG_MAX */
#include <stdarg.h>
#include <stdbool.h>
#include <unistd.h> /* pid_t */

#include "libaddb/addb-bgmap.h"
#include "libaddb/addb.h"
#include "libgraph/graph.h"

typedef addb_msclock_t pdb_msclock_t;
#define PDB_PAST_DEADLINE(c, d) ADDB_PAST_DEADLINE(c, d)

#define PDB_ID_SIZE 42 /* # of bytes to render one */
#define PDB_ID_NONE ((addb_gmap_id)-1)
#define PDB_ITERATOR_LOW_ANY 0

#define PDB_ITERATOR_HIGH_ANY ((1ull) << 34)
#define PDB_COUNT_UNBOUNDED (-1ull)

#define PDB_FACILITY_ITERATOR (1ul << 16)
#define PDB_FACILITY_COST (1ul << 17)

#define PDB_ERR_BASE (-4000)
#define PDB_ERR_NO (ADDB_ERR_NO)
#define PDB_ERR_MORE (ADDB_ERR_MORE)
#define PDB_ERR_PRIMITIVE_TOO_LARGE (ADDB_ERR_PRIMITIVE_TOO_LARGE)
#define PDB_ERR_EXISTS (ADDB_ERR_EXISTS)
#define PDB_ERR_ALREADY (ADDB_ERR_ALREADY)
#define PDB_ERR_DATABASE (ADDB_ERR_DATABASE)
#define PDB_ERR_TOO_MANY (PDB_ERR_BASE + 1)
#define PDB_ERR_SYNTAX (PDB_ERR_BASE + 3)
#define PDB_ERR_NOT_SUPPORTED (PDB_ERR_BASE + 4)

/**
 * @brief VIP threshold
 *
 *  How many links going in or out of the same primitive before
 *  that primitive is considered a "VIP" and gets its own per-typeguid
 *  hashed index?
 */
#define PDB_VIP_MIN 100

#define PDB_PRIMITIVE_POSITION_START ADDB_GMAP_POSITION_START
#define PDB_PRIMITIVE_POSITION_END ADDB_GMAP_POSITION_END

typedef addb_gmap_id pdb_id; /* 34-bit integer, actually */
typedef struct pdb_handle pdb_handle;
typedef graph_guid pdb_guid;
typedef addb_istore_reference pdb_primitive_reference;

extern char const pdb_build_version[];
extern cl_facility const pdb_facilities[];
typedef long long pdb_budget;
struct pdb_binset;

typedef struct pdb_iterator_account pdb_iterator_account;
typedef struct pdb_iterator_base pdb_iterator_base;

/*  Different, but essentially exchangeable, ways of connecting
 *  two primitives.
 */
#define PDB_LINKAGE_TYPEGUID 0
#define PDB_LINKAGE_RIGHT 1
#define PDB_LINKAGE_LEFT 2
#define PDB_LINKAGE_SCOPE 3
#define PDB_LINKAGE_N 4
#define PDB_IS_LINKAGE(l) (((unsigned int)(l)) < PDB_LINKAGE_N)

/*  "I don't care which linkage" for
 *  pdb_iterator_gmap_is_instance()
 */
#define PDB_LINKAGE_ANY (-1)

/*  Word types returned by the parser in pdb-word.c
 */
#define PDB_WORD_SPACE ' '
#define PDB_WORD_PUNCTUATION '-'
#define PDB_WORD_ATOM 'a'
#define PDB_WORD_NUMBER '1'

/*  Indices maintained by pdb
 */
#define PDB_INDEX_TYPEGUID PDB_LINKAGE_TYPEGUID
#define PDB_INDEX_RIGHT PDB_LINKAGE_RIGHT
#define PDB_INDEX_LEFT PDB_LINKAGE_LEFT
#define PDB_INDEX_SCOPE PDB_LINKAGE_SCOPE
#define PDB_INDEX_HMAP 4
#define PDB_INDEX_PREFIX 5
#define PDB_INDEX_DEAD 6
#define PDB_INDEX_N 7

#define PDB_VERIFY_TYPEGUID 1
#define PDB_VERIFY_LEFT 2
#define PDB_VERIFY_RIGHT 4
#define PDB_VERIFY_SCOPE 8
#define PDB_VERIFY_PRIMITIVE 16
#define PDB_VERIFY_NAME 32
#define PDB_VERIFY_VALUE 64
#define PDB_VERIFY_PREFIX 128
#define PDB_VERIFY_VIPL 256
#define PDB_VERIFY_VIPR 512
#define PDB_VERIFY_WORD 1024
#define PDB_VERIFY_GENERATION 2048
#define PDB_VERIFY_DEAD 4096
#define PDB_VERIFY_BIN 8192

/* pdb-iterator-*.c */

#define PDB_ITERATOR_MAGIC 0xec01a11a

#define PDB_IS_ITERATOR(cl, it)                                               \
  do {                                                                        \
    cl_assert((cl), (it) != NULL);                                            \
    cl_assert((cl), (it)->it_magic == PDB_ITERATOR_MAGIC);                    \
    cl_assert((cl), (it)->it_type != NULL);                                   \
    cl_assert((cl), (it)->it_original != NULL);                               \
    cl_assert((cl), (it)->it_original->it_magic == PDB_ITERATOR_MAGIC);       \
    cl_assert((cl), (it)->it_original->it_original == (it)->it_original);     \
    cl_assert((cl), (it)->it_original->it_refcount > 0);                      \
    cl_assert((cl), (it)->it_refcount >= (it)->it_clones);                    \
    cl_assert((cl), (it)->it_next == NULL || (it)->it_next->it_prev == (it)); \
    cl_assert((cl), (it)->it_prev == NULL || (it)->it_prev->it_next == (it)); \
    cl_assert((cl), (it)->it_suspend_next == NULL ||                          \
                        (it)->it_suspend_next->it_suspend_prev == (it));      \
    cl_assert((cl), (it)->it_suspend_prev == NULL ||                          \
                        (it)->it_suspend_prev->it_suspend_next == (it));      \
  } while (0)

#define PDB_IS_FINISHING_ITERATOR(cl, it)                  \
  do {                                                     \
    cl_assert((cl), (it) != NULL);                         \
    cl_assert((cl), (it)->it_magic == PDB_ITERATOR_MAGIC); \
    cl_assert((cl), (it)->it_type != NULL);                \
  } while (0)

#define PDB_IS_ORIGINAL_ITERATOR(cl, it)        \
  do {                                          \
    PDB_IS_ITERATOR(cl, it);                    \
    cl_assert((cl), (it)->it_original == (it)); \
  } while (0)

typedef struct pdb_configuration {
  /**
   * @brief When flushing data to disk, should pdb
   * 	wait until the data has actually hit the disk,
   *	or should it just initiate the write?
   */
  bool pcf_sync;

  /**
   * Enable transactional writes.
   *
   * - Enable it if you want to support backup tiles and keep the
   *   underlying file consistent (executable will be bigger, slower, but
   *   very reliable and crash-proof)
   *
   * - Disable it if you do not want backup tiles (executable will be
   *   smaller, much faster, but if you crash you're doomed!)
   */
  bool pcf_transactional;

  /*
   * Do we create the database if it doesn't already exist?
   * By default, we create one. If this gets turned off (-D or must-exist)
   * we will error (good for ops)
   */
  bool pcf_create_database;

  /* Specifies the max memory parameter used when sizing a new database
   * on disk. The default is 0, which will then use sysinfo, sysctl, etc
   * to determine.
   */
  long long pcf_total_memory;

  addb_gmap_configuration pcf_gcf;
  addb_hmap_configuration pcf_hcf;
  addb_istore_configuration pcf_icf;

} pdb_configuration;

typedef struct pdb_primitive {
  addb_data pr_data;
  graph_guid *pr_database_guid;
  graph_guid pr_guid;

} pdb_primitive;

struct pdb_iterator;
typedef struct pdb_iterator_chain {
  struct pdb_iterator *pic_head;
  struct pdb_iterator *pic_tail;

  size_t pic_count;

/*  In the per-request iterator queue,
 *  pic_count is the number of iterators
 *  in that queue that are suspended.
 */
#define pic_n_suspended pic_count

} pdb_iterator_chain;

/*  Different kinds of hash tables in the system.
 */
typedef enum pdb_hash_type {
  /* string keys below this line. */

  PDB_HASH_NAME = 0,
  PDB_HASH_VALUE = 1,
  PDB_HASH_WORD = 2,
  PDB_HASH_BIN = 3,
  PDB_HASH_reserved2,
  PDB_HASH_reserved3,
  PDB_HASH_reserved4,

  /* binary keys below this line. */

  PDB_HASH_TYPEGUID,
  PDB_HASH_SCOPE,
  PDB_HASH_VIP,
  PDB_HASH_KEY,
  PDB_HASH_GEN,
  PDB_HASH_PREFIX,
  PDB_HASH_LAST /* last type enum */

} pdb_hash_type;

/*  Primitive Iterator state.
 *  Opaque; part of the interface merely to allow direct inclusion.
 */
typedef struct pdb_iterator pdb_iterator;

/*  The job of the primitive summary is to paint the
 *  picture of an iterator or primitive fragment that an
 *  iterator matches against.
 */
typedef struct pdb_primitive_summary {
  /* If 1 << L is set in psum_locked, the
   * corresponding "arm" of the returned
   * primitives has the value psum_guid[L].
   */
  unsigned int psum_locked : PDB_LINKAGE_N;
  graph_guid psum_guid[PDB_LINKAGE_N];

  /* The value that the iterator returns, one of
   * PDB_LINKAGE_* or PDB_LINKAGE_N for GUID.
   */
  unsigned int psum_result : 3;

  /*  If this is set, the primitive summary
   *  completely expresses an iterator (other
   *  than low/high).  If this is clear, there
   *  are additional constraints on top of the
   *  ones listed here.
   */
  unsigned int psum_complete : 1;

} pdb_primitive_summary;

typedef struct pdb_range_estimate {
  /* An id below which none will be returned.
   */
  unsigned long long range_low;

  /* PDB_ITERATOR_HIGH_ANY or the first ID
   * so large that it'll never be returned.
   */
  unsigned long long range_high;

  /* Size estimates.  Use PDB_COUNT_UNBOUNDED
   * for "I don't know".
   */
  unsigned long long range_n_exact;
  unsigned long long range_n_max;

  /* If set, this part of the range is expected
   * to move over time.
   */
  unsigned int range_low_rising : 1;
  unsigned int range_high_falling : 1;

} pdb_range_estimate;

typedef struct pdb_iterator_text {
  char const *pit_set_s;
  char const *pit_set_e;

  char const *pit_position_s;
  char const *pit_position_e;

  char const *pit_state_s;
  char const *pit_state_e;

} pdb_iterator_text;

typedef pdb_iterator_account *pdb_iterator_base_account_resolver(
    void *_data, pdb_iterator_base const *_pib, size_t _account_number);

struct pdb_iterator_base {
  /*  Allocate iterator incidentals through this.
   */
  cm_handle *pib_cm;

  /*  Set -> original iterator.
   */
  cm_hashtable pib_by_name;

  /*  Arbitrary other name space.
   */
  cm_hashtable pib_hash;

  pdb_iterator_base_account_resolver *pib_account_resolve_callback;
  void *pib_account_resolve_callback_data;
};

/*  Track how many times an iterator was called.
 */
struct pdb_iterator_account {
  size_t ia_id;

  unsigned long long ia_next_n;
  pdb_budget ia_next_cost;

  unsigned long long ia_find_n;
  pdb_budget ia_find_cost;

  unsigned long long ia_check_n;
  pdb_budget ia_check_cost;
};

/*  Synonyms are encoded sets that evaluate to the
 *  same iterator.
 */
typedef struct pdb_iterator_by_name {
  /* The original that goes by this name.
   */
  struct pdb_iterator *is_it;

  /*  Where do we live?
   */
  pdb_iterator_base *is_pib;

} pdb_iterator_by_name;

/*  Cost estimates
 */
#define PDB_COST_GMAP_ELEMENT 2
#define PDB_COST_GMAP_ARRAY 10
#define PDB_COST_HMAP_ELEMENT (1 + PDB_COST_GMAP_ELEMENT)
#define PDB_COST_HMAP_ARRAY (1 + PDB_COST_GMAP_ARRAY)
#define PDB_COST_FUNCTION_CALL 1
#define PDB_COST_HIGH 999999
#define PDB_COST_HIGH_NEGATIVE -999999
#define PDB_COST_PRIMITIVE (PDB_COST_HMAP_ARRAY + 1)
#define PDB_COST_ITERATOR (PDB_COST_HMAP_ARRAY * 2)

/**
 * @brief pdb-prefix.c uses this to track prefix hashes
 */
typedef struct pdb_prefix_context {
  pdb_handle *ppc_pdb;

  /**
   * @brief The number of UTF-8 characters in the prefix.
   *  	(Between 1 and 4.)
   */
  unsigned int ppc_len : 3;

  /** @brief The hash we're working on right now. */
  unsigned long ppc_hash_current;

  /** @brief The bits we're not supposed to change.
   *  If we do, we've overshot (and are done).
   */
  unsigned long ppc_hash_mask;

  /**
   * @brief The hash of the original prefix.
   *
   *  If the bits in the hash_current not in ppc_hash_mask
   *  start differing from ppc_hash_original, we're done.
   */
  unsigned long ppc_hash_original;

  /** @brief The prefix (in UTF-8 characters) we're rendering. */
  char ppc_title[5 * 6 + 1];

  /** @brief Is this the first call?  */
  unsigned int ppc_first : 1;

} pdb_prefix_context;

/**
 * @brief Check whether a given ID matches
 *
 *  The iterator returns 0 if it could have produced that ID,
 *  PDB_ERR_NO if not.
 *
 *  A call to check() must not be interleaved with other calls
 *  to the same iterator.
 *
 *  The iterator's type may change as a result of this call,
 *  even when returning an error.
 *
 * @param pdb		module handle
 * @param it		iterator against which we're checking.
 * @param id		id to check
 * @param cost		in/out: cost to spend.
 * @param state		in/out: state from last round, if the
 *			last round returned PDB_ERR_MORE;
 *			otherwise, a well-defined zero state.
 *
 * @return 0		if the id is likely to be in the iterator
 * @return PDB_ERR_NO	if it is definitely not in the iterator.
 * @return PDB_ERR_MORE	if the iterator ran out of time while
 *			processing the question.
 *
 * @return other nonzero errors on system error.
 */
typedef int pdb_iterator_check(pdb_handle *_pdb, pdb_iterator *_it, pdb_id _id,
                               pdb_budget *_cost);

/**
 * @brief Produce an ID that matches, iterative version.
 *
 *  Successive calls to next() will produce successive IDs
 *  in the iterator.  If sorted indicates that the iterator
 *  is sorted, the IDs are returned in ascending numerical order.
 *
 *  The iterator's type may change as a result of this call,
 *  even when returning an error.
 *
 * @param pdb		module handle
 * @param it_inout	iterator to produce an ID with.
 * @param id_out	out: id
 * @param cost		in/out: cost to spend.
 * @param state		in/out: state from last round.
 * @param file		filled in by macro, calling source file
 * @param line		filled in by macro, calling source file line
 *
 * @return 0		An id has been assigned to *id_out.
 * @return PDB_ERR_NO	The iterator is out of IDs.
 * @return PDB_ERR_MORE	Some work has been done, but we ran out of
 *			cost before finishing.  More calls with the
 *			same next_state are needed before finishing.
 * @return other nonzero errors on system error.
 */
typedef int pdb_iterator_next_loc(pdb_handle *pdb, pdb_iterator *it_inout,
                                  pdb_id *id_out, pdb_budget *cost_inout,
                                  char const *file, int line);

/**
 * @brief Produce an ID that's on or after some other ID.
 *
 * @param pdb		module handle
 * @param it_inout	iterator to produce an ID with.
 * @param id_limit	PDB_ID_NONE or limit beyond one need
 *			not search
 * @param id_inout	in/out: id
 * @param changed	out: did the id change?
 * @param cost		in/out: budget to spend.
 * @param state		in/out: state from last round.
 * @param file		filled in by macro, calling source file
 * @param line		filled in by macro, calling source file line
 *
 * @return 0		An id has been assigned to *id_out.
 * @return PDB_ERR_NO	The iterator is out of IDs.
 * @return PDB_ERR_MORE	Some work has been done, but we ran out of
 *			budget before finishing.  More calls with the
 *			same next_state are needed before finishing.
 * @return other nonzero errors on system error.
 */
typedef int pdb_iterator_find_loc(pdb_handle *pdb, pdb_iterator *it_inout,
                                  pdb_id id_in, pdb_id *id_out,
                                  pdb_budget *cost, char const *file, int line);

/**
 * @brief Compile statistics about this iterator.
 *
 *  This fills in
 *
 *	n
 *	check_cost
 *	next_cost
 *	sorted
 *
 *  where necessesary.
 *
 * @param pdb		module handle
 * @param it		iterator to produce an ID with.
 * @param cost		in/out: budget to spend.
 *
 * @return 0		Done.
 * @return PDB_ERR_MORE	Some work has been done, but we ran out of
 *			budget before finishing.  More calls with the
 *			same iterator are needed before finishing.
 * @return other nonzero errors on system error.
 */
typedef int pdb_iterator_statistics(pdb_handle *pdb, pdb_iterator *it,
                                    pdb_budget *cost);

/**
 * @brief Return a rendering of the iterator for debugging.
 *
 * @param _pdb		module handle
 * @param _it		iterator
 * @param _buf		formatting buffer to use
 * @param _size		number of bytes pointed to by buf
 *
 * @return a string representation of the iterator
 *
 *	This string includes the type and parametrization of the
 *	iterator, but not necessarily its current iteration state.
 */
typedef char const *pdb_iterator_to_string(pdb_handle *_pdb, pdb_iterator *_it,
                                           char *_buf, size_t _size);

/**
 * @brief Free resources allocated by an iterator
 *
 * @param _pdb		module handle
 * @param _it		iterator
 */
typedef void pdb_iterator_finish(pdb_handle *_pdb, pdb_iterator *_it);

/**
 * @brief Reset the iterator to the beginning
 *
 * @param _pdb		module handle
 * @param _it		iterator
 *
 * @return 0 on success, a nonzero error code on error.
 */
typedef int pdb_iterator_reset(pdb_handle *_pdb, pdb_iterator *_it);

/**
 * @brief Clone this iterator
 *
 *  Create a copy of the iterator that will return the same
 *  sequence of IDs as this iterator.
 *
 *  The clone inherits the reset state from the passed
 *  in iterator.
 *
 *  The uncloned iterator that a clone has been created from
 *  is called the clone's original.  The relationship is
 *  transitive - if I clone a clone, both the source and
 *  destination of that operation have the same original.
 *  Originals are their own original.
 *
 * @param _pdb		module handle
 * @param _clone_in	iterator to copy
 * @param _clone_out	clone of the iterator
 *
 * @return 0 on success, a nonzero error code on error.
 */
typedef int pdb_iterator_clone(pdb_handle *_pdb, pdb_iterator *_clone_in,
                               pdb_iterator **_clone_out);

/**
 * @brief Restrict this iterator
 *
 *  Create a new version (not a clone) of this iterator
 *  that takes into account the restrictions implied by
 *  the given primitive summary.
 *
 *  If this doesn't apply, just return PDB_ERR_ALREADY.
 *  If the resulting set is empty, return PDB_ERR_NO
 *  or a null iterator.
 *
 * @param _pdb		module handle
 * @param _it		iterator to restrict
 * @param _psum		primitive summary to restrict by
 * @param _it_out	new restricted iterator
 *
 * @return 0 on success, a nonzero error code on error.
 */
typedef int pdb_iterator_restrict(pdb_handle *_pdb, pdb_iterator *_it,
                                  pdb_primitive_summary const *_psum,
                                  pdb_iterator **_it_out);

/**
 * @brief Get the type and current position of an iterator, as a string.
 *
 * @param _pdb		module handle
 * @param _it		iterator
 * @param _flags	which things do we need to freeze?
 * @param _buf		buffer to write to.
 *
 * @return 0 on success, a nonzero error code on error.
 */
#define PDB_ITERATOR_FREEZE_SET 0x01
#define PDB_ITERATOR_FREEZE_POSITION 0x02
#define PDB_ITERATOR_FREEZE_STATE 0x04
#define PDB_ITERATOR_FREEZE_EVERYTHING 0x07

typedef int pdb_iterator_freeze(pdb_handle *_pdb, pdb_iterator *_it,
                                unsigned int _flags, cm_buffer *_buf);

/**
 * @brief Suspend an iterator's access to the database
 *
 * @param _pdb		module handle
 * @param _it		iterator
 *
 * @return 0 on success, a nonzero error code on unexpected error.
 */
typedef int pdb_iterator_suspend(pdb_handle *_pdb, pdb_iterator *_it);

/**
 * @brief Resume an iterator's access to the database
 *
 * @param _pdb		module handle
 * @param _it		iterator
 *
 * @return 0 on success, a nonzero error code on unexpected error.
 */
typedef int pdb_iterator_unsuspend(pdb_handle *_pdb, pdb_iterator *_it);

/**
 * @brief Return this iterator content as an idarray, if convenient
 *
 *  If an iterator knows it can never supply that idarray,
 *  it should leave this method pointer NULL.
 *
 * @param _pdb		module handle
 * @param _it		iterator
 * @param _idarray_out	out: the idarray
 * @param _s_out	out: the beginning of the iterator's range
 * @param _e_out	out: the end of the iterator's range
 *
 * @return 0 on success, a nonzero error code on error.
 * @return PDB_ERR_NO if the iterator can't supply the array
 */
typedef int pdb_iterator_idarray(pdb_handle *_pdb, pdb_iterator *_it,
                                 addb_idarray **_ida_out,
                                 unsigned long long *_s_out,
                                 unsigned long long *_e_out);

/**
 * @brief Return the primitive summary of this iterator
 *
 * @param _pdb		module handle
 * @param _it		iterator
 * @param _psum_out	out: the primitive summary
 *
 * @return 0 on success, a nonzero error code on error.
 * @return PDB_ERR_NO if the iterator can't supply a primitive summary
 */
typedef int pdb_iterator_primitive_summary(pdb_handle *_pdb, pdb_iterator *_it,
                                           pdb_primitive_summary *_psum_out);

/**
 * @brief Return the current range estimate of this iterator
 *
 * @param _pdb		module handle
 * @param _it		iterator
 * @param _range_out	out: the range estimate
 *
 * @return 0 on success, a nonzero error code on error.
 * @return PDB_ERR_NO if the iterator can't supply a range
 *  	estimate.
 */
typedef int pdb_iterator_range_estimate(pdb_handle *_pdb, pdb_iterator *_it,
                                        pdb_range_estimate *_pest_out);

/**
 * @brief Has this iterator progressed beyond this value?
 *
 *  The type of the value is implicit in the sort root.
 *  The iterator knows what the comparison is.
 *  The value has been previously returned by the iterator.
 *
 *  If an iterator is ordered, but does not define this
 *  method, it is assumed to be fully ordered - as if it
 *  always returned { true, 0 }
 *
 * @param _pdb		module handle
 * @param _it		iterator
 * @param _s		first byte of the value we're asking about
 * @param _e		last byte of the value we're asking about
 * @param _beyond_out	out: true - we'll never go below this one again,
 *			     false - we might.
 *
 * @return 0 on success, a nonzero error code on error.
 * @return PDB_ERR_NO if the iterator can't supply a primitive summary
 */

typedef int pdb_iterator_beyond(pdb_handle *_pdb, pdb_iterator *_it,
                                char const *_s, char const *_e,
                                bool *_beyond_out);

typedef struct pdb_iterator_type {
  char const *itt_name;

  pdb_iterator_finish *itt_finish;
  pdb_iterator_reset *itt_reset;
  pdb_iterator_clone *itt_clone;
  pdb_iterator_freeze *itt_freeze;
  pdb_iterator_to_string *itt_to_string;

  pdb_iterator_next_loc *itt_next_loc;
  pdb_iterator_find_loc *itt_find_loc;
  pdb_iterator_check *itt_check;
  pdb_iterator_statistics *itt_statistics;

  pdb_iterator_idarray *itt_idarray;
  pdb_iterator_primitive_summary *itt_primitive_summary;
  pdb_iterator_beyond *itt_beyond;
  pdb_iterator_range_estimate *itt_range_estimate;
  pdb_iterator_restrict *itt_restrict;

  pdb_iterator_suspend *itt_suspend;
  pdb_iterator_unsuspend *itt_unsuspend;

} pdb_iterator_type;

#define pdb_iterator_finish(a, b) \
  (((b) && (b)->it_type ? ((b)->it_type->itt_finish)((a), (b)) : (void)0))

#define pdb_iterator_reset(a, b) \
  (((b)->it_has_position = true), (((b)->it_type->itt_reset)((a), (b))))

#define pdb_iterator_clone(a, b, c) (((b)->it_type->itt_clone)((a), (b), (c)))

#define pdb_iterator_freeze(a, b, c, d) \
  (((b)->it_type->itt_freeze)((a), (b), (c), (d)))

#define pdb_iterator_next(a, b, c, d) \
  pdb_iterator_next_loc(a, b, c, d, __FILE__, __LINE__)

#define pdb_iterator_next_loc(a, b, c, e, f, g)                           \
  ((b)->it_has_position                                                   \
       ? (b)->it_type->itt_next_loc((a), (b), (c), (e), (f), (g))         \
       : (cl_notreached(pdb_log(a),                                       \
                        "pdb_iterator_next_loc() on %s-iterator without " \
                        "well-defined position",                          \
                        (b)->it_type->itt_name),                          \
          0))

#define pdb_iterator_find(a, b, c, d, e) \
  pdb_iterator_find_loc(a, b, c, d, e, __FILE__, __LINE__)
#define pdb_iterator_find_loc(a, b, c, d, e, f, g) \
  (((b)->it_has_position = true),                  \
   ((b)->it_type->itt_find_loc)((a), (b), (c), (d), (e), (f), (g)))

#define pdb_iterator_statistics(a, b, c)                                    \
  ((b)->it_original->it_statistics_done                                     \
       ? 0                                                                  \
       : ((b)->it_original->it_type->itt_statistics)((a), (b)->it_original, \
                                                     (c)))

#define pdb_iterator_check(a, b, c, d) \
  ((b)->it_has_position = false, ((b)->it_type->itt_check)((a), (b), (c), (d)))

#define pdb_iterator_to_string(a, b, c, d)                                \
  (((b) == NULL ? "(null)" : ((b)->it_type == NULL                        \
                                  ? "(no iterator)"                       \
                                  : ((b)->it_displayname != NULL          \
                                         ? (b)->it_displayname            \
                                         : ((b)->it_type->itt_to_string)( \
                                               (a), (b), (c), (d))))))

#define pdb_iterator_statistics_done(a, b) \
  ((b)->it_original->it_statistics_done)
#define pdb_iterator_statistics_done_set(a, b) \
  ((b)->it_original->it_statistics_done = true)

#define pdb_iterator_has_position(a, b) ((b)->it_has_position)

#define pdb_iterator_n(a, b) ((b)->it_original->it_n)
#define pdb_iterator_n_valid(a, b) ((b)->it_original->it_n_valid)
#define pdb_iterator_n_set(a, b, c) \
  ((b)->it_original->it_n = (c), (b)->it_original->it_n_valid = true)

#define pdb_iterator_find_cost(a, b) ((b)->it_original->it_find_cost)
#define pdb_iterator_find_cost_valid(a, b) \
  ((b)->it_original->it_check_cost_valid)
#define pdb_iterator_find_cost_set(a, b, c) \
  ((b)->it_original->it_find_cost = (c),    \
   (b)->it_original->it_find_cost_valid = true)

#define pdb_iterator_check_cost(a, b) ((b)->it_original->it_check_cost)
#define pdb_iterator_check_cost_valid(a, b) \
  ((b)->it_original->it_check_cost_valid)
#define pdb_iterator_check_cost_set(a, b, c) \
  ((b)->it_original->it_check_cost = (c),    \
   (b)->it_original->it_check_cost_valid = true)

#define pdb_iterator_next_cost(a, b) ((b)->it_original->it_next_cost)
#define pdb_iterator_next_cost_valid(a, b) \
  ((b)->it_original->it_next_cost_valid)
#define pdb_iterator_next_cost_set(a, b, c) \
  ((b)->it_original->it_next_cost = (c),    \
   (b)->it_original->it_next_cost_valid = true)

#define pdb_iterator_forward(a, b) ((b)->it_original->it_forward)
#define pdb_iterator_sorted(a, b) ((b)->it_original->it_sorted)
#define pdb_iterator_account(a, b) ((b)->it_account)
#define pdb_iterator_ordering(a, b) ((b)->it_original->it_ordering)
#define pdb_iterator_ordered(a, b) ((b)->it_original->it_ordered)
#define pdb_iterator_sorted_valid(a, b) ((b)->it_original->it_sorted_valid)
#define pdb_iterator_ordered_valid(a, b) ((b)->it_original->it_ordered_valid)

#define pdb_iterator_forward_set(a, b, c)    \
  ((b)->it_original->it_sorted = (true),     \
   (b)->it_original->it_sorted_valid = true, \
   (b)->it_original->it_forward = (c))

#define pdb_iterator_sorted_set(a, b, c) \
  ((b)->it_original->it_sorted = (c), (b)->it_original->it_sorted_valid = true)

#define pdb_iterator_ordering(a, b) ((b)->it_original->it_ordering)

#define pdb_iterator_ordering_set(a, b, c) ((b)->it_original->it_ordering = (c))

#define pdb_iterator_ordering_is(pdb, it, ordering)                 \
  ((ordering) != NULL && pdb_iterator_ordered_valid((pdb), (it)) && \
   pdb_iterator_ordering((pdb), (it)) != NULL &&                    \
   strcasecmp((ordering), pdb_iterator_ordering((pdb), (it))) == 0)

#define pdb_iterator_ordering_wants(pdb, it, ordering)                 \
  ((ordering) != NULL && pdb_iterator_ordering((pdb), (it)) != NULL && \
   strcasecmp((ordering), pdb_iterator_ordering((pdb), (it))) == 0 &&  \
   (!pdb_iterator_ordered_valid((pdb), (it)) ||                        \
    pdb_iterator_ordered((pdb), (it))))

#define pdb_iterator_ordered(a, b) ((b)->it_original->it_ordered)

#define pdb_iterator_ordered_set(a, b, c) \
  ((b)->it_original->it_ordered = (c),    \
   (b)->it_original->it_ordered_valid = true)

#define pdb_iterator_idarray(a, b, c, d, e) \
  ((b)->it_type->itt_idarray == NULL        \
       ? PDB_ERR_NO                         \
       : ((b)->it_type->itt_idarray)((a), (b), (c), (d), (e)))

#define pdb_iterator_range_estimate(a, b, c) \
  ((b)->it_type->itt_range_estimate == NULL  \
       ? PDB_ERR_NO                          \
       : ((b)->it_type->itt_range_estimate)((a), (b), (c)))

#define pdb_iterator_primitive_summary(a, b, c) \
  ((b)->it_type->itt_primitive_summary == NULL  \
       ? PDB_ERR_NO                             \
       : ((b)->it_type->itt_primitive_summary)((a), (b), (c)))

#define pdb_iterator_call_reset(pdb, it) ((it)->it_call_state = 0)

#define pdb_iterator_beyond(a, b, c, d, e)                            \
  ((pdb_iterator_ordered_valid(a, b) && pdb_iterator_ordered(a, b) && \
    (b)->it_original->it_type->itt_beyond != NULL)                    \
       ? (*(b)->it_original->it_type->itt_beyond)(a, b, c, d, e)      \
       : ((*(e) = false), 0))

#define pdb_iterator_account_set(pdb, it, acc) \
  ((void)((void)(pdb), (it)->it_account = (acc)))

#define pdb_iterator_account_charge(pdb, it, what, n_call, n_cost) \
  ((it)->it_account == NULL                                        \
       ? 0                                                         \
       : (((it)->it_account->ia_##what##_n += (n_call)),           \
          ((it)->it_account->ia_##what##_cost += (n_cost))))

#define pdb_iterator_account_charge_budget(pdb, it, what)               \
  ((it)->it_account == NULL                                             \
       ? 0                                                              \
       : (((it)->it_account->ia_##what##_n += ((err) != PDB_ERR_MORE)), \
          ((it)->it_account->ia_##what##_cost += budget_in - *budget_inout)))

#define pdb_iterator_restrict(pdb, it, psum, out)                       \
  ((it)->it_original->it_type->itt_restrict == NULL                     \
       ? pdb_iterator_restrict_default(pdb, it->it_original, psum, out) \
       : (*(it)->it_original->it_type->itt_restrict)(pdb, it, psum, out))

#define pdb_iterator_suspend(pdb, it)               \
  ((it)->it_suspended = true,                       \
   ((it)->it_original->it_type->itt_suspend == NULL \
        ? 0                                         \
        : (*(it)->it_original->it_type->itt_suspend)(pdb, it)))

#define pdb_iterator_unsuspend(pdb, it)               \
  ((it)->it_suspended = false,                        \
   ((it)->it_original->it_type->itt_unsuspend == NULL \
        ? 0                                           \
        : (*(it)->it_original->it_type->itt_unsuspend)(pdb, it)))

struct pdb_iterator {
  unsigned long it_magic;

  pdb_iterator_type const *it_type;

  /**
   * @brief The original.
   *
   *	Complex iterators keep their statistics in the
   *	original, sharing results.
   *
   *	For iterators that aren't clones, this pointer
   *	points to the iterator itself.
   */
  struct pdb_iterator *it_original;

  /**
   * @brief a unique id shared between original and clones
   *  	at time of cloning.  Used to detect substitutions.
   */
  unsigned long it_id;

  /**
   * @brief Call state for interruptible calls next, find,
   *  	check, and statistics.
   */
  short it_call_state;

  /**
   * @brief NULL or a cached displayname, allocated in the
   *	pdb handle.
   */
  char *it_displayname;

  /**
   * @brief original only: NULL or a desired ordering.
   *	If the iterator is marked as "ordered", it's ordered
   *	by this ordering.
   */
  char const *it_ordering;

  /**
   * @brief For an original, the number of references to the original,
   *  	including itself.  For a clone, 1.
   */
  size_t it_refcount;

  /**
   * @brief How many iterators than refer to this one as their
   *  	original?
   *
   *   	We're counting them not just as another link
   *	because we need to count the number of clones
   * 	of a primitive iterator -- uncloned iterators
   *	can be moved, cloned ones cannot.
   */
  size_t it_clones;

  /**
   * @brief The estimated average cost of a call to pdb_iterator_next().
   */
  pdb_budget it_next_cost;

  /**
   * @brief The estimated average cost of a call to pdb_iterator_check().
   */
  pdb_budget it_check_cost;

  /**
   * @brief The estimated average cost of an initial "on-or-after"
   * 	or "next", without already being in the neighborhood.
   */
  pdb_budget it_find_cost;

  /**
   * @brief Is this iterator's output known to be ordered?
   */
  unsigned int it_ordered : 1;

  /**
   * @brief Is this iterator's output known to be sorted?   Only
   *  	sorted iterators support calls to pdb_iterator_find().
   */
  unsigned int it_sorted : 1;

  /**
   * @brief If sorted, does this iterator run from low to high?
   */
  unsigned int it_forward : 1;

  /**
   * @brief Is the estimate of it_n valid yet?  If not, more calls
   * 	to pdb_iterator_statistics() are needed.
   */
  unsigned int it_n_valid : 1;

  /**
   * @brief Is the it_ordered flag valid yet?
   */
  unsigned int it_ordered_valid : 1;

  /**
   * @brief Is the it_sorted flag valid yet?  If not, more
   * 	calls to pdb_iterator_statistics() are needed.
   */
  unsigned int it_sorted_valid : 1;

  /**
   * @brief Is the production cost valid yet?  If not, more
   * 	calls to pdb_iterator_statistics() are needed.
   */
  unsigned int it_next_cost_valid : 1;

  /**
   * @brief Is the check_cost valid yet?  If not, more calls to
   * 	pdb_iterator_statistics() are needed.
   */
  unsigned int it_check_cost_valid : 1;

  /**
   * @brief Is the find_cost valid yet?  If not, more calls
   *  	to  pdb_iterator_statistics() are needed.
   */
  unsigned int it_find_cost_valid : 1;

  /**
   * @brief The first ID  that the iterator returns must not
   *	be < it_low_id.   Default: 0.
   */
  unsigned long long it_low;

  /**
   * @brief The last ID  that the iterator returns must not
   *	be >= it_high_id.  (In other words, it_high_id is
   *  	the first ID that is not returned.)  Default: ULLONG_MAX.
   */
  unsigned long long it_high;

  /**
   * @brief -1ull, or the number of elements in this iterator.
   */
  unsigned long long it_n;

  /**
   * @brief Set to true once statistics have been completed.
   */
  unsigned int it_statistics_done : 1;

  /**
   * @brief Set to true if the iterator has a well-defined position.
   *	(This happens largely in macros.)
   */
  unsigned int it_has_position : 1;

  /**
   * @brief Set to true if the iterator is suspended, cleared
   *  	when it is resumed.
   */
  unsigned int it_suspended : 1;

  /* @brief Doubly linked list of all iterators, helps debug leaks.
   */
  struct pdb_iterator *it_next;
  struct pdb_iterator *it_prev;

  /* @brief Doubly linked list of iterators who want to get
   *  	suspend- or unsuspend notices.
   */
  struct pdb_iterator *it_suspend_next;
  struct pdb_iterator *it_suspend_prev;

  /* @brief Where in the source code was this created?
   */
  char const *it_file;
  int it_line;
  pdb_iterator_chain *it_chain;

  /* @brief NULL or track iterator budget consumption here.
   */
  pdb_iterator_account *it_account;

  /** @brief NULL or track iterator names here.
   */
  pdb_iterator_by_name *it_by_name;

  union {
    struct pdb_iterator_hmap {
      addb_hmap *body_hmap_hmap;
      addb_hmap_id body_hmap_hash_of_key;
      char *body_hmap_key;
      size_t body_hmap_key_len;
      addb_hmap_type body_hmap_type;
      addb_idarray body_hmap_ida;

      /* The current position; somewhere in 0...N.
       * (N after returning PDB_ERR_NO to next.)
       */
      unsigned long long body_hmap_offset;

      /* Start offset in the underlying array.
       */
      unsigned long long body_hmap_start;

      /* End offset in the underlying array (first not
       * included.)
       */
      unsigned long long body_hmap_end;

      /* ID at position (end - 1), if any.
       */
      addb_gmap_id body_hmap_last;

#define it_hmap it_body.body_hmap.body_hmap_hmap
#define it_hmap_hash_of_key it_body.body_hmap.body_hmap_hash_of_key
#define it_hmap_key it_body.body_hmap.body_hmap_key
#define it_hmap_key_len it_body.body_hmap.body_hmap_key_len
#define it_hmap_type it_body.body_hmap.body_hmap_type
#define it_hmap_iterator it_body.body_hmap.body_hmap_iterator

#define it_hmap_last it_body.body_hmap.body_hmap_last
#define it_hmap_source it_body.body_hmap.body_hmap_source
#define it_hmap_ida it_body.body_hmap.body_hmap_ida
#define it_hmap_offset it_body.body_hmap.body_hmap_offset
#define it_hmap_start it_body.body_hmap.body_hmap_start
#define it_hmap_end it_body.body_hmap.body_hmap_end

    } body_hmap;

    struct pdb_iterator_gmap {
      /* To iterate over links from a GMAP entry,
       * use a gmap iterator.
       */
      addb_gmap *body_gmap_gmap;
      addb_gmap_id body_gmap_source;
      int body_gmap_linkage;
      addb_idarray body_gmap_ida;

      graph_guid body_gmap_source_guid;
      unsigned int body_gmap_source_guid_valid : 1;

      /* The current position; somewhere in 0...N.
       * (N after returning PDB_ERR_NO to next.)
       */
      unsigned long long body_gmap_offset;

      /* Start offset in the underlying array.
       */
      unsigned long long body_gmap_start;

      /* End offset in the underlying array (first not
       * included.)
       */
      unsigned long long body_gmap_end;

      /*  Cached most recent query.
       *  Set to PDB_ID_NONE when not in use.
       */
      pdb_id body_gmap_cached_check_id;
      unsigned int body_gmap_cached_check_result : 1;

#define it_gmap it_body.body_gmap.body_gmap_gmap
#define it_gmap_source it_body.body_gmap.body_gmap_source
#define it_gmap_source_guid it_body.body_gmap.body_gmap_source_guid
#define it_gmap_source_guid_valid it_body.body_gmap.body_gmap_source_guid_valid
#define it_gmap_linkage it_body.body_gmap.body_gmap_linkage
#define it_gmap_ida it_body.body_gmap.body_gmap_ida
#define it_gmap_offset it_body.body_gmap.body_gmap_offset
#define it_gmap_start it_body.body_gmap.body_gmap_start
#define it_gmap_end it_body.body_gmap.body_gmap_end
#define it_gmap_cached_check_id it_body.body_gmap.body_gmap_cached_check_id
#define it_gmap_cached_check_result \
  it_body.body_gmap.body_gmap_cached_check_result

    } body_gmap;

    struct pdb_iterator_all {
      /* To iterate over everything, use a maximum
       * and a counter.
       */
      addb_istore_id body_all_i;
      addb_istore_id body_all_m;

#define it_all_i it_body.body_all.body_all_i
#define it_all_m it_body.body_all.body_all_m

    } body_all;

    struct pdb_iterator_bgmap {
      addb_gmap *body_bgmap_gmap;
      addb_bgmap *body_bgmap_bgmap;
      addb_gmap_id body_bgmap_source;
      int body_bgmap_linkage;
      unsigned long long body_bgmap_offset;
      addb_gmap_id body_bgmap_find_hold;
      addb_gmap_id body_bgmap_recover_n;
      addb_gmap_id body_bgmap_recover_count;
      addb_gmap_id body_bgmap_recover_pos;
      bool body_bgmap_need_recover;
    } body_bgmap;

#define it_bgmap it_body.body_bgmap.body_bgmap_bgmap
#define it_bgmap_gmap it_body.body_bgmap.body_bgmap_gmap
#define it_bgmap_source it_body.body_bgmap.body_bgmap_source
#define it_bgmap_linkage it_body.body_bgmap.body_bgmap_linkage
#define it_bgmap_offset it_body.body_bgmap.body_bgmap_offset
#define it_bgmap_find_hold it_body.body_bgmap.body_bgmap_find_hold
#define it_bgmap_recover_n it_body.body_bgmap.body_bgmap_recover_n
#define it_bgmap_recover_count it_body.body_bgmap.body_bgmap_recover_count
#define it_bgmap_recover_pos it_body.body_bgmap.body_bgmap_recover_pos
#define it_bgmap_need_recover it_body.body_bgmap.body_bgmap_need_recover
    void *body_theory;
#define it_theory it_body.body_theory

  } it_body;
};

typedef size_t pdb_reference;
typedef unsigned long long pdb_timestamp_t;

typedef struct pdb_runtime_statistics {
  /**
   * @brief Number of primitives written to the database
   */
  unsigned long long rts_primitives_written;

  /**
   * @brief Number of primitives read from the database
   */
  unsigned long long rts_primitives_read;

  /**
   * @brief Number of index file sizes that were looked up;
   *  if the resulting array had more than a single element,
   *  its sentinel element has been read to determine its size.
   */
  unsigned long long rts_index_extents_read;

  /**
   * @brief Number of index file elements that were read, usually as part
   *  of an array traversal.
   */
  unsigned long long rts_index_elements_read;

  /**
   * @brief Number of elements that were added to an index file,
    *	such as a gmap or hmap.
   */
  unsigned long long rts_index_elements_written;

} pdb_runtime_statistics;

typedef struct pdb_iterator_property {
  char const *pip_name;
  char const *pip_s;
  char const *pip_e;

} pdb_iterator_property;

/*  Callback in the subscribe/unsubscribe interface to primitive allocations
 */
typedef int pdb_primitive_callback(void *_callback_data, pdb_handle *_handle,
                                   pdb_id _id, pdb_primitive const *_primitive);
/* pdb-checkpoint.c */

pdb_id pdb_checkpoint_id_on_disk(pdb_handle *p);
int pdb_checkpoint_mandatory(pdb_handle *, bool block);
int pdb_checkpoint_optional(pdb_handle *, pdb_msclock_t);
int pdb_checkpoint_rollback(pdb_handle *, pdb_msclock_t);
int pdb_checkpoint_synchronize(pdb_handle *);
unsigned long long pdb_checkpoint_deficit(pdb_handle *pdb);
bool pdb_checkpoint_urgent(pdb_handle *pdb);
addb_istore_id pdb_checkpoint_horizon(pdb_handle *pdb);

/* pdb-concentric.c */

int pdb_concentric_initialize(pdb_handle *pdb, pdb_id *state);

/* pdb-configure.c */

pdb_configuration *pdb_config(pdb_handle *pdb);
void pdb_configure(pdb_handle *, pdb_configuration const *);

/* pdb-create.c  */

pdb_handle *pdb_create(cm_handle *, cl_handle *, int version);

/* pdb-database-id.c */

unsigned long long pdb_database_id(pdb_handle *);

/* pdb-database-path.c */

char const *pdb_database_path(pdb_handle *);

/* pdb-destroy.c */

int pdb_destroy(pdb_handle *);
int pdb_close_databases(pdb_handle *);

/* pdb-disk.c */

bool pdb_disk_is_available(pdb_handle const *);
void pdb_disk_set_available(pdb_handle *, bool);

/* pdb-generation.c */

int pdb_generation_lineage_n(pdb_handle *_pdb, pdb_id _id,
                             unsigned long long *_n_out);

int pdb_generation_guid_idarray(pdb_handle *_pdb, pdb_guid const *_guid,
                                pdb_id *_lineage_id_out, pdb_id *_gen_out,
                                addb_idarray *_ida_out);

int pdb_generation_guid_to_iterator(pdb_handle *_pdb, pdb_guid const *_guid,
                                    pdb_id *_lineage_id_out, pdb_id *_gen_out,
                                    addb_hmap_iterator *_lineage_iter_out);

int pdb_generation_guid_to_lineage(pdb_handle *_pdb, graph_guid const *_guid,
                                   pdb_id *_lineage_id_out, pdb_id *_gen_out);

int pdb_generation_check_range(pdb_handle *_pdb, graph_dateline const *_asof,
                               graph_guid const *_guid_key, pdb_id _id,
                               int _new_valid, unsigned long long _new_min,
                               unsigned long long _new_max, int _old_valid,
                               unsigned long long _old_min,
                               unsigned long long _old_max);

int pdb_generation_last_n(pdb_handle *_pdb, graph_dateline const *_asof,
                          graph_guid const *_guid_key, pdb_id *_last_out,
                          pdb_id *_n_out);

int pdb_generation_nth(pdb_handle *_pdb, graph_dateline const *_asof,
                       graph_guid const *_guid_key, bool _is_newest,
                       unsigned long long _off, pdb_id *_id_out,
                       graph_guid *_guid_out);

int pdb_generation_synchronize(pdb_handle *_pdb, pdb_id _id,
                               pdb_primitive const *_pr);

/* pdb-hash.c */

int pdb_hash_iterator(pdb_handle *pdb, addb_hmap_type t, char const *key,
                      size_t key_len, pdb_id low, pdb_id high, bool forward,
                      pdb_iterator **it_out);

int pdb_hash_count(pdb_handle *pdb, addb_hmap_type t, char const *key,
                   size_t key_len, pdb_id low, pdb_id high,
                   unsigned long long upper_bound, unsigned long long *n_out);

int pdb_hash_number_iterator(pdb_handle *pdb, const graph_number *n, pdb_id low,
                             pdb_id high, bool forward, pdb_iterator **out);

/* pdb-id.c */

#define pdb_id_to_guid(a, b, c) pdb_id_to_guid_loc(a, b, c, __FILE__, __LINE__)
int pdb_id_to_guid_loc(pdb_handle *_pdb, pdb_id _id, graph_guid *_guid_out,
                       char const *_file, int _line);

int pdb_id_from_guid(pdb_handle *_pdb, pdb_id *_id_out,
                     graph_guid const *_guid);

#define pdb_id_read(a, b, c) pdb_id_read_loc(a, b, c, __FILE__, __LINE__)
int pdb_id_read_loc(pdb_handle *_pdb, pdb_id _id, pdb_primitive *_pr,
                    char const *_file, int _line);

char const *pdb_id_to_string(pdb_handle *_pdb, pdb_id _id, char *_buf,
                             size_t _size);

int pdb_id_from_string(pdb_handle *_pdb, pdb_id *_id_out, char const **_s_ptr,
                       char const *_e);

/* pdb-initialize.c */

int pdb_initialize_checkpoint(pdb_handle *pdb);
int pdb_configure_done(pdb_handle *);
int pdb_initialize_names(pdb_handle *);
int pdb_initialize(pdb_handle *pdb);
int pdb_spawn(pdb_handle *, pid_t);
int pdb_get_max_files(void);

/* pdb-iterator.c */

int pdb_iterator_range_estimate_default(pdb_handle *_pdb, pdb_iterator *_it,
                                        pdb_range_estimate *_range);

unsigned long long pdb_iterator_spread(pdb_handle *_pdb,
                                       pdb_iterator const *_it);

void pdb_iterator_statistics_copy(pdb_handle *_pdb, pdb_iterator *_dst,
                                  pdb_iterator const *_src);

void pdb_iterator_chain_finish(pdb_handle *const _pdb,
                               pdb_iterator_chain *const _chain,
                               char const *const _name);

void pdb_iterator_chain_clear(pdb_handle *_pdb, pdb_iterator_chain *_chain);

void pdb_iterator_chain_set(pdb_handle *_pdb, pdb_iterator_chain *_chain);

void pdb_iterator_chain_out(pdb_handle *_pdb, pdb_iterator *_it);

void pdb_iterator_chain_in(pdb_handle *pdb, pdb_iterator *it);

void pdb_iterator_base_finish(pdb_handle *_pdb, pdb_iterator_base *_pib);

int pdb_iterator_base_initialize(pdb_handle *_pdb, cm_handle *_cm,
                                 pdb_iterator_base *_pib);

void *pdb_iterator_base_lookup(pdb_handle *_pdb, pdb_iterator_base *_pib,
                               char const *_name);

pdb_iterator_account *pdb_iterator_base_account_lookup(
    pdb_handle *_pdb, pdb_iterator_base const *_pib, size_t _number);

int pdb_iterator_base_delete(pdb_handle *_pdb, pdb_iterator_base *_pib,
                             char const *_name);

int pdb_iterator_base_set(pdb_handle *_pdb, pdb_iterator_base *_pib,
                          char const *_name, void *_ptr);

void pdb_iterator_base_set_account_resolver(
    pdb_handle *_pdb, pdb_iterator_base *_pib,
    pdb_iterator_base_account_resolver *_callback, void *_callback_data);

int pdb_iterator_single_id(pdb_handle *_pdb, pdb_iterator *_it,
                           pdb_id *_id_out);

int pdb_iterator_scan_forward_low_high(cl_handle *_cl, char const *_who,
                                       char const **_s_ptr, char const *_e,
                                       bool *_forward_out,
                                       unsigned long long *_low_out,
                                       unsigned long long *_high_out);

int pdb_iterator_freeze_statistics(pdb_handle *_pdb, cm_buffer *_buf,
                                   pdb_iterator *_it);

int pdb_iterator_freeze_account(pdb_handle *_pdb, cm_buffer *_buf,
                                pdb_iterator *_it);

int pdb_iterator_freeze_ordering(pdb_handle *_pdb, cm_buffer *_buf,
                                 pdb_iterator *_it);

int pdb_iterator_freeze_intro(cm_buffer *_buf, pdb_iterator *_it,
                              char const *_name);

void pdb_iterator_dup(pdb_handle *, pdb_iterator *);
int pdb_iterator_new_id(pdb_handle *);
int pdb_iterator_refresh(pdb_handle *, pdb_iterator *);
int pdb_iterator_refresh_pointer(pdb_handle *, pdb_iterator **);
void pdb_iterator_unlink_clone(pdb_handle *, pdb_iterator *);
void pdb_iterator_destroy(pdb_handle *, pdb_iterator **);
int pdb_iterator_substitute(pdb_handle *_pdb, pdb_iterator *_destination,
                            pdb_iterator *_source);

void pdb_iterator_parse(char const *_s, char const *_e,
                        pdb_iterator_text *_pit);

bool pdb_iterator_parse_parallel_next(char const *set_s, char const *set_e,
                                      char const *posstate_s,
                                      char const *posstate_e, char const *seps,
                                      pdb_iterator_text *pit);

bool pdb_iterator_parse_next(char const *s, char const *e,
                             char const *separators, char const **out_s,
                             char const **out_e);

int pdb_iterator_thaw(pdb_handle *_pdb, pdb_iterator_text const *_pit,
                      pdb_iterator_base *_pib, pdb_iterator **_it_out);

void pdb_iterator_initialize(pdb_iterator *);
void pdb_iterator_make_loc(pdb_handle *_pdb, pdb_iterator *_it, pdb_id _low,
                           pdb_id _high, bool _forward, char const *_file,
                           int _line);
#define pdb_iterator_make(a, b, c, d, e) \
  pdb_iterator_make_loc(a, b, c, d, e, __FILE__, __LINE__)

int pdb_iterator_make_clone_loc(pdb_handle *pdb, pdb_iterator *original_in,
                                pdb_iterator **clone_out, char const *file,
                                int line);
#define pdb_iterator_make_clone(a, b, c) \
  pdb_iterator_make_clone_loc(a, b, c, __FILE__, __LINE__)

pdb_budget pdb_iterator_bsearch_cost(unsigned long long _n,
                                     unsigned long long _n_per_tile,
                                     pdb_budget _array_cost,
                                     pdb_budget _element_cost);

int pdb_iterator_check_nonstep(pdb_handle *_pdb, pdb_iterator *_it, pdb_id _id);

#define pdb_iterator_next_nonstep(a, b, c) \
  pdb_iterator_next_nonstep_loc((a), (b), (c), __FILE__, __LINE__)
int pdb_iterator_next_nonstep_loc(pdb_handle *_pdb, pdb_iterator *_it,
                                  pdb_id *_id_out, char const *_file,
                                  int _line);

#define pdb_iterator_find_nonstep(a, b, c, d) \
  pdb_iterator_find_nonstep_loc((a), (b), (c), (d), __FILE__, __LINE__)
int pdb_iterator_find_nonstep_loc(pdb_handle *_pdb, pdb_iterator *_it,
                                  pdb_id _id_in, pdb_id *_id_out,
                                  char const *_file, int _line);

int pdb_iterator_intersect_loc(pdb_handle *_pdb, pdb_iterator *_a,
                               pdb_iterator *_b, pdb_id _low, pdb_id _high,
                               pdb_budget *_budget_inout, pdb_id *_id_out,
                               size_t *_n_inout, size_t _m, char const *file,
                               int line);

#define pdb_iterator_intersect(a, b, c, d, e, f, g, h, i) \
  pdb_iterator_intersect_loc(a, b, c, d, e, f, g, h, i, __FILE__, __LINE__)

#define pdb_iterator_fixed_intersect(a, b, c, d, e, f, g) \
  pdb_iterator_fixed_intersect_loc(a, b, c, d, e, f, g, __FILE__, __LINE__)

int pdb_iterator_fixed_intersect_loc(pdb_handle *pdb, pdb_iterator *a,
                                     pdb_id *b_id, size_t b_n, pdb_id *id_inout,
                                     size_t *n_inout, size_t m,
                                     char const *file, int line);

int pdb_iterator_restrict_default(pdb_handle *_pdb, pdb_iterator *_it,
                                  pdb_primitive_summary const *_psum,
                                  pdb_iterator **_it_out);

/* pdb-iterator-all.c */

int pdb_iterator_all_create(pdb_handle *_pdb, pdb_id _low, pdb_id _high,
                            bool _forward, pdb_iterator **_it_out);

int pdb_iterator_all_thaw(pdb_handle *_pdb, pdb_iterator_text const *_pit,
                          pdb_iterator_base *_pib, pdb_iterator **_it_out);

bool pdb_iterator_all_is_instance(pdb_handle *_pdb, pdb_iterator const *_it);

/* pdb-iterator-bgmap.c */

int pdb_iterator_bgmap_thaw(pdb_handle *pdb, pdb_iterator_text const *pit,
                            pdb_iterator_base *pib, pdb_iterator **it_out);

int pdb_iterator_bgmap_create(pdb_handle *pdb, addb_gmap *gm, pdb_id source,
                              int linkage, pdb_id high, pdb_id low,
                              bool forward, pdb_iterator **id_out);

bool pdb_iterator_bgmap_is_instance(pdb_handle *pdb, pdb_iterator *it,
                                    int linakge);

bool pdb_iterator_xgmap_is_instance(pdb_handle *pdb, pdb_iterator *it,
                                    int linkage);

int pdb_iterator_bgmap_position_recover_init(pdb_handle *pdb, pdb_iterator *it,
                                             pdb_id gmap_position);

int pdb_iterator_bgmap_position_recover_work(pdb_handle *pdb, pdb_iterator *it,
                                             pdb_budget *budget_inout);

const char *pdb_iterator_bgmap_name(pdb_handle *pdb, pdb_iterator *it);

const char *pdb_gmap_to_name(pdb_handle *pdb, addb_gmap *gmap);

#define pdb_iterator_bgmap_next(a, b, c, d) \
  pdb_iterator_bgmap_next_loc(a, b, c, d, __FILE__, __LINE__)

#define pdb_iterator_bgmap_find(a, b, c, d, e) \
  pdb_iterator_bgmap_find_loc(a, b, c, d, e, __FILE__, __LINE__)

/* pdb-iterator-gmap.c */

int pdb_iterator_gmap_verify_check(pdb_handle *_pdb, pdb_iterator *_it,
                                   pdb_id _id, pdb_budget *_budget_inout);

int pdb_iterator_gmap_create(pdb_handle *_pdb, addb_gmap *_gmap, int _linkage,
                             pdb_id _source, pdb_id _low, pdb_id _high,
                             bool _forward, bool _error_if_null,
                             pdb_iterator **_it_out);

int pdb_iterator_gmap_thaw(pdb_handle *_pdb, pdb_iterator_text const *_pit,
                           pdb_iterator_base *_pib, pdb_iterator **_it_out);

bool pdb_iterator_gmap_is_instance(pdb_handle *_pdb, pdb_iterator const *_it,
                                   int _linkage);

int pdb_iterator_gmap_linkage(pdb_handle *_pdb, pdb_iterator *_it,
                              int *_linkage);

int pdb_iterator_gmap_source_id(pdb_handle *_pdb, pdb_iterator *_it,
                                pdb_id *_source_id);

int pdb_iterator_gmap_to_vip(pdb_handle *_pdb, pdb_iterator *_it, int _linkage,
                             graph_guid const *_qualifier,
                             pdb_iterator **_it_out);

/* pdb-iterator-hmap.c */

int pdb_iterator_hmap_create(pdb_handle *_pdb, addb_hmap *_hmap,
                             addb_hmap_id _hash_of_key, char const *const _key,
                             size_t _key_len, addb_hmap_type _type, pdb_id _low,
                             pdb_id _high, bool _forward, bool _error_if_null,
                             pdb_iterator **_it_out);

int pdb_iterator_hmap_thaw(pdb_handle *_pdb, pdb_iterator_text const *_pit,
                           pdb_iterator_base *_pib, pdb_iterator **_it_out);

bool pdb_iterator_hmap_is_instance(pdb_handle *pdb, pdb_iterator *it,
                                   char const **name_out,
                                   unsigned long long *hash_out,
                                   char const **key_s_out,
                                   char const **key_e_out);

/* pdb-iterator-null.c */

int pdb_iterator_null_thaw(pdb_handle *_pdb, pdb_iterator_text const *_pit,
                           pdb_iterator_base *_pib, pdb_iterator **_it_out);

int pdb_iterator_null_create_loc(pdb_handle *, pdb_iterator **, char const *,
                                 int);
#define pdb_iterator_null_create(a, b) \
  pdb_iterator_null_create_loc(a, b, __FILE__, __LINE__)

int pdb_iterator_null_become(pdb_handle *, pdb_iterator *);
void pdb_iterator_null_reinitialize(pdb_handle *, pdb_iterator *);
bool pdb_iterator_null_is_instance(pdb_handle *_pdb, pdb_iterator const *_it);

bool pdb_iterator_thaw_scan(pdb_handle *pdb, char const **s_ptr, char const *e,
                            char const *fmt, ...);

/* pdb-iterator-by-name.c */

pdb_iterator *pdb_iterator_by_name_lookup(pdb_handle *pdb,
                                          pdb_iterator_base const *pib,
                                          char const *s, char const *e);

int pdb_iterator_by_name_link(pdb_handle *pdb, pdb_iterator_base *pib,
                              pdb_iterator *it, char const *s, char const *e);

void pdb_iterator_by_name_unlink(pdb_handle *_pdb, pdb_iterator *_it);

/* pdb-iterator-suspend.c */

int pdb_iterator_unsuspend_chain(pdb_handle *pdb, pdb_iterator_chain *ch);
int pdb_iterator_suspend_all(pdb_handle *);

void pdb_iterator_suspend_chain_out(pdb_handle *pdb, pdb_iterator *it);

void pdb_iterator_suspend_chain_in(pdb_handle *pdb, pdb_iterator *it);

void pdb_iterator_unsuspend_chain_out(pdb_handle *pdb, pdb_iterator *it);

void pdb_iterator_unsuspend_chain_in(pdb_handle *pdb, pdb_iterator *it);

bool pdb_iterator_suspend_is_chained_in(pdb_handle *pdb, pdb_iterator *it);

bool pdb_iterator_unsuspend_is_chained_in(pdb_handle *pdb, pdb_iterator *it);

void pdb_iterator_suspend_save(pdb_handle *pdb, pdb_iterator *it,
                               pdb_iterator_chain **chain_out);

void pdb_iterator_suspend_restore(pdb_handle *pdb, pdb_iterator *it,
                                  pdb_iterator_chain *chain);

/* pdb-iterator-util.c */

char const *pdb_unparenthesized(char const *s, char const *e, int ch);

int pdb_iterator_util_thaw(pdb_handle *_pdb, char const **_s_ptr,
                           char const *_e, char const *_fmt, ...);

void pdb_iterator_util_finish(pdb_handle *, pdb_iterator *);

int pdb_iterator_util_statistics_none(pdb_handle *_pdb, pdb_iterator *_it,
                                      pdb_budget *_budget);

/* pdb-linkage.c */

int pdb_linkage_from_string(char const *s, char const *e);
char const *pdb_linkage_to_string(int linkage);

int pdb_linkage_count(pdb_handle *_pdb, int _linkage, pdb_id _source,
                      pdb_id _low, pdb_id _high,
                      unsigned long long _upper_bound,
                      unsigned long long *_n_out);

int pdb_linkage_count_est(pdb_handle *_pdb, int _linkage, pdb_id _source,
                          pdb_id _low, pdb_id _high,
                          unsigned long long _upper_bound,
                          unsigned long long *_n_out);

int pdb_linkage_guid_count_est(pdb_handle *_pdb, int _linkage,
                               graph_guid const *_source_guid, pdb_id _low,
                               pdb_id _high, unsigned long long _upper_bound,
                               unsigned long long *_n_out);

int pdb_linkage_iterator(pdb_handle *_pdb, int _linkage,
                         graph_guid const *_linkage_guid, pdb_id _low,
                         pdb_id _high, bool _forward, bool _error_if_null,
                         pdb_iterator **_it_out);

int pdb_linkage_id_iterator(pdb_handle *_pdb, int _linkage, pdb_id _source,
                            pdb_id _low, pdb_id _high, bool _forward,
                            bool _error_if_null, pdb_iterator **_it_out);

int pdb_linkage_synchronize(pdb_handle *pdb, pdb_id id,
                            pdb_primitive const *pr);

/* pdb-log.c */

cl_handle *pdb_log(pdb_handle *);

/* pdb-mem.c */

cm_handle *pdb_mem(pdb_handle *);

/* pdb-msclock.c */

pdb_msclock_t pdb_msclock(pdb_handle *);

/* pdb-prefix.c */

void pdb_prefix_statistics_drift(pdb_handle *_pdb, char const *_s,
                                 char const *_e);

void pdb_prefix_statistics_store(pdb_handle *_pdb, pdb_iterator const *_it,
                                 char const *_s, char const *_e);

int pdb_prefix_statistics_load(pdb_handle *_pdb, pdb_iterator *_it,
                               char const *_s, char const *_e);

int pdb_prefix_next(pdb_prefix_context *_ppc, pdb_id _low, pdb_id _high,
                    bool _forward, pdb_iterator **_it_out);

void pdb_prefix_initialize(pdb_handle *_pdb, char const *_s, char const *_e,
                           pdb_prefix_context *_ppc);

/* pdb-primitive-alloc.c */

int pdb_primitive_alloc(pdb_handle *_pdb, graph_timestamp_t _now,
                        graph_guid const *_pred_guid, pdb_primitive *_pr_out,
                        graph_guid *_guid_out, unsigned long long _timestamp,
                        unsigned char _valuetype, unsigned _bits,
                        size_t _name_size, size_t _value_size,
                        const char *_name, const char *_value,
                        const graph_guid *_type, const graph_guid *_right,
                        const graph_guid *_left, const graph_guid *_scope,
                        const graph_guid *_myguid, char *_errbuf,
                        size_t _errbuf_size);

int pdb_primitive_alloc_commit(pdb_handle *_pdb, graph_guid const *_pred_guid,
                               graph_guid const *_guid, pdb_primitive *_pr,
                               char *_errbuf, size_t _errbuf_size);

/* pdb-primitive-alloc-subscription.c */

int pdb_primitive_alloc_subscription_add(pdb_handle *pdb,
                                         pdb_primitive_callback *callback,
                                         void *data);

/* pdb-primitive-compress.c */
size_t pdb_primitive_guid_offset(const pdb_primitive *pr, int link);

void pdb_primitive_linkage_get_ptr(const pdb_primitive *pr, int link,
                                   graph_guid *g);

int pdb_primitive_linkage_set_ptr(pdb_primitive *pr, const graph_guid *g,
                                  unsigned char *buffer);

size_t pdb_primitive_len(const pdb_primitive *pr);

void pdb_primitive_zero(pdb_primitive *pr);
/* pdb-primitive-previous.c */

int pdb_primitive_previous_guid(pdb_handle *_pdb, pdb_primitive const *_pr,
                                graph_guid *_prev_out);

/* pdb-primitive-dump.c */

char const *pdb_primitive_check(pdb_primitive const *_pr, char *_buf,
                                size_t _size);

char const *pdb_primitive_to_string(pdb_primitive const *_pr, char *_buf,
                                    size_t _size);

#define pdb_primitive_dump(a, b) \
  pdb_primitive_dump_loc(a, b, __FILE__, __LINE__)
void pdb_primitive_dump_loc(cl_handle *_cl, pdb_primitive const *_pr,
                            char const *_file, int _line);

/* pdb-primitive-finish.c */

#define pdb_primitive_finish(a, b) \
  pdb_primitive_finish_loc(a, b, __FILE__, __LINE__)
void pdb_primitive_finish_loc(pdb_handle *_pdb, pdb_primitive *_pr,
                              char const *_file, int _line);

/* pdb-primitive-initialize.c */

void pdb_primitive_initialize(pdb_primitive *_pr);

/* pdb-primitive-n.c */

unsigned long long pdb_primitive_n(pdb_handle *pdb);

/* pdb-primitive-read.c  */

#define pdb_primitive_read(a, b, c) \
  pdb_primitive_read_loc(a, b, c, __FILE__, __LINE__)
int pdb_primitive_read_loc(pdb_handle *_pdb, graph_guid const *_guid,
                           pdb_primitive *_pr, char const *_file, int _line);

unsigned long pdb_primitive_link_bitmask(pdb_primitive *pr);

/* pdb-primitive-reference.c */

#define pdb_primitive_reference_is_empty(pref) \
  addb_istore_reference_is_empty(pref)

#define pdb_primitive_reference_initialize(pref) \
  addb_istore_reference_initialize(pref)

#define pdb_primitive_reference_free(a) \
  pdb_primitive_reference_free_loc(a, __FILE__, __LINE__)

#define pdb_primitive_reference_free_loc(pr, file, line) \
  addb_istore_reference_free_loc(pr, file, line)

#define pdb_primitive_reference_from_primitive(a, b) \
  pdb_primitive_reference_from_primitive_loc(a, b, __FILE__, __LINE__)
void pdb_primitive_reference_from_primitive_loc(pdb_primitive_reference *_pref,
                                                pdb_primitive const *_pr,
                                                char const *_file, int _line);

#define pdb_primitive_reference_dup(a) \
  pdb_primitive_reference_dup_loc(a, __FILE__, __LINE__)
void pdb_primitive_reference_dup_loc(pdb_primitive_reference *_pref,
                                     char const *_file, int _line);

/* pdb-primitive-summary.c */

bool pdb_primitive_summary_allows(pdb_primitive_summary const *_a,
                                  pdb_primitive_summary const *_b);

bool pdb_primitive_summary_contains(pdb_primitive_summary const *_a,
                                    pdb_primitive_summary const *_b);

char const *pdb_primitive_summary_to_string(pdb_handle *_pdb,
                                            pdb_primitive_summary const *_psum,
                                            char *_buf, size_t _size);

bool pdb_primitive_summary_match(pdb_handle *_pdb, pdb_primitive const *_pr,
                                 pdb_primitive_summary const *_psum);

void pdb_primitive_summary_normalize(pdb_primitive_summary const *_psum,
                                     pdb_primitive_summary *_out);

/* pdb-restore.c */

int pdb_restore_avoid_database_id(pdb_handle *_pdb, graph_guid const *_guid);

int pdb_restore_adopt_database_id(pdb_handle *_pdb, graph_guid const *_guid);

int pdb_restore_prepare(pdb_handle *_pdb, pdb_id _start);

/* pdb-runtime-statistics.c */

void pdb_runtime_statistics_max(pdb_runtime_statistics *);
int pdb_runtime_statistics_get(pdb_handle *_pdb,
                               pdb_runtime_statistics *_stat_out);

void pdb_runtime_statistics_diff(pdb_runtime_statistics const *_a,
                                 pdb_runtime_statistics const *_b,
                                 pdb_runtime_statistics *_c);

void pdb_runtime_statistics_add(pdb_runtime_statistics const *_a,
                                pdb_runtime_statistics const *_b,
                                pdb_runtime_statistics *_c);

bool pdb_runtime_statistics_exceeds(pdb_runtime_statistics const *_small,
                                    pdb_runtime_statistics const *_large,
                                    pdb_runtime_statistics *_report);

void pdb_runtime_statistics_limit_below(
    pdb_runtime_statistics const *_limit_below, pdb_runtime_statistics *_large);

/* pdb-rxs.c */

void pdb_rxs_set(pdb_handle *pdb, size_t depth);
size_t pdb_rxs_get(pdb_handle *pdb);
void pdb_rxs_push(pdb_handle *pdb, char const *fmt, ...)
#if __GNUC__
    __attribute__((format(printf, 2, 3)))
#endif
    ;
void pdb_rxs_pop(pdb_handle *pdb, char const *fmt, ...)
#if __GNUC__
    __attribute__((format(printf, 2, 3)))
#endif
    ;

void pdb_rxs_pop_test(pdb_handle *pdb, int err, pdb_budget cost,
                      char const *fmt, ...)
#if __GNUC__
    __attribute__((format(printf, 4, 5)))
#endif
    ;

void pdb_rxs_pop_id(pdb_handle *pdb, int err, pdb_id id, pdb_budget cost,
                    char const *fmt, ...)
#if __GNUC__
    __attribute__((format(printf, 5, 6)))
#endif
    ;

void pdb_rxs_log(pdb_handle *pdb, char const *fmt, ...)
#if __GNUC__
    __attribute__((format(printf, 2, 3)))
#endif
    ;

/* pdb-set-database-id.c */

int pdb_set_database_id(pdb_handle *, unsigned long long);

/* pdb-set-database-name.c */

int pdb_set_database_name(pdb_handle *, char const *);

/* pdb-set-mode.c */

int pdb_set_mode(pdb_handle *pdb, int mode);

/* pdb-set-predictable.c */

void pdb_set_predictable(pdb_handle *, bool);

/* pdb-set-sync.c */

void pdb_set_sync(pdb_handle *_pdb, bool);

/* pdb-set-path.c */

int pdb_set_path(pdb_handle *pdb, char const *path);

/* pdb-status.c */

typedef int pdb_status_callback(void *_application_data, char const *_name,
                                char const *_value);

int pdb_status(pdb_handle *_pdb, pdb_status_callback *_callback,
               void *_callback_data);

int pdb_status_tiles(pdb_handle *_pdb, pdb_status_callback *_callback,
                     void *_callback_data);

/* pdb-sync.c */

bool pdb_sync(pdb_handle *pdb);

bool pdb_transactional(pdb_handle *pdb);

/* pdb-refresh.c */

int pdb_refresh(pdb_handle *pdb);

/* pdb-truncate.c */

int pdb_truncate(pdb_handle *);

/* pdb-util.c */

int pdb_scan_ull(char const **_s_ptr, char const *_e,
                 unsigned long long *_ull_out);

int pdb_xx_encode(pdb_handle *_pdb, const char *_key, size_t _key_n,
                  cm_buffer *_buf);

int pdb_xx_decode(pdb_handle *_pdb, char const *_s, char const *_e,
                  cm_buffer *_buf);

/* pdb-word.c */

int pdb_hmap_value_normalize(pdb_handle *_pdb, char const *_s, char const *_e,
                             char const **_norm_s_out, char const **_norm_e_out,
                             char **_buf_out);
int pdb_word_number_split(char const *_s, char const *_e, char const **_pre_s,
                          char const **_point_s, char const **_post_s);
int pdb_word_number_normalize(cm_handle *_cm, char const *_s, char const *_e,
                              char **_norm_buf, char const **_norm_s_out,
                              char const **_norm_e_out);
bool pdb_word_fragment_next(char const *_s0, char const **_s, char const *_e,
                            char const **_word_s_out, char const **_word_e_out,
                            int *_word_type_out);

void pdb_word_key(unsigned long, char *);

bool pdb_word_has_prefix(pdb_handle *_pdb, char const *_prefix, char const *_s,
                         char const *_e);

int pdb_word_synchronize(pdb_handle *_pdb, pdb_id _id,
                         pdb_primitive const *_pr);

int pdb_word_add(pdb_handle *_pdb, pdb_id _id, char const *_s, char const *_e);

unsigned long pdb_word_hash(pdb_handle *_pdb, char const *_s, char const *_e);

int pdb_word_utf8len(pdb_handle *_pdb, char const *_s, char const *_e);

int pdb_iterator_word_create(pdb_handle *_pdb, char const *_s, char const *_e,
                             pdb_id _low, pdb_id _high, bool _forwards,
                             bool _error_if_null, pdb_iterator **_it_out);

bool pdb_word_has_prefix_hash(pdb_handle *_pdb,
                              unsigned long long const *_prefix, char const *_s,
                              char const *_e);

void pdb_word_has_prefix_hash_compile(pdb_handle *_pdb,
                                      unsigned long long *_prefix,
                                      char const *_s, char const *_e);

/* pdb-vip.c */

int pdb_vip_linkage_id_iterator(pdb_handle *_pdb, pdb_id _source, int _linkage,
                                graph_guid const *_qualifier, pdb_id _low,
                                pdb_id _high, bool _forward,
                                bool _error_if_null, pdb_iterator **_it_out,
                                bool *_true_vip_out);

int pdb_vip_linkage_iterator(pdb_handle *_pdb, graph_guid const *_node,
                             int _linkage, graph_guid const *_qualifier,
                             pdb_id _low, pdb_id _high, bool _forward,
                             bool _error_if_null, pdb_iterator **_it_out,
                             bool *_true_vip_out);

int pdb_vip_id_iterator(pdb_handle *_pdb, pdb_id _source, int _linkage,
                        graph_guid const *_qualifier, pdb_id _low, pdb_id _high,
                        bool _forward, bool _error_if_null,
                        pdb_iterator **_it_out);

int pdb_vip_linkage_id_count(pdb_handle *_pdb, pdb_id _node_id, int _linkage,
                             graph_guid const *_qualifier, pdb_id _low,
                             pdb_id _high, unsigned long long _upper_bound,
                             unsigned long long *_n_out);

int pdb_vip_linkage_guid_count(pdb_handle *_pdb, graph_guid const *_node_guid,
                               int _linkage, graph_guid const *_qualifier,
                               pdb_id _low, pdb_id _high,
                               unsigned long long _upper_bound,
                               unsigned long long *_n_out);

int pdb_vip_iterator(pdb_handle *_pdb, graph_guid const *_node, int _direction,
                     graph_guid const *_qualifier, pdb_id _low, pdb_id _high,
                     bool _forward, bool _error_if_null,
                     pdb_iterator **_it_out);

int pdb_vip_id_count(pdb_handle *_pdb, pdb_id _id, int _direction,
                     graph_guid const *_qualifier, pdb_id _low, pdb_id _high,
                     unsigned long long _upper_bound,
                     unsigned long long *_n_out);

int pdb_vip_synchronize(pdb_handle *_pdb, pdb_id _id, pdb_primitive const *_pr);

bool pdb_vip_is_endpoint_id(pdb_handle *_pdb, pdb_id _endpoint_id,
                            int _direction, graph_guid const *_qualifier);

int pdb_vip_add(pdb_handle *pdb, pdb_id endpoint_id, int linkage,
                pdb_id type_id, pdb_id link_id);

int pdb_vip_id(pdb_handle *_pdb, pdb_id _source, int _linkage, bool *_vip_out);

/* pdb-verify.c */

int pdb_verify_id(pdb_handle *pdb, pdb_id n, unsigned long *error_code);

int pdb_verify_range(pdb_handle *pdb, pdb_id low, pdb_id high, int *count);

int pdb_verify_render_error(char *output, size_t len, unsigned long error);

/* pdb-versioned.c */

int pdb_is_versioned(pdb_handle *pdb, pdb_id id, bool *r);

/* pdb-bins.c */

int pdb_bin_to_iterator(pdb_handle *pdb, int bin, pdb_id low, pdb_id high,
                        bool forward, bool error_if_null, pdb_iterator **it);

extern struct pdb_binset *pdb_binset_numbers_ptr;
extern struct pdb_binset *pdb_binset_strings_ptr;

#define PDB_BINSET_NUMBERS (pdb_binset_numbers_ptr)
#define PDB_BINSET_STRINGS (pdb_binset_strings_ptr)

int pdb_bin_lookup(pdb_handle *pdb, struct pdb_binset *binset, const void *s,
                   const void *e, bool *exact);

size_t pdb_bin_end(pdb_handle *pdb, struct pdb_binset *binset);

size_t pdb_bin_start(pdb_handle *pdb, struct pdb_binset *binset);

void pdb_bin_value(pdb_handle *pdb, struct pdb_binset *binset, int bin,
                   void **out_s);

#include "libpdb/pdb-primitive.h"

/* pdb-strerror.c */

char const *pdb_xstrerror(int);
char const *pdb_strerror(int);

#endif /* PDB_H */
