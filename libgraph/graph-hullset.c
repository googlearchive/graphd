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

/*
 *  Approximately store a set of indices.
 *
 *  The strength of the hull set is to make trade-offs when storing
 *  large sets of numbers.  If the set gets too complex, the hull
 *  adds indices that aren't actually in the set to simplify its
 *  representation and save space.
 *
 *  E.g., instead of  storing
 *
 *        +---+     +---+     +---+
 * 	  | 1 |---->| 3 |---->| 5 |
 *        +---+     +---+     +---+
 *
 *  the hull might decide to store
 *
 *        +-----------------------+
 * 	  | 1    . . . . . . .  5 |
 *        +-----------------------+
 *
 *  which can be represented more efficiently.
 *
 *  A number stored in a hull comes out again exactly once.
 *  (If the same number is added twice, it comes out only once.)
 *
 *  A number that wasn't stored in it may or may not come out.
 *  (I.e., the set may hallucinate indices that weren't actually stored.)
 *
 *  When iterating, numbers come out in ascending order.
 *
 *  When adding, indices tend to be added in order, although there
 *  can be multiple "waves" of such additions.
 *
 *  The stored numbers lie between 0 and (1 << 34) - 2.
 */

/**
 * @brief One slot in the hull set.
 *  Carries 2*64 bits; the real accesses happen via macros.
 */
typedef struct graph_hullslot {
  unsigned long long hs_a;
  unsigned long long hs_b;

} graph_hullslot;

#define NBITS(n) ((1ull << (n)) - 1ull)
#define SUBST_BITS(v, n, off, rep)    \
  ((v) = ((v) & ~(NBITS(n) << off)) | \
         (((unsigned long long)(rep)&NBITS(n)) << (off)))
#define VALUE_MAX (NBITS(34) - 1ull)

/*  Slot indices run from 0 .. 32k-1.  The top 6 bits select a table;
 *  the bottom 9 bits are an offset into the table.
 */
#define index_to_table_index(x) (((x) >> 9) & NBITS(6))
#define index_to_slot_index(x) ((x)&NBITS(9))
#define index_to_slot(hull, x) \
  ((hull)->hull_table[index_to_table_index(x)][index_to_slot_index(x)])

/*  The  SPAN of a slot is the start of a range of numbers that it stands
 *  for.  So, if slot #5 encods the numbers  72, 73, 74, its slot_span_start
 *  is 72.
 */
#define slot_span_start(sl) ((sl).hs_a >> 30)
#define slot_span_start_set(sl, v) SUBST_BITS((sl).hs_a, 34, 30, (v))

/*  When iterating, slots are walked along a doubly linked chain.
 *  slot_prev() is the previous, slot_next() the next slot in the list.
 *  The first slot is always #0, the very last slot always #1; all other
 *  slots have both a prev and a next pointer, unless they're in the
 *  free list.
 */
#define slot_prev(sl) (((sl).hs_a >> 15) & NBITS(15))
#define slot_prev_set(sl, p) SUBST_BITS((sl).hs_a, 15, 15, (p))

#define slot_next(sl) ((sl).hs_a & NBITS(15))
#define slot_next_set(sl, n) SUBST_BITS((sl).hs_a, 15, 0, (n))

#define slot_span_set(sl, v, p, n) \
  ((sl).hs_a = (((v) << 30) | ((p) << 15) | (n)))

/*  The SKIP of a slot marks the end of the span - the place where
 *  the occupied range ends and the emptiness starts.
 *
 *  Slots are chained into doubly-linked chains per log2 of their
 *  skip width (the distance between slot_skip_start() and the
 *  slot_span_start() of their successor as per slot_next().)
 *
 *  So, slots with skip=1 are chained together, slots with skip=2 and 3
 *  are chained together and so on.  This is in order to, for very
 *  large data sets, find smallest gaps to dissolve to make room
 *  for more information with constant storage in constant time
 *  per call.  These skip-width related chains are connected via
 *  the slot_skip_prev() and slot_skip_next() pointers.
 */
#define slot_skip_start(sl) ((sl).hs_b >> 30)
#define slot_skip_start_set(sl, v) SUBST_BITS((sl).hs_b, 34, 30, (v))

#define slot_skip_prev(sl) (((sl).hs_b >> 15) & NBITS(15))
#define slot_skip_prev_set(sl, p) SUBST_BITS((sl).hs_b, 15, 15, (p))

#define slot_skip_next(sl) ((sl).hs_b & NBITS(15))
#define slot_skip_next_set(sl, n) SUBST_BITS((sl).hs_b, 15, 0, (n))

#define slot_skip_set(sl, v, p, n) \
  ((sl).hs_b = (((v) << 30) | ((p) << 15) | (n)))

#define SLOT_I_NULL NBITS(15)
#define SLOT_I_IS_NULL(x) ((x) == SLOT_I_NULL)
#define SLOT_I_SET_NULL(x) ((x) = SLOT_I_NULL)

#define SLOTS_PER_TABLE (512)
#define TABLES_PER_HULL (64)

/**
 * @brief Two-level lossy table of an index hull set.
 */
struct graph_hullset {
  /**
   * @brief allocate through this handler.
   */
  cm_handle *hull_cm;

  /**
   * @brief log through this handler.
   */
  cl_handle *hull_cl;

  /**
   * @brief TABLES_PER_HULL tables of SLOTS_PER_TABLE slots each.
   *
   *  The two-dimensional table is used like a variable-length
   *  table with a fixed limit, accessed via macros.
   */
  graph_hullslot *hull_table[TABLES_PER_HULL];

  /**
   * @brief Number of tables allocated.
   */
  size_t hull_table_n;

  /**
   * @brief Index of the first unused slot in the highest used table.
   */
  size_t hull_slot_n;

  /**
   * @brief Index of the most recently used slot.
   */
  size_t hull_slot_recent;

  /**
   * @brief How many slots, at most, will we allocate?
   */
  size_t hull_slot_max;

  /**
   * @brief First free (recycled) slot.
   */
  size_t hull_slot_free_head;

  /**
   * @brief Bins by skip size.
   *
   *  Each occupied slot is linked into a chain of slots
   *  whose skip sizes have the same order of magnitude
   * (n with skip < 2^n).  These are the heads of those
   *  chains: the bins into which occupied slots are sorted.
   */
  unsigned long long hull_skip[35];

  /**
   * @brief Current iterator position
   */
  graph_hullset_iterator hull_iterator;
};

#if 0
static void graph_hullset_dump(
	cl_handle		* cl,
	graph_hullset const	* hull)
{
	size_t	i;
	if (hull == NULL)
	{
		cl_log(cl, CL_LEVEL_SPEW, "null");
		return;
	}

	for (i = 0; i < hull->hull_slot_n; i++)
	{
		graph_hullslot	* s;
		s = &index_to_slot(hull, i);

		cl_log(cl, CL_LEVEL_SPEW,
			"[%d] span=%llu skip=%llu   slot chain: %d <-> %d   skip chain: %d <-> %d",
			(int)i,
			(unsigned long long)slot_span_start(*s) ,
			(unsigned long long)slot_skip_start(*s) ,

			(int)slot_prev(*s),
			(int)slot_next(*s),

			(int)slot_skip_prev(*s),
			(int)slot_skip_next(*s));
	}
}
#endif

/**
 * @brief Display hullset contents for debugging
 *
 * @param hull 	Hullset to display
 * @param buf	Use bytes pointed to by buf, if needed
 * @param size	Number of bytes pointed to by buf.
 *
 * @return a human-readable string representation of the hullset,
 *	abbreviated if the space provided didn't suffice.
 */
char const *graph_hullset_to_string(graph_hullset const *hull, char *buf,
                                    size_t size) {
  size_t i;
  char *w = buf, *e = buf + size;

  if (hull == NULL) return "null";
  if (buf == NULL || size == 0) return "hullset";

  *w = '\0';
  for (i = 0; i < hull->hull_slot_n; i++) {
    graph_hullslot *s;
    s = &index_to_slot(hull, i);

    if (e - w <= 42) {
      if (e - w >= 4) {
        *w++ = '.';
        *w++ = '.';
        *w++ = '.';
      }
      *w = '\0';
      return buf;
    }

    if (slot_span_start(*s) == slot_skip_start(*s)) continue;

    if (w != buf) *w++ = ',';

    if (slot_span_start(*s) == slot_skip_start(*s) - 1) {
      snprintf(w, (e - w), "%llu", (unsigned long long)slot_span_start(*s));
    } else {
      snprintf(w, (e - w), "%llu-%llu", (unsigned long long)slot_span_start(*s),
               (unsigned long long)slot_skip_start(*s) - 1);
    }
    w += strlen(w);
  }

  if (w == buf) return "empty";
  return buf;
}

/**
 * @brief Utility: what is the largest x with n < (1 << x) ?
 *
 *  Picture a big pachinko machine.
 *
 * @param n a number between 0 and (1 << 34) - 1, inclusive.
 * @return the largest x with n < (1 << x).
 */
static unsigned int skip_to_bin(unsigned long long n) {
#define x(a, b, c) (n < (1ull << (b)) ? (a) : (c))

  return x(x(x(x(x(0, 0, 1), 1, x(2, 2, 3)), 3, x(x(4, 4, 5), 5, x(6, 6, 7))),
             7, x(x(x(8, 8, 9), 9, x(10, 10, 11)), 11,
                  x(x(12, 12, 13), 13, x(14, 14, 15)))),
           15, x(x(x(x(x(16, 16, 17), 17, x(18, 18, 19)), 19,
                     x(x(20, 20, 21), 21, x(22, 22, 23))),
                   23, x(x(x(24, 24, 25), 25, x(26, 26, 27)), 27,
                         x(x(28, 28, 29), 29, x(30, 30, 31)))),
                 31, x(x(32, 32, 33), 33, 34)));

#undef x
}

/**
 * @brief Allocate a new table.
 *
 * @param hull 		hull set we're allocating for
 * @return 0 on success, a nonzero error number on failure.
 */
static int hull_table_alloc(graph_hullset *hull) {
  graph_hullslot **s;

  if (hull->hull_table_n >=
      sizeof(hull->hull_table) / sizeof(*hull->hull_table)) {
    cl_log(hull->hull_cl, CL_LEVEL_FAIL,
           "hull_table_alloc: full already (%lu tables); failing",
           (unsigned long)hull->hull_table_n);
    cl_cover(hull->hull_cl);

    return GRAPH_ERR_NO;
  }

  s = hull->hull_table + hull->hull_table_n;

  *s = cm_talloc(hull->hull_cm, graph_hullslot, SLOTS_PER_TABLE);
  if (*s == NULL) {
    cl_log(hull->hull_cl, CL_LEVEL_FAIL,
           "hull_table_alloc: failed to allocate %lu slots",
           (unsigned long)SLOTS_PER_TABLE);

    return ENOMEM;
  }

  cl_log(hull->hull_cl, CL_LEVEL_DEBUG, "hull_table_alloc: allocated %p (#%lu)",
         (void *)*s, (unsigned long)hull->hull_table_n);

  hull->hull_table_n++;
  cl_cover(hull->hull_cl);

  return 0;
}

/**
 * @brief Where does <val> go?
 *
 * @param hull hullset the slot is in
 * @param hit NULL or an iterator that might apply
 * @param val  value between 0 and (1 << 34) - 2, inclusive.
 *
 * @return index of the slot that contains <val>
 *	(either in its allocated area or in its skipped area.)
 */
static size_t slot_in_or_after(graph_hullset *hull, graph_hullset_iterator *hit,
                               unsigned long long val) {
  size_t i, i_prev;
  graph_hullslot *s;

  if (val >= NBITS(34))
    cl_notreached(hull->hull_cl,
                  "slot_in_or_after: value %llu (hex %llx) out of range\n", val,
                  val);

  cl_assert(hull->hull_cl, val < NBITS(34));
  cl_assert(hull->hull_cl, hull->hull_slot_n >= 2);

  if (hull->hull_slot_n == 2) return 0;

  /*  Usually, numbers are added at the end.
   *  Let's try the last slot first.
   */
  i = slot_prev(index_to_slot(hull, 1));
  s = &index_to_slot(hull, i);

  cl_assert(hull->hull_cl, slot_next(*s) != SLOT_I_NULL);
  cl_assert(hull->hull_cl, slot_next(*s) < hull->hull_slot_n);

  if (val >= slot_span_start(*s) &&
      val < slot_span_start(index_to_slot(hull, slot_next(*s)))) {
    cl_cover(hull->hull_cl);
    return i;
  }

  /*  If the caller supplied an iterator, try the iterator position.
   */
  if (hit != NULL && (i = hit->hit_slot) != SLOT_I_NULL) {
    s = &index_to_slot(hull, i);
    if (slot_next(*s) != SLOT_I_NULL) {
      if (val >= slot_span_start(*s) &&
          val < slot_span_start(index_to_slot(hull, slot_next(*s)))) {
        cl_cover(hull->hull_cl);
        return i;
      }
    }
  }

  /*  Start with the most recent addition; if that one's too
   *  large, start at 0.
   */
  i = hull->hull_slot_recent;
  if (i >= hull->hull_slot_n || i == hull->hull_slot_n - 1 ||
      val < slot_span_start(index_to_slot(hull, i))) {
    cl_cover(hull->hull_cl);
    i = 0;
  }

  s = &index_to_slot(hull, i);
  do {
    i_prev = i;
    i = slot_next(*s);

    cl_assert(hull->hull_cl, i != SLOT_I_NULL);
    cl_assert(hull->hull_cl, i < hull->hull_slot_n);

    s = &index_to_slot(hull, i);

    /*
                    cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
                            "slot_in_or_after: looking at val %llu, i=%lu, start
       %llu, prev %d, next %d, skip %llu, prev %d, next %d",
                            val,
                            (unsigned long)i,
                            (unsigned long long)slot_span_start(*s),
                            (int)slot_prev(*s),
                            (int)slot_next(*s),
                            (unsigned long long)slot_skip_start(*s),
                            (int)slot_skip_prev(*s),
                            (int)slot_skip_next(*s));
    */

  } while (val >= slot_span_start(*s));

  cl_cover(hull->hull_cl);
  return i_prev;
}

static void slot_skip_chain_out(graph_hullset *hull, size_t i) {
  size_t i_prev, i_next, bin;
  graph_hullslot *s;

  /*  Sentinel slots 0 and 1 aren't chained in.
   */
  if (i <= 1) {
    cl_cover(hull->hull_cl);
    return;
  }

  s = &index_to_slot(hull, i);
  cl_assert(hull->hull_cl, slot_next(*s) != SLOT_I_NULL);
  cl_assert(hull->hull_cl, slot_prev(*s) != SLOT_I_NULL);
  /*
          cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
                  "slot_skip_chain_out: chain out s %lu, start %llu, prev %d,
     next %d, skip %llu, prev %d, next %d",
                          (unsigned long)i,
                          (unsigned long long)slot_span_start(*s),
                          (int)slot_prev(*s),
                          (int)slot_next(*s),
                          (unsigned long long)slot_skip_start(*s),
                          (int)slot_skip_prev(*s),
                          (int)slot_skip_next(*s));
  */

  i_prev = slot_skip_prev(*s);
  slot_skip_prev_set(*s, SLOT_I_NULL);

  i_next = slot_skip_next(*s);
  slot_skip_next_set(*s, SLOT_I_NULL);

  if (i_prev != SLOT_I_NULL) {
    s = &index_to_slot(hull, i_prev);
    slot_skip_next_set(*s, i_next);
    cl_cover(hull->hull_cl);
  } else {
    graph_hullslot *s_next;
    unsigned long long skip;

    s_next = &index_to_slot(hull, slot_next(*s));
    skip = slot_span_start(*s_next) - slot_skip_start(*s);
    bin = skip_to_bin(skip);

    cl_assert(hull->hull_cl, bin >= 0);
    cl_assert(hull->hull_cl,
              bin < sizeof(hull->hull_skip) / sizeof(*hull->hull_skip));

    cl_assert(hull->hull_cl, hull->hull_skip[bin] == i);
    hull->hull_skip[bin] = i_next;
    cl_cover(hull->hull_cl);
  }

  if (i_next != SLOT_I_NULL) {
    s = &index_to_slot(hull, i_next);
    slot_skip_prev_set(*s, i_prev);
    cl_cover(hull->hull_cl);
  }
}

static void slot_skip_chain_in(graph_hullset *hull, size_t i) {
  size_t bin;
  unsigned long long skip;
  graph_hullslot *s, *s_next;
  size_t i_head;

  /*  Sentinel slots 0 and 1 aren't chained in.
   */
  if (i <= 1) return;

  s = &index_to_slot(hull, i);
  cl_assert(hull->hull_cl, slot_next(*s) != SLOT_I_NULL);
  cl_assert(hull->hull_cl, slot_prev(*s) != SLOT_I_NULL);

  s_next = &index_to_slot(hull, slot_next(*s));
  skip = slot_span_start(*s_next) - slot_skip_start(*s);
  bin = skip_to_bin(skip);
  cl_assert(hull->hull_cl, bin <= 34);

  /*
          cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
                  "slot_skip_chain_in i=%lu, bin=%lu, old head=%lu",
                  (unsigned long)i,
                  (unsigned long)bin,
                  (unsigned long)hull->hull_skip[bin]);
  */

  /*  if there is a bin head, head.prev := <i>.
   */
  i_head = hull->hull_skip[bin];
  if (i_head != SLOT_I_NULL) {
    slot_skip_prev_set(index_to_slot(hull, i_head), i);
    cl_cover(hull->hull_cl);
  }
  slot_skip_next_set(*s, i_head);

  /* binhead := <i>.
   */
  slot_skip_prev_set(*s, SLOT_I_NULL);
  hull->hull_skip[bin] = i;
}

static void slot_chain_out(graph_hullset *hull, size_t i) {
  size_t i_prev, i_next;
  graph_hullslot *s;

  slot_skip_chain_out(hull, i);

  s = &index_to_slot(hull, i);

  i_prev = slot_prev(*s);
  slot_prev_set(*s, SLOT_I_NULL);

  i_next = slot_next(*s);
  slot_next_set(*s, SLOT_I_NULL);

  if (i_prev != SLOT_I_NULL) {
    s = &index_to_slot(hull, i_prev);

    if (i_prev != 0) {
      slot_skip_chain_out(hull, i_prev);
      cl_cover(hull->hull_cl);
    }

    slot_next_set(*s, i_next);

    if (i_prev != 0) {
      slot_skip_chain_in(hull, i_prev);
      cl_cover(hull->hull_cl);
    }
  }
  if (i_next != SLOT_I_NULL) {
    s = &index_to_slot(hull, i_next);
    slot_prev_set(*s, i_prev);

    cl_cover(hull->hull_cl);
  }
}

static void slot_chain_in(graph_hullset *hull, size_t i_prev, size_t i,
                          size_t i_next) {
  graph_hullslot *s;

  if (i_prev != SLOT_I_NULL) {
    s = &index_to_slot(hull, i_prev);

    if (i_prev != 0) {
      slot_skip_chain_out(hull, i_prev);
      cl_cover(hull->hull_cl);
    }

    slot_next_set(*s, i);

    if (i_prev != 0) {
      slot_skip_chain_in(hull, i_prev);
      cl_cover(hull->hull_cl);
    }

    cl_cover(hull->hull_cl);
  }
  if (i_next != SLOT_I_NULL) {
    s = &index_to_slot(hull, i_next);
    slot_prev_set(*s, i);

    cl_cover(hull->hull_cl);
  }

  s = &index_to_slot(hull, i);
  slot_prev_set(*s, i_prev);
  slot_next_set(*s, i_next);

  slot_skip_chain_in(hull, i);
}

static void slot_free_chain_in(graph_hullset *hull, size_t i) {
  graph_hullslot *s;

  s = &index_to_slot(hull, i);
  slot_span_start_set(*s, hull->hull_slot_free_head);
  hull->hull_slot_free_head = i;

  cl_cover(hull->hull_cl);
}

static size_t slot_alloc(graph_hullset *hull, size_t distance) {
  graph_hullslot *s, *s_next;
  size_t i, bin_max, i_prev;

  cl_assert(hull->hull_cl, distance > 0);

  /* If we have a freelist, reuse it.
   */
  if ((i = hull->hull_slot_free_head) != SLOT_I_NULL) {
    s = &index_to_slot(hull, hull->hull_slot_free_head);
    hull->hull_slot_free_head = slot_span_start(*s);
    cl_cover(hull->hull_cl);

    return i;
  }

  /* If we can allocate more records, allocate them.
   */
  if (hull->hull_slot_n < hull->hull_slot_max &&
      ((hull->hull_slot_n % SLOTS_PER_TABLE) != 0 ||
       hull_table_alloc(hull) == 0)) {
    return hull->hull_slot_n++;
  }

  bin_max = skip_to_bin(distance);

  /*  Sacrifice the smallest gap.
   */
  for (i = 0; i < bin_max; i++)
    if (hull->hull_skip[i] != SLOT_I_NULL) {
      cl_cover(hull->hull_cl);
      break;
    }

  /*  The gap we could sacrifice is larger than the gap that
   *  the caller holds.  Let the caller use their own gap.
   */
  if (i >= bin_max || hull->hull_skip[i] == SLOT_I_NULL) {
    cl_cover(hull->hull_cl);
    return SLOT_I_NULL;
  }

  i = hull->hull_skip[i];

  /* Pull in the record following this one to start earlier.
   */
  s = &index_to_slot(hull, i);
  s_next = &index_to_slot(hull, slot_next(*s));
  i_prev = slot_prev(*s);

  slot_chain_out(hull, i);

  slot_skip_chain_out(hull, i_prev);
  slot_span_start_set(*s_next, slot_span_start(*s));
  slot_skip_chain_in(hull, i_prev);

  cl_cover(hull->hull_cl);

  return i;
}

/**
 * @brief Create a new hullset.
 *
 * @param cm     allocate via this
 * @param cl     log via this
 * @param n	 number of records (including the two boundary sentinels)
 *		 to limit the hullset storage to.  Minimum 3, maximum
 *		 32k.  Minimum and maximum are enforced by overriding
 *		 user-specified limits if necessary.
 * @return  NULL on allocation failure, otherwise a pointer that must
 *	be free'd with graph_hullset_destroy().
 */
graph_hullset *graph_hullset_create(struct cm_handle *cm, struct cl_handle *cl,
                                    size_t n) {
  graph_hullset *hull;
  graph_hullslot *s;
  size_t i;

  hull = cm_talloc(cm, graph_hullset, 1);
  if (hull == NULL) return NULL;

  memset(hull, 0, sizeof(*hull));
  hull->hull_cm = cm;
  hull->hull_cl = cl;

  hull->hull_table[0] = cm_talloc(cm, graph_hullslot, SLOTS_PER_TABLE);
  if (hull->hull_table[0] == NULL) {
    cm_free(cm, hull);
    return NULL;
  }
  hull->hull_table_n = 1;

  /*  Create the boundaries:
   *  [0], the beginning.
   *  [1], the end
   *  Both boundary slots cannot be dissolved and hence don't
   *  show up in the skip list; their skip prev- and next indices
   *  are SLOT_I_NULL.
   *
   *   +---+           +-----------+
   *   | 0 |---------> | 1<<34 - 1 |
   *   +---+           +-----------+
   */

  s = hull->hull_table[0];
  slot_span_set(*s, 0, SLOT_I_NULL, 1);
  slot_skip_set(*s, 0, SLOT_I_NULL, SLOT_I_NULL);

  s++;
  slot_span_set(*s, NBITS(34), 0, SLOT_I_NULL);
  slot_skip_set(*s, NBITS(34), SLOT_I_NULL, SLOT_I_NULL);

  n += 2;
  if (n > SLOTS_PER_TABLE * TABLES_PER_HULL) {
    n = SLOTS_PER_TABLE * TABLES_PER_HULL;
    cl_cover(cl);
  }

  hull->hull_slot_max = n;
  hull->hull_slot_n = 2;
  hull->hull_slot_free_head = SLOT_I_NULL;

  for (i = 0; i < sizeof(hull->hull_skip) / sizeof(*hull->hull_skip); i++)
    hull->hull_skip[i] = SLOT_I_NULL;

  cl_cover(cl);
  cl_log(cl, CL_LEVEL_DEBUG, "graph_hullset_create %p; max %lu", (void *)hull,
         (unsigned long)hull->hull_slot_max);

  return hull;
}

/**
 * @brief Free a hull set previously allocated with graph_hullset_create().
 *
 *  It is safe to call this function with a NULL pointer;
 *  in that case, it does nothing.
 *
 * @param hull 		NULL or a valid hullset.
 */
void graph_hullset_destroy(graph_hullset *hull) {
  size_t i;
  graph_hullslot **sl;

  if (hull == NULL) return;

  cl_log(hull->hull_cl, CL_LEVEL_DEBUG, "graphd_hullset_destroy %p",
         (void *)hull);

  /* Free the allocated tables. */
  for (i = 0, sl = hull->hull_table; i < hull->hull_table_n; i++, sl++) {
    cl_cover(hull->hull_cl);
    cm_free(hull->hull_cm, *sl);
  }

  /* Free the overall hull structure. */
  cm_free(hull->hull_cm, hull);
}

/**
 * @brief Add a range of indices to a hullset.
 *
 * @param hull 	hullset to add to.
 * @param start first index to add.
 * @param end 	index just after the last to add.
 */
void graph_hullset_add_range(graph_hullset *hull, unsigned long long start,
                             unsigned long long end) {
  cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
         "graph_hullset_add_range %p %llu .. %llu", (void *)hull, start, end);

  cl_assert(hull->hull_cl, start <= VALUE_MAX);
  cl_assert(hull->hull_cl, end <= VALUE_MAX);

  while (start < end) {
    cl_cover(hull->hull_cl);
    graph_hullset_add(hull, start++);
  }
}

/**
 * @brief Add a hullset of indices to a hullset.
 *
 * @param a hullset to add to.
 * @param b hullset to add
 */
void graph_hullset_add_hullset(graph_hullset *a, graph_hullset *b) {
  unsigned long long start = 0;
  unsigned long long end = 0;
  graph_hullset_iterator hit;

  graph_hullset_iterator_initialize(&hit);
  while (graph_hullset_iterator_next_range(b, &hit, &start, &end)) {
    cl_cover(a->hull_cl);
    graph_hullset_add_range(a, start, end);
  }
}

/**
 * @brief Add an index to a hullset.
 *
 *  Usage pattern: indices are typically added in waves;
 *  within each wave, indices tend to increase.
 *  E.g., 2 3 6 8 / 1 4 5 9 /  2 5 7 16
 *  Indices may not be unique (i.e. the candidate may already exist.)
 *
 * @param hull 	hullset to add to.
 * @param val 	index to add.
 */
void graph_hullset_add(graph_hullset *hull, unsigned long long val) {
  size_t i, i_next;
  graph_hullslot *s, *s_next;

  if (val > VALUE_MAX)
    cl_notreached(hull->hull_cl,
                  "graph_hullset_add: value %llu is "
                  "out of range for hullset values",
                  (unsigned long long)val);

  cl_log(hull->hull_cl, CL_LEVEL_DEBUG, "graph_hullset_add %p %llu",
         (void *)hull, val);

  i = slot_in_or_after(hull, NULL, val);
  s = &index_to_slot(hull, i);

  /* Already included? */
  if (val < slot_skip_start(*s)) {
    /*
                    cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
                            "graph_hullset_add: already included below start "
                            "%llu of containing slot %lu",
                            slot_skip_start(*s), (unsigned long)i);
    */
    cl_cover(hull->hull_cl);
    return;
  }

  i_next = slot_next(*s);
  s_next = &index_to_slot(hull, i_next);

  /* Directly adjacent? */
  if (val == slot_skip_start(*s)) {
    size_t new_skip_start;

    cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
           "graph_hullset_add: value is head of skipped area");

    if (i_next != 1 && val + 1 == slot_span_start(*s_next)) {
      /*
                              cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
                                      "graph_hullset_add: value fills gap");
      */

      /*  We filled up the gap between two adjacent slots.
       *  Remove the second one.  Extend the first slot
       *  to include the area covered by the second.
       *
       *  This is the only way anything ever gets freed.
       */
      new_skip_start = slot_skip_start(*s_next);

      /*
                              cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
                                      "graph_hullset_add: removing s_next %lu, "
                                      "start %llu, prev %d, next %d, skip %llu,
         "
                                      "prev %d, next %d",
                                      (unsigned long)i_next,
                                      (unsigned long
         long)slot_span_start(*s_next),
                                      (int)slot_prev(*s_next),
                                      (int)slot_next(*s_next),
                                      (unsigned long
         long)slot_skip_start(*s_next),
                                      (int)slot_skip_prev(*s_next),
                                      (int)slot_skip_next(*s_next));
      */
      if (hull->hull_slot_recent == i_next) {
        cl_cover(hull->hull_cl);
        hull->hull_slot_recent = i;
      }

      slot_chain_out(hull, i_next);
      slot_free_chain_in(hull, i_next);
      cl_cover(hull->hull_cl);
    } else {
      cl_cover(hull->hull_cl);
      new_skip_start = val + 1;
    }

    slot_skip_chain_out(hull, i);
    slot_skip_start_set(*s, new_skip_start);
    slot_skip_chain_in(hull, i);
  } else if (val + 1 == slot_span_start(*s_next)) {
    /*
                    cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
                            "graph_hullset_add: value is at end of skipped
       area");
    */
    cl_cover(hull->hull_cl);

    slot_skip_chain_out(hull, i);
    slot_span_start_set(*s_next, val);
    slot_skip_chain_in(hull, i);
  } else {
    unsigned long long before, after, best;
    size_t i_new;

    /*
                    cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
                            "graph_hullset_add: value is independent");
    */

    before = val - slot_skip_start(*s);
    after = slot_span_start(*s_next) - (val + 1);
    best = (before <= after) ? before : after;

    /*
                    cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
                            "graph_hullset_add: before %llu, after %llu",
                             before, after);
    */

    /* Add a slot between this and the next. */

    if ((i_new = slot_alloc(hull, best)) == SLOT_I_NULL) {
      cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
             "graph_hullset_add: no new slot available; "
             "expanding %lu",
             (unsigned long)i);

      slot_skip_chain_out(hull, i);

      if (before <= after)
        slot_skip_start_set(*s, val + 1);
      else
        slot_span_start_set(*s_next, val - 1);

      slot_skip_chain_in(hull, i);
      cl_cover(hull->hull_cl);
    } else {
      graph_hullslot *s_new;
      /*
                              cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
                                      "graph_hullset_add: got new slot %lu",
                                      (unsigned long)i_new);
      */
      s_new = &index_to_slot(hull, i_new);

      slot_span_set(*s_new, val, SLOT_I_NULL, SLOT_I_NULL);
      slot_skip_set(*s_new, val + 1, SLOT_I_NULL, SLOT_I_NULL);

      slot_chain_in(hull, i, i_new, slot_next(*s));
      cl_cover(hull->hull_cl);
    }
  }

  /*
  cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
          "graph_hullset_add %p %llu: done:", (void *)hull, val);
  graph_hullset_dump(hull->hull_cl, hull);
  */
}

/**
 * @brief Initialize a hullset iterator.
 *
 * @param pos_inout 	iterator to initialize
 */
void graph_hullset_iterator_initialize(graph_hullset_iterator *pos_inout) {
  memset(pos_inout, 0, sizeof(*pos_inout));
}

/**
 * @brief Get an index from a hullset, keep state externally.
 *
 * @param hull 	hullset to iterate over
 * @param hit 	iterator to position
 * @param i_out out: the next index.
 *
 * @return 0 if there's an index left, GRAPH_ERR_NO if we run out.
 */
int graph_hullset_iterator_next(graph_hullset const *hull,
                                graph_hullset_iterator *hit,
                                unsigned long long *i_out) {
  /*
          cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
                  "graph_hullset_iterator_next(%p): slot %lu, offset %llu",
                  (void *)hull,
                  (unsigned long)hit->hit_slot,
                  (unsigned long long)hit->hit_offset);
  */

  while (hit->hit_slot < hull->hull_slot_n && hit->hit_slot != SLOT_I_NULL) {
    graph_hullslot const *s;

    s = &index_to_slot(hull, hit->hit_slot);

    /*
            cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
            "graph_hullset_iterator_next: looking at slot "
            "%lu, start %llu, prev %d, next %d, skip %llu, prev %d, next %d",
                            (unsigned long)hit->hit_slot,
                            (unsigned long long)slot_span_start(*s),
                            (int)slot_prev(*s),
                            (int)slot_next(*s),
                            (unsigned long long)slot_skip_start(*s),
                            (int)slot_skip_prev(*s),
                            (int)slot_skip_next(*s));
    */

    *i_out = slot_span_start(*s) + hit->hit_offset;
    if (*i_out < slot_skip_start(*s)) {
      cl_cover(hull->hull_cl);
      cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
             "graph_hullset_iterator_next: return %llu",
             (unsigned long long)*i_out);
      hit->hit_offset++;

      return 0;
    }
    hit->hit_offset = 0;
    hit->hit_slot = slot_next(*s);

    cl_cover(hull->hull_cl);
  }
  cl_cover(hull->hull_cl);
  return GRAPH_ERR_NO;
}

/**
 * @brief Get a range of indices from a hullset.
 *
 * @param hull 	hullset to iterate over
 * @param hit	iterator to position
 * @param start_out out: the next index.
 * @param end_out out: the first index just after the last.
 *
 * @return 0 if there's anything left, GRAPH_ERR_NO if we run out.
 */
int graph_hullset_iterator_next_range(graph_hullset const *hull,
                                      graph_hullset_iterator *hit,
                                      unsigned long long *start_out,
                                      unsigned long long *end_out) {
  cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
         "graph_hullset_iterator_next_range(%p): slot %lu, offset %llu",
         (void *)hull, (unsigned long)hit->hit_slot,
         (unsigned long long)hit->hit_offset);

  while (hit->hit_slot < hull->hull_slot_n && hit->hit_slot != SLOT_I_NULL) {
    graph_hullslot const *s;

    s = &index_to_slot(hull, hit->hit_slot);
    hit->hit_slot = slot_next(*s);

    *start_out = slot_span_start(*s) + hit->hit_offset;
    hit->hit_offset = 0;

    *end_out = slot_skip_start(*s);

    if (*start_out < *end_out) {
      cl_cover(hull->hull_cl);
      return 0;
    }
    cl_cover(hull->hull_cl);
  }
  cl_cover(hull->hull_cl);
  return GRAPH_ERR_NO;
}

/**
 * @brief Get an index from a hullset.
 *
 * @param hull 	hullset to iterate over
 * @param i_out out: the next index.
 *
 * @return 0 if there's an index left, GRAPH_ERR_NO if we run out.
 */
int graph_hullset_next(graph_hullset *hull, unsigned long long *i_out) {
  return graph_hullset_iterator_next(hull, &hull->hull_iterator, i_out);
}

/**
 * @brief Position a hullset on or after an index.
 *
 * @param hull 	hullset to iterate over
 * @param i_in_out out: the next index.
 * @param changed_out: set to true if the call changed *i_in_out,
 *	false otherwise.
 *
 * @return 0 if there's an index left, GRAPH_ERR_NO if we run out.
 */
int graph_hullset_find(graph_hullset *hull, unsigned long long *i_in_out,
                       bool *changed_out) {
  return graph_hullset_iterator_find(hull, &hull->hull_iterator, i_in_out,
                                     changed_out);
}

/**
 * @brief Get a range of indices from a hullset.
 *
 * @param hull 	hullset to iterate over
 * @param start_out out: the next index.
 * @param end_out out: the first index just after the last.
 *
 * @return 0 if there's anything left, GRAPH_ERR_NO if we run out.
 */
int graph_hullset_next_range(graph_hullset *hull, unsigned long long *start_out,
                             unsigned long long *end_out) {
  return graph_hullset_iterator_next_range(hull, &hull->hull_iterator,
                                           start_out, end_out);
}

/**
 * @brief Position on a number
 *
 *  The next call to graph_hullset_next() will return the number, or
 *  the first larger number after that that's part of the set.
 *
 * @param hull 	hullset to position in
 * @param hit 	iterator to position
 * @param val 	number to go to
 *
 * @return 0 if the number is in the set, GRAPH_ERR_NO if not
 */
int graph_hullset_iterator_seek_to(graph_hullset *hull,
                                   graph_hullset_iterator *hit,
                                   unsigned long long val) {
  size_t i;
  graph_hullslot const *s;

  cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
         "graph_hullset_iterator_seek_to %p, %llu", (void *)hull, val);

  i = slot_in_or_after(hull, hit, val);
  hit->hit_slot = i;

  s = &index_to_slot(hull, hit->hit_slot);
  hit->hit_offset = val - slot_span_start(*s);

  if (val > slot_skip_start(*s)) {
    cl_cover(hull->hull_cl);
    return GRAPH_ERR_NO;
  }
  cl_cover(hull->hull_cl);
  return 0;
}

/**
 * @brief Position on a number
 *
 *  The next call to graph_hullset_next() will return the number.
 *
 * @param hull 	hullset to position in
 * @param val 	number to position on
 *
 * @return 0 if the number is in the set, GRAPH_ERR_NO if not
 */
int graph_hullset_seek_to(graph_hullset *hull, unsigned long long val) {
  return graph_hullset_iterator_seek_to(hull, &hull->hull_iterator, val);
}

/**
 * @brief Reset the position of a hullset to the beginning.
 *
 *  The next call to graph_hullset_next() will return the first
 *  element of set.
 *
 * @param hull 	hullset to reset
 */
void graph_hullset_reset(graph_hullset *hull) {
  cl_log(hull->hull_cl, CL_LEVEL_DEBUG, "graph_hullset_reset %p", (void *)hull);

  hull->hull_iterator.hit_slot = 0;
  hull->hull_iterator.hit_offset = 0;

  cl_cover(hull->hull_cl);
}

/**
 * @brief Count the number of elements in a hullset.
 *
 *  The next call to graph_hullset_next() will return the first
 *  element of set.
 *
 *  XXX This could and should be made a lot more efficient by updating
 *      a centrally held count as hull slots change.
 *
 * @param hull 	hullset to test
 * @return the number of values returned by the hullset.
 */
unsigned long long graph_hullset_count(graph_hullset *hull) {
  graph_hullset_iterator hit;
  unsigned long long n = 0, start, end;

  if (hull == NULL) return 0;

  graph_hullset_iterator_initialize(&hit);
  while (graph_hullset_iterator_next_range(hull, &hit, &start, &end) == 0)
    n += end - start;

  cl_cover(hull->hull_cl);

  return n;
}

/**
 * @brief Does this hullset contain a single element only?
 * @param hull 	hullset we're asking about.
 * @return true if the hullset contains zero elements or a single element.
 */
bool graph_hullset_is_singleton(graph_hullset const *hull) {
  graph_hullset_iterator hit;
  unsigned long long n = 0, start, end;

  if (hull == NULL) return true;

  graph_hullset_iterator_initialize(&hit);
  while (graph_hullset_iterator_next_range(hull, &hit, &start, &end) == 0) {
    if ((n += end - start) > 1) {
      cl_cover(hull->hull_cl);
      return false;
    }
    cl_cover(hull->hull_cl);
  }
  cl_cover(hull->hull_cl);
  return n;
}

/**
 * @brief Position on a number
 *
 *  The next call to graph_hullset_next() will return the number, or
 *  the first larger number after that that's part of the set.
 *
 * @param hull 	hullset to position in
 * @param hit 	iterator to position
 * @param val_in_out 	number to position on or after
 * @param changed_out 	set this to true if *val_in_out is changed.
 *
 * @return 0 if the number is in the set, GRAPH_ERR_NO if not
 */
int graph_hullset_iterator_find(graph_hullset *hull,
                                graph_hullset_iterator *hit,
                                unsigned long long *val_in_out,
                                bool *changed_out) {
  unsigned long long v0 = *val_in_out;
  size_t i;
  graph_hullslot const *s;

  *changed_out = false;

  if (*val_in_out >= NBITS(34))
    cl_notreached(hull->hull_cl,
                  "graph_hullset_iterator_find: "
                  "value %llu (hex %llx) out of range\n",
                  *val_in_out, *val_in_out);

  cl_assert(hull->hull_cl, *val_in_out < NBITS(34));
  i = slot_in_or_after(hull, hit, v0);
  s = &index_to_slot(hull, i);

  for (;;) {
    if (*val_in_out < slot_skip_start(*s)) break;

    if ((i = slot_next(*s)) == SLOT_I_NULL) return GRAPH_ERR_NO;
    s = &index_to_slot(hull, i);

    *val_in_out = slot_span_start(*s);
    *changed_out = true;
  }
  hit->hit_slot = i;
  hit->hit_offset = *val_in_out - slot_span_start(*s);

  if (*changed_out)
    cl_log(hull->hull_cl, CL_LEVEL_DEBUG,
           "graph_hullset_iterator_find %llu -> %llu", v0, *val_in_out);

  return 0;
}
