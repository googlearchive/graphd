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
#ifndef GRAPHD_H
#define GRAPHD_H

#include "graphd/graphd-sabotage.h"

#include <stdbool.h> /* bool */

#include "libgdp/gdp-error.h" /* micro-parser */
#include "libgdp/gdp-micro.h" /* micro-parser */
#include "libgdp/gdp-token.h" /* tokens 	*/
#include "libgraph/graph.h"
#include "libpdb/pdb.h"
#include "libsrv/srv.h"

#define GRAPHD_ERR_SEMANTICS (GDP_ERR_SEMANTICS)
#define GRAPHD_ERR_SYNTAX (GDP_ERR_SYNTAX)
#define GRAPHD_ERR_LEXICAL (GDP_ERR_LEXICAL)
#define GRAPHD_ERR_MALFORMED (GDP_ERR_MALFORMED)
#define GRAPHD_ERR_TOO_LONG (GDP_ERR_TOO_LONG)

#define GRAPHD_ERR_NO (PDB_ERR_NO)
#define GRAPHD_ERR_MORE (PDB_ERR_MORE)
#define GRAPHD_ERR_PRIMITIVE_TOO_LARGE (PDB_ERR_PRIMITIVE_TOO_LARGE)
#define GRAPHD_ERR_ALREADY (PDB_ERR_ALREADY)
#define GRAPHD_ERR_NOT_SUPPORTED (PDB_ERR_NOT_SUPPORTED)

#define GRAPHD_ERR_BASE (-2000)
#define GRAPHD_ERR_UNIQUE_EXISTS (GRAPHD_ERR_BASE + 1)
#define GRAPHD_ERR_TILE_LEAK (GRAPHD_ERR_BASE + 2)
#define GRAPHD_ERR_TOO_MANY_MATCHES (GRAPHD_ERR_BASE + 3)
#define GRAPHD_ERR_TOO_LARGE (GRAPHD_ERR_BASE + 4)
#define GRAPHD_ERR_TOO_SMALL (GRAPHD_ERR_BASE + 5)
#define GRAPHD_ERR_NOT_A_REPLICA (GRAPHD_ERR_BASE + 6)
#define GRAPHD_ERR_TOO_HARD (GRAPHD_ERR_BASE + 7)
#define GRAPHD_ERR_RESTORE_MISMATCH (GRAPHD_ERR_BASE + 8)
#define GRAPHD_ERR_SMP_STARTUP (GRAPHD_ERR_BASE + 9)
#define GRAPHD_ERR_SMP (GRAPHD_ERR_BASE + 10)
#define GRAPHD_ERR_SUSPEND (GRAPHD_ERR_BASE + 11)
#define GRAPHD_ERR_SMP_WRITE (GRAPHD_ERR_BASE + 12)
#define GRAPHD_ERR_BADCURSOR (GRAPHD_ERR_BASE + 13)

extern char const graphd_build_version[];
extern cl_facility const graphd_facilities[];

#define GRAPHD_FACILITY_QUERY (1ul << 9)
#define GRAPHD_FACILITY_COST PDB_FACILITY_COST
#define GRAPHD_FACILITY_SCHEDULER SRV_FACILITY_SCHEDULER
#define GRAPHD_FACILITY_LINKSTO (1ul << 10)

/*
 * These attempt to extend the exit codes from sysexits.h
 * and provide specific feedback for failure.
 */
#define EX_GRAPHD_BASE 1000
#define EX_GRAPHD_DATABASE (EX_GRAPHD_BASE + 1)
#define EX_GRAPHD_REPLICA_MASTER (EX_GRAPHD_BASE + 2)
#define EX_GRAPHD_REPLICA_STREAM (EX_GRAPHD_BASE + 3)

/* The maximum number of records we allow per restore - 1 million.
 */
#define GRAPHD_RESTORE_MAX 1000000
#define GRAPHD_ISLINK_SIZE \
  (11 > sizeof(graph_guid) ? 11 : sizeof(graph_guid) + 1)

/* The maximum number of returned records at one level.
 */
#define GRAPHD_RESULT_PAGE_SIZE_DEFAULT 1024ul
#define GRAPHD_RESULT_PAGE_SIZE_MAX (64 * 1024ul)

typedef pdb_id graphd_localstate;
typedef struct graphd_handle graphd_handle;
typedef struct graphd_islink_handle graphd_islink_handle;
typedef struct graphd_session graphd_session;
typedef struct graphd_request graphd_request;
typedef struct graphd_sort_context graphd_sort_context;
typedef struct graphd_stack_context graphd_stack_context;
typedef struct graphd_stack graphd_stack;
typedef struct graphd_stack_type graphd_stack_type;
typedef struct graphd_property graphd_property;
typedef struct graphd_value graphd_value;
typedef struct graphd_and_slow_check_state graphd_and_slow_check_state;
typedef struct graphd_constraint graphd_constraint;
typedef struct graphd_value_range graphd_value_range;
typedef struct graphd_constraint_clause graphd_constraint_clause;
typedef struct graphd_constraint_or graphd_constraint_or;
typedef struct graphd_read_or_map graphd_read_or_map;
typedef struct graphd_xstate_ticket graphd_xstate_ticket;
typedef struct graphd_startup_todo_item graphd_startup_todo_item;

/*  The size of the iterator resource cache.
 */
#define GRAPHD_ITERATOR_RESOURCE_MAX (1024ull * 1024 * 20)

typedef unsigned int graphd_iterator_hint;

#define GRAPHD_ITERATOR_HINT_OR 0x0001
#define GRAPHD_ITERATOR_HINT_FIXED 0x0002

/*  Do not resolve originals using the per-request
 *  by-name database; create a new original.
 */
#define GRAPHD_ITERATOR_HINT_HARD_CLONE 0x0100

#define GRAPHD_ITERATOR_ISA_HINT_OPTIMIZE 0x01
#define GRAPHD_ITERATOR_ISA_HINT_CURSOR 0x02

typedef unsigned int graphd_iterator_isa_hint;

#define GRAPHD_BAD_CACHE_N 5

typedef struct graphd_bad_cache {
  pdb_id bc_id[GRAPHD_BAD_CACHE_N];
  size_t bc_n;

} graphd_bad_cache;

struct graphd_startup_todo_item {
  /*  If this is set, the item has been registered
   *  with the central startup todo list.
   */
  unsigned int sti_requested : 1;

  /*  If this is set, the item had been registered
   *  with the central startup todo list, and has been
   *  completed, as reported by the application.
   */
  unsigned int sti_done : 1;

  struct graphd_startup_todo_item *sti_next;
  struct graphd_startup_todo_item *sti_prev;
};

typedef struct graphd_islink_key {
  pdb_id key_type_id;

  /* The endpoint of all the links we're studying,
   * or PDB_ID_NONE if it's unspecific.
   */
  pdb_id key_endpoint_id;

  /* The linkage of the group or job results,
   * or PDB_LINKAGE_NONE if we're just generally
   * studying the type.
   */
  unsigned char key_result_linkage;

} graphd_islink_key;

typedef enum {
  /*  Ascending
   */
  GRAPHD_DIRECTION_FORWARD = 2,

  /*  Descending.
   */
  GRAPHD_DIRECTION_BACKWARD = 3,

  /*  Order as dictated by the ordering.
   */
  GRAPHD_DIRECTION_ORDERING = 4,

  /*  Unspecified.
   */
  GRAPHD_DIRECTION_ANY = 5

} graphd_direction;

#define GRAPHD_PIB_REQUEST "graphd.request"

#define GRAPHD_DIRECTION_VALID(d) \
  ((d) >= GRAPHD_DIRECTION_FORWARD && (d) <= GRAPHD_DIRECTION_ANY)

struct graphd_stack_context {
  cm_resource sc_resource;
  graphd_stack_type const *sc_type;
  int (*sc_run)(graphd_stack *st, graphd_stack_context *con);

  /*  Set if the stack has been suspended and must be resumed
   *  before running it.
   */
  unsigned int sc_suspended : 1;

  /* Open-ended, filled in by the implementation. */
};

typedef struct graphd_idle_checkpoint_context {
  /* srv_idle_context must be first -- struct punning.
   */
  srv_idle_context gic_srv;
  graphd_handle *gic_g;

} graphd_idle_checkpoint_context;

typedef struct graphd_idle_islink_context {
  /* srv_idle_context must be first -- struct punning.
   */
  srv_idle_context gii_srv;
  graphd_handle *gii_g;

} graphd_idle_islink_context;

typedef struct graphd_variable_declaration {
  /*  How many places use this variable on
   *  the right-hand-side of assignments?
   *
   *  This isn't updated as variables are assigned,
   *  but computed once centrally once we need to know.
   */
  size_t vdecl_linkcount;

  /* Where in the local frame is the value of the variable stored?
   */
  size_t vdecl_local;

  /*  Which constraint is this addressing?
   */
  graphd_constraint *vdecl_constraint;

  /*  How many parentheses around this variable during use,
   *  at most?
   */
  unsigned int vdecl_parentheses : 2;

} graphd_variable_declaration;

struct graphd_stack_type {
  int (*sct_run_default)(graphd_stack *st, graphd_stack_context *con);

  int (*sct_suspend)(graphd_stack *st, graphd_stack_context *con);

  int (*sct_unsuspend)(graphd_stack *st, graphd_stack_context *con);
};

typedef struct graphd_deferred_type {
  char const *dt_name;

  int (*dt_push)(graphd_request *greq, graphd_value *);

  int (*dt_suspend)(cm_handle *cm, cl_handle *cl, graphd_value *val);

  int (*dt_unsuspend)(cm_handle *cm, cl_handle *cl, graphd_value *val);

  void (*dt_finish)(graphd_value *);

} graphd_deferred_type;

typedef struct graphd_storable_type {
  char const *st_name;
  void (*st_destroy)(void *);
  bool (*st_equal)(void const *, void const *);
  unsigned long (*st_hash)(void const *);

} graphd_storable_type;

typedef struct graphd_storable {
  graphd_storable_type const *gs_type;

  /*  # of references to this piece of data.  If it drops to 0,
   *  the record can be deleted by calling st_destroy.
   */
  unsigned int gs_linkcount : 31;
  unsigned int gs_stored : 1;
  size_t gs_size;

} graphd_storable;

#define GRAPHD_STORABLE(s) ((graphd_storable *)(s))
#define GRAPHD_STORABLE_TYPE(s) (((graphd_storable const *)(s))->gs_type)

#define graphd_storable_destroy(gst) \
  (*GRAPHD_STORABLE_TYPE(gst)->st_destroy)((void *)(gst))

#define graphd_storable_hash(gst) \
  (*GRAPHD_STORABLE_TYPE(gst)->st_hash)((void const *)(gst))

#define graphd_storable_size(gs) (GRAPHD_STORABLE(gs)->gs_size)
#define graphd_storable_size_set(g, gs, s)                           \
  do {                                                               \
    if (GRAPHD_STORABLE(gs)->gs_stored)                              \
      (g)->g_iterator_resource_size += (s)-graphd_storable_size(gs); \
    graphd_storable_size(gs) = (s);                                  \
  } while (0)

#define graphd_storable_size_add(g, gs, s)                                    \
  do {                                                                        \
    if (GRAPHD_STORABLE(gs)->gs_stored) (g)->g_iterator_resource_size += (s); \
    graphd_storable_size(gs) += (s);                                          \
  } while (0)

#define graphd_storable_equal(A, B)                        \
  ((GRAPHD_STORABLE_TYPE(A) == GRAPHD_STORABLE_TYPE(B)) && \
   (*GRAPHD_STORABLE_TYPE(A)->st_equal)((void const *)(A), (void const *)(B)))

#define graphd_storable_link(A) \
  ((void)((A) != NULL && (((graphd_storable *)(A))->gs_linkcount++)))

#define graphd_storable_unlink(A)                                    \
  do {                                                               \
    if ((A) == NULL || ((graphd_storable *)(A))->gs_linkcount-- > 1) \
      ;                                                              \
    else                                                             \
      graphd_storable_destroy(A);                                    \
  } while (0)

#define graphd_storable_stored(gst) ((graphd_storable *)(gst)->st_stored)

typedef struct graphd_deferred_base {
  graphd_deferred_type const *db_type;

  /* If this drops to 0, free the base.
   */
  size_t db_link;

  /* The temporary results of an evaluation
   *
   * The first con->con_pframe_n are the deferrals
   * returned when first creating the base; the
   * next con->con_pframe_n are the results returned
   * after evaluating the data.
   */
  graphd_value *db_result;

  /* Data for use by the evaluator.
   */
  void *db_data;
  unsigned int db_suspended : 1;

} graphd_deferred_base;

struct graphd_stack {
  /* The stack as a whole is a resource that must be free'd.
   */
  cm_resource s_resource;
  cm_resource_manager s_resource_manager;
};

typedef struct graphd_check_cache {
  cm_hashtable cc_hash;
  unsigned int cc_initialized : 1;
} graphd_check_cache;

typedef struct graphd_guid_counter {
  graph_guid gc_guid[2];
  unsigned long long gc_n;
  unsigned long long gc_upper_bound;

} graphd_guid_counter;

typedef struct graphd_primitive_cache {
  pdb_id pc_id;
  graph_guid pc_guid;

  pdb_primitive pc_pr;
  unsigned int pc_pr_valid : 1;

} graphd_primitive_cache;

typedef enum graphd_access_global {
  /**
   * @brief Normal state.  Reading and writing works. (default)
   */
  GRAPHD_ACCESS_READ_WRITE,

  /**
   * @brief Write requests are denied.
   */
  GRAPHD_ACCESS_READ_ONLY,

  /**
   * @brief This is a replica, reads and replica-writes
   * are allowed, writes are forwarded to the master.
   */
  GRAPHD_ACCESS_REPLICA,

  /**
   * @brief Like the "replica" access mode, but without the replica
   * connection (only the write-through link is kept alive).
   */
  GRAPHD_ACCESS_REPLICA_SYNC,

  /**
   * @brief This is a read-only replica, reads and
   * replica-writes are allowed, writes fail.
   */
  GRAPHD_ACCESS_ARCHIVE,

  /**
   * @brief Database maintenance in progress.
   *  Read and write requests are denied with ERESTORE.
   */
  GRAPHD_ACCESS_RESTORE,

  /**
   * @brief Database shutdown in progress.  All requests are denied.
   */
  GRAPHD_ACCESS_SHUTDOWN,

  /**
   * @brief Limbo.  External operator intervention is required
   *	(and we're sticking around to say that until someone
   *	hears us).
   */
  GRAPHD_ACCESS_LIMBO

} graphd_access_global;

typedef enum graphd_flag_constraint {
  GRAPHD_FLAG_UNSPECIFIED,
  GRAPHD_FLAG_FALSE,
  GRAPHD_FLAG_TRUE,
  GRAPHD_FLAG_DONTCARE,
  GRAPHD_FLAG_TRUE_LOCAL

} graphd_flag_constraint;

typedef enum graphd_operator {
  GRAPHD_OP_UNSPECIFIED,
  GRAPHD_OP_LT,
  GRAPHD_OP_LE,
  GRAPHD_OP_EQ,
  GRAPHD_OP_GE,
  GRAPHD_OP_GT,
  GRAPHD_OP_NE,
  GRAPHD_OP_MATCH

} graphd_operator;

/* Flags returned by the cursor */

#define GRAPHD_MATCHES_INTRINSICS 0x01
#define GRAPHD_MATCHES_STRUCTURE 0x02

typedef struct graphd_loglevel {
  cl_loglevel_configuration gl_loglevel;
  bool gl_loglevel_valid;

  /*  If tok_s/tok_e are non-NULL, this restricts the
   *  changed loglevel to a specific session.
   */
  gdp_token gl_session;

} graphd_loglevel;

typedef struct graphd_core { bool gc_want_core; } graphd_core;

typedef struct graphd_generational_constraint {
  unsigned long gencon_min;
  unsigned long gencon_max;

  /*  If this is false, there are no constraints
   *  on the generation.
   */
  unsigned int gencon_valid : 1;

  /*  If this is false, nothing has been assigned to this
   *  generational constraint - the default applies.
   */
  unsigned int gencon_assigned : 1;

} graphd_generational_constraint;

typedef struct graphd_count_constraint {
  unsigned long long countcon_min;
  unsigned long long countcon_max;
  unsigned int countcon_min_valid : 1;
  unsigned int countcon_max_valid : 1;

  /*  If this is false, nothing has been assigned to this
   *  generational constraint - the default applies.
   */
  unsigned int countcon_assigned : 1;

} graphd_count_constraint;

typedef struct graphd_guid_set {
  /* For match sets only; sequential sets are ANDed.  */
  struct graphd_guid_set *gs_next;

  size_t gs_n;
  size_t gs_m;
  graph_guid *gs_guid;
  graph_guid gs_buf[1];

  /*  If set, the set contains the pseudo-option "null" as
   *  well as the included GUIDs.
   */
  unsigned int gs_null : 1;

} graphd_guid_set;

typedef struct graphd_guid_constraint {
  unsigned int guidcon_match_valid : 1;
  graphd_guid_set guidcon_match;

  unsigned int guidcon_include_annotated : 1;
  unsigned int guidcon_include_valid : 1;
  graphd_guid_set guidcon_include;

  unsigned int guidcon_exclude_valid : 1;
  graphd_guid_set guidcon_exclude;

} graphd_guid_constraint;

typedef struct graphd_dateline_constraint {
  graph_dateline *dateline_min;
  graph_dateline *dateline_max;

} graphd_dateline_constraint;

typedef struct graphd_string_constraint_element {
  struct graphd_string_constraint_element *strcel_next;
  /**
   * @brief NULL or a pointer to the first byte of the string to match
   */
  char const *strcel_s;

  /**
   * @brief NULL or a pointer just past the last byte of the
   *	 string to match
   */
  char const *strcel_e;

} graphd_string_constraint_element;

typedef struct graphd_string_constraint {
  /* @brief Next string constraint of the constraint.
   *	All string constraints in a constraint are ANDed together.
   */
  struct graphd_string_constraint *strcon_next;

  /* @brief First string of the set.  The strings of the
   * 	set in a strcon are ORed together.
   */
  graphd_string_constraint_element *strcon_head;

  /* @brief Append new set elements here.
   */
  graphd_string_constraint_element **strcon_tail;

  /**
   * @brief GRAPHD_OP_*
   */
  graphd_operator strcon_op;

} graphd_string_constraint;

typedef struct graphd_string_constraint_queue {
  graphd_string_constraint *strqueue_head;
  graphd_string_constraint **strqueue_tail;
} graphd_string_constraint_queue;

/** @brief A single element of a sort, result, or assignment pattern. */
typedef struct graphd_pattern graphd_pattern;

typedef enum graphd_pattern_type {
  /** @brief Not yet assigned. */
  GRAPHD_PATTERN_UNSPECIFIED = 0,

  /** @brief The archival bit. */
  GRAPHD_PATTERN_ARCHIVAL = 1,

  /** @brief The datatype: string, integer, etc.
   *  This is obsolete; use a numeric VALUETYPE instead.
   */
  GRAPHD_PATTERN_DATATYPE = 2,

  /** @brief How many times it has been versioned, until now. */
  GRAPHD_PATTERN_GENERATION = 3,

  /** @brief The primitive's GUID. */
  GRAPHD_PATTERN_GUID = 4,

  /** @brief The primitive's type GUID. */
  GRAPHD_PATTERN_LINKAGE_0 = 5,

  /** @brief The primitive's right GUID. */
  GRAPHD_PATTERN_LINKAGE_1 = 6,

  /** @brief The primitive's left GUID. */
  GRAPHD_PATTERN_LINKAGE_2 = 7,

  /** @brief The primitive's scope GUID. */
  GRAPHD_PATTERN_LINKAGE_3 = 8,

/** @brief The primitive's left GUID. */
#define GRAPHD_PATTERN_LEFT (GRAPHD_PATTERN_LINKAGE_0 + PDB_LINKAGE_LEFT)

/** @brief The primitive's right GUID. */
#define GRAPHD_PATTERN_RIGHT (GRAPHD_PATTERN_LINKAGE_0 + PDB_LINKAGE_RIGHT)

/** @brief The primitive's scope GUID. */
#define GRAPHD_PATTERN_SCOPE (GRAPHD_PATTERN_LINKAGE_0 + PDB_LINKAGE_SCOPE)

/** @brief The primitive's type GUID. */
#define GRAPHD_PATTERN_TYPEGUID \
  (GRAPHD_PATTERN_LINKAGE_0 + PDB_LINKAGE_TYPEGUID)
/**
* @brief The l'th linkage (0 through PDB_LINKAGE_N-1)
*/
#define GRAPHD_PATTERN_LINKAGE(l) (GRAPHD_PATTERN_LINKAGE_0 + (l))

  /** @brief A literal string. */
  GRAPHD_PATTERN_LITERAL = 9,

  /** @brief The live bit. */
  GRAPHD_PATTERN_LIVE = 10,

  /** @brief Unused. */
  GRAPHD_PATTERN_META = 11,

  /** @brief The name, as a lexical string. */
  GRAPHD_PATTERN_NAME = 12,

  /** @brief The GUID of the next version, or NULL. */
  GRAPHD_PATTERN_NEXT = 13,

  /** @brief The GUID of the previous version, or NULL. */
  GRAPHD_PATTERN_PREVIOUS = 14,

  /** @brief The timestamp. */
  GRAPHD_PATTERN_TIMESTAMP = 15,

  /** @brief The type, as a lexical string. */
  GRAPHD_PATTERN_TYPE = 16,

  /** @brief The value, as a lexical string. */
  GRAPHD_PATTERN_VALUE = 17,

  /** @brief A variable; for the name, see the data. */
  GRAPHD_PATTERN_VARIABLE = 18,

  /** @brief A list; for the elements, see the data. */
  GRAPHD_PATTERN_LIST = 19,

  /** @brief The count of matches until now. */
  GRAPHD_PATTERN_COUNT = 20,

  /** @brief A cursor. */
  GRAPHD_PATTERN_CURSOR = 21,

  /** @brief The result of nested constraints. */
  GRAPHD_PATTERN_CONTENTS = 22,

  /** @brief The optimization strategy and measurements. */
  GRAPHD_PATTERN_ESTIMATE = 23,

  /** @brief Datatype as a number */
  GRAPHD_PATTERN_VALUETYPE = 24,

  /** @brief The iterator data */
  GRAPHD_PATTERN_ITERATOR = 25,

  /** @brief We timed out?  Why? */
  GRAPHD_PATTERN_TIMEOUT = 26,

  /** @brief Rouch guess at the result count */
  GRAPHD_PATTERN_ESTIMATE_COUNT = 27,

  /** @brief Pick the first of ... */
  GRAPHD_PATTERN_PICK = 28,

  /** @brief Nothing at all. */
  GRAPHD_PATTERN_NONE = 29

} graphd_pattern_type;

#define GRAPHD_PATTERN_IS_COMPOUND(type) \
  ((type) == GRAPHD_PATTERN_LIST || (type) == GRAPHD_PATTERN_PICK)

#define GRAPHD_PATTERN_IS_SET_VALUE(type)                                    \
  ((type) == GRAPHD_PATTERN_COUNT || (type) == GRAPHD_PATTERN_CURSOR ||      \
   (type) == GRAPHD_PATTERN_ESTIMATE || (type) == GRAPHD_PATTERN_ITERATOR || \
   (type) == GRAPHD_PATTERN_TIMEOUT ||                                       \
   (type) == GRAPHD_PATTERN_ESTIMATE_COUNT)

#define GRAPHD_PATTERN_IS_PRIMITIVE_VALUE(type)                                \
  ((type) != GRAPHD_PATTERN_UNSPECIFIED && (type) != GRAPHD_PATTERN_LITERAL && \
   (type) != GRAPHD_PATTERN_NONE && (type) != GRAPHD_PATTERN_LIST &&           \
   (type) != GRAPHD_PATTERN_PICK && !GRAPHD_PATTERN_IS_SET_VALUE(type))

/**
 * @brief A single element of a sort, result, or assignment pattern.
 *
 *  Patterns are possibly nested lists.  Their structure is similar to
 *  graphd_values, but pattern talk about where something comes from, not
 *  what its actual value is.  (The value is "bob"; the pattern is "name".)
 */
struct graphd_pattern {
  /**
   * @brief Parent node in the tree.
   */
  graphd_pattern *pat_parent;

  /**
   * @brief Next sibling of a list; NULL at the end of a list.
   */
  graphd_pattern *pat_next;

  /**
   * @brief Which pattern is this node?
   */
  graphd_pattern_type pat_type;

  /**
   * @brief This pattern prints only if the primitive is a link.
   */
  unsigned int pat_link_only : 1;

  /**
   * @brief This pattern prints only if the primitive has contents.
   */
  unsigned int pat_contents_only : 1;

  /**
   * @brief For sort patterns only: If unset, reverse sense of the sort.
   */
  unsigned int pat_sort_forward : 1;

  /**
   * @brief This pattern is needed only if we're sorting.
   *
   *	If the sort was dropped for some reason (because
   * 	we're done with it, or we have a pre-sorted iterator),
   *	we don't need to read these samples.
   */
  unsigned int pat_sort_only : 1;

  /**
   * @brief The pattern is only non-null if this "or" branch is true.
   */
  size_t pat_or_index;

  /**
   * @brief Additional information for a specific pattern.
   */
  union {
    /** @brief Elements of a list. */
    struct {
      graphd_pattern *list_head;
      graphd_pattern **list_tail;
      size_t list_n;
    } data_list;

#define pat_list_head pat_data.data_list.list_head
#define pat_list_tail pat_data.data_list.list_tail
#define pat_list_n pat_data.data_list.list_n

    /** @brief Text of a literal string. */
    struct {
      char const *string_s;
      char const *string_e;
    } data_string;

#define pat_string_s pat_data.data_string.string_s
#define pat_string_e pat_data.data_string.string_e

    /**
     * @brief Name and location of a variable.
     *
     *  Variables are implicitly declared in the lowest
     *  context that uses them.
     */
    struct {
      struct graphd_constraint *variable_constraint;
      graphd_variable_declaration *variable_declaration;
    } data_variable;

#define pat_variable_constraint pat_data.data_variable.variable_constraint
#define pat_variable_declaration pat_data.data_variable.variable_declaration

  } pat_data;

  /**
   * @brief Sort and deferred sampling only: The pframe index of
   *	the result structure that contains the value we're actually
   *	sorting by (sort) / are sampling (deferred).
   *
   *	The result values returned to the caller's "contents"
   *	are at con_assignment_n; variables start at 0.
   *	An unnamed result allocated just to contain the sort()
   *  	and sampling values is at con_pframe_temporary.
   */
  size_t pat_result_offset;

  /**
   * @brief Sort and deferred sampling only: Within the list
   *	of elements nested into the result or variable pattern
   *	identified by the pat_result_offset, the offset
   *	of the subelement by which we sort (sort) /
   *	which we're sampling (deferred).
   */
  short pat_element_offset;

  /*
   * What comparator should be used for this element of the sort.
   */
  const struct graphd_comparator *pat_comparator;

  /* If 1, this value is sampled.  The first or the first
   * sorted element is copied into the result value.
   */
  unsigned int pat_sample : 1;

  /* If 1, this value is "collected".  A sequence of
   * per-matching-primitive clones takes its position.
   */
  unsigned int pat_collect : 1;
};

/**
 * @brief Assignment of a value to a named or unnamed variable.
 *
 *  This data structure is used to remember which values to
 *  extract from the alternatives and subconstraint results,
 *  and where to store them once they've been extracted.
 */

typedef struct graphd_assignment {
  /**
   * @brief Next assignment for the same constraint.
   */
  struct graphd_assignment *a_next;

  /* @brief Destination: "where does this value go?"
   */
  struct graphd_variable_declaration *a_declaration;

  /* @brief Depth (nesting level) of the assigment.
   */
  unsigned int a_depth : 2;
  /**
   * @brief Pattern or up to twice-nested list of patterns.
   *
   * The unnested or one-deep nested elements are extracted
   * once for the whole traversal; twice-nested elements
   * are extracted once per matching alternative, and returned
   * as a list.
   *
   *  So, (count (value datatype)) might turn
   *  into (2 ("a" string) ("1.2" float))
   *  for two primitives.
   */
  graphd_pattern *a_result;

} graphd_assignment;

typedef struct graphd_comparator {
  char const *cmp_locale;
  char const *cmp_name;
  char const *const *cmp_alias;

  /*
   * Check the syntax of strcon and call graphd_request_error if
   * it isn't valid
   */
  int (*cmp_syntax)(graphd_request *greq,
                    graphd_string_constraint const *strcon);

  /*
   * Create an iterator that is a superset of all value
   * equality constraints in this strcon.
   */
  int (*cmp_eq_iterator)(graphd_request *greq, int operation, const char *s,
                         const char *e, unsigned long long low,
                         unsigned long long high, graphd_direction direction,
                         char const *ordering, bool *indexed_inout,
                         pdb_iterator **it_out);

  /*
   * Create an iterator that is a superset of all inequality
   * constraints in this strcon.
   */
  int (*cmp_iterator_range)(graphd_request *greq, const char *lo_s,
                            const char *lo_e, const char *ho_s,
                            const char *hi_e, pdb_iterator *and_it,
                            unsigned long long low, unsigned long long high,
                            graphd_direction direction, bool value_forward,
                            char const *ordering, bool *indexed_inout);

  /*
   * Does s..e match the glob expression in glob_s..glob_e
   */
  bool (*cmp_glob)(graphd_request *greq, char const *glob_s, char const *glob_e,
                   char const *s, char const *e);

  /*
   * How do s1..e1 and s2..e2 sort under this comparator
   */
  int (*cmp_sort_compare)(graphd_request *greq, char const *s1, char const *e1,
                          char const *s2, char const *e2);

  /*
   * About cmp_vrange:
   *
   * This collection of functions allows comparators to to define
   * a mechanism for iterating over every primitive with a value
   * inside a particular range.
   *
   * These functions are always called from the context of a "vr"
   * value ranged iterator. They are passed an graphd_value_range structure
   * as well as a pre-allocated block of "private" storage.
   */

  /*
   * Return how many bytes of private storage I will need.
   * (Note that the strings lo and hi are always accessible through
   * vr
   */
  size_t (*cmp_vrange_size)(graphd_request *greq, const char *lo_s,
                            const char *lo_e, const char *hi_s,
                            const char *hi_e);

  /*
   * Prepare cmp_vrange_size(...) bytes of data for iteration over
   * the range from vr->lo .. vr->hi.
   * This should respect the vr->vr_valueforward flag. That is it
   * should return things "loosely" in ascending or descendign order
   * if it can.
   */

  int (*cmp_vrange_start)(graphd_request *greq, graphd_value_range *vr,
                          void *private_state);

  /*
   * Return the next iterator that has values inside the range we
   * care about. The old one will be destroyed automatically.
   *
   */
  int (*cmp_vrange_it_next)(graphd_request *greq, graphd_value_range *vr,
                            void *private_state, pdb_id low, pdb_id high,
                            pdb_iterator **it_out, pdb_budget *budget);

  /*
   * Calculate and return statistics over this value range.
   * This is always called before vrange_it_next and after vrange_it_start
   */
  int (*cmp_vrange_statistics)(graphd_request *greq, graphd_value_range *vr,
                               void *private_state,
                               unsigned long long *total_ids,
                               pdb_budget *next_cost, pdb_budget *budget);

  /*
   * This recovers the state after a cursor thaw.
   * This gets called immediatly after vrange_start().
   * It should recover whatever subiterator (i.e. bin) would have
   * returned the value s..e and then seek to id within that.
   *
   * This assumes that we're using a binning-like interface
   */
  int (*cmp_vrange_seek)(graphd_request *greq, graphd_value_range *vr,
                         void *private_state, const char *s, const char *e,
                         pdb_id id, pdb_id low, pdb_id high,
                         pdb_iterator **it_out);

  /*
   * Will the value s..e ever be returned?  This function should
   * use its knowledge about the structure of the underlying bins
   * and what bins it has already visited ti decide if primitives
   * with the value s..e might still be in the output or not.
   *
   * This is used to stop sorting quickly. A pessemistic
   * comparator could always return true here.
   */
  int (*cmp_value_in_range)(graphd_request *greq, graphd_value_range *vr,
                            void *private_state, const char *s, const char *e,
                            bool *string_in_range);

  int (*cmp_vrange_freeze)(graphd_request *greq, graphd_value_range *vr,
                           void *private_data, cm_buffer *buf);

  int (*cmp_vrange_thaw)(graphd_request *greq, graphd_value_range *vr,
                         void *private_data, const char *s, const char *e);

  /*
   * A string that sorts <= any other string
   */
  const char *cmp_lowest_string;

  /*
   * A string that sorts >= any other string
   */
  const char *cmp_highest_string;

} graphd_comparator;

struct graphd_value_range {
  /*
   * Internal state for a value inequality iterator
   */

  /*
   * Magic number
   */
  int vr_magic;

  /*  NULL or lower, upper boundaries.
   */
  const char *vr_lo_s;
  const char *vr_lo_e;
  const char *vr_hi_s;
  const char *vr_hi_e;

  /*
   * True if we exclude vr_lo
   */
  bool vr_lo_strict;

  /*
   * True if we exclude vr_hi
   */
  bool vr_hi_strict;

  /*  The iterator we use to extract new IDs. This is either going to
   *  be vr_internal_bit or a fixed iterator that intersects
   *  vr_internal_bin and vr_internal_and
   */
  pdb_iterator *vr_cvit;

  /*  The last id we've gotten from _ANY_ bin,
   *  or -1 if we haven't extracted any values yet.
   */
  pdb_id vr_last_id_out;

  /*
   * The ID we're currently holding to do on-or-after with inside
   * vrange_on_or_after. Only valid when it_call_state is 1
   */
  pdb_id vr_on_or_after_id;

  /*  The last ID we got from the current bin, or -1 if we havn't
   *  gotten anything from this bin yet
   */
  pdb_id vr_cvit_last_id_out;

  /*
   * Which comparator is used for this range iterator
   */
  const struct graphd_comparator *vr_cmp;

  bool vr_eof;

  /* How much state do we tack on to the end of this structure?
   */
  size_t vr_cmp_state_size;

  graphd_request *vr_greq;

  /*
   * What is the preferred ordering for this value range?
   */
  bool vr_valueforward;

  /*
   * This is an sorted iterator that we try to intersect with each
   * bin that we produce in order to cull results quickly. It may
   * be NULL
   */
  pdb_iterator *vr_internal_and;

  /*
   * This iterator is exactly the current bin that was returned
   * by the vrange system
   */
  pdb_iterator *vr_internal_bin;
};

typedef struct graphd_runtime_statistics {
  unsigned long long grts_system_millis;
  unsigned long long grts_user_millis;
  unsigned long long grts_wall_millis;
  unsigned long long grts_endtoend_millis;

  unsigned long long grts_system_micros;
  unsigned long long grts_user_micros;
  unsigned long long grts_wall_micros;
  unsigned long long grts_endtoend_micros;
  unsigned long long grts_endtoend_micros_start;

  unsigned long long grts_minflt;
  unsigned long long grts_majflt;
  unsigned long long grts_values_allocated;

  pdb_runtime_statistics grts_pdb;

} graphd_runtime_statistics;

typedef struct graphd_sort_root {
  struct graphd_constraint *sr_con;
  graphd_pattern sr_pat;
  char *sr_ordering;

} graphd_sort_root;

typedef struct graphd_pattern_frame {
  /*  The whole pattern.
   */
  graphd_pattern *pf_set;

  /*  The repeated piece.  This just points
   *  somewhere into the whole pattern.
   */
  graphd_pattern *pf_one;

  /*  What is the offset of the "one" result record within
   *  the "frame" result record?
   */
  size_t pf_one_offset;

} graphd_pattern_frame;

typedef struct graphd_comparator_list {
  const graphd_comparator **gcl_comp;
  size_t gcl_n;
  size_t gcl_m;
  bool gcl_used;

} graphd_comparator_list;

struct graphd_constraint {
  struct graphd_constraint *con_parent;
  struct graphd_constraint *con_next;
  struct graphd_constraint *con_head;
  struct graphd_constraint **con_tail;
  size_t con_subcon_n;

  /*  A temporary parser construct; once the constraint
   *  has been built, these pointers should be NULL.
   */
  graphd_constraint_clause *con_cc_head;
  graphd_constraint_clause **con_cc_tail;

  /*  The "or" alternative chain.
   */
  graphd_constraint_or *con_or_head;
  graphd_constraint_or **con_or_tail;

  /* If not NULL, the "or" that this optional sub constraint
   * is a part of.
   */
  graphd_constraint_or *con_or;

  /*  Index into the read-set "or" map, grsc_ros.
   *  Usually, 0.
   */
  size_t con_or_index;

  graphd_string_constraint_queue con_type;
  graphd_string_constraint_queue con_name;
  graphd_string_constraint_queue con_value;

  graphd_string_constraint con_strcon_buf[1];
  size_t con_strcon_n;

  /* result of (depreciated) comparator="xxx" clauses.
   * This is copied into con_value_comparator and con_sort_comparator
   * if those are not set explicitly.
   */
  graphd_comparator const *con_comparator;

  /* Which comparator to use for equality and inequality constraints
   */
  graphd_comparator const *con_value_comparator;
  unsigned int con_timestamp_valid : 1;
  graph_timestamp_t con_timestamp_min;
  graph_timestamp_t con_timestamp_max;

  graphd_generational_constraint con_newest;
  graphd_generational_constraint con_oldest;

#define GRAPHD_META_UNSPECIFIED 000
#define GRAPHD_META_ANY 007
#define GRAPHD_META_NODE 001
#define GRAPHD_META_LINK_TO 002
#define GRAPHD_META_LINK_FROM 004
#define GRAPHD_META_LINK_BIDI 006

  unsigned int con_meta : 4;

  /*  If for a linkage L not GRAPH_GUID_IS_NULL(con_linkguid[L]),
   *  then all matching primitives have that guid at linkage L.
   *  (It's like a primitive summary for the constraint.)
   */
  graph_guid con_linkguid[PDB_LINKAGE_N];

  unsigned int con_linkage : 4;

/* The values in con_linkage are derived from the four PDB_LINKAGE..
 * values:   0 type   1 right   2 left	 3 scope
 *
 * i am:  my parent points to me, and I am its <linkage>.
 * my:    I point to my parent; it is my <linkage>.
 */
#define graphd_linkage_is_i_am(c) ((c) >= 5 && (c) <= 9)
#define graphd_linkage_i_am(c) ((c)-5)
#define graphd_linkage_make_i_am(c) ((c) + 5)

#define graphd_linkage_is_my(c) ((c) >= 1 && (c) <= 4)
#define graphd_linkage_my(c) ((c)-1)
#define graphd_linkage_make_my(c) ((c) + 1)

  graphd_guid_constraint con_guid;
  graphd_guid_constraint con_linkcon[PDB_LINKAGE_N];

#define con_left con_linkcon[PDB_LINKAGE_LEFT]
#define con_right con_linkcon[PDB_LINKAGE_RIGHT]
#define con_scope con_linkcon[PDB_LINKAGE_SCOPE]
#define con_typeguid con_linkcon[PDB_LINKAGE_TYPEGUID]

  /* The next two are pseudoconstraints that eventually
   * translate into GUID restrictions.
   */
  graphd_guid_constraint con_version_previous;
  graphd_guid_constraint con_version_next;

  graphd_flag_constraint con_anchor;
  graphd_flag_constraint con_archival;
  graphd_flag_constraint con_live;
  graphd_count_constraint con_count;

  /* datatype/valuetype values */
  unsigned int con_valuetype : 8;

  /*  If this is set, the constraint cannot be satisfied.
   *  This does not include ``count'' constraints - a count=0
   *  constraint with con_false set is actually true.
   */
  unsigned int con_false : 1;

  /* If this is set, the constraint
   *
   *  - has a single-GUID GUID constraint
   *  - the intrinsics are true for that particular GUID.
   *
   *  (In other words, it's something like
   *	(GUID=1234 left=null right=null)
   *
   *  and we've checked left, right, and liveness.
   */
  unsigned int con_true : 1;

  /* Should we iterate from low to high or from high to low?
   */
  unsigned int con_forward : 1;

  /* This expression uses "contents" somewhere -- in a sort,
   * assignment, or return (possibly an implicit return).
   */
  unsigned int con_uses_contents : 1;

  /* Hint for a possible error message (usually, "SEMANTIC ...");
   * static or allocated on the request heap.
   */
  char const *con_error;

  int con_unique;
  int con_key;

  /*  There are up to assignment_n + 2 pframes in total.
   *
   *  pframe 0..n-1 are the first assignment_n pframes.
   *  pframe N is the returned result.
   *  pframe N + 1 is the unnamed temporary.
   */
  graphd_pattern_frame *con_pframe;
  size_t con_pframe_n;
  cm_hashtable con_pframe_by_name;

  size_t con_pframe_temporary;
  unsigned int con_pframe_want_count : 1;
  unsigned int con_pframe_want_cursor : 1;
  unsigned int con_pframe_want_data : 1;

  graphd_pattern *con_result;

  graphd_pattern *con_sort;
  graphd_sort_root con_sort_root;
  unsigned int con_sort_valid : 1;

  /* The estimated maximum number of matches for this
   * constraint, given any fixed parent.
   */
  unsigned long long con_setsize;

  size_t con_pagesize;
  unsigned int con_pagesize_valid : 1;

  size_t con_resultpagesize_parsed;
  unsigned int con_resultpagesize_parsed_valid : 1;

  size_t con_resultpagesize;
  unsigned int con_resultpagesize_valid : 1;

  unsigned long long con_countlimit;
  unsigned int con_countlimit_valid : 1;

  size_t con_start;

  /*  Some IDs that don't match this.
   */
  graphd_bad_cache con_bad_cache;

  graphd_dateline_constraint con_dateline;

  /* The constraint title.  Used to
   * identify this constraint in log entries.
   */
  char *con_title;
  char con_title_buf[256];

  /**
   * @brief Either NULL or start and end of a cursor token.
   */
  char const *con_cursor_s;
  char const *con_cursor_e;

  /**
   * @brief A parsed initial offset.  When encoded in the
   * 	cursor, it is the number of elements in the result
   * 	set before the cursor.  Default is 0.
   */
  unsigned long long con_cursor_offset;

  /* @brief If set, this cursor may be used in a cursor.
   */
  unsigned int con_cursor_usable : 1;

  /*  (READ)
   *
   *  A counted chain that keeps track of the variables assigned
   *  to via $tag=value statements.
   */
  graphd_assignment *con_assignment_head;
  graphd_assignment **con_assignment_tail;
  size_t con_assignment_n;

  /*  How many variables in this context are assigned to
   *  in subconstraints.
   */
  size_t con_local_n;

  /*  Map the variable name to an index into the temporary
   *  local results.
   */
  cm_hashtable con_local_hash;

  /*  (READ)  Variables declared in this constraint.
   *
   *  The hashtable hashes their name to a
   *  graphd_variable_declaration record.
   */
  cm_hashtable con_variable_declaration;
  unsigned int con_variable_declaration_valid : 1;

  /*  (READ)  An iterator that produces candidates for this
   *  constraint (not taking into account the parent).
   */
  pdb_iterator *con_it;

  /*  (READ) low, high boundaries
   */
  unsigned long long con_low;
  unsigned long long con_high;

  /**
   * @brief The next in a chain of constraints that hash to the
   *	same shape slot.
   */
  struct graphd_constraint *con_shape_next;

  /**
   * @brief Null, or an anchor constraint starting here.
   */
  struct graphd_constraint *con_anchor_dup;

  /**
   * @brief Null, or a keyed constraint starting here.
   */
  struct graphd_constraint *con_key_dup;

  /**
   * @brief Null, or a unique constraint starting here.
   */
  struct graphd_constraint *con_unique_dup;

  /**
   * @brief Constraint has a cursor result and is therefore
   * 	  resumable after a soft timeout.
   */
  unsigned int con_resumable : 1;

  graphd_comparator_list con_sort_comparators;

  /* @brief Accounting, used if the caller wants a constraint
   *  	heat map.
   */
  pdb_iterator_account con_iterator_account;

  /* @brief The unique (per request) ID of the constraint.
   */
  size_t con_id;
};

typedef enum graphd_constraint_clause_type {
  GRAPHD_CC_ANCHOR,
  GRAPHD_CC_ARCHIVAL,
  GRAPHD_CC_ASSIGNMENT,
  GRAPHD_CC_COMPARATOR,
  GRAPHD_CC_COUNT,
  GRAPHD_CC_COUNTLIMIT,
  GRAPHD_CC_CURSOR,
  GRAPHD_CC_DATELINE,
  GRAPHD_CC_FALSE,
  GRAPHD_CC_GUID,
  GRAPHD_CC_GUIDLINK,
  GRAPHD_CC_LIVE,
  GRAPHD_CC_LINKAGE,
  GRAPHD_CC_META,
  GRAPHD_CC_NAME,
  GRAPHD_CC_NEWEST,
  GRAPHD_CC_NEXT,
  GRAPHD_CC_OLDEST,
  GRAPHD_CC_BOR,
  GRAPHD_CC_LOR,
  GRAPHD_CC_PAGESIZE,
  GRAPHD_CC_PREV,
  GRAPHD_CC_RESULT,
  GRAPHD_CC_RESULTPAGESIZE,
  GRAPHD_CC_SEQUENCE,
  GRAPHD_CC_SORT,
  GRAPHD_CC_SORTCOMPARATOR,
  GRAPHD_CC_START,
  GRAPHD_CC_SUBCON,
  GRAPHD_CC_TIMESTAMP,
  GRAPHD_CC_TYPE,
  GRAPHD_CC_VALUE,
  GRAPHD_CC_VALTYPE,
  GRAPHD_CC_VALUECOMPARATOR

} graphd_constraint_clause_type;

struct graphd_constraint_clause {
  graphd_constraint_clause_type cc_type;
  graphd_constraint_clause *cc_next;
  union {
    struct {
      char const *asn_name_s;
      char const *asn_name_e;
      graphd_pattern *asn_pattern;

    } cd_assignment;

    graphd_comparator const *cd_comparator;

    struct {
      unsigned long long count_value;
      graphd_operator count_op;

    } cd_count;

    struct {
      char const *cursor_s;
      char const *cursor_e;
    } cd_cursor;

    struct {
      graph_dateline *dateline_value;
      graphd_operator dateline_op;
    } cd_dateline;

    graphd_flag_constraint cd_flag;

    struct {
      unsigned long long gencon_value;
      graphd_operator gencon_op;
    } cd_gencon;

    struct {
      graphd_operator guidcon_op;
      graphd_guid_set *guidcon_set;
      int guidcon_linkage;
    } cd_guidcon;

    unsigned long long cd_limit;
    int cd_meta;

    graphd_constraint_or *cd_or;
    graphd_pattern *cd_pattern;
    graphd_constraint_clause *cd_sequence;
    graphd_comparator_list cd_sortcomparators;

    unsigned long long cd_start;
    graphd_string_constraint *cd_strcon;
    graphd_constraint *cd_subcon;

    struct {
      graph_timestamp_t timestamp_value;
      graphd_operator timestamp_op;
    } cd_timestamp;

    unsigned char cd_valtype;
    unsigned int cd_linkage;

  } cc_data;
};

struct graphd_constraint_or {
  graphd_constraint_or *or_next;
  graphd_constraint *or_prototype;

  graphd_constraint or_head;
  graphd_constraint *or_tail;

  unsigned int or_short_circuit : 1;
};

#define GRAPHD_CONSTRAINT_IS_OPTIONAL(con)  \
  ((con)->con_count.countcon_min == 0 &&    \
   (!(con)->con_count.countcon_max_valid || \
    (con)->con_count.countcon_max > 0))

#define GRAPHD_CONSTRAINT_IS_MANDATORY(con) ((con)->con_count.countcon_min > 0)

/**
 * @brief Parsed argument of the "set" command.
 */
typedef struct graphd_set_subject {
  /* Must be first to easily pun tail pointer as record pointer. */
  struct graphd_set_subject *set_next;

  const char *set_name_s;
  const char *set_name_e;
  const char *set_value_s;
  const char *set_value_e;

} graphd_set_subject;

/**
 * @brief Parsed argument of the "smp" command.
 */

typedef enum {
  GRAPHD_SMP_UNSPECIFIED,
  GRAPHD_SMP_PREWRITE,
  GRAPHD_SMP_POSTWRITE,
  GRAPHD_SMP_PAUSED,
  GRAPHD_SMP_RUNNING,
  GRAPHD_SMP_STATUS,
  GRAPHD_SMP_CONNECT
} graphd_smp_command;

/**
 * @brief What type of SMP process am I?
 */

typedef enum {
  GRAPHD_SMP_PROCESS_SINGLE,
  GRAPHD_SMP_PROCESS_LEADER,
  GRAPHD_SMP_PROCESS_FOLLOWER
} graphd_smp_process_type;

/**
 * @brief What state is this session in?
 */
typedef enum {
  GRAPHD_SMP_SESSION_RUN,
  GRAPHD_SMP_SESSION_SENT_PAUSE,
  GRAPHD_SMP_SESSION_PAUSE,
  GRAPHD_SMP_SESSION_SENT_RUN
} graphd_session_smp_state;

/**
 * @brief Parsed argument of the "status" command.
 */
typedef struct graphd_status_subject {
  struct graphd_status_subject *stat_next;
  enum {
    GRAPHD_STATUS_UNSPECIFIED = 0,
    GRAPHD_STATUS_CONNECTION = 1,
    GRAPHD_STATUS_DATABASE = 2,
    GRAPHD_STATUS_DIARY = 3,
    GRAPHD_STATUS_MEMORY = 4,
    GRAPHD_STATUS_RUSAGE = 5,
    GRAPHD_STATUS_PROPERTY = 6,
    GRAPHD_STATUS_TILES = 7,
    GRAPHD_STATUS_REPLICA = 8,
    GRAPHD_STATUS_ISLINK = 9
  } stat_subject;
  unsigned long long stat_number;
  graphd_property const *stat_property;

} graphd_status_subject;

typedef int graphd_property_set(graphd_property const *prop,
                                graphd_request *greq,
                                graphd_set_subject const *su);

typedef int graphd_property_status(graphd_property const *prop,
                                   graphd_request *greq, graphd_value *val_out);

/*  A property is something you can set (via "set (name=...)" or
 *  sometimes configuration) and get (via "status (name)").
 */
struct graphd_property {
  char const *prop_name;
  graphd_property_set *prop_set;
  graphd_property_status *prop_status;
};

/**
 * @brief Singly linked list of "status subjects".
 */
typedef struct graphd_status_queue {
  graphd_status_subject *statqueue_head;
  graphd_status_subject **statqueue_tail;

} graphd_status_queue;

/**
 * @brief Singly linked list of "set subjects".
 */
typedef struct graphd_set_queue {
  graphd_set_subject *setqueue_head;
  graphd_set_subject **setqueue_tail;

} graphd_set_queue;

/**
 * @brief The basic building block of graphd query results.
 *  Not to be confused with the primitive component called "value".
 *
 *  Graphd queries generally return nested lists of lists and
 *  strings or atoms.  Prior to formatting, each node in these
 *  lists is internally represented as one graphd_value; "(1 (2 3))"
 *  takes five graphd_value structures to represent.
 *
 *  A graphd_value that contains text from a primitive holds
 *  a lock on the primitive and just points to text that has been
 *  mapped in from the primitive database.  This means that
 *  graphd values must be free'd explicitly; you can't just rely
 *  on the per-request heap deallocation.
 */

typedef struct graphd_timestamp {
  graph_timestamp_t gdt_timestamp;

  /* PDB_ID_NONE or the pdb_id of the primitive at that timestamp.
   */
  pdb_id gdt_id;

} graphd_timestamp;

struct graphd_value {
  enum {
    GRAPHD_VALUE_UNSPECIFIED = 0,
    GRAPHD_VALUE_STRING = 1, /* text with ""  */
    GRAPHD_VALUE_ATOM = 2,   /* text without  */
    GRAPHD_VALUE_NUMBER = 3,
    GRAPHD_VALUE_GUID = 4,
    GRAPHD_VALUE_LIST = 5,     /* array with () */
    GRAPHD_VALUE_SEQUENCE = 6, /* array without */
    GRAPHD_VALUE_TIMESTAMP = 7,
    GRAPHD_VALUE_BOOLEAN = 8,
    GRAPHD_VALUE_DATATYPE = 9,
    GRAPHD_VALUE_NULL = 10,
    GRAPHD_VALUE_RECORDS = 11,
    GRAPHD_VALUE_DEFERRED = 12

#define GRAPHD_VALUE_IS_TYPE(x) ((x) >= 0 && (x) <= 10)
#define GRAPHD_VALUE_IS_ARRAY(val)        \
  ((val).val_type == GRAPHD_VALUE_LIST || \
   (val).val_type == GRAPHD_VALUE_SEQUENCE)

  } val_type;

  union {
    struct {
      cm_handle *text_cm;
      char const *text_s;
      char const *text_e;
      pdb_primitive_reference text_ref;
    } data_text; /* string or atom */

    unsigned long long data_number;
    graph_guid data_guid;
    graphd_timestamp data_timestamp;
    int data_boolean;
    int data_datatype;

    struct {
      cm_handle *array_cm;
      struct graphd_value *array_contents;
      size_t array_n;
      size_t array_m;

    } data_array; /* list or sequence */

    struct {
      pdb_handle *records_pdb;
      pdb_id records_i;
      pdb_id records_n;
    } data_records;

    struct {
      graphd_deferred_base *deferred_base;
      size_t deferred_index;
    } data_deferred;

  } val_data;

#define val_text_cm val_data.data_text.text_cm
#define val_text_s val_data.data_text.text_s
#define val_text_e val_data.data_text.text_e
#define val_text_ref val_data.data_text.text_ref

#define val_array_cm val_data.data_array.array_cm
#define val_array_contents val_data.data_array.array_contents
#define val_array_n val_data.data_array.array_n
#define val_array_m val_data.data_array.array_m

#define val_guid val_data.data_guid
#define val_timestamp val_data.data_timestamp.gdt_timestamp
#define val_timestamp_id val_data.data_timestamp.gdt_id
#define val_boolean val_data.data_boolean
#define val_datatype val_data.data_datatype
#define val_number val_data.data_number

#define val_sequence_contents val_array_contents
#define val_sequence_n val_array_n
#define val_sequence_m val_array_m

#define val_list_contents val_array_contents
#define val_list_n val_array_n
#define val_list_m val_array_m

#define val_records_pdb val_data.data_records.records_pdb
#define val_records_i val_data.data_records.records_i
#define val_records_n val_data.data_records.records_n

#define val_deferred_base val_data.data_deferred.deferred_base
#define val_deferred_index val_data.data_deferred.deferred_index

#define GRAPHD_VALUE_IS_TEXT_LIT(v, lit)                   \
  (((v)->val_type == GRAPHD_VALUE_STRING ||                \
    (v)->val_type == GRAPHD_VALUE_ATOM) &&                 \
   (v)->val_text_e - (v)->val_text_s == sizeof(lit) - 1 && \
   strncasecmp((v)->val_text_s, (lit), sizeof(lit) - 1) == 0)
};

typedef struct graphd_request_parameter graphd_request_parameter;

typedef int graphd_request_parameter_format(graphd_request_parameter *_grp,
                                            graphd_request *_greq, char **_s,
                                            char *_e);

struct graphd_request_parameter {
  graphd_request_parameter *grp_next;
  graphd_request_parameter_format *grp_format;
};

typedef struct graphd_request_parameter_id {
  graphd_request_parameter id_generic;
  char *id_s;
  char *id_e;

} graphd_request_parameter_id;

typedef struct graphd_request_parameter_heatmap {
  graphd_request_parameter hm_generic;
  char *hm_s;
  char *hm_e;

} graphd_request_parameter_heatmap;

typedef struct {
  graph_guid verify_guid_low;
  graph_guid verify_guid_high;
  pdb_id verify_id;
  int verify_result_slot;
  unsigned long long verify_count;
  pdb_id verify_pdb_low;
  pdb_id verify_pdb_high;
  graphd_value *verify_count_value;
  unsigned long long verify_pagesize;
} graphd_verify_query;

typedef void graphd_request_format(void *_data, srv_handle *_srv,
                                   void *_session_data, void *_request_data,
                                   char **_s, char *_e);

/*  This define is a large estimate for what we actually see, replaying backups
 * .
 *  gbackup provides (by default) 128 * 1024 entries per page, the longest of
 * which
 *  is ~ 500 bytes, coming out to about 64M as the square bound.
 *
 *  Double that to be safe.
 */

#define GRAPHD_MAX_REQUEST_LENGTH (1024 * 1024 * 128)

/*  The largest number of write requests we'll allow to have waiting
 *  on a replica connection.
 */
#define GRAPHD_OUTGOING_REQUESTS_MAX (3)

typedef struct graphd_request_type {
  char const *grt_name;

  void (*grt_input_arrived)(graphd_request *);
  int (*grt_output_sent)(graphd_request *);
  int (*grt_run)(graphd_request *, unsigned long long deadline);
  void (*grt_cancel)(graphd_request *);
  void (*grt_free)(graphd_request *);

} graphd_request_type;

#define graphd_request_session(greq) \
  ((graphd_session *)((greq)->greq_req.req_session))

#define graphd_request_cm(greq) ((greq)->greq_req.req_cm)

#define graphd_request_cl(greq) (((greq)->greq_req.req_session)->ses_bc.bc_cl)

#define graphd_request_graphd(greq) (graphd_request_session(greq)->gses_graphd)

#define graphd_request_srv(greq) ((greq)->greq_req.req_session->ses_srv)

#define graphd_request_pdb(greq) (graphd_request_graphd(greq)->g_pdb)

struct graphd_request {
  srv_request greq_req;
  graphd_request_type const *greq_type;

  pdb_iterator_base greq_pib;

  /* micro-parser state */
  gdp_micro greq_micro;

  /*  # of incoming bytes (result of adding up all the incoming
   *  buffer parts).  If this number gets larger than
   *  g_request_size_max, the request gets thrown out.
   */
  size_t greq_request_size;

  gdp_token greq_error_token;

  /*  Outgoing error message. Use graphd_request_errprintf() or
   *  graphd_request_error() to change this.
   */
  char const *greq_error_message;

  /*  In outgoing requests, sometimes we don't have
   *  the request text yet as the request starts - use
   *  this text instead.
   */
  char const *greq_request_start_hint;

  enum graphd_command {
    GRAPHD_REQUEST_UNSPECIFIED,
    GRAPHD_REQUEST_CRASH,
    GRAPHD_REQUEST_DUMP,
    GRAPHD_REQUEST_ERROR,
    GRAPHD_REQUEST_ITERATE,
    GRAPHD_REQUEST_ISLINK, /* 5 */
    GRAPHD_REQUEST_READ,
    GRAPHD_REQUEST_RESTORE,
    GRAPHD_REQUEST_SET,
    GRAPHD_REQUEST_SKIP,
    GRAPHD_REQUEST_SMP, /* 10 */
    GRAPHD_REQUEST_SMP_OUT,
    GRAPHD_REQUEST_STATUS,
    GRAPHD_REQUEST_SMP_FORWARD,
    GRAPHD_REQUEST_SYNC,
    GRAPHD_REQUEST_WRITE, /* 15 */
    GRAPHD_REQUEST_VERIFY,
    GRAPHD_REQUEST_PASSTHROUGH,
    GRAPHD_REQUEST_REPLICA,

    /*  A write request that's being forwarded
     *  to a write master.
     */
    GRAPHD_REQUEST_WRITETHROUGH,

    /*  This is a replica-write command from the
     *  replica's side.  The sender's version of
     *  this is ASYNC_REPLICA_WRITE.
     */
    GRAPHD_REQUEST_REPLICA_WRITE,

    /*  A synchronous "replica" command, seen from the
     *  client, sent from the client to the master.
     */
    GRAPHD_REQUEST_CLIENT_REPLICA,

    /*  Grammar for asynchronous requests:
     *  GRAPHD_REQUEST_ASYNC_[RECIPIENT]_[FUNCTION]
     */
    GRAPHD_REQUEST_ASYNC_REPLICA_WRITE,
    GRAPHD_REQUEST_ASYNC_REPLICA_RESTORE,
    GRAPHD_REQUEST_ASYNC_REPLICA_CATCH_UP,

    GRAPHD_REQUEST_CLIENT_READ
#define GRAPHD_REQUEST_MAX GRAPHD_REQUEST_CLIENT_READ

  } greq_request;

#define GRAPHD_REQUEST_IS_REPLICA(x)              \
  ((x) == GRAPHD_REQUEST_CLIENT_REPLICA ||        \
   (x) == GRAPHD_REQUEST_ASYNC_REPLICA_RESTORE || \
   (x) == GRAPHD_REQUEST_ASYNC_REPLICA_WRITE ||   \
   (x) == GRAPHD_REQUEST_ASYNC_REPLICA_CATCH_UP)

  graphd_guid_set greq_guidset;

  /*  In an OK or ERROR reply, which one was it?
   */
  bool greq_response_ok;

  /*  Did we get a response at all?  If this error is
   *  nonzero, it's the connection's error.
   */
  int greq_transmission_error;

  /*  While parsing read or write, the constraint currently being
   *  parsed.  Afterwards, the root of the constraint tree.
   */
  graphd_constraint *greq_constraint;

  /* Save enough for a "couple" -- a node and link. */
  graphd_constraint greq_constraint_buf[2];
  size_t greq_constraint_n;

  /* The request parameters, in the order received (which will
   * also be their order of formatting on completion.)
   */
  graphd_request_parameter *greq_parameter_head;
  graphd_request_parameter **greq_parameter_tail;

  /* Save enough for about three result= instructions
   */
  graphd_pattern greq_pattern_buf[4];
  size_t greq_pattern_n;

  graphd_verify_query greq_verifyquery;

  /* The offset into the text of greq_writethrough to
   * copy from.
   */
  size_t greq_offset;

  /* Building a reply
   */
  graphd_value greq_reply;
  int greq_reply_err;

  void *greq_serve_context;

  unsigned long greq_timeout;

  /*  The statistics from when the request started.
   */
  graphd_runtime_statistics greq_runtime_statistics;
  graphd_runtime_statistics greq_runtime_statistics_accumulated;
  graphd_runtime_statistics greq_runtime_statistics_allowance;

  /* For RESTORE and REPLICA-WRITE
   */
  void *greq_restore_base;
  size_t greq_restore_n;
  size_t greq_restore_i;
  unsigned char greq_restore_version;

  /* For REPLICA_WRITE, DUMP and RESTORE: start=.. end=.. result
   *  set boundaries.
   */
  unsigned long long greq_start;
  unsigned long long greq_end;
  unsigned long long greq_pagesize;

  /* Formatting a reply
   */
  graphd_request_format *greq_format;
  char const *greq_format_s;
  char *greq_format_s_buf;
  cm_handle *greq_format_s_cm;

  graphd_value **greq_format_stack;
  size_t greq_format_stack_n;
  size_t greq_format_stack_m;
  void *greq_format_records_context;

  graph_dateline *greq_dateline;

  size_t greq_format_dateline_offset;
  void *greq_format_dateline_state;
  size_t greq_format_dateline_io;
  bool greq_format_dateline_io_done;

  /*  As-of constraint on all requests.  If non-NULL, return results
   *  as of, that is, before, this dateline.
   */
  graph_dateline *greq_asof;

  /*  A less parsed version of the input to the graph_dateline.
   *  We delay resolution of dates into GUIDs until just before
   *  execution.
   */
  graphd_value *greq_asof_value;

  /*  The database state horizon.
   *
   *  If the database state is up to or beyond this number,
   *  the data that this request result is based on has hit disk.
   */
  unsigned long long greq_horizon;

  /*  Per-request loglevel, and is it valid?
   */
  cl_loglevel_configuration greq_loglevel;

  graphd_stack greq_stack;
  unsigned int greq_indent;

#define GRAPHD_ERRORSTATE_INITIAL 0
#define GRAPHD_ERRORSTATE_KEYWORD 1
#define GRAPHD_ERRORSTATE_QUOTE 2
#define GRAPHD_ERRORSTATE_MESSAGE 3

  /* Formatter state in graphd-request-output:format_error()
   */
  unsigned int greq_error_state : 2;

  /*  When set to true, causes %t in error string to be
   *  replaced with the current error token.
   */
  unsigned int greq_error_substitute : 1;

  /*  Does this request have its own loglevel?
   */
  unsigned int greq_loglevel_valid : 1;

  /*  Did this request have a dateline="" parameter that causes
   *  us to return a dateline in response?
   */
  unsigned int greq_dateline_wanted : 1;

  /*  State for list value formatting in
   *  graphd-request-output.c
   */
  unsigned int greq_format_list_first : 1;
  unsigned int greq_format_list_sep : 1;
  unsigned int greq_format_list_finishing : 1;

  /* Have runtime statistics been started, but not yet completed?
   */
  unsigned int greq_runtime_statistics_started : 1;

  /* Can this request overlap (coroutine-wise) with other requests?
   */
  enum {
    /* can overlap with absolutely everything. */
    GRAPHD_XSTATE_NONE,

    /* can overlap with anything except GRAPHD_XSTATE_EXCLUSIVE */
    GRAPHD_XSTATE_SHARED,

    /* can overlap with nothing except GRAPHD_STATE_NONE. */
    GRAPHD_XSTATE_EXCLUSIVE
  } greq_xstate;

  /*  If non-NULL, a ticket owned by the request.
   */
  graphd_xstate_ticket *greq_xstate_ticket;

  /* Do we desire constraint heatmap accounting?
   */
  unsigned int greq_heatmap : 1;

  /*  A request is marked as "pushed back" if it was running,
   *  ran too long, and was pushed back into the front of the
   *  session queue.
   *  Use graphd_request_push_back() to do this.
   */
  unsigned int greq_pushed_back : 1;

  /* When we time out, return a cursor rather than an error.
   */
  unsigned int greq_soft_timeout : 1;
  char *greq_soft_timeout_triggered;

  /* Everything that is hooked into this resource manager is
   * freed late in graphd_request_finish, after the request
   * has been formatted.
   */
  cm_resource_manager greq_resource;
  pdb_iterator_chain greq_iterator_chain;

  unsigned long long greq_timeout_deadline;
  unsigned long long greq_timeout_ticks;

  unsigned int greq_completed : 1;

  /* Session that the request is waiting for room in.
   */
  graphd_session *greq_session_wait;

  /* What the request will be ready for if it wakes up.
   */
  unsigned short greq_session_wait_ready;

  /* Next request waiting for room in the same session.
   */
  graphd_request *greq_session_wait_next;

  /* Previous request waiting for room in the same session.
   */
  graphd_request *greq_session_wait_prev;

  /* If this request is being forwarded, this is its outgoing
   * request
   */
  graphd_request *greq_master_req;

  /* Chain for keeping track of outgoing SMP copies of this request
   */
  graphd_request *greq_smp_request_collection_chain;
  bool greq_smp_forward_started;

  /*  Semantic data
   */
  union {
    struct {
      /* start with this primitive */
      unsigned long long gdrep_start_id;
      graph_dateline *gdrep_start;
      int gdrep_version;
      bool gdrep_master;

    } gd_replica;

    struct {
      /* Request: start with this primitive */
      unsigned long long gdcrep_start_id;
      int gdcrep_version;
      bool gdcrep_master;

      /* Response: Did the master say "OK"?
       */
      bool gdcrep_ok;

      /* Response: The URL of the write master.
       */
      char const *gdcrep_write_url_s;
      char const *gdcrep_write_url_e;

    } gd_client_replica;

    struct {
      graphd_set_queue gds_setqueue;
    } gd_set;

    struct {
      graphd_status_queue gds_statqueue;

    } gd_status;

    struct {
      bool gdsf_finished;
      graphd_request *gdsf_request_collection_next;
      char *gdsf_malloced_buf;
      char const *gdsf_response_s;
      char const *gdsf_response_e;
      char const *gdsf_response_tok_s;
      char const *gdsf_response_tok_e;
#define gdsf_tok_s greq_data.gd_smp_forward.gdsf_response_tok_s
#define gdsf_tok_e greq_data.gd_smp_forward.gdsf_response_tok_e
#define gdsf_s greq_data.gd_smp_forward.gdsf_response_s
#define gdsf_e greq_data.gd_smp_forward.gdsf_response_e

    } gd_smp_forward;

    struct {
      graphd_smp_command gdso_smpcmd;
      unsigned long long gdso_smppid;

    } gd_smp_out;

    struct {
      graphd_smp_command gds_smpcmd;
      unsigned long long gds_smppid;

    } gd_smp;

    struct {
      /*  The first write bit is set on the first
       *  primitive write in a write command.
       *  The TXSTART primitive bit is the inverse
       *  of this.
       */
      unsigned int gdw_txstart_written : 1;

    } gd_write;

    struct {
      graphd_request *gdwt_client;

    } gd_writethrough;

    struct {
      graphd_request *gdpt_client;

    } gd_passthrough;

  } greq_data;
};

typedef enum graphd_command graphd_command;

#define GRAPHD_TOKEN_MORE (-1)
#define GRAPHD_TOKEN_ERROR_MEMORY (-2)

typedef struct graphd_tokenizer {
  enum {
    GRAPHD_TS_INITIAL,
    GRAPHD_TS_ATOM,
    GRAPHD_TS_STRING_ESCAPED,
    GRAPHD_TS_STRING,
    GRAPHD_TS_CR,
    GRAPHD_TS_SKIP
  } ts_state;

  unsigned int ts_nesting_depth;

  cm_handle *ts_cm;
  cl_handle *ts_cl;

  char *ts_buf_s;
  size_t ts_buf_n;
  size_t ts_buf_m;

  unsigned char ts_char_class_current;

} graphd_tokenizer;

/*  Abstract iterator resource managed by the opportunistic iterator
 *  resource cache.
 */
#define GRAPHD_ITERATOR_RESOURCE_STAMP_SIZE 100

struct graphd_iterator_resource;
struct graphd_sabotage_handle;

typedef void graphd_iterator_resource_free(void *, void const *, size_t);
typedef struct graphd_iterator_resource {
  /*  NULL for non-storables, otherwise a
   *  graphd_storable_type **.
   */
  graphd_storable *gir_storable;
  struct graphd_iterator_resource *gir_storable_next;

  graphd_iterator_resource_free *gir_callback;
  void *gir_callback_data;

  unsigned int gir_used : 1;

  struct graphd_iterator_resource *gir_next, *gir_prev;

  /* Points to the entry in the resource stamp hashtable.
   */
  void *gir_stamp;

} graphd_iterator_resource;

struct graphd_handle {
  /* The argument to graphd -d
   */
  char const *g_dir_arg;
  graph_timestamp_t g_now;
  pdb_handle *g_pdb;

  char *g_interface_id;
  unsigned int g_predictable : 1;

  /*  A ring buffer whose contents that can be queried using gstatus.
   */
  cl_diary_handle *g_diary;
  cl_handle *g_diary_cl;

  /*  Global runtime statistics
   */
  unsigned long long g_rts_values_allocated;

  /*  Dateline for the whole system as seen from here.
   */
  graph_dateline *g_dateline;

  /*  Maximum dateline for which sessions are suspended and waiting.
   *  If this is PDB_ID_NONE, we don't need to look for suspended
   *  sessions.
   */
  pdb_id g_dateline_suspended_max;

  /*  A request pulls a number ("ticket") when it first requests
   *  to run.
   *
   *  All requests with a ticket less than or equal to the
   *  current number ("g_xstate_ticket_running") can run
   *  if the g_xstate is right for them (promiscuous for reads,
   *  exclusive for writes).
   *
   *  The ticket printer increments after printing an exclusive
   *  ticket.
   *
   *  The running ticket increments after the owner of an
   *  exclusive ticket completes its run.
   *
   *  The xstate switches from PROMISCUOUS to EXCLUSIVE when
   *  the n_running count drops to zero at the end of a turn.
   *  (All read requests have finished or voluntarily given
   *  up their turn.)
   */
  unsigned long long g_xstate_ticket_printer;
  graphd_xstate_ticket *g_xstate_head, *g_xstate_tail;

  /*  The number of connections that can currently run.
   */
  size_t g_xstate_n_running;

  /*  The number of connections that have a ticket, but can't
   *  yet run because the state isn't right or because their
   *  number hasn't come up yet.
   */
  size_t g_xstate_n_suspended;

  graphd_access_global g_access;

  enum {
    /* We're not in the middle of a checkpoint.
     */
    GRAPHD_CHECKPOINT_CURRENT,

    /* We are in the middle of a checkpoint.
     */
    GRAPHD_CHECKPOINT_PENDING,
  } g_checkpoint_state;

  /* The request that initiated the checkpoint (if any) */
  graphd_request *g_checkpoint_req;

  /*  A srv_delay posted by the checkpointing code to
   *  resume asynchronous writes.
   */
  srv_delay *g_checkpoint_delay;

  /*  GUIDs for types we use internally to translate between
   *  type names and type GUIDs.
   */
  graph_guid g_namespace_bootstrap, g_attribute_has_key, g_namespace_root,
      g_core_scope;

  /*  Maximum number of bytes in a request.
   */
  unsigned long long g_request_size_max;

  /* Number of worker processes to run. We are in
   * SMP mode if this is > 1
   */
  unsigned long long g_smp_processes;

  srv_handle *g_srv;

  /*  The argument to graphd -r
   */
  char const *g_rep_arg;

  /*  The argument to graphd -M
   */
  char const *g_rep_write_arg;

  /* Command line specified -S.
   */
  unsigned int g_nosync : 1;

  /* Command line specified -Z.
   */
  unsigned int g_should_delay_replica_writes : 1;
  size_t g_delay_replica_writes_secs;

  /* Command line specified -T.
   */
  unsigned int g_notransactional : 1;

  /* Have we fully started up yet?
   */
  unsigned int g_started : 1;

  /*  Test behaviors
   */
  unsigned int g_test_sleep_write : 1;
  unsigned int g_test_sleep_forever_write : 1;

  /* Are we still waiting to see a valid replica connection before
     declaring our startup successful?
   */
  bool g_require_replica_connection_for_startup;
  graphd_startup_todo_item g_startup_todo_replica_connection;
  bool g_startup_want_replica_connection;

  /* The head of the list of replica sessions
   */
  graphd_session *g_rep_sessions;

  /* Our connection to the replica master
   */
  graphd_session *g_rep_master;
  srv_address *g_rep_master_address;

  /* Placeholder for a long time out to allow us
   * to cut a replica connection if it's idle too long
   * (and then reconnect)
   */
  srv_timeout *g_rep_master_timeout;

  /* What type of graphd process are we? */

  graphd_smp_process_type g_smp_proc_type;
  graphd_session_smp_state g_smp_state;

  /* The head of the list of smp sessions
   */
  graphd_session *g_smp_sessions;

  /*  Requests waiting for SMP
   */
  graphd_request *g_smp_request;

  /*  The argument to graphd -U, the leader address
   */
  char const *g_leader_address_arg;

  /* Our connection to the smp master
   */
  graphd_session *g_smp_leader;
  graphd_session *g_smp_leader_passthrough;
  char *g_smp_leader_address;

  /* Our connection to the write master
   */
  graphd_session *g_rep_write;
  srv_address *g_rep_write_address;

  /* Delay the passing forward of replica writes with this delay
   * and horizon.
   */

  pdb_id g_rep_write_delay_horizon_start;
  pdb_id g_rep_write_delay_horizon_end;
  srv_delay *g_rep_write_delay;

  /* A callback created by the replication code
   * to attempt reconnection to the master.
   */
  srv_delay *g_rep_reconnect_delay;

  /* An analogus callback created by the smp code
   * to attempt reconnection to the leader.
   */
  srv_delay *g_smp_reconnect_delay;

  /* Set if we actually send the replica command.
   */
  unsigned int g_rep_replica_sent : 1;

  /* Set if we ever successfully connected to
   * a replica server
   */
  unsigned int g_rep_ever_connected : 1;

  /*  Maximum per-request cost allowance.
   */
  graphd_runtime_statistics g_runtime_statistics_allowance;

  /*  Is there an asynchronous write in progress?
   *
   *  Set when we first install an idle handler in the server;
   *  cleared when an optional update returns something other
   *  than GRAPHD_ERR_MORE.
   */
  unsigned int g_asynchronous_write_in_progress : 1;

  /*  Should we verify at startup?
   */
  unsigned int g_verify : 1;

  bool g_force;

  /* Should we create the database at startup (command line option for tests)
   */
  bool g_database_must_exist;

  /* Specifies the max memory parameter used when sizing a new database
   * on disk. The default is 0, which will then use sysinfo, sysctl, etc
   * to determine.
   */
  long long g_total_memory;

  /*  Just for tracking purposes, let's give each of these
   *  writes an ID, so we can track them more easily.
   */
  unsigned long g_asynchronous_write_id;

  cl_handle *g_cl;
  cm_handle *g_cm;
  graph_handle *g_graph;

  /* managed by graphd-islink*.c
   */
  graphd_islink_handle *g_islink;

  /* managed by graphd-startup.c
   */
  graphd_startup_todo_item *g_startup_todo_head, *g_startup_todo_tail;

  cm_hashtable g_iterator_resource_stamp;
  cm_hashtable g_iterator_resource;
  unsigned long long g_iterator_resource_id;
  unsigned long long g_iterator_resource_size;
  unsigned long long g_iterator_resource_max;
  graphd_iterator_resource *g_iterator_resource_head, *g_iterator_resource_tail;

  graphd_sabotage_handle *g_sabotage;

  /* Freeze-factor.  If non-0, freeze at every <g_freeze>th chance.
   */
  size_t g_freeze;

  /*  idle callbacks
   */
  graphd_idle_checkpoint_context g_idle_checkpoint;
  graphd_idle_islink_context g_idle_islink;

  char g_instance_id[GRAPH_INSTANCE_ID_SIZE + 1];

  /* A metric -- how many times have we cycled the followers?  */
  unsigned long long g_smp_cycles;

  /* A timeout for followers so that we can kill them */
  srv_timeout *g_smp_follower_timeout;

  /* A ticket held by the smp to hold off everybody else.
   */
  graphd_xstate_ticket *g_smp_xstate_ticket;

  /*  A map for a concentric graph.
   */
  graph_grmap *g_concentric;

  /*  The number of "read"- (or "iterate"-) suspends per minute
   */
  unsigned long long g_read_suspends_per_minute_timer;
  unsigned long g_read_suspends_per_minute;
  unsigned long g_read_suspends_per_minute_current;
};

typedef struct graphd_database_config {
  char const *dcf_path;
  char const *dcf_snap;
  char const *dcf_type;
  char const *dcf_id;
  pdb_configuration dcf_pdb_cf;

} graphd_database_config;

typedef struct graphd_replica_config {
  srv_address *rcf_master_address;
  bool rcf_archive;
} graphd_replica_config;

/*  Graphd-specific configuration file options.
 */
typedef struct graphd_config {
  unsigned int gcf_initialized;
  graphd_database_config *gcf_database_cf;
  graphd_replica_config *gcf_replica_cf;
  unsigned long long gcf_request_size_max;
  unsigned long long gcf_smp_processes;
  char const *gcf_smp_leader;
  graphd_runtime_statistics gcf_runtime_statistics_allowance;
  graphd_sabotage_config gcf_sabotage_cf;
  char gcf_instance_id[32];

} graphd_config;

/*  Things that a session may be suspended for.
 */
typedef enum {
  GRAPHD_SUSPEND_NOTHING = 0,
  GRAPHD_SUSPEND_XSTATE,
  GRAPHD_SUSPEND_WRITETHROUGH,
  GRAPHD_SUSPEND_SMP,
  GRAPHD_SUSPEND_DATELINE

} graphd_suspend_reason;

#define GRAPHD_SUSPEND_REASON_VALID(R__) \
  ((R__) >= GRAPHD_SUSPEND_NOTHING && (R__) <= GRAPHD_SUSPEND_DATELINE)

/*  Graphd-specific session state.
 */
struct graphd_session {
  /*  This must be the first element -- pointers to
   *  graphd sessions can be type-punned to srv_sessions.
   */
  srv_session gses_ses;

  enum {
    GRAPHD_SESSION_UNSPECIFIED,

    /* Your regular old server session.
     */
    GRAPHD_SESSION_SERVER,

    /* An SMP connection; the other side
     * is following us; we are its leader.
     */
    GRAPHD_SESSION_SMP_FOLLOWER,

    /* An SMP connection; the other side
     * is our leader; we are a follower.
     */
    GRAPHD_SESSION_SMP_LEADER,

    /* We are a replica master and send
     * write updates to this client.
     */
    GRAPHD_SESSION_REPLICA_CLIENT,

    /* We are a replica client and receive
     * write updates from this master.
     */
    GRAPHD_SESSION_REPLICA_MASTER,

  } gses_type;

  graphd_tokenizer gses_tokenizer;
  graphd_handle *gses_graphd;
  cl_handle *gses_cl;

  /*  Per session loglevel
   */
  cl_loglevel_configuration gses_loglevel;

  graph_timestamp_t gses_time_created;
  graph_timestamp_t gses_time_active;
  char const *gses_last_action;

  /*  If non-NULL, the session was suspended for a delay.
   */
  srv_delay *gses_delay;

  /* Why this session is suspended
   */
  graphd_suspend_reason gses_suspend_reason;

  /* The dateline id this session is waiting for
   */
  pdb_id gses_dateline_id;

  /* Requests waiting for room in this session's request queue.
   */
  graphd_request *gses_request_wait_head;
  graphd_request *gses_request_wait_tail;

  unsigned int gses_loglevel_valid : 1;
  unsigned int gses_skipping : 1;

  union {
    struct {
    } gd_rep_master;

    struct {
      /* linked list of replication clients
       */
      graphd_session *gdrc_next;
      graphd_session *gdrc_prev;

      /*  The next id which should be sent to the replica.
       */
      pdb_id gdrc_next_id;

    } gd_rep_client;

    struct {
      /* The state-machine variable of the SMP session
      */
      graphd_session_smp_state gdsf_smp_state;
      pid_t gdsf_smp_pid;

      /* linked list of SMP followers
       */
      graphd_session *gdsf_next;
      graphd_session *gdsf_prev;

    } gd_smp_follower;

    struct {
    } gd_smp_leader;
  } gses_data;
};

typedef struct graphd_iterator_cache {
  graphd_storable gic_storable;
  graphd_handle *gic_graphd;
  cm_handle *gic_cm;

  pdb_id *gic_id;
  size_t gic_n;
  size_t gic_m;

  pdb_budget gic_cost;
  pdb_budget gic_cost_total;
  pdb_budget gic_use_total;

  bool gic_eof;

} graphd_iterator_cache;

#define graphd_iterator_cache_n(gic) ((gic)->gic_n)

/* graphd.c */

void graphd_set_time(graphd_handle *);

/* graphd-suspend-a-read.c */

unsigned long graphd_suspend_a_read(graphd_handle *g, unsigned long long msnow,
                                    bool suspend);

/* graphd-access.c */

char const *graphd_access_global_to_string(graphd_access_global);
int graphd_access_set_global(graphd_handle *g, graphd_access_global acc,
                             bool *error_is_retriable, char *error_buf,
                             size_t error_buf_size);

bool graphd_access_allow_global(graphd_handle *_g, graphd_request *_greq);

graphd_access_global graphd_access_global_from_string(const char *s,
                                                      const char *e);

/* graphd-assignment.c */

int graphd_assignment_infer(graphd_request *greq, graphd_constraint *con);

graphd_assignment *graphd_assignment_by_declaration(
    graphd_constraint const *con, graphd_variable_declaration const *vdecl);

graphd_assignment *graphd_assignment_by_name(graphd_constraint const *con,
                                             char const *s, char const *e);

bool graphd_assignment_is_recursive(cl_handle *const cl,
                                    graphd_constraint const *const con,
                                    graphd_assignment const *const a);

void graphd_assignments_hash(cl_handle *const cl, graphd_assignment const *a,
                             unsigned long *const hash_inout);

bool graphd_assignments_equal(cl_handle *const cl,
                              graphd_constraint const *a_con,
                              graphd_assignment const *a,
                              graphd_constraint const *b_con,
                              graphd_assignment const *b);

graphd_assignment *graphd_assignment_alloc_declaration(
    graphd_request *greq, graphd_constraint *con,
    graphd_variable_declaration *vdecl);

graphd_assignment *graphd_assignment_alloc(graphd_request *greq,
                                           graphd_constraint *con,
                                           char const *s, char const *e);

int graphd_assignment_sort(graphd_request *greq, graphd_constraint *con);

int graphd_assignment_parenthesize(graphd_request *greq,
                                   graphd_constraint *con);

void graphd_assignment_dump(graphd_request *greq, graphd_constraint *con);

/* graphd-bad-cache.c */

void graphd_bad_cache_initialize(graphd_bad_cache *bc);
bool graphd_bad_cache_member(graphd_bad_cache const *bc, pdb_id id);
void graphd_bad_cache_add(graphd_bad_cache *bc, pdb_id id);

/* graphd-checkcache.c */

int graphd_check_cache_initialize(graphd_handle *g, graphd_check_cache *cc);

void graphd_check_cache_finish(graphd_handle *g, graphd_check_cache *cc);

int graphd_check_cache_add(graphd_handle *g, graphd_check_cache *cc, pdb_id id,
                           bool is_present);

int graphd_check_cache_test(graphd_handle *g, graphd_check_cache *cc, pdb_id id,
                            bool *is_present_out);

/* graphd-checkpoint.c */

int graphd_checkpoint_mandatory(graphd_handle *g);
int graphd_checkpoint_optional(graphd_handle *g);
int graphd_checkpoint_work(graphd_handle *g);
int graphd_checkpoint_rollback(graphd_handle *_g, unsigned long long _horizon);

/* graphd-client-replica.c */

int graphd_client_replica_send(graphd_handle *g, graphd_session *gses);

/* graphd-comparator.c */

int graphd_comparator_value_match(graphd_request *greq,
                                  graphd_string_constraint const *strcon,
                                  const char *s, const char *e,
                                  graphd_comparator const *cmp);

char const *graphd_comparator_to_string(graphd_comparator const *cmp);
graphd_comparator const *graphd_comparator_from_string(char const *s,
                                                       char const *e);

/* graphd-comparator-default.c */

extern graphd_comparator const graphd_comparator_default[],
    graphd_comparator_unspecified[];

bool graphd_comparator_default_prefix_word_next(char const *s, char const *e,
                                                char const **word_s,
                                                char const **word_e,
                                                bool *prefix,
                                                char const **state);

int graphd_comparator_default_iterator(
    graphd_request *_greq, graphd_string_constraint const *_strcon,
    pdb_iterator *_and_it, unsigned long long _low, unsigned long long _high,
    graphd_direction _direction, char const *_ordering, bool *_indexed_inout);

int graphd_comparator_default_name_iterator(
    graphd_request *_greq, graphd_string_constraint const *_strcon,
    pdb_iterator *_and_it, unsigned long long _low, unsigned long long _high,
    graphd_direction _direction, char const *_ordering, bool *_indexed_inout);

int graphd_value_default_iterator(
    graphd_request *greq, int operation, const char *s, const char *e,
    unsigned long long low, unsigned long long high, graphd_direction direction,
    char const *ordering, bool *indexed_inout, pdb_iterator **it_out);

int graphd_iterator_null_value_create(graphd_request *greq,
                                      unsigned long long low,
                                      unsigned long long high,
                                      pdb_iterator **it_out);

/* graphd-comparator-octet.c */

extern graphd_comparator const graphd_comparator_octet[1];

extern graphd_comparator const graphd_comparator_case[1];

extern graphd_comparator const graphd_comparator_number[1];

extern graphd_comparator const graphd_comparator_datetime[1];

/* graphd-constraint-path.c */

int graphd_constraint_path(cl_handle *_cl, graphd_constraint const *_con,
                           cm_buffer *_buf);

int graphd_constraint_path_lookup(graphd_request *_greq, char const *_name_s,
                                  char const *_name_e,
                                  graphd_constraint **_con_out);

/* graphd-constraint-signature.c */

#define GRAPHD_SIGNATURE_OMIT_CURSOR 0x01
#define GRAPHD_SIGNATURE_OMIT_COMMON_GUID 0x02

int graphd_constraint_signature(graphd_handle *_g,
                                graphd_constraint const *_con,
                                unsigned int _flags, cm_buffer *_out);

/* graphd-constraint-setsize.c */

void graphd_constraint_setsize_initialize(graphd_handle *_g,
                                          graphd_constraint *_con);

int graphd_constraint_setsize(graphd_handle *_g, graphd_constraint *_con);

/* graphd-constraint-to-string.c */

char const *graphd_constraint_strqueue_to_string(
    graphd_string_constraint_queue const *q, char const *name, char *buf,
    size_t size);

char const *graphd_constraint_guidset_to_string(graphd_guid_set const *_gs,
                                                char *_buf, size_t _size);

char const *graphd_constraint_linkage_to_string(int _linkage, char *_buf,
                                                size_t _size);
char const *graphd_constraint_meta_to_string(int _meta, char *_buf,
                                             size_t _size);

char const *graphd_constraint_to_string(graphd_constraint *_con);

char const *graphd_constraint_optimization_to_string(
    graphd_constraint const *_con, char *_buf, size_t _bufsize);

char const *graphd_constraint_flag_to_string(int flag, char *buf, size_t size);

/* graphd-constraint.c */

int graphd_constraint_get_heatmap(graphd_request const *_greq,
                                  graphd_constraint *_con, cm_buffer *_buf);

void graphd_constraint_account(graphd_request *_greq, graphd_constraint *_con,
                               pdb_iterator *_it);

graphd_constraint *graphd_constraint_by_id(graphd_request const *_greq,
                                           size_t _id);

int graphd_constraint_defaults(graphd_request *_greq, graphd_constraint *_con);

bool graphd_constraint_uses_pattern(graphd_constraint const *_con, int _pat);

bool graphd_constraint_uses_contents(graphd_constraint const *);
void graphd_constraint_hash(cl_handle *_cl, graphd_constraint const *_con,
                            unsigned long *_hash_inout);

bool graphd_constraint_equal(cl_handle *_cl, graphd_constraint const *_a,
                             graphd_constraint const *_b);

void graphd_constraint_free(graphd_request *_greq, graphd_constraint *_con);

unsigned int graphd_constraint_linkage_pattern(graphd_constraint const *);

int graphd_guid_constraint_intersect_with_guid(graphd_request *_greq,
                                               graphd_constraint *_con,
                                               graphd_guid_constraint *_guidcon,
                                               graph_guid const *_guid);

void graphd_constraint_initialize(graphd_handle *_g, graphd_constraint *_con);

void graphd_constraint_append(graphd_constraint *_parent,
                              graphd_constraint *_child);

bool graphd_constraint_dateline_too_young(graphd_handle *_g,
                                          graphd_constraint *_con, pdb_id _id);

pdb_id graphd_constraint_dateline_first(graphd_handle *_g,
                                        graphd_constraint *_con);

int graphd_constraint_use_result_instruction(graphd_request *greq,
                                             graphd_constraint const *con,
                                             graphd_pattern const *pat);

/* graphd-constraint-clause.c */

char const *graphd_constraint_clause_to_string(
    graphd_constraint_clause const *cc, char *buf, size_t size);

graphd_constraint_clause *graphd_constraint_clause_alloc(graphd_request *greq,
                                                         int type);

graphd_constraint_clause *graphd_constraint_clause_alloc_cursor(
    graphd_request *greq, char const *s, char const *e);

graphd_constraint_clause *graphd_constraint_clause_alloc_assignment(
    graphd_request *greq, char const *s, char const *e, graphd_pattern *pat);

graphd_constraint_clause *graphd_constraint_clause_alloc_sequence(
    graphd_request *greq, graphd_constraint_clause **head,
    graphd_constraint_clause *tail);

void graphd_constraint_clause_append(graphd_constraint *con,
                                     graphd_constraint_clause *cc);

int graphd_constraint_clause_merge(graphd_request *greq, graphd_constraint *con,
                                   graphd_constraint_clause *cc);

int graphd_constraint_clause_merge_all(graphd_request *greq,
                                       graphd_constraint *con);

/* graphd-constraint-cursor.c */

void graphd_constraint_cursor_mark_usable(graphd_request *_greq,
                                          graphd_constraint *_con);

int graphd_constraint_cursor_scan_prefix(graphd_request *_greq,
                                         graphd_constraint *_con,
                                         char const **_s_ptr, char const *_e);

int graphd_constraint_cursor_thaw(graphd_request *_greq,
                                  graphd_constraint *_con,
                                  pdb_iterator **_it_out);

int graphd_constraint_cursor_from_iterator(graphd_request *_greq,
                                           graphd_constraint *_con,
                                           char const *_prefix,
                                           pdb_iterator *_it,
                                           graphd_value *_val_out);

/* graphd-constraint-or.c */

int graphd_constraint_or_move_declarations(graphd_request *greq,
                                           graphd_constraint *arch,
                                           graphd_constraint *con);

void graphd_constraint_or_move_assignments(graphd_request *greq,
                                           graphd_constraint *arch,
                                           graphd_constraint *con);

int graphd_constraint_or_declare(graphd_request *greq, graphd_constraint *orcon,
                                 char const *name_s, char const *name_e,
                                 graphd_variable_declaration **lhs_vdecl_out,
                                 graphd_variable_declaration **new_vdecl_out);

int graphd_constraint_or_compile_declaration(
    graphd_request *_greq, graphd_constraint *_arch,
    graphd_variable_declaration *_old_vdecl,
    graphd_variable_declaration **_new_vdecl);

graphd_constraint *graphd_constraint_or_prototype_root(
    graphd_constraint const *con);

int graphd_constraint_or_default_from_prototype(graphd_request *greq,
                                                graphd_constraint *prototype,
                                                graphd_constraint *sub);

size_t graphd_constraint_or_index(graphd_request *_greq,
                                  graphd_constraint *_con, size_t _n);

int graphd_constraint_or_complete_parse(graphd_request *_greq,
                                        graphd_constraint *_prototype,
                                        graphd_constraint *_sub);

graphd_constraint_or *graphd_constraint_or_below(
    graphd_constraint const *_prototype, graphd_constraint const *_sub);

graphd_constraint_or *graphd_constraint_or_create(graphd_request *_greq,
                                                  graphd_constraint *_prototype,
                                                  bool _short_circuit);

void graphd_constraint_or_append_to_prototype(graphd_constraint *_prototype,
                                              graphd_constraint_or *_new_or);

/* graphd-constraint-iterator.c */

int graphd_constraint_iterator(graphd_request *_greq, graphd_constraint *_con);

/* graphd-cost.c */

char const *graphd_cost_limit_to_string(graphd_runtime_statistics const *_rts,
                                        char *_buf, size_t _size);

unsigned long long *graphd_cost_to_address(graphd_runtime_statistics *_rts,
                                           char const *_name_s,
                                           char const *_name_e);

int graphd_cost_config_read(void *_data, srv_handle *_srv, void *_config_data,
                            srv_config *_srv_cf, char **_s, char const *_e);

int graphd_cost_config_open(void *_data, srv_handle *_srv, void *_config_data,
                            srv_config *_srv_cf);

void graphd_cost_set(graphd_handle *_g, graphd_runtime_statistics const *_grt);

int graphd_cost_from_string(graphd_runtime_statistics *_rts, char const *_s,
                            char const *_e, char *_errbuf, size_t _errsize);

/* graphd-cost-parse.c */

void graphd_cost_parse(graphd_request *greq, gdp_token const *tok,
                       graphd_runtime_statistics *a);

/* graphd-database.c */

int graphd_database_config_read(void *_data, srv_handle *_srv,
                                void *_config_data, srv_config *_srv_config,
                                char **_s, char const *_e);

int graphd_database_config_open(void *_data, srv_handle *_srv,
                                void *_config_data, srv_config *_srv_config);

int graphd_database_config_run(void *_data, srv_handle *_srv,
                               void *_config_data, srv_config *_srv_config);

int graphd_database_option_set(void *_data, srv_handle *_srv, cm_handle *_cm,
                               int _opt, char const *_optarg);

int graphd_database_option_configure(void *_data, srv_handle *_srv,
                                     void *_config_data,
                                     srv_config *_srv_config);

int graphd_nosync_option_set(void *data, srv_handle *_srv, cm_handle *cm,
                             int opt, char const *opt_arg);

int graphd_notransactional_option_set(void *data, srv_handle *_srv,
                                      cm_handle *cm, int opt,
                                      char const *opt_arg);

int graphd_database_total_memory_set(void *data, srv_handle *srv, cm_handle *cm,
                                     int opt, char const *opt_arg);

/* graphd-dateline.c */

void graphd_dateline_constraint_hash(cl_handle *_cl,
                                     graphd_dateline_constraint const *_condat,
                                     unsigned long *_hash_inout);

graph_dateline *graphd_dateline(graphd_handle *);
void graphd_dateline_expire(graphd_handle *);

pdb_id graphd_dateline_low(graphd_handle const *_g,
                           graphd_constraint const *_con);

pdb_id graphd_dateline_high(graphd_handle const *_g,
                            graphd_constraint const *_con);

/* graphd-dump.c */

void graphd_dump_initialize(graphd_request *greq);

/* graphd-guid-constraint.c */

bool graphd_guid_constraint_single_linkage(graphd_constraint const *_con,
                                           int _linkage, graph_guid *_guid_out);

void graphd_guid_constraint_initialize(graphd_guid_constraint *);
int graphd_guid_constraint_merge(graphd_request *_greq, graphd_constraint *_con,
                                 graphd_guid_constraint *_accu,
                                 graphd_operator _op, graphd_guid_set *_gs);

bool graphd_guid_constraint_generational_equal(
    cl_handle *const _cl, graphd_generational_constraint const *_a,
    graphd_generational_constraint const *_b);

void graphd_guid_constraint_generational_hash(
    cl_handle *_cl, graphd_generational_constraint const *_gencon,
    unsigned long *h_ash_inout);

int graphd_guid_constraint_convert(graphd_request *_greq,
                                   graphd_constraint *_con, bool _is_read);

bool graphd_guid_constraint_equal(cl_handle *_cl,
                                  graphd_guid_constraint const *_a,
                                  graphd_guid_constraint const *_b);

void graphd_guid_constraint_hash(cl_handle *_cl,
                                 graphd_guid_constraint const *_guidcon,
                                 unsigned long *_hash_inout);

/* graphd-guid-set.c */

void graphd_guid_set_dump(cl_handle *, graphd_guid_set const *);
bool graphd_guid_set_match(graphd_guid_set const *, graph_guid const *);

void graphd_guid_set_initialize(graphd_guid_set *gs);
bool graphd_guid_set_contains_null(graphd_guid_set const *);
void graphd_guid_set_move(graphd_guid_set *_dst, graphd_guid_set *_src);

size_t graphd_guid_set_find(graphd_guid_set const *_gs,
                            graph_guid const *_guid);

bool graphd_guid_set_delete(graphd_guid_set *_gs, graph_guid const *_guid);

int graphd_guid_set_add(graphd_request *_greq, graphd_guid_set *_gs,
                        graph_guid const *_guid);

int graphd_guid_set_add_generations(graphd_request *_greq,
                                    graph_guid const *_guid,
                                    unsigned long _gen_i, unsigned long _gen_n,
                                    graphd_guid_set *_gs);

int graphd_guid_set_convert_generations(graphd_request *_greq,
                                        graphd_constraint *_con, bool is_guid,
                                        graphd_guid_set *_out);

int graphd_guid_set_normalize_match(graphd_request *_greq,
                                    graphd_guid_set *_gs);

int graphd_guid_set_filter_match(graphd_request *_greq, graphd_constraint *_con,
                                 graphd_guid_set *_accu, graphd_guid_set *_fil);

bool graphd_guid_set_equal(cl_handle *_cl, graphd_guid_set const *_a,
                           graphd_guid_set const *_b);

void graphd_guid_set_hash(cl_handle *_cl, graphd_guid_set const *_gs,
                          unsigned long *_hash_inout);

int graphd_guid_set_intersect(graphd_request *_greq, graphd_constraint *_con,
                              bool _postpone, graphd_guid_set *_accu,
                              graphd_guid_set *_in);

bool graphd_guid_set_subtract(graphd_request *_greq, graphd_guid_set *_accu,
                              graphd_guid_set const *_in);

int graphd_guid_set_union(graphd_request *_greq, graphd_guid_set *_accu,
                          graphd_guid_set *_in);

/* graphd-idle.c */

void graphd_idle_initialize(graphd_handle *g);
void graphd_idle_finish(graphd_handle *g);

int graphd_idle_install_checkpoint(graphd_handle *);
int graphd_idle_install_islink(graphd_handle *);

/* graphd-interface-id.c */

char const *graphd_interface_id(graphd_handle *);

/* graphd-instace-id.c */

int graphd_instance_id_config_read(void *data, srv_handle *srv,
                                   void *config_data, srv_config *srv_config,
                                   char **s, char const *e);

int graphd_instance_id_config_open(void *data, srv_handle *srv,
                                   void *config_data, srv_config *srv_cf);

/* graphd-islink.c */

int graphd_islink_examine_constraint(graphd_request *_greq,
                                     graphd_constraint const *_con);
int graphd_islink(graphd_request *_greq, unsigned long long _deadline);
int graphd_islink_initialize(graphd_handle *);
void graphd_islink_finish(graphd_handle *);
int graphd_islink_truncate(graphd_handle *);
int graphd_islink_idle(graphd_handle *);
int graphd_islink_donate(graphd_handle *, pdb_budget *);
int graphd_islink_add_type_guid(graphd_handle *, graph_guid const *);
int graphd_islink_add_type_id(graphd_handle *, pdb_id);
int graphd_islink_type_guid_donate(graphd_handle *_g,
                                   graph_guid const *_type_guid,
                                   pdb_budget *_budget_inout);
int graphd_islink_type_id_donate(graphd_handle *_g, pdb_id _type_id,
                                 pdb_budget *_budget_inout);

/* graphd-islink-key.c */

graphd_islink_key *graphd_islink_key_make(graphd_handle *g, int result_linkage,
                                          pdb_id type_id, pdb_id endpoint_id,
                                          graphd_islink_key *buf);
/* graphd-islink-status.c */

int graphd_islink_status(graphd_request *_greq, graphd_value *_val);

/* graphd-iterate.c */

struct graphd_read_base;
void graphd_iterate_constraint_push(graphd_request *greq,
                                    graphd_constraint *con,
                                    struct graphd_read_base *grb,
                                    graphd_value *contents_out, int *err_out);

/* graphd-iterator.c */

int graphd_iterator_quick_intersect_estimate(graphd_handle *g, pdb_iterator *a,
                                             pdb_iterator *b,
                                             pdb_budget *budget_inout,
                                             unsigned long long *n_out);

void graphd_iterator_set_direction_ordering(pdb_handle *_pdb, pdb_iterator *_it,
                                            graphd_direction _dir,
                                            char const *_ordering);

char graphd_iterator_direction_to_char(graphd_direction dir);
graphd_direction graphd_iterator_direction_from_char(int dirchar);

char const *graphd_iterator_ordering_internalize_request(graphd_request *_greq,
                                                         char const *_ord_s,
                                                         char const *_ord_e);

char const *graphd_iterator_ordering_internalize(graphd_handle *_g,
                                                 pdb_iterator_base *_pib,
                                                 char const *_ord_s,
                                                 char const *_ord_e);

int graphd_iterator_freeze(graphd_handle *_g, pdb_iterator *_it,
                           cm_buffer *_buf);

int graphd_iterator_substitute(graphd_request *_greq, pdb_iterator *_dest,
                               pdb_iterator *_source);

int graphd_iterator_thaw_statistics(
    cl_handle *cl, char const *who, char const **s_ptr, char const *e,
    unsigned long long upper_limit, cl_loglevel loglevel,
    bool *have_statistics_out, pdb_budget *check_cost_out,
    pdb_budget *next_cost_out, pdb_budget *find_cost_out,
    unsigned long long *n_out);

int graphd_iterator_thaw_loc(graphd_handle *g, pdb_iterator_text const *pit,
                             pdb_iterator_base *pib, graphd_iterator_hint hints,
                             cl_loglevel loglevel, pdb_iterator **it_out,
                             graphd_iterator_hint *hints_out, char const *file,
                             int line);
#define graphd_iterator_thaw(a, b, c, d, e, f, g) \
  graphd_iterator_thaw_loc(a, b, c, d, e, f, g, __FILE__, __LINE__)

int graphd_iterator_thaw_bytes_loc(graphd_request *greq, char const *s,
                                   char const *e, graphd_iterator_hint hints,
                                   cl_loglevel loglevel, pdb_iterator **it_out,
                                   char const *file, int line);
#define graphd_iterator_thaw_bytes(a, b, c, d, e, f) \
  graphd_iterator_thaw_bytes_loc(a, b, c, d, e, f, __FILE__, __LINE__)

int graphd_iterator_intersect(graphd_handle *_graphd, pdb_iterator *_a,
                              pdb_iterator *_b, unsigned long long _low,
                              unsigned long long _high, bool _forward,
                              bool _error_if_null, pdb_budget *_budget_inout,
                              pdb_iterator **_it_out);

int graphd_iterator_hard_clone(graphd_request *greq, pdb_iterator *it,
                               pdb_iterator **it_out);

int graphd_iterator_util_freeze_subiterator(pdb_handle *pdb, pdb_iterator *it,
                                            unsigned int flags, cm_buffer *buf);

int graphd_iterator_util_thaw_subiterator(graphd_handle *g, char const **s_ptr,
                                          char const *e, pdb_iterator_base *pib,
                                          cl_loglevel loglevel,
                                          pdb_iterator **it_out);

int graphd_iterator_util_thaw_partial_subiterator(
    graphd_handle *_g, char const **_s_ptr, char const *_e, int _flags,
    pdb_iterator_text const *_pit, pdb_iterator_base *_pib,
    cl_loglevel _loglevel, pdb_iterator **_it_out);

int graphd_iterator_util_freeze_position(pdb_handle *_pdb, bool _eof,
                                         pdb_id _last_id, pdb_id _resume_id,
                                         cm_buffer *_buf);

int graphd_iterator_util_thaw_position(pdb_handle *_pdb, char const **_s_ptr,
                                       char const *_e, cl_loglevel _loglevel,
                                       bool *_eof, pdb_id *_last_id,
                                       pdb_id *_resume_id);

int graphd_iterator_save_original(graphd_handle *g, pdb_iterator_base *pib,
                                  pdb_iterator *it,
                                  pdb_iterator_base **pib_out);

int graphd_iterator_get_original(graphd_handle *g, pdb_iterator_base *pib,
                                 pdb_iterator_text const *pit,
                                 pdb_iterator **it_out);

int graphd_iterator_remove_saved_original(graphd_handle *g, pdb_iterator *it,
                                          pdb_iterator_base **pib_inout);

/* graphd-iterator-dump.c */

int graphd_iterator_dump(graphd_request *_greq, pdb_iterator *_it,
                         graphd_value *_val);

/* graphd-iterator-fixed.c */

int graphd_iterator_fixed_thaw_loc(
    graphd_handle *_graphd, pdb_iterator_text const *_pit,
    pdb_iterator_base *_pib, graphd_iterator_hint _hints, cl_loglevel _loglevel,
    pdb_iterator **_it_out, char const *_file, int _line);
#define graphd_iterator_fixed_thaw(a, b, c, d, e, f) \
  graphd_iterator_fixed_thaw_loc(a, b, c, d, e, f, __FILE__, __LINE__)

bool graphd_iterator_fixed_is_instance(pdb_handle *pdb, pdb_iterator *it,
                                       pdb_id **values_out, size_t *n_out);

int graphd_iterator_fixed_create_loc(graphd_handle *g, size_t n,
                                     unsigned long long low,
                                     unsigned long long high, bool forward,
                                     pdb_iterator **it_out, char const *file,
                                     int line);
#define graphd_iterator_fixed_create(a, b, c, d, e, f) \
  graphd_iterator_fixed_create_loc(a, b, c, d, e, f, __FILE__, __LINE__)

int graphd_iterator_fixed_add_id(pdb_iterator *, pdb_id);
void graphd_iterator_fixed_create_commit(pdb_iterator *);
void graphd_iterator_fixed_create_commit_n(pdb_iterator *it, size_t n,
                                           bool sorted);
int graphd_iterator_fixed_create_array(graphd_handle *_g, pdb_id const *_array,
                                       size_t _array_n, unsigned long long _low,
                                       unsigned long long _high, bool _forward,
                                       pdb_iterator **_it_out);
#define graphd_iterator_fixed_create_array(a, b, c, d, e, f, g)         \
  graphd_iterator_fixed_create_array_loc(a, b, c, d, e, f, g, __FILE__, \
                                         __LINE__)

int graphd_iterator_fixed_create_array_loc(graphd_handle *g,
                                           pdb_id const *array, size_t array_n,
                                           unsigned long long low,
                                           unsigned long long high,
                                           bool forward, pdb_iterator **it_out,
                                           char const *file, int line);

int graphd_iterator_fixed_create_guid_array(
    graphd_handle *_g, graph_guid const *_array, size_t _array_n,
    unsigned long long _low, unsigned long long _high, bool _forward,
    pdb_iterator **_it_out);

int graphd_iterator_fixed_create_fast_intersect(
    graphd_handle *_g, pdb_iterator *_a, pdb_iterator *_b,
    unsigned long long _low, unsigned long long _high, bool _forward,
    pdb_iterator **_it_out);

int graphd_iterator_fixed_create_from_iterator(graphd_request *_greq,
                                               pdb_iterator *_it,
                                               char const *_mas,
                                               pdb_iterator **_it_out);

int graphd_iterator_fixed_intersect(cl_handle *_cl, pdb_id *_a_base,
                                    size_t _a_n, pdb_id *_b_base, size_t _b_n,
                                    pdb_id *_id_inout, size_t *_id_n,
                                    size_t _id_m);

int graphd_iterator_fixed_set_masquerade(pdb_iterator *_it, char const *_mas);
int graphd_iterator_fixed_set_offset(pdb_handle *pdb, pdb_iterator *it,
                                     unsigned long long off);

/* graphd-iterator-and.c */

#define graphd_iterator_and_create(a, b, c, d, e, f, g) \
  graphd_iterator_and_create_loc(a, b, c, d, e, f, g, __FILE__, __LINE__)
int graphd_iterator_and_create_loc(graphd_request *_greq, size_t _n,
                                   unsigned long long _low,
                                   unsigned long long _high,
                                   graphd_direction _direction,
                                   char const *_ordering,
                                   pdb_iterator **_it_out, char const *_file,
                                   int _line);

int graphd_iterator_and_add_subcondition(graphd_handle *_g, pdb_iterator *_and,
                                         pdb_iterator **_it);

void graphd_iterator_and_set_context_pagesize(graphd_handle *_g,
                                              pdb_iterator *_it,
                                              unsigned long long _size);

void graphd_iterator_and_set_context_setsize(graphd_handle *_g,
                                             pdb_iterator *_it,
                                             unsigned long long _size);

int graphd_iterator_and_create_commit(graphd_handle *_g, pdb_iterator *_it);

#define graphd_iterator_and_thaw(a, b, c, d, e, f) \
  graphd_iterator_and_thaw_loc(a, b, c, d, e, f, __FILE__, __LINE__)

int graphd_iterator_and_thaw_loc(graphd_handle *_graphd,
                                 pdb_iterator_text const *_pit,
                                 pdb_iterator_base *_pib,
                                 graphd_iterator_hint _hint,
                                 cl_loglevel _loglevel, pdb_iterator **_it_out,
                                 char const *_file, int _line);

bool graphd_iterator_and_is_instance(pdb_handle *_pdb, pdb_iterator *_it,
                                     size_t *_n_out, size_t *_producer_out);

int graphd_iterator_and_get_subconstraint(pdb_handle *_pdb, pdb_iterator *_it,
                                          size_t _i, pdb_iterator **_sub_out);

int graphd_iterator_and_cheapest_subiterator(graphd_request *greq,
                                             pdb_iterator *it_and,
                                             unsigned long min_size,
                                             pdb_iterator **it_out, int *gia_i);

/* graphd-iterator-cache.c */

graphd_iterator_cache *graphd_iterator_cache_create(graphd_handle *_g,
                                                    size_t _m);

void graphd_iterator_cache_destroy(graphd_iterator_cache *_gic);

void graphd_iterator_cache_dup(graphd_iterator_cache *gic);

int graphd_iterator_cache_thaw(graphd_handle *_g, char const **_s,
                               char const *_e, cl_loglevel _loglevel,
                               graphd_iterator_cache **_gic);

int graphd_iterator_cache_rethaw(graphd_handle *_g, char const **_s_ptr,
                                 char const *_e, cl_loglevel _loglevel,
                                 graphd_iterator_cache **_gic);

pdb_budget graphd_iterator_cache_cost(graphd_iterator_cache *_gic);

void graphd_iterator_cache_eof(graphd_iterator_cache *_gic);

int graphd_iterator_cache_add(graphd_iterator_cache *_gic, pdb_id _id,
                              pdb_budget _id_cost);

int graphd_iterator_cache_search(pdb_handle *_pdb, pdb_iterator *_it,
                                 graphd_iterator_cache *_gic, pdb_id *_id_inout,
                                 size_t *_off_out);

int graphd_iterator_cache_check(pdb_handle *_pdb, pdb_iterator *_it,
                                graphd_iterator_cache *_gic, pdb_id _id);

int graphd_iterator_cache_index(graphd_iterator_cache *_gic, size_t _offset,
                                pdb_id *_id_out, pdb_budget *_budget_inout);

int graphd_iterator_cache_frozen_initialize(graphd_handle *);
void graphd_iterator_cache_frozen_finish(graphd_handle *);
int graphd_iterator_cache_freeze(graphd_handle *_g, graphd_iterator_cache *_gic,
                                 cm_buffer *_buf);

/* graphd-iterator-idset.c */

#define graphd_iterator_idset_create(a, b, c, d, e, f, g, h, i, j, k, l, m) \
  graphd_iterator_idset_create_loc(a, b, c, d, e, f, g, h, i, j, k, l, m,   \
                                   __FILE__, __LINE__)

int graphd_iterator_idset_create_loc(
    graphd_handle *g, unsigned long long low, unsigned long long high,
    bool forward, graph_idset *set, char const *frozen_set,
    pdb_primitive_summary const *psum,
    int (*recover_callback)(void *, graphd_handle *, graph_idset **,
                            pdb_budget *),
    void *recover_callback_data,
    void (*finish_callback)(void *, graphd_handle *, graph_idset *),
    void *finish_callback_data, pdb_iterator **it_out, char const *file,
    int line);

#define graphd_iterator_idset_position_thaw(a, b, c, d) \
  graphd_iterator_idset_position_thaw_loc(a, b, c, d, __FILE__, __LINE__)

int graphd_iterator_idset_position_thaw_loc(graphd_handle *graphd,
                                            pdb_iterator *it,
                                            pdb_iterator_text const *pit,
                                            cl_loglevel loglevel,
                                            char const *file, int line);

void graphd_iterator_idset_recover(graphd_handle *graphd, pdb_iterator *it,
                                   int (*recover_callback)(void *,
                                                           graphd_handle *,
                                                           graph_idset **,
                                                           pdb_budget *),
                                   void *recover_callback_data);

/* graphd-iterator-isa.c */

#define graphd_iterator_isa_create(a, b, c, d, e, f, g, h, i) \
  graphd_iterator_isa_create_loc(a, b, c, d, e, f, g, h, i, __FILE__, __LINE__)
int graphd_iterator_isa_create_loc(graphd_request *_greq, int _linkage,
                                   pdb_iterator **_sub, unsigned long long _low,
                                   unsigned long long _high,
                                   graphd_direction _direction,
                                   char const *_ordering,
                                   graphd_iterator_isa_hint _isa_hint,
                                   pdb_iterator **_it_out, char const *_file,
                                   int _line);

#define graphd_iterator_isa_thaw(a, b, c, d, e, f) \
  graphd_iterator_isa_thaw_loc(a, b, c, d, e, f, __FILE__, __LINE__)
int graphd_iterator_isa_thaw_loc(graphd_handle *_graphd,
                                 pdb_iterator_text const *_pit,
                                 pdb_iterator_base *_pib,
                                 graphd_iterator_hint _hint,
                                 cl_loglevel _loglevel, pdb_iterator **_it_out,
                                 char const *_file, int _line);

bool graphd_iterator_isa_is_instance(pdb_handle *pdb, pdb_iterator *it,
                                     int *linkage_out, pdb_iterator **sub_out);

/* graphd-iterator-islink.c */

int graphd_iterator_islink_thaw_loc(graphd_handle *g,
                                    pdb_iterator_text const *pit,
                                    pdb_iterator_base *pib,
                                    cl_loglevel loglevel, pdb_iterator **it_out,
                                    char const *file, int line);

int graphd_iterator_islink_create_loc(graphd_handle *g, unsigned long long low,
                                      unsigned long long high, bool forward,
                                      graphd_islink_key const *key,
                                      pdb_iterator **it_out, char const *file,
                                      int line);

#define graphd_iterator_islink_create(a, b, c, d, e, f) \
  graphd_iterator_islink_create_loc(a, b, c, d, e, f, __FILE__, __LINE__)

/* graphd-iterator-linksto.c */

#define graphd_iterator_linksto_create(a, b, c, d, e, f, g, h, i, j)         \
  graphd_iterator_linksto_create_loc(a, b, c, d, e, f, g, h, i, j, __FILE__, \
                                     __LINE__)
int graphd_iterator_linksto_create_loc(
    graphd_request *_greq, int _linkage, int _hint_linkage,
    graph_guid const *_hint_guid, pdb_iterator **_sub, unsigned long long _low,
    unsigned long long _high, graphd_direction _direction,
    char const *_ordering, pdb_iterator **_it_out, char const *_file,
    int _line);

#define graphd_iterator_linksto_thaw(a, b, c, d, e, f) \
  graphd_iterator_linksto_thaw_loc(a, b, c, d, e, f, __FILE__, __LINE__)
int graphd_iterator_linksto_thaw_loc(
    graphd_handle *_graphd, pdb_iterator_text const *_pit,
    pdb_iterator_base *_pib, graphd_iterator_hint _hint, cl_loglevel _loglevel,
    pdb_iterator **_it_out, char const *_file, int _line);

bool graphd_iterator_linksto_is_instance(pdb_handle *_pdb, pdb_iterator *_it,
                                         int *_linkage_out,
                                         pdb_iterator **_sub_out);

/* graphd-iterator-or.c */

bool graphd_iterator_or_is_instance(pdb_handle *_pdb, pdb_iterator *_it,
                                    size_t *_n_out);

int graphd_iterator_or_get_subconstraint(pdb_handle *_pdb, pdb_iterator *_it,
                                         size_t _i, pdb_iterator **_sub_out);

#define graphd_iterator_or_create(a, b, c, d) \
  graphd_iterator_or_create_loc(a, b, c, d, __FILE__, __LINE__)
int graphd_iterator_or_create_loc(graphd_request *_greq, size_t _n,
                                  bool _forward, pdb_iterator **_it_out,
                                  char const *_file, int _line);

int graphd_iterator_or_create_commit(pdb_iterator *it);

int graphd_iterator_or_add_subcondition(pdb_iterator *_gio_it,
                                        pdb_iterator **_sub_it);

#define graphd_iterator_or_thaw(a, b, c, d, e) \
  graphd_iterator_or_thaw_loc(a, b, c, d, e, __FILE__, __LINE__)
int graphd_iterator_or_thaw_loc(graphd_handle *_graphd,
                                pdb_iterator_text const *_pit,
                                pdb_iterator_base *_pib, cl_loglevel _loglevel,
                                pdb_iterator **_it_out, char const *_file,
                                int _line);

bool graphd_iterator_or_is_vip_type(pdb_handle *_pdb, pdb_iterator *_it,
                                    pdb_id *_type_id_out);

int graphd_iterator_or_set_masquerade(pdb_iterator *_it, char const *_mas);

int graphd_iterator_or_set_check(pdb_iterator *_it, pdb_iterator **_check_it);

/* graphd-iterator-prefix.c */

int graphd_iterator_prefix_create(graphd_request *_greq, char const *_s,
                                  char const *_e, unsigned long long _low,
                                  unsigned long long _high,
                                  graphd_direction _direction,
                                  pdb_iterator **_it_out);

int graphd_iterator_prefix_thaw(graphd_handle *_graphd,
                                pdb_iterator_text const *_pit,
                                pdb_iterator_base *_pib, cl_loglevel _loglevel,
                                pdb_iterator **_it_out);

int graphd_iterator_prefix_is_instance(pdb_handle *pdb, pdb_iterator *it,
                                       char const **s_out, char const **e_out);

int graphd_iterator_prefix_or(pdb_handle *pdb, pdb_iterator *it,
                              pdb_iterator **sub_out);

/* graphd-iterator-resource.c */

graphd_iterator_resource *graphd_iterator_resource_storable_lookup(
    graphd_handle *_g, graphd_storable const *_iss);

int graphd_iterator_resource_store(graphd_handle *_g, graphd_storable *_data,
                                   char *_stamp_buf, size_t _stamp_size);

int graphd_iterator_resource_initialize(graphd_handle *g);
void graphd_iterator_resource_finish(graphd_handle *g);
void *graphd_iterator_resource_thaw(graphd_handle *g, char const **s_ptr,
                                    char const *e,
                                    graphd_storable_type const *expected_type);

/* graphd-iterator-sort.c */

#define graphd_iterator_sort_create(a, b, c, d) \
  graphd_iterator_sort_create_loc(a, b, c, d, __FILE__, __LINE__)

int graphd_iterator_sort_create_loc(graphd_request *_greq, bool _forward,
                                    pdb_iterator **_sub, pdb_iterator **_it_out,
                                    char const *_file, int _line);

int graphd_iterator_sort_thaw_loc(graphd_handle *_graphd,
                                  pdb_iterator_text const *_pit,
                                  pdb_iterator_base *_pib,
                                  cl_loglevel _loglevel, pdb_iterator **_it_out,
                                  char const *_file, int _line);

/* graphd-iterator-state.c */

int graphd_iterator_state_store(graphd_handle *_g, cm_buffer *_in,
                                size_t _offset);

int graphd_iterator_state_restore(graphd_handle *_g, char const **_state_s,
                                  char const **_state_e);

/* graphd-iterator-vip.c */

int graphd_iterator_vip_create(graphd_handle *_graphd, pdb_id _source_id,
                               int _linkage, pdb_id _type_id,
                               graph_guid const *_type_guid,
                               unsigned long long _low,
                               unsigned long long _high, bool _forward,
                               bool _error_if_nulll, pdb_iterator **_it_out);

int graphd_iterator_vip_thaw(graphd_handle *_graphd,
                             pdb_iterator_text const *_pit,
                             pdb_iterator_base *_pib, cl_loglevel _loglevel,
                             pdb_iterator **_it_out);

bool graphd_iterator_vip_is_instance(pdb_handle *pdb, pdb_iterator *it);

int graphd_iterator_vip_linkage(pdb_handle *pdb, pdb_iterator *it);

pdb_id graphd_iterator_vip_type_id(pdb_handle *pdb, pdb_iterator *it);

pdb_id graphd_iterator_vip_source_id(pdb_handle *pdb, pdb_iterator *it);

bool graphd_iterator_vip_is_fixed_instance(pdb_handle *_pdb, pdb_iterator *_it,
                                           pdb_id **_values_out,
                                           size_t *_n_out);

/*
 * graphd-iterator-vrange.c
 */

int graphd_iterator_vrange_create(
    graphd_request *greq, const char *lo_s, const char *lo_e, bool lo_strict,
    const char *hi_s, const char *hi_e, bool hi_strict, unsigned long long low,
    unsigned long long high, bool value_forward,
    const graphd_comparator *cmp_type, const char *ordering,
    pdb_iterator *iterator_and, pdb_iterator **it_out);

int graphd_iterator_vrange_thaw(graphd_handle *g, pdb_iterator_text const *pit,
                                pdb_iterator_base *pib, cl_loglevel loglevel,
                                pdb_iterator **it_out);

bool graphd_vrange_forward(graphd_request *greq, graphd_value_range *vr);

int graphd_vrange_value_in_range(graphd_request *greq, pdb_iterator *vit,
                                 const char *s, const char *e,
                                 bool *string_in_range);

/* graphd-iterator-without.c */

int graphd_iterator_without_thaw(graphd_handle *_graphd,
                                 pdb_iterator_text const *_pit,
                                 pdb_iterator_base *_pib,
                                 graphd_iterator_hint _hint,
                                 cl_loglevel _loglevel, pdb_iterator **_it_out);

int graphd_iterator_without_create(graphd_request *_greq,
                                   pdb_iterator **_producer,
                                   pdb_iterator **_checker,
                                   pdb_iterator **_it_out);

int graphd_iterator_without_any_value_create(graphd_request *greq,
                                             pdb_iterator **producer,
                                             pdb_iterator **it_out);

bool graphd_iterator_without_is_instance(pdb_handle *_pdb, pdb_iterator *_it);

/* graphd-key.c */

int graphd_key_bind(graphd_request *_greq, graphd_constraint *_con,
                    pdb_primitive const *_pr_parent, graphd_value *_reply);

int graphd_key_align(graphd_request *_greq, graphd_constraint *_con,
                     graph_guid const *_pr_parent, pdb_primitive *_pr,
                     graphd_value *_reply);

int graphd_key_parse_check(graphd_request *_greq, graphd_constraint const *_con,
                           int _k);

/* graphd-match.c */

bool graphd_match_guidcon_member(cl_handle *_cl,
                                 graphd_guid_constraint const *_guidcon,
                                 graph_guid const *_guid);

int graphd_match(graphd_request *_greq, graphd_constraint *_con,
                 graphd_read_or_map *_rom, pdb_primitive const *_pr,
                 graph_guid const *_guid_parent);

int graphd_match_intrinsics(graphd_request *_greq, graphd_constraint *_con,
                            pdb_primitive const *_pr);

int graphd_match_intrinsics_guid(graphd_handle *_graphd, cl_handle *_cl,
                                 graph_dateline const *_asof,
                                 graphd_constraint *_con, graph_guid const *_g);

/* graphd-pattern.c */

unsigned long long graphd_pattern_spectrum(graphd_pattern const *);

bool graphd_pattern_is_sort_dependent(cl_handle *cl,
                                      graphd_constraint const *con,
                                      graphd_pattern const *pat);

int graphd_pattern_move_declaration_target(graphd_request *_greq,
                                           graphd_pattern *_pat,
                                           graphd_constraint *_old_con,
                                           graphd_constraint *_new_con);

bool graphd_pattern_is_set_dependent(cl_handle *_cl,
                                     graphd_constraint const *_con,
                                     graphd_pattern const *_pat);

bool graphd_pattern_is_primitive_dependent(cl_handle *_cl,
                                           graphd_constraint const *_con,
                                           graphd_pattern const *_pat);

int graphd_pattern_depth(graphd_pattern const *);
bool graphd_pattern_inside(graphd_pattern const *, int);
void graphd_pattern_append(graphd_request *_greq, graphd_pattern *_parent,
                           graphd_pattern *_child);

int graphd_pattern_dup_in_place(cm_handle *_cm, graphd_pattern *_to,
                                graphd_pattern const *_from);

graphd_pattern *graphd_pattern_empty(void);

graphd_pattern *graphd_pattern_dup(graphd_request *_greq,
                                   graphd_pattern *_parent,
                                   graphd_pattern const *_source);

bool graphd_pattern_head(graphd_pattern const *, graphd_pattern *);

void graphd_pattern_hash(cl_handle *_cl, graphd_pattern const *_pat,
                         unsigned long *_hash_inout);

bool graphd_pattern_equal_value(cl_handle *_cl, graphd_constraint const *_a_con,
                                graphd_pattern const *_a,
                                graphd_constraint const *_b_con,
                                graphd_pattern const *_b);

bool graphd_pattern_equal(cl_handle *_cl, graphd_constraint const *_a_con,
                          graphd_pattern const *_a,
                          graphd_constraint const *_b_con,
                          graphd_pattern const *_b);

graphd_pattern *graphd_pattern_preorder_next(graphd_pattern const *);
void graphd_pattern_null(graphd_pattern *pat);

graphd_pattern const *graphd_pattern_read_default(void);
graphd_pattern const *graphd_pattern_write_default(void);

graphd_pattern *graphd_pattern_alloc(graphd_request *_greq,
                                     graphd_pattern *_parent, int _type);

graphd_pattern *graphd_pattern_alloc_string(graphd_request *_greq,
                                            graphd_pattern *_parent, int _type,
                                            char const *_s, char const *_e);

graphd_pattern *graphd_pattern_alloc_variable(
    graphd_request *_greq, graphd_pattern *_parent,
    graphd_variable_declaration *_vdecl);

bool graphd_pattern_frame_uses_per_primitive_data(graphd_request *_greq,
                                                  graphd_constraint *_con);

int graphd_pattern_from_primitive(graphd_request *_greq,
                                  graphd_pattern const *_pat,
                                  pdb_primitive const *_pr,
                                  graphd_constraint const *_con,
                                  graphd_value *_tok);

char const *graphd_pattern_to_string(graphd_pattern const *_pat, char *_buf,
                                     size_t _size);

char const *graphd_pattern_dump(graphd_pattern const *pat, char *buf,
                                size_t size);

char const *graphd_pattern_type_to_string(graphd_pattern_type _pat, char *_buf,
                                          size_t _size);

int graphd_pattern_from_null(cl_handle *_cl, graphd_pattern const *_pat,
                             graphd_value *_val);

graphd_pattern *graphd_pattern_per_match(graphd_pattern const *);

graphd_pattern *graphd_pattern_wrap(graphd_request *_greq,
                                    graphd_pattern *_child);

graphd_pattern *graphd_pattern_lookup(graphd_pattern const *, int);

void graphd_pattern_variable_rename(graphd_pattern *_pat,
                                    graphd_variable_declaration *_source,
                                    graphd_variable_declaration *_dest);

/* graphd-pattern-frame.c */

void graphd_pattern_frame_spectrum(graphd_request *_greq,
                                   graphd_constraint *_con,
                                   unsigned long long *_set_out,
                                   unsigned long long *_one_out);

int graphd_pattern_frame_create(graphd_request *_greq, graphd_constraint *_con);

char const *graphd_pattern_frame_to_string(graphd_pattern_frame const *_pf,
                                           char *_buf, size_t _size);

/* graphd-predictable.c */

int graphd_predictable_option_set(void *_data, srv_handle *_srv, cm_handle *_cm,
                                  int _opt, char const *_optarg);

int graphd_predictable_option_configure(void *_data, srv_handle *_srv,
                                        void *_config_data,
                                        srv_config *_srv_config_data);

/* graphd-property.c */

graphd_property const *graphd_property_by_name(char const *_s, char const *_e);

int graphd_set_initialize(graphd_request *greq);
int graphd_set(graphd_request *greq);

/* graphd-read.c */

int graphd_read_convert_types(graphd_request *_greq, graphd_constraint *_con);
void graphd_read_push(graphd_request *_greq, graphd_constraint *_con,
                      graphd_value *_val_out, int *_err_out);

int graphd_read_suspend(graphd_request *);
int graphd_read_unsuspend(graphd_request *);

/* deprecated */

int graphd_read(graphd_request *_greq, srv_msclock_t _deadline);

/* graphd-read-or.c */

bool graphd_read_or_check(graphd_request *_greq, size_t _i,
                          graphd_read_or_map const *_rom);

void graphd_read_or_fail(graphd_request *_greq, graphd_constraint *_con,
                         graphd_read_or_map *_rom);

void graphd_read_or_match_subconstraints(graphd_request *_greq,
                                         graphd_constraint *_con,
                                         graphd_read_or_map *_rom);

void graphd_read_or_match_intrinsics(graphd_request *_greq,
                                     graphd_constraint *_con,
                                     graphd_read_or_map *_rom);

/* graphd-replica.c */

void graphd_replica_initialize(graphd_request *greq);

void graphd_unlink_writethrough(graphd_request *src);

int graphd_archive_config_read(void *data, srv_handle *srv, void *config_data,
                               srv_config *srv_cf, char **s, char const *e);

int graphd_replica_config_read(void *_data, srv_handle *_srv,
                               void *_config_data, srv_config *_srv_cf,
                               char **_s, char const *_e);

int graphd_replica_config_open(void *data, srv_handle *srv, void *config_data,
                               srv_config *srv_cf);

int graphd_replica_config_run(void *data, srv_handle *srv, void *config_data,
                              srv_config *srv_cf);

int graphd_replica_option_set_required(void *_data, srv_handle *_srv,
                                       cm_handle *_cm, int _opt,
                                       char const *_optarg);

int graphd_replica_option_set_not_required(void *_data, srv_handle *_srv,
                                           cm_handle *_cm, int _opt,
                                           char const *_optarg);

int graphd_replica_option_configure(void *_data, srv_handle *_srv,
                                    void *_config_data,
                                    srv_config *_srv_config_data);

int graphd_write_master_option_set(void *_data, srv_handle *_srv,
                                   cm_handle *_cm, int _opt,
                                   char const *_optarg);

void graphd_replicate_primitives(graphd_handle *g, pdb_id start, pdb_id end);

int graphd_replicate_restore(graphd_handle *g, pdb_id start, pdb_id end);

void graphd_replica_session_shutdown(graphd_session *gses);

int graphd_replica(graphd_request *greq);

int graphd_rok(graphd_request *greq);

int graphd_replica_write(graphd_request *greq);

int graphd_replica_connect(graphd_handle *g);

void graphd_replica_schedule_reconnect(graphd_handle *g);

int graphd_replica_disconnect(graphd_handle *g);
int graphd_replica_disconnect_oneway(graphd_handle *g);

bool graphd_replica_protocol_session(graphd_session *gses);

/* graphd-request.c */

void graphd_request_start(graphd_request *greq);
void graphd_request_unlink_pointer(graphd_request **);
void graphd_request_link_pointer(graphd_request *_req, graphd_request **_ptr);

int graphd_request_input(void *_data, srv_handle *_srv, void *_session_data,
                         void *_request_data, char **_s, char *_e,
                         srv_msclock_t _deadline);

void graphd_request_cancel(graphd_request *greq);

char const *graphd_request_to_string(graphd_request const *const greq,
                                     char *const buf, size_t const size);

graphd_request *graphd_request_create_outgoing(graphd_session *gses,
                                               enum graphd_command command);

graphd_request *graphd_request_create_asynchronous(
    graphd_session *gses, enum graphd_command type,
    graphd_request_format *callback);

bool graphd_request_has_error(graphd_request const *);
void graphd_request_arrived(graphd_request *);

int graphd_request_initialize(void *_data, srv_handle *_srv,
                              void *_session_data, void *_request_data);

#define graphd_request_error(req, msg) \
  graphd_request_error_loc(req, msg, __FILE__, __LINE__)
void graphd_request_error_loc(graphd_request *_greq, char const *_message,
                              char const *_file, int _line);

void graphd_request_errprintf_loc(graphd_request *_greq, int _subst,
                                  char const *_file, int _line,
                                  char const *_fmt, ...)
#if __GNUC__
    __attribute__((format(printf, 5, 6)))
#endif
    ;

void graphd_request_diary_log(graphd_request *_greq, unsigned long long _millis,
                              char const *_activity);

#define graphd_request_errprintf(r, su, ...) \
  graphd_request_errprintf_loc(r, su, __FILE__, __LINE__, __VA_ARGS__)

void graphd_request_free_specifics(graphd_request *_greq);

void graphd_request_completed_log(graphd_request *, const char *);
void graphd_request_finish_running(graphd_request *greq);

void graphd_request_finish(void *_data, srv_handle *_srv, void *_session_data,
                           void *_request_data);

void graphd_request_served(graphd_request *_greq);

graphd_request_parameter *graphd_request_parameter_append(
    graphd_request *_greq, graphd_request_parameter_format *_format_callback,
    size_t _size);

int graphd_request_as_malloced_string(graphd_request *greq, char **buf_out,
                                      char const **s_out, int *n_out);

void graphd_request_reply_as_string(graphd_request *greq, char *buf,
                                    size_t size, char const **s_out, int *n_out,
                                    bool *incomplete_out);

void graphd_request_as_string(graphd_request *greq, char *buf, size_t size,
                              char const **s_out, int *n_out,
                              bool *incomplete_out);

int graphd_request_become(graphd_request *, graphd_command);

int graphd_request_push_back(graphd_request *greq);
int graphd_request_push_back_resume(graphd_request *greq);

void graphd_request_suspend(graphd_request *greq, graphd_suspend_reason reason);

void graphd_request_suspend_for_dateline(graphd_request *greq,
                                         pdb_id dateline_id);

void graphd_request_resume(graphd_request *);

/* graphd-request-output.c */

int graphd_request_output_text(graphd_request *greq, cm_handle *cm,
                               char const *text);

int graphd_request_output(void *_data, srv_handle *_srv, void *_session_data,
                          void *_request_data, char **_s, char *_e,
                          srv_msclock_t _deadline);

int graphd_format_request_id(graphd_request_parameter *_grp,
                             graphd_request *_greq, char **_s, char *_e);

int graphd_format_request_dateline(graphd_request_parameter *_grp,
                                   graphd_request *_greq, char **_s, char *_e);

int graphd_format_request_heatmap(graphd_request_parameter *_heatmap,
                                  graphd_request *_greq, char **_s, char *_e);

int graphd_format_request_cost(graphd_request_parameter *_cost,
                               graphd_request *_greq, char **_s, char *_e);

void graphd_format_value_records_finish(graphd_request *greq);

int graphd_format_stack_push(graphd_session *gses, graphd_request *greq,
                             graphd_value *t);

void graphd_format_result(void *data, srv_handle *srv, void *session_data,
                          void *request_data, char **s, char *e);

/* graphd-request-run.c */

int graphd_request_run(void *data, srv_handle *srv, void *session_data,
                       void *request_data, unsigned long long deadline);

/* graphd-request-timer.c */

void graphd_request_timer_start(graphd_request *_greq,
                                unsigned long long _timeout);

bool graphd_request_timer_check(graphd_request *);
void graphd_request_timer_stop(graphd_request *);

unsigned long long graphd_request_timer_get_tsc(void);

/* graphd-request-xstate.c */

int graphd_request_xstate_get_ticket(graphd_request *greq);
int graphd_request_xstate_set(graphd_request *greq, int type);
bool graphd_request_xstate_break(graphd_request *greq);
int graphd_request_xstate_type(graphd_request const *greq);

/* graphd-restore.c */

graph_timestamp_t graphd_restore_transaction_end(graphd_request *);
int graphd_restore_import_write_transaction(graphd_request *);

graph_timestamp_t graphd_restore_next_timestamp(graphd_request *greq);
int graphd_restore_create_primitives(graphd_request *greq);

int graphd_restore_checkpoint(cl_handle *cl, graphd_handle *g,
                              graphd_session *gses);

int graphd_restore(graphd_request *_greq);

/* graphd-runtime-statistics.c */

void graphd_runtime_statistics_start_request(graphd_request *greq);
void graphd_runtime_statistics_max(graphd_runtime_statistics *);

void graphd_runtime_statistics_limit(graphd_runtime_statistics *_req,
                                     graphd_runtime_statistics const *_lim);

int graphd_runtime_statistics_get(graphd_request *_greq,
                                  graphd_runtime_statistics *_st);

void graphd_runtime_statistics_publish(graphd_runtime_statistics const *_a,
                                       graphd_runtime_statistics *_b);

void graphd_runtime_statistics_add(graphd_runtime_statistics const *_a,
                                   graphd_runtime_statistics const *_b,
                                   graphd_runtime_statistics *_c);

void graphd_runtime_statistics_diff(graphd_request *_greq,
                                    graphd_runtime_statistics const *_a,
                                    graphd_runtime_statistics const *_b,
                                    graphd_runtime_statistics *_c);

void graphd_runtime_statistics_accumulate(
    graphd_request *_greq, graphd_runtime_statistics *_acc,
    graphd_runtime_statistics const *_before);

bool graphd_runtime_statistics_exceeds(graphd_runtime_statistics const *_small,
                                       graphd_runtime_statistics const *_large,
                                       graphd_runtime_statistics *_report);

void graphd_runtime_statistics_limit_below(
    graphd_runtime_statistics const *_lower_limit,
    graphd_runtime_statistics *_large);

void graphd_runtime_endtoend_initialize(graphd_request *greq);

/* graphd-semantic.c */

void graphd_semantic_constraint_complete(graphd_request *_greq,
                                         graphd_constraint *_con);

void graphd_semantic_constraint_complete_parse(graphd_request *_req,
                                               graphd_constraint *_con);

/* graphd-session.c */

bool graphd_session_receives_replica_write(graphd_session const *);
bool graphd_session_has_room_for_request(graphd_session const *);
void graphd_session_request_wait_add(graphd_session *gses, graphd_request *greq,
                                     unsigned int wakeup_ready);

void graphd_session_request_wait_remove(graphd_request *greq);

void graphd_session_request_wait_abort(graphd_request *greq);

void graphd_session_request_wait_wakeup(graphd_session *gses);

void graphd_session_resume(graphd_session *gses);

char const *graphd_session_to_string(graphd_session const *gses, char *buf,
                                     size_t size);

int graphd_session_dateline_monitor(graphd_handle *);
int graphd_session_delay(graphd_session *_gses, unsigned long _seconds);

void graphd_session_shutdown(void *_data, srv_handle *_srv,
                             void *_session_data);

int graphd_session_initialize(void *_data, srv_handle *_srv,
                              void *_session_data);

int graphd_session_parse(void *_data, srv_handle *_srv, void *_session_data,
                         char **_s, char *_e, srv_msclock_t _deadline);

char const *graphd_session_interactive_prompt(void *_data, srv_handle *_srv,
                                              void *_session_data, char *_buf,
                                              size_t _size);

graphd_session *graphd_session_by_displayname(srv_handle *srv,
                                              gdp_token const *tok);

int graphd_defer_write(graphd_request *greq);

/* graphd-set.c */

/* graphd-shutdown.c */

void graphd_shutdown(void *, srv_handle *);

/* graphd-size-max-config.c */

int graphd_request_size_max_config_read(void *_data, srv_handle *_srv,
                                        void *_config_data, srv_config *_srv_cf,
                                        char **_s, char const *_e);

int graphd_request_size_max_config_open(void *_data, srv_handle *_srv,
                                        void *_config_data,
                                        srv_config *_srv_cf);

/* graphd-sleep.c */

int graphd_sleep(void *data, srv_handle *srv, unsigned long long now,
                 void *session_data, void *request_data);

/* graphd-smp-config.c */

int graphd_smp_initialize(graphd_request *greq);

int graphd_smp_processes_config_read(void *_data, srv_handle *_srv,
                                     void *_config_data, srv_config *_srv_cf,
                                     char **_s, char const *_e);

int graphd_smp_processes_config_open(void *_data, srv_handle *_srv,
                                     void *_config_data, srv_config *_srv_cf);

int graphd_smp_leader_option_set(void *_data, srv_handle *_srv, cm_handle *_cm,
                                 int _opt, char const *_optarg);

int graphd_smp_leader_config_read(void *_data, srv_handle *_srv,
                                  void *_config_data, srv_config *_srv_cf,
                                  char **_s, char const *_e);

int graphd_smp_leader_config_open(void *_data, srv_handle *_srv,
                                  void *_config_data, srv_config *_srv_cf);

/* graphd-smp.c */

int graphd_smp_broadcast(graphd_handle *g, graphd_smp_command cmd);

int graphd_smp_connect(graphd_handle *);

void graphd_smp_session_shutdown(graphd_session *gses);

int graphd_smp_test_follower_state(graphd_handle *g,
                                   graphd_session_smp_state state);
bool graphd_smp_leader_state_machine(graphd_handle *g,
                                     graphd_session_smp_state desired_state);

void graphd_smp_update_followers(graphd_handle *g);

int graphd_suspend_for_smp(graphd_request *greq);

int graphd_resume_from_smp(graphd_session *gses);

void graphd_format_smp_response(void *data, srv_handle *srv, void *session_data,
                                void *request_data, char **s, char *e);

graphd_request *graphd_smp_out_request(graphd_handle *_g, graphd_session *_gses,
                                       graphd_smp_command _smpcmd);

int graphd_smp_pause_for_write(graphd_request *greq);

int graphd_smp_resume_for_write(graphd_request *greq);

/* graphd-smp-startup.c */

int graphd_smp_manage(graphd_handle *g);
int graphd_smp_finish(void *data, srv_handle *srv, size_t index, int status);

int graphd_smp_startup(void *data, srv_handle *srv, size_t index);

/* graphd-smp-passthrough.c */

void graphd_leader_passthrough_initialize(graphd_request *greq);
int graphd_leader_passthrough_connect(graphd_handle *g);

int graphd_leader_passthrough(graphd_request *greq);

/* graphd-sort.c */

#define GRAPHD_SORT_INDEX_NONE ((size_t)-1)

int graphd_sort_unsuspend(cm_handle *_cm, cl_handle *_cl,
                          graphd_sort_context *_gsc);

int graphd_sort_suspend(cm_handle *_cm, cl_handle *_cl,
                        graphd_sort_context *_gsc);

graphd_direction graphd_sort_iterator_direction(graphd_pattern const *);

bool graphd_sort_needed(graphd_request *_greq, graphd_constraint const *_con,
                        pdb_iterator const *_it);

graphd_value *graphd_sort_value(graphd_sort_context *_gsc,
                                graphd_pattern const *_pat, long _loc);

graphd_sort_context *graphd_sort_create(graphd_request *_greq,
                                        graphd_constraint *_con,
                                        graphd_value *_result);

int graphd_sort_accept_prefilter(graphd_sort_context *_gsc, pdb_iterator *_it,
                                 pdb_primitive const *_pr,
                                 size_t *_position_out);

bool graphd_sort_accept_ended(graphd_sort_context *gsc);

int graphd_sort_accept(graphd_sort_context *_gsc, pdb_iterator *_it,
                       graphd_value **_deferred_out);

void graphd_sort_finish(graphd_sort_context *);
void graphd_sort_destroy(graphd_sort_context *);

int graphd_sort_cursor_get(graphd_sort_context *_gsc, char const *_prefix,
                           graphd_value *_val_out);

int graphd_sort_cursor_set(graphd_sort_context *_gsc, char const *_cursor_s,
                           char const *_cursor_e);

int graphd_sort_cursor_peek(graphd_request *_greq, graphd_constraint *_con);

int graphd_sort_is_cursor(char const *s, char const *e);

size_t graphd_sort_n_results(graphd_sort_context *gsc);

int graphd_sort_check(graphd_request *, graphd_constraint const *);

/* graphd-sort-compile.c */

int graphd_sort_compile(graphd_request *_greq, graphd_constraint *_con);

/* graphd-sort-root.c */

bool graphd_sort_root_has_ordering(graphd_sort_root const *_sr,
                                   char const *_ordering);

char const *graphd_sort_root_ordering(graphd_request *_greq,
                                      graphd_sort_root *_sr);

int graphd_sort_root_mark(graphd_request *_greq, graphd_constraint *_con);

void graphd_sort_root_unmark(graphd_request *_greq, graphd_constraint *_con);

int graphd_sort_root_promote(graphd_request *_greq, graphd_constraint *_con);

bool graphd_sort_root_equal(cl_handle *_cl, graphd_sort_root const *_a,
                            graphd_sort_root const *_b);

int graphd_sort_root_from_string(graphd_request *_greq, char const *_s,
                                 char const *_e, graphd_sort_root *_sr);

char const *graphd_sort_root_to_string(graphd_sort_root const *sr, char *buf,
                                       size_t bufsize);

graphd_direction graphd_sort_root_iterator_direction(graphd_request *greq,
                                                     graphd_constraint *con,
                                                     char const **ordering_out);

/* graphd-stack.c */
void graphd_stack_remove(graphd_stack *_stack, graphd_stack_context *_sc);
int graphd_stack_suspend(graphd_stack *stack);
int graphd_stack_unsuspend(graphd_stack *stack);

int graphd_stack_run_until_deadline(graphd_request *_greq, graphd_stack *_stack,
                                    unsigned long long _deadline);

void graphd_stack_push(graphd_stack *_stack, graphd_stack_context *_context,
                       cm_resource_type const *_resource_type,
                       graphd_stack_type const *_stack_type);
int graphd_stack_run(graphd_stack *);
int graphd_stack_pop(graphd_stack *);

void graphd_stack_free(graphd_stack *);
void graphd_stack_list(graphd_stack *, cl_handle *);
graphd_stack_context *graphd_stack_top(graphd_stack *);

void graphd_stack_alloc(graphd_stack *_stack, cm_resource_manager *_rm,
                        cm_handle *_cm);

void graphd_stack_resume(graphd_stack *stack, graphd_stack_context *context,
                         int (*func)(graphd_stack *, graphd_stack_context *));
/* graphd-startup.c */

int graphd_startup_check_max_procs(graphd_handle *g);
void graphd_startup_todo_initialize(graphd_startup_todo_item *);
void graphd_startup_todo_check(graphd_handle *g);
void graphd_startup_todo_add(graphd_handle *g, graphd_startup_todo_item *sti);

void graphd_startup_todo_complete(graphd_handle *g,
                                  graphd_startup_todo_item *sti);

void graphd_startup_todo_cancel(graphd_handle *g,
                                graphd_startup_todo_item *sti);

/* graphd-status.c */

int graphd_status(graphd_request *);
int graphd_status_initialize(graphd_request *greq);

/* graphd-smp-forward.c */

int graphd_smp_start_forward_outgoing(graphd_request *greq);

bool graphd_smp_finished_forward_outgoing(graphd_request *greq);

graphd_request *graphd_smp_forward_outgoing_request(graphd_handle *g,
                                                    graphd_session *gses,
                                                    graphd_request *client_req);

int graphd_smp_status_append_to_list(graphd_request *greq, graphd_value *list);
int graphd_smp_status_init_tokens(graphd_request *greq);
int graphd_smp_status_next_tokens(graphd_request *greq);
void graphd_smp_forward_unlink_all(graphd_request *greq);

/* graphd-strerror.c */

char const *graphd_strerror(int);

/* graphd-string-constraint.c */

graphd_string_constraint_element *graphd_string_constraint_pick(
    graphd_request *_greq, graphd_constraint *_con,
    graphd_string_constraint *_strcon, int _which);

char const *graphd_string_constraint_to_string(
    graphd_string_constraint const *_strcon, char *_buf, size_t _size);

int graphd_string_constraint_member(graphd_request *_greq,
                                    graphd_comparator const *_cmp,
                                    graphd_string_constraint const *_strcon,
                                    char const *_s, char const *_e);

void graphd_string_constraint_add_element(
    graphd_string_constraint *_strcon, graphd_string_constraint_element *_cel);

graphd_string_constraint_element *graphd_string_constraint_element_alloc(
    graphd_request *_greq, char const *_s, char const *_e);

bool graphd_string_constraint_queue_equal(
    cl_handle *_cl, graphd_string_constraint_queue const *_a_queue,
    graphd_string_constraint_queue const *_b_queue);

void graphd_string_constraint_hash(cl_handle *_cl,
                                   graphd_string_constraint_queue const *_q,
                                   unsigned long *_hash_inout);

bool graphd_string_constraint_contradiction(
    cl_handle *cl, graphd_string_constraint const *strcon, char const *s,
    char const *e, char const **s_out, char const **e_out);

graphd_string_constraint *graphd_string_constraint_alloc(
    graphd_request *_greq, graphd_constraint *_con,
    graphd_string_constraint_queue *_q, int _op);

int graphd_string_constraint_element_last(
    graphd_string_constraint_queue const *_q, char const **_con_s,
    char const **_con_e);

graphd_string_constraint *graphd_string_constraint_last(
    graphd_string_constraint_queue const *q);

int graphd_string_constraint_to_signature(
    graphd_string_constraint const *strcon, cm_buffer *sig, bool write_values);

/* graphd-sync.c */

int graphd_sync(graphd_request *greq);

void graphd_sync_initialize(graphd_request *greq);

/* graphd-text-compare.c */

bool graphd_text_token(char const *s0, char const **s, char const *e,
                       char const **tok_s, char const **tok_e);
int graphd_text_compare(char const *_as, char const *_ae, char const *_bs,
                        char const *_be);

/* graphd-timestamp.c */

int graphd_timestamp_to_id(pdb_handle *_pdb,
                           graph_timestamp_t const *_timestamp,
                           graphd_operator _op, pdb_id *_id_out,
                           graph_guid *_guid_out);

/* graphd-token.c */

int graphd_bytes_to_ull(char const **s, char const *e,
                        unsigned long long *n_out);

char const *graphd_unparenthesized_curchr(char const *_s, char const *_e,
                                          char const _ch);

char const *graphd_unparenthesized_textchr(char const *_s, char const *_e,
                                           char const _ch);

char *graphd_escape(cl_handle *_cl, char const *_s, char const *_e, char *_w,
                    char *_w_e);

char *graphd_unescape(cl_handle *_cl, char const *_s, char const *_e, char *_w,
                      char *_w_e);

char const *graphd_string_end(char const *s, char const *e);
char const *graphd_whitespace_end(char const *s, char const *e);

int graphd_next_expression(char const **_s, char const *_e, char const **_s_out,
                           char const **_e_out);
/* graphd-type.c */

void graphd_type_initialize(graphd_handle *g);

int graphd_type_bootstrap_read(graphd_handle *);

int graphd_type_bootstrap(graphd_request *_greq);

int graphd_type_make_name(graphd_request *_greq, char const *_name_s,
                          size_t _name_n, graph_guid *_guid_out);

int graphd_type_value_from_guid(graphd_handle *_g, graph_dateline const *_asof,
                                graph_guid const *_guid,
                                graphd_value *_val_out);

int graphd_type_guid_from_name(graphd_handle *_g, graph_dateline const *_asof,
                               char const *_name_s, size_t _name_n,
                               graph_guid *_guid_out);

/* graphd-used.c */

char const *graphd_used_version(void);

int graphd_used_option_run(void *_data, srv_handle *_srv, cm_handle *_cm,
                           int _opt, char const *_optarg);

/* graphd-unique.c */

char const *graphd_unique_to_string(int _u, char *_buf, size_t _size);

int graphd_unique_parse_check(graphd_request *_greq,
                              graphd_constraint const *_con, int _u);

/* graphd-value.c */

graphd_value *graphd_value_locate(graphd_value const *, int);
int graphd_value_deferred_push(graphd_request *_greq, graphd_value *_val);

void graphd_value_deferred_set(graphd_value *_val, size_t _ind,
                               graphd_deferred_base *_db);

int graphd_value_unsuspend(cm_handle *_cm, cl_handle *_cl, graphd_value *_val);

int graphd_value_suspend(cm_handle *_cm, cl_handle *_cl, graphd_value *_val);

int graphd_value_compare(graphd_request *_greq, graphd_comparator const *_cmp,
                         graphd_value const *_a, graphd_value const *_b);

int graphd_value_copy(graphd_handle *_g, cm_handle *_cm, cl_handle *_cl,
                      graphd_value *_dst, graphd_value const *_src);

int graphd_value_text_alloc(cm_handle *_cm, graphd_value *_val, int _type,
                            size_t _n);

void graphd_value_datatype_set(cl_handle *_cl, graphd_value *_val, int _dt);

void graphd_value_null_set(graphd_value *);
void graphd_value_boolean_set(graphd_value *, int);

int graphd_value_list_alloc(graphd_handle *_g, cm_handle *_cm, cl_handle *_cl,
                            graphd_value *_tok, size_t _n);

#define graphd_value_text_set(a, b, c, d, e) \
  graphd_value_text_set_loc(a, b, c, d, e, __FILE__, __LINE__)

void graphd_value_text_set_loc(graphd_value *_tok, int _type, char const *_s,
                               char const *_e, pdb_primitive const *_pr,
                               char const *_file, int _line);

void graphd_value_text_set_cm(graphd_value *_val, int _type, char *_s,
                              size_t _n, cm_handle *_cm);

int graphd_value_text_strdup(cm_handle *_cm, graphd_value *_tok, int _type,
                             char const *_s, char const *_e);

void graphd_value_number_set(graphd_value *_tok, unsigned long long _num);

void graphd_value_timestamp_set(graphd_value *_tok, graph_timestamp_t _ts,
                                pdb_id _id);

void graphd_value_guid_set(graphd_value *_tok, graph_guid const *_guid);

void graphd_value_sequence_set(cm_handle *_cm, graphd_value *_val);

int graphd_value_array_add(graphd_handle *_g, cl_handle *_cl,
                           graphd_value *_arval, graphd_value const *_tok);

#define graphd_value_array_alloc(g, a, b, c) \
  graphd_value_array_alloc_loc(g, a, b, c, __FILE__, __LINE__)
graphd_value *graphd_value_array_alloc_loc(graphd_handle *_g, cl_handle *_cl,
                                           graphd_value *_arval, size_t _n,
                                           char const *_file, int _line);

void graphd_value_array_alloc_commit(cl_handle *_cl, graphd_value *_arval,
                                     size_t _n);

void graphd_value_array_delete_range(cl_handle *_cl, graphd_value *_arval,
                                     size_t _offset, size_t _nelems);

int graphd_value_sequence_append(graphd_handle *_g, cl_handle *_cl,
                                 graphd_value *_dst, graphd_value *_src);

void graphd_value_array_truncate(cl_handle *_cl, graphd_value *_arval,
                                 size_t _len);

int graphd_value_array_set(graphd_handle *_g, cl_handle *_cl,
                           graphd_value *_arval, size_t _i, graphd_value *_el);

void graphd_value_atom_set_constant(graphd_value *_tok, char const *_lit,
                                    size_t _n);

char const *graphd_value_to_string(graphd_value const *_t, char *_buf,
                                   size_t _size);

void graphd_value_finish(cl_handle *, graphd_value *);
void graphd_value_initialize(graphd_value *);

int graphd_value_serialize(cl_handle *_cl, graphd_value const *_val,
                           cm_buffer *_buf);

int graphd_value_deserialize(graphd_handle *_g, cm_handle *_cm, cl_handle *_cl,
                             graphd_value *_val_out, char const **_s,
                             char const *_e);

void graphd_value_records_set(graphd_value *_val, pdb_handle *_pdb, pdb_id _i,
                              unsigned long long _n);

/* graphd-variable.c */

void graphd_variable_rename(graphd_request *_greq, graphd_constraint *_con,
                            graphd_variable_declaration *_source,
                            graphd_variable_declaration *_dest);

int graphd_variable_remove_unused(graphd_request *_greq,
                                  graphd_constraint *const _con);
int graphd_variable_replace_aliases(graphd_request *const _greq,
                                    graphd_constraint *const _con);

bool graphd_variable_is_assigned_in_or_below(cl_handle *_cl,
                                             graphd_constraint const *_con,
                                             char const *_s, char const *_e);

bool graphd_variable_is_used(cl_handle *_cl, graphd_constraint const *_con,
                             char const *_name_s, char const *_name_e,
                             size_t *_index_out);

graphd_pattern *graphd_variable_declare(graphd_request *_greq,
                                        graphd_constraint *_con,
                                        graphd_pattern *_parent,
                                        char const *_var_s, char const *_var_e);

int graphd_variable_anchor(graphd_request *_greq, graphd_constraint *_con,
                           char const *_name_s, char const *_name_e);

/* graphd-variable-analysis.c */

int graphd_variable_analysis(graphd_request *greq);

/* graphd-variable-declaration.c */

char const *graphd_variable_declaration_to_string(
    graphd_variable_declaration const *vdecl, char *buf, size_t size);

graphd_constraint *graphd_variable_user_in_or_above(
    cl_handle *_cl, graphd_constraint const *_con, char const *_name_s,
    char const *_name_e, size_t *_index_out);

void graphd_variable_declaration_name(graphd_variable_declaration const *_vdecl,
                                      char const **_s_out, char const **_e_out);

bool graphd_variable_declaration_equal(cl_handle *cl,
                                       graphd_constraint const *a_con,
                                       graphd_variable_declaration const *a,
                                       graphd_constraint const *b_con,
                                       graphd_variable_declaration const *b);

graphd_variable_declaration *graphd_variable_declaration_by_name(
    graphd_constraint const *_con, char const *_s, char const *_e);

graphd_variable_declaration *graphd_variable_declaration_add(
    cm_handle *_cm, cl_handle *_cl, graphd_constraint *_con, char const *_s,
    char const *_e);

void graphd_variable_declaration_delete(graphd_variable_declaration *_vdecl);

void graphd_variable_declaration_assign_slots(graphd_constraint *);
void graphd_variable_declaration_destroy(graphd_constraint *);
size_t graphd_variable_declaration_n(graphd_constraint *);

graphd_variable_declaration *graphd_variable_declaration_next(
    graphd_constraint *_con, graphd_variable_declaration *_ptr);

/* graphd-verify.c */

int graphd_verify(graphd_request *greq);
int graphd_verify_setup(graphd_request *greq);

/* graphd-write.c */

bool graphd_write_result_ok(graphd_request *_greq, graphd_pattern const *_pat);

int graphd_write_initialize(graphd_request *greq);
int graphd_write_constraint(graphd_request *_greq, graphd_constraint *_con,
                            graph_guid const *_guid_parent,
                            pdb_primitive const *_pr_parent,
                            graphd_value *_val_out);

/* graphd-writethrough.c */

void graphd_writethrough_initialize(graphd_request *);
void graphd_writethrough_session_fail(graphd_handle *const g);
int graphd_writethrough(graphd_request *greq);

bool graphd_request_copy_request_text(graphd_handle *g, graphd_request *dst,
                                      graphd_request *src, char **s, char *e);

/* graphd-xstate.c */

void graphd_xstate_ticket_delete(graphd_handle *g, graphd_xstate_ticket **x);

bool graphd_xstate_ticket_is_running(graphd_handle *g,
                                     graphd_xstate_ticket const *x);

bool graphd_xstate_any_waiting_behind(graphd_xstate_ticket const *);
int graphd_xstate_ticket_get_exclusive(graphd_handle *g,
                                       void (*callback)(void *), void *data,
                                       graphd_xstate_ticket **tick_out);

int graphd_xstate_ticket_get_shared(graphd_handle *g, void (*callback)(void *),
                                    void *data,
                                    graphd_xstate_ticket **tick_out);

void graphd_xstate_notify_ticketholders(graphd_handle *g);
void graphd_xstate_ticket_reissue(graphd_handle *g, graphd_xstate_ticket *x,
                                  int type);

#endif /* GRAPHD_H */
