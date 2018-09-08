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
#ifndef GRAPH_H
#define GRAPH_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#define GRAPH_ERR_BASE (-5000)
#define GRAPH_ERR_DONE (GRAPH_ERR_BASE + 1)
#define GRAPH_ERR_LEXICAL (GRAPH_ERR_BASE + 2)
#define GRAPH_ERR_SEMANTICS (GRAPH_ERR_BASE + 3)
#define GRAPH_ERR_NO (GRAPH_ERR_BASE + 4)
#define GRAPH_ERR_INSTANCE_ID_MISMATCH (GRAPH_ERR_BASE + 5)
#define GRAPH_ERR_RANGE_OVERLAP (GRAPH_ERR_BASE + 6)
#define GRAPH_ERR_USED (GRAPH_ERR_BASE + 7)

struct cl_handle;
struct cm_handle;

/**
 * @brief Version string for this library.
 */
extern char const graph_build_version[];

/**
 * @file graph.h
 * @brief Graph repository basic data types.
 *
 *  The libgraph C library contains miscellaneous utilities
 *  and type definitions that deal with parts of the
 *  graph repository primitives, such as GUIDs and timestamps.
 */

/**
 * @brief Module handle.
 */
typedef struct graph_handle graph_handle;

/**
 * @brief A lossy set of indices.
 */
typedef struct graph_hullset graph_hullset;

/**
 * @brief A single 128-bit GUID.
 *
 * This struct is public because it is convenient to be able to
 * directly allocate and embed objects of this type.
 * Use macros and functions to access its contents; don't
 * access them directly.
 */
typedef struct graph_guid {
  /*  When seen through the eyes of RFC 4122 (UUIDs),
   *  our GUIDs can pass for pseudorandom UUIDs.
   *
   *  But in reality, there's very little going on in
   *  terms of actual randomness - we simply have our
   *  node name and an index.
   *
   *      graphd view                     RFC 4122 view
   *
   *	  	       most significant
   *                    64-bit integer (A)
   *
   *               high   +-- 63 --+
   *            +--dbid --+    :   +-- pseudorandom value (32)
   *            |(16..47) +-- 32 --+
   *            |
   *            |             31 --+   0
   *            |  4 (4)       :   |-- 1 version=4, pseudorandom
   *  database  |              :   |   0
   *  id (48) --+             28 --+   0
   *	      |
   *            |          +- 27 --+
   *            |  0 (12)  |   :   |
   *            |          +- 16   |
   *            |                  |
   *            |    low   +--15   +-- pseudorandom value (30)
   *            +-- dbid --+   :   |
   *               (0..15) +-- 0---+
   *
   *
   *	  	       least significant
   *                    64-bit integer (B)
   *
   *              2 (2)       63---+  1
   *                          62 --+- 0   variant 1 0 x
   *
   *                      +-- 61---+
   *              0 (28)  |        |
   *                      +-- 34   |
   *               high            +-- pseudorandom value (30)
   *              local   +-- 33   |
   *            +--- id --+    .   |
   *  local     | (32-33) +-- 32 --+
   * id (34) ---+
   *            |   low   +-- 31 --+
   *            +- local--+    .   +-- pseudorandom value (32)
   *                id    |    .   |
   *              (0..31) +--- 0---+
   */

  /** @cond */
  unsigned long long guid_a;
  unsigned long long guid_b;
  /** @endcond */

} graph_guid;

/**
 * @brief A constant null GUID.
 */
extern graph_guid const graph_guid_null;

typedef struct {
  /*
   * Position of the first significant digit
   */
  const char *num_fnz;

  /*
   * Position of the last significant digit
   */
  const char *num_lnz;

  /*
   * The string above may have an extranous dot in it,
   * where is it.
   * NULL if there is not dot within fnz..lnz
   */
  const char *num_dot;
  /*
   * What "power of ten" is this number
   */
  int num_exponent;

  /* Is the number positive or negative?
   */
  bool num_positive;

  /* Is the number zero?
   */
  bool num_zero;

  /* Is the number infinite?
   */
  bool num_infinity;

} graph_number;

/*  A parsed datetime string.
 */
typedef struct {
  char const *dt_year_s;
  char const *dt_year_e;

  char const *dt_mon_s;
  char const *dt_mon_e;

  char const *dt_day_s;
  char const *dt_day_e;

  char const *dt_hour_s;
  char const *dt_hour_e;

  char const *dt_min_s;
  char const *dt_min_e;

  char const *dt_sec_s;
  char const *dt_sec_e;

  char const *dt_sub_s;
  char const *dt_sub_e;

  char dt_sign;

} graph_datetime;

/**
 * @brief How large a buffer to declare to hold a timestamp,
 * 	formatted as a string.
 */
#define GRAPH_TIMESTAMP_SIZE (sizeof("12345-12-12T12:12:12.12345Z"))

/**
 * @brief How large a buffer to declare to hold a GUID, formatted as a string.
 */
#define GRAPH_GUID_SIZE (sizeof("1234567890abcdef1234567890abcdef"))

#ifndef DOCUMENTATION_GENERATOR_ONLY

#define GRAPH_GUID_BITS(part, right, n) \
  (((part) >> (right)) & ((1ull << (n)) - 1))

#define GRAPH_GUID_HOST(guid) GRAPH_GUID_BITS((guid).guid_a, 32, 32)
#define GRAPH_GUID_RANDOM(guid) GRAPH_GUID_BITS((guid).guid_a, 0, 16)

#define GRAPH_GUID_DB(guid) \
  ((GRAPH_GUID_HOST(guid) << 16) | GRAPH_GUID_RANDOM(guid))
#endif

/**
 * @brief Extract the local serial number from a GUID.
 *
 * For GUIDs created on the local system, the local serial number
 * describes where on the system data for the object is stored.
 *
 * @param guid	the guid object to extract from
 */
#define GRAPH_GUID_SERIAL(guid) GRAPH_GUID_BITS((guid).guid_b, 0, 34)

#ifndef DOCUMENTATION_GENERATOR_ONLY

/*  These macros prepare values for assignment to the binary GUID.
 *  "right" is how many bits to the right one shifts to extract them
 *  (same number as in GRAPH_GUID_BITS); "n" is the number of bits.
 */
#define GRAPH_GUID_MAKE_BITS(part, right, n) \
  (((part) & ((1ull << n) - 1)) << (right))

#define GRAPH_GUID_MAKE_A_HOST(x) GRAPH_GUID_MAKE_BITS((x), 32, 32)
#define GRAPH_GUID_MAKE_A_RANDOM(x) GRAPH_GUID_MAKE_BITS((x), 0, 16)

#define GRAPH_GUID_MAKE_B_SERIAL(x) GRAPH_GUID_MAKE_BITS((x), 0, 34)

#define GRAPH_GUID_MAKE_B_RFC4122 GRAPH_GUID_MAKE_BITS(2, 62, 2)
#define GRAPH_GUID_MAKE_A_RFC4122 GRAPH_GUID_MAKE_BITS(4, 28, 4)

/*  Compatibility macros for V2.  (We are now at V3.)
 */
#define GRAPH_V2GUID_TIME(guid) GRAPH_GUID_BITS((guid).guid_a, 32, 32)
#define GRAPH_V2GUID_TIME_FRACTION(guid) GRAPH_GUID_BITS((guid).guid_a, 18, 14)
#define GRAPH_V2GUID_TIMESTAMP(guid) GRAPH_GUID_BITS((guid).guid_a, 18, 46)
#define GRAPH_V2GUID_APPLICATION_ID(guid) GRAPH_V2GUID_TIMESTAMP(guid)
#define GRAPH_V2GUID_HOST(guid)                    \
  ((GRAPH_GUID_BITS((guid).guid_a, 0, 18) << 14) | \
   GRAPH_GUID_BITS((guid).guid_b, 50, 14))
#define GRAPH_V2GUID_RANDOM(guid) GRAPH_GUID_BITS((guid).guid_b, 34, 16)
#define GRAPH_V2GUID_DB(guid)                      \
  ((GRAPH_GUID_BITS((guid).guid_a, 0, 18) << 30) | \
   GRAPH_GUID_BITS((guid).guid_b, 34, 30))
#endif

/**
 * @brief Test two GUIDs for equality.
 * @param x a GUID
 * @param y a GUID
 * @return 1 if the two GUIDs are equal, 0 otherwise.
 */
#define GRAPH_GUID_EQ(x, y) \
  ((x).guid_a == (y).guid_a && (x).guid_b == (y).guid_b)

/**
 * @brief Test whether a GUID is null.
 * @param x a GUID
 * @return 1 if the GUID is null, 0 otherwise.
 */
#define GRAPH_GUID_IS_NULL(x) ((x).guid_a == 0 && (x).guid_b == 0)

/**
 * @brief Set a GUID to null.
 * @param x a GUID
 */
#define GRAPH_GUID_MAKE_NULL(x) ((x).guid_a = 0, (x).guid_b = 0)

/**
 * @brief The datatype value in a graph repository tuple.
 */
typedef enum graph_datatype {

  /**
   * @brief Unspecified data type (0).
   *
   * This value shouldn't occur
   * in a tuple that actually gets stored or accessed,
   * but can be useful to indicate that a value hasn't
   * yet been assigned.
   */
  GRAPH_DATA_UNSPECIFIED,

  /**
   * @brief Null data type.
   *
   * The primitive has no value.
   */
  GRAPH_DATA_NULL,

  /**
   * @brief String data type.
   *
   * The primitive value is null or a UTF-8 string.
   */
  GRAPH_DATA_STRING,

  /**
   * @brief Integer data type.
   *
   * The primitive value is null or a string of digits, interpreted
   * as a decimal integer.
   */
  GRAPH_DATA_INTEGER,

  /**
   * @brief Floating point data type.
   *
   * The primitive value is null or a string of digits and punctuation,
   * interpreted as a floating point number.
   */
  GRAPH_DATA_FLOAT,

  /**
   * @brief GUID data type.
   *
   * The primitive value is either null, "null", "0", or
   * a string of hexadecimal digits, interpreted as a GUID.
   */
  GRAPH_DATA_GUID,

  /**
   * @brief Timestamp data type.
   *
   * The primitive value is a string of us-ascii characters
   * in the form "12345-12-12T12:12:12.1234Z", interpreted
   * as a timestamp.
   */
  GRAPH_DATA_TIMESTAMP,

  /**
   * @brief URL data type.
   *
   * The primitive value is a string of UTF-8 characters,
   * interpreted as a URL.
   */
  GRAPH_DATA_URL,

  /**
   * @brief Bytestring data type.
   *
   * The primitive value is an uninterpreted bytestring.
   */
  GRAPH_DATA_BYTESTRING,

  /**
   * @brief Bytestring data type.
   *
   * The primitive value is "true" or "false"
   */
  GRAPH_DATA_BOOLEAN

} graph_datatype;

/**
 * @brief Test whether the argument is a valid graph_datatype value.
 */
#define GRAPH_IS_DATATYPE(x) \
  ((x) >= GRAPH_DATA_UNSPECIFIED && (x) <= GRAPH_DATA_BOOLEAN)

/*  The "graph_timestamp_t" data type and some utility functions that
 *  manipulate it.
 *
 *  	63 --+
 *	 :   +-- padding, must be 0.
 *  	48 --+
 *
 *  	47 --+
 *	 :   +-- 32 bit time_t-style timestamp
 *  	16 --+
 *
 *	15 --+
 *	 :   +-- 0...9999, saved in the least significant bits
 *	 0---+	 of a 16-bit serial number
 */

/**
 * @brief System timestamps; only the lower 48 bits are used.
 */
typedef uint64_t graph_timestamp_t;

/**
 * @brief Maximum possible value of a timestamp.
 */
#define GRAPH_TIMESTAMP_MAX 0xffffffffffffull

/**
 * @brief Minimum possible value of a timestamp.
 */
#define GRAPH_TIMESTAMP_MIN 0ull

/**
 * @brief Construct a timestamp.
 * @param t	current time in seconds since 1970 (Unix "epoch")
 * @param seq	sub-second sequence number, between 1 and 2^16-1
 * @returns a timestamp that encodes the given instant.
 */
#define GRAPH_TIMESTAMP_MAKE(t, seq) \
  ((graph_timestamp_t)(t) << 16 | (0xFFFF & seq))

/**
 * @brief Extract time in seconds from a timestamp.
 * @param ts	48-bit timestamp
 * @returns the time encoded in the timestamp
 */
#define GRAPH_TIMESTAMP_TIME(ts) ((ts) >> 16)

/**
 * @brief Extract 16-bit serial number from a timestamp
 * @param ts	48-bit timestamp
 * @returns the sub-second sequence number encoded in a timestamp.
 */
#define GRAPH_TIMESTAMP_SERIAL(ts) ((int)((ts) & ((1ul << 16) - 1)))

/**
 * @brief an instance ID, part of a dateline.
 */
#define GRAPH_INSTANCE_ID_SIZE (31)

/**
 * @brief Opaque structure that holds a set of database/primitive-count pairs.
 */
typedef struct graph_dateline graph_dateline;

#define GRAPH_GRMAP_DEFAULT_TABLE_SIZE 1024

typedef struct graph_grmap_table graph_grmap_table;

typedef struct graph_grmap_table_slot {
  graph_grmap_table *ts_table;
  unsigned long long ts_low;

} graph_grmap_table_slot;

typedef struct graph_grmap_dbid_slot {
  unsigned long long dis_dbid;

  graph_grmap_table_slot *dis_table;
  size_t dis_n;
  size_t dis_m;

} graph_grmap_dbid_slot;

typedef struct graph_grmap {
  graph_handle *grm_graph;

  /*  A short table of DBID slots; usually flatly inside
   *  the handle.   The typical number of slots is 2 - one
   *  reference database, one local database overlayed on it.
   */
  graph_grmap_dbid_slot grm_dbid_buf[3];
  graph_grmap_dbid_slot *grm_dbid;
  size_t grm_n;
  size_t grm_m;

  /* The number of elements in a graph_grmap_table.
   */
  size_t grm_table_size;

} graph_grmap;

typedef struct graph_grmap_read_state {
  unsigned int grs_state;

  int grs_sign;
  unsigned long long grs_number;

  unsigned long long grs_dbid;
  unsigned char grs_num_i;
  long long grs_num[4];

  unsigned int grs_in_number : 1;
  unsigned int grs_in_dbid : 1;
  unsigned int grs_in_map : 1;
  char const *grs_literal;

} graph_grmap_read_state;

typedef struct graph_grmap_next_state {
  size_t grn_dis_i;
  size_t grn_tab_i;
  size_t grn_range_i;

} graph_grmap_next_state;

typedef struct graph_grmap_write_state {
  unsigned int grw_state;

  size_t grw_dis_i;
  size_t grw_tab_i;
  size_t grw_range_i;

} graph_grmap_write_state;

/* graph.c */

graph_handle *graph_create(struct cm_handle *, struct cl_handle *);
void graph_destroy(graph_handle *);

/* graph-grmap.c */

graph_dateline *graph_grmap_dateline(graph_grmap const *grmap);

int graph_grmap_map(graph_grmap const *grmap, graph_guid const *source,
                    graph_guid *destination);

int graph_grmap_add_range(graph_grmap *grmap, graph_guid const *source,
                          graph_guid const *destination, long long n);

#define graph_grmap_invariant(grm) \
  graph_grmap_invariant_loc(grm, __FILE__, __LINE__)

void graph_grmap_invariant_loc(graph_grmap const *const grm, char const *file,
                               int line);

void graph_grmap_initialize(graph_handle *graph, graph_grmap *grmap);

void graph_grmap_finish(graph_grmap *grmap);
int graph_grmap_set_table_size(graph_grmap *grm, size_t tab_size);

unsigned long long graph_grmap_dbid_high(graph_grmap const *grmap,
                                         unsigned long long dbid);

/* graph-grmap-equal.c */

bool graph_grmap_equal(graph_grmap const *a, graph_grmap const *b);

/* graph-grmap-next.c */

void graph_grmap_next_initialize(graph_grmap const *grm,
                                 graph_grmap_next_state *state);

bool graph_grmap_next(graph_grmap const *grm, graph_grmap_next_state *state,
                      graph_guid *source, graph_guid *destination,
                      unsigned long long *n_out);

void graph_grmap_next_dbid_initialize(graph_grmap const *grm,
                                      graph_guid const *source,
                                      graph_grmap_next_state *state);

bool graph_grmap_next_dbid(graph_grmap const *grm,
                           graph_grmap_next_state *state, graph_guid *source,
                           graph_guid *destination, unsigned long long *n_out);

bool graph_grmap_next_dbid(graph_grmap const *grm,
                           graph_grmap_next_state *state, graph_guid *source,
                           graph_guid *destination, unsigned long long *n_out);

/* graph-grmap-read.c */

void graph_grmap_read_initialize(graph_grmap const *grm,
                                 graph_grmap_read_state *state);

int graph_grmap_read_next(graph_grmap *grm, char const **s, char const *const e,
                          graph_grmap_read_state *state);

/* graph-grmap-write.c */

void graph_grmap_write_initialize(graph_grmap const *grm,
                                  graph_grmap_write_state *state);

int graph_grmap_write_next(graph_grmap const *grm, char **s,
                           char const *const e, graph_grmap_write_state *state);

/* graph-guid.c */

int graph_ull_from_hexstring(unsigned long long *_out, char const *_s,
                             char const *_e);

void graph_guid_from_db_serial(graph_guid *_buf, unsigned long long _database,
                               unsigned long long _serial);

int graph_guid_from_string(graph_guid *_buf, char const *_s, char const *_e);

char const *graph_guid_to_string(graph_guid const *_guid, char *_buf,
                                 size_t _bufsize);

char const *graph_guid_to_network(graph_guid const *_guid, char *_buf,
                                  size_t _bufsize);

int graph_guid_from_network(graph_guid *_guid, char const *_buf,
                            size_t _bufsize);

int graph_guid_compare(void const *, void const *);

char const *graph_guid_compress(unsigned long long _default_database_id,
                                graph_guid const *_guid, char *_buf,
                                size_t _size);

int graph_guid_uncompress(unsigned long long _default_database_id,
                          graph_guid *_guid, char const *_s, char const *_e);

/*  A sorted set.
 */
typedef struct graph_idset graph_idset;
typedef struct graph_idset_position {
  unsigned long long gip_ull;
  size_t gip_size;

} graph_idset_position;

/* graph-idset-tile.c */

graph_idset *graph_idset_tile_create(graph_handle *g);

/* Add, sorting automatically, duplicates silently discarded. */
typedef int graph_idset_insert(graph_idset *idset, unsigned long long id);
#define graph_idset_insert(idset, a) ((idset)->gi_type->git_insert)(idset, (a))

/* Add another link to an existing set. */

#define graph_idset_link(idset) ((idset)->gi_linkcount++)

/* Free the whole set. */
typedef void graph_idset_free(graph_idset *idset);
#define graph_idset_free(idset)                              \
  ((idset)->gi_linkcount > 1 ? (void)(idset)->gi_linkcount-- \
                             : ((idset)->gi_type->git_free)(idset))

/* Is id in the graph? */
typedef bool graph_idset_check(graph_idset *idset, unsigned long long id);
#define graph_idset_check(idset, a) ((idset)->gi_type->git_check)((idset), (a))

/* pos = where id is, or its next higher relative.  */
typedef bool graph_idset_locate(graph_idset *idset, unsigned long long id,
                                graph_idset_position *pos_out);
#define graph_idset_locate(idset, a, b) \
  ((idset)->gi_type->git_locate)((idset), (a), (b))

/* pos = 0 */
typedef void graph_idset_next_reset(graph_idset *idset,
                                    graph_idset_position *pos_out);
#define graph_idset_next_reset(idset, a) \
  ((idset)->gi_type->git_next_reset)((idset), (a))

/* *id_out = *pos++ */
typedef bool graph_idset_next(graph_idset *idset, unsigned long long *id_out,
                              graph_idset_position *pos_inout);
#define graph_idset_next(idset, a, b) \
  ((idset)->gi_type->git_next)((idset), (a), (b))

/* pos = N */
typedef void graph_idset_prev_reset(graph_idset *idset,
                                    graph_idset_position *pos_out);
#define graph_idset_prev_reset(idset, a) \
  ((idset)->gi_type->git_prev_reset)((idset), (a))

/* *id_out = *--pos */
typedef bool graph_idset_prev(graph_idset *idset, unsigned long long *id_out,
                              graph_idset_position *pos_inout);
#define graph_idset_prev(idset, a, b) \
  ((idset)->gi_type->git_prev)((idset), (a), (b))

/* How many elements between here and there? */
typedef long long graph_idset_offset(graph_idset *idset,
                                     graph_idset_position *pos,
                                     unsigned long long id);
#define graph_idset_offset(idset, pos, id) \
  ((idset)->gi_type->git_offset)((idset), (pos), (id))

typedef struct graph_idset_type {
  graph_idset_insert *git_insert;
  graph_idset_check *git_check;
  graph_idset_locate *git_locate;
  graph_idset_next *git_next;
  graph_idset_next_reset *git_next_reset;
  graph_idset_prev *git_prev;
  graph_idset_prev_reset *git_prev_reset;
  graph_idset_offset *git_offset;
  graph_idset_free *git_free;

} graph_idset_type;

struct graph_idset {
  graph_idset_type const *gi_type;
  graph_handle *gi_graph;
  unsigned long long gi_n;
  unsigned int gi_linkcount;

  /* other data here. */
};

/* graph-timestamp.c */

int graph_timestamp_from_time(graph_timestamp_t *, time_t);
int graph_timestamp_from_members(graph_timestamp_t *_buf,
                                 unsigned int _year,  /* 2004  */
                                 unsigned int _mon,   /* 1..12 */
                                 unsigned int _mday,  /* 1..31 */
                                 unsigned int _hour,  /* 0..23 */
                                 unsigned int _min,   /* 0..59 */
                                 unsigned int _sec,   /* 0..60 */
                                 unsigned long _seq); /* 0..16k */

int graph_timestamp_from_string(graph_timestamp_t *_out, char const *_s,
                                char const *_e);

char const *graph_timestamp_to_string(graph_timestamp_t, char *, size_t);
int graph_timestamp_to_time(graph_timestamp_t, time_t *);
struct tm *graph_timestamp_to_tm(graph_timestamp_t, struct tm *);
void graph_timestamp_sync(graph_timestamp_t *, time_t);
graph_timestamp_t graph_timestamp_next(graph_timestamp_t *);

/* graph-datatype.c */

int graph_datatype_from_string(graph_datatype *_buf, char const *_s,
                               char const *_e);

char const *graph_datatype_to_string(graph_datatype dt);

/* graph-dateline.c */

int graph_dateline_split(graph_dateline **);
graph_dateline *graph_dateline_create(struct cm_handle *);
graph_dateline *graph_dateline_copy(struct cm_handle *, graph_dateline const *);
graph_dateline *graph_dateline_dup(graph_dateline *);
void graph_dateline_destroy(graph_dateline *);

int graph_dateline_add(graph_dateline **_dl, unsigned long long _dbid,
                       unsigned long long _count, char const *_instance_id);

int graph_dateline_add_minimum(graph_dateline **_dl, unsigned long long _dbid,
                               unsigned long long _count,
                               char const *_instance_id);

bool graph_dateline_instance_verify(const char *, const char *);
char const *graph_dateline_instance_id(graph_dateline const *);

int graph_dateline_get(graph_dateline const *_dl, unsigned long long _dbid,
                       unsigned long long *_count_out);

int graph_dateline_next(graph_dateline const *_dl,
                        unsigned long long *_dbid_out,
                        unsigned long long *_count_out, void **_state_inout);

int graph_dateline_from_string(graph_dateline **_dl, char const *_s,
                               char const *_e);

char const *graph_dateline_to_string(graph_dateline const *_dl, char *_s,
                                     size_t _n);

int graph_dateline_format(graph_dateline const *_dl, char **_s, char *_e,
                          void **_state, size_t *_offset);

size_t graph_dateline_n(graph_dateline *);

int graph_dateline_merge(graph_dateline **_a, graph_dateline const *_b);

int graph_dateline_merge_minimum(graph_dateline **_a, graph_dateline const *_b);

bool graph_dateline_equal(graph_dateline const *_a, graph_dateline const *_b);

unsigned long long graph_dateline_hash(graph_dateline const *);

/* graph-hullset.c */

/**
 * @brief Opaque position in a hullset.
 */
typedef struct graph_hullset_iterator {
  /**
   * @brief slot.
   */
  size_t hit_slot;

  /**
   * @brief offset relative to the slot.
   */
  unsigned long long hit_offset;

} graph_hullset_iterator;

graph_hullset *graph_hullset_create(struct cm_handle *, struct cl_handle *,
                                    size_t);
void graph_hullset_destroy(graph_hullset *);

int graph_hullset_seek_to(graph_hullset *, unsigned long long);
void graph_hullset_add(graph_hullset *, unsigned long long);
void graph_hullset_add_range(graph_hullset *_hull, unsigned long long _start,
                             unsigned long long _end);
void graph_hullset_add_hullset(graph_hullset *, graph_hullset *);
bool graph_hullset_is_singleton(graph_hullset const *hull);

int graph_hullset_next(graph_hullset *, unsigned long long *);
int graph_hullset_find(graph_hullset *_hull, unsigned long long *_i_out,
                       bool *_changed_out);
int graph_hullset_next_range(graph_hullset *_hull,
                             unsigned long long *_start_out,
                             unsigned long long *_end_out);
void graph_hullset_reset(graph_hullset *);
unsigned long long graph_hullset_count(graph_hullset *);
char const *graph_hullset_to_string(graph_hullset const *_hull, char *_buf,
                                    size_t _size);

void graph_hullset_iterator_initialize(graph_hullset_iterator *);

int graph_hullset_iterator_next(graph_hullset const *_hull,
                                graph_hullset_iterator *_hit,
                                unsigned long long *_i_out);

int graph_hullset_iterator_next_range(graph_hullset const *_hull,
                                      graph_hullset_iterator *_hit,
                                      unsigned long long *_start_out,
                                      unsigned long long *_end_out);

int graph_hullset_iterator_seek_to(graph_hullset *_hull,
                                   graph_hullset_iterator *_hit,
                                   unsigned long long _val);

int graph_hullset_iterator_find(graph_hullset *hull,
                                graph_hullset_iterator *hit,
                                unsigned long long *val_in_out,
                                bool *changed_out);

int graph_fuzzycmp(const char *a_s, const char *a_e, const char *b_s,
                   const char *b_e);

int graph_strcasecmp(const char *a_s, const char *a_e, const char *b_s,
                     const char *b_e);

int graph_decode_number(const char *s, const char *e, graph_number *n,
                        bool scientific);

int graph_number_compare(graph_number const *a, graph_number const *b);

/* graph-strerror.c */

char const *graph_strerror(int err);

#ifdef __cplusplus
} /* end extern "C" */
#endif

#endif /* GRAPH_H */
