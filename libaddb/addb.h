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
#ifndef ADDB_H
#define ADDB_H

/**
 * @file addb.h
 * @brief Simple databases.
 *
 * Libaddb implements specific, file-based databases that are used
 * by libpdb to implement fast access to link- or node data.
 */

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h> /* uint_least64_t etc. */
#include <stdlib.h> /* size_t */

#include "libcm/cm.h"
#include "libcl/cl.h"

#define ADDB_ERR_BASE (-3000)

#define ADDB_ERR_NO (ADDB_ERR_BASE + 1)
#define ADDB_ERR_MORE (ADDB_ERR_BASE + 2)
#define ADDB_ERR_PRIMITIVE_TOO_LARGE (ADDB_ERR_BASE + 3)
#define ADDB_ERR_BITMAP (ADDB_ERR_BASE + 4)
#define ADDB_ERR_EXISTS (ADDB_ERR_BASE + 5)
#define ADDB_ERR_ALREADY (ADDB_ERR_BASE + 6)
#define ADDB_ERR_DATABASE (ADDB_ERR_BASE + 7)

#ifndef ADDB_SYNC_USES_CLONE
#ifdef __APPLE__
#define ADDB_SYNC_USES_CLONE 0
#else
#define ADDB_SYNC_USES_CLONE 1
#endif
#endif

#ifndef ADDB_HAVE_FDATASYNC
#ifdef __APPLE__
#define ADDB_HAVE_FDATASYNC 0
#else
#define ADDB_HAVE_FDATASYNC 1
#endif
#endif

#ifndef ADDB_FSYNC_DIRECTORY
#ifdef __APPLE__
#define ADDB_FSYNC_DIRECTORY 0
#else
#define ADDB_FSYNC_DIRECTORY 1
#endif
#endif

/**
 * @brief Version string for this library.
 */
extern char const addb_build_version[];

/**
 * @brief Opaque module handle.
 */
typedef struct addb_handle addb_handle;

/**
 * @brief Flat, constant-size file.
 */
typedef struct addb_flat addb_flat;

/**
 * @brief Map indices to variable-sized records.
 */
typedef struct addb_istore addb_istore;

/**
 * @brief A single index into an addb_istore.
 */
typedef uint_fast64_t addb_istore_id;

/**
 * @brief Map indices to sets of indices.
 */
typedef struct addb_gmap addb_gmap;

/**
 * @brief Map anything to sets of indices.
 */
typedef struct addb_hmap addb_hmap;
typedef struct addb_bmap addb_bmap;

/**
 * @brief An istore marker file (horizon or next-id).
 */
typedef struct addb_istore_marker addb_istore_marker;

/**
 * @brief One index inside an #addb_gmap.
 */
typedef uint_fast64_t addb_gmap_id;
typedef addb_gmap_id addb_hmap_id;
typedef addb_gmap_id addb_id;

/**
 * @brief A reference to a tile within the tiled access layer.
 */
typedef size_t addb_tiled_reference;
#define ADDB_TILED_REFERENCE_EMPTY ((size_t)-1)

/**
 * @brief A deadline passed into a synchronize function.
 *
 *  The value may wrap, although it is unlikely to.
 */
typedef unsigned long long addb_msclock_t;

/**
 * @brief The number of discrete steps performed by this server.
 */
typedef unsigned long long addb_opcount_t;

/**
 * @brief The maximum size of a single tile or chunk of memory.
 */
#define ADDB_TILE_SIZE (32ull * 1024ull)

/**
 * @brief Map "tile" to ADDB_FACILITY_TILE, etc.
 */
extern const cl_facility addb_facilities[];

/**
 * @brief Debug facility for the tile cache
 */
#define ADDB_FACILITY_TILE (1ul << 22)

/**
 * @brief Debug facility for code involved in recovery after a crash
 */
#define ADDB_FACILITY_RECOVERY (1ul << 23)

/**
 * @brief Are we past the deadline yet?
 * @param now		the current time, as an addb_msclock_t
 * @param deadline	the deadline, as an addb_msclock_t
 * @result true or false: has this run past its deadline?
 */
#define ADDB_PAST_DEADLINE(now, deadline)                                   \
  ((deadline) != 0 && ((addb_msclock_t)(now) - (addb_msclock_t)(deadline) < \
                       (addb_msclock_t)(deadline) - (addb_msclock_t)(now)))

typedef struct addb_tiled addb_tiled;

/**
 * @brief Reference to a tile in an istore partition.
 */
typedef struct addb_istore_reference {
  /**
   * @brief Per-partition tiled file.
   */
  struct addb_tiled* iref_td;

  /**
   * @brief Within that partition, where are we?
   */
  addb_tiled_reference iref_tref;

} addb_istore_reference;

#define addb_istore_reference_is_empty(iref) \
  ((iref)->iref_td == NULL || (iref)->iref_tref == (size_t)-1)

/**
 * @brief Configuration parameters for a single istore table.
 */
typedef struct addb_istore_configuration {
  /* @brief The number of tiles in the initial mmap'd region
   */
  unsigned long long icf_init_map;

  /**
   * @brief Lock the istore in memory if true
   */
  int icf_mlock;
} addb_istore_configuration;

/**
 * @brief Configuration parameters for a single gmap table.
 */
typedef struct addb_gmap_configuration {
  /**
   * @brief How much memory to initially map for each partition of
   * this gmap */
  unsigned long long gcf_init_map;

  /**
   * @brief Lock the gmap in memory if true
   */
  bool gcf_mlock;

  /**
   * @brief the threshhold at which a gmap is moved into its own file
   */
  unsigned long long gcf_split_thr;

  /**
   * @brief how many largefiles may be open
   */

  unsigned long long gcf_max_lf;

  /**
   * @brief The maximum amount of memory to possibly use for the
   * initial mmap of a largefile backed gmap.
   * Guestimation functions may and do usually choose a smaller amount.
   */
  unsigned long long gcf_lf_init_map;

  bool gcf_allow_bgmaps;

} addb_gmap_configuration;

/**
 * @brief Configuration parameters for an hmap
 */
typedef struct addb_hmap_configuration {
  /**
   * @brief The number of tiles in the initial mmap'd region of
   * an hmap
   */
  unsigned long long hcf_init_map;

  /**
   * @brief The number of tiles in the initial mmap'd region of
   * an hmap's gmap
   */
  unsigned long long hcf_gm_init_map;

  /**
   * @brief Lock the hmap in memory if true
   */
  bool hcf_mlock;

  /**
   * @brief Configuration for the underlying gmap.
   */
  addb_gmap_configuration hcf_gmap_cf;

} addb_hmap_configuration;

/**
 * @brief Highest possible index that can be used with an #addb_gmap.
 */
#define ADDB_GMAP_ID_MAX ((1ull << 34) - 1)

struct addb_gmap_partition;

/**
 * @brief A reference to data within an istore or flat file.
 */
typedef struct addb_data {
  /**
   * @brief Start of the record in memory
   */
  char* data_memory;

  /**
   * @brief Number of bytes pointed to by #data_memory.
   */
  size_t data_size;

  /**
   * @brief Where does the memory come from?
   */
  enum {
    /**
     * @brief Memory?  What memory?
     */
    ADDB_DATA_NONE,

    /**
     * @brief An istore table, mapped via the tile cache.
     */
    ADDB_DATA_ISTORE,

    /**
     * @brief A flat file database.
     */
    ADDB_DATA_FLAT,

    /**
     * @brief Dynamic allocation.
     */
    ADDB_DATA_CM
  } data_type;

  /**
   * @brief Detail information for the chosen reference.
   */
  union {
    /**
     * @brief #data_type is #ADDB_DATA_ISTORE, and
     *	this is a counted tile reference associated with
     *  	this piece of memory.
     */
    addb_istore_reference ref_iref;

    /**
     * @brief #data_type is #ADDB_DATA_FLAT, and
     *	this is the flat database that the memory
     * 	is from.
     */
    addb_flat* ref_flat;

    /**
     * @brief #data_type is #ADDB_DATA_CM, and
     *	this is the memory manager the memory was
     *  	allocated through (and must be free'd through).
     */
    cm_handle* ref_cm;

  } data_ref;

/**
 * @brief Only valid if #data_type is #ADDB_DATA_ISTORE
 */
#define data_iref data_ref.ref_iref

/**
 * @brief Only valid if #data_type is #ADDB_DATA_FLAT
 */
#define data_flat data_ref.ref_flat

/**
 * @brief Only valid if #data_type is #ADDB_DATA_CM
 */
#define data_cm data_ref.ref_cm

} addb_data;

/**
 * @brief this structures hoolds all the information needed to access
 * a particular ID of a particular gmap.
 */
typedef struct addb_gmap_accessor {
  struct addb_gmap_partition* gac_part;
  struct addb_largefile* gac_lf;
  struct addb_bgmap* gac_bgmap;
  unsigned long long gac_offset;
  unsigned long long gac_length;
  unsigned long long gac_index;

} addb_gmap_accessor;

/**
 * @brief One set of indexes in a gmap or hmap.
 */
typedef struct addb_idarray {
  /**
   * @brief File access abstraction
   *
   *  Hides the difference between largefile and gmap entries.
   */
  addb_gmap_accessor ida_gac;

  /**
   * @brief Use the ida_single_id and ida_single_bytes buffers.
   */
  unsigned int ida_is_single : 1;

  /**
   * @brief Buffer for a single ID that may be stored in a header.
   */
  addb_id ida_single_id;

  /**
   * @brief Buffer for a five-byte "raw" representation of a single ID
   *
   *  Some idarray representations store single entries specially.
   *  Having a buffer for them in the idarray allows us to treat
   *  these special cases as if they were flat idarray files.
   */
  unsigned char ida_single_bytes[5];

  /**
   * @param Tiled reference for accessing tiled partitions.
   *	Only one valid at a time.
   */
  addb_tiled_reference ida_tref;

  /**
   * @brief Whatever happens, log through this.
   */
  cl_handle* ida_cl;

} addb_idarray;

/**
 * @brief Iterator over a set of indices in an addb_gmap.
 * 	The indices are not written to or moved during the
 * 	iterator's lifetime.
 */
typedef struct addb_gmap_iterator {
  /**
   * @brief current element index.
   *
   *  The next call to addb_gmap_iterator_next() will
   *  return this element and increment iter_i.  Invalid
   *  (and treated as if it were 0) if iter_part is NULL.
   */
  addb_gmap_id iter_i;

  /**
   * @brief Total number of items in this iterator.
   */
  unsigned long long iter_n;

  /**
   * @brief If this is set, iter_i is the offset from 0.
   *  	If it is unset, iter_i is subtracted from iter_n - 1
   *	to get the true offset of the current entry,
   *	and _next() and _find() have their meanings
   * 	inverted (to _prev() and _on_or_before().)
   */
  unsigned int iter_forward : 1;

  /**
   * @brief File access abstraction
   *
   *  Hides the difference between largefile and gmap entries.
   */
  addb_gmap_accessor iter_ac;

} addb_gmap_iterator;

struct addb_largefile {
  /**
   * @brief the tiled structure for this file
   * if it is null, it implies the file is closed
   */
  struct addb_tiled* lf_td;

  /**
   * @brief First unused location measured from the begining of
   * the file
   */
  size_t lf_size;

  /**
   * @brief linked list fields
   */
  struct addb_largefile* lf_next;
  struct addb_largefile* lf_prev;

  /**
   * @brief name to use to pretty-print this largefile for diagnostics
   */
  char* lf_display_name;

  /**
   * @brief the ID this largefile coresponds to
   */
  unsigned long long lf_id;

  /**
   * Set if this largefile has been just-created and is still being
   * populated with its initial data set.  It prevents recycling of
   * the descriptor and use of the callback functions to store the
   * data set size.
   */
  bool lf_setting_up;

  bool lf_delete;

  int lf_delete_count;

  /**
   * @brief Has the file been modified since the last checkpoint?
   */
  bool lf_dirty;

  struct addb_largefile_handle* lf_lfhandle;
};

/*
 * The state that we need for a pthreaded asynchronous sync
 */
typedef struct addb_fsync_ctx {
  addb_handle* fsc_addb;
  unsigned long long fsc_guard; /* must be zero */
  int fsc_fd;                   /* fd to fsync */
  pthread_t fsc_thread;         /* pthread of the thread doing the sync */
  long fsc_done; /* Set by the child thread when the sync finishes */
} addb_fsync_ctx;

typedef struct addb_largefile addb_largefile;

/**
 * @brief Location in an iterator, as used by addb_gmap_get_position()
 *  	and addb_gmap_set_position().
 */
typedef addb_gmap_id addb_gmap_iterator_position;

/**
 * @brief special addb_gmap_iterator_position, means "at the very beginning".
 */
#define ADDB_GMAP_POSITION_START (1ull << 34)

/**
 * @brief special addb_gmap_iterator_position, means "at the very end".
 */
#define ADDB_GMAP_POSITION_END ((1ull << 34) | 1)

/** The type of value stored in an HMAP bucket
*/
typedef enum addb_hmap_type {
  addb_hmt_name, /* string keys ... */
  addb_hmt_value,
  addb_hmt_word, /* exact word hash */
  addb_hmt_bin,  /* reserved types */
  addb_hmt_reserved2,
  addb_hmt_reserved3,
  addb_hmt_reserved4,
  addb_hmt_typeguid, /* binary keys ... */
  addb_hmt_scope,
  addb_hmt_vip,
  addb_hmt_key,
  addb_hmt_gen,
  addb_hmt_reserved5,
  addb_hmt_LAST /* last type enum */

} addb_hmap_type;

typedef addb_hmap_id addb_hmap_iterator_position;

typedef struct addb_hmap_iterator {
  /**
   * @brief HMAP we're iterating in.
   *
   *  If NULL, the iteration hasn't begun yet.
   */
  addb_hmap* hmit_hmap;

  /**
   * @brief singleton value
   */
  addb_hmap_id hmit_single;

  addb_hmap_id hmit_gmap_source;
  addb_gmap_iterator hmit_gmap_iter;

  /**
   * @brief A singleton value is stored in hmit_single,
   * 	and has not been returned by an iterator yet.
   */
  unsigned int hmit_unread_singleton : 1;

  /**
   * @brief If true, see gmap iterator, !hmit_is_singleton
   */
  unsigned int hmit_see_gmap : 1;

  /**
   * @brief If true, run from low to high.  Otherwise, high to low.
   */
  unsigned int hmit_forward : 1;

} addb_hmap_iterator;

/**
 * @brief Report a name/value pair.
 *
 *  The addb_status() and addb_status_tiles() functions are invoked
 *  with this callback to report on the internal database state;
 *  they call it back with various name/value pairs.
 *
 * @param callback_data	opaque application data, third parameter
 *		to addb_istore_status() or addb_gmap_status()
 * @param name human-readable name of the value being reported,
 *		for example "gmap.partition.1.size"
 * @param value human-readable string value, for example "789008".
 *
 * @return 0 to continue, a non-zero error value to abort a status
 *	report and return that non-zero error value.
 */
typedef int addb_status_callback(void* callback_data, char const* name,
                                 char const* value);

/**
 * @brief Open a database for reading.
 *
 *  Used a part of the #ADDB_MODE_READ_ONLY and
 *  #ADDB_MODE_READ_WRITE access modes.
 */
#define ADDB_MODE_READ 0x01

/**
 * @brief Open a database for writing.
 *
 *  Used a part of the #ADDB_MODE_WRITE_ONLY and
 *  #ADDB_MODE_READ_WRITE access modes.
 */
#define ADDB_MODE_WRITE 0x02

/**
 * @brief Open a database for reading only.
 *
 *  Used as a flag passed to addb_istore_open() or
 *  addb_gmap_open().
 */
#define ADDB_MODE_READ_ONLY ADDB_MODE_READ

/**
 * @brief Open a database for writing only.
 *
 *  Used as a flag passed to addb_istore_open() or
 *  addb_gmap_open().
 */
#define ADDB_MODE_WRITE_ONLY ADDB_MODE_WRITE

/**
 * @brief Open a database for reading or writing.
 *
 *  Used as a flag passed to addb_istore_open() or
 *  addb_gmap_open().
 */
#define ADDB_MODE_READ_WRITE (ADDB_MODE_READ | ADDB_MODE_WRITE)

/**
 * @brief backup application
 *
 *  Used as an argument addb_tiled_get_loc when applying
 *  backup records...  Which we do not want to be backed up.
 */
#define ADDB_MODE_BACKUP ADDB_MODE_READ

/* addb-clock.c */

addb_msclock_t addb_msclock(addb_handle*);

/* addb-create.c */

addb_handle* addb_create(cm_handle* cm, cl_handle* cl,
                         unsigned long long total_memory, bool transactional);

/*
 * addb-statuc.c */

int addb_status(addb_handle* abbb, cm_prefix* prefix, addb_status_callback* cb,
                void* cb_data);

/* addb-destroy.c */

void addb_destroy(addb_handle*);

addb_istore* addb_istore_open(addb_handle* _addb, char const* _path, int _mode,
                              addb_istore_configuration* icf);
int addb_istore_close(addb_istore*);
int addb_istore_remove(addb_handle*, char const* _path);
int addb_istore_truncate(addb_istore* is, char const* path);

/*  Use:
 * 		read + free	-- read, free once you don't need the storage
 *		write		-- just write and return the ID.
 *		alloc + free	-- don't care about writing
 *		alloc + write	-- care about writing.
 */

/**
 * @brief Read a record from an istore database.
 *
 * @param is	istore database pointer, returned by addb_istore_open()
 * @param id 	local index of record to read
 * @param data	assign the resulting record to this.
 *  The pointer returned in the #data returned points to sections
 *  of the underlying database file that have been mapped into memory.
 *  These mappings are link counted and must be released explicitly
 *  with a call to addb_istore_free().
 *
 * @return 0 on success, a nonzero error code on error.
 * @return ADDB_ERR_NO if a record with this index doesn't exist.
 */
#define addb_istore_read(is, id, data) \
  addb_istore_read_loc(is, id, data, __FILE__, __LINE__)
int addb_istore_read_loc(addb_istore* _is, addb_istore_id _id, addb_data* _data,
                         char const* _file, int _line);

/**
 * @brief Release a record that was read from an istore database.
 *
 * @param is	istore database pointer, returned by addb_istore_open()
 * @param data	record returned by addb_istore_read().
 */
#define addb_istore_free(is, data) \
  addb_istore_free_loc(is, data, __FILE__, __LINE__)
void addb_istore_free_loc(addb_istore* _is, addb_data* _data, char const* _file,
                          int _line);

int addb_istore_write(addb_istore* _is, char const* _data, size_t _size,
                      addb_istore_id* _id_out);

int addb_istore_alloc(addb_istore* _is, size_t _size, addb_data* _data_out,
                      addb_istore_id* _id_out);

void addb_istore_reference_initialize(addb_istore_reference*);

/**
 * @brief Duplicate a reference into an istore database.
 *
 *  After a call to addb_istore_reference_dup, the addb_istore_reference
 *  can be free'd twice (possibly after duplicating it by assignment).
 *
 * @param iref	Pointer into the database.
 */
#define addb_istore_reference_dup(iref) \
  addb_istore_reference_dup_loc(iref, __FILE__, __LINE__)
void addb_istore_reference_dup_loc(addb_istore_reference* _iref,
                                   char const* _file, int _line);

/**
 * @brief Free a reference into an istore database.
 * @param iref	Pointer into the database.
 */
#define addb_istore_reference_free(iref) \
  addb_istore_reference_free_loc(iref, __FILE__, __LINE__)
void addb_istore_reference_free_loc(addb_istore_reference* _iref,
                                    char const* _file, int _line);

/**
 * @brief Given a data pointer, obtain its embedded reference.
 * @param iref	out: reference into the database.
 * @param data	data pointer, filled in by addb_istore_read.
 */
#define addb_istore_reference_from_data(iref, data) \
  addb_istore_reference_from_data_loc(iref, data, __FILE__, __LINE__)
void addb_istore_reference_from_data_loc(addb_istore_reference* _iref_out,
                                         addb_data const* _data,
                                         char const* _file, int _line);

addb_istore_id addb_istore_next_id(addb_istore const*);

int addb_istore_status(addb_istore* _store, cm_prefix const* _prefix,
                       addb_status_callback* _callback, void* _callback_data);

int addb_istore_status_tiles(addb_istore* _store, cm_prefix const* _prefix,
                             addb_status_callback* _callback,
                             void* _callback_data);

void addb_istore_configure(addb_istore* _store, addb_istore_configuration* _cf);

/* addb-istore-checkpoint.c */

int addb_istore_checkpoint_rollback(addb_istore* _is,
                                    unsigned long long horizon);

int addb_istore_checkpoint(addb_istore* _is, int sync, int block);

/* addb-istore-horizon.c */

addb_istore_id addb_istore_horizon(addb_istore const* is);
void addb_istore_horizon_set(addb_istore*, addb_istore_id);

/* addb-istore-marker.c */

int addb_istore_marker_horizon_write_start(addb_istore*, bool);
int addb_istore_marker_horizon_write_finish(addb_istore*, bool);
int addb_istore_marker_next_checkpoint(addb_istore*, bool);

addb_istore_id addb_istore_marker_next(addb_istore* is);
addb_istore_id addb_istore_marker_horizon(addb_istore* is);

int addb_istore_refresh(addb_istore* is, unsigned long long n);
/* addb-flat-open.c */

addb_flat* addb_flat_open(addb_handle* _addb, char const* _path, int _mode,
                          char const* _data, size_t _size);

/* addb-flat-close.c */

int addb_flat_close(addb_flat*);

/* addb-flat-read.c */

int addb_flat_read(addb_flat*, addb_data*);

/* addb-flat-remove.c */

int addb_flat_remove(addb_handle*, char const*);

/* addb-flat-write.c */

int addb_flat_write(addb_flat*, char const*, size_t);

int addb_gmap_remove(addb_handle*, char const*);
int addb_gmap_truncate(addb_gmap* gm, char const* path);

int addb_gmap_close(addb_gmap*);
void addb_gmap_configure(addb_gmap*, addb_gmap_configuration*);

/* addb-gmap-add.c */

int addb_gmap_add(addb_gmap* _gm, addb_gmap_id _source, addb_gmap_id _dest,
                  bool _exclusive);

/* addb-gmap-array.c */

int addb_gmap_idarray(addb_gmap* _gm, addb_gmap_id _source, addb_idarray* _ida);

int addb_gmap_array_n(addb_gmap* _gm, addb_gmap_id _source,
                      unsigned long long* _n_out);

int addb_gmap_array_n_bounded(addb_gmap* _gm, addb_gmap_id _source,
                              unsigned long long _upper_bound,
                              unsigned long long* _n_out);

int addb_gmap_array_last(addb_gmap* _gm, addb_gmap_id _source,
                         addb_gmap_id* _id_out);

/* addb-gmap-backup.c */

int addb_gmap_backup(addb_gmap*, unsigned long long);

/* addb-gmap-checkpoint.c */

int addb_gmap_checkpoint_rollback(addb_gmap* _gm);

int addb_gmap_checkpoint_finish_backup(addb_gmap* gm, bool hard_sync,
                                       bool block);
int addb_gmap_checkpoint_sync_backup(addb_gmap* gm, bool hard_sync, bool block);
int addb_gmap_checkpoint_sync_directory(addb_gmap* gm, bool hard_sync,
                                        bool block);
int addb_gmap_checkpoint_start_writes(addb_gmap* gm, bool hard_sync,
                                      bool block);
int addb_gmap_checkpoint_finish_writes(addb_gmap* gm, bool hard_sync,
                                       bool block);
int addb_gmap_checkpoint_remove_backup(addb_gmap* gm, bool hard_sync,
                                       bool block);

/* addb-gmap-open.c */

addb_gmap* addb_gmap_open(addb_handle* addb, char const* path, int mode,
                          unsigned long long horizon,
                          addb_gmap_configuration* gcf);

/* addb-gmap-horizon.c */

unsigned long long addb_gmap_horizon(addb_gmap*);

void addb_gmap_horizon_set(addb_gmap* gm, unsigned long long horizon);

/* addb-gmap-iterator.c */

/**
 * @brief Pull the next element from an iterator.
 *
 * @param gm		gmap database pointer, returned by addb_gmap_open()
 * @param source	array we're iterating over
 * @param iter		iterator
 * @param out		out: the current id
 *
 * @return 0 on success
 * @return ADDB_ERR_NO if the iterator ran out of elements.
 */
#define addb_gmap_iterator_next(gm, source, iter, out) \
  addb_gmap_iterator_next_loc(gm, source, iter, out, __FILE__, __LINE__)
int addb_gmap_iterator_next_loc(addb_gmap* _gm, addb_gmap_id _source,
                                addb_gmap_iterator* _iter, addb_gmap_id* _out,
                                char const* _file, int _line);

#define addb_gmap_iterator_find(gm, source, iter, id, ch) \
  addb_gmap_iterator_find_loc(gm, source, iter, id, ch, __FILE__, __LINE__)
int addb_gmap_iterator_find_loc(addb_gmap* _gm, addb_gmap_id _source,
                                addb_gmap_iterator* _iter,
                                addb_gmap_id* _id_in_out, bool* _changed_out,
                                char const* _file, int _line);
int addb_gmap_iterator_set_position(addb_gmap* _gm, addb_gmap_id _source,
                                    addb_gmap_iterator* _iter,
                                    addb_gmap_iterator_position const* _pos);

void addb_gmap_iterator_get_position(addb_gmap* _gm, addb_gmap_id _source,
                                     addb_gmap_iterator* _iter,
                                     addb_gmap_iterator_position* _pos_out);

int addb_gmap_iterator_set_offset(addb_gmap* _gm, addb_gmap_id _source,
                                  addb_gmap_iterator* _iter,
                                  unsigned long long _offset);

void addb_gmap_iterator_finish(addb_gmap_iterator*);

void addb_gmap_iterator_initialize(addb_gmap_iterator*);

int addb_gmap_iterator_n(addb_gmap* gm, addb_gmap_id source,
                         addb_gmap_iterator* iter, unsigned long long* n_out);

void addb_gmap_iterator_unget(addb_gmap* _gm, addb_gmap_id _source,
                              addb_gmap_iterator* _iter, addb_gmap_id _id);

char const* addb_gmap_iterator_to_string(addb_gmap* _gm, addb_gmap_id _source,
                                         addb_gmap_iterator* _iter, char* _buf,
                                         size_t _size);

int addb_gmap_array_nth(addb_gmap* _gm, addb_gmap_id _source,
                        unsigned long long _offset, addb_gmap_id* _id_out);

int addb_gmap_status(addb_gmap* _gm, cm_prefix const* _prefix,
                     addb_status_callback* _callback, void* _callback_data);

int addb_gmap_status_tiles(addb_gmap* _gm, cm_prefix const* _prefix,
                           addb_status_callback* _callback,
                           void* _callback_data);

void addb_gmap_iterator_set_forward(addb_gmap* _gm, addb_gmap_iterator* _iter,
                                    bool _forward);

int addb_gmap_refresh(addb_gmap* gm, unsigned long long last_id);
/* addb-hmap.c */

int addb_hmap_add(addb_hmap* hm, unsigned long long hash_of_key,
                  char const* key, size_t key_len, addb_hmap_type type,
                  addb_gmap_id id);

int addb_hmap_read_value(addb_hmap* hm, unsigned long long hash_of_key,
                         char const* key, size_t key_len, addb_hmap_type type,
                         addb_gmap_id* val_out);

int addb_hmap_open(addb_handle* addb, char const* path, int mode,
                   unsigned long long n_slots, unsigned long long horizon,
                   addb_hmap_configuration* hcf, addb_gmap_configuration* gcf,
                   addb_hmap** hm_out);

int addb_hmap_close(addb_hmap* hm);

int addb_hmap_remove(addb_handle* addb, char const* path);

int addb_hmap_truncate(addb_hmap* hm, char const* path);

int addb_hmap_array_n_bounded(addb_hmap* hm, unsigned long long hash_of_key,
                              char const* key, size_t key_len,
                              addb_hmap_type type,
                              unsigned long long upper_bound,
                              unsigned long long* n_out);

int addb_hmap_array_n(addb_hmap* hm, unsigned long long hash_of_key,
                      char const* key, size_t key_len, addb_hmap_type type,
                      unsigned long long* n_out);

int addb_hmap_last(addb_hmap* hm, unsigned long long hash_of_key,
                   char const* key, size_t key_len, addb_hmap_type type,
                   addb_gmap_id* val_out);

int addb_hmap_array_nth(addb_hmap* hm, unsigned long long hash_of_key,
                        char const* key, size_t key_len, addb_hmap_type type,
                        unsigned long long i, addb_gmap_id* id_out);

int addb_hmap_status(addb_hmap* hm, cm_prefix const* prefix,
                     addb_status_callback* callback, void* callback_data);

int addb_hmap_status_tiles(addb_hmap* hm, cm_prefix const* prefix,
                           addb_status_callback* callback, void* callback_data);

/* addb-hmap-iterator.c */

void addb_hmap_iterator_initialize(addb_hmap_iterator* iter);

int addb_hmap_iterator_finish(addb_hmap_iterator* iter);

/**
 * @brief special addb_hmap_iterator_position, means "at the very beginning".
 */
#define ADDB_HMAP_POSITION_START (1ull << 34)

/**
 * @brief special addb_hmap_iterator_position, means "at the very end".
 */
#define ADDB_HMAP_POSITION_END ((1ull << 34) | 1)

#define addb_hmap_iterator_next(gm, hok, key, key_len, type, iter, out) \
  addb_hmap_iterator_next_loc(gm, hok, key, key_len, type, iter, out,   \
                              __FILE__, __LINE__)

int addb_hmap_iterator_next_loc(addb_hmap* hm, addb_hmap_id hash_of_key,
                                char const* const key, size_t key_len,
                                addb_hmap_type type, addb_hmap_iterator* iter,
                                addb_hmap_id* out, char const* file, int line);

int addb_hmap_iterator_set_offset(addb_hmap* hm, addb_hmap_id hash_of_key,
                                  char const* const key, size_t key_len,
                                  addb_hmap_type type, addb_hmap_iterator* iter,
                                  unsigned long long i);

int addb_hmap_iterator_n(addb_hmap* hm, addb_hmap_id hash_of_key,
                         char const* const key, size_t key_len,
                         addb_hmap_type type, addb_hmap_iterator* iter,
                         unsigned long long* n_out);

#define addb_hmap_iterator_find(hm, hash_of_key, key, key_len, type, iter, \
                                id_in_out, changed_out)                    \
  addb_hmap_iterator_find_loc(hm, hash_of_key, key, key_len, type, iter,   \
                              id_in_out, changed_out, __FILE__, __LINE__)

int addb_hmap_iterator_find_loc(addb_hmap* hm, addb_hmap_id hash_of_key,
                                char const* const key, size_t key_len,
                                addb_hmap_type type, addb_hmap_iterator* iter,
                                addb_hmap_id* id_in_out, bool* changed_out,
                                char const* file, int line);

void addb_hmap_iterator_unget(addb_hmap* hm, addb_hmap_iterator* iter);

void addb_hmap_iterator_get_position(addb_hmap* hm, addb_hmap_iterator* iter,
                                     addb_hmap_iterator_position* pos_out);

int addb_hmap_iterator_set_position(addb_hmap* gm, addb_hmap_id hash_of_key,
                                    char const* const key, size_t key_len,
                                    addb_hmap_type type,
                                    addb_hmap_iterator* iter,
                                    addb_hmap_iterator_position const* pos);

#define addb_hmap_sparse_iterator_next(gm, source, type, iter, out)         \
  addb_hmap_sparse_iterator_next_loc(gm, source, type, iter, out, __FILE__, \
                                     __LINE__)

int addb_hmap_sparse_iterator_next_loc(addb_hmap* hm, addb_hmap_id source,
                                       addb_hmap_type type,
                                       addb_hmap_iterator* iter,
                                       addb_hmap_id* out, char const* file,
                                       int line);

int addb_hmap_sparse_iterator_set_offset(addb_hmap* hm, addb_hmap_id source,
                                         addb_hmap_type type,
                                         addb_hmap_iterator* iter,
                                         unsigned long long i);

int addb_hmap_sparse_iterator_n(addb_hmap* hm, addb_hmap_id source,
                                addb_hmap_type type, addb_hmap_iterator* iter,
                                unsigned long long* n_out);

#define addb_hmap_sparse_iterator_find(hm, source, type, iter, id_in_out, \
                                       changed_out)                       \
  addb_hmap_sparse_iterator_find_loc(hm, source, type, iter, id_in_out,   \
                                     changed_out, __FILE__, __LINE__)

int addb_hmap_sparse_iterator_find_loc(addb_hmap* hm, addb_hmap_id source,
                                       addb_hmap_type type,
                                       addb_hmap_iterator* iter,
                                       addb_hmap_id* id_in_out,
                                       bool* changed_out, char const* file,
                                       int line);

#define addb_hmap_sparse_iterator_unget(hm, iter) \
  addb_hmap_iterator_unget(hm, iter)

#define addb_hmap_sparse_iterator_get_position(hm, iter, pos_out) \
  addb_hmap_iterator_get_position(hm, iter, pos_out)

int addb_hmap_sparse_iterator_set_position(
    addb_hmap* hm, addb_hmap_id source, addb_hmap_type type,
    addb_hmap_iterator* iter, addb_hmap_iterator_position const* pos);

void addb_hmap_iterator_set_forward(addb_hmap* _hmap, addb_hmap_iterator* _iter,
                                    bool _forward);

/* addb-hmap-checkpoint.c */

int addb_hmap_checkpoint(addb_hmap* hm, unsigned long long horizon,
                         addb_msclock_t deadline, int hard_sync);

int addb_hmap_checkpoint_rollback(addb_hmap* hm);

int addb_hmap_backup(addb_hmap* hm, unsigned long long horizon);

char const* addb_hmap_iterator_to_string(addb_hmap* hm,
                                         addb_hmap_id hash_of_key,
                                         char const* const key, size_t key_len,
                                         addb_hmap_type type,
                                         addb_hmap_iterator* iter, char* buf,
                                         size_t size);

int addb_hmap_checkpoint_finish_backup(addb_hmap* hm, bool hard_sync,
                                       bool block);
int addb_hmap_checkpoint_sync_backup(addb_hmap* hm, bool hard_sync, bool block);
int addb_hmap_checkpoint_sync_directory(addb_hmap* hm, bool hard_sync,
                                        bool block);
int addb_hmap_checkpoint_start_writes(addb_hmap* hm, bool hard_sync,
                                      bool block);
int addb_hmap_checkpoint_finish_writes(addb_hmap* hm, bool hard_sync,
                                       bool block);
int addb_hmap_checkpoint_remove_backup(addb_hmap* hm, bool hard_sync,
                                       bool block);

unsigned long long addb_hmap_horizon(addb_hmap* hm);

void addb_hmap_horizon_set(addb_hmap* hm, unsigned long long horizon);

int addb_hmap_sparse_add(addb_hmap* hm, addb_gmap_id source,
                         addb_hmap_type type, addb_gmap_id id);

int addb_hmap_sparse_array_n_bounded(addb_hmap* hm, addb_gmap_id source,
                                     addb_hmap_type type,
                                     unsigned long long upper_bound,
                                     unsigned long long* n_out);

int addb_hmap_sparse_array_n(addb_hmap* hm, addb_gmap_id source,
                             addb_hmap_type type, unsigned long long* n_out);

int addb_hmap_sparse_last(addb_hmap* hm, addb_gmap_id source,
                          addb_hmap_type type, addb_gmap_id* val_out);

int addb_hmap_sparse_array_nth(addb_hmap* hm, addb_gmap_id source,
                               addb_hmap_type type, unsigned long long i,
                               addb_gmap_id* id_out);

void addb_hmap_configure(addb_hmap* hmap, addb_hmap_configuration* hcf,
                         addb_gmap_configuration*);

int addb_hmap_idarray(addb_hmap* hm, unsigned long long hash_of_key,
                      char const* key, size_t key_len, addb_hmap_type type,
                      addb_idarray* ida);

int addb_hmap_sparse_idarray(addb_hmap* hm, addb_gmap_id source,
                             addb_hmap_type type, addb_idarray* ida);

int addb_hmap_refresh(addb_hmap* hm, unsigned long long n);
/* addb-idarray.c */

void addb_idarray_single(cl_handle*, addb_idarray*, addb_id);
void addb_idarray_multiple(cl_handle*, addb_idarray*);
void addb_idarray_finish(addb_idarray* ida);
void addb_idarray_initialize(addb_idarray* ida);

int addb_idarray_read_raw(addb_idarray* ida, unsigned long long start_ofset,
                          unsigned long long end_offset,
                          unsigned char const** ptr_out,
                          unsigned long long* end_offset_out);

int addb_idarray_read(addb_idarray* ida, unsigned long long start,
                      unsigned long long end, addb_id* id_buf,
                      unsigned long long* end_out);

int addb_idarray_read1(addb_idarray const* _ida, unsigned long long _offset,
                       addb_id* _id_out);

int addb_idarray_search(addb_idarray* ida, unsigned long long s,
                        unsigned long long e, addb_id id,
                        unsigned long long* off_out, addb_id* id_out);

unsigned long long addb_idarray_n(addb_idarray const* ida);

/* addb-idarray-intersect.c */

int addb_idarray_intersect(addb_handle* _addb, addb_idarray* _a,
                           unsigned long long _a_s, unsigned long long _a_e,
                           addb_idarray* _b, unsigned long long _b_s,
                           unsigned long long _b_e, addb_id* _id_inout,
                           size_t* _n_inout, size_t _m);

int addb_idarray_fixed_intersect(addb_handle* _addb, addb_idarray* _a,
                                 unsigned long long _a_s,
                                 unsigned long long _a_e, addb_id* _b_base,
                                 size_t _b_n, addb_id* _id_inout,
                                 size_t* _n_inout, size_t _m);

/* addb-gmap-bmap.c */

int addb_gmap_bgmap_read_size(addb_gmap* gm, addb_gmap_id id,
                              unsigned long long* n);

int addb_bgmap_fixed_intersect(addb_handle* addb, struct addb_bgmap* bgm,
                               addb_id const* id_in, size_t n_in,
                               addb_id* id_out, size_t* n_out, size_t m);

/* addb-serial.c */

unsigned long addb_lazy_reload(addb_handle* addb);
/* addb-strerror.c */

char const* addb_strerror(int);
char const* addb_xstrerror(int);

#endif /* ADDB_H */
