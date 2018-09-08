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
#ifndef PDBP_H
#define PDBP_H

#include "libpdb/pdb.h"

#include <stdbool.h>

#include "libcl/cl.h"
#include "libcm/cm.h"
#include "libaddb/addb.h"
#include "libaddb/addb-hmap.h"
#include "libaddb/addb-bmap.h"

#define PDB_PATH_DEFAULT "./PDB"

/* The stages of the index checkpoint process
 */
typedef enum pdb_checkpoint_stage {
  PDB_CKS_START,
  PDB_CKS_FINISH_BACKUP,
  PDB_CKS_SYNC_BACKUP,
  PDB_CKS_SYNC_DIRECTORY,
  PDB_CKS_START_WRITES,
  PDB_CKS_FINISH_WRITES,
  PDB_CKS_START_MARKER,
  PDB_CKS_FINISH_MARKER,
  PDB_CKS_REMOVE_BACKUP,
  PDB_CKS_N, /* number of checkpoint stages */
} pdb_checkpoint_stage;

typedef struct pdb_index_instance pdb_index_instance;

/* An index checkpoint proceeds in stages with an individual
 * checkpoint function for each stage.
 *
 * Returns:
 *	0  if this stage is complete
 * 	PDB_ERR_MORE  if this stage needs to wait for IO
 *	errno   for some sort of error
 */

typedef int pdb_checkpoint_fn(pdb_index_instance* ii, bool hard_sync,
                              bool block);

/* An abstraction for indexes
 *
 *	Generically, an index is a function which takes a key and
 *      returns a value, which in our case will always be a set of
 *	local ids.
 *
 *	Since the keys vary depending on the type of index, we can't
 *	abstract index lookups, but we can provide a consistent interface
 *	to the remainder of the index operations.
 *
 *	In previous versions of this code we had two index maintenance
 *	functions: *_synchronize and *_add.  The add function was used
 *	for primitives known to be "new," the synchronize function otherwise.
 *	Since the system maintained no exact global notion of index
 *	horizon, invocations of the _synchronize functions needed to
 *	ignore the case where redundant index entries were made.  Aside
 *	from the fragility inherent in having two nearly identical functions,
 *	not all types of indices can detect redundant entries.
 *
 *	Since index writing now happens entirely in the background, there's
 *	no reason not to keep all indices synchronized to exactly the same
 *	horizon, and hence no need for _synchronize and _add functions so,
 *	they've been merged into a single function, pdb_index_new_primitive.
 */

typedef struct pdb_index_type {
  char const* ixt_name; /* the name of this index type (ie. gmap, hmap) */

  /* Close an index.
   */
  int (*ixt_close)(pdb_handle* pdb, pdb_index_instance* ii);

  /* Delete an index's content
   */
  int (*ixt_truncate)(pdb_handle* pdb, pdb_index_instance* ii);

  /* Report the status of an index
   */
  int (*ixt_status)(pdb_handle* pdb, pdb_index_instance* ii, cm_prefix const*,
                    pdb_status_callback* callback, void* callback_data);

  /* Report the status of an index, tile statistics
   */
  int (*ixt_status_tiles)(pdb_handle* pdb, pdb_index_instance* ii,
                          cm_prefix const*, pdb_status_callback* callback,
                          void* callback_data);

  /* Retrieve the index's current horizon (where a rollback would
   * take us to).
   */
  unsigned long long (*ixt_horizon)(pdb_handle* pdb, pdb_index_instance* ii);

  /* Mark the index as "in sync" up to the passed horizon.
   */
  void (*ixt_advance_horizon)(pdb_handle* pdb, pdb_index_instance* ii,
                              unsigned long long horizon);

  /* Rollback to the previous consistent state (set by the most recent
   * call to advance_horizon)
   */
  int (*ixt_rollback)(pdb_handle* pdb, pdb_index_instance* ii);

  /*
   * Database may have changed on disk or shared memory. Update
   * internal strucctures as needed
   */
  int (*itx_refresh)(pdb_handle* pdb, pdb_index_instance* ii,
                     unsigned long long n);

  /* Checkpoint stage functions, see: pdb_checkpoint_optional
   */
  pdb_checkpoint_fn* ixt_checkpoint_fns[PDB_CKS_N];
} pdb_index_type;

extern pdb_index_type pdb_index_gmap;
extern pdb_index_type pdb_index_hmap;
extern pdb_index_type pdb_index_bmap;

/*  An index instance is the fixed-size slot in the pdb world
 *  which holds an an index.  A pointer to the  index implementation
 *  structure is held in the ii_impl union which is extended as new
 *  index types are added.
 */

struct pdb_index_instance {
  /* type of this instance (jump table) */
  pdb_index_type* ii_type;

  /* path of this index */
  char const* ii_path;

  /* current checkpoint stage */
  pdb_checkpoint_stage ii_stage;

  union {
    addb_hmap* hm;
    addb_gmap* gm;
    addb_bmap* bm;
    void* any;
  } ii_impl;
};

/*  Cached estimates for prefixes; used by the
 *  graphd prefix iterator to delay instantiating gigantic
 *  OR-iterators for single- and double-letter prefixes.
 */
typedef struct pdb_prefix_statistics {
  unsigned short pps_find_cost;
  unsigned short pps_next_cost;
  unsigned long long pps_n;
  unsigned long long pps_drift;

} pdb_prefix_statistics;

/*  The pdb_primitive_callback is public and defined
 *  in pdb.h.
 */
typedef struct pdb_primitive_subscription {
  pdb_primitive_callback* pps_callback;
  void* pps_callback_data;

  struct pdb_primitive_subscription *pps_next, *pps_prev;

} pdb_primitive_subscription;

struct pdb_handle {
  graph_handle* pdb_graph;
  addb_handle* pdb_addb;
  cl_handle* pdb_cl;
  cm_handle* pdb_cm;
  pdb_runtime_statistics pdb_runtime_statistics;
  unsigned long long pdb_database_id;
  graph_guid pdb_database_guid;
  pdb_id pdb_id_on_disk;
  int pdb_version;

#define PDB_MODE_READ 0x01
#define PDB_MODE_WRITE 0x02
#define PDB_MODE_READ_ONLY PDB_MODE_READ
#define PDB_MODE_WRITE_ONLY PDB_MODE_WRITE
#define PDB_MODE_READ_WRITE (PDB_MODE_READ | PDB_MODE_WRITE)

  int pdb_mode;

  char* pdb_path;
  char* pdb_lockfile_path;

  char* pdb_primitive_path;
  addb_istore* pdb_primitive;

  char* pdb_header_path;
  addb_flat* pdb_header;

  pdb_index_instance pdb_indices[PDB_INDEX_N];

#define pdb_left pdb_indices[PDB_INDEX_LEFT].ii_impl.gm
#define pdb_left_path pdb_indices[PDB_INDEX_LEFT].ii_path
#define pdb_right pdb_indices[PDB_INDEX_RIGHT].ii_impl.gm
#define pdb_right_path pdb_indices[PDB_INDEX_RIGHT].ii_path
#define pdb_typeguid pdb_indices[PDB_INDEX_TYPEGUID].ii_impl.gm
#define pdb_typeguid_path pdb_indices[PDB_INDEX_TYPEGUID].ii_path
#define pdb_scope pdb_indices[PDB_INDEX_SCOPE].ii_impl.gm
#define pdb_scope_path pdb_indices[PDB_INDEX_SCOPE].ii_path
#define pdb_hmap pdb_indices[PDB_INDEX_HMAP].ii_impl.hm
#define pdb_hmap_path pdb_indices[PDB_INDEX_HMAP].ii_path
#define pdb_prefix pdb_indices[PDB_INDEX_PREFIX].ii_impl.bm
#define pdb_prefix_path pdb_indices[PDB_INDEX_PREFIX].ii_path
#define pdb_versioned pdb_indices[PDB_INDEX_DEAD].ii_impl.bm
#define pdb_versioned_path pdb_indices[PDB_INDEX_DEAD].ii_path

  /* New index horizon for the ongoing checkpoint
   */
  unsigned long long pdb_new_index_horizon;

  pdb_configuration pdb_cf;

  /**
   *  @brief How much memory do we have, in total?
   *  From sysinfo (or a guess of 1G).
   */
  unsigned long long pdb_total_mem;
  unsigned int pdb_predictable : 1;

  /**
   * @brief Internal incrementing iterator id, used
   *  	to identify clone replacements.
   */
  unsigned long pdb_iterator_id;

  /**
   * @brief Do we have disk space left, or are we out?
   */
  unsigned int pdb_disk_available : 1;
  time_t pdb_disk_warning;

  bool pdb_deficit_exceeded;
  time_t pdb_started_checkpoint;
  bool pdb_active_checkpoint_sync;

  pdb_prefix_statistics pdb_prefix_statistics[32 * 32];

  pdb_primitive_subscription *pdb_primitive_alloc_head,
      *pdb_primitive_alloc_tail;

  pdb_iterator_chain pdb_iterator_chain_buf, *pdb_iterator_chain;

  pdb_iterator_chain pdb_iterator_suspend_chain;

  /*  If this counter is larger than 0, there may be
   *  unsuspended iterators that require suspending
   *  before a write.
   */
  size_t pdb_iterator_n_unsuspended;

  /*  The indentation depth of RXS log lines.
   */
  size_t pdb_rxs_depth;

  /*  If non-NULL, translation table from GUID to ID.
   */
  graph_grmap* pdb_concentric_map;
};
#define PDB_GUID_IS_LOCAL(pdb, guid) \
  (GRAPH_GUID_DB(guid) == (pdb)->pdb_database_id)

unsigned long pdb_local_ip(void);

/* pdb-configure.c */

void pdb_configure_databases(pdb_handle* pdb, bool _is_default);
void pdb_configure(pdb_handle*, pdb_configuration const*);

/* pdb-count.c */

int pdb_count_gmap_est(pdb_handle* _pdb, addb_gmap* _gm, pdb_id _source,
                       pdb_id _low, pdb_id _high,
                       unsigned long long _upper_bound,
                       unsigned long long* _n_out);

int pdb_count_gmap(pdb_handle* _pdb, addb_gmap* _gm, pdb_id _source,
                   pdb_id _low, pdb_id _high, unsigned long long _upper_bound,
                   unsigned long long* _n_out);

int pdb_count_hmap(pdb_handle* _pdb, addb_hmap* _hm, addb_hmap_id _hash_of_key,
                   char const* const _key, size_t _key_len,
                   addb_hmap_type _type, pdb_id _low, pdb_id _high,
                   unsigned long long _upper_bound, unsigned long long* _n_out);

/* pdb-from-node.c */

int pdb_from_node_add(pdb_handle* _pdb, pdb_id _node_id, pdb_id _link_id);

int pdb_from_node_synchronize(pdb_handle* _pdb, pdb_id _id,
                              pdb_primitive const* _pr);

/* pdb-hash.c */

char const* pdb_hash_type_to_string(int);
int pdb_hash_add(pdb_handle* pdb, addb_hmap_type t, char const* key,
                 size_t key_len, pdb_id id);

int pdb_hash_synchronize(pdb_handle* _pdb, pdb_id _id,
                         pdb_primitive const* _pr);

/* pdb-id.c */

#define pdb_id_iterator_from_application_key(a, b, c, d) \
  pdb_id_iterator_from_application_key_loc(a, b, c, d, __FILE__, __LINE__)

int pdb_id_iterator_from_application_key_loc(
    pdb_handle* _pdb, pdb_id* _appkey_id_out,
    addb_hmap_iterator* _appkey_iter_out, graph_guid const* _guid,
    char const* _file, int _line);

int pdb_id_add_guid(pdb_handle* _pdb, pdb_id _original_id,
                    graph_guid const* _guid);

int pdb_id_synchronize(pdb_handle* _pdb, pdb_id _id, pdb_primitive const* _pr);

/* pdb-initialize.c */

int pdb_configure_done(pdb_handle*);
int pdb_initialize_names(pdb_handle*);
int pdb_initialize(pdb_handle*);
int pdb_initialize_open_databases(pdb_handle* pdb);
int pdb_initialize_open_header(pdb_handle* pdb);

/* pdb-is-remote-mounted.c */

bool pdb_is_remote_mounted(char const*);

/* pdb-lockfile.c */

int pdb_lockfile_create(pdb_handle*, char const*);
int pdb_lockfile_rewrite(pdb_handle*, char const*, pid_t);

/* pdb-to-node.c */

int pdb_to_node_add(pdb_handle* _pdb, pdb_id _node_id, pdb_id _link_id);
int pdb_to_node_synchronize(pdb_handle* _pdb, pdb_id _id,
                            pdb_primitive const* _pr);

/* pdb-index.c */

char const* pdb_index_name(int);
int pdb_index_do_checkpoint_stage(pdb_handle* pdb, pdb_index_instance* ii,
                                  pdb_checkpoint_stage target_stage,
                                  bool hard_sync, bool block);

int pdb_index_new_primitive(pdb_handle* pdb, pdb_id id,
                            pdb_primitive const* pr);

extern pdb_index_type pdb_index_gmap;
extern pdb_index_type pdb_index_hmap;

/* pdb-iterator-bgmap.c */

int pdb_iterator_bgmap_idarray_intersect(pdb_handle* pdb, pdb_iterator* bgmap,
                                         addb_idarray* ida, pdb_id low,
                                         pdb_id high, pdb_id* id_out,
                                         size_t* n_out, size_t m);

int pdb_iterator_bgmap_fixed_intersect(pdb_handle* pdb, pdb_iterator* bgmap,
                                       pdb_id const* id_in, size_t n_in,
                                       pdb_id* id_out, size_t* id_n,
                                       size_t id_m);

/* pdb-linkage.c */

addb_gmap* pdb_linkage_to_gmap(pdb_handle* pdb, int linkage);

/* pdb-primitive-alloc-subscription.c */

int pdb_primitive_alloc_subscription_call(pdb_handle* _pdb, pdb_id _id,
                                          pdb_primitive const* _pr);

void pdb_primitive_alloc_subscription_free(pdb_handle*);

/* pdb-util.c */

int pdb_scan_ull(char const** _s_ptr, char const* _e,
                 unsigned long long* _ull_out);

/* pdb-vip.c */

int pdb_vip_hmap_primitive_summary(pdb_handle* pdb, char const* key,
                                   size_t size,
                                   pdb_primitive_summary* psum_out);

/* pdb-word.c */

typedef int pdb_word_chop_callback(void* data, pdb_handle* pdb, pdb_id id,
                                   char const* s, char const* e);

int pdb_word_chop(void* data, pdb_handle* pdb, pdb_id id, const char* s,
                  const char* e, pdb_word_chop_callback* callback);

addb_gmap* pdb_gmap_by_name(pdb_handle* pdb, char const* s, char const* e);

int pdb_versioned_synchronize(pdb_handle* pdb, pdb_id id,
                              const pdb_primitive* pr);

int pdb_value_bin_synchronize(pdb_handle* pdb, pdb_id id,
                              pdb_primitive const* pr);

int pdb_bin_one_iterator(pdb_handle* pdb, const char* start, bool forward,
                         pdb_iterator** it);

int pdb_iterator_bin_thaw(pdb_handle* pdb, pdb_iterator_text const* pit,
                          pdb_iterator_base* pib, pdb_iterator** it_out);

char* pdb_render_unsafe_text(pdb_handle* pdb, char* buf_out, size_t size,
                             size_t* used_out, const char* s, const char* e);

int pdb_bin_compare(pdb_handle* pdb, const char* a_s, const char* a_e,
                    const char* b_s, const char* b_e);

int pdb_value_bin_synchronize(pdb_handle* pdb, pdb_id id,
                              pdb_primitive const* pr);

char* pdb_number_to_string(cm_handle* cm, const graph_number* n);

extern const char* pdb_bins_string_table[];
extern size_t pdb_bins_string_size;

extern size_t pdb_bins_number_size;
extern const char* pdb_bins_number_table[];
#endif /* PDBP_H */
