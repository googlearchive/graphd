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
#ifndef ADDBP_H
#define ADDBP_H

#include "libaddb/addb.h"
#include "libaddb/addb-scalar.h"

#include <stdbool.h> /* bool */
#include <stdlib.h>  /* size_t */
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>  /* off_t */

#include "libcl/cl.h"
#include "libcm/cm.h"

/*  Opaque handle structure managed by addb.c.
 */
typedef struct addb_tiled_pool addb_tiled_pool;
struct addb_bmap;

struct addb_handle {
  /*  Whenever a time is modified, the sequence number is incremented
   *  and stored as the "last-modified" date of the tile.
   */
  addb_opcount_t addb_opcount;

  cl_handle *addb_cl;
  cm_handle *addb_cm;

  /* The amount of locked (in the sense of mlock) memory.  A value
  * of -1 indicates that we are no longer locking memory.
  */
  long long addb_bytes_locked;

  /* The amount of memory available for mlocking
  */
  unsigned long long addb_mlock_max;

  /* Enable transactional writes
   *
   * See `pcf_transactional' for more information.
   */
  bool addb_transactional;

  /* Default size to use if initializing a new bmap.
   */
  unsigned long long addb_bmap_default_filesize;

  addb_tiled_pool *addb_master_tiled_pool;

  size_t addb_fsync_started;
  size_t addb_fsync_finished;
};

#define ADDB_MAGIC_SIZE 4
#define ADDB_BACKUP_MAGIC "ab1t" /* Addb Backup v1 Tiles */

#define addb_round_up(v, f) ((((v) + ((f)-1)) / (f)) * (f))
#define addb_round_down(v, f) (((v) / (f)) * (f))

#include "libaddb/addb-istore.h"
#include "libaddb/addb-gmap.h"

/* addb-file.c */

int addb_file_read(addb_handle *_addb, int _fd, char const *_name, char *_s,
                   size_t _n, bool _expect_eof);

int addb_file_lseek(addb_handle *_addb, int _fd, char const *_name, off_t _off,
                    int _whence);

int addb_file_truncate(addb_handle *_addb, int _fd, char const *_name,
                       off_t _off);

int addb_file_sync(addb_handle *_addb, int _fd, char const *_name);

int addb_file_rename(addb_handle *_addb, char const *_source,
                     char const *_target, bool _sync);

int addb_file_write(addb_handle *_addb, int _fd, char const *_name,
                    char const *_s, size_t _n);

int addb_file_unlink(addb_handle *, char const *);

int addb_file_munmap(cl_handle *_cl, char const *_name, char *_ptr,
                     size_t _size);

int addb_file_close(addb_handle *_addb, int _fd, char const *_name);

int addb_file_mkdir(addb_handle *, char const *);

int addb_file_next(addb_handle *_addb, char const *_name,
                   char **_filename_inout, void **_pointer_inout);

int addb_file_grow(cl_handle *_cl, int _fd, char const *_name, off_t _size);

int addb_file_sabotage(cl_handle *_cl, char const *_file, int _line,
                       char const *_func);

int addb_file_advise_log(cl_handle *cl, int fd, char const *filename);

int addb_file_advise_random(cl_handle *_cl, int _fd, char const *_filename);

void addb_file_sync_initialize(addb_handle *, addb_fsync_ctx *);

int addb_file_sync_start(cl_handle *cl, int fd, addb_fsync_ctx *fsc,
                         char const *filename, bool is_directory);

int addb_file_sync_finish(cl_handle *cl, addb_fsync_ctx *fsc, bool block,
                          char const *filename);

int addb_file_sync_cancel(cl_handle *cl, int fd, addb_fsync_ctx *fsc,
                          char const *filename);

int addb_file_fstat(cl_handle *_cl, int _fd, char const *_filename,
                    struct stat *_sb);

/* addb-mem.c */

int addb_mem_icmp(char const *s, char const *t, size_t n);

#define except_throw(x) goto x
#define except_catch(x) \
  while (0) x:
#define except_hthrow(cl, x)                                              \
  do {                                                                    \
    cl_log(cl, CL_LEVEL_FAIL, "*** exception %s:%d", __FILE__, __LINE__); \
    goto x;                                                               \
  } while (0)

/* addb-tiled.c */

#define ADDB_TILED_REFERENCE_INITIALIZE(tref) \
  ((tref) = ADDB_TILED_REFERENCE_EMPTY)

unsigned long long addb_tiled_first_map(addb_tiled *td);

unsigned char const *addb_tiled_peek(addb_tiled *td, unsigned long long offset,
                                     size_t len);

static inline bool addb_tiled_peek5(addb_tiled *td, unsigned long long offset,
                                    unsigned long long *out) {
  unsigned char const *p;

  if ((p = addb_tiled_peek(td, offset, 5))) {
    *out = ADDB_GET_U5(p);
    return true;
  }

  return false;
}

#define addb_tiled_get(a, b, c, d, e) \
  addb_tiled_get_loc(a, b, c, d, e, __FILE__, __LINE__)
void *addb_tiled_get_loc(addb_tiled *_td, unsigned long long _s,
                         unsigned long long _e, int _mode,
                         addb_tiled_reference *_ref_out, char const *_file,
                         int _line);

#define addb_tiled_alloc(a, b, c, d) \
  addb_tiled_alloc_loc(a, b, c, d, __FILE__, __LINE__)
void *addb_tiled_alloc_loc(addb_tiled *_td, unsigned long long _s,
                           unsigned long long _e,
                           addb_tiled_reference *_ref_out, char const *_file,
                           int _line);

#define addb_tiled_link(a, b) addb_tiled_link_loc(a, b, __FILE__, __LINE__)
void addb_tiled_link_loc(addb_tiled *_td, addb_tiled_reference const *_ref,
                         char const *_file, int _line);

#define addb_tiled_free(a, b) addb_tiled_free_loc(a, b, __FILE__, __LINE__)
void addb_tiled_free_loc(addb_tiled *_td, addb_tiled_reference *_tref,
                         char const *_file, int _line);

int addb_tiled_destroy(addb_tiled *);

addb_tiled *addb_tiled_create(addb_tiled_pool *tdp, char *path, int mode,
                              unsigned long long init_map);

int addb_tiled_backup(addb_tiled *_td, bool _on);

int addb_tiled_read_backup(addb_tiled *_td, unsigned long long _horizon);

struct addb_tiled_pool *addb_tiled_pool_create(addb_handle *addb);

size_t addb_tiled_total_linked(addb_tiled *);

void addb_tiled_pool_destroy(addb_tiled_pool *tdp);

int addb_tiled_status(addb_tiled *_td, cm_prefix const *_prefix,
                      addb_status_callback *_callback, void *_callback_data);

int addb_tiled_status_tiles(addb_tiled *_td, cm_prefix const *_prefix,
                            addb_status_callback *_callback,
                            void *_callback_data);

int addb_tiled_align(addb_tiled *_td, off_t *_s, off_t *_e);

void addb_tiled_pool_set_max(addb_tiled_pool *, unsigned long long);
/*
int 		  addb_tiled_pool_set_initial_map_tiles(
                        addb_tiled_pool 		* _tdp,
                        unsigned long long 	 	  _m);
                        */
void addb_tiled_set_mlock(addb_tiled *td, bool lock);

int addb_tiled_pool_status(addb_tiled_pool *_tdp, cm_prefix const *_prefix,
                           addb_status_callback *_callback,
                           void *_callback_data);

bool addb_tiled_is_dirty(addb_tiled *td);
bool addb_tiled_is_in_use(addb_tiled *td);

typedef int addb_tiled_checkpoint_fn(addb_tiled *td, unsigned long long horizon,
                                     bool hard_sync, bool block);

int addb_tiled_checkpoint_finish_backup(addb_tiled *td,
                                        unsigned long long horizon,
                                        bool hard_sync, bool block);

int addb_tiled_checkpoint_sync_backup(addb_tiled *td,
                                      unsigned long long horizon,
                                      bool hard_sync, bool block);

int addb_tiled_checkpoint_start_writes(addb_tiled *td,
                                       unsigned long long horizon,
                                       bool hard_sync, bool block);

int addb_tiled_checkpoint_finish_writes(addb_tiled *td,
                                        unsigned long long horizon,
                                        bool hard_sync, bool block);

int addb_tiled_checkpoint_remove_backup(addb_tiled *td,
                                        unsigned long long horizon,
                                        bool hard_sync, bool block);

int addb_tiled_checkpoint_write(addb_tiled *td, bool hard_sync, bool block);

int addb_tiled_checkpoint_linear_start(addb_tiled *td, bool hard_sync,
                                       bool block);

int addb_tiled_checkpoint_linear_finish(addb_tiled *td, bool hard_sync,
                                        bool block);

void addb_tiled_mlock(addb_tiled *td);
void addb_tiled_munlock(addb_tiled *td);

int addb_tiled_apply_backup_record(addb_tiled *td, unsigned long long offset,
                                   char *mem, unsigned long long size);

#define addb_tiled_read_array(a, b, c, d, e) \
  addb_tiled_read_array_loc(a, b, c, d, e, __FILE__, __LINE__)
void *addb_tiled_read_array_loc(addb_tiled *const td, unsigned long long s,
                                unsigned long long e, unsigned long long *e_out,
                                addb_tiled_reference *ref_out, char const *file,
                                int line);

cl_handle *addb_tiled_cl(addb_tiled *td);
int addb_tiled_stretch(addb_tiled *td);
unsigned long long addb_tiled_physical_file_size(addb_tiled *td);

/* addb-backup.c */

/* Backup information for a tiled file
 */

typedef struct addb_tbkf {
  int fd;           /* backup file file descriptor, -1 -> none */
  char const *path; /* backup file path */
} addb_tbkf;

typedef struct addb_tbk {
  char const *tbk_a_path[2]; /* active backup file paths */
  char const *tbk_v_path;    /* valid backup file path */
  addb_tbkf tbk_a;           /* active backup file 	*/
  addb_tbkf tbk_w;           /* waiting backup file 	*/
  addb_fsync_ctx tbk_fsc;    /* context for cloned fsync */

  unsigned int tbk_do_backup : 1;
  unsigned int tbk_published : 1;

} addb_tbk;

int addb_backup_init(addb_handle *_addb, addb_tbk *tbk, char const *a0_path,
                     char const *a1_path, char const *v_path);

int addb_backup_write(addb_handle *_addb, addb_tbk *_tbk,
                      unsigned long long _offset, char const *_mem,
                      size_t _size);

int addb_backup_finish(addb_handle *_addb, addb_tbk *_tbk,
                       unsigned long long _horizon);

int addb_backup_publish(addb_handle *addb, addb_tbk *tbk);

int addb_backup_unpublish(addb_handle *addb, addb_tbk *tbk);

int addb_backup_sync_start(addb_handle *_addb, addb_tbk *_tbk);

int addb_backup_sync_finish(addb_handle *addb, addb_tbk *tbk, bool block);

int addb_backup_abort(addb_handle *_addb, addb_tbk *_tbk);

int addb_backup_close(addb_handle *addb, addb_tbk *tbk,
                      unsigned long long *bytes_written);

int addb_backup_read(addb_handle *_addb, addb_tiled *_td, addb_tbk *_tbk,
                     unsigned long long _horizon);

void addb_backup_punt(addb_tbk *);

/* addb-clock.c */

void addb_opcount_advance(addb_handle *);
addb_opcount_t addb_opcount_now(addb_handle *);
addb_msclock_t addb_msclock(addb_handle *addb);

/* addb-largefile.c */

typedef struct addb_largefile_handle addb_largefile_handle;

typedef int (*lh_size_get_callback)(void *cookie, unsigned long long id,
                                    size_t *size);

typedef int (*lh_size_set_callback)(void *cookie, unsigned long long id,
                                    size_t size);

int addb_largefile_stat(addb_largefile_handle *lh, unsigned long long id,
                        size_t *len);

int addb_largefile_append(addb_largefile_handle *h, unsigned long long id,
                          const char *data, int l);

addb_largefile_handle *addb_largefile_init(char *path, addb_handle *addb,
                                           cl_handle *cl, cm_handle *cm,
                                           lh_size_get_callback size_get_cb,
                                           lh_size_set_callback size_set_cb,
                                           void *cookie);

int addb_largefile_get(addb_largefile_handle *lh, unsigned long long id,
                       addb_largefile **out);

int addb_largefile_new_done(addb_largefile_handle *handle,
                            unsigned long long id);

int addb_largefile_new(addb_largefile_handle *lh, unsigned long long id,
                       unsigned long long size_guess, addb_largefile **out);

void addb_largefile_close(addb_largefile_handle *lh);

int addb_largefile_status(addb_largefile_handle *handle,
                          cm_prefix const *prefix, addb_status_callback *cb,
                          void *cb_handle);

int addb_largefile_status_tiles(addb_largefile_handle *handle,
                                cm_prefix const *prefix,
                                addb_status_callback *cb, void *cb_handle);

int addb_largefile_remove(const char *p, cl_handle *cl, cm_handle *cm);

int addb_largefile_read5(addb_largefile *lf, size_t offset,
                         unsigned long long *out);

int addb_largefile_read_raw(addb_largefile *lf, unsigned long long offset,
                            unsigned long long end,
                            unsigned char const **ptr_out,
                            unsigned long long *end_out,
                            addb_tiled_reference *tref);

void addb_largefile_set_maxlf(addb_largefile_handle *lh, int maxfd);

void addb_largefile_set_init_map(addb_largefile_handle *lh,
                                 unsigned long long initmap);

int addb_largefile_checkpoint(addb_largefile_handle *lh,
                              unsigned long long horizon, bool hard_sync,
                              bool block, addb_tiled_checkpoint_fn *cpfn);

int addb_largefile_rollback(addb_largefile_handle *lh,
                            unsigned long long horizon);

int addb_largefile_refresh(addb_largefile_handle *lh);

/* addb-bmap.c */

int addb_bmap_checkpoint(struct addb_bmap *bmap, bool hard_sync, bool bloc,
                         addb_tiled_checkpoint_fn *cpfn);

int addb_bmap_fixed_intersect(addb_handle *addb, struct addb_bmap *a,
                              addb_id const *id_in, size_t n_in,
                              addb_id *id_out, size_t *n_out, size_t m);

#endif /* ADDBP_H */
