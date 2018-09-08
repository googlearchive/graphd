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
#include "libaddb/addb-gmap.h"
#include "libaddb/addb.h"
#include "libaddb/addb-largefile-file.h"
#include "libaddb/addbp.h"

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/* Using addb_largefile interfaces.
 * =================================
 *
 * addb_largefiles are files that back particularly large subarrays
 * in gmaps or hmaps. They have the format
 *
 *    +--------------+
 *    | magic [0]    | The magic is "lfv2".
 *    :              |
 *    | magic [3]    |
 *    +--------------+
 *    |size [0]      +  the size is the position to add the next byte of
 *    :              :  data to the file. The length of the file may be
 *    |size [7]      +  longer because it is tiled.
 *    +--------------+
 *    | wasted space +
 *    :              :
 * 80 +--------------+
 *    : real data    :
 *    .              .
 *    .              .
 *    +--------------+
 *    | next byte    |  <-- size points to this location.
 *    :              :  [ zero or more bytes]
 *    .              .  <-- EOF
 *    +--------------+
 *
 *
 * The interface operates on sets of largefiles with unique ID spaces.
 * For GMAPS, there is a separate addb_largefile_handle structure
 * (representing a set of largefiles) for each gmap: from, to, type, etc..
 * largefile_handles are created with the addb_largefile_init() call
 * that returns a largefile_handle() structure.
 *
 * addb_largefile_get() takes a handle and an ID and returns a pointer to
 * an addb_largefile structure with the lf_td and lf_size members that can
 * be used to access the data.
 *
 * addb_largefile_new() and addb_largefile_new_commit() make new files.
 * You should call addb_largefile_new and then use addb_largefile_append
 * to populate the file with data.  During this initial population phase,
 * the callbacks to get/set the data size are disabled and the file is
 * locked open.  When the file is populated and consistent, call
 * addb_largefile_new_commit().  This will unlock the file, call the
 * size set callback and enable future callbacks.
 *
 * addb_largefile_append() appends data to file given the handle and id.
 *
 */

/*
 * One of these structures exists per gmap. (i.e. to, from, type...)
 * It holds the large_file_d list as well and information taken from
 * the parent gmap (tile pool, path, and cl_handle.).
 *
 * Largfile_handles also form a list so that we can iterate over every
 * largefile structure in the system for debugging.
 */
struct addb_largefile_handle {
  /* Tiled pool to use
   */
  addb_tiled_pool* lh_tdp;

  /* The base path of the map. for example /db/to/
   */
  char* lh_basepath;

  /* The head and tail of the list of addb_largefiles for this handle
   */
  addb_largefile* lh_list;
  addb_largefile* lh_list_tail;

  cl_handle* lh_cl;
  cm_handle* lh_cm;

  /* Count the number of items in the largefile list and the
   * number of items with open files in the largefile list
   */
  int lh_count;
  int lh_file_count;

  /* Watch for thrashing
   */
  unsigned long lh_file_thrash_count;
  int lh_file_thrash_count_step;

  cm_hashtable* lh_hash;
  bool lh_soft_limit_exceeded;

  int lh_max_lf;

  void* lh_size_cookie;
  lh_size_get_callback lh_size_get;
  lh_size_set_callback lh_size_set;
  bool lh_no_more_remaps;
};

/*  Passed to cm_list_.*(addb_largefile, ...) calls
 */
static const cm_list_offsets addb_largefile_offsets =
    CM_LIST_OFFSET_INIT(addb_largefile, lf_next, lf_prev);

/**
 * @brief Try to close large files if we're over the soft limit
 *
 *  Close at most n largefiles with zero references from h. We start at the
 *  tail of the list and work backwards to implement LRU semantics.
 *  (lookup always moves its result to the begining of the list)
 * @param handle	The addb_largefile_handle to search
 *
 * @returns	0 on success, error otherwise
 */

static int try_to_close(addb_largefile_handle* handle) {
  addb_largefile* lf = handle->lh_list_tail;
  addb_largefile* lf_prev;
  int const maxcount = handle->lh_max_lf;
  int n = 1 + maxcount / 10;
  int count = 0;
  int err;

  if (handle->lh_file_count <= maxcount) return 0;

  for (; lf && n > 0; lf = lf_prev, n--, count++) {
    lf_prev = lf->lf_prev;

    cl_assert(handle->lh_cl, lf->lf_td);
    if (lf->lf_setting_up) continue;
    if (addb_tiled_is_in_use(lf->lf_td)) continue;

    cl_log(handle->lh_cl, CL_LEVEL_DEBUG,
           "try_to_close: closing %llu in"
           " map: %s",
           lf->lf_id, handle->lh_basepath);
    err = addb_tiled_destroy(lf->lf_td);
    if (err) return err;

    lf->lf_td = NULL;
    cm_list_remove(addb_largefile, addb_largefile_offsets, &handle->lh_list,
                   &handle->lh_list_tail, lf);

    handle->lh_file_count--;
  }
  cl_log(handle->lh_cl, CL_LEVEL_DEBUG, "Closed %i unused files in %s", count,
         handle->lh_basepath);

  handle->lh_file_thrash_count += count;

  if ((handle->lh_file_thrash_count / 1000) >
      handle->lh_file_thrash_count_step) {
    /* TODO: Turn down the loglevel of this thing */
    cl_log(handle->lh_cl, CL_LEVEL_INFO,
           "Largefile thrash counter "
           "now at %ld",
           handle->lh_file_thrash_count);
    handle->lh_file_thrash_count_step = handle->lh_file_thrash_count / 1000;
  }

  return 0;
}

/**
 * @brief Given a filename, get its directory.
 *
 *  Convert /foo/bar/baz to /foo/bar, and foo to "."
 *
 * @param in	The input string
 * @param out_s	Out: a pointer to the first byte of the directory pathname.
 * @param out_e Out: pointer to just after the directory pathname.
 */
static void getbasedir(char const* in, char const** out_s, char const** out_e) {
  char* p;

  if ((p = strrchr(in, '/')) == NULL) {
    *out_e = (*out_s = ".") + 1;
  } else {
    *out_s = in;
    *out_e = p;
  }
}

/**
 * @brief get the largefile entery for a particular ID.
 *
 *  This function searches through the ID list in h for a large file
 *  entry corresponding to h. If it finds it, it returns it and moves
 *  it to the head of the list.
 *
 *  @param handle	the largefile_handle to perform the lookup in
 *  @param id		the id to look for
 */
static addb_largefile* addb_largefile_lookup(addb_largefile_handle* handle,
                                             unsigned long long id) {
  addb_largefile* lf =
      cm_hash(handle->lh_hash, &id, sizeof id, CM_HASH_READ_ONLY);

  if (!lf)
    cl_log(handle->lh_cl, CL_LEVEL_SPEW, "largefile %llu not in cache for %s.",
           id, handle->lh_basepath);

  return lf;
}

/*
 * Calculate the file name that a particular large file should have
 */
static char* addb_largefile_name(addb_largefile_handle* handle,
                                 addb_largefile* lf) {
  char* fname;

  const char *dir_s, *dir_e;
  getbasedir(handle->lh_basepath, &dir_s, &dir_e);

  fname = cm_sprintf(handle->lh_cm, "%.*s/large/%llu.glf", (int)(dir_e - dir_s),
                     dir_s, (unsigned long long)lf->lf_id);

  return fname;
}

/**
 * @brief open a largefile.
 *
 * This opens a largefile and sets the fields of *e as required.
 * it does not link it into the linked list. it is used by addb_largfile_open
 * in the case where the opened the file and then closed it but kept the
 * structure around for faster size lookup AND in the case where we
 * are actually opening a new file.
 *
 * @param handle	largefile handle
 * @param lf		the largefile structure to setup
 * @param id		the id of the file to open
 * @param flags		either 0 or O_CREAT
 * @param size_guess
 */
static int addb_largefile_load(addb_largefile_handle* handle,
                               addb_largefile* lf, unsigned long long id,
                               unsigned flags, unsigned long long size_guess) {
  addb_large_header* h = NULL;
  addb_tiled_reference r;
  char* fname;
  int err;
  char const *dir_s, *dir_e;

  getbasedir(handle->lh_basepath, &dir_s, &dir_e);

  fname = cm_sprintf(handle->lh_cm, "%.*s/large/%llu.glf", (int)(dir_e - dir_s),
                     dir_s, (unsigned long long)id);
  if (!fname) {
    err = errno ? errno : ENOMEM;
    cl_log_errno(handle->lh_cl, CL_LEVEL_ERROR, "cm_sprintf", err,
                 "addb_largefile_load: failed to allocate "
                 "filename for %.*s/large/%llu.glf",
                 (int)(dir_e - dir_s), dir_s, id);

    return err;
  }

  {
    struct stat sb;

    if (!stat(fname, &sb)) {
      if (flags & O_CREAT)
        cl_log(handle->lh_cl, CL_LEVEL_INFO, "Overwriting file: %s.", fname);
    } else {
      err = errno;
      if (err == ENOENT) {
        if (!(flags & O_CREAT)) {
          cl_log_errno(handle->lh_cl, CL_LEVEL_ERROR, "stat", err,
                       "Cannot stat: \"%s\"", fname);
          goto free_fname;
        }
      } else {
        cl_log_errno(handle->lh_cl, CL_LEVEL_FAIL, "stat", errno,
                     "addb_largefile_load: "
                     "cannot stat \"%s\"",
                     fname);
        /* try to continue */
      }
    }
  }

  lf->lf_td = addb_tiled_create(handle->lh_tdp, fname, O_RDWR, size_guess);

  if (!lf->lf_td) {
    err = errno;
    cl_log_errno(handle->lh_cl, CL_LEVEL_ERROR, "addb_tiled_create", err,
                 "Can't open %s", fname);

    goto free_fname;
  }

  if (flags & O_CREAT) {
    h = addb_tiled_alloc(lf->lf_td, 0, ADDB_LARGE_HEADER, &r);
    if (!h) {
      err = errno;
      cl_log_errno(handle->lh_cl, CL_LEVEL_ERROR, "addb_tiled_alloc", err,
                   "largefile_load: couldn't allocate header");

      goto free_td;
    }

    memcpy(h->lhr_magic, ADDB_LARGE_MAGIC, ADDB_MAGIC_SIZE);
    lf->lf_size = ADDB_LARGE_HEADER;
  } else {
    h = addb_tiled_get(lf->lf_td, 0, ADDB_LARGE_HEADER, ADDB_MODE_READ, &r);
    if (!h) {
      err = errno;
      cl_log_errno(handle->lh_cl, CL_LEVEL_ERROR, "addb_tiled_alloc", err,
                   "largefile_load: couldn't read header");

      goto free_td;
    }

    if (!lf->lf_setting_up) {
      err = handle->lh_size_get(handle->lh_size_cookie, id, &lf->lf_size);
      if (err) goto free_td;

      if (lf->lf_size == 0) {
        cl_log(handle->lh_cl, CL_LEVEL_INFO,
               "addb_largefile_load: gmap doesn't know "
               "about largefile \"%s\" (size is 0)",
               fname);
        err = EINVAL;
        goto free_td;
      }
    }
  }

  if (memcmp(h->lhr_magic, ADDB_LARGE_MAGIC, 4)) {
    cl_log(handle->lh_cl, CL_LEVEL_ERROR,
           "Largefile %s has bad magic %x:%x:%x:%x", fname, h->lhr_magic[0],
           h->lhr_magic[1], h->lhr_magic[2], h->lhr_magic[3]);
    addb_tiled_free(lf->lf_td, &r);
    err = EINVAL;
    goto free_td;
  }

  cm_free(handle->lh_cm, fname);
  addb_tiled_free(lf->lf_td, &r);

  lf->lf_dirty = false;

  return 0;

free_td:
  addb_tiled_destroy(lf->lf_td);
  lf->lf_td = (addb_tiled*)0;

free_fname:
  cm_free(handle->lh_cm, fname);
  cl_assert(handle->lh_cl, err);

  return err;
}

/**
 * @brief open a largefile
 *
 *  This function searches for the largefile ID in handle. If it cannot find
 *  it it attempts to open that file and at it to the list.  it also
 *  attempts to close large files if too many are open.
 *  This does not update the reference on the returned structure.
 *
 *  If you want to get the largfile_dict_e for a handle:id pair, this is
 *  probably the function you want.
 *
 *  @parar handle	the largefile handle to use
 *  @param id		the id of the file to open
 *  @param flags	either 0 or O_CREAT.  If flags has O_CREAT in it,
 *			then try to make a new file (headers and all) with
 * 			zero length.
 *  @param size_guess
 *  @param out		*out will be set to the addb_largefile pointer
 *  			for this file.
 *
 * @returns 		0 on success or errno.
 *
 */
static int addb_largefile_open(addb_largefile_handle* handle,
                               unsigned long long id, unsigned flags,
                               unsigned long long size_guess,
                               addb_largefile** out) {
  addb_largefile* lf = addb_largefile_lookup(handle, id);
  int err;

  cl_assert(handle->lh_cl, flags == 0 || flags == O_CREAT);

  if (lf == NULL) {
    /* We haven't seen this before. try and create it.
     */
    lf = cm_hash(handle->lh_hash, &id, sizeof id, CM_HASH_CREATE_ONLY);
    if (!lf) {
      err = errno;
      cl_log_errno(handle->lh_cl, CL_LEVEL_FAIL, "cm_hash", err,
                   "addb_largefile_open: "
                   "could not hash in id: %llu",
                   id);

      return err;
    }
    lf->lf_lfhandle = handle;
    lf->lf_display_name = NULL;
    lf->lf_id = id;
    handle->lh_count++;
    lf->lf_td = NULL;
    lf->lf_setting_up = false;
    lf->lf_delete = false;
  }

  if (lf->lf_td) {
    cm_list_remove(addb_largefile, addb_largefile_offsets, &handle->lh_list,
                   &handle->lh_list_tail, lf);
    cm_list_push(addb_largefile, addb_largefile_offsets, &handle->lh_list,
                 &handle->lh_list_tail, lf);
  } else {
    /* Either we didn't find the file at all, or we are opening a
     * new file (implies we won't find it in the cache) or we are
     * re-opening a file.
     */
    try_to_close(handle);

    if (handle->lh_file_count > handle->lh_max_lf &&
        !handle->lh_soft_limit_exceeded) {
      cl_log(handle->lh_cl, CL_LEVEL_FAIL,
             "addb_largefile_open: exceeding soft "
             "file descriptor limit of %d for %s. "
             "(This message will not be generated "
             "again.)",
             handle->lh_max_lf, handle->lh_basepath);
      handle->lh_soft_limit_exceeded = true;
    }

    err = addb_largefile_load(handle, lf, id, flags, size_guess);
    if (err) {
      cm_hashdelete(handle->lh_hash, lf);
      return err;
    }

    cl_log(handle->lh_cl, CL_LEVEL_DEBUG, "Opened large file %s:%llu",
           handle->lh_basepath, id);

    cm_list_push(addb_largefile, addb_largefile_offsets, &handle->lh_list,
                 &handle->lh_list_tail, lf);
    handle->lh_file_count++;

    handle->lh_file_thrash_count += 1;
    if ((handle->lh_file_thrash_count / 1000) >
        handle->lh_file_thrash_count_step) {
      /* TODO: Turn down the loglevel of this thing */
      cl_log(handle->lh_cl, CL_LEVEL_INFO,
             "Largefile thrash counter "
             "now at %ld",
             handle->lh_file_thrash_count);
      handle->lh_file_thrash_count_step = handle->lh_file_thrash_count / 1000;
    }
  }
  *out = lf;
  return 0;
}

/**
 * @brief change the maximum number of files to have open at once
 */
void addb_largefile_set_maxlf(addb_largefile_handle* handle, int m) {
  if (!m) return;
  if (m < 10 || m > 10000) {
    cl_log(handle->lh_cl, CL_LEVEL_FAIL,
           "addb_largefile_set_maxlf: not setting fd limit"
           " to %i. fd limit will remain at: %i",
           m, handle->lh_max_lf);
    return;
  }
  handle->lh_max_lf = m;
}

/**
 * @brief create a addb_largefile_handle
 *
 *  Creates and sets up a new largfile_handle structure.
 *
 *  @param path		The path were the files will be found/places. The
 *  			last entery is taken off the path, so it must either
 *  			be a _file_ in that directory or end in /.
 *  			i.e.: /my/path/ or /my/path/_some_file_. but not
 *			/my/path
 *
 * @param tp		The tiled pool to use for these files.
 * @param c		the cl handle to use.
 * @param cm		the cm handle to use
 *
 * @returns 		a pointer to an addb_largefile_handle
 */
addb_largefile_handle* addb_largefile_init(char* path, addb_handle* addb,
                                           cl_handle* cl, cm_handle* cm,
                                           lh_size_get_callback size_get_cb,
                                           lh_size_set_callback size_set_cb,
                                           void* cookie) {
  addb_largefile_handle* const lh =
      cm_malloc(cm, strlen(path) + 1 + sizeof *lh);

  if (!lh) return NULL;

  lh->lh_tdp = addb->addb_master_tiled_pool;
  cl_assert(cl, lh->lh_tdp != NULL);

  lh->lh_basepath = (char*)(lh + 1);
  strcpy(lh->lh_basepath, path);
  lh->lh_list = NULL;
  lh->lh_list_tail = NULL;
  lh->lh_cm = cm;
  lh->lh_cl = cl;
  lh->lh_count = 0;
  lh->lh_file_count = 0;
  lh->lh_size_get = size_get_cb;
  lh->lh_size_set = size_set_cb;
  lh->lh_size_cookie = cookie;

  lh->lh_hash = cm_hashcreate(cm, sizeof(addb_largefile), 16);
  if (!lh->lh_hash) {
    cm_free(cm, lh);
    return NULL;
  }
  lh->lh_max_lf = 5000;
  lh->lh_file_thrash_count = 0;
  lh->lh_file_thrash_count_step = 0;
  lh->lh_soft_limit_exceeded = false;
  lh->lh_no_more_remaps = false;

  cl_log(cl, CL_LEVEL_DEBUG, "Initializing largefiles under path: %s", path);

  return lh;
}

/**
 * @brief close a addb_largefile_handle and all largefiles asscociated with it.
 *
 * @param handle	the largefile handle to close
 */
void addb_largefile_close(addb_largefile_handle* handle) {
  addb_largefile* lf = NULL;

  if (handle == NULL) return;

  cl_log(handle->lh_cl, CL_LEVEL_DEBUG, "addb_largefile_close: %s",
         handle->lh_basepath);
  while ((lf = cm_hashnext(handle->lh_hash, lf))) {
    if (lf->lf_td) {
      addb_tiled_destroy(lf->lf_td);
      lf->lf_td = NULL;
    }
    if (lf->lf_display_name) cm_free(handle->lh_cm, lf->lf_display_name);
  }
  cm_hashdestroy(handle->lh_hash);
  cm_free(handle->lh_cm, handle);
}

/**
 * @brief Get the largefile structure for an id.
 */
int addb_largefile_get(addb_largefile_handle* handle, unsigned long long id,
                       addb_largefile** out) {
  int err = addb_largefile_open(handle, id, 0, 1, out);

  if (!err) cl_assert(handle->lh_cl, (*out)->lf_td);

  return err;
}

/*
 * Declare that we have finished the initial population of a large file
 * and that we would like to enable callbacks to the size_set function.
 */
int addb_largefile_new_done(addb_largefile_handle* handle,
                            unsigned long long id) {
  addb_largefile* lf;
  int err;
  err = addb_largefile_open(handle, id, 0, 1, &lf);

  if (err) return err;
  lf->lf_setting_up = false;
  err = handle->lh_size_set(handle->lh_size_cookie, id, lf->lf_size);
  return err;
}

/**
 * @brief create a new largefile
 *
 * This creates a new largefile under a specific handle. The "large" directory
 * is created if needed.  The file has zero length, and is ready to be
 * appended to.
 */
int addb_largefile_new(addb_largefile_handle* handle, unsigned long long id,
                       unsigned long long size_guess, addb_largefile** out) {
  char const* dir_s;
  char const* dir_e;
  char* fname;
  int err;

  getbasedir(handle->lh_basepath, &dir_s, &dir_e);
  fname = cm_sprintf(handle->lh_cm, "%.*s/large", (int)(dir_e - dir_s), dir_s);
  if (fname == NULL) {
    err = errno ? errno : ENOMEM;
    cl_log_errno(handle->lh_cl, CL_LEVEL_ERROR, "cm_sprintf", err,
                 "addb_largefile_load: failed to allocate "
                 "filename for %.*s/large",
                 (int)(dir_e - dir_s), dir_s);

    return err;
  }
  if (mkdir(fname, 0755) < 0) {
    if (errno != EEXIST) {
      err = errno;
      cl_log_errno(handle->lh_cl, CL_LEVEL_ERROR, "mkdir", err,
                   "addb_largefile_new: cannot "
                   "create large-file directory \"%s\"",
                   fname);
      cm_free(handle->lh_cm, fname);

      return err;
    }
  }
  cm_free(handle->lh_cm, fname);

  err = addb_largefile_open(handle, id, O_CREAT, size_guess, out);

  if (err) return err;

  cl_assert(handle->lh_cl, (*out)->lf_td);
  (*out)->lf_setting_up = true;
  return 0;
}

/**
 * @brief read an ADDB_GMAP_ENTRY_SIZEd chunk from a large file
 */
int addb_largefile_read5(addb_largefile* lf, size_t offset,
                         unsigned long long* out) {
  unsigned char const* data;
  size_t boundry;
  size_t i;
  addb_tiled_reference tref;

  if (lf->lf_td == 0) {
    /* Reopen */
    addb_largefile* reopen_lf;
    int err = addb_largefile_open(lf->lf_lfhandle, lf->lf_id, 0, 1, &reopen_lf);
    if (err) return err;
    if (reopen_lf != lf) {
      cl_handle* const reopen_cl = addb_tiled_cl(reopen_lf->lf_td);
      cl_notreached(reopen_cl,
                    "Reopened handle is not the same as "
                    "the original? Failing.");
    }
  }

  if (addb_tiled_peek5(lf->lf_td, offset, out)) return 0;

  if (offset / ADDB_TILE_SIZE ==
      (offset + (ADDB_GMAP_ENTRY_SIZE - 1)) / ADDB_TILE_SIZE) {
    data = addb_tiled_get(lf->lf_td, offset, offset + ADDB_GMAP_ENTRY_SIZE,
                          ADDB_MODE_READ_ONLY, &tref);
    if (data == NULL) return ENOENT;

    *out = ADDB_GET_U5(data);
    addb_tiled_free(lf->lf_td, &tref);
  } else {
    *out = 0;
    boundry = ((offset / ADDB_TILE_SIZE) + 1) * ADDB_TILE_SIZE;

    data =
        addb_tiled_get(lf->lf_td, offset, boundry, ADDB_MODE_READ_ONLY, &tref);

    if (data == NULL) return ENOENT;

    for (i = offset; i < boundry; i++) {
      *out = (*out << 8) | *data++;
    }

    addb_tiled_free(lf->lf_td, &tref);

    data = addb_tiled_get(lf->lf_td, boundry, offset + ADDB_GMAP_ENTRY_SIZE,
                          ADDB_MODE_READ_ONLY, &tref);
    if (data == NULL) return ENOENT;

    for (i = boundry; i < offset + ADDB_GMAP_ENTRY_SIZE; i++) {
      *out = (*out << 8) | *data++;
    }
    addb_tiled_free(lf->lf_td, &tref);
  }

  return 0;
}

/**
 * @brief Return a pointer to a raw chunk of bytes from the
 * 	specified large file.
 *
 *  Note that the offsets aren't quite as "raw" as they could be -
 *  the largefile management structure in the file's header is
 *  not included in the count.
 *
 *  The returned chunk may be less than what's requested, but
 *  will always be at least one byte large (if there is any data
 *  there at all and the specified range isn't empty).
 *
 * @param lf		largefile management structure
 * @param offset	first byte to retrieve
 * @param end		first byte to not retrieve
 * @param ptr_out	assign data pointer to this
 * @param end_out	assign adjusted "end" offset to this
 * @param ref_out	keep track of the reference in this.
 *
 * @return 0 on success,
 * @return ENOENT if we're out of data to return
 */
int addb_largefile_read_raw(addb_largefile* lf, unsigned long long offset,
                            unsigned long long end,
                            unsigned char const** ptr_out,
                            unsigned long long* end_out,
                            addb_tiled_reference* ref_out) {
  if (lf->lf_td == 0) {
    /* Reopen */
    addb_largefile* reopen_lf;
    int err = addb_largefile_open(lf->lf_lfhandle, lf->lf_id, 0, 1, &reopen_lf);
    if (err) return err;
    if (reopen_lf != lf) {
      cl_handle* const reopen_cl = addb_tiled_cl(reopen_lf->lf_td);
      cl_notreached(reopen_cl,
                    "Reopened handle is not the same as "
                    "the original? Failing.");
    }
  }

  cl_handle* const cl = addb_tiled_cl(lf->lf_td);

  cl_assert(cl, end <= lf->lf_size);
  cl_assert(cl, offset < end);

  *ptr_out = addb_tiled_read_array(lf->lf_td, offset, end, end_out, ref_out);

  if (!*ptr_out) return errno ? errno : ENOMEM;

  cl_assert(cl, *end_out <= end);

  return 0;
}

/**
 * @brief append some data to a largefile
 *
 * This appends data to the largefile referenced by handle:id, opening
 * the file if need be.  The file must already exist and be a valid largefile.
 *
 * @param handle	largefile handle
 * @param id		identifier of the specific largefile
 * @param data		data to append.
 * @param count		number of bytes pointed to by data
 */
int addb_largefile_append(addb_largefile_handle* handle, unsigned long long id,
                          const char* data, int count) {
  addb_largefile* lf;
  addb_tiled_reference tref;
  void* ptr = NULL;
  int err;
  size_t old_lf_size;
  unsigned long long mapped;

  err = addb_largefile_open(handle, id, 0, 1, &lf);
  if (err) {
    cl_log_errno(handle->lh_cl, CL_LEVEL_FAIL, "addb_largefile_open", err,
                 "basepath=%s id=%llu", handle->lh_basepath, id);

    return err;
  }

  cl_assert(handle->lh_cl, lf->lf_size > 0);
  mapped = addb_tiled_first_map(lf->lf_td) * ADDB_TILE_SIZE;

  if ((!handle->lh_no_more_remaps) &&
      ((mapped < lf->lf_size) || (lf->lf_size - mapped < count))) {
    unsigned long newsz;

    /*
     * The file may be dirty here. That's okay. we're going
     * to re-open it and immediatly re-dirty it. We're an append-
     * only large file so don't have a real backup file
     */
    err = addb_tiled_destroy(lf->lf_td);

    if (err) {
      /*
       * It didn't work and we don't know exactly what state
       * the tile is in.  Kill the td and hope for the
       * best if we ever come back to re-open it
       */
      lf->lf_td = NULL;
      cl_log_errno(handle->lh_cl, CL_LEVEL_ERROR, "addb_tiled_destroy", err,
                   "Unexpected error closing largefile %llu "
                   " under %s",
                   (unsigned long long)(lf->lf_id), handle->lh_basepath);

      /*
       * and the write didn't work.
       */
      return err;
    }

    lf->lf_td = NULL;
    err = addb_largefile_load(handle, lf, lf->lf_id,
                              ADDB_MODE_READ | ADDB_MODE_WRITE,
                              lf->lf_size * 2ull);

    if (err) {
      cl_log_errno(handle->lh_cl, CL_LEVEL_ERROR, "addb_largefile_load", err,
                   "Can't load largefile %llu under %s (I had"
                   " it a nanosecond ago)",
                   (unsigned long long)lf->lf_id, handle->lh_basepath);
      lf->lf_td = NULL;
    }
    cl_assert(handle->lh_cl, lf->lf_td);

    newsz = addb_tiled_first_map(lf->lf_td) * ADDB_TILE_SIZE;

    if (newsz < (lf->lf_size * 2)) {
      /*
       * the tile subsystem didn't want to give me that much
       * space. This is okay but lots not keep trying forever
       */
      handle->lh_no_more_remaps = true;
      cl_log(handle->lh_cl, CL_LEVEL_ERROR,
             "addb_largefile_append: addb_tiled_create "
             "did not give the requested init map size "
             "%llu (only got %llu); largefile remapping "
             "disabled.",
             (unsigned long long)(lf->lf_size * 2), (unsigned long long)newsz);
    }

    cl_log(handle->lh_cl, CL_LEVEL_INFO,
           "re-opening largefile %llu under %s to increase"
           " init-map size to %llu",
           (unsigned long long)(lf->lf_id), handle->lh_basepath,
           (unsigned long long)lf->lf_size * 2);
  }

  old_lf_size = lf->lf_size;
  cl_log(handle->lh_cl, CL_LEVEL_VERBOSE,
         "Largefile appending %u bytes of data to id: %llu "
         "under map: %s.",
         count, id, handle->lh_basepath);

  while (count > 0) {
    size_t boundry = ((lf->lf_size / ADDB_TILE_SIZE) + 1) * ADDB_TILE_SIZE;
    size_t pass_length;

    if (boundry >= (lf->lf_size + count)) boundry = lf->lf_size + count;

    pass_length = boundry - lf->lf_size;

    ptr = addb_tiled_alloc(lf->lf_td, lf->lf_size, boundry, &tref);
    if (!ptr) {
      err = errno;
      cl_log_errno(handle->lh_cl, CL_LEVEL_ERROR, "addb_tiled_alloc", err,
                   "could not create new tile for: %s:%llu",
                   handle->lh_basepath, id);

      /* Pretend the append never happened
       */
      lf->lf_size = old_lf_size;
      return err;
    }
    memcpy(ptr, data, pass_length);
    addb_tiled_free(lf->lf_td, &tref);

    data += pass_length;
    count -= pass_length;
    lf->lf_size = boundry;
  }

  if (!lf->lf_setting_up) {
    err = handle->lh_size_set(handle->lh_size_cookie, id, lf->lf_size);
    if (err) {
      cl_log_errno(handle->lh_cl, CL_LEVEL_ERROR, "handle->lh_size_set", err,
                   "table %s:%llu", handle->lh_basepath, id);
      return err;
    }
  }
  lf->lf_dirty = true;

  return 0;
}

/**
 * @brief return status information for gstatus
 *
 * Do this by appending our the name of the field to prefix (which
 * contains a base name of sorts.) using dot as the separator.
 * Then, call teh callback function with its private data and the
 * name/value pair.
 *
 * things we report:
 * 	total_files
 * 	open_files
 *	number of bytes in all open files
 *	the number of times we have exceeded the open file soft limit
 *
 * @param handle	the addb_largefile_handle to report on
 * @param prefix	the prefix to be used for all name:value pairs
 * @param cb		callback function to give name:value pairs to
 * @param cb_handle	cookie passed back to cb
 */
int addb_largefile_status(addb_largefile_handle* handle,
                          cm_prefix const* prefix, addb_status_callback* cb,
                          void* cb_handle) {
  char val[100];
  addb_largefile* c;
  int err;
  unsigned long long total_size;
  cm_prefix lf_pre = cm_prefix_push(prefix, "lf");

  snprintf(val, sizeof val, "%i", handle ? handle->lh_file_count : 0);
  if ((err = (*cb)(cb_handle, cm_prefix_end(&lf_pre, "open-files"), val)))
    return err;

  snprintf(val, sizeof val, "%i", handle ? handle->lh_count : 0);
  if ((err = (*cb)(cb_handle, cm_prefix_end(&lf_pre, "known-files"), val)))
    return err;

  total_size = 0;
  if (handle) {
    for (c = handle->lh_list; c; c = c->lf_next) {
      cl_assert(handle->lh_cl, c->lf_td);
      total_size += c->lf_size;
    }
  }

  snprintf(val, sizeof val, "%llu", total_size);
  if ((err = (*cb)(cb_handle, cm_prefix_end(&lf_pre, "open-files-total-size"),
                   val)))
    return err;

  snprintf(val, sizeof val, "%i", handle ? handle->lh_soft_limit_exceeded : 0);

  if ((err = (*cb)(cb_handle,
                   cm_prefix_end(&lf_pre, "soft-limit-exceeded-count"), val)))
    return err;

  snprintf(val, sizeof val, "%i", handle ? handle->lh_max_lf : 0);

  if ((err = (*cb)(cb_handle, cm_prefix_end(&lf_pre, "maximum-files"), val)))
    return err;

  return 0;
}

/**
 * @brief return status information for gstatus
 *
 * Do this by appending our the name of the field to prefix (which
 * contains a base name of sorts) using dot as the separator.
 * Then, call the callback function with its private data and the
 * name/value pair.
 *
 * things we report:
 * 	total_files
 * 	open_files
 *	number of bytes in all open files
 *	the number of times we have exceeded the open file soft limit
 *
 * @param handle	the addb_largefile_handle to report on
 * @param prefix	the prefix to be used for all name:value pairs
 * @param cb		callback function to give name:value pairs to
 * @param cb_handle	cookie passed back to cb
 */
int addb_largefile_status_tiles(addb_largefile_handle* handle,
                                cm_prefix const* prefix,
                                addb_status_callback* cb, void* cb_handle) {
  cm_prefix lf_pre = cm_prefix_push(prefix, "lf");
  addb_largefile* lf;
  int err;

  if (handle == NULL) return 0;

  for (lf = handle->lh_list; lf; lf = lf->lf_next)
    if (lf->lf_td) {
      cm_prefix const lf_pre_i =
          cm_prefix_pushf(&lf_pre, "%llu", (unsigned long long)lf->lf_id);
      err = addb_tiled_status_tiles(lf->lf_td, &lf_pre_i, cb, cb_handle);
      if (err != 0) return err;
    }

  return 0;
}

/*
 * delete every glf file under <basepath>/large. Only delete
 * files ending in .glf.  We cannot check for valid magic
 * numbers because the largefile may be deleted before being
 * flushed to disk.
 */
int addb_largefile_remove(const char* p, cl_handle* cl, cm_handle* cm) {
  DIR* d = (DIR*)0;
  struct dirent* de;
  char* suffix;
  char* basepath = NULL;
  int err = 0;
  char* tname = NULL;

  basepath = cm_sprintf(cm, "%s/large/", p);
  if (!basepath) {
    err = ENOMEM;
    goto done;
  }

  d = opendir(basepath);
  if (!d) {
    err = errno;
    if (err == ENOENT) err = 0;
    goto done;
  }

  while ((de = readdir(d))) {
    if (!strcmp(de->d_name, ".")) continue;
    if (!strcmp(de->d_name, "..")) continue;
    suffix = strrchr(de->d_name, '.');
    if (!suffix || strcmp(suffix, ".glf")) {
      cl_log(cl, CL_LEVEL_ERROR,
             "Refusing to delete unknown file:"
             " %s living in %s.",
             de->d_name, basepath);
      err = EEXIST;
      goto done;
    }

    tname = cm_sprintf(cm, "%s/%s", basepath, de->d_name);
    if (!tname) {
      err = ENOMEM;
      goto done;
    }

    if (unlink(tname)) {
      err = errno;
      cl_log_errno(cl, CL_LEVEL_ERROR, "unlink", err, "failed to remove %s",
                   tname);
      goto done;
    }
    cm_free(cm, tname);
    tname = NULL;
  }
  if (closedir(d)) {
    err = errno;
    cl_log_errno(cl, CL_LEVEL_ERROR, "closedir", err, "huh?");
  }
  d = (DIR*)0;

  if (rmdir(basepath)) {
    err = errno;
    cl_log_errno(cl, CL_LEVEL_ERROR, "rmdir", err, "failed to remove %s",
                 basepath);

    goto done;
  }

done:
  if (d) (void)closedir(d);

  if (tname) cm_free(cm, tname);

  if (basepath) cm_free(cm, basepath);

  return err;
}

static void addb_largefile_dead(addb_largefile_handle* lh, addb_largefile* lf,
                                bool delete_on_disk) {
  int err;
  int rv;
  char const *dir_s, *dir_e;
  char* fname;

  cl_log(lh->lh_cl, CL_LEVEL_DEBUG, "Deleting largefile %llu", lf->lf_id);
  cl_assert(lh->lh_cl, !(addb_tiled_is_in_use(lf->lf_td)));

  getbasedir(lh->lh_basepath, &dir_s, &dir_e);

  fname = cm_sprintf(lh->lh_cm, "%.*s/large/%llu.glf", (int)(dir_e - dir_s),
                     dir_s, (unsigned long long)lf->lf_id);

  if (!fname) {
    cl_log(lh->lh_cl, CL_LEVEL_ERROR, "addb_tiled_dead: out of memory");
    return;
  }

  err = addb_tiled_destroy(lf->lf_td);

  if (err) {
    cl_log_errno(lh->lh_cl, CL_LEVEL_ERROR, "addb_tiled_destroy", err,
                 "Can't destroy tile for %s after upgrading to bgmap",
                 lf->lf_display_name);
  }

  if (delete_on_disk) {
    rv = unlink(fname);

    if (rv) {
      err = errno;
      cl_log_errno(lh->lh_cl, CL_LEVEL_ERROR, "unlink", err, "Can't unlink %s",
                   fname);
    }
  }

  if (lf->lf_display_name) {
    cm_free(lh->lh_cm, lf->lf_display_name);
  }
  cm_list_remove(addb_largefile, addb_largefile_offsets, &lh->lh_list,
                 &lh->lh_list_tail, lf);

  cm_hashdelete(lh->lh_hash, lf);
  cm_free(lh->lh_cm, fname);
}

/* Iterate over large files, applying a checkpoint function
 */
int addb_largefile_checkpoint(addb_largefile_handle* lh,
                              unsigned long long horizon, bool hard_sync,
                              bool block, addb_tiled_checkpoint_fn* cpfn) {
  bool wouldblock = false;
  int err = 0;
  addb_largefile *lf, *lf_next;

  for (lf = lh->lh_list; lf; lf = lf_next) {
    lf_next = lf->lf_next;

    if (!lf->lf_td) continue;

    /* Ick!  Can we do this without direct pointer comparisons?
     */

    if (lf->lf_dirty) {
      if (cpfn == addb_tiled_checkpoint_start_writes) {
        err = addb_tiled_checkpoint_linear_start(lf->lf_td, hard_sync, block);
      } else if (cpfn == addb_tiled_checkpoint_finish_writes) {
        err = addb_tiled_checkpoint_linear_finish(lf->lf_td, hard_sync, block);

        if (err == 0) lf->lf_dirty = false;
      }

      /* XXX
       *    What happens to writes that happen between
       *    the start and finish?  For them, lf->lf_dirty
       *    will still be set to false.  I think there's
       *    a race condition here.
       */
    }
    if (err == ADDB_ERR_MORE) {
      wouldblock = true;
    } else if (err) {
      cl_log(lh->lh_cl, CL_LEVEL_ERROR, "addb_largefile_checkpoint filed: %s",
             addb_xstrerror(err));
      return err;
    }

    if (cpfn == addb_tiled_checkpoint_remove_backup) {
      if (lf->lf_delete) {
        lf->lf_delete_count--;
        if (lf->lf_delete_count == 0) addb_largefile_dead(lh, lf, true);
      }
    }
  }

  /* If we just finished a checkpoint, see if we should close
   * a few files.
   */
  if (!err && addb_tiled_checkpoint_remove_backup == cpfn) try_to_close(lh);
  /* TODO: also close (and re-open) really big largefiles so as to
   * cut down on tile thrashing
   */

  if (!err && wouldblock) err = ADDB_ERR_MORE;

  return err;
}

int addb_largefile_rollback(addb_largefile_handle* lh,
                            unsigned long long horizon) {
  addb_largefile* lf;
  addb_largefile* next_lf;
  int err = 0;
  int e = 0;
  const char *dir_e, *dir_s;
  char* fname;

  for (lf = lh->lh_list; lf; lf = next_lf) {
    next_lf = lf->lf_next;
    if (!lf->lf_td) continue;

    e = lh->lh_size_get(lh->lh_size_cookie, lf->lf_id, &(lf->lf_size));

    if (e) {
      if (!err) err = e;
      cl_log(lh->lh_cl, CL_LEVEL_ERROR,
             "addb_largefile_rollback: could not re-read "
             "largefile size using callback: %s. ",
             addb_xstrerror(errno));
      continue;
    }

    if (lf->lf_size == 0) {
      /*
       *  If there's no error, but the size was set to zero,
       *  assume that we created the largefile during the
       *  rollback. Destroy it now.
       */

      addb_tiled_destroy(lf->lf_td);

      /*
       * Delete the largefile
       */
      getbasedir(lh->lh_basepath, &dir_s, &dir_e);

      fname = cm_sprintf(lh->lh_cm, "%.*s/large/%llu.glf", (int)(dir_e - dir_s),
                         dir_s, (unsigned long long)lf->lf_id);

      if (!fname) {
        err = errno ? errno : ENOMEM;
        cl_log_errno(lh->lh_cl, CL_LEVEL_ERROR, "cm_sprintf", err,
                     "addb_largefile_rollback: "
                     "failed to allocate "
                     "filename for %.*s/large/%llu.glf",
                     (int)(dir_e - dir_s), dir_s, lf->lf_id);
      } else {
        e = unlink(fname);
        if (e) {
          cl_log_errno(lh->lh_cl, CL_LEVEL_ERROR, "unlink", err,
                       "can't remove: %s "
                       "[ignored]",
                       fname);
          /*
           * This is okay. The file will get truncated
           * next time we try to use it.
           * don't return an error and cause a panic
           */
        }
        cm_free(lh->lh_cm, fname);
      }
      /*
       * Remove it from our in-memory records lest
       * badthings(tm) happen next time we try to
       * create it.
       */
      cm_list_remove(addb_largefile, addb_largefile_offsets, &lh->lh_list,
                     &lh->lh_list_tail, lf);
      cm_hashdelete(lh->lh_hash, lf);
    }
  }

  return err;
}

/*
 * Inform the largefile subsystem that files may have changed on disk.
 * We scan through every largefile we know about and update internal data
 * structures as required.
 *
 * If the file no longer exists, get rid of the corresponding lf structure.
 *
 */
int addb_largefile_refresh(addb_largefile_handle* lh) {
  addb_largefile *lf, *next;
  cl_handle* cl = lh->lh_cl;
  int err;
  size_t size;

  for (lf = lh->lh_list; lf; lf = next) {
    char* name;
    /*
     * lf may be destroyed during the loop. Keep track of next.
     */
    next = lf->lf_next;

    name = addb_largefile_name(lh, lf);
    if (access(name, F_OK)) {
      addb_largefile_dead(lh, lf, false);
      cm_free(lh->lh_cm, name);
      continue;
    }
    /*
     * Stretch the td if we have one. (but we might not)
     */
    if (lf->lf_td) {
      err = addb_tiled_stretch(lf->lf_td);
      if (err) {
        cl_log_errno(cl, CL_LEVEL_ERROR, "addb_tiled_stretch", err,
                     "Unable to stretch largefile %s", name);
        cm_free(lh->lh_cm, name);
        return err;
      }
    }

    /*
     * The logical size is stored in the gmap slot and may have
     * changed. update that as well
     */
    err = lh->lh_size_get(lh->lh_size_cookie, lf->lf_id, &size);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_ERROR, "lh->lh_size_get", err,
                   "Unable to get size of potentially changed "
                   "largefile %s",
                   name);
      cm_free(lh->lh_cm, name);
      return err;
    }

    if (size == 0) {
      /* The large file has been rolled back; the GMAP
       * structure knows nothing of it.
       */
      addb_largefile_dead(lh, lf, false);

      cl_log(cl, CL_LEVEL_INFO,
             "addb_largefile_refresh: truncated "
             "large file \"%s\", since gmap doesn't "
             "record its existence.",
             name);
      cm_free(lh->lh_cm, name);
      continue;
    }

    lf->lf_size = size;
    cm_free(lh->lh_cm, name);
  }
  return 0;
}
