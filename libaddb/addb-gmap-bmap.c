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
#include "libaddb/addb-bgmap.h"
#include "libaddb/addb-bmap.h"
#include "libaddb/addb-gmap.h"
#include "libaddb/addb.h"
#include "libaddb/addbp.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/*
 * Hold a list of bgmaps for specific gmap IDs
 */
struct addb_bgmap {
  /*
   * The gmap soruce ID of the bgmap
   */
  unsigned long long bgm_id;

  /*
   * The file name of the bgmap
   */
  char *bgm_name;

  /*
   * The bmap structure for this bgmap
   */
  addb_bmap *bgm_bmap;

  addb_bgmap *bgm_next;
};

struct addb_bgmap_handle {
  addb_bgmap *abh_list;
  addb_handle *abh_addb;
  cm_handle *abh_cm;
  char *abh_path;
};

const char *addb_bgmap_name(addb_bgmap *bg) { return bg->bgm_name; };

/*
 * Open a new bgmap handle under a particular path.
 * Thets gets called from gmap_open and creates the bgmap directory
 * as needed.
 */
addb_bgmap_handle *addb_bgmap_create(addb_handle *addb, const char *gpath) {
  addb_bgmap_handle *r;
  int err;
  struct stat st;
  char *path;

  path = cm_sprintf(addb->addb_cm, "%s/bgmap", gpath);

  if (!path) {
    cl_log(addb->addb_cl, CL_LEVEL_ERROR, "Out of memory in addb_bgmap_create");
    return NULL;
  }

  /*
   * Make the directory
   */
  if (mkdir(path, 0755)) {
    err = errno;
    if (err == EEXIST) {
      if (stat(path, &st)) {
        err = errno;
        cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "stat", err,
                     "can't state a file that exists: %s", path);
        return NULL;
      }
      if (!S_ISDIR(st.st_mode)) {
        cl_log(addb->addb_cl, CL_LEVEL_ERROR,
               "addb_bgmap_create: %s exists but is"
               " not a directory",
               path);
        errno = EINVAL;
        return NULL;
      }
    } else {
      cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "mkdir", err,
                   "Can't make directory: %s", path);
      return NULL;
    }
  }

  /*
   * At this point we should have a nice directory, path, to put
   * bmaps in.
   */
  r = cm_malloc(addb->addb_cm, sizeof(*r));

  if (!r) {
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "Out of memory, cannot allocate %i bytes", (int)(sizeof(*r)));
    return NULL;
  }

  r->abh_list = NULL;
  r->abh_cm = addb->addb_cm;
  r->abh_addb = addb;
  r->abh_path = path;

  return r;
}

int addb_bgmap_estimate(addb_gmap *gm, addb_gmap_id source, addb_gmap_id low,
                        addb_gmap_id high, unsigned long long *result) {
  const unsigned long long tries = 500;
  unsigned long long range = high - low;
  int i;
  unsigned long long count = 0;
  int err;
  addb_bgmap *bg;

  if (range == 0) {
    *result = 0;
    return 0;
  }

  err = addb_bgmap_lookup(gm, source, &bg);
  if (err) {
    cl_log_errno(gm->gm_addb->addb_cl, CL_LEVEL_FAIL, "addb_bgmap_lookup", err,
                 "Can't find bgmap for %llu", (unsigned long long)source);
    return err;
  }

  for (i = 0; i < tries; i++) {
    unsigned long long p = random() % range + low;
    bool b;
    err = addb_bgmap_check(gm, bg, p, &b);
    if (err == 0) {
      count += b;
    } else {
      cl_log_errno(gm->gm_addb->addb_cl, CL_LEVEL_FAIL, "addb_bgmap_check", err,
                   "Can't check bgmap %llu at id %llu",
                   (unsigned long long)source, p);
      return err;
    }
  }

  *result = count * (high - low) / tries;
  return 0;
}

int addb_bgmap_refresh(addb_gmap *gm, unsigned long long max) {
  int err;
  addb_bgmap_handle *ah = gm->gm_bgmap_handle;
  addb_bgmap *cur;
  for (cur = ah->abh_list; cur; cur = cur->bgm_next) {
    err = addb_bmap_refresh(cur->bgm_bmap, max);
    if (err) return err;
  }

  return 0;
}

/*
 * Get a bgmap structure for a gmap:id pair.
 * We look the structure up our list of know bgmaps. If it doesn't exist
 * we try to open it on disk.  If it doesn't exist on disk, we try to
 * make a new one.
 */
int addb_bgmap_lookup(addb_gmap *gm, addb_gmap_id s, addb_bgmap **out) {
  addb_bgmap_handle *ah = gm->gm_bgmap_handle;
  addb_bgmap *cur;
  int err;

  /*
   * Search the list of bmaps for the bmap for this ID
   */

  for (cur = ah->abh_list; cur; cur = cur->bgm_next) {
    if (cur->bgm_id == s) {
      *out = cur;
      return 0;
    }
  }
  /*
   * Not found? Make a new one!
   */

  cur = cm_malloc(gm->gm_addb->addb_cm, sizeof(addb_bgmap));

  if (!cur) {
    cl_log(gm->gm_addb->addb_cl, CL_LEVEL_ERROR,
           "addb_bgmap_lookup: out of memory "
           "cannot malloc %i bytes",
           (int)(sizeof(addb_bgmap)));

    return ENOMEM;
  }

  cur->bgm_id = s;

  cur->bgm_bmap = NULL;

  cur->bgm_name = cm_sprintf(ah->abh_cm, "%s/%llu.bgm", ah->abh_path,
                             (unsigned long long)s);

  if (!cur->bgm_name) {
    cm_free(gm->gm_addb->addb_cm, cur);
    return ENOMEM;
  }

  err = addb_bmap_open(gm->gm_addb, cur->bgm_name, 0, /* variable size */
                       0,    /* don't have a horizon yet*/
                       true, /*this is an append only bmap */
                       &(cur->bgm_bmap));

  if (err) {
    cl_log_errno(gm->gm_addb->addb_cl, CL_LEVEL_ERROR, "addb_bmap_open", err,
                 "Can't open or create: %s", cur->bgm_name);
    cm_free(gm->gm_addb->addb_cm, cur->bgm_name);
    cm_free(gm->gm_addb->addb_cm, cur);
    return err;
  }

  cur->bgm_next = ah->abh_list;
  ah->abh_list = cur;

  *out = cur;

  return 0;
}

/*
 * Free and destroy a bgmap handle and every bgmap associated with it.
 */
void addb_bgmap_handle_destroy(addb_bgmap_handle *ah) {
  addb_bgmap *cur, *next;

  if (!ah) return;

  for (cur = ah->abh_list; cur; cur = next) {
    next = cur->bgm_next;

    addb_bmap_close(cur->bgm_bmap);
    cur->bgm_bmap = NULL;

    cm_free(ah->abh_cm, cur->bgm_name);
    cm_free(ah->abh_cm, cur);
  }
  cm_free(ah->abh_cm, ah->abh_path);
  cm_free(ah->abh_cm, ah);
}

/*
 * Check a single bit in a bgmap
 */
int addb_bgmap_check(addb_gmap *gm, addb_bgmap *bg, addb_gmap_id s, bool *out) {
  int err;

  err = addb_bmap_check(bg->bgm_bmap, s, out);

  if (err) {
    cl_log_errno(gm->gm_addb->addb_cl, CL_LEVEL_ERROR, "addb_bmap_check", err,
                 "Can't check bit in %s at %llu", bg->bgm_name,
                 (unsigned long long)s);
    return err;
  }
  return 0;
}

/*
 * Scan a bgmap for the next ID set. Start is should be the first ID to check.
 * The function returns ADDB_ERR_NO when we reach the end.
 * It returns ADDB_ERR_MORE and sets start to the NEXT ID TO CHECK
 * 	if it runs out of 'time' and should be called again.
 * It it finds an ID, it sets start to the NEXT ID (id = start - 1) and returns
 * 	0.
 *
 * This function can only scan forwards
 */

static int addb_bgmap_next_fast(addb_gmap *gm, addb_bgmap *bg,
                                addb_gmap_id *start, addb_gmap_id low,
                                addb_gmap_id high, bool direction) {
  int err;
  unsigned long scan_at_once = 1000000;

  /* The first and last bits to check */
  unsigned long long s, e;

  /* The bit found by bmap_scan, if any */
  unsigned long long res;

  if (direction) {
    s = *start;
    e = s + scan_at_once;
    if (e > high) e = high;
  } else {
    e = *start;
    if (e > scan_at_once)
      s = e - scan_at_once;
    else
      s = 0;

    if (s < low) s = low;
  }

  /*
   * Scan from s to e and find the first bit that is set
   * (not including e)
   */
  err = addb_bmap_scan(bg->bgm_bmap, s, e, &res, direction);
  if (err == ADDB_ERR_NO) {
    if (direction) {
      if (e >= high) return ADDB_ERR_NO;
      *start = e;
    } else {
      if (s <= low) return ADDB_ERR_NO;
      *start = s;
    }
    return ADDB_ERR_MORE;
  } else if (err != 0) {
    cl_log_errno(gm->gm_addb->addb_cl, CL_LEVEL_ERROR, "addb_bgmap_scan", err,
                 "Unexpected error scanning from %llu to %llu",
                 (unsigned long long)s, (unsigned long long)e);

    /* something actually bad went wrong */
    return err;
  }

  cl_log(gm->gm_addb->addb_cl, CL_LEVEL_SPEW, "addb_bgmap_next_fast: %s:%llu",
         bg->bgm_name, (unsigned long long)res);

  *start = res;
  return 0;
}

int addb_bgmap_next(addb_gmap *gm, addb_bgmap *bg, addb_gmap_id *start,
                    addb_gmap_id low, addb_gmap_id high, bool forward) {
  int err;

  /*
   * If we're not going to return anything, bail out early
   */
  if (*start == high) return ADDB_ERR_NO;

  err = addb_bgmap_next_fast(gm, bg, start, low, high, forward);

  /*
   *  Note that err==0 means that *start-1 is set.  (Or +1, if backwards)
   *  In that case we must return success.
   *  On our next call, addb_bgmap_next_fast should fail with ADDB_ERR_NO
   */

  if (*start > high) return ADDB_ERR_NO;

  switch (err) {
    case 0:
    case ADDB_ERR_MORE:
    case ADDB_ERR_NO:
      return err;

    default:
      break;
  }
  cl_log_errno(gm->gm_addb->addb_cl, CL_LEVEL_ERROR, "addb_bgmap_next_fast",
               err, "unexpected error code. start: %llu high: %llu",
               (unsigned long long)*start, (unsigned long long)high);
  return err;
}

int addb_bgmap_append(addb_gmap *gm, addb_bgmap *bg, addb_gmap_id s) {
  int err;
  cl_handle *cl = gm->gm_addb->addb_cl;

  err = addb_bmap_set(bg->bgm_bmap, s);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_bmap_set:", errno,
                 "Can't set id %llu", (unsigned long long)s);
    return err;
  }

  return 0;
}

int addb_bgmap_horizon_set(addb_gmap *gm, unsigned long long horizon)

{
  addb_bgmap *bgm;
  bgm = gm->gm_bgmap_handle->abh_list;

  for (; bgm; bgm = bgm->bgm_next) {
    addb_bmap_horizon_set(bgm->bgm_bmap, horizon);
  }
  return 0;
}

int addb_bgmap_checkpoint(addb_gmap *gm, unsigned long long horizon,
                          bool hard_sync, bool block,
                          addb_tiled_checkpoint_fn *cpfn)

{
  addb_bgmap *bgm;
  addb_bmap *bm;
  bgm = gm->gm_bgmap_handle->abh_list;
  bool wouldblock = false;
  int err = 0;

  addb_bgmap_horizon_set(gm, horizon);
  for (; bgm; bgm = bgm->bgm_next) {
    bm = bgm->bgm_bmap;
    err = addb_bmap_checkpoint(bm, hard_sync, block, cpfn);
    if (err == ADDB_ERR_MORE)
      wouldblock = true;
    else if (err) {
      cl_log_errno(gm->gm_addb->addb_cl, CL_LEVEL_ERROR,
                   "addb_bmapcheckpoint**", err, "addb_bmap_checkpoint failed");
    }
  }
  if (err) return err;

  if (wouldblock == true) return ADDB_ERR_MORE;

  return 0;
}

int addb_bgmap_truncate(addb_bgmap_handle *ah) {
  DIR *dir;
  struct dirent *ent;
  int err;
  addb_bgmap *bg;
  cl_handle *cl;
  char *path;

  path = ah->abh_path;
  cl = ah->abh_addb->addb_cl;

  for (bg = ah->abh_list; bg; bg = bg->bgm_next) {
    /*
     * This is kinda messy. We should rethink how bmap
     * truncate should work.
     */
    addb_bmap_truncate(bg->bgm_bmap);
  }

  dir = opendir(path);
  if (!dir) return 0;

  /*
   * Step 2: remove any other bitmap files lying around.
   */

  while ((ent = readdir(dir))) {
    size_t l;
    /*
     * Check files names to make sure that match.
     * Files should look like:
     * nnnn-nn.bgm
     */

    if (!strcmp(ent->d_name, ".")) continue;
    if (!strcmp(ent->d_name, "..")) continue;

    l = strlen(ent->d_name);

    if ((l < 7) || strcmp(ent->d_name + l - 4, ".bgm")) {
      cl_log(cl, CL_LEVEL_ERROR,
             "addb_gmap_bmap_truncate: file %s in "
             "directory %s does not match the bgmap "
             "pattern: nnnn-nn.bgm",
             ent->d_name, path);
    }

    if (unlink(ent->d_name)) {
      err = errno;
      cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_bmap_truncate", err,
                   "Can't delete %s/%s", path, ent->d_name);
      return err;
    }
  }

  if (rmdir(path)) {
    err = errno;
    cl_log_errno(cl, CL_LEVEL_ERROR, "rmdir", err, "Can't remove: %s", path);
    return err;
  }

  return 0;
}

/**
 * @brief Intersect between a bmap and a fixed set of ids.
 *
 * @param addb 		opaque addb module handle
 *
 * @param bmap		one bmap
 *
 * @param id_in		a fixed set of indices
 * @param n_in		number of indices pointed to by id_in.
 *
 * @param id_out	out: elements shared by bmap and id_in[].
 * @param n_out		out: number of occupied slots in id_out.
 * @param m		maximum number of slots available.
 *
 * @return ADDB_ERR_MORE	ran out of slots
 */
int addb_bgmap_fixed_intersect(addb_handle *addb, addb_bgmap *const bgm,
                               addb_id const *id_in, size_t n_in,
                               addb_id *id_out, size_t *n_out, size_t m) {
  return addb_bmap_fixed_intersect(addb, bgm->bgm_bmap, id_in, n_in, id_out,
                                   n_out, m);
}
