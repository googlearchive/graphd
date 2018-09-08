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
#include "libpdb/pdbp.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "libaddb/addb-bmap.h"
#include "libcl/cl.h"
#include "libcm/cm.h"


static int pdb_count_subdirs(pdb_handle* pdb, char* base) {
  DIR* d;

  d = opendir(base);

  if (!d) {
    /*
     * newpath is a file, count it.
     */
    if (errno == ENOTDIR)
      return 1;
    else {
      int err = errno;
      cl_log(pdb->pdb_cl, CL_LEVEL_INFO, "Cannot open %s:%s", base,
             strerror(err));
      /*
       * We're intentionally being noosy and looking at
       * everything under the database directory.  If we
       * can't look at something, it isn't nessesarily an
       * error.
       */
      return 0;
    }
  } else {
    int count = 0; /* Don't count directories */
    struct dirent* sub;
    char* end;
    end = base + strlen(base);
    while ((sub = readdir(d))) {
      if (!strcmp(sub->d_name, ".")) continue;

      if (!strcmp(sub->d_name, "..")) continue;
      if (((end - base) + strlen(sub->d_name) + 1) > PATH_MAX) continue;
      sprintf(end, "/%s", sub->d_name);

      count += pdb_count_subdirs(pdb, base);
    }
    *end = 0;
    closedir(d);
    return count;
  }
}

/*
 * The great thing about standards is that there are so many to choose from.
 */
#ifdef RLIMIT_NOFILE
#define PDB_RLIMIT_NOFILES RLIMIT_NOFILE
#else
#ifdef RLIMIT_OFILE
#define PDB_RLIMIT_NOFILES RLIMIT_OFILE
#endif
#endif
#ifndef PDB_RLIMIT_NOFILES
#error Your operating system defines neither RLIMIT_NOFILES nor RLIMIT_OFILES
#endif

int pdb_get_max_files(void) {
  struct rlimit lim;
  int rv;
  rv = getrlimit(PDB_RLIMIT_NOFILES, &lim);

  if (rv < 0)
    return 0;
  else
    return (lim.rlim_cur);
}

/*
 * Check to make sure we have enough file descriptors to run under this
 * database.
 */
static int pdb_check_max_files(pdb_handle* pdb) {
  int count;
  struct rlimit lim;
  int rv;
  char newpath[PATH_MAX];

  if (strlen(pdb->pdb_path) >= PATH_MAX) {
    return ENAMETOOLONG;
  }
  strcpy(newpath, pdb->pdb_path);

  count = pdb_count_subdirs(pdb, newpath);

  rv = getrlimit(PDB_RLIMIT_NOFILES, &lim);

  if (rv < 0) {
    int err = errno;
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "Cannot query resource limit for RLIMIT_NOFILES:%s", strerror(err));
    return PDB_ERR_NOT_SUPPORTED;
  }

  if (lim.rlim_cur != lim.rlim_max) {
    lim.rlim_cur = lim.rlim_max;

    rv = setrlimit(PDB_RLIMIT_NOFILES, &lim);
    if (rv < 0) {
      int err = errno;
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "setrlimit", err,
                   "Cannot change resource limit for RLIMIT_NOFILES");
      /*
       * Keep going. if it isn't enough, we'll learn about it
       * later.
       */
    }
  }

  rv = getrlimit(PDB_RLIMIT_NOFILES, &lim);
  if (rv < 0) {
    int err = errno;
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "getrlimit", err, "WTF!");
    return PDB_ERR_NOT_SUPPORTED;
  }

  if (lim.rlim_cur < (count * 3)) {
    cl_log(pdb->pdb_cl, CL_LEVEL_OPERATOR_ERROR,
           "pdb_check_max_files: You don't have enough"
           " file descriptors to run graphd. I counted %i"
           " files, which means you should have at least %i"
           " descriptors, but rlimit only reports %i descroptors."
           "\nPlease use 'limit' or 'ulimit' to give graphd more"
           " file descriptors.",
           count, count * 3, (int)lim.rlim_cur);

    return PDB_ERR_TOO_MANY;
  }
  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "This database has %i files and rlimit reports %i descriptor"
         " slots.",
         count, (int)(lim.rlim_cur));

  return 0;
}

int pdb_initialize_names(pdb_handle* pdb) {
  char const* dir;
  char *dir_buf, *dir_base;
  size_t dir_n, dir_base_n;

  if (pdb == NULL) return EINVAL;

  if (!(dir = pdb->pdb_path)) dir = PDB_PATH_DEFAULT;
  cl_assert(pdb->pdb_cl, dir != NULL);

  dir_n = strlen(dir);

  dir_buf = cm_malloc(pdb->pdb_cm, dir_n + 80);
  if (dir_buf == NULL) return ENOMEM;
  if (dir_n) memcpy(dir_buf, dir, dir_n);

  dir_base = dir_buf + dir_n;
  dir_base_n = 80;
  if (dir_base > dir_buf && dir_base[-1] != '/')
    *dir_base++ = '/', dir_base_n--;

  /* Create names of the various database directories.
   */
  strcpy(dir_base, "lock");
  pdb->pdb_lockfile_path = cm_strmalcpy(pdb->pdb_cm, dir_buf);
  if (!pdb->pdb_lockfile_path) goto oom;

  strcpy(dir_base, "primitive");
  pdb->pdb_primitive_path = cm_strmalcpy(pdb->pdb_cm, dir_buf);
  if (!pdb->pdb_primitive_path) goto oom;

  strcpy(dir_base, "header");
  pdb->pdb_header_path = cm_strmalcpy(pdb->pdb_cm, dir_buf);
  if (!pdb->pdb_header_path) goto oom;

  strcpy(dir_base, "from");
  pdb->pdb_left_path = cm_strmalcpy(pdb->pdb_cm, dir_buf);
  if (!pdb->pdb_left_path) goto oom;

  strcpy(dir_base, "to");
  pdb->pdb_right_path = cm_strmalcpy(pdb->pdb_cm, dir_buf);
  if (!pdb->pdb_right_path) goto oom;

  strcpy(dir_base, "scope");
  pdb->pdb_scope_path = cm_strmalcpy(pdb->pdb_cm, dir_buf);
  if (!pdb->pdb_scope_path) goto oom;

  strcpy(dir_base, "type");
  pdb->pdb_typeguid_path = cm_strmalcpy(pdb->pdb_cm, dir_buf);
  if (!pdb->pdb_typeguid_path) goto oom;

  strcpy(dir_base, "hmap");
  pdb->pdb_hmap_path = cm_strmalcpy(pdb->pdb_cm, dir_buf);
  if (!pdb->pdb_hmap_path) goto oom;

  strcpy(dir_base, "bmap/prefix");
  pdb->pdb_prefix_path = cm_strmalcpy(pdb->pdb_cm, dir_buf);
  if (!pdb->pdb_prefix_path) goto oom;

  strcpy(dir_base, "bmap/versioned");
  pdb->pdb_versioned_path = cm_strmalcpy(pdb->pdb_cm, dir_buf);
  if (!pdb->pdb_versioned_path) goto oom;

  cm_free(pdb->pdb_cm, dir_buf);

  return 0;

oom:
  cm_free(pdb->pdb_cm, dir_buf);

  return ENOMEM;
}

static int pdb_initialize_gmap(pdb_handle* pdb, addb_gmap** gmap_out,
                               char const* path, addb_gmap_id horizon) {
  int err;

  *gmap_out = addb_gmap_open(pdb->pdb_addb, path, ADDB_MODE_READ_WRITE, horizon,
                             &pdb->pdb_cf.pcf_gcf);
  if (!*gmap_out) return errno ? errno : ENOMEM;

  err = addb_gmap_backup(*gmap_out, horizon);
  if (err) {
    (void)addb_gmap_close(*gmap_out);
    *gmap_out = NULL;

    return err;
  }

  return 0;
}

static int pdb_initialize_hmap(pdb_handle* pdb, addb_hmap** hmap_out,
                               char const* path, addb_hmap_id horizon) {
  unsigned long long estimated_size = pdb->pdb_total_mem / 20;

  addb_gmap_configuration gcf = pdb->pdb_cf.pcf_gcf;
  int err;

  /* The gmap which provides storage for the hmap is much
   * larger than your garden variety gmap because every "id"
   * is used.
   */
  gcf.gcf_init_map = pdb->pdb_cf.pcf_hcf.hcf_gm_init_map;

  /*  The estimated size here is the size of the hmap only,
   *  that is, of the keys and administrative data -
   *  it does not include the gmap used to hold the actual
   *  destination IDs.
   *
   *  Making the size too small means long hash table chains;
   *  making it too large means unused space and even poorer
   *  locality in the hashtable than it has to begin with.
   */

  err =
      addb_hmap_open(pdb->pdb_addb, path, ADDB_MODE_READ_WRITE, estimated_size,
                     horizon, &pdb->pdb_cf.pcf_hcf, &gcf, hmap_out);
  if (err) return err;

  err = addb_hmap_backup(*hmap_out, horizon);
  if (err) goto close_hmap;
  return 0;

close_hmap:
  cl_assert(pdb->pdb_cl, err);

  (void)addb_hmap_close(*hmap_out);
  *hmap_out = (addb_hmap*)0;

  return err;
}

/*
 * make the bmap directory for the versioned and prefix maps
 */
static int pdb_initialize_bmap_dir(pdb_handle* pdb) {
  int err;
  char* dir;

  dir = cm_sprintf(pdb->pdb_cm, "%s/bmap", pdb->pdb_path);

  err = mkdir(dir, 0755);

  if (err && errno != EEXIST) {
    err = errno;
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "mkdir", errno,
                 "Can't can't make directory: %s", dir);
    return err;
  }

  cm_free(pdb->pdb_cm, dir);
  return 0;
}

int pdb_initialize_open_databases(pdb_handle* pdb) {
  int err;
  unsigned long long horizon;

  if (pdb == NULL) return EINVAL;

  cl_enter(pdb->pdb_cl, CL_LEVEL_SPEW, "enter");

  pdb->pdb_primitive =
      addb_istore_open(pdb->pdb_addb, pdb->pdb_primitive_path,
                       ADDB_MODE_READ_WRITE, &pdb->pdb_cf.pcf_icf);
  if (pdb->pdb_primitive == NULL) {
    err = errno ? errno : ENOMEM;
    cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "%s", strerror(err));
    return err;
  }

  horizon = addb_istore_horizon(pdb->pdb_primitive);

  if ((err = pdb_initialize_gmap(pdb, &pdb->pdb_left, pdb->pdb_left_path,
                                 horizon)) != 0 ||
      (err = pdb_initialize_gmap(pdb, &pdb->pdb_right, pdb->pdb_right_path,
                                 horizon) != 0) ||
      (err = pdb_initialize_gmap(pdb, &pdb->pdb_typeguid,
                                 pdb->pdb_typeguid_path, horizon) != 0) ||
      (err = pdb_initialize_gmap(pdb, &pdb->pdb_scope, pdb->pdb_scope_path,
                                 horizon) != 0)) {
    cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "one of the GMAPs fails: %s",
             strerror(err));
    return err;
  }

  err = pdb_initialize_hmap(pdb, &pdb->pdb_hmap, pdb->pdb_hmap_path, horizon);
  if (err) {
    cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "initialize_hmap failed: %s",
             strerror(err));

    return err;
  }

  err = pdb_initialize_bmap_dir(pdb);
  if (err) {
    cl_leave(pdb->pdb_cl, CL_LEVEL_FAIL, "Can't make bmap directory: %s",
             pdb_xstrerror(err));
    return err;
  }

  err = addb_bmap_open(pdb->pdb_addb, pdb->pdb_prefix_path,
                       32 * 32 * 32 * 32 * 32, /* 5 characters, 32 bits each */
                       horizon, false, &pdb->pdb_prefix);
  if (err) {
    cl_leave(pdb->pdb_cl, CL_LEVEL_FAIL, "bmap initialization failed: %s",
             strerror(err));
    return err;
  }

  err = addb_bmap_open(pdb->pdb_addb, pdb->pdb_versioned_path, 0, horizon,
                       false, &pdb->pdb_versioned);

  if (err) {
    cl_leave(pdb->pdb_cl, CL_LEVEL_FAIL,
             "bmap/versioned initialization failed: %s", strerror(err));
    return err;
  }
  cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "ok horizon=%llu", horizon);
  return 0;
}

int pdb_initialize_open_header(pdb_handle* pdb) {
  char flat_id[6 + 5 + 4];
  addb_data d;
  int err;
  unsigned int disk_version;
  unsigned int version = pdb->pdb_version;

  /*  Load our database ID from the contents of a file.
   *  It may be what we put in there; it may be something different.
   */
  if (pdb->pdb_database_id == (unsigned long long)-1) {
    pdb->pdb_database_id =
        (pdb->pdb_predictable ? 0x123456
                              : (((unsigned long long)pdb_local_ip() << 16) |
                                 (0xFFFFu & getpid())));
  }

  pdb_set5(flat_id, 0ull);
  pdb_set6(flat_id + 5, pdb->pdb_database_id);
  pdb_set4(flat_id + 5 + 6, version);

  pdb->pdb_header =
      addb_flat_open(pdb->pdb_addb, pdb->pdb_header_path, ADDB_MODE_READ_WRITE,
                     flat_id, sizeof(flat_id));

  if (!pdb->pdb_header) return errno ? errno : ENOMEM;

  if ((err = addb_flat_read(pdb->pdb_header, &d)) != 0) {
    (void)addb_flat_close(pdb->pdb_header);
    pdb->pdb_header = NULL;

    return err;
  }
  if (d.data_size < sizeof flat_id) {
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "%s needs to be at least 14 bytes long. "
           "Database format mismatch?",
           pdb->pdb_header_path);
    return PDB_ERR_DATABASE;
  }

  disk_version = pdb_get4((unsigned char*)d.data_memory + 11);

  if (version != disk_version) {
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "Graph format mismatch: %s reports format %u"
           " but this graphd supports version %u",
           pdb->pdb_header_path, disk_version, version);
    return PDB_ERR_DATABASE;
  }

  cl_assert(pdb->pdb_cl, d.data_size >= sizeof flat_id);
  pdb->pdb_database_id = pdb_get6((unsigned char*)d.data_memory + 5);

  /*
   *  Set the GUID in the pdb_handle that primitives should be
   *  compressed against.  It is simply the GUID formed with our
   *  database id and a local ID of zero.
   */
  graph_guid_from_db_serial(&pdb->pdb_database_guid, pdb->pdb_database_id, 0);

  return 0;
}

int pdb_configure_done(pdb_handle* pdb) {
  int err;
  struct stat st;
  char const* path;

  if (pdb == NULL) return EINVAL;
  if ((err = pdb_initialize_names(pdb)) != 0) return err;

  pdb->pdb_graph = graph_create(pdb->pdb_cm, pdb->pdb_cl);

  /* Physically create our containing directory
   */
  if (!(path = pdb->pdb_path)) path = PDB_PATH_DEFAULT;

  if (stat(path, &st) == 0) {
    if (!S_ISDIR(st.st_mode)) {
      cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
             "pdb: \"%s\" exists, but isn't a directory", path);
      return ENOTDIR;
    }
  } else {
    if (pdb->pdb_cf.pcf_create_database) {
      if (mkdir(path, 0755) < 0) {
        err = errno;
        cl_log(pdb->pdb_cl,
               errno == ENOENT || errno == EPERM ? CL_LEVEL_OPERATOR_ERROR
                                                 : CL_LEVEL_ERROR,
               "%s: can't create database "
               "directory: %s",
               path, strerror(errno));
        return err ? err : ENOTDIR;
      }
    } else {
      return PDB_ERR_SYNTAX;
    }
  }

  /*  Can we get a lock?
   */
  if ((err = pdb_lockfile_create(pdb, pdb->pdb_lockfile_path)) != 0) {
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL, "%s: can't get database lock %s: %s",
           path, pdb->pdb_lockfile_path, strerror(err));
    return err;
  }

  /* Initialize the ADDB.
   */
  if (pdb->pdb_addb == NULL) {
    unsigned long long tile_memory;

    tile_memory = pdb->pdb_total_mem * 0.5;

    /* Limit memory on 32-bit systems to no more
     * than 2G to avoid running out of mappable
     * address space.
     */
    if (sizeof(void*) < 8 && tile_memory > 2ull * 1024 * 1024 * 1024)
      tile_memory = 2ull * 1024 * 1024 * 1024;

    pdb->pdb_addb = addb_create(pdb->pdb_cm, pdb->pdb_cl, tile_memory,
                                pdb->pdb_cf.pcf_transactional);
    if (!pdb->pdb_addb) return ENOMEM;
  }
  pdb_check_max_files(pdb);
  return 0;
}

int pdb_initialize_checkpoint(pdb_handle* pdb) {
  addb_istore_id const next_id = addb_istore_next_id(pdb->pdb_primitive);
  addb_istore_id const horizon = addb_istore_horizon(pdb->pdb_primitive);

  if (next_id == horizon) return 0;

  cl_log(
      pdb->pdb_cl, CL_LEVEL_INFO | ADDB_FACILITY_RECOVERY,
      "pdb: synchronization needed--server didn't shut down cleanly; consider\n"
      "     reporting a bug.  Indices run up to %llu; primitives up to %llu.\n"
      "     Reindexing...",
      (unsigned long long)horizon, (unsigned long long)next_id);

  return pdb_checkpoint_synchronize(pdb);
}

/**
 * @brief Reread databases, given a previously initialized handle.
 * @param pdb	An initialized pdb handle
 * @return 0 on success, a nonzero error code on error.
 *
 * IMPORTANT: Previous databases must be closed (see `pdb_close_databases()').
 */
int pdb_initialize(pdb_handle* pdb) {
  int err;

  if (pdb == NULL) return EINVAL;

  err = pdb_initialize_open_databases(pdb);
  if (err) return err;

  err = pdb_initialize_open_header(pdb);
  if (err) return err;

  return 0;
}

int pdb_spawn(pdb_handle* pdb, pid_t pid) {
  cl_log(pdb->pdb_cl, CL_LEVEL_DEBUG, "pdb_spawn to pid=%lu",
         (unsigned long)pid);

  return pdb_lockfile_rewrite(pdb, pdb->pdb_lockfile_path, pid);
}
