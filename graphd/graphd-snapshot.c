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
#include "graphd/graphd-snapshot.h"

#include <errno.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

/**
 * Load the most recent snapshot of the database.
 *
 * Returns zero on success, otherwise a positive error code, in particular
 * ENODATA if a snapshot is not available (EIO on FreeBSD).
 */
int graphd_snapshot_restore(graphd_handle* g, srv_handle* srv,
                            graphd_database_config const* dcf) {
  char const* const fn = __FUNCTION__;
  cl_handle* const cl = srv_log(srv);
  bool ready_to_boot;
  char tmp_dir[512];   // a temporary directory
  char snap_rdy[512];  // path of ready-to-use snapshot
  char snap_tgz[512];  // path of .tgz snapshot
  char snap_tmp[512];  // path of snapshot in `tmp_dir'
  char tar_cmd[1024];  // the "tar -xzf ..." command line
  char rm_cmd[512];    // the "rm -fr TMPDIR" command line
  char path[512];      // for miscellaneous use
  char name[64];       // snapshot name ("graph.TIME")
  struct stat st;
  ssize_t len;
  int err = 0;

  if (dcf->dcf_snap == NULL) {
    cl_log(cl, CL_LEVEL_ERROR, "%s(): no snapshot directory specified", fn);
#if __FreeBSD__
    return EIO;
#else
    return ENODATA;
#endif
  }

  /*
   * RENAME CORRUPTED DATABASE DIRECTORY
   */

  if (stat(dcf->dcf_path, &st) == 0) {
    /* remove trailing '/' characters from the database path */
    int len = strlen(dcf->dcf_path);
    while (len && dcf->dcf_path[len - 1] == '/') len--;
    cl_assert(cl, len > 0);
    /* the new name is "graph-bad-TIME-PID" */
    len = snprintf(path, sizeof(path), "%.*s-bad-%ld-%lu", len, dcf->dcf_path,
                   (long)time(NULL), (unsigned long)getpid());
    if (len >= sizeof(path)) return ENAMETOOLONG;
    /* rename database directory */
    if (rename(dcf->dcf_path, path) && errno != ENOENT) {
      err = errno;
      cl_log(cl, CL_LEVEL_ERROR,
             "%s(): failed to rename "
             "database from `%s' to `%s': %s",
             fn, dcf->dcf_path, path, strerror(err));
      return err;
    }
  }

  int attempts = 0;
  int const attempts_max = 4;

retry:
  if (attempts >= attempts_max) {
    cl_log(cl, CL_LEVEL_ERROR, "%s(): giving up", fn);
#if __FreeBSD__
    return EIO;
#else
    return ENODATA;
#endif
  } else if (attempts > 0) {
    sleep(1);
    cl_log(cl, CL_LEVEL_ERROR, "%s(): retrying...", fn);
  }

  attempts++;

  /* Find out the name of the most recent snapshot by reading the
   * "graph.latest" symbolic link.
   */

  len = snprintf(path, sizeof(path), "%s/graph.latest", dcf->dcf_snap);
  if (len >= sizeof(path)) return ENAMETOOLONG;

  if ((len = readlink(path, name, sizeof(name))) == -1) {
    err = errno;
    if (err == ENOENT) {
      cl_log(cl, CL_LEVEL_ERROR, "%s(): file not found: %s", fn, path);
      goto retry;
    } else {
      cl_log(cl, CL_LEVEL_ERROR, "%s(): readlink(): %s", fn, strerror(err));
      return err;
    }
  }
  if (len >= sizeof(name)) return ENAMETOOLONG;
  name[len] = '\0';

  if (strncmp(name, "graph.", 6))  // just in case..
  {
    cl_log(cl, CL_LEVEL_ERROR, "%s(): unexpected snapshot name: %s", fn, name);
#if __FreeBSD__
    return EIO;
#else
    return ENODATA;
#endif
  }

  cl_log(cl, CL_LEVEL_INFO, "%s(): most recent snapshot: %s", fn, name);

  /*
   * some strings that will be needed later..
   */

  /* path of ready-to-use (uncompressed) snapshot */
  len = snprintf(snap_rdy, sizeof(snap_rdy), "%s/%s", dcf->dcf_snap, name);
  if (len >= sizeof(snap_rdy)) return ENAMETOOLONG;
  /* path of .tgz snapshot */
  len = snprintf(snap_tgz, sizeof(snap_tgz), "%s/%s.tgz", dcf->dcf_snap, name);
  if (len >= sizeof(snap_tgz)) return ENAMETOOLONG;
  /* path of temporary directory */
  len = snprintf(tmp_dir, sizeof(tmp_dir), "%s/%ld-%lu.tmp", dcf->dcf_snap,
                 (long)time(NULL), (unsigned long)getpid());
  if (len >= sizeof(tmp_dir)) return ENAMETOOLONG;
  /* path of ready-to-use snapshot inside `tmp_dir' */
  len = snprintf(snap_tmp, sizeof(snap_tmp), "%s/%s", tmp_dir, name);
  if (len >= sizeof(snap_tmp)) return ENAMETOOLONG;
  /* "tar -C TMPDIR -xzf SNAPSHOT.tgz" command line */
  len = snprintf(tar_cmd, sizeof(tar_cmd),
                 "tar -C \"%s\" -xzf \"%s\" &>/dev/null", tmp_dir, snap_tgz);
  if (len >= sizeof(tar_cmd)) return ENAMETOOLONG;
  /* "rm -fr TMPDIR" command line */
  len = snprintf(rm_cmd, sizeof(rm_cmd), "rm -fr \"%s\" &>/dev/null", tmp_dir);
  if (len >= sizeof(rm_cmd)) return ENAMETOOLONG;

  /*
   * BOOT FROM THE READY-TO-USE SNAPSHOT
   */

  ready_to_boot = false;  // <- this flag will tell us whether we have a
                          //    ready-to-use database snapshot at hand

  if (stat(snap_rdy, &st) == 0) {
    /* the ready-to-use snapshot becomes our new database */
    if (rename(snap_rdy, dcf->dcf_path) == 0) {
      ready_to_boot = true;
    } else if (errno == ENOENT) {
      /* maybe the snapshot directory is being updated in
       * this very instant? try again.. */
      cl_log(cl, CL_LEVEL_ERROR, "%s(): file `%s' no longer exists", fn,
             snap_rdy);
      goto retry;
    } else {
      err = errno;
      cl_log(cl, CL_LEVEL_ERROR, "%s(): failed to rename `%s' to `%s': %s", fn,
             snap_rdy, dcf->dcf_path, strerror(err));
      return err;
    }
  } else if (errno != ENOENT) {
    err = errno;
    cl_log(cl, CL_LEVEL_ERROR, "%s(): failed to stat() file `%s': %s", fn,
           snap_rdy, strerror(err));
    return err;
  }

reboot:
  if (ready_to_boot) {
    cl_log(cl, CL_LEVEL_INFO, "%s(): ready to boot from %s", fn, name);

    /* At this point we usually want to fork a child process to
     * unpack the .tgz snapshot for future use. However, it is
     * possible that a "tar -xzf" command left over from a very
     * recent crash already did this for us... so let's check!
     */
    if (stat(snap_rdy, &st) == 0) {
      return 0;
    } else {
      pid_t pid;

      if ((pid = fork()) == -1) {
        err = errno;
        cl_log_errno(cl, CL_LEVEL_ERROR, "fork", err, "unexpected error");
        return err;
      } else if (pid != 0) {
        /* PARENT: Return and start graphd */
        return 0;
      }

      /* CHILD: Proceed to unpack snapshot for future use */
    }
  } else {
    cl_log(cl, CL_LEVEL_INFO, "%s(): no ready-to-use snapshot available", fn);
  }

  /*
   * UNTAR SNAPSHOT
   */

  if (stat(snap_tgz, &st))  // first of all, do we have a .tgz file?
  {
    if (errno == ENOENT) {
      /* The .tgz file no longer exists. This can only mean
       * that the snapshot directory has been updated. If
       * graphd was previously booted from a ready-to-use
       * snapshot, then we're done, otherwise start over.
       */
      cl_log(cl, CL_LEVEL_ERROR, "%s(): file `%s' no longer exists", fn,
             snap_tgz);
      if (ready_to_boot)
        exit(0);
      else
        goto retry;
    } else {
      err = errno;
      cl_log(cl, CL_LEVEL_ERROR, "%s(): failed to stat() file `%s': %s", fn,
             snap_tgz, strerror(err));
      goto fail_untar;
    }
  }

  /* create a temp directory, the .tgz file will be extracted there */
  if (mkdir(tmp_dir, S_IRWXU)) {
    err = errno;
    cl_log(cl, CL_LEVEL_ERROR, "%s(): mkdir(): cannot create `%s': %s", fn,
           tmp_dir, strerror(err));
    goto fail_untar;
  }

  /* extract .tgz file */
  cl_log(cl, CL_LEVEL_INFO, "%s(): extracting: %s.tgz", fn, name);
  int ex = system(tar_cmd);
  if (ex == -1 || WEXITSTATUS(ex) != 0) {
    cl_log(cl, CL_LEVEL_INFO, "%s(): tar command error", fn);
    if (system(rm_cmd) != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "system", errno, "%s", rm_cmd);
      /* Ignore */
    }

    /* maybe the tar file has been deleted? if graphd is not
     * already running, then start over.. */
    if (!ready_to_boot)
      goto retry;
    else {
      err = EIO;
      goto fail_untar;
    }
  }

  /* Move the untarred snapshot to the appropriate directory, depending
   * on whether the snapshot is needed now or in the future.
   */

  char const* const dest = ready_to_boot ?  // graphd already booted?
                               snap_rdy
                                         :     // (needed in the future)
                               dcf->dcf_path;  // (needed now!)

  if (rename(snap_tmp, dest) == -1) {
    err = errno;
    cl_log(cl, CL_LEVEL_ERROR, "%s(): failed to rename `%s' to `%s': %s", fn,
           snap_tmp, dest, strerror(err));
    if (system(rm_cmd) != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "system", errno, "%s", rm_cmd);
    }
    goto fail_untar;
  }

  rmdir(tmp_dir);  // (`tmp_dir' should be empty now)

  /* if graphd is already running, then our job here is done... */
  if (ready_to_boot) exit(0);
  /* ...otherwise go back and start graphd at this time; we will also
   * untar the tgz snapshot again, so that we always have a ready-to-use
   * copy at hand */
  ready_to_boot = true;
  goto reboot;

fail_untar:
  cl_assert(cl, err);
  /* if graphd is already running, then exit quietly */
  if (ready_to_boot) exit(1);
  return err;
}
