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

#include <errno.h>

#include "libcl/cl.h"

/*
 *  Refresh pdb and addb when on-disk data structures may have changed.
 *
 *  This cannot handle destructive changes, but can handle the kinds of
 *  changes expected during an asynchronous write from another process.
 *
 *  (Of course we need to be paused while that write occurs to avoid returning
 *  bad data.)
 */
int pdb_refresh(pdb_handle* pdb) {
  cl_handle* cl = pdb->pdb_cl;
  addb_istore* is = pdb->pdb_primitive;
  int err;
  int i;
  pdb_id old_pdb_n, new_pdb_n;

  cl_enter(cl, CL_LEVEL_VERBOSE, "pdb_refresh");
  cl_assert(cl, !pdb_transactional(pdb));

  old_pdb_n = pdb_primitive_n(pdb);

  /*
   * Ordering here is important. We need to pick up the marker files
   * so that we can tell istore_refresh about the new pdb_n. Only then
   * may we refresh the istore files and the indecies themselves.
   */
  err = addb_istore_marker_read(is, &is->is_next);
  err = addb_istore_marker_read(is, &is->is_horizon);

  /*
   * Database is empty. Not an error
   */
  if (err == ENOENT && old_pdb_n == 0) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "pdb_refresh");
    return 0;
  } else if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_istore_marker_read", err,
                 "failed to read 'next' file");
    cl_leave(cl, CL_LEVEL_VERBOSE, "pdb_refresh: error: %s",
             pdb_xstrerror(err));
    return err;
  }

  new_pdb_n = pdb_primitive_n(pdb);

  /*
   * Nothing changed
   */
  if (old_pdb_n == new_pdb_n) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "pdb_refresh: no change");
    return 0;
  }

  cl_log(cl, CL_LEVEL_VERBOSE, "Refreshed database to id %llu",
         (unsigned long long)new_pdb_n);

  if (old_pdb_n == 0) {
    /* We may have restored from zero, or written from 0.
       Writing from 0 is fine. Restoring from zero causes files to
       close; so let's just close the whole thing and reopen it
       in the (extremely rare!) times we need to
     */
    pdb_close_databases(pdb);

    err = pdb_initialize_open_databases(pdb);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_ERROR, "pdb_initialize_open_databases", err,
                   "while moving from %llu to %llu",
                   (unsigned long long)old_pdb_n,
                   (unsigned long long)new_pdb_n);
      goto err;
    }

    err = pdb_initialize_open_header(pdb);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_ERROR, "pdb_initialize_open_header", err,
                   "while moving from %llu to %llu",
                   (unsigned long long)old_pdb_n,
                   (unsigned long long)new_pdb_n);
      goto err;
    }
  }

  err = addb_istore_refresh(is, new_pdb_n);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_istore_refresh", err,
                 "can't bring istore from %llu to %llu",
                 (unsigned long long)old_pdb_n, (unsigned long long)new_pdb_n);
    goto err;
  }

  for (i = 0; i < PDB_INDEX_N; i++) {
    pdb_index_instance* ii = pdb->pdb_indices + i;

    if (ii->ii_type->itx_refresh) {
      err = ii->ii_type->itx_refresh(pdb, ii, new_pdb_n);
      if (err) {
        cl_log_errno(cl, CL_LEVEL_ERROR, "ii->ii_type->itx_refresh", err,
                     "%s at path %s, %llu to %llu", ii->ii_type->ixt_name,
                     ii->ii_path, (unsigned long long)old_pdb_n,
                     (unsigned long long)new_pdb_n);
        goto err;
      }
    }
  }

/*
 * This function can get called the first time a smp follower runs
 * and pdb_n will be sitting around in memory with some horribly old
 * value.  That's okay, but lets not spend the rest of the hours
 *  verifying the world.
 */
// verify:
#if 0
	pdb_id verify_start;
	if ((pdb_primitive_n(pdb) - old_pdb_n) > 10000)
		verify_start = new_pdb_n - 10000;
	else
		verify_start = old_pdb_n;

	err = pdb_verify_range(pdb, verify_start, new_pdb_n, NULL);
	if (err)
	{
		cl_notreached(cl, "Verify of new primitives from %llu to %llu failed: %s "
			"Your database may be frobnicated or SMP synchronization may be brocken.",
			(unsigned long long)(old_pdb_n),
			(unsigned long long)(new_pdb_n),
			pdb_strerror(err));
	}
#endif

  /* Tell anybody on our subscription chain about our new primitives */
  pdb_id id;
  pdb_primitive pr;

  for (id = old_pdb_n; id < new_pdb_n; id++) {
    err = pdb_id_read(pdb, id, &pr);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err, "id=%llx",
                   (unsigned long long)id);
      goto err;
    }

    err = pdb_primitive_alloc_subscription_call(pdb, id, &pr);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_primitive_alloc_subscription_call",
                   err, "id=%llx", (unsigned long long)id);
      goto err;
    }

    pdb_primitive_finish(pdb, &pr);
  }

  cl_leave(cl, CL_LEVEL_VERBOSE, "pdb_refresh");
  return 0;

err:
  cl_leave(cl, CL_LEVEL_VERBOSE, "pdb_refresh: error: %s", pdb_xstrerror(err));
  return err;
}
