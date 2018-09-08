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
#include <unistd.h>

int pdb_close_databases(pdb_handle* pdb) {
  size_t i;
  int result = 0, err;

  cl_enter(pdb->pdb_cl, CL_LEVEL_SPEW, "enter");
  err = addb_istore_close(pdb->pdb_primitive);
  if (err) {
    if (!result) result = err;
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "addb_istore_close", err,
                 "Unable to close: %s", pdb->pdb_primitive_path);
  }
  pdb->pdb_primitive = NULL;

  for (i = 0; i < PDB_INDEX_N; i++) {
    err = (*pdb->pdb_indices[i].ii_type->ixt_close)(pdb, &pdb->pdb_indices[i]);
    if (err) {
      if (!result) result = err;
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "ixt_close", err,
                   "Unable to close: %s", pdb->pdb_indices[i].ii_path);
    }
    pdb->pdb_indices[i].ii_impl.any = (void*)0;
  }

  err = addb_flat_close(pdb->pdb_header);
  if (err) {
    if (!result) result = err;
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "addb_flat_close", err,
                 "Unable to close: %s", pdb->pdb_header_path);
  }
  pdb->pdb_header = NULL;

  cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "leave");
  return result;
}

/**
 * @brief Free resources associated with a pdb handle
 *
 *  An error indicates that an error occurred while
 *  freeing resources; the process of freeing resources
 *  nevertheless continues.
 *
 * @param pdb	NULL or a pdb handle.
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_destroy(pdb_handle* pdb) {
  int result = 0;
  int err;
  int i;

  if (pdb == NULL) return 0;

  cl_cover(pdb->pdb_cl);
  pdb_primitive_alloc_subscription_free(pdb);
  graph_destroy(pdb->pdb_graph);

  /*  Complain about leftover iterators, and crash
   *  if there are any!
   */
  pdb_iterator_chain_finish(pdb, &pdb->pdb_iterator_chain_buf, "pdb_destroy");

  if (pdb->pdb_addb != NULL) {
    err = pdb_close_databases(pdb);
    if (err != 0 && result == 0) result = err;

    addb_destroy(pdb->pdb_addb);
    if (unlink(pdb->pdb_lockfile_path)) {
      if ((err = errno) != 0 && errno != ENOENT) {
        if (result == 0) result = err;

        cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "unlink", err,
                     "unexpected error "
                     "while removing database lockfile "
                     "\"%s\"",
                     pdb->pdb_lockfile_path);
      }
    }
  }

  /* Free the pathnames.
   */
  if (pdb->pdb_lockfile_path != NULL)
    cm_free(pdb->pdb_cm, pdb->pdb_lockfile_path);

  if (pdb->pdb_primitive_path != NULL)
    cm_free(pdb->pdb_cm, pdb->pdb_primitive_path);

  if (pdb->pdb_header_path != NULL) cm_free(pdb->pdb_cm, pdb->pdb_header_path);

  for (i = 0; i < PDB_INDEX_N; i++)
    if (pdb->pdb_indices[i].ii_path)
      cm_free(pdb->pdb_cm, (void*)pdb->pdb_indices[i].ii_path);

  if (pdb->pdb_path != NULL) cm_free(pdb->pdb_cm, pdb->pdb_path);

  /*  Free the type analysis cache.
   */
  cm_free(pdb->pdb_cm, pdb);

  return result;
}
