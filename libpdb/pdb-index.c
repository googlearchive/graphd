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

char const* pdb_index_name(int i) {
  switch (i) {
    case PDB_INDEX_LEFT:
      return "from";
    case PDB_INDEX_RIGHT:
      return "to";
    case PDB_INDEX_TYPEGUID:
      return "type";
    case PDB_INDEX_SCOPE:
      return "scope";

    case PDB_INDEX_HMAP:
    default:
      return NULL;
  }
}

/* Do one checkpoint stage if we are not yet at the target stage
 *
 * Return
 *	0 if the stage was completed successfully,
 *	PDB_ERR_MORE if the operation would block
 *	errno otherwise
 */

int pdb_index_do_checkpoint_stage(pdb_handle* pdb, pdb_index_instance* ii,
                                  pdb_checkpoint_stage target_stage,
                                  bool hard_sync, bool block) {
  int err;

  cl_assert(pdb->pdb_cl, target_stage > PDB_CKS_START);
  cl_assert(pdb->pdb_cl, target_stage <= PDB_CKS_N);
  cl_assert(pdb->pdb_cl, ii->ii_stage >= PDB_CKS_START);
  cl_assert(pdb->pdb_cl, ii->ii_stage <= PDB_CKS_N);

  if (ii->ii_stage >= target_stage) {
    cl_assert(pdb->pdb_cl, (ii->ii_stage == target_stage) ||
                               (ii->ii_stage - 1 == target_stage));
    return PDB_ERR_ALREADY; /* We're already there */
  } else {
    cl_assert(pdb->pdb_cl, ii->ii_stage == target_stage - 1);
  }

  if (ii->ii_stage >= PDB_CKS_N) return PDB_ERR_ALREADY; /* all done */

  if (ii->ii_type->ixt_checkpoint_fns[ii->ii_stage] == NULL)
    /* Nothing to do. */
    err = 0;
  else
    err =
        (*ii->ii_type->ixt_checkpoint_fns[ii->ii_stage])(ii, hard_sync, block);

  if (block && PDB_ERR_MORE == err)
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb_index_do_checkpoint_stage: PDB_ERR_MORE from type %s stage %d",
           ii->ii_type->ixt_name, ii->ii_stage);

  if (!err || PDB_ERR_ALREADY == err) ii->ii_stage++;

  return err;
}

/* Add a new primitive to the indices.  Not idempotent.
 */
int pdb_index_new_primitive(pdb_handle* pdb, pdb_id id,
                            pdb_primitive const* pr) {
  int err;

  /*  This will modify the database.  If there are iterators
   *  around that still point to database things, they need
   *  to suspend themselves.
   */
  if (pdb->pdb_iterator_n_unsuspended > 0) {
    err = pdb_iterator_suspend_all(pdb);
    if (err != 0) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_iterator_suspend_all", err,
                   "id=%llu", (unsigned long long)id);
      return err;
    }
  }
  cl_assert(pdb->pdb_cl, pdb->pdb_iterator_n_unsuspended == 0);

  err = pdb_linkage_synchronize(pdb, id, pr);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_linkage_synchronize", err,
                 "id=%llx", (unsigned long long)id);
    return err;
  }

  err = pdb_vip_synchronize(pdb, id, pr);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_vip_synchronize", err,
                 "id=%llx", (unsigned long long)id);
    return err;
  }

  err = pdb_generation_synchronize(pdb, id, pr);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_generation_synchronize", err,
                 "id=%llx", (unsigned long long)id);
    return err;
  }

  err = pdb_hash_synchronize(pdb, id, pr);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_hash_synchronize", err,
                 "id=%llx", (unsigned long long)id);
    return err;
  }

  err = pdb_value_bin_synchronize(pdb, id, pr);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_hash_synchronize", err,
                 "id=%llx", (unsigned long long)id);
    return err;
  }

  /*
   * Mark primitives that this one has versioned as
   * obsolete in the versioned bitmap
   */
  err = pdb_versioned_synchronize(pdb, id, pr);

  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_versioned_synchronize", err,
                 "id=%llx", (unsigned long long)id);
    return err;
  }
  err = pdb_primitive_alloc_subscription_call(pdb, id, pr);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL,
                 "pdb_primitive_alloc_subscription_call", err, "id=%llx",
                 (unsigned long long)id);
    return err;
  }

  return 0;
}
