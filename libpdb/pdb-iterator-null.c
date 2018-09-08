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
#include <stdlib.h>
#include <string.h>

/**
 * @brief access the next primitive in an iteration
 *
 * @param pdb		opaque module handle, created with pdb_create()
 * @param it		iteration to set through
 * @param pdb_id_out	assign the pdb_id to this
 *
 * @return PDB_ERR_NO always
 */
static int pdb_iterator_null_next_loc(pdb_handle *pdb, pdb_iterator *it,
                                      pdb_id *pdb_id_out,
                                      pdb_budget *budget_inout,
                                      char const *file, int line) {
  PDB_IS_ITERATOR(pdb->pdb_cl, it);
  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "pdb_iterator_null_next");

  *budget_inout -= PDB_COST_FUNCTION_CALL;
  pdb_iterator_account_charge(pdb, it, next, 1, PDB_COST_FUNCTION_CALL);

  pdb_rxs_log(pdb, "NEXT %p null done ($%d)", (void *)it,
              PDB_COST_FUNCTION_CALL);
  return PDB_ERR_NO;
}

/**
 * @brief access the next primitive in an iteration
 *
 * @param pdb		opaque module handle, created with pdb_create()
 * @param it		iteration to set through
 * @param id_in_out	pdb-id
 * @param changed_out	set to true if changed.
 * @param file		source file name of calling code
 * @param line		source line of calling code
 *
 * @return PDB_ERR_NO always
 */
static int pdb_iterator_null_find_loc(pdb_handle *pdb, pdb_iterator *it,
                                      pdb_id id_in, pdb_id *id_out,
                                      pdb_budget *budget_inout,
                                      char const *file, int line) {
  (void)id_out;

  PDB_IS_ITERATOR(pdb->pdb_cl, it);
  *budget_inout -= PDB_COST_FUNCTION_CALL;
  pdb_iterator_account_charge(pdb, it, find, 1, PDB_COST_FUNCTION_CALL);

  pdb_rxs_log(pdb, "FIND %p null %llx done ($%d)", (void *)it,
              (unsigned long long)id_in, PDB_COST_FUNCTION_CALL);
  return PDB_ERR_NO;
}

static int pdb_iterator_null_freeze(pdb_handle *pdb, pdb_iterator *it,
                                    unsigned int flags, cm_buffer *buf) {
  char const *sep = "";
  int err;

  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  if (flags & PDB_ITERATOR_FREEZE_SET) {
    err = cm_buffer_add_string(buf, "null:");
    if (err != 0) return err;

    err = pdb_iterator_freeze_account(pdb, buf, it);
    if (err != 0) return err;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    err = cm_buffer_add_string(buf, sep);
    if (err != 0) return err;
    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_STATE) {
    err = cm_buffer_add_string(buf, sep);
    if (err != 0) return err;
  }
  return 0;
}

int pdb_iterator_null_thaw(pdb_handle *pdb, pdb_iterator_text const *pit,
                           pdb_iterator_base *pib, pdb_iterator **it_out) {
  int err;
  pdb_iterator_account *acc = NULL;
  char const *s = pit->pit_set_s, *e = pit->pit_set_e;

  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "pdb_iterator_null_thaw: %.*s/%.*s/%.*s",
         (int)(pit->pit_set_e - pit->pit_set_s), pit->pit_set_s,
         pit->pit_position_s ? (int)(pit->pit_position_e - pit->pit_position_s)
                             : 4,
         pit->pit_position_s ? pit->pit_position_s : "null",
         pit->pit_state_s ? (int)(pit->pit_state_e - pit->pit_state_s) : 4,
         pit->pit_state_s ? pit->pit_state_s : "null");

  err = pdb_iterator_util_thaw(pdb, &s, e, "%{account}%{extensions}%{end}", pib,
                               &acc, (pdb_iterator_property *)NULL);
  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_iterator_util_thaw", err,
                 "set \"%.*s\" (expected: ~LOW-HIGH)",
                 (int)(pit->pit_set_e - pit->pit_set_s), pit->pit_set_s);
    return err;
  }

  err = pdb_iterator_null_create(pdb, it_out);
  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_iterator_null_create", err,
                 "thawed from \"%.*s\"", (int)(pit->pit_set_e - pit->pit_set_s),
                 pit->pit_set_s);
    return err;
  }

  pdb_iterator_account_set(pdb, *it_out, acc);
  return 0;
}

/**
 * @brief Reset the current position in an iteration to the beginning
 *
 * @param pdb		opaque module handle, created with pdb_create()
 * @param it		iteration to set
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int pdb_iterator_null_reset(pdb_handle *pdb, pdb_iterator *it) {
  PDB_IS_ITERATOR(pdb->pdb_cl, it);
  cl_cover(pdb->pdb_cl);

  it->it_has_position = true;
  return 0;
}

static int pdb_iterator_null_clone(pdb_handle *pdb, pdb_iterator *it_in,
                                   pdb_iterator **it_out) {
  int err;

  PDB_IS_ITERATOR(pdb->pdb_cl, it_in);
  PDB_IS_ORIGINAL_ITERATOR(pdb->pdb_cl, it_in->it_original);

  if ((err = pdb_iterator_make_clone(pdb, it_in->it_original, it_out)) != 0)
    return err;
  (*it_out)->it_has_position = true;

  pdb_rxs_log(pdb, "CLONE %p null %p", (void *)it_in, (void *)*it_out);

  return 0;
}

static char const *pdb_iterator_null_to_string(pdb_handle *pdb,
                                               pdb_iterator *it, char *buf,
                                               size_t size) {
  it->it_displayname = cm_strmalcpy(pdb->pdb_cm, "null");
  return "null";
}

static int pdb_iterator_null_check(pdb_handle *pdb, pdb_iterator *it, pdb_id id,
                                   pdb_budget *budget_inout) {
  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  /*  We really should have headed this off algorithmically.
   *  Log so we can at least see that we're doing something wrong.
   */
  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE, "pdb_iterator_null_check id=%llx",
         (unsigned long long)id);

  pdb_iterator_account_charge(pdb, it, check, 1, PDB_COST_FUNCTION_CALL);
  *budget_inout -= PDB_COST_FUNCTION_CALL;
  pdb_rxs_log(pdb, "CHECK %p null %llx no", (void *)it, (unsigned long long)id);
  return PDB_ERR_NO;
}

static int pdb_iterator_null_range_estimate(pdb_handle *pdb, pdb_iterator *it,
                                            pdb_range_estimate *range) {
  pdb_iterator_range_estimate_default(pdb, it, range);

  range->range_n_max = range->range_n_exact = 0;

  return 0;
}

static const pdb_iterator_type pdb_iterator_null = {
    "null",

    pdb_iterator_util_finish,
    pdb_iterator_null_reset,
    pdb_iterator_null_clone,
    pdb_iterator_null_freeze,
    pdb_iterator_null_to_string,

    pdb_iterator_null_next_loc,
    pdb_iterator_null_find_loc,
    pdb_iterator_null_check,
    pdb_iterator_util_statistics_none,

    NULL,
    NULL,
    NULL,
    pdb_iterator_null_range_estimate,

    /* restrict */ NULL,

    /* suspend */ NULL,
    /* unsuspend  */ NULL};

/**
 * @brief initialize an empty iterator.
 *
 *  Once an iterator has been initialized, it is safe to
 *  free it with pdb_iterator_null_finish().
 *  If iterated over, it will return no records.
 *
 * @param pdb		module handle
 * @param it_out	the iterator to initialize
 */
int pdb_iterator_null_create_loc(pdb_handle *pdb, pdb_iterator **it_out,
                                 char const *file, int line) {
  pdb_iterator *it;
  if ((*it_out = it = cm_malloc(pdb->pdb_cm, sizeof(*it))) == NULL)
    return ENOMEM;

  pdb_iterator_make_loc(pdb, it, 0, 0, true, file, line);

  pdb_iterator_n_set(pdb, it, 0);
  pdb_iterator_check_cost_set(pdb, it, PDB_COST_FUNCTION_CALL);
  pdb_iterator_next_cost_set(pdb, it, PDB_COST_FUNCTION_CALL);
  pdb_iterator_sorted_set(pdb, it, true);
  pdb_iterator_statistics_done_set(pdb, it);

  it->it_type = &pdb_iterator_null;
  pdb_rxs_log(pdb, "CREATE %p null", (void *)it);

  return 0;
}

/**
 * @brief reinitialize an iterator as a null iterator.
 *
 *  Once an iterator has been initialized, it is safe to
 *  free it with pdb_iterator_null_finish().
 *  If iterated over, it will return no records.
 *
 * @param pdb		module handle
 * @param it		the preallocated iterator to initialize
 */
void pdb_iterator_null_reinitialize(pdb_handle *pdb, pdb_iterator *it) {
  pdb_iterator_make(pdb, it, 0, 0, true);

  pdb_iterator_n_set(pdb, it, 0);
  pdb_iterator_check_cost_set(pdb, it, PDB_COST_FUNCTION_CALL);
  pdb_iterator_next_cost_set(pdb, it, PDB_COST_FUNCTION_CALL);
  pdb_iterator_find_cost_set(pdb, it, 0);
  pdb_iterator_sorted_set(pdb, it, true);
  pdb_iterator_statistics_done_set(pdb, it);

  it->it_type = &pdb_iterator_null;

  pdb_rxs_log(pdb, "REINITIALIZE %p null", (void *)it);
}

/**
 * @brief reinitialize an iterator as a null iterator.
 *
 *  Once an iterator has been initialized, it is safe to
 *  free it with pdb_iterator_null_finish().
 *  If iterated over, it will return no records.
 *
 * @param pdb		module handle
 * @param it		the preallocated iterator to initialize
 */
int pdb_iterator_null_become(pdb_handle *pdb, pdb_iterator *it) {
  size_t refcount = it->it_refcount;
  size_t clones = it->it_clones;

  PDB_IS_ITERATOR(pdb->pdb_cl, it);

  if (it->it_original != it) pdb_iterator_unlink_clone(pdb, it);
  (it->it_type->itt_finish)(pdb, it);

  pdb_iterator_chain_out(pdb, it);
  pdb_iterator_suspend_chain_out(pdb, it);

  pdb_iterator_make(pdb, it, 0, 0, true);

  it->it_refcount = refcount;
  it->it_clones = clones;

  pdb_iterator_n_set(pdb, it, 0);
  pdb_iterator_check_cost_set(pdb, it, PDB_COST_FUNCTION_CALL);
  pdb_iterator_next_cost_set(pdb, it, PDB_COST_FUNCTION_CALL);
  pdb_iterator_find_cost_set(pdb, it, 0);
  pdb_iterator_sorted_set(pdb, it, true);
  pdb_iterator_statistics_done_set(pdb, it);

  it->it_type = &pdb_iterator_null;

  pdb_rxs_log(pdb, "BECOME %p null", (void *)it);
  return 0;
}

/**
 * @brief test for presence of an empty iterator.
 *
 *  Once an iterator has been initialized, it is safe to
 *  free it with pdb_iterator_null_finish().
 *  If iterated over, it will return no records.
 *
 * @param pdb	module handle
 * @param it	iterator to test
 * @return 	true if the passed-in iterator is a null
 *		iterator, false if not.
 */
bool pdb_iterator_null_is_instance(pdb_handle *pdb, pdb_iterator const *it) {
  PDB_IS_ITERATOR(pdb->pdb_cl, it);
  return it->it_type == &pdb_iterator_null;
}
