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
#include "libpdb/pdb.h"
#include "libpdb/pdbp.h"

#include <errno.h>

static cm_list_offsets const pdb_primitive_subscription_offsets =
    CM_LIST_OFFSET_INIT(pdb_primitive_subscription, pps_next, pps_prev);

/*  Subscribe to primitive updates.  Whenever a primitive
 *  is added to the store, the callback is called with it.
 *
 *  There is no way of removing a subscriptiion - the callbacks
 *  are only freed at the very end.
 *
 *   @param pdb		pdb handle
 *   @param callback	function to call
 *   @param data	first parameter to pass to function
 *
 *   @return 0 on success, a nonzero error code on memory allocation error
 */
int pdb_primitive_alloc_subscription_add(pdb_handle* pdb,
                                         pdb_primitive_callback* callback,
                                         void* data) {
  pdb_primitive_subscription* pps;

  pps = cm_malloc(pdb->pdb_cm, sizeof(*pps));
  if (pps == NULL) return errno ? errno : ENOMEM;

  memset(pps, 0, sizeof(*pps));

  pps->pps_callback = callback;
  pps->pps_callback_data = data;
  pps->pps_prev = NULL;
  pps->pps_next = NULL;

  cm_list_enqueue(
      pdb_primitive_subscription, pdb_primitive_subscription_offsets,
      &pdb->pdb_primitive_alloc_head, &pdb->pdb_primitive_alloc_tail, pps);

  return 0;
}

/*  Invoke primitive allocation callbacks.
 *
 *   @param pdb		pdb handle
 *   @param id		the local database ID of the new primitive,
 *			or PDB_ID_NONE
 *   @param pr		the primitive structure, or NULL
 *
 *   @return 0 on success, a nonzero error code on (fatal, unexpected)
 *   	error
 *
 *  If the id is PDB_ID_NONE, and pr is NULL, the database is being
 *  truncated, and the callback should reset its information.
 *
 *  There is no transactional interface to these callbacks - if any
 *  other than the first fails, it's very bad.  It's a good idea to be
 *  robust against duplicate insertions to the database.
 */
int pdb_primitive_alloc_subscription_call(pdb_handle* pdb, pdb_id id,
                                          pdb_primitive const* pr) {
  pdb_primitive_subscription const* pps;

  for (pps = pdb->pdb_primitive_alloc_head; pps != NULL; pps = pps->pps_next) {
    int err = (*pps->pps_callback)(pps->pps_callback_data, pdb, id, pr);
    if (err != 0) return err;
  }
  return 0;
}

/*  Free subscription management data structure.
 *   @param pdb		pdb handle
 */
void pdb_primitive_alloc_subscription_free(pdb_handle* pdb) {
  pdb_primitive_subscription *pps, *pps_next;

  pps_next = pdb->pdb_primitive_alloc_head;
  while ((pps = pps_next) != NULL) {
    pps_next = pps->pps_next;
    cm_free(pdb->pdb_cm, pps);
  }
  pdb->pdb_primitive_alloc_head = pdb->pdb_primitive_alloc_tail = NULL;
}
