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

#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static cm_list_offsets const pdb_iterator_suspend_offsets =
    CM_LIST_OFFSET_INIT(pdb_iterator, it_suspend_next, it_suspend_prev);

/*  Unsuspend all iterators that are getting suspend/unsuspend notifications.
 */
int pdb_iterator_unsuspend_chain(pdb_handle* pdb, pdb_iterator_chain* pic) {
  pdb_iterator *next, *it;
  size_t n_customers = 0;
  int err;

  for (next = pic->pic_head; (it = next) != NULL;) {
    next = it->it_next;
    if (!it->it_suspended) continue;

    if (it->it_original != NULL && it->it_original->it_suspended) {
      /*  Always unsuspend the original
       *  before the clone.
       */
      pdb->pdb_iterator_n_unsuspended++;
      n_customers++;

      err = pdb_iterator_unsuspend(pdb, it->it_original);
      if (err != 0) {
        char buf[200];
        cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_iterator_unsuspend", err,
                     "it=%s", pdb_iterator_to_string(pdb, it, buf, sizeof buf));
        return err;
      }
      if (it->it_original->it_chain != NULL) {
        cl_assert(pdb->pdb_cl, it->it_original->it_chain->pic_n_suspended > 0);
        it->it_original->it_chain->pic_n_suspended--;
      }
    }

    if (it->it_suspended) {
      pdb->pdb_iterator_n_unsuspended++;
      n_customers++;

      err = pdb_iterator_unsuspend(pdb, it);
      if (err != 0) {
        char buf[200];
        cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_iterator_unsuspend", err,
                     "it=%s", pdb_iterator_to_string(pdb, it, buf, sizeof buf));
        return err;
      }
      cl_assert(pdb->pdb_cl, !it->it_suspended);

      if (it->it_chain != NULL) {
        cl_assert(pdb->pdb_cl, it->it_chain->pic_n_suspended > 0);
        it->it_chain->pic_n_suspended--;
      }
    }
  }
  if (n_customers > 0) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_unsuspend_chain: %zu unsuspended.", n_customers);
  }

  return 0;
}

/*  Iterators place themselves in the global "suspend chain"
 *  when they want to get global suspend notices.  Iterators
 *  can exist outside the chain if they are not influenced by
 *  database growth.
 */
int pdb_iterator_suspend_all(pdb_handle* pdb) {
  if (pdb->pdb_iterator_n_unsuspended > 0) {
    pdb_iterator *it, *it_next;
    int err;
    size_t n_customers = 0;

    it_next = pdb->pdb_iterator_suspend_chain.pic_head;
    while ((it = it_next) != NULL) {
      it_next = it->it_suspend_next;
      if (!it->it_suspended) {
        cl_assert(pdb->pdb_cl, pdb->pdb_iterator_n_unsuspended > 0);

        pdb->pdb_iterator_n_unsuspended--;
        n_customers++;

        /*  The request that's currently running
         *  may not be this iterator's owner.
         *  Switch into this iterator's resource
         *  monitor context, if needed.
         */
        err = pdb_iterator_suspend(pdb, it);
        if (err != 0) {
          char buf[200];

          cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_iterator_suspend", err,
                       "it=%s",
                       pdb_iterator_to_string(pdb, it, buf, sizeof buf));
          return err;
        }
        cl_assert(pdb->pdb_cl, it->it_suspended);

        if (it->it_chain != NULL) it->it_chain->pic_n_suspended++;
      }
    }
    if (n_customers > 0) {
      cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
             "pdb_iterator_suspend_all: %zu suspended.", n_customers);
    }
  }

  cl_assert(pdb->pdb_cl, pdb->pdb_iterator_n_unsuspended == 0);
  return 0;
}

/*  Remove <it> from the suspend- or unsuspend chain,
 *  whichever it is in at the moment.
 *
 *  If it isn't in any of those chains, the call is harmless and does nothing.
 */
void pdb_iterator_suspend_chain_out(pdb_handle* pdb, pdb_iterator* it) {
  cl_assert(pdb->pdb_cl, it != NULL);

  if (!pdb_iterator_suspend_is_chained_in(pdb, it)) return;

  if (!it->it_suspended) {
    cl_assert(pdb->pdb_cl, pdb->pdb_iterator_n_unsuspended > 0);
    pdb->pdb_iterator_n_unsuspended--;
  } else {
    if (it->it_chain != NULL) it->it_chain->pic_n_suspended--;
  }

  cm_list_remove(pdb_iterator, pdb_iterator_suspend_offsets,
                 &pdb->pdb_iterator_suspend_chain.pic_head,
                 &pdb->pdb_iterator_suspend_chain.pic_tail, it);
  it->it_suspend_next = NULL;
  it->it_suspend_prev = NULL;
  it->it_suspended = false;
}

void pdb_iterator_suspend_chain_in(pdb_handle* pdb, pdb_iterator* it) {
  cl_assert(pdb->pdb_cl, it != NULL);

  if (!it->it_suspended) {
    pdb->pdb_iterator_n_unsuspended++;
  } else {
    if (it->it_chain != NULL) {
      it->it_chain->pic_n_suspended++;
    }
  }

  cm_list_enqueue(pdb_iterator, pdb_iterator_suspend_offsets,
                  &pdb->pdb_iterator_suspend_chain.pic_head,
                  &pdb->pdb_iterator_suspend_chain.pic_tail, it);
}

bool pdb_iterator_suspend_is_chained_in(pdb_handle* pdb, pdb_iterator* it) {
  return it->it_suspend_prev != NULL ||
         pdb->pdb_iterator_suspend_chain.pic_head == it;
}

/*  The iterator <it> is about to move.  Save its suspended- or
 *  unsuspended subscription state, and cancel that subscription
 *  at the old location.
 */
void pdb_iterator_suspend_save(pdb_handle* pdb, pdb_iterator* it,
                               pdb_iterator_chain** chain_out) {
  if (pdb_iterator_suspend_is_chained_in(pdb, it)) {
    *chain_out = &pdb->pdb_iterator_suspend_chain;
    cm_list_remove(pdb_iterator, pdb_iterator_suspend_offsets,
                   &pdb->pdb_iterator_suspend_chain.pic_head,
                   &pdb->pdb_iterator_suspend_chain.pic_tail, it);
    it->it_suspend_next = NULL;
    it->it_suspend_prev = NULL;

    if (it->it_suspended) {
      if (it->it_chain != NULL) it->it_chain->pic_n_suspended--;
    } else {
      pdb->pdb_iterator_n_unsuspended--;
    }
  } else {
    *chain_out = NULL;
  }
}

/*  The iterator <it> has just moved.  Restore its suspended- or
 *  unsuspended subscription state, as saved in <chain> with a
 *  call to pdb_iterator_suspend_save().
 */
void pdb_iterator_suspend_restore(pdb_handle* pdb, pdb_iterator* it,
                                  pdb_iterator_chain* chain) {
  if (chain != NULL) {
    if (it->it_suspended) {
      if (it->it_chain != NULL) it->it_chain->pic_n_suspended++;
    } else {
      pdb->pdb_iterator_n_unsuspended++;
    }

    cm_list_enqueue(pdb_iterator, pdb_iterator_suspend_offsets,
                    &pdb->pdb_iterator_suspend_chain.pic_head,
                    &pdb->pdb_iterator_suspend_chain.pic_tail, it);
  }
}
