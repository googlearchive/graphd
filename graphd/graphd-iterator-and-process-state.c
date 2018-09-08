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
#include "graphd/graphd.h"
#include "graphd/graphd-iterator-and.h"

#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

extern const pdb_iterator_type graphd_iterator_and_type;

void graphd_iterator_and_process_state_clear(and_process_state *ps) {
  memset(ps, 0, sizeof(*ps));

  ps->ps_id = PDB_ID_NONE;
  ps->ps_producer_id = PDB_ID_NONE;
  ps->ps_next_find_resume_id = PDB_ID_NONE;
  ps->ps_check_order = NULL;
  ps->ps_it = NULL;
  ps->ps_magic = GRAPHD_AND_PROCESS_STATE_MAGIC;
}

void graphd_iterator_and_process_state_finish(graphd_iterator_and *gia,
                                              and_process_state *ps) {
  cl_handle *cl = gia->gia_cl;

  GRAPHD_AND_IS_PROCESS_STATE(cl, ps);
  cl_enter(cl, CL_LEVEL_VERBOSE, "ps=%p", (void *)ps);

  if (ps->ps_it != NULL) {
    size_t i;
    for (i = 0; i < ps->ps_n; i++)
      pdb_iterator_destroy(gia->gia_pdb, ps->ps_it + i);
    cm_free(gia->gia_cm, ps->ps_it);
    ps->ps_it = NULL;
  }
  if (ps->ps_check_order != NULL) {
    cm_free(gia->gia_cm, ps->ps_check_order);
    ps->ps_check_order = NULL;
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "ps=%p", (void *)ps);
}

void graphd_iterator_and_process_state_delete_subcondition(
    pdb_iterator *it, and_process_state *ps, size_t i) {
  graphd_iterator_and *ogia = it->it_theory;
  pdb_handle *pdb = ogia->gia_pdb;
  cl_handle *cl = ogia->gia_cl;
  size_t k;

  /*  Not instantiated?
   */
  if (!ps->ps_n) {
    cl_assert(cl, ps->ps_it == NULL);
    cl_assert(cl, ps->ps_check_order == NULL);

    return;
  }

  cl_log(cl, CL_LEVEL_VERBOSE,
         "and_process_state_delete_subcondition %zu from ps->ps_n %zu", i,
         ps->ps_n);
  cl_assert(cl, i < ogia->gia_n);
  cl_assert(cl, i < ps->ps_n);
  cl_assert(cl, it->it_original == it);

  GRAPHD_AND_IS_PROCESS_STATE(cl, ps);
  if (ps->ps_it != NULL) {
    pdb_iterator_destroy(pdb, ps->ps_it + i);
    memmove(ps->ps_it + i, ps->ps_it + i + 1,
            sizeof(*ps->ps_it) * (ps->ps_n - (i + 1)));
  }
  if (ps->ps_check_order != NULL) {
    for (k = 0; k < ps->ps_n; k++) {
      if (ps->ps_check_order[k] > i)
        ps->ps_check_order[k]--;

      else if (ps->ps_check_order[k] == i) {
        /*  If we're in the middle of a slow
         *  check while you're deleting the guy
         *  we're slow-checking against, the
         *  call state jumps back to 0, and we'll
         *  resume with the guy behind the deleted
         *  one.
         */
        if (k == ps->ps_check_i)
          it->it_call_state = 0;
        else if (k < ps->ps_check_i)
          ps->ps_check_i--;

        if (k != ps->ps_n - 1) {
          memmove(ps->ps_check_order + k, ps->ps_check_order + k + 1,
                  (ps->ps_n - (k + 1)) * sizeof(*ps->ps_check_order));
          /*  Reexamine the index we just
           *  pulled over the deleted one!
           */
          k--;
        }
        ps->ps_n--;
      }
    }
  }
  if (ps->ps_check_i > ps->ps_n) ps->ps_check_i = ps->ps_n;
}

int graphd_iterator_and_process_state_clone(pdb_handle *pdb, pdb_iterator *it,
                                            and_process_state const *src,
                                            and_process_state *dst) {
  graphd_iterator_and *gia = it->it_theory;
  graphd_iterator_and *ogia = ogia(it);
  cm_handle *cm = gia->gia_cm;
  cl_handle *cl = gia->gia_cl;
  size_t i;
  int err;

  cl_assert(cl, src->ps_it != NULL);
  GRAPHD_AND_IS_PROCESS_STATE(cl, src);

  dst->ps_it = cm_malloc(cm, sizeof(*dst->ps_it) * ogia->gia_n);
  if (dst->ps_it == NULL) return ENOMEM;

  for (i = 0; i < ogia->gia_n; i++) {
    PDB_IS_ITERATOR(cl, src->ps_it[i]);

    err = pdb_iterator_clone(pdb, src->ps_it[i], dst->ps_it + i);
    if (err != 0) {
      while (i > 0) {
        i--;
        pdb_iterator_destroy(pdb, dst->ps_it + i);
        cm_free(cm, dst->ps_it);
        dst->ps_it = NULL;
      }
      return err;
    }
    cl_assert(cl, pdb_iterator_has_position(pdb, dst->ps_it[i]));
  }
  return graphd_iterator_and_check_sort_refresh(it, dst);
}

int graphd_iterator_and_process_state_initialize(pdb_handle *pdb,
                                                 pdb_iterator *it,
                                                 and_process_state *ps) {
  graphd_iterator_and *gia = it->it_theory;
  cl_handle *cl = gia->gia_cl;
  size_t i;
  int err;
  char buf[200];

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_and_process_state_initialize: %p for %s", (void *)ps,
         pdb_iterator_to_string(pdb, it, buf, sizeof buf));

  ps->ps_magic = GRAPHD_AND_PROCESS_STATE_MAGIC;

  if (ps->ps_it != NULL) return 0;
  cl_assert(cl, gia->gia_n > 0);

  ps->ps_it = cm_malloc(gia->gia_cm, sizeof(*ps->ps_it) * gia->gia_n);
  if (ps->ps_it == NULL) return errno ? errno : ENOMEM;

  for (i = 0; i < gia->gia_n; i++) {
    err = pdb_iterator_clone(pdb, ogia(it)->gia_sc[i].sc_it, ps->ps_it + i);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "it=%s",
                   pdb_iterator_to_string(pdb, ogia(it)->gia_sc[i].sc_it, buf,
                                          sizeof buf));

      while (i > 0) pdb_iterator_destroy(pdb, ps->ps_it + --i);
      cm_free(gia->gia_cm, ps->ps_it);
      ps->ps_it = NULL;

      return err;
    }
    cl_assert(cl, pdb_iterator_has_position(pdb, ps->ps_it[i]));
  }
  GRAPHD_AND_IS_PROCESS_STATE(cl, ps);
  err = graphd_iterator_and_check_sort_refresh(it, ps);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "garphd_iterator_and_check_sort_refresh",
                 err, "it=%s",
                 pdb_iterator_to_string(pdb, it, buf, sizeof buf));

    for (i = gia->gia_n; i > 0; i--) pdb_iterator_destroy(pdb, ps->ps_it + i);
    cm_free(gia->gia_cm, ps->ps_it);
    ps->ps_it = NULL;
  }
  return err;
}
