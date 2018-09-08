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

/*  When building a new complex iterator, such as
 *  an "or", "and", "isa", or "linksto" (see graph/src),
 *  that iterator, once built, can check in with the
 *  by-name cache and mark itself as a clone of an
 *  existing original that embodies the same structure.
 *  This saves on redundant statistics calls.
 *
 *  Normally, this redundancy isn't a problem, because
 *  same iterators are cloned, not created; but when
 *  an iterator is frozen and thawed, the clone/original
 *  relationship among its subiterators is not saved and
 *  thus cannot be restored.  The by-name cache provides
 *  a central registry that iterators returning from
 *  a frozen state use to reconnect with their originals.
 */


/* Is there an original that's the by_name for s..e ?
 */
pdb_iterator *pdb_iterator_by_name_lookup(pdb_handle *pdb,
                                          pdb_iterator_base const *pib,
                                          char const *s, char const *e) {
  pdb_iterator_by_name const *is;

  is = cm_haccess((cm_hashtable *)&pib->pib_by_name, pdb_iterator_by_name, s,
                  e - s);
  if (is != NULL) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_by_name_lookup \"%.*s\" -> %p", (int)(e - s), s,
           (void *)is->is_it);
    return is->is_it;
  }
  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "pdb_iterator_by_name_lookup \"%.*s\" not found", (int)(e - s), s);
  return NULL;
}

/* The set s..e is a by_name for iterator <it>.
 */
int pdb_iterator_by_name_link(pdb_handle *pdb, pdb_iterator_base *pib,
                              pdb_iterator *it, char const *s, char const *e) {
  pdb_iterator_by_name *is;

  if (it->it_original != NULL) it = it->it_original;

  is = cm_hnew(&pib->pib_by_name, pdb_iterator_by_name, s, e - s);
  if (is == NULL) return errno ? errno : ENOMEM;

  if (is->is_it == NULL) {
    /*  Remember where we live.
     */
    is->is_pib = pib;
    is->is_it = it;
    it->it_by_name = is;

    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_by_name_link %.*s -> %p", (int)(e - s), s, (void *)it);
  } else {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_by_name_link: ignore %.*s -> %p (already "
           "links to %p)",
           (int)(e - s), s, (void *)it, (void *)is->is_it);
  }
  return 0;
}

/*  An original with a given by_name chain is being destroyed;
 *  free the by_name chain.
 */
void pdb_iterator_by_name_unlink(pdb_handle *pdb, pdb_iterator *it) {
  pdb_iterator_by_name *is;

  if ((is = it->it_by_name) != NULL) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_by_name_unlink: %.*s -> %p",
           (int)cm_hsize(&is->is_pib->pib_by_name, pdb_iterator_by_name, is),
           (char const *)cm_hmem(&is->is_pib->pib_by_name, pdb_iterator_by_name,
                                 is),
           (void *)it);

    cl_assert(pdb->pdb_cl, is->is_it == it);
    cm_hdelete(&is->is_pib->pib_by_name, pdb_iterator_by_name, is);
    it->it_by_name = NULL;
  }
}
