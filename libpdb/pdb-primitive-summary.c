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

#include <stdio.h>

char const* pdb_primitive_summary_to_string(pdb_handle* pdb,
                                            pdb_primitive_summary const* psum,
                                            char* buf, size_t size) {
  char const* b0 = buf;
  char* w = buf;
  char* e = buf + size;
  char const* sep = "";
  int linkage;

  snprintf(w, e - w, "%s%c", psum->psum_result == PDB_LINKAGE_N
                                 ? "primitive"
                                 : pdb_linkage_to_string(psum->psum_result),
           psum->psum_complete ? '(' : '{');
  w += strlen(w);

  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++) {
    char const* gp;
    char guid[GRAPH_GUID_SIZE];

    if (!(psum->psum_locked & (1 << linkage))) continue;

    gp = graph_guid_to_string(psum->psum_guid + linkage, guid, sizeof guid);
    /* Skip the start, it's boring */
    if (gp != NULL && strlen(gp) > 20) gp += 20;

    snprintf(w, e - w, "%s%s=%s", sep, pdb_linkage_to_string(linkage), gp);
    w += strlen(w);
    sep = ", ";
  }
  snprintf(w, e - w, "%c", psum->psum_complete ? ')' : '}');
  return b0;
}

/*  Does the primitive <pr> match the summary <psum> ?
 */
bool pdb_primitive_summary_match(pdb_handle* pdb, pdb_primitive const* pr,
                                 pdb_primitive_summary const* psum) {
  int l;

  if (psum->psum_result < PDB_LINKAGE_N)
    if (!pdb_primitive_has_linkage(pr, psum->psum_result)) return false;

  for (l = 0; l < PDB_LINKAGE_N; l++) {
    graph_guid guid;

    if (!(psum->psum_locked & (1 << l))) continue;

    if (!pdb_primitive_has_linkage(pr, l)) {
      if (!GRAPH_GUID_IS_NULL(psum->psum_guid[l])) return false;
    } else {
      pdb_primitive_linkage_get(pr, l, guid);

      if (!GRAPH_GUID_EQ(guid, psum->psum_guid[l])) return false;
    }
  }
  return true;
}

/* "Normalize" a primitive summary structure so that
 *  summaries with the same meaning have identical
 *  bit patterns (i.e., are useable as a key in the
 *  psum hashtable.)
 */
void pdb_primitive_summary_normalize(pdb_primitive_summary const* psum,
                                     pdb_primitive_summary* out) {
  int l;
  pdb_primitive_summary tmp;

  if (out == psum) {
    /* Move the input into a temporary.  Otherwise,
     * the memset() below this branch will clear
     * out {psum} as well as {out}!
     */
    tmp = *psum;
    psum = &tmp;
  }

  memset(out, 0, sizeof(*out));

  out->psum_result = psum->psum_result;
  out->psum_locked = psum->psum_locked;
  out->psum_complete = psum->psum_complete;

  /*  Make sure the uninvolved GUIDs are NULL.
   */
  for (l = 0; l < PDB_LINKAGE_N; l++)
    if (!(psum->psum_locked & (1 << l)))
      GRAPH_GUID_MAKE_NULL(out->psum_guid[l]);
    else
      out->psum_guid[l] = psum->psum_guid[l];
}

/*  We know <a> is true.  Is <b> possible?
 */
bool pdb_primitive_summary_allows(pdb_primitive_summary const* a,
                                  pdb_primitive_summary const* b) {
  int lin;

  /* If the result types aren't the same, we can't
   * really compare them.
   */
  if (a->psum_result != PDB_LINKAGE_N || b->psum_result != PDB_LINKAGE_N)
    return true;

  /*  All things that are locked in A and B are
   *  the same GUID in both.
   */
  for (lin = 0; lin < PDB_LINKAGE_N; lin++)

    if ((a->psum_locked & (1 << lin)) && (b->psum_locked & (1 << lin)) &&
        !GRAPH_GUID_EQ(a->psum_guid[lin], b->psum_guid[lin]))

      /* A excludes b. */
      return false;

  return true;
}

/*  Is a equal to, or a superset of, b ?
 */
bool pdb_primitive_summary_contains(pdb_primitive_summary const* a,
                                    pdb_primitive_summary const* b) {
  int lin;

  if (a->psum_result != b->psum_result) return false;

  /*  All things that are locked in A
   *  are also locked in B.
   *
   *  In other words, there are no things that are not
   *  locked in B and locked in A.
   */
  if ((a->psum_locked & ~b->psum_locked) != 0) return false;

  /*  All things that are locked in A are
   *  the same GUID in B.
   */
  for (lin = 0; lin < PDB_LINKAGE_N; lin++)

    if ((a->psum_locked & (1 << lin)) &&
        !GRAPH_GUID_EQ(a->psum_guid[lin], b->psum_guid[lin]))

      return false;

  /*  It looks like A is a superset of B.
   *
   *  The only way that could not be true would be if
   *  there are secret other side conditions about A
   *  that can't be seen in the primitive summary.
   */
  return a->psum_complete;
}
