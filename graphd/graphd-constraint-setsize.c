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
#include "graphd/graphd-hash.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>

#define HAS_GUID(guidcon)                                                    \
  ((guidcon).guidcon_include_valid && (guidcon).guidcon_include.gs_n == 1 && \
   !GRAPH_GUID_IS_NULL((guidcon).guidcon_include.gs_guid[0]))

static int psum_setsize(graphd_handle* g, graphd_constraint const* con,
                        pdb_primitive_summary const* psum,
                        unsigned long long* n_inout) {
  pdb_handle* pdb = g->g_pdb;
  int err = 0, linkage;
  unsigned int no_need = 0;
  unsigned long long n_min, n;

  n_min = *n_inout;

  if (psum->psum_result != PDB_LINKAGE_N) return GRAPHD_ERR_NO;

  if (psum->psum_locked & (1 << PDB_LINKAGE_TYPEGUID)) {
    if (psum->psum_locked & (1 << PDB_LINKAGE_RIGHT)) {
      err = pdb_vip_linkage_guid_count(pdb, psum->psum_guid + PDB_LINKAGE_RIGHT,
                                       PDB_LINKAGE_RIGHT,
                                       psum->psum_guid + PDB_LINKAGE_TYPEGUID,
                                       con->con_low, con->con_high, n_min, &n);
      if (err == 0 && n < n_min) {
        n_min = n;
        no_need |= 1 << PDB_LINKAGE_RIGHT | 1 << PDB_LINKAGE_TYPEGUID;
      }
    }

    if (psum->psum_locked & (1 << PDB_LINKAGE_LEFT)) {
      err = pdb_vip_linkage_guid_count(pdb, psum->psum_guid + PDB_LINKAGE_LEFT,
                                       PDB_LINKAGE_LEFT,
                                       psum->psum_guid + PDB_LINKAGE_TYPEGUID,
                                       con->con_low, con->con_high, n_min, &n);
      if (err == 0 && n < n_min) {
        n_min = n;
        no_need |= 1 << PDB_LINKAGE_LEFT | 1 << PDB_LINKAGE_TYPEGUID;
      }
    }
  }

  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++) {
    if (!(psum->psum_locked & (1 << linkage)) || no_need & (1 << linkage))
      continue;

    err = pdb_linkage_guid_count_est(pdb, linkage, psum->psum_guid + linkage,
                                     con->con_low, con->con_high, n_min, &n);
    if (err == 0 && n < n_min) n_min = n;
  }
  return err;
}

static int linkguid_setsize(graphd_handle* g, graphd_constraint const* con,
                            unsigned long long* n_inout) {
  graph_guid const* const linkguid = con->con_linkguid;
  pdb_primitive_summary psum;
  int linkage;

  memset(&psum, 0, sizeof psum);
  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++) {
    if (!GRAPH_GUID_IS_NULL(linkguid[linkage])) {
      psum.psum_guid[linkage] = linkguid[linkage];
      psum.psum_locked |= 1 << linkage;
    }
  }
  psum.psum_result = PDB_LINKAGE_N;

  return psum_setsize(g, con, &psum, n_inout);
}

/*  Limit the number of IDs that can possibly match
 *  this constraint, given a fixed parent.
 */
void graphd_constraint_setsize_initialize(graphd_handle* g,
                                          graphd_constraint* con) {
  if (con->con_false)
    con->con_setsize = 0;

  else if (graphd_linkage_is_i_am(con->con_linkage))

    /*  For the purpose of this calculation, I have one
     *  fixed parent.  That parent points to me.
     */
    con->con_setsize = 1;

  else {
    con->con_setsize = pdb_primitive_n(g->g_pdb);
    if (con->con_high < con->con_setsize) con->con_setsize = con->con_high;

    con->con_setsize -= con->con_low;
  }
}

/*  Limit the number of IDs that can possibly match
 *  this constraint, given a fixed parent.
 */
int graphd_constraint_setsize(graphd_handle* g, graphd_constraint* con) {
  unsigned long long n;
  pdb_handle* pdb = g->g_pdb;
  int err;

  /*  Some of the functions below will use n as an
   *  upper bound (if we find more than that, we can
   *  stop looking.)
   */
  n = con->con_setsize;

  /*  Further reduce the setsize assumptions based on
   *  things we know about the iterator.
   */
  if (con->con_false)
    con->con_setsize = 0;

  else if (graphd_linkage_is_i_am(con->con_linkage))

    /*  For the purpose of this calculation, I have one
     *  fixed parent.  That parent points to me.
     */
    con->con_setsize = 1;

  else {
    pdb_primitive_summary psum;

    if (con->con_it != NULL && pdb_iterator_n_valid(pdb, con->con_it)) {
      n = pdb_iterator_n(pdb, con->con_it);
      if (n < con->con_setsize) con->con_setsize = n;
    } else if (con->con_it != NULL &&
               (err = pdb_iterator_primitive_summary(pdb, con->con_it,
                                                     &psum)) == 0) {
      err = psum_setsize(g, con, &psum, &n);
      if (err == 0 && n < con->con_setsize) con->con_setsize = n;
    } else {
      err = linkguid_setsize(g, con, &n);
      if (err == 0 && n < con->con_setsize) con->con_setsize = n;
    }
  }
  return 0;
}
