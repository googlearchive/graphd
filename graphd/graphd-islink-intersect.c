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
#include "graphd/graphd-islink.h"

#include <errno.h>

/*  After finding this many in an intersection, stop keeping track
 *  of the specific values, and just count the number of results.
 */
#define GRAPHD_ISLINK_INTERSECT_SMALL_MAX (16 * 1024)

/*  Free contents of an intersection structure.
 */
void graphd_islink_intersect_finish(graphd_handle* g,
                                    graphd_islink_intersect* ii) {
  if (ii->ii_include_set) {
    if (ii->ii_include.ii_idset != NULL) {
      graph_idset_free(ii->ii_include.ii_idset);
      ii->ii_include.ii_idset = NULL;
    }
  }
  if (ii->ii_exclude_set) {
    if (ii->ii_exclude.ii_idset != NULL) {
      graph_idset_free(ii->ii_exclude.ii_idset);
      ii->ii_exclude.ii_idset = NULL;
    }
  }
}

/*  Look up an existing group in the group hashtable.
 *  Return GRAPHD_ERR_MORE if we don't know what it is.
 */
int graphd_islink_intersect_lookup(graphd_handle* g,
                                   graphd_islink_key const* key1,
                                   graphd_islink_key const* key2,
                                   graphd_islink_intersect* ii_out) {
  graphd_islink_group const *g1, *g2;
  unsigned long long isec_key;
  graphd_islink_intersect* ii;

  if (g->g_islink == NULL) return GRAPHD_ERR_MORE;

  /*  Get the two groups.
   */
  if ((g1 = graphd_islink_group_lookup(g, key1)) == NULL ||
      (g2 = graphd_islink_group_lookup(g, key2)) == NULL)
    return GRAPHD_ERR_MORE;

  /*  Look up their intersection.
   */
  if (g1->group_id == GRAPHD_ISLINK_GROUPID_NONE ||
      g2->group_id == GRAPHD_ISLINK_GROUPID_NONE)
    return GRAPHD_ERR_MORE;

  /*  Make the key.
   */
  isec_key = GRAPHD_ISLINK_INTERSECT(g1->group_id, g2->group_id);

  /*  Do we have anything for that key?
   */
  ii = cm_haccess(&g->g_islink->ih_intersect, graphd_islink_intersect,
                  &isec_key, sizeof isec_key);

  /* We know the intersection results, either implicitly
   * (it's empty) or explicitly.
   */
  if (ii == NULL) {
    ii_out->ii_include_set = true;
    ii_out->ii_include.ii_idset = NULL;
    ii_out->ii_exclude_set = false;
    ii_out->ii_exclude.ii_count = 0;
  } else {
    *ii_out = *ii;
  }
  return 0;
}

int graphd_islink_intersect_make(graphd_handle* g, graphd_islink_group* g1,
                                 graphd_islink_group* g2,
                                 graphd_islink_intersect* ii) {
  graph_idset_position pos;
  unsigned long long id, n_intersect = 0;

  /*  Make sure g1 is the smaller one of the two.
   */
  if (g1->group_idset->gi_n > g2->group_idset->gi_n) {
    graphd_islink_group* g_tmp = g1;
    g1 = g2;
    g2 = g_tmp;
  }

  for (n_intersect = 0, graph_idset_next_reset(g1->group_idset, &pos);
       graph_idset_next(g1->group_idset, &id, &pos); n_intersect++) {
    bool both = graph_idset_check(g2->group_idset, id);
    int err;

    if (ii == NULL) {
      unsigned long long isec_key;

      if (!both) continue;

      isec_key = GRAPHD_ISLINK_INTERSECT(g1->group_id, g2->group_id);

      /*  Create an intersection record.
       */
      ii = cm_hnew(&g->g_islink->ih_intersect, graphd_islink_intersect,
                   &isec_key, sizeof isec_key);
      if (ii == NULL) {
        graphd_islink_panic(g);
        return errno ? errno : ENOMEM;
      }

      ii->ii_exclude_set = false;
      ii->ii_exclude.ii_count = n_intersect;

      ii->ii_include_set = true;
      ii->ii_include.ii_idset = NULL;
    }

    err = graphd_islink_intersect_add(g, ii, id, both);
    if (err != 0) {
      cl_log_errno(g->g_cl, CL_LEVEL_FAIL, "graphd_islink_intersect_add", err,
                   "id=%llu", id);
      return err;
    }
  }
  return 0;
}

/*  Given the knowledge that <id> is (not) included in the
 *  intersection between g1 and g2, update their intersection
 *  data structure <ii>.
 *
 *  So far, knowledge about <n_added> ids has been presented
 *  to the intersection <ii>.
 */
int graphd_islink_intersect_add(graphd_handle* g, graphd_islink_intersect* ii,
                                pdb_id id, bool included) {
  graph_idset** set = NULL;
  int err;

  if (included) {
    if (ii->ii_include_set) {
      if (ii->ii_include.ii_idset->gi_n >= GRAPHD_ISLINK_INTERSECT_SMALL_MAX) {
        /*  Switch from enumerating included things
         *  to just counting them.
         */
        graph_idset_free(ii->ii_include.ii_idset);

        ii->ii_include_set = false;
        ii->ii_include.ii_count = GRAPHD_ISLINK_INTERSECT_SMALL_MAX + 1;

        return 0;
      }
      set = &ii->ii_include.ii_idset;
    } else {
      ii->ii_include.ii_count++;
      return 0;
    }
  } else {
    if (ii->ii_exclude_set) {
      if (ii->ii_exclude.ii_idset->gi_n >= GRAPHD_ISLINK_INTERSECT_SMALL_MAX) {
        /*  Switch from enumerating included things
         *  to just counting them.
         */
        graph_idset_free(ii->ii_exclude.ii_idset);
        ii->ii_exclude.ii_idset = NULL;
        ii->ii_exclude_set = false;
        ii->ii_exclude.ii_count = GRAPHD_ISLINK_INTERSECT_SMALL_MAX + 1;
        return 0;
      }
      set = &ii->ii_exclude.ii_idset;
    } else {
      ii->ii_exclude.ii_count++;
      return 0;
    }
  }

  if (*set == NULL) {
    /* With an idset with a single element.
     */
    *set = graph_idset_tile_create(g->g_graph);
    if (*set == NULL) {
      graphd_islink_panic(g);
      return errno ? errno : ENOMEM;
    }
  }
  err = graph_idset_insert(*set, id);
  if (err != 0) return err;

  return 0;
}
