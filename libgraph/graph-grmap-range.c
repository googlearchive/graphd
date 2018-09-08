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
#include <time.h>
#include <errno.h>
#include <ctype.h>

#include "libgraph/graphp.h"

#define tab_low(tab) ((tab)->tab_data->range_low)
#define tab_high(tab)                            \
  ((tab)->tab_data[(tab)->tab_n - 1].range_low + \
   (tab)->tab_data[(tab)->tab_n - 1].range_n)

/* Utility: remove a range record.  Caller takes care of the
 * 	larger rebalancing that may happen as a result.
 */
void graph_grmap_range_delete(graph_grmap *grm, graph_grmap_table *tab,
                              size_t i) {
  if (i >= tab->tab_n) return;

  if (i + 1 < tab->tab_n) {
    cl_cover(grm->grm_graph->graph_cl);
    memmove(tab->tab_data + i, tab->tab_data + i + 1,
            (tab->tab_n - (i + 1)) * sizeof(*tab->tab_data));
  } else {
    cl_cover(grm->grm_graph->graph_cl);
  }
  tab->tab_n--;
}

bool graph_grmap_range_adjacent(graph_grmap_range const *const a,
                                graph_grmap_range const *const b) {
  return a->range_dbid == b->range_dbid && a->range_offset == b->range_offset &&
         a->range_high == b->range_low;
}

/* Repack a range record.
 *
 *  Check whether this range record has become adjacent to
 *  one of its neighbors, and flow them together if possible.
 */
void graph_grmap_range_repack(graph_grmap *grm, graph_grmap_dbid_slot *dis,
                              size_t tab_i, size_t i) {
  cl_handle *const cl = grm->grm_graph->graph_cl;
  graph_grmap_table *tab;

tail_rec:
  cl_assert(cl, dis != NULL);
  cl_assert(cl, tab_i < dis->dis_n);

  cl_log(cl, CL_LEVEL_VERBOSE, "graph_grmap_range_repack(tab_i=%zu, i=%zu)",
         tab_i, i);
  tab = dis->dis_table[tab_i].ts_table;
  cl_assert(cl, i < tab->tab_n);

  /*  Update the cached table low.
   */
  dis->dis_table[tab_i].ts_low = tab->tab_data->range_low;

  while (i + 1 < tab->tab_n &&
         graph_grmap_range_adjacent(tab->tab_data + i, tab->tab_data + i + 1)) {
    cl_cover(cl);
    tab->tab_data[i].range_high = tab->tab_data[i + 1].range_high;
    graph_grmap_range_delete(grm, tab, i + 1);
  }

  while (i > 0 &&
         graph_grmap_range_adjacent(tab->tab_data + i - 1, tab->tab_data + i)) {
    cl_cover(cl);
    tab->tab_data[i - 1].range_high += tab->tab_data[i].range_high;
    graph_grmap_range_delete(grm, tab, i);

    i--;
  }

  /*  Merge with the last element of the previous table.
   */
  if (i == 0 && tab_i > 0 &&
      graph_grmap_range_adjacent(dis->dis_table[tab_i - 1].ts_table->tab_data +
                                     dis->dis_table[tab_i - 1].ts_table->tab_n -
                                     1,
                                 tab->tab_data)) {
    graph_grmap_range *pred = dis->dis_table[tab_i - 1].ts_table->tab_data +
                              dis->dis_table[tab_i - 1].ts_table->tab_n - 1;

    /*  Expand the predecessor.
     */
    pred->range_high = tab->tab_data->range_high;

    /*  Delete the obsolete range.
     */
    graph_grmap_range_delete(grm, tab, 0);

    /* If this was the last range in the table,
     * delete the table.
     */
    if (tab->tab_n == 0) {
      cl_cover(cl);
      graph_grmap_table_delete(grm, dis, tab_i);
    }

    /* Rebalance the merge destination.
     */
    tab = dis->dis_table[--tab_i].ts_table;
    i = tab->tab_n - 1;

    goto tail_rec;
  }

  /*  Merge with the first element of the next table.
   */
  if (i == tab->tab_n - 1 && tab_i + 1 < dis->dis_n &&
      graph_grmap_range_adjacent(
          tab->tab_data + tab->tab_n - 1,
          dis->dis_table[tab_i + 1].ts_table->tab_data)) {
    cl_cover(cl);

    dis->dis_table[tab_i + 1].ts_low =
        dis->dis_table[tab_i + 1].ts_table->tab_data->range_low =
            tab->tab_data[tab->tab_n - 1].range_low;
    graph_grmap_range_delete(grm, tab, tab->tab_n - 1);

    /* If this was the last range in the table,
     * delete the table.
     */
    if (tab->tab_n == 0) {
      cl_cover(cl);
      graph_grmap_table_delete(grm, dis, tab_i);
    } else {
      cl_cover(cl);
      tab_i++;
    }

    /* Rebalance the merge destination.
     */
    i = 0;
    goto tail_rec;
  }
}

/* Insert a record into a grmap table at a given location.
 *  This is a low-level utility; it's up to the caller to rebalance.
 */
int graph_grmap_range_insert(graph_grmap *grm, graph_grmap_table *tab, size_t i,
                             unsigned long long low, unsigned long long high,
                             unsigned long long dbid, long long offset) {
  graph_grmap_range *range;

  cl_assert(grm->grm_graph->graph_cl, tab != NULL);
  cl_log(grm->grm_graph->graph_cl, CL_LEVEL_VERBOSE,
         "graph_grmap_range_insert %llu..%llu tab %p, slot %zu", low, high,
         (void *)tab, i);

  if (tab->tab_n >= sizeof(tab->tab_data) / sizeof(*tab->tab_data)) {
    cl_cover(grm->grm_graph->graph_cl);
    return E2BIG;
  }

  cl_assert(grm->grm_graph->graph_cl, low < high);

  if (i < tab->tab_n) {
    cl_cover(grm->grm_graph->graph_cl);
    memmove(tab->tab_data + i + 1, tab->tab_data + i,
            (tab->tab_n - i) * sizeof(*tab->tab_data));
  } else {
    cl_cover(grm->grm_graph->graph_cl);
  }

  tab->tab_n++;
  range = tab->tab_data + i;

  range->range_low = low;
  range->range_high = high;
  range->range_dbid = dbid;
  range->range_offset = offset;

  return 0;
}

/* @brief  Look up the record that a given local ID is inside of.
 *
 * The ID may not have a corresponding slot at all.
 * If there was a match, the location is stored in *loc_out.
 * If there was no match, *loc_out is either the place where
 * the new record should inserted, or the record that should
 * be extended to contain the index.
 *
 * @param grm		overall map
 * @param tab		specific table we're looking in
 * @param i 		id we're looking for
 * @param loc_out	where to look/insert if necessary
 *
 * @return GRAPH_ERR_NO if there exists no mapping,
 * @return 0 if there exists a mapping, and *loc_out is its offset.
 */
bool graph_grmap_range_lookup(graph_grmap const *grm,
                              graph_grmap_table const *tab,
                              unsigned long long i, size_t *loc_out) {
  cl_handle *cl = grm->grm_graph->graph_cl;
  unsigned short hi, lo, med;
  graph_grmap_range const *range;

  lo = 0;
  hi = tab->tab_n;

  while (lo + 1 < hi) {
    graph_grmap_range const *range;

    med = lo + (hi - lo) / 2;
    cl_assert(cl, lo < med);
    cl_assert(cl, med < hi);

    range = tab->tab_data + med;
    if (i < range->range_low) {
      cl_cover(cl);
      hi = med;
    } else if (i >= range->range_high) {
      cl_cover(cl);
      lo = med + 1;
    } else {
      cl_cover(cl);
      *loc_out = med;
      return true;
    }
  }

  /*  The entry either belongs to this record,
   *  or belongs into the gap before or after this
   *  record.
   */
  if ((*loc_out = lo) >= tab->tab_n) {
    cl_cover(cl);
    return false;
  }

  range = tab->tab_data + lo;
  if (i < range->range_low) {
    if (lo > 0 && i <= range[-1].range_high) {
      cl_cover(cl);
      --*loc_out;
    } else {
      cl_cover(cl);
    }
  } else if (i >= range->range_high) {
    if (lo + 1 < tab->tab_n && i + 1 >= range[1].range_low) {
      cl_cover(cl);
      ++*loc_out;
    } else {
      cl_cover(cl);
    }
  } else {
    cl_cover(cl);
    return true;
  }
  return false;
}

/* @brief  Look up the record that a given local ID is inside of.
 *
 * The ID may not have a corresponding slot at all.
 * If there was a match, the location is stored in *loc_out.
 * If there was no match, *loc_out is either the place where
 * the new record should inserted, or the record that should
 * be extended to contain the index.
 *
 * @param grm		overall map
 * @param tab		specific table we're looking in
 * @param i 		id we're looking for
 * @param loc_out	where to look/insert if necessary
 *
 * @return GRAPH_ERR_NO if there exists no mapping,
 * @return 0 if there exists a mapping, and *loc_out is its offset.
 */
bool graph_grmap_range_next_overlap(graph_grmap const *grm,
                                    graph_grmap_table const *tab,
                                    unsigned long long *lo,
                                    unsigned long long hi,

                                    bool *found_out, unsigned long long *lo_out,
                                    unsigned long long *hi_out,
                                    size_t *loc_out) {
  cl_handle *cl = grm->grm_graph->graph_cl;
  graph_grmap_range const *range;

  /*  Out of stuff to look for.
   */
  if (*lo >= hi) {
    cl_cover(cl);
    return false;
  }

  if (!(*found_out = graph_grmap_range_lookup(grm, tab, *lo, loc_out)) &&
      *loc_out >= tab->tab_n) {
    cl_cover(cl);
    *hi_out = *lo = hi;
    return true;
  }

  range = tab->tab_data + *loc_out;
  if (!*found_out) {
    /* Wasn't found.  Return a non-overlap. */

    if (*lo >= range->range_high) {
      if (*loc_out + 1 >= tab->tab_n) {
        cl_cover(cl);
        *hi_out = *lo = hi;
        return true;
      } else {
        cl_cover(cl);
      }

      ++tab;
      ++*loc_out;
    } else {
      cl_cover(cl);
      cl_assert(cl, *lo < range->range_low);
    }
    *hi_out = *lo = (hi <= range->range_low ? hi : range->range_low);
  } else {
    cl_cover(cl);
    *hi_out = *lo = (hi <= range->range_high ? hi : range->range_high);
  }
  return true;
}
