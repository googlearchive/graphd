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
#define tab_high(tab) ((tab)->tab_data[(tab)->tab_n - 1].range_high)

#define tab_size(grm)          \
  (sizeof(graph_grmap_table) + \
   (sizeof(graph_grmap_range) * ((grm)->grm_table_size - 1)))

/* @brief  Look up the table that a given local ID is inside of.
 *
 *  The ID may not have a corresponding slot at all.
 *
 *  If there was a match, the location is stored in *loc_out.
 *  If there was no match, *loc_out is either the place where
 *  the new record should inserted, or the record that should
 *  be extended to contain the index.
 *
 * @param grm		overall map
 * @param tab		specific table we're looking in
 * @param i 		id we're looking for
 * @param loc_out	where to look/insert if necessary
 *
 * @return true if there's a mapping, and *loc_out is its offset;
 * @return false if there's no mapping, and *loc_out is where it would be
 *	(or that location's neighbor.)
 */
bool graph_grmap_table_lookup(graph_grmap const* grm,
                              graph_grmap_dbid_slot const* dis,
                              unsigned long long i, size_t* loc_out) {
  cl_handle* cl = grm->grm_graph->graph_cl;
  unsigned short hi, lo, med;
  graph_grmap_table_slot const* ts;
  graph_grmap_table const *tab, *tab_neighbor;

  lo = 0;
  hi = dis->dis_n;

  while (lo + 1 < hi) {
    graph_grmap_table_slot const* ts;

    med = lo + (hi - lo) / 2;
    cl_assert(cl, lo < med);
    cl_assert(cl, med < hi);

    ts = dis->dis_table + med;

    /* Too low */
    if (i < ts->ts_low) {
      cl_cover(grm->grm_graph->graph_cl);
      hi = med;
    }

    /* Too high */
    else if (med + 1 < dis->dis_n && i >= ts[1].ts_low) {
      cl_cover(grm->grm_graph->graph_cl);
      lo = med + 1;
    }

    else if (i >= tab_high(ts->ts_table)) {
      /*  This is the right spot, but
       *  we're not actually contained in it.
       */
      cl_cover(grm->grm_graph->graph_cl);
      lo = med;
      break;
    }

    /* Just right */
    else {
      cl_cover(grm->grm_graph->graph_cl);
      *loc_out = med;
      return true;
    }
  }

  /*  The entry either belongs to this record,
   *  or belongs into the gap before or after this
   *  record.
   */
  if ((*loc_out = lo) >= dis->dis_n) {
    cl_cover(grm->grm_graph->graph_cl);
    return false;
  }

  ts = dis->dis_table + lo;
  tab = ts->ts_table;
  if (i < ts->ts_low) {
    if (lo > 0) {
      tab_neighbor = dis->dis_table[lo - 1].ts_table;
      cl_cover(grm->grm_graph->graph_cl);

      if (i <= tab_high(tab_neighbor)) {
        cl_cover(grm->grm_graph->graph_cl);
        --*loc_out;
      } else {
        cl_cover(grm->grm_graph->graph_cl);
      }
    }
  } else if (i >= tab_high(tab)) {
    if (lo + 1 < dis->dis_n) {
      tab_neighbor = dis->dis_table[lo + 1].ts_table;
      if (i + 1 >= tab_low(tab_neighbor)) {
        cl_cover(grm->grm_graph->graph_cl);
        ++*loc_out;
      } else {
        cl_cover(grm->grm_graph->graph_cl);
      }
    }
  } else {
    cl_cover(grm->grm_graph->graph_cl);
    return true;
  }
  cl_cover(grm->grm_graph->graph_cl);
  return false;
}

/* @brief  Return the first table that overlaps with the given range.
 *
 * @param grm		overall map
 * @param tab		specific table we're looking in
 * @param i 		id we're looking for
 * @param loc_out	where to look/insert if necessary
 *
 * @return true while there was still overlapping data.
 */
bool graph_grmap_table_next_overlap(graph_grmap const* grm,
                                    graph_grmap_dbid_slot const* dis,

                                    unsigned long long* lo,
                                    unsigned long long hi,

                                    bool* found_out, unsigned long long* lo_out,
                                    unsigned long long* hi_out,
                                    size_t* loc_out) {
  graph_grmap_table const* tab;
  cl_handle* const cl = grm->grm_graph->graph_cl;

  *lo_out = *lo;

  /*  Out of stuff to look for.
   */
  if (*lo >= hi) {
    cl_cover(grm->grm_graph->graph_cl);
    return false;
  }

  if (!(*found_out = graph_grmap_table_lookup(grm, dis, *lo, loc_out)) &&
      *loc_out >= dis->dis_n) {
    cl_cover(grm->grm_graph->graph_cl);
    *hi_out = *lo = hi;
    return true;
  }

  tab = dis->dis_table[*loc_out].ts_table;

  if (!*found_out) {
    /* Wasn't found.  Return a non-overlap. */

    if (*lo >= tab_high(tab)) {
      if (*loc_out + 1 >= dis->dis_n) {
        *hi_out = *lo = hi;
        cl_cover(grm->grm_graph->graph_cl);
        return true;
      }
      cl_cover(grm->grm_graph->graph_cl);

      ++tab;
      ++*loc_out;
    } else {
      cl_cover(grm->grm_graph->graph_cl);
      cl_assert(cl, *lo < tab_low(tab));
    }
    *hi_out = *lo = (hi <= tab_low(tab) ? hi : tab_low(tab));
  } else {
    cl_cover(grm->grm_graph->graph_cl);
    *hi_out = *lo = (hi <= tab_high(tab) ? hi : tab_high(tab));
  }
  return true;
}

/* @brief  Delete a table.
 *
 *  The ID may not have a corresponding slot at all.
 *
 *  If there was a match, the location is stored in *loc_out.
 *  If there was no match, *loc_out is either the place where
 *  the new record should inserted, or the record that should
 *  be extended to contain the index.
 *
 * @param grm		overall map
 * @param tab		specific table we're looking in
 * @param i 		id we're looking for
 * @param loc_out	where to look/insert if necessary
 *
 * @return GRAPH_ERR_NO if there exists no mapping,
 * @return 0 if there exists a mapping, and *loc_out is its offset.
 */
void graph_grmap_table_delete(graph_grmap* grm, graph_grmap_dbid_slot* dis,
                              size_t i) {
  cl_assert(grm->grm_graph->graph_cl, dis != NULL);
  cl_assert(grm->grm_graph->graph_cl, i < dis->dis_n);

  cm_free(grm->grm_graph->graph_cm, dis->dis_table[i].ts_table);
  if (i < dis->dis_n) {
    cl_cover(grm->grm_graph->graph_cl);
    memmove(dis->dis_table + i, dis->dis_table + i + 1,
            (dis->dis_n - (i + 1)) * sizeof(*dis->dis_table));
  } else {
    cl_cover(grm->grm_graph->graph_cl);
  }
  dis->dis_n--;
}

/* @brief  Allocate a range table, and add it to the list at a given index.
 *
 *  The ID may not have a corresponding slot at all.
 *
 *  If there was a match, the location is stored in *loc_out.
 *  If there was no match, *loc_out is either the place where
 *  the new record should inserted, or the record that should
 *  be extended to contain the index.
 *
 * @param grm		overall map
 * @param tab		specific table we're looking in
 * @param i 		id we're looking for
 * @param loc_out	where to look/insert if necessary
 *
 * @return GRAPH_ERR_NO if there exists no mapping,
 * @return 0 if there exists a mapping, and *loc_out is its offset.
 */
int graph_grmap_table_insert(graph_grmap* grm, graph_grmap_dbid_slot* dis,
                             size_t i, unsigned long long low) {
  graph_grmap_table_slot* ts;
  graph_grmap_table* tab;

  cl_assert(grm->grm_graph->graph_cl, dis != NULL);
  cl_assert(grm->grm_graph->graph_cl, i <= dis->dis_n);

  tab = cm_malloc(grm->grm_graph->graph_cm, tab_size(grm));
  if (tab == NULL) {
    int err = errno ? errno : ENOMEM;
    cl_log_errno(grm->grm_graph->graph_cl, CL_LEVEL_FAIL,
                 "graph_grmap_table_alloc", err,
                 "can't allocate range table for low=%llu", low);
    return err;
  }
  tab->tab_n = 0;

  if (dis->dis_n >= dis->dis_m) {
    graph_grmap_table_slot* tmp;

    tmp = cm_realloc(grm->grm_graph->graph_cm, dis->dis_table,
                     (dis->dis_n + 1024) * sizeof(*dis->dis_table));
    if (tmp == NULL) {
      int err = errno ? errno : ENOMEM;
      free(tab);
      cl_log_errno(grm->grm_graph->graph_cl, CL_LEVEL_FAIL, "cm_realloc", err,
                   "can't grow table slots to %zu for "
                   "low=%llu",
                   dis->dis_n + 1024, low);
      return err;
    }
    dis->dis_table = tmp;
    dis->dis_m += 1024;
    cl_cover(grm->grm_graph->graph_cl);
  }

  if (i < dis->dis_n) {
    cl_cover(grm->grm_graph->graph_cl);
    memmove(dis->dis_table + i + 1, dis->dis_table + i,
            (dis->dis_n - i) * sizeof(*dis->dis_table));
  } else {
    cl_cover(grm->grm_graph->graph_cl);
  }

  dis->dis_n++;
  ts = dis->dis_table + i;

  ts->ts_table = tab;
  ts->ts_low = low;

  return 0;
}

/* @brief  Split a range table.
 *
 * @param grm		overall map
 * @param tab		specific table we're looking in
 * @param i 		id we're looking for
 * @param loc_out	where to look/insert if necessary
 *
 * @return GRAPH_ERR_NO if there exists no mapping,
 * @return 0 if there exists a mapping, and *loc_out is its offset.
 */
int graph_grmap_table_split(graph_grmap* grm, graph_grmap_dbid_slot* dis,
                            size_t i) {
  graph_grmap_table *ntab, *otab;
  size_t split;
  int err;

  otab = dis->dis_table[i].ts_table;

  split = otab->tab_n / 2;
  err = graph_grmap_table_insert(grm, dis, i + 1,
                                 otab->tab_data[split].range_low);
  if (err != 0) return err;

  ntab = dis->dis_table[i + 1].ts_table;
  ntab->tab_n = otab->tab_n - split;

  memcpy(ntab->tab_data, otab->tab_data + (otab->tab_n = split),
         ntab->tab_n * sizeof(*ntab->tab_data));
  cl_cover(grm->grm_graph->graph_cl);

  return 0;
}
