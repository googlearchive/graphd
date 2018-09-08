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

/* grmap - Guid Range Map
 *
 * A data structure for mapping ranges of GUIDs to each other.
 */

#define tab_low(tab) ((tab)->tab_data->range_low)
#define tab_high(tab) ((tab)->tab_data[(tab)->tab_n - 1].range_high)

#define graph_grmap_dbid_invariant(a, b) \
  graph_grmap_dbid_invariant_loc(a, b, __FILE__, __LINE__)

static void graph_grmap_table_invariant(graph_grmap const* grm,
                                        graph_grmap_table const* tab) {
  cl_handle* const cl = grm->grm_graph->graph_cl;
  graph_grmap_range const* r;
  size_t i;

  cl_assert(cl, tab->tab_n > 0);

  for (i = 0, r = tab->tab_data; i < tab->tab_n; i++, r++) {
    cl_assert(cl, r->range_low < r->range_high);
    if (i + 1 < tab->tab_n) {
      cl_cover(grm->grm_graph->graph_cl);
      cl_assert(cl, r->range_high <= r[1].range_low);
      cl_assert(cl, !graph_grmap_range_adjacent(r, r + 1));
    } else
      cl_cover(grm->grm_graph->graph_cl);
  }
}

static void graph_grmap_dbid_invariant_loc(graph_grmap const* grm,
                                           graph_grmap_dbid_slot const* dis,
                                           char const* file, int line) {
  cl_handle* const cl = grm->grm_graph->graph_cl;
  size_t i;

  cl_assert_loc(cl, dis->dis_n > 0, file, line);

  for (i = 0; i < dis->dis_n; i++) {
    graph_grmap_table const* const tab = dis->dis_table[i].ts_table;

    cl_assert_loc(cl, tab != NULL, file, line);
    cl_assert_loc(cl, tab->tab_n > 0, file, line);

    cl_assert_loc(cl, tab->tab_data->range_low == dis->dis_table[i].ts_low,
                  file, line);
    graph_grmap_table_invariant(grm, tab);

    if (i + 1 < dis->dis_n) {
      graph_grmap_table const* const t2 = dis->dis_table[i + 1].ts_table;

      cl_assert_loc(cl, tab_high(tab) <= tab_low(t2), file, line);
      cl_assert_loc(cl, !graph_grmap_range_adjacent(
                            tab->tab_data + (tab->tab_n - 1), t2->tab_data),
                    file, line);
      cl_cover(grm->grm_graph->graph_cl);
    }
  }
}

void graph_grmap_invariant_loc(graph_grmap const* const grm, char const* file,
                               int line) {
  size_t i;
  for (i = 0; i < grm->grm_n; i++) {
    cl_cover(grm->grm_graph->graph_cl);
    graph_grmap_dbid_invariant_loc(grm, grm->grm_dbid + i, file, line);
  }
}

graph_grmap_dbid_slot* graph_grmap_dbid_lookup_id(graph_grmap const* grm,
                                                  unsigned long long dbid) {
  graph_grmap_dbid_slot const* dis = grm->grm_dbid + grm->grm_n;

  while (dis > grm->grm_dbid)
    if ((--dis)->dis_dbid == dbid) {
      cl_cover(grm->grm_graph->graph_cl);
      return (graph_grmap_dbid_slot*)dis;
    } else {
      cl_cover(grm->grm_graph->graph_cl);
    }
  return NULL;
}

/**
 * @brief Given a GUID, look up the grmap dbid structure for its database ID.
 *
 *  This is a linear lookup.  We expect this data structure to
 *  have two, maybe three entries.
 *
 * @param grmap	the overall mapping system handle
 * @param guid the GUID we're looking up.
 *
 * @return NULL if the dbid doesn't have an entry (yet),
 * @return a pointer to the dbid's per-database-ID table.
 *	The pointer does not survive additions to the database -
 *  	if a new DBID is created, the array that contains
 * 	it may be reallocated.
 */
graph_grmap_dbid_slot* graph_grmap_dbid_lookup(graph_grmap const* grm,
                                               graph_guid const* guid) {
  return graph_grmap_dbid_lookup_id(grm, GRAPH_GUID_DB(*guid));
}

/**
 * @brief Given a GUID, create a new grmap dbid structure
 * 	for its database ID.
 *
 *  The caller must have previously checked that the
 *  DBID doesn't already exist.
 *
 * @param grmap	the overall mapping system handle
 * @param guid the GUID we're creating or looking up.
 *
 * @return NULL on allocation error.
 * @return a pointer to the dbid's newly created per-database-ID table.
 *	The pointer does not survive additions to the database -
 *  	if a new DBID is created, the array that contains
 * 	it may be reallocated.
 */
static graph_grmap_dbid_slot* graph_grmap_dbid_new(graph_grmap* grm,
                                                   graph_guid const* guid) {
  cm_handle* const cm = grm->grm_graph->graph_cm;
  size_t need;
  graph_grmap_dbid_slot* dis;

  cl_assert(grm->grm_graph->graph_cl,
            graph_grmap_dbid_lookup(grm, guid) == NULL);

  if (grm->grm_n >= grm->grm_m) {
    /*  I don't really expect us to outgrow the in-core buffer.
     *  The code is written to cope with it, but just in case
     *  it's due to a programmer error, make a note in the
     *  debug stream.
     */
    if (grm->grm_n ==
        sizeof(grm->grm_dbid_buf) / sizeof(grm->grm_dbid_buf[0])) {
      cl_cover(grm->grm_graph->graph_cl);
      cl_log(grm->grm_graph->graph_cl, CL_LEVEL_DEBUG,
             "graph_grmap_dbid_new: more than %zu different "
             "database IDs.",
             sizeof(grm->grm_dbid_buf) / sizeof(grm->grm_dbid_buf[0]));
    }

    need = grm->grm_m + 16;
    if (grm->grm_dbid != grm->grm_dbid_buf) {
      cl_cover(grm->grm_graph->graph_cl);
      dis = cm_realloc(cm, grm->grm_dbid, sizeof(*dis) * need);
    } else {
      cl_cover(grm->grm_graph->graph_cl);
      dis = cm_malloc(cm, sizeof(*dis) * need);
      if (dis != NULL) {
        cl_cover(grm->grm_graph->graph_cl);
        memcpy(dis, grm->grm_dbid, grm->grm_m * sizeof(*dis));
      }
    }
    if (dis == NULL) return NULL;

    grm->grm_dbid = dis;
    grm->grm_m = need;
  }
  dis = grm->grm_dbid + grm->grm_n++;

  dis->dis_dbid = GRAPH_GUID_DB(*guid);
  dis->dis_table = NULL;
  dis->dis_n = dis->dis_m = 0;
  cl_cover(grm->grm_graph->graph_cl);

  return dis;
}

/**
 * @brief Translate a GUID according to a GUID range map.
 *
 * @param grm 	map to use
 * @param source	map this guid
 * @param destination	store the result here
 *
 * @return 0 on success
 * @return a nonzero error code on error.
 */
int graph_grmap_map(graph_grmap const* grm, graph_guid const* source,
                    graph_guid* destination) {
  cl_handle* const cl = grm->grm_graph->graph_cl;

  graph_grmap_dbid_slot const* dis;
  graph_grmap_range const* range;
  unsigned long long source_id;
  size_t tab_i;
  size_t range_i;
  char buf[200];

  /*  Which database is this in?
   */
  dis = graph_grmap_dbid_lookup(grm, source);
  if (dis == NULL) {
    cl_cover(grm->grm_graph->graph_cl);
    cl_log(cl, CL_LEVEL_VERBOSE, "graph_grmap_map: %s: unknown database ID",
           graph_guid_to_string(source, buf, sizeof buf));
    goto fail;
  }

  if (cl_is_logged(cl, CL_LEVEL_DEBUG)) graph_grmap_dbid_invariant(grm, dis);

  /* The ID within that database.
   */
  source_id = GRAPH_GUID_SERIAL(*source);

  /*  Which table within that database slot is this in?
   */
  if (!graph_grmap_table_lookup(grm, dis, source_id, &tab_i)) {
    cl_cover(grm->grm_graph->graph_cl);
    cl_log(cl, CL_LEVEL_VERBOSE, "graph_grmap_map: %s: no table",
           graph_guid_to_string(source, buf, sizeof buf));
    goto fail;
  }

  /*  Where is it within that table?
   */
  if (!graph_grmap_range_lookup(grm, dis->dis_table[tab_i].ts_table, source_id,
                                &range_i)) {
    cl_cover(grm->grm_graph->graph_cl);
    cl_log(cl, CL_LEVEL_VERBOSE, "graph_grmap_map: %s: no range",
           graph_guid_to_string(source, buf, sizeof buf));
    goto fail;
  }

  /*  This is our slot.
   */
  range = dis->dis_table[tab_i].ts_table->tab_data + range_i;

  /*  Map to the destination database, and add the offset.
   */
  graph_guid_from_db_serial(destination, range->range_dbid,
                            source_id + range->range_offset);
  cl_cover(grm->grm_graph->graph_cl);
  return 0;

fail:
  GRAPH_GUID_MAKE_NULL(*destination);
  return GRAPH_ERR_NO;
}

/**
 * @brief Utility: add a new range to an existing table.
 *
 * @param grm		overall context
 * @param dis		database ID we're workign in
 * @param tab_i		table to add to.
 * @param range_i	slot in the table that we're adding at
 * @param lolow		low end of our range
 * @param high		high end of our range
 * @param dbid		destination dbid of the mapping
 * @param offset	shift applied to incoming GUIDs.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int graph_grmap_add_nonexistent_range(
    graph_grmap* grm, graph_grmap_dbid_slot* dis, size_t tab_i, size_t range_i,
    unsigned long long low, unsigned long long high, unsigned long long dbid,
    long long offset) {
  graph_grmap_table *tab1, *tab2;
  graph_grmap_range *range1, *range2;
  size_t tab_i1, tab_i2;
  cl_handle* cl = grm->grm_graph->graph_cl;
  int err;

  /*  If the database itself didn't exist,
   *  the caller created it for us.
   */
  cl_assert(cl, dis != NULL);
  cl_assert(cl, low < high);

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graph_grmap_add_nonexistent_range %llx..%llx dbid=%llx offset=%lld",
         low, high, dbid, offset);

  /*  Empty dbid table?
   */
  if (dis->dis_n == 0) {
    graph_grmap_table_slot* ts;

    /* Insert a new table.
     */
    err = graph_grmap_table_insert(grm, dis, 0, low);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graph_grmap_table_insert", err,
                   "%llu...%llu in [%zu][%zu]", low, high, tab_i, range_i);
      return err;
    }

    /* Insert a slot into the new table.
     */
    err = graph_grmap_range_insert(grm, dis->dis_table[0].ts_table, 0, low,
                                   high, dbid, offset);
    if (err == 0) {
      /* Update the cached table slot low.
       */
      ts = dis->dis_table;
      ts->ts_low = ts->ts_table->tab_data->range_low;

      if (cl_is_logged(cl, CL_LEVEL_DEBUG))
        graph_grmap_dbid_invariant(grm, dis);
      cl_cover(grm->grm_graph->graph_cl);
    } else {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graph_grmap_range_insert", err,
                   "%llu...%llu in [%zu][%zu]", low, high, tab_i, range_i);
    }
    return err;
  }

  cl_assert(cl, tab_i < dis->dis_n);

  /*  Most common case: extending the range at the end.
   */
  tab1 = dis->dis_table[tab_i].ts_table;
  if (low == tab_high(tab1)) {
    range1 = tab1->tab_data + tab1->tab_n - 1;

    if (range1->range_dbid == dbid && range1->range_offset == offset &&
        range1->range_high == low) {
      range1->range_high = high;

      cl_cover(grm->grm_graph->graph_cl);
      graph_grmap_range_repack(grm, dis, tab_i, tab1->tab_n - 1);
      if (cl_is_logged(cl, CL_LEVEL_DEBUG))
        graph_grmap_dbid_invariant(grm, dis);
      return 0;
    }
  }

  if (low >= tab_high(tab1)) {
    /*  If there's still space in this table, add a range
     *  at the end.
     */
    if (tab1->tab_n < grm->grm_table_size) {
      cl_assert(cl, tab1->tab_n > 0);

      /*  Does it append to its predecessor?  Maybe
       *  we can just grow the range.
       */
      err = graph_grmap_range_insert(grm, tab1, tab1->tab_n, low, high, dbid,
                                     offset);
      if (err == 0) {
        graph_grmap_range_repack(grm, dis, tab_i, tab1->tab_n - 1);
        if (cl_is_logged(cl, CL_LEVEL_DEBUG))
          graph_grmap_dbid_invariant(grm, dis);
        cl_cover(grm->grm_graph->graph_cl);
        return 0;
      }
      if (err != E2BIG) {
        cl_log_errno(grm->grm_graph->graph_cl, CL_LEVEL_FAIL,
                     "graph_grmap_range_insert", err,
                     "%llu...%llu in [%zu][%zu]", low, high, tab_i, range_i);
        return err;
      }
    }

    /*  Append a new table.
     */
    err = graph_grmap_table_insert(grm, dis, tab_i + 1, low);
    if (err != 0) return err;

    /* Insert a slot into the new table.
     */
    err = graph_grmap_range_insert(grm, dis->dis_table[tab_i + 1].ts_table, 0,
                                   low, high, dbid, offset);
    if (err != 0) return err;

    graph_grmap_range_repack(grm, dis, tab_i + 1, 0);

    if (cl_is_logged(cl, CL_LEVEL_DEBUG)) graph_grmap_dbid_invariant(grm, dis);
    cl_cover(grm->grm_graph->graph_cl);

    return 0;
  }

  /*  Insertion.
   */
  cl_assert(cl, tab_i < dis->dis_n);
  if (low < dis->dis_table[tab_i].ts_low) {
    /*  This one or its predecessor.
     */
    tab_i2 = tab_i;
    tab2 = dis->dis_table[tab_i].ts_table;
    range2 = tab2->tab_data + range_i;

    if (tab_i == 0) {
      tab1 = NULL;
      range1 = NULL;
      tab_i1 = 0;
      cl_cover(grm->grm_graph->graph_cl);
    } else {
      tab_i1 = tab_i - 1;
      tab1 = dis->dis_table[tab_i1].ts_table;
      range1 = tab1->tab_data + (tab1->tab_n - 1);
      cl_cover(grm->grm_graph->graph_cl);
    }
  } else if (low >= tab_high(dis->dis_table[tab_i].ts_table)) {
    tab_i1 = tab_i;
    tab1 = dis->dis_table[tab_i].ts_table;
    range1 = tab1->tab_data + (tab1->tab_n - 1);

    /*  This one or its successor.
     */
    if (tab_i + 1 >= dis->dis_n) {
      tab_i2 = tab_i;
      tab2 = NULL;
      range2 = NULL;
      cl_cover(grm->grm_graph->graph_cl);
    } else {
      tab_i2 = tab_i + 1;
      tab2 = dis->dis_table[tab_i2].ts_table;
      range2 = tab2->tab_data;
      cl_cover(grm->grm_graph->graph_cl);
    }
  } else {
    /*  Both candidates are in the same table.
     */
    tab_i1 = tab_i2 = tab_i;
    tab1 = tab2 = dis->dis_table[tab_i].ts_table;

    range1 = tab1->tab_data + range_i;
    if (range1->range_low >= high) {
      range2 = range1--;
      cl_assert(cl, range_i > 0);
      cl_cover(grm->grm_graph->graph_cl);
    } else {
      cl_assert(cl, range1->range_high <= low);
      cl_assert(cl, range_i + 1 < tab1->tab_n);
      range2 = NULL;
      tab2 = NULL;
      cl_cover(grm->grm_graph->graph_cl);
    }
  }

  /*  Does it expand the first one?
   */
  if (range1 != NULL && range1->range_high == low &&
      range1->range_dbid == dbid && range1->range_offset == offset) {
    range1->range_high = high;
    graph_grmap_range_repack(grm, dis, tab_i1, range1 - tab1->tab_data);
    if (cl_is_logged(cl, CL_LEVEL_DEBUG)) graph_grmap_dbid_invariant(grm, dis);
    cl_cover(grm->grm_graph->graph_cl);

    return 0;
  }

  /*  Does it expand the second one?
   */
  if (range2 != NULL && range2->range_low == high &&
      range2->range_dbid == dbid && range2->range_offset == offset) {
    range2->range_low -= high - low;

    graph_grmap_range_repack(grm, dis, tab_i2, range2 - tab2->tab_data);
    if (cl_is_logged(cl, CL_LEVEL_DEBUG)) graph_grmap_dbid_invariant(grm, dis);
    cl_cover(grm->grm_graph->graph_cl);

    return 0;
  }

  cl_assert(cl, range1 || range2);

  /*  Insert after the first one, if that exists and
   *  has space; otherwise before the second one.
   */
  if (range1 != NULL) {
    err = graph_grmap_range_insert(grm, tab1, 1 + (range1 - tab1->tab_data),
                                   low, high, dbid, offset);
    if (err == 0) {
      cl_cover(grm->grm_graph->graph_cl);
      graph_grmap_range_repack(grm, dis, tab_i1, 1 + (range1 - tab1->tab_data));
      if (cl_is_logged(cl, CL_LEVEL_DEBUG))
        graph_grmap_dbid_invariant(grm, dis);
      return 0;
    }
    if (err != E2BIG) return err;
    cl_cover(grm->grm_graph->graph_cl);
  }
  if (range2 != NULL) {
    err = graph_grmap_range_insert(grm, tab2, range2 - tab2->tab_data, low,
                                   high, dbid, offset);
    if (err == 0) {
      graph_grmap_range_repack(grm, dis, tab_i2, range2 - tab2->tab_data);

      if (cl_is_logged(cl, CL_LEVEL_DEBUG))
        graph_grmap_dbid_invariant(grm, dis);
      cl_cover(grm->grm_graph->graph_cl);

      return 0;
    }
    if (err != E2BIG) return err;
  }

  /*  Split the table at our location, and adjust
   *  the location to the new size.
   */
  err = graph_grmap_table_split(grm, dis, tab_i);
  if (err != 0) return err;

  tab1 = dis->dis_table[tab_i].ts_table;
  if (range_i >= tab1->tab_n) {
    cl_cover(grm->grm_graph->graph_cl);
    range_i -= tab1->tab_n;
    tab_i++;
  }

  err = graph_grmap_range_insert(grm, tab1, range_i, low, high, dbid, offset);
  cl_assert(cl, err != E2BIG);
  if (err == 0) {
    cl_cover(grm->grm_graph->graph_cl);
    graph_grmap_range_repack(grm, dis, tab_i2, range2 - tab2->tab_data);
    if (cl_is_logged(cl, CL_LEVEL_DEBUG)) graph_grmap_dbid_invariant(grm, dis);
  }
  return err;
}

/**
 * @brief Add a range to a GRMAP.
 *
 *  	The addition fails if the mapping conflicts with any
 *  	existing mappings.  Overlaps are okay, as long as the
 *	results are the same.
 *
 * @param grm 	map to add to
 * @param source	map guids starting here
 * @param destination	map to guids starting here
 * @param n		map this many.
 *
 * @return 0 on success
 * @return a nonzero error code on allocation error.
 */
int graph_grmap_add_range(graph_grmap* grm, graph_guid const* source,
                          graph_guid const* destination, long long n) {
  cl_handle* const cl = grm->grm_graph->graph_cl;
  graph_grmap_dbid_slot* dis;
  int err = 0;
  bool lap_found, any_not_found = false;
  size_t lap_i;
  unsigned long long lap_lo, lap_hi, my_lo, my_hi;
  long long offset;
  char b1[200], b2[200];

  cl_log(cl, CL_LEVEL_VERBOSE, "graph_grmap_add_range %s->%s [%lld]",
         graph_guid_to_string(source, b1, sizeof b1),
         graph_guid_to_string(destination, b2, sizeof b2), n);

  my_lo = GRAPH_GUID_SERIAL(*source);
  my_hi = my_lo + n;
  offset = GRAPH_GUID_SERIAL(*destination) - GRAPH_GUID_SERIAL(*source);

  /*  Create or obtain the source database table.
   */
  if ((dis = graph_grmap_dbid_lookup(grm, source)) == NULL) {
    dis = graph_grmap_dbid_new(grm, source);
    if (dis == NULL) {
      char buf[GRAPH_GUID_SIZE];
      err = errno ? errno : ENOMEM;
      cl_log_errno(grm->grm_graph->graph_cl, CL_LEVEL_FAIL,
                   "graph_grmap_dbid_new", err, "can't allocate dbid for %s",
                   graph_guid_to_string(source, buf, sizeof buf));
      return err;
    }
    err = graph_grmap_add_nonexistent_range(
        grm, dis, 0, 0, my_lo, my_hi, GRAPH_GUID_DB(*destination), offset);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graph_grmap_add_nonexistent_range", err,
                   "lap_i=0, tab_i=0, %llx-%llx", (unsigned long long)my_lo,
                   (unsigned long long)my_hi);
      return err;
    }

    if (cl_is_logged(cl, CL_LEVEL_DEBUG)) graph_grmap_dbid_invariant(grm, dis);
    cl_cover(grm->grm_graph->graph_cl);

    return 0;
  }

  if (cl_is_logged(cl, CL_LEVEL_DEBUG)) graph_grmap_dbid_invariant(grm, dis);

  /*  Check for overlaps.
   */

  while (graph_grmap_table_next_overlap(grm, dis, &my_lo, my_hi, &lap_found,
                                        &lap_lo, &lap_hi, &lap_i)) {
    if (!lap_found) {
      any_not_found = true;
      cl_cover(grm->grm_graph->graph_cl);
    } else {
      graph_grmap_table const* const tab = dis->dis_table[lap_i].ts_table;

      unsigned long long tab_lo, tab_hi;
      size_t tab_i;
      bool tab_found;

      while (graph_grmap_range_next_overlap(
          grm, tab, &lap_lo, lap_hi, &tab_found, &tab_lo, &tab_hi, &tab_i)) {
        if (!tab_found) {
          any_not_found = true;
          cl_cover(grm->grm_graph->graph_cl);
        } else {
          graph_grmap_range const* const range = tab->tab_data + tab_i;

          if (range->range_offset != offset ||
              range->range_dbid != GRAPH_GUID_DB(*destination)) {
            cl_cover(grm->grm_graph->graph_cl);
            return GRAPH_ERR_RANGE_OVERLAP;
          } else {
            cl_cover(grm->grm_graph->graph_cl);
          }
        }
      }
    }
  }

  /*  OK, no conflicting overlaps.  Good.
   */

  /*  All the mappings we're trying to install existed already?
   */
  if (!any_not_found) {
    cl_cover(grm->grm_graph->graph_cl);
    if (cl_is_logged(cl, CL_LEVEL_DEBUG)) graph_grmap_invariant(grm);
    return 0;
  }

  /*  Let's do it all over again, this time creating
   *  nonexistent ranges as we go.
   */
  my_lo = GRAPH_GUID_SERIAL(*source);
  while (graph_grmap_table_next_overlap(grm, dis, &my_lo, my_hi, &lap_found,
                                        &lap_lo, &lap_hi, &lap_i)) {
    /*  There's an overlap with the range of a table.
     *  But there may still be gaps between slots
     *  within that table.
     */
    if (lap_found) {
      bool tab_found;

      unsigned long long tab_lo, tab_hi;
      size_t tab_i;

      while (graph_grmap_range_next_overlap(grm, dis->dis_table[lap_i].ts_table,
                                            &lap_lo, lap_hi, &tab_found,
                                            &tab_lo, &tab_hi, &tab_i)) {
        /*  In the check for overlap, we
         *  made sure that the overlaps that
         *  exist have the same range.
         */
        if (tab_found) {
          cl_cover(grm->grm_graph->graph_cl);
          continue;
        }

        cl_assert(cl, tab_lo < tab_hi);
        err = graph_grmap_add_nonexistent_range(
            grm, dis, lap_i, tab_i, tab_lo, tab_hi, GRAPH_GUID_DB(*destination),
            offset);
        cl_cover(grm->grm_graph->graph_cl);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "graph_grmap_add_nonexistent_range",
                       err, "lap_i=%zu, tab_i=%zu, %llx-%llx", lap_i, tab_i,
                       (unsigned long long)tab_lo, (unsigned long long)tab_hi);
          return err;
        }
      }
    } else {
      /*  The whole range we passed in does not overlap
       *  with any existing table.  That means it can
       *  be inserted either after the previous or
       *  before the next table.
       */
      cl_assert(cl, lap_lo < lap_hi);
      err = graph_grmap_add_nonexistent_range(
          grm, dis, lap_i, 0, lap_lo, lap_hi, GRAPH_GUID_DB(*destination),
          offset);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graph_grmap_add_nonexistent_range",
                     err, "[%zu][0], %llx-%llx", lap_i,
                     (unsigned long long)lap_lo, (unsigned long long)lap_hi);
        return err;
      }
      cl_cover(grm->grm_graph->graph_cl);
    }
  }

  if (cl_is_logged(cl, CL_LEVEL_DEBUG)) graph_grmap_dbid_invariant(grm, dis);
  return 0;
}

/**
 * @brief Initialize an empty grmap.
 * @param grm 	the map object
 */
void graph_grmap_initialize(graph_handle* graph, graph_grmap* grm) {
  memset(grm, 0, sizeof(*grm));

  grm->grm_graph = graph;

  /*  Set up the DBID table.
   */
  grm->grm_dbid = grm->grm_dbid_buf;
  grm->grm_m = sizeof(grm->grm_dbid_buf) / sizeof(grm->grm_dbid_buf[0]);
  grm->grm_table_size = GRAPH_GRMAP_DEFAULT_TABLE_SIZE;

  if (cl_is_logged(grm->grm_graph->graph_cl, CL_LEVEL_DEBUG))
    graph_grmap_invariant(grm);
  cl_cover(grm->grm_graph->graph_cl);
}

static void graph_grmap_dbid_finish(graph_grmap* grm,
                                    graph_grmap_dbid_slot* dis) {
  size_t i;
  cm_handle* const cm = grm->grm_graph->graph_cm;

  for (i = 0; i < dis->dis_n; i++) {
    cm_free(cm, dis->dis_table[i].ts_table);
    cl_cover(grm->grm_graph->graph_cl);
  }

  if (dis->dis_m > 0 && dis->dis_table != NULL) {
    cl_cover(grm->grm_graph->graph_cl);
    cm_free(cm, dis->dis_table);
  }

  dis->dis_m = 0;
  dis->dis_n = 0;
  dis->dis_table = NULL;
}

/**
 * @brief Free resources allocated to a grmap.
 * @param grmap 	the map object
 */
void graph_grmap_finish(graph_grmap* grm) {
  size_t i;

  if (grm == NULL || grm->grm_graph == NULL) return;

  cl_cover(grm->grm_graph->graph_cl);
  if (cl_is_logged(grm->grm_graph->graph_cl, CL_LEVEL_DEBUG))
    graph_grmap_invariant(grm);

  for (i = 0; i < grm->grm_n; i++) {
    cl_cover(grm->grm_graph->graph_cl);
    graph_grmap_dbid_finish(grm, grm->grm_dbid + i);
  }

  if (grm->grm_dbid != grm->grm_dbid_buf) {
    cl_cover(grm->grm_graph->graph_cl);
    cm_free(grm->grm_graph->graph_cm, grm->grm_dbid);
  }

  graph_grmap_initialize(grm->grm_graph, grm);
}

/**
 * @brief Set table size of a grmap.
 * @param grmap 	the map object
 */
int graph_grmap_set_table_size(graph_grmap* grm, size_t tab_size) {
  if (grm->grm_n != 0) return GRAPH_ERR_USED;

  if (tab_size == 0) return EINVAL;

  grm->grm_table_size = tab_size;
  return 0;
}

/**
 * @brief What's the first ID we don't have for this dbid?
 *
 * @param grm 	the map object
 * @param dbid 	dbid the caller is asking about.
 */
unsigned long long graph_grmap_dbid_high(graph_grmap const* grm,
                                         unsigned long long dbid) {
  cl_handle* cl;
  size_t db_i;
  graph_grmap_dbid_slot* dis;

  if (grm == NULL) return 0;

  cl = grm->grm_graph->graph_cl;

  for (db_i = 0, dis = grm->grm_dbid; db_i < grm->grm_n; db_i++, dis++)

    if (dis->dis_dbid == dbid) {
      if (dis->dis_n == 0)
        return 0;
      else {
        graph_grmap_table const* tab;

        tab = dis->dis_table[dis->dis_n - 1].ts_table;
        cl_assert(cl, tab->tab_n > 0);
        return tab->tab_data[tab->tab_n - 1].range_high;
      }
    }

  return 0;
}
