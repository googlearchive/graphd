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

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

#define TOLOWER(a) (isascii(a) ? tolower(a) : (a))
#define EQ(a, b) (TOLOWER(a) == TOLOWER(b))
#define ISWORD(a) (!isascii(a) || isalnum(a))
#define ISSPACE(a) (isascii(a) && isspace(a))
#define ISPUNCT(a) (isascii(a) && ispunct(a))

bool graphd_match_guidcon_member(cl_handle* cl,
                                 graphd_guid_constraint const* guidcon,
                                 graph_guid const* guid) {
  if (guidcon->guidcon_include.gs_n == 0) return GRAPH_GUID_IS_NULL(*guid);

  return graphd_guid_set_find(&guidcon->guidcon_include, guid) <
         guidcon->guidcon_include.gs_n;
}

static int match_guidcon(cl_handle* cl, graphd_guid_constraint const* guidcon,
                         graph_guid const* guid) {
  cl_assert(cl, !guidcon->guidcon_match_valid);

  if (guidcon->guidcon_include_annotated) return 0;

  if (guidcon->guidcon_include_valid)
    if (!graphd_guid_set_match(&guidcon->guidcon_include, guid))
      return GRAPHD_ERR_NO;

  if (guidcon->guidcon_exclude_valid)
    if (graphd_guid_set_match(&guidcon->guidcon_exclude, guid))
      return GRAPHD_ERR_NO;

  return 0;
}

/**
 * @brief Does con match pr structurally, as far as our
 *  	child/parent relationship goes?
 *
 * @return 0 for yes
 * @return GRAPHD_ERR_NO for no
 */

static int graphd_match_structure(graphd_request* greq, graphd_constraint* con,
                                  pdb_primitive const* pr,
                                  graph_guid const* guid_parent) {
  cl_handle* cl = graphd_request_cl(greq);
  graph_guid guid;

  cl_assert(cl, con != NULL);
  cl_assert(cl, pr != NULL);

  if (con->con_parent == NULL) {
    cl_cover(cl);
    return 0;
  }

  /*  Are we at the correct end of our parent?
   */
  if (graphd_linkage_is_i_am(con->con_linkage)) {
    unsigned int linkage;
    linkage = graphd_linkage_i_am(con->con_linkage);

    if (guid_parent == NULL) {
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_match_structure: "
             "no parent primitive, but "
             "I'm supposed to be my parent's %s",
             pdb_linkage_to_string(linkage));
      return GRAPHD_ERR_NO;
    }

    /*  If we have a parent, we've already followed
     *  the parent's linkage pointer in order to
     *  get here - no need to revisit the parent primitive.
     */
  }

  /*  Is our parent at the correct end of us?
   */
  else if (graphd_linkage_is_my(con->con_linkage)) {
    unsigned int linkage = graphd_linkage_my(con->con_linkage);

    if (guid_parent == NULL) {
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_match_structure: no parent GUID "
             "in call, but linkage constraint %s",
             pdb_linkage_to_string(linkage));
      return GRAPHD_ERR_NO;
    }
    if (!pdb_primitive_has_linkage(pr, linkage)) {
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_match_structure: primitive has no %s "
             "linkage",
             pdb_linkage_to_string(linkage));
      return GRAPHD_ERR_NO;
    }

    pdb_primitive_linkage_get(pr, linkage, guid);
    if (!GRAPH_GUID_EQ(guid, *guid_parent)) {
      cl_cover(cl);
      char buf[200], buf2[200];
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_match_structure: "
             "parent %s is not my %s %s",
             graph_guid_to_string(&guid, buf, sizeof buf),
             pdb_linkage_to_string(linkage),
             graph_guid_to_string(guid_parent, buf2, sizeof buf2));
      return GRAPHD_ERR_NO;
    }
  }
  return 0;
}

/**
 * @brief Match a GUID against a constraint.
 *
 * @param graphd 	overall graphd module
 * @param cl 		log through here
 * @param asof		NULL or pretend current dateline
 * @param con 		constraint to match against
 * @param guid 		guid to match
 *
 * @return 0 on match,
 * @return GRAPHD_ERR_NO if the condition doesn't match
 * @return other nonzero values on system error
 */

int graphd_match_intrinsics_guid(graphd_handle* graphd, cl_handle* cl,
                                 graph_dateline const* asof,
                                 graphd_constraint* con,
                                 graph_guid const* guid) {
  int err = 0;
  char buf[GRAPH_GUID_SIZE];

  cl_enter(cl, CL_LEVEL_VERBOSE, "guid=%s con=%s",
           graph_guid_to_string(guid, buf, sizeof buf),
           graphd_constraint_to_string(con));

  if (con->con_false) {
    cl_cover(cl);
    cl_leave(cl, CL_LEVEL_VERBOSE, "no: constraint is impossible");
    return GRAPHD_ERR_NO;
  }

  if (con->con_dateline.dateline_min != NULL) {
    unsigned long long dbid;
    unsigned long long serial;
    unsigned long long ull;

    dbid = GRAPH_GUID_DB(*guid);
    serial = GRAPH_GUID_SERIAL(*guid);

    if (graph_dateline_get(con->con_dateline.dateline_min, dbid, &ull) == 0 &&
        serial < ull) {
      cl_cover(cl);
      cl_leave(cl, CL_LEVEL_VERBOSE,
               "no: "
               "too old (< minimum dateline)");
      return GRAPHD_ERR_NO;
    }
  }

  if (con->con_dateline.dateline_max != NULL) {
    unsigned long long dbid;
    unsigned long long serial;
    unsigned long long ull;

    dbid = GRAPH_GUID_DB(*guid);
    serial = GRAPH_GUID_SERIAL(*guid);

    if (graph_dateline_get(con->con_dateline.dateline_max, dbid, &ull) == 0 &&
        serial >= ull) {
      cl_cover(cl);
      cl_leave(cl, CL_LEVEL_VERBOSE, "no: too young (> maximum dateline)");
      return GRAPHD_ERR_NO;
    }
  }

  /*  GUID
   */

  if (con->con_guid.guidcon_include_valid ||
      con->con_guid.guidcon_exclude_valid ||
      con->con_guid.guidcon_match_valid) {
    err = match_guidcon(cl, &con->con_guid, guid);
    if (err) {
      cl_cover(cl);
      cl_leave(cl, CL_LEVEL_VERBOSE, "no: guidcon fails");
      return err;
    }
    cl_cover(cl);
  }

  /*  Newest, Oldest
   */

  if (con->con_newest.gencon_valid || con->con_oldest.gencon_valid) {
    pdb_id id;
    err = pdb_id_from_guid(graphd->g_pdb, &id, guid);
    if (err) {
      cl_leave(cl, CL_LEVEL_VERBOSE,
               "no: "
               "can't get id for guid %s: %s",
               graph_guid_to_string(guid, buf, sizeof buf), strerror(err));
      cl_cover(cl);
      return err;
    }

    err = pdb_generation_check_range(
        graphd->g_pdb, asof, guid, id, con->con_newest.gencon_valid,
        con->con_newest.gencon_min, con->con_newest.gencon_max,
        con->con_oldest.gencon_valid, con->con_oldest.gencon_min,
        con->con_oldest.gencon_max);
    if (err != 0) {
      char buf[200];
      cl_cover(cl);
      cl_leave(cl, CL_LEVEL_VERBOSE,
               "no: "
               "guid %s, id %llx out of new(?%d "
               "%lu..%lu), old(?%d %lu..%lu) range",
               graph_guid_to_string(guid, buf, sizeof buf),
               (unsigned long long)id, con->con_newest.gencon_valid,
               con->con_newest.gencon_min, con->con_newest.gencon_max,
               con->con_oldest.gencon_valid, con->con_oldest.gencon_min,
               con->con_oldest.gencon_max);
      return err;
    }
    cl_cover(cl);
  }

  cl_leave(cl, CL_LEVEL_VERBOSE, "ok");
  return 0;
}

/**
 * @brief Match conditions completely internal to a constraint
 *
 * @param graphd 	overall graphd module
 * @param cl 		log through here
 * @param asof		NULL or pretend current dateline
 * @param con 		constraint to match against
 * @param pr 		primitive to match
 *
 * @return 0 on match,
 * @return GRAPHD_ERR_NO if the condition doesn't match
 * @return other nonzero values on system error
 */

int graphd_match_intrinsics(graphd_request* greq, graphd_constraint* con,
                            pdb_primitive const* pr) {
  graph_dateline const* asof = greq->greq_asof;
  cl_handle* cl = graphd_request_cl(greq);
  graphd_handle* graphd = graphd_request_graphd(greq);
  size_t n;
  int err = 0;
  graph_guid guid;
  char buf[200];
  graphd_string_constraint const* strcon;
  int linkage;

  cl_enter(cl, CL_LEVEL_VERBOSE, "pr %s, con %s",
           pdb_primitive_to_string(pr, buf, sizeof buf),
           graphd_constraint_to_string(con));

  if (con->con_false) {
    cl_cover(cl);
    goto no;
  }

  if (con->con_true) {
    cl_cover(cl);

    pdb_primitive_guid_get(pr, guid);
    err = (GRAPH_GUID_EQ(guid, con->con_guid.guidcon_include.gs_guid[0])
               ? 0
               : GRAPHD_ERR_NO);
    goto ok;
  }

#define MATCH_STRING(word)                                    \
  do {                                                        \
    if ((n = pdb_primitive_##word##_get_size(pr)) > 0) {      \
      str = pdb_primitive_##word##_get_memory(pr);            \
      str_end = str + n - 1;                                  \
    } else                                                    \
      str = str_end = NULL;                                   \
    err = match_strqueue(cl, &con->con_##word, str, str_end); \
    if (err) {                                                \
      if (err == GRAPHD_ERR_NO) goto no;                      \
      cl_log_errno(cl, CL_LEVEL_FAIL, "match_strqueue", err,  \
                   "matching " #word);                        \
      return err;                                             \
    }                                                         \
  } while (0)

  if (con->con_name.strqueue_head != NULL) {
    char const *s, *e;
    graphd_comparator const* cmp;

    cmp = graphd_comparator_default;
    if ((n = pdb_primitive_name_get_size(pr)) > 0)
      e = (s = pdb_primitive_name_get_memory(pr)) + n - 1;
    else
      e = s = NULL;

    for (strcon = con->con_name.strqueue_head; strcon != NULL;
         strcon = strcon->strcon_next) {
      if ((err = graphd_comparator_value_match(greq, strcon, s, e, cmp)) != 0) {
        cl_cover(cl);
        cl_log(cl, CL_LEVEL_VERBOSE,
               "graphd_match_intrinsics: name %.*s doesn't match",
               s ? (int)(e - s) : 4, s ? s : "null");
        goto no;
      }
    }
  }

  if (con->con_value.strqueue_head != NULL) {
    char const *s, *e;
    graphd_comparator const* cmp;

    if ((cmp = con->con_value_comparator) == NULL)
      cmp = graphd_comparator_default;

    if ((n = pdb_primitive_value_get_size(pr)) > 0)
      e = (s = pdb_primitive_value_get_memory(pr)) + n - 1;
    else
      e = s = NULL;

    for (strcon = con->con_value.strqueue_head; strcon;
         strcon = strcon->strcon_next) {
      if ((err = graphd_comparator_value_match(greq, strcon, s, e, cmp)) != 0) {
        cl_cover(cl);
        cl_log(cl, CL_LEVEL_VERBOSE, "value %.*s doesn't match %s",
               s ? (int)(e - s) : 4, s ? s : "null",
               graphd_string_constraint_to_string(strcon, buf, sizeof buf));
        goto no;
      }
    }
  }

  /* Timestamp
   */
  if (con->con_timestamp_valid) {
    graph_timestamp_t timestamp;
    timestamp = pdb_primitive_timestamp_get(pr);

    if (con->con_timestamp_min > timestamp ||
        con->con_timestamp_max < timestamp) {
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_VERBOSE,
             "timestamp %llx out of range %llx...%llx [%s:%d]",
             (unsigned long long)timestamp,
             (unsigned long long)con->con_timestamp_min,
             (unsigned long long)con->con_timestamp_max, __FILE__, __LINE__);
      goto no;
    }
    cl_cover(cl);
  }

  /*  Atleast -- not yet.
   */

  /*  Linkages
   */
  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++) {
    if (con->con_linkcon[linkage].guidcon_include_valid ||
        con->con_linkcon[linkage].guidcon_exclude_valid ||
        con->con_linkcon[linkage].guidcon_match_valid) {
      graph_guid* guid_ptr;

      if (!pdb_primitive_has_linkage(pr, linkage)) {
        guid_ptr = NULL;
        cl_cover(cl);
      } else {
        guid_ptr = &guid;
        pdb_primitive_linkage_get(pr, linkage, guid);
        cl_cover(cl);
      }
      err = match_guidcon(cl, &con->con_linkcon[linkage], guid_ptr);
      if (err) {
        cl_cover(cl);
        cl_log(cl, CL_LEVEL_VERBOSE, "linkage mismatch for %s %s",
               pdb_linkage_to_string(linkage),
               graph_guid_to_string(&guid, buf, sizeof buf));
        goto no;
      }
    }
    cl_cover(cl);
  }

  /* Flags: Archival, Live.
   */
  if (con->con_archival != GRAPHD_FLAG_UNSPECIFIED &&
      con->con_archival != GRAPHD_FLAG_DONTCARE) {
    if (!pdb_primitive_is_archival(pr) !=
        (con->con_archival == GRAPHD_FLAG_FALSE)) {
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_VERBOSE, "archival mismatch");
      goto no;
    }
    cl_cover(cl);
  }
  if (con->con_live != GRAPHD_FLAG_UNSPECIFIED &&
      con->con_live != GRAPHD_FLAG_DONTCARE) {
    if (!pdb_primitive_is_live(pr) != (con->con_live == GRAPHD_FLAG_FALSE)) {
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_VERBOSE, "live mismatch");
      goto no;
    }
    cl_cover(cl);
  }

  /*  Value type
   */
  if (con->con_valuetype != GRAPH_DATA_UNSPECIFIED) {
    unsigned char dt;

    dt = pdb_primitive_valuetype_get(pr);
    if (dt != con->con_valuetype) {
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_VERBOSE, "valuetype mismatch (have: %hu, want: %hu)",
             dt, con->con_valuetype);
      goto no;
    }
    cl_cover(cl);
  }

  /*  GUID-related
   */
  pdb_primitive_guid_get(pr, guid);
  err = graphd_match_intrinsics_guid(graphd, cl, asof, con, &guid);
  if (err != 0) {
    if (err != GRAPHD_ERR_NO) {
    }
    cl_log(cl, CL_LEVEL_VERBOSE,
           "no: "
           "graphd_match_intrinsics_guid fails: %s",
           graphd_strerror(err));
    goto no;
  }

  /*  If this is a constraint with a single GUID, mark it
   *  as "true" -- in other words, cache the results of the
   *  intrinsics test in the constraint.
   */
  if (con->con_guid.guidcon_include_valid &&
      con->con_guid.guidcon_include.gs_n == 1)
    con->con_true = true;

ok:
  cl_leave(cl, CL_LEVEL_VERBOSE, "ok%s", con->con_true ? " (con_true)" : "");
  return 0;

no:
  cl_leave(cl, CL_LEVEL_VERBOSE, "no%s", con->con_false ? " (con_false)" : "");
  return GRAPHD_ERR_NO;
}

static int graphd_match_or(graphd_request* greq, graphd_constraint* con,
                           graphd_read_or_map* rom, pdb_primitive const* pr,
                           graph_guid const* guid_parent) {
  cl_handle* cl = graphd_request_cl(greq);
  graphd_constraint_or* cor;
  int err;

  /* ORs: For each "or" group, one of the left or the right
   *  	branch must be true.
   *
   *      We note in the "or map" which branches are still
   *      active.  The more we match, the more branches become
   *      inactive.
   */
  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    int e2;

    err = graphd_match(greq, &cor->or_head, rom, pr, guid_parent);
    if (err != 0 && err != GRAPHD_ERR_NO) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_match", err, "con=%s",
                   graphd_constraint_to_string(&cor->or_head));
      return err;
    }

    /*  Intrinsics-match the other alternatives while
     *  we're at it, in case the first one falls through later.
     */
    e2 = con->con_or_tail
             ? graphd_match(greq, cor->or_tail, rom, pr, guid_parent)
             : 0;
    if (e2 != 0 && e2 != GRAPHD_ERR_NO) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_match_intrinsics", err, "con=%s",
                   graphd_constraint_to_string(cor->or_tail));
      return err;
    }

    if (err != 0 && e2 != 0) {
      cl_log(cl, CL_LEVEL_VERBOSE, "match_or: both branches fail");
      return GRAPHD_ERR_NO;
    }
  }
  return 0;
}

/**
 * @brief Does con match pr?
 *
 *  We don't know contents yet, and variables may not have
 *  their values yet.
 *
 * @param greq		request pointer
 * @param con		constraint to check
 * @param rom		note here which parts of an "or" matched.
 * @param pr		primitive to match against it
 * @param guid_parent	NULL or the primitive matching con's parent constraint
 *
 * @return 0 for yes, an error number -- typically, GRAPHD_ERR_NO -- on error.
 */

int graphd_match(graphd_request* greq, graphd_constraint* con,
                 graphd_read_or_map* rom, pdb_primitive const* pr,
                 graph_guid const* guid_parent) {
  cl_handle* cl = graphd_request_cl(greq);
  int err = 0;

  if ((err = graphd_match_intrinsics(greq, con, pr)) != 0) {
    if (err != GRAPHD_ERR_NO) return err;
    goto no;
  }
  if ((err = graphd_match_structure(greq, con, pr, guid_parent)) != 0) {
    if (err != GRAPHD_ERR_NO) return err;
    goto no;
  }
  if ((err = graphd_match_or(greq, con, rom, pr, guid_parent)) != 0) {
    if (err != GRAPHD_ERR_NO) return err;
    goto no;
  }
  cl_cover(cl);

  /* Tell the surrounding "or" (if any) that these
   * intrinsics match.
   */
  graphd_read_or_match_intrinsics(greq, con, rom);
  return 0;

no: /*  Tell the surrounding "or" (if any) that these
     *  intrinsics don't match, deactivating the
     *  corresponding subconstraints (if any).
     */
  graphd_read_or_fail(greq, con, rom);
  return GRAPHD_ERR_NO;
}
