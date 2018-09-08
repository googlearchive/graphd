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

#include <limits.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#define HAS_GUID(guidcon)                                                    \
  ((guidcon).guidcon_include_valid && (guidcon).guidcon_include.gs_n == 1 && \
   !GRAPH_GUID_IS_NULL((guidcon).guidcon_include.gs_guid[0]))

unsigned int graphd_constraint_linkage_pattern(graphd_constraint const* con) {
  unsigned int pattern = 0;
  size_t i;

  if (con == NULL) return 0;

  if (graphd_linkage_is_my(con->con_linkage))
    pattern |= 1 << GRAPHD_PATTERN_LINKAGE(graphd_linkage_my(con->con_linkage));

  for (i = 0; i < PDB_LINKAGE_N; i++)
    if (con->con_linkcon[i].guidcon_include_valid)
      pattern |= 1 << GRAPHD_PATTERN_LINKAGE(i);

  for (con = con->con_head; con != NULL; con = con->con_next)
    if (graphd_linkage_is_i_am(con->con_linkage))
      pattern |=
          1 << GRAPHD_PATTERN_LINKAGE(graphd_linkage_i_am(con->con_linkage));

  return pattern;
}

void graphd_constraint_initialize(graphd_handle* g, graphd_constraint* con) {
  memset(con, 0, sizeof(*con));

  con->con_head = NULL;
  con->con_tail = &con->con_head;

  con->con_assignment_head = NULL;
  con->con_assignment_tail = &con->con_assignment_head;

  con->con_variable_declaration_valid = false;
  con->con_pframe_temporary = (size_t)-1;

  con->con_cc_head = NULL;
  con->con_cc_tail = &con->con_cc_head;

  con->con_or_head = NULL;
  con->con_or_tail = &con->con_or_head;

  con->con_type.strqueue_head = NULL;
  con->con_type.strqueue_tail = &con->con_type.strqueue_head;

  con->con_name.strqueue_head = NULL;
  con->con_name.strqueue_tail = &con->con_name.strqueue_head;

  con->con_value.strqueue_head = NULL;
  con->con_value.strqueue_tail = &con->con_value.strqueue_head;

  con->con_high = PDB_ITERATOR_HIGH_ANY;

  con->con_valuetype = GRAPH_DATA_UNSPECIFIED;
  con->con_archival = GRAPHD_FLAG_UNSPECIFIED;
  con->con_live = GRAPHD_FLAG_UNSPECIFIED;
  con->con_meta = GRAPHD_META_UNSPECIFIED;
  con->con_forward = true;
  con->con_sort_valid = true;

  con->con_setsize = pdb_primitive_n(g->g_pdb);

  con->con_key_dup = NULL;
  con->con_unique_dup = NULL;

  con->con_comparator = graphd_comparator_unspecified;
  con->con_it = NULL;
  graphd_bad_cache_initialize(&con->con_bad_cache);
}

int graphd_constraint_use_result_instruction(graphd_request* greq,
                                             graphd_constraint const* con,
                                             graphd_pattern const* pat) {
  cl_handle* cl = graphd_request_cl(greq);

  /*  Can't use "contents" in a constraint that doesn't have
   *  subconstraints.
   */
  if (!con->con_head) {
    if (pat->pat_type == GRAPHD_PATTERN_CONTENTS) {
      cl_cover(cl);
      graphd_request_error(greq,
                           "SEMANTICS can't use \"contents\" return "
                           "instruction in template without "
                           "contained templates");
      return GRAPHD_ERR_SEMANTICS;
    }
    if (pat->pat_type == GRAPHD_PATTERN_LIST) {
      graphd_pattern const* ric;

      for (ric = pat->pat_data.data_list.list_head; ric != NULL;
           ric = ric->pat_next) {
        int err;
        err = graphd_constraint_use_result_instruction(greq, con, ric);
        if (err) {
          cl_cover(cl);
          return err;
        }
      }
    }

    cl_cover(cl);
  }

  cl_cover(cl);
  return 0;
}

void graphd_constraint_append(graphd_constraint* parent,
                              graphd_constraint* child) {
  child->con_parent = parent;
  child->con_next = NULL;
  *parent->con_tail = child;
  parent->con_tail = &child->con_next;
  parent->con_subcon_n++;
}

pdb_id graphd_constraint_dateline_first(graphd_handle* g,
                                        graphd_constraint* con) {
  unsigned long long ull;
  graph_dateline const* dl;

  if ((dl = con->con_dateline.dateline_min) != NULL &&
      graph_dateline_get(dl, pdb_database_id(g->g_pdb), &ull) == 0)
    return ull;
  return 0;
}

bool graphd_constraint_dateline_too_young(graphd_handle* g,
                                          graphd_constraint* con, pdb_id id) {
  unsigned long long ull;
  graph_dateline const* dl;

  return (dl = con->con_dateline.dateline_max) != NULL &&
         graph_dateline_get(dl, pdb_database_id(g->g_pdb), &ull) == 0 &&
         id >= ull;
}

void graphd_constraint_free(graphd_request* greq, graphd_constraint* con) {
  graphd_constraint *sub, *sub_next;
  graphd_constraint_or *cor, *cor_next;
  graphd_handle* g = graphd_request_graphd(greq);

  if (con == NULL) return;

  if (con->con_unique_dup != NULL)
    graphd_constraint_free(greq, con->con_unique_dup);
  if (con->con_key_dup != NULL) graphd_constraint_free(greq, con->con_key_dup);
  if (con->con_anchor_dup != NULL)
    graphd_constraint_free(greq, con->con_anchor_dup);

  pdb_iterator_destroy(g->g_pdb, &con->con_it);

  /*  "Or" alternatives do not free their subconstraints;
   *  they're considered part of their non-alternative
   *  prototype.
   */
  if (con->con_or == NULL) {
    sub_next = con->con_head;
    while ((sub = sub_next) != NULL) {
      sub_next = sub->con_next;
      graphd_constraint_free(greq, sub);
    }
  }

  cor_next = con->con_or_head;
  while ((cor = cor_next) != NULL) {
    cor_next = cor->or_next;

    if (cor->or_tail != NULL) graphd_constraint_free(greq, cor->or_tail);
    graphd_constraint_free(greq, &cor->or_head);

    cm_free(greq->greq_req.req_cm, cor);
  }

  if (con->con_pframe != NULL) {
    cm_free(greq->greq_req.req_cm, con->con_pframe);
    con->con_pframe = NULL;
  }

  if (con->con_sort_comparators.gcl_comp != NULL) {
    cm_free(greq->greq_req.req_cm, con->con_sort_comparators.gcl_comp);
  }

  /*  Unless this constraint is embedded in the request
   *  or a containing "or" alternative branch, free it.
   */
  if (con != greq->greq_constraint_buf &&
      con != greq->greq_constraint_buf + 1 &&
      (con->con_or == NULL || con != &con->con_or->or_head))
    cm_free(greq->greq_req.req_cm, con);
}

/**
 * @brief Do constraints a and b match the same result set?
 *
 * @param cl	log and assert through here.
 * @param a	a constraint
 * @param b	another constraint
 *
 * @return true if they definitely match the same result set;
 *	false if they may or may not match the same result set.
 */
bool graphd_constraint_equal(cl_handle* const cl, graphd_constraint const* a,
                             graphd_constraint const* b) {
  int linkage;
  graphd_constraint_or const *a_or, *b_or;

  if (a == NULL || b == NULL) return a == NULL && b == NULL;

  cl_assert(cl, a != NULL);
  cl_assert(cl, b != NULL);

  if (a->con_subcon_n != b->con_subcon_n || a->con_linkage != b->con_linkage ||
      a->con_valuetype != b->con_valuetype ||
      a->con_archival != b->con_archival || a->con_live != b->con_live ||
      a->con_key != b->con_key || a->con_unique != b->con_unique ||
      a->con_resultpagesize_valid != b->con_resultpagesize_valid ||
      (a->con_resultpagesize_valid &&
       a->con_resultpagesize != b->con_resultpagesize) ||
      a->con_countlimit_valid != b->con_countlimit_valid ||
      (a->con_countlimit_valid && a->con_countlimit != b->con_countlimit) ||
      a->con_pagesize_valid != b->con_pagesize_valid ||
      (a->con_pagesize_valid && a->con_pagesize != b->con_pagesize) ||
      a->con_start != b->con_start)
    return false;

  /*  Parent links enter into the comparison only if they're
   *  a single GUID literal.
   */
  if ((a->con_parent != NULL && HAS_GUID(a->con_parent->con_guid))) {
    if (b->con_parent == NULL || !HAS_GUID(b->con_parent->con_guid) ||
        !graphd_guid_constraint_equal(cl, &a->con_parent->con_guid,
                                      &b->con_parent->con_guid))
      return false;
  } else {
    if (b->con_parent != NULL && HAS_GUID(b->con_parent->con_guid))
      return false;
  }

  if (!graphd_string_constraint_queue_equal(cl, &a->con_type, &b->con_type) ||
      !graphd_string_constraint_queue_equal(cl, &a->con_name, &b->con_name) ||
      !graphd_string_constraint_queue_equal(cl, &a->con_value, &b->con_value))
    return false;

  if (!graphd_guid_constraint_generational_equal(cl, &a->con_newest,
                                                 &b->con_newest) ||
      !graphd_guid_constraint_generational_equal(cl, &a->con_oldest,
                                                 &b->con_oldest))
    return false;

  if (!graphd_guid_constraint_equal(cl, &a->con_guid, &b->con_guid))
    return false;

  if (!graphd_guid_constraint_equal(cl, &a->con_version_next,
                                    &b->con_version_next) ||
      !graphd_guid_constraint_equal(cl, &a->con_version_previous,
                                    &b->con_version_previous))
    return false;

  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++)
    if (!graphd_guid_constraint_equal(cl, a->con_linkcon + linkage,
                                      b->con_linkcon + linkage))
      return false;

  if (a->con_timestamp_valid != b->con_timestamp_valid) return false;
  if (a->con_timestamp_valid) {
    if (a->con_timestamp_max != b->con_timestamp_max ||
        a->con_timestamp_min != b->con_timestamp_min)
      return false;
  }

  if (a->con_count.countcon_min_valid != b->con_count.countcon_min_valid ||
      a->con_count.countcon_max_valid != b->con_count.countcon_max_valid)
    return false;
  if (a->con_count.countcon_min_valid &&
      a->con_count.countcon_min != b->con_count.countcon_min)
    return false;
  if (a->con_count.countcon_max_valid &&
      a->con_count.countcon_max != b->con_count.countcon_max)
    return false;

  if (!graphd_pattern_equal(cl, a, a->con_result, b, b->con_result) ||
      !graphd_pattern_equal(cl, a, a->con_sort_valid ? a->con_sort : NULL, b,
                            b->con_sort_valid ? b->con_sort : NULL))
    return false;

  if (!graph_dateline_equal(a->con_dateline.dateline_min,
                            b->con_dateline.dateline_min) ||
      !graph_dateline_equal(a->con_dateline.dateline_max,
                            b->con_dateline.dateline_max))
    return false;

  if ((a->con_cursor_s == NULL) != (b->con_cursor_s == NULL)) return false;

  if (a->con_cursor_s != NULL &&
      memcmp(a->con_cursor_s, b->con_cursor_s,
             (size_t)(b->con_cursor_e - b->con_cursor_s)) != 0)
    return false;

  if (!graphd_assignments_equal(cl, a, a->con_assignment_head, b,
                                b->con_assignment_head))
    return false;

  for (a_or = a->con_or_head, b_or = b->con_or_head;
       a_or != NULL && b_or != NULL;
       a_or = a_or->or_next, b_or = b_or->or_next) {
    if (!graphd_constraint_equal(cl, &a_or->or_head, &b_or->or_head))
      return false;

    if (!graphd_constraint_equal(cl, a_or->or_tail, b_or->or_tail))
      return false;
  }

  for (a = a->con_head, b = b->con_head; a != NULL && b != NULL;
       a = a->con_next, b = b->con_next) {
    if (!graphd_constraint_equal(cl, a, b)) return false;
  }
  return a == NULL && b == NULL;
}

/**
 * @brief Hash a constraint.
 *
 *  Hashes of equal constraints are equal.  Hashes of unequal
 *  constraints may or may not be equal.
 *
 * @param cl		log and assert through here.
 * @param con		a constraint
 * @param hash_inout	hash accumulator.
 */
void graphd_constraint_hash(cl_handle* const cl, graphd_constraint const* con,
                            unsigned long* const hash_inout) {
  int linkage;

  cl_assert(cl, con != NULL);

  GRAPHD_HASH_VALUE(*hash_inout, con->con_subcon_n);
  GRAPHD_HASH_VALUE(*hash_inout, con->con_linkage);
  GRAPHD_HASH_VALUE(*hash_inout, con->con_valuetype);
  GRAPHD_HASH_VALUE(*hash_inout, con->con_archival);
  GRAPHD_HASH_VALUE(*hash_inout, con->con_live);
  GRAPHD_HASH_VALUE(*hash_inout, con->con_key);
  GRAPHD_HASH_VALUE(*hash_inout, con->con_unique);
  GRAPHD_HASH_VALUE(*hash_inout, con->con_pagesize_valid);

  if (con->con_pagesize_valid)
    GRAPHD_HASH_VALUE(*hash_inout, con->con_pagesize);
  GRAPHD_HASH_VALUE(*hash_inout, con->con_start);

  graphd_string_constraint_hash(cl, &con->con_type, hash_inout);
  graphd_string_constraint_hash(cl, &con->con_name, hash_inout);
  graphd_string_constraint_hash(cl, &con->con_value, hash_inout);

  graphd_guid_constraint_generational_hash(cl, &con->con_newest, hash_inout);
  graphd_guid_constraint_generational_hash(cl, &con->con_oldest, hash_inout);

  /*  Parent links enter into the comparison only if they're
   *  a single GUID literal.
   */
  if ((con->con_parent != NULL && HAS_GUID(con->con_parent->con_guid)))
    graphd_guid_constraint_hash(cl, &con->con_parent->con_guid, hash_inout);

  graphd_guid_constraint_hash(cl, &con->con_guid, hash_inout);
  graphd_guid_constraint_hash(cl, &con->con_version_next, hash_inout);
  graphd_guid_constraint_hash(cl, &con->con_version_previous, hash_inout);

  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++)
    graphd_guid_constraint_hash(cl, con->con_linkcon + linkage, hash_inout);

  GRAPHD_HASH_BIT(*hash_inout, con->con_timestamp_valid);
  if (con->con_timestamp_valid) {
    GRAPHD_HASH_VALUE(*hash_inout, con->con_timestamp_max);
    GRAPHD_HASH_VALUE(*hash_inout, con->con_timestamp_min);
  }

  GRAPHD_HASH_BIT(*hash_inout, con->con_count.countcon_min_valid);
  GRAPHD_HASH_BIT(*hash_inout, con->con_count.countcon_max_valid);

  if (con->con_count.countcon_min_valid)
    GRAPHD_HASH_VALUE(*hash_inout, con->con_count.countcon_min);

  if (con->con_count.countcon_max_valid)
    GRAPHD_HASH_VALUE(*hash_inout, con->con_count.countcon_max);

  graphd_pattern_hash(cl, con->con_result, hash_inout);

  /* Include con_sort in the hash, even if it has been declared
   * invalid by the optimizer.
   */
  graphd_pattern_hash(cl, con->con_sort, hash_inout);

  graphd_dateline_constraint_hash(cl, &con->con_dateline, hash_inout);

  if (con->con_cursor_s != NULL)
    GRAPHD_HASH_BYTES(*hash_inout, con->con_cursor_s, con->con_cursor_e);

  graphd_assignments_hash(cl, con->con_assignment_head, hash_inout);

  for (con = con->con_head; con != NULL; con = con->con_next)
    graphd_constraint_hash(cl, con, hash_inout);
}

/**
 * @brief Does this constraint use the "count" pattern?
 */
bool graphd_constraint_uses_pattern(graphd_constraint const* con, int pat) {
  graphd_assignment const* a;

  if (con->con_result != NULL && graphd_pattern_lookup(con->con_result, pat))
    return true;

  if (con->con_sort != NULL && con->con_sort_valid &&
      graphd_pattern_lookup(con->con_sort, pat))
    return true;

  for (a = con->con_assignment_head; a != NULL; a = a->a_next)
    if (a->a_result != NULL && graphd_pattern_lookup(a->a_result, pat))
      return true;

  return false;
}

/**
 * @brief Does this constraint use the "contents" pattern?
 *
 *  This is valid only after NULL result constraints have been
 *  filled in with defaults.
 *
 */
bool graphd_constraint_uses_contents(graphd_constraint const* con) {
  return graphd_constraint_uses_pattern(con, GRAPHD_PATTERN_CONTENTS);
}

/* Pull branch invariants from the branches into their prototype.
 */
static int graphd_constraint_branch_invariants(graphd_request* greq,
                                               graphd_constraint* con) {
  graphd_constraint* proto = NULL;
  graphd_constraint_or* cor;
  int err;

  if (con->con_or != NULL) proto = con->con_or->or_prototype;

  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    err = graphd_constraint_branch_invariants(greq, &cor->or_head);
    if (err != 0) return err;

    if (cor->or_tail) {
      err = graphd_constraint_branch_invariants(greq, cor->or_tail);
      if (err != 0) return err;
    }

    /*  If both sides of the "or" have something in common,
     *  that thing is true for the combined constraint, too.
     */
    if (cor->or_tail != NULL &&
        cor->or_head.con_linkage == cor->or_tail->con_linkage &&
        cor->or_head.con_linkage != 0) {
      con->con_linkage = cor->or_head.con_linkage;

      /* etc. */
    }
  }
  return 0;
}

/**
 * @brief Fill in defaults of a constraint from its prototype.
 */
static int graphd_constraint_branch_defaults(graphd_request* greq,
                                             graphd_constraint* proto,
                                             graphd_constraint* con) {
  graphd_constraint_or* cor;

  if (proto == NULL) return 0;

  /*  Generational constraints
   *
   *  If neither oldest nor newest have been set, the
   *  constraint defaults to newest=0
   */
  if (!con->con_newest.gencon_assigned && !con->con_oldest.gencon_assigned) {
    con->con_newest = proto->con_newest;
    con->con_oldest = proto->con_oldest;
  }

  /*  Dateline constraints
   */
  if (con->con_dateline.dateline_min == NULL &&
      con->con_dateline.dateline_max == NULL) {
    con->con_dateline.dateline_min = proto->con_dateline.dateline_min;
    con->con_dateline.dateline_max = proto->con_dateline.dateline_max;
  }

  con->con_countlimit_valid = proto->con_countlimit_valid;
  con->con_countlimit = proto->con_countlimit;

  con->con_resultpagesize = proto->con_resultpagesize;
  con->con_resultpagesize_valid = proto->con_resultpagesize_valid;

  con->con_resultpagesize_parsed = proto->con_resultpagesize_parsed;
  con->con_resultpagesize_parsed_valid = proto->con_resultpagesize_parsed_valid;

  if (proto->con_linkage != 0) con->con_linkage = proto->con_linkage;

  /*  Fill in the defaults of our subbranches from ourselves.
   */
  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    int err = graphd_constraint_branch_defaults(greq, con, &cor->or_head);
    if (err != 0) return err;

    if (cor->or_tail != NULL) {
      err = graphd_constraint_branch_defaults(greq, con, &cor->or_head);
      if (err != 0) return err;
    }
  }
  return 0;
}

/**
 * @brief Fill in defaults of a constraint.
 *
 *  XXX finish and call..
 */
int graphd_constraint_defaults(graphd_request* greq, graphd_constraint* con) {
  /*  Promote common aspects of branches up into the
   *  prototype.
   */
  graphd_constraint_branch_invariants(greq, con);

  /*  If this is a branch, inherit from the prototype.
   */
  if (con->con_or != NULL) {
    /*  Inherit from our prototype.
     */
    int err =
        graphd_constraint_branch_defaults(greq, con->con_or->or_prototype, con);
    if (err != 0) return err;
  }

  /* Actual defalting starts here.
   */

  /* Generational constraints
   *
   *  If neither oldest nor newest have been set, the
   *  constraint defaults to newest=0
   */
  if (!con->con_newest.gencon_assigned && !con->con_oldest.gencon_assigned) {
    /*  Default to newest=0
     */
    con->con_newest.gencon_valid = true;
    con->con_newest.gencon_min = con->con_newest.gencon_max = 0;
    con->con_oldest.gencon_valid = false;
  }
  return 0;
}

static graphd_constraint* graphd_constraint_by_id_recursive(
    graphd_constraint* par, size_t id) {
  graphd_constraint* con;

  if (par->con_id == id) return par;

  for (par = par->con_head; par != NULL; par = par->con_next) {
    con = graphd_constraint_by_id_recursive(par, id);
    if (con != NULL) return con;
  }
  return NULL;
}

graphd_constraint* graphd_constraint_by_id(graphd_request const* greq,
                                           size_t id) {
  return graphd_constraint_by_id_recursive(greq->greq_constraint, id);
}

int graphd_constraint_get_heatmap(graphd_request const* greq,
                                  graphd_constraint* con, cm_buffer* buf) {
  int err;

  if (con == NULL) return 0;

  err =
      cm_buffer_sprintf(buf, "(nn=%llu nc=%lld cn=%llu cc=%lld fn=%llu fc=%lld",
                        con->con_iterator_account.ia_next_n,
                        (long long)con->con_iterator_account.ia_next_cost,
                        con->con_iterator_account.ia_check_n,
                        (long long)con->con_iterator_account.ia_check_cost,
                        con->con_iterator_account.ia_find_n,
                        (long long)con->con_iterator_account.ia_find_cost);
  if (err != 0) return err;

  if (con->con_head) {
    for (con = con->con_head; con != NULL; con = con->con_next) {
      err = cm_buffer_add_string(buf, " ");
      if (err != 0) return err;

      err = graphd_constraint_get_heatmap(greq, con, buf);
      if (err != 0) return err;
    }
  }
  err = cm_buffer_add_string(buf, ")");
  if (err != 0) return err;

  return 0;
}
