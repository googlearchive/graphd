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

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>

graphd_constraint_clause *graphd_constraint_clause_alloc(graphd_request *greq,
                                                         int type) {
  graphd_constraint_clause *cc;

  cc = cm_malloc(greq->greq_req.req_cm, sizeof(*cc));
  if (cc == NULL) return cc;

  memset(cc, 0, sizeof(*cc));
  cc->cc_type = type;
  cc->cc_next = NULL;

  return cc;
}

graphd_constraint_clause *graphd_constraint_clause_alloc_cursor(
    graphd_request *greq, char const *s, char const *e) {
  graphd_constraint_clause *cc;

  cc = cm_malloc(greq->greq_req.req_cm, sizeof(*cc) + (e - s) + 1);
  if (cc == NULL) return cc;
  memset(cc, 0, sizeof(*cc));

  memcpy((char *)(cc + 1), s, e - s);
  ((char *)(cc + 1))[e - s] = '\0';

  cc->cc_type = GRAPHD_CC_CURSOR;
  cc->cc_next = NULL;
  cc->cc_data.cd_cursor.cursor_s = (char *)(cc + 1);
  cc->cc_data.cd_cursor.cursor_e = (char *)(cc + 1) + (e - s);

  return cc;
}

graphd_constraint_clause *graphd_constraint_clause_alloc_assignment(
    graphd_request *greq, char const *s, char const *e, graphd_pattern *pat) {
  graphd_constraint_clause *cc;

  cc = cm_malloc(greq->greq_req.req_cm, sizeof(*cc) + (e - s) + 1);
  if (cc == NULL) return cc;
  memset(cc, 0, sizeof(*cc));

  memcpy((char *)(cc + 1), s, e - s);
  ((char *)(cc + 1))[e - s] = '\0';

  cc->cc_type = GRAPHD_CC_ASSIGNMENT;
  cc->cc_next = NULL;
  cc->cc_data.cd_assignment.asn_pattern = pat;
  cc->cc_data.cd_assignment.asn_name_s = (char *)(cc + 1);
  cc->cc_data.cd_assignment.asn_name_e = (char *)(cc + 1) + (e - s);
  return cc;
}

graphd_constraint_clause *graphd_constraint_clause_alloc_sequence(
    graphd_request *greq, graphd_constraint_clause **head,
    graphd_constraint_clause *tail) {
  graphd_constraint_clause *cc;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_SEQUENCE);
  if (cc == NULL) return cc;

  /*  Replace the sub-chain *head..tail with cc(*head..tail).
   */
  cc->cc_data.cd_sequence = *head;
  cc->cc_next = tail->cc_next;
  tail->cc_next = NULL;

  return *head = cc;
}

void graphd_constraint_clause_append(graphd_constraint *con,
                                     graphd_constraint_clause *cc) {
  *con->con_cc_tail = cc;
  con->con_cc_tail = &cc->cc_next;
}

/*  During the semantic analysis, individual constraint clauses
 *  are merged into one big semantic graphd_constraint.
 */

static void clause_merge_timestamp(graphd_request *greq, graphd_constraint *con,
                                   graphd_constraint_clause *cc) {
  cl_handle *cl = graphd_request_cl(greq);
  graph_timestamp_t ts;

  if (!con->con_timestamp_valid) {
    con->con_timestamp_valid = 1;
    con->con_timestamp_min = GRAPH_TIMESTAMP_MIN;
    con->con_timestamp_max = GRAPH_TIMESTAMP_MAX;
  }

  ts = cc->cc_data.cd_timestamp.timestamp_value;
  switch (cc->cc_data.cd_timestamp.timestamp_op) {
    case GRAPHD_OP_LT:
      if (ts == GRAPH_TIMESTAMP_MIN)
        con->con_false = true;
      else
        con->con_timestamp_max = ts - 1;
      break;
    case GRAPHD_OP_LE:
      if (con->con_timestamp_max > ts) con->con_timestamp_max = ts;
      break;
    case GRAPHD_OP_EQ:
      if (con->con_timestamp_min < ts) con->con_timestamp_min = ts;
      if (con->con_timestamp_max > ts) con->con_timestamp_max = ts;
      break;
    case GRAPHD_OP_NE:
      if (con->con_timestamp_min == ts) con->con_timestamp_min++;
      if (con->con_timestamp_max == ts) con->con_timestamp_max--;
      break;
    case GRAPHD_OP_GE:
      if (con->con_timestamp_min < ts) con->con_timestamp_min = ts;
      break;
    case GRAPHD_OP_GT:
      if (ts >= GRAPH_TIMESTAMP_MAX)
        con->con_false = true;
      else if (con->con_timestamp_min <= ts)
        con->con_timestamp_min = ts + 1;
      break;
    default:
      cl_notreached(cl, "unexpected timestamp operator %d",
                    (int)cc->cc_data.cd_timestamp.timestamp_op);
  }
  if (con->con_timestamp_max < con->con_timestamp_min) con->con_false = true;
}

static void clause_merge_count(graphd_request *greq, graphd_constraint *con,
                               graphd_constraint_clause *cc) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_count_constraint *count;
  unsigned long long val;

  /* initialize the boundary condition if the numerical
   * operator implies it; enum values for numerical
   * operators are ordered < <= = >= >
   */
  count = &con->con_count;
  val = cc->cc_data.cd_count.count_value;

  if (cc->cc_data.cd_count.count_op <= GRAPHD_OP_EQ) /* < <= = */
    if (!count->countcon_max_valid) {
      count->countcon_max_valid = true;
      count->countcon_max = val;
    }

  if (cc->cc_data.cd_count.count_op >= GRAPHD_OP_EQ) /* = >= > */
    if (!count->countcon_min_valid) {
      count->countcon_min_valid = true;
      count->countcon_min = 0;
    }

  /* adjust the boundary the specific operator is talking about
   */
  switch (cc->cc_data.cd_count.count_op) {
    case GRAPHD_OP_LT:
      cl_assert(cl, count->countcon_max_valid);
      if (val == 0) con->con_false = true;
      if (count->countcon_max >= val) count->countcon_max = val - 1;
      break;
    case GRAPHD_OP_LE:
      cl_assert(cl, count->countcon_max_valid);
      if (count->countcon_max > val) count->countcon_max = val;
      break;
    case GRAPHD_OP_EQ:
      cl_assert(cl, count->countcon_max_valid);
      cl_assert(cl, count->countcon_min_valid);
      if (count->countcon_min < val) count->countcon_min = val;
      if (count->countcon_max > val) count->countcon_max = val;
      break;
    case GRAPHD_OP_NE:
      cl_assert(cl, count->countcon_max_valid);
      cl_assert(cl, count->countcon_min_valid);
      if (count->countcon_min == val) count->countcon_min++;
      if (count->countcon_max == val) count->countcon_max--;
      break;
    case GRAPHD_OP_GE:
      cl_assert(cl, count->countcon_min_valid);
      if (count->countcon_min < val) count->countcon_min = val;
      break;
    case GRAPHD_OP_GT:
      cl_assert(cl, count->countcon_min_valid);
      if (val >= ULONG_MAX) con->con_false = true;
      if (count->countcon_min <= val) count->countcon_min = val + 1;
      break;
    default:
      cl_notreached(cl, "unexpected count operator %d",
                    cc->cc_data.cd_count.count_op);
  }

  /* if there's a valid maximum that's smaller than
   * the default minimum, adjust the minimum
   */
  if (!count->countcon_min_valid && count->countcon_max_valid &&
      count->countcon_max < 1) {
    count->countcon_min_valid = true;
    count->countcon_min = 0;
  }

  /*  Mark the constraint as impossible if the
   *  maximum is smaller than the minimum.
   */
  if (count->countcon_max_valid && count->countcon_min_valid &&
      count->countcon_max < count->countcon_min)

    con->con_false = true;
}

static int clause_merge_dateline(graphd_request *greq, graphd_constraint *con,
                                 graphd_constraint_clause *cc) {
  cl_handle *cl = graphd_request_cl(greq);
  graph_dateline *dl;
  void *state;
  unsigned long long dbid;
  unsigned long long n;
  int err;

  dl = cc->cc_data.cd_dateline.dateline_value;
  switch (cc->cc_data.cd_dateline.dateline_op) {
    case GRAPHD_OP_LT:
      if (con->con_dateline.dateline_max == NULL) {
        con->con_dateline.dateline_max = dl;
        break;
      }
      state = NULL;
      while (!graph_dateline_next(dl, &dbid, &n, &state))
        if ((err = graph_dateline_add_minimum(&con->con_dateline.dateline_max,
                                              dbid, n,
                                              graph_dateline_instance_id(dl))))
          return err;
      graph_dateline_destroy(dl);
      break;

    case GRAPHD_OP_GT:
      if (con->con_dateline.dateline_min == NULL) {
        con->con_dateline.dateline_min = dl;
        break;
      }
      state = NULL;
      while (!graph_dateline_next(dl, &dbid, &n, &state))
        if ((err = graph_dateline_add(&con->con_dateline.dateline_min, dbid, n,
                                      graph_dateline_instance_id(dl))))
          return err;
      graph_dateline_destroy(dl);
      break;

    default:
      cl_notreached(cl, "unexpected dateline operator %d",
                    (int)cc->cc_data.cd_dateline.dateline_op);
  }
  return 0;
}

static void clause_merge_gencon(graphd_request *greq, graphd_constraint *con,
                                graphd_generational_constraint *gencon,
                                graphd_constraint_clause *cc) {
  unsigned long long gen;
  cl_handle *cl = graphd_request_cl(greq);

  gen = cc->cc_data.cd_gencon.gencon_value;

  if (!gencon->gencon_valid) {
    gencon->gencon_valid = 1;
    gencon->gencon_min = 0;
    gencon->gencon_max = ULONG_MAX;
  }

  switch (cc->cc_data.cd_gencon.gencon_op) {
    case GRAPHD_OP_LT:
      if (gen == 0)
        con->con_false = true;
      else
        gencon->gencon_max = gen - 1;
      break;
    case GRAPHD_OP_LE:
      if (gencon->gencon_max > gen) gencon->gencon_max = gen;
      break;
    case GRAPHD_OP_EQ:
      if (gencon->gencon_min < gen) gencon->gencon_min = gen;
      if (gencon->gencon_max > gen) gencon->gencon_max = gen;
      break;
    case GRAPHD_OP_GE:
      if (gencon->gencon_min < gen) gencon->gencon_min = gen;
      break;
    case GRAPHD_OP_GT:
      if (gen >= ULONG_MAX)
        con->con_false = true;
      else if (gencon->gencon_min <= gen)
        gencon->gencon_min = gen + 1;
      break;
    case GRAPHD_OP_NE:
      if (gencon->gencon_min == gen) gencon->gencon_min++;
      if (gencon->gencon_max == gen) gencon->gencon_max--;
      break;
    default:
      cl_notreached(cl, "unexpected gencon op %d",
                    (int)cc->cc_data.cd_gencon.gencon_op);
  }
  if (gencon->gencon_max < gencon->gencon_min) con->con_false = true;
}

char const *graphd_constraint_clause_to_string(
    graphd_constraint_clause const *cc, char *buf, size_t size) {
  char const *b0 = buf, *r, *name;
  char b2[200];
  graphd_constraint_clause const *sub_cc;

  switch (cc->cc_type) {
    case GRAPHD_CC_ANCHOR:
      name = "anchor";
      goto have_flag;

    case GRAPHD_CC_ARCHIVAL:
      name = "archival";
    /* FALL THROUGH */
    have_flag:
      snprintf(
          buf, size, "{%s=%s}", name,
          graphd_constraint_flag_to_string(cc->cc_data.cd_flag, b2, sizeof b2));
      break;

    case GRAPHD_CC_ASSIGNMENT:
      snprintf(buf, size, "{%.*s=%s}",
               (int)(cc->cc_data.cd_assignment.asn_name_e -
                     cc->cc_data.cd_assignment.asn_name_s),
               cc->cc_data.cd_assignment.asn_name_s,
               graphd_pattern_to_string(cc->cc_data.cd_assignment.asn_pattern,
                                        b2, sizeof b2));
      break;

    case GRAPHD_CC_LINKAGE:
      if (graphd_linkage_is_my(cc->cc_data.cd_linkage)) {
        snprintf(
            buf, size, "<-%s",
            pdb_linkage_to_string(graphd_linkage_my(cc->cc_data.cd_linkage)));
      } else if (graphd_linkage_is_i_am(cc->cc_data.cd_linkage)) {
        snprintf(
            buf, size, "%s->(~)",
            pdb_linkage_to_string(graphd_linkage_i_am(cc->cc_data.cd_linkage)));
      } else
        snprintf(buf, size, "{unexpected linkage %u}", cc->cc_data.cd_linkage);
      break;

    case GRAPHD_CC_COMPARATOR:
    case GRAPHD_CC_COUNT:
    case GRAPHD_CC_DATELINE:
    case GRAPHD_CC_GUID:
    case GRAPHD_CC_GUIDLINK:
    case GRAPHD_CC_NEWEST:
    case GRAPHD_CC_NEXT:
    case GRAPHD_CC_OLDEST:
    case GRAPHD_CC_PREV:
    case GRAPHD_CC_SORTCOMPARATOR:
    case GRAPHD_CC_START:
    case GRAPHD_CC_TIMESTAMP:
    case GRAPHD_CC_VALUECOMPARATOR:
      snprintf(buf, size, "{cc type #%d (implement me!)}", cc->cc_type);
      break;

    case GRAPHD_CC_SUBCON:
      snprintf(buf, size, "{subcon %s}",
               graphd_constraint_to_string(cc->cc_data.cd_subcon));
      break;

    case GRAPHD_CC_COUNTLIMIT:
      snprintf(buf, size, "{countlimit=%llu}", cc->cc_data.cd_limit);
      break;

    case GRAPHD_CC_CURSOR:
      snprintf(buf, size, "{cursor=%.*s}",
               (int)(cc->cc_data.cd_cursor.cursor_e -
                     cc->cc_data.cd_cursor.cursor_s),
               cc->cc_data.cd_cursor.cursor_s);
      break;

    case GRAPHD_CC_FALSE:
      return "{false}";

    case GRAPHD_CC_LIVE:
      name = "live";
      goto have_flag;

    case GRAPHD_CC_NAME:
      name = "name";
    /* FALL THROUGH */
    have_strcon:
      snprintf(buf, size, "{%s=%s}", name,
               graphd_string_constraint_to_string(cc->cc_data.cd_strcon, b2,
                                                  sizeof b2));
      break;

    case GRAPHD_CC_BOR:
    case GRAPHD_CC_LOR:
      snprintf(buf, size, "{%s%s%s}",
               graphd_constraint_to_string(&cc->cc_data.cd_or->or_head),
               "||" + (cc->cc_type == GRAPHD_CC_BOR),
               graphd_constraint_to_string(cc->cc_data.cd_or->or_tail));
      break;

    case GRAPHD_CC_PAGESIZE:
      snprintf(buf, size, "{pagesize=%llu}", cc->cc_data.cd_limit);
      break;

    case GRAPHD_CC_RESULT:
      snprintf(buf, size, "{result=%s}",
               graphd_pattern_to_string(cc->cc_data.cd_pattern, b2, sizeof b2));
      break;

    case GRAPHD_CC_SORT:
      snprintf(buf, size, "{sort=%s}",
               graphd_pattern_to_string(cc->cc_data.cd_pattern, b2, sizeof b2));
      break;

    case GRAPHD_CC_RESULTPAGESIZE:
      snprintf(buf, size, "{resultpagesize=%llu}", cc->cc_data.cd_limit);
      break;

    case GRAPHD_CC_SEQUENCE:
      if (size <= 10) return "{..sequence..}";
      sub_cc = cc->cc_data.cd_sequence;
      while (sub_cc != NULL) {
        char const *tmp =
            graphd_constraint_clause_to_string(sub_cc, b2, sizeof b2);
        if (strlen(tmp) + 2 > size) {
          snprintf(buf, size, /*{*/ "...}");
          break;
        }
        snprintf(buf, size, "%s%s",
                 sub_cc == cc->cc_data.cd_sequence ? "{" /*}*/ : " ", tmp);

        size -= strlen(buf);
        buf += strlen(buf);

        if (size <= 1) {
          memcpy(buf - 4, /*{*/ "..}", 4);
          break;
        }
        sub_cc = sub_cc->cc_next;
      }
      if (size <= 1)
        memcpy(buf - 2, /*{*/ "}", 2);
      else {
        *buf++ = /*{*/ '}';
        *buf = '\0';
        break;
      }
      break;

    case GRAPHD_CC_TYPE:
      name = "type";
      goto have_strcon;

    case GRAPHD_CC_VALUE:
      name = "value";
      goto have_strcon;

    case GRAPHD_CC_VALTYPE:
      r = graph_datatype_to_string(cc->cc_data.cd_valtype);
      if (r == NULL)
        snprintf(buf, size, "{valtype=%d}", (int)cc->cc_data.cd_valtype);
      else
        snprintf(buf, size, "{valtype=%s}", r);
      break;

    case GRAPHD_CC_META:
      snprintf(buf, size, "{%s}", graphd_constraint_meta_to_string(
                                      cc->cc_data.cd_meta, b2, sizeof b2));
      break;

    default:
      snprintf(buf, size, "{%p: unexpected cc->cc_type %d}", (void *)cc,
               (int)cc->cc_type);
      break;
  }
  return b0;
}

static int graphd_constraint_clause_merge_or_brach(graphd_request *greq,
                                                   graphd_constraint_or *cor,
                                                   graphd_constraint *branch) {
  int err;
  graphd_constraint *proto = cor->or_prototype;

  branch->con_or = cor;
  branch->con_parent = proto->con_parent;

  err = graphd_constraint_clause_merge_all(greq, branch);
  if (err != 0) return err;

  /*  Merge the subconstraint chains.  The subordinate
   *  branch keeps weak pointers.
   */
  if (branch->con_head != NULL) {
    *proto->con_tail = branch->con_head;
    proto->con_tail = branch->con_tail;
    proto->con_subcon_n += branch->con_subcon_n;
  }

  return 0;
}

int graphd_constraint_clause_merge(graphd_request *greq, graphd_constraint *con,
                                   graphd_constraint_clause *cc) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_string_constraint_queue *q;
  graphd_string_constraint *strcon;
  graphd_flag_constraint *fl;
  graphd_constraint *sub;
  graphd_assignment *asn;
  graphd_comparator const **comparator_dest;
  graphd_constraint_clause *sub_cc, *sub_cc_next;
  graphd_constraint_or *cor;
  char const *name = "*unknown*";
  int err;

  con->con_title = NULL;
  switch (cc->cc_type) {
    case GRAPHD_CC_ANCHOR:
      fl = &con->con_anchor;
      name = "anchor";
      goto have_flag;

    case GRAPHD_CC_ARCHIVAL:
      fl = &con->con_archival;
      name = "archival";
    /* FALL THROUGH */
    have_flag:
      if (*fl != GRAPHD_FLAG_UNSPECIFIED) {
        graphd_request_errprintf(
            greq, false, "SEMANTICS duplicate assignment to \"%s\" flag", name);
        return GRAPHD_ERR_SEMANTICS;
      }
      *fl = cc->cc_data.cd_flag;
      break;

    case GRAPHD_CC_ASSIGNMENT:
      asn = graphd_assignment_alloc(greq, con,
                                    cc->cc_data.cd_assignment.asn_name_s,
                                    cc->cc_data.cd_assignment.asn_name_e);
      if (asn == NULL) return ENOMEM;

      asn->a_result = cc->cc_data.cd_assignment.asn_pattern;
      break;

    case GRAPHD_CC_COMPARATOR:
      comparator_dest = &con->con_comparator;
      name = "comparator";
    /* FALL THROUGH */
    have_comparator_dest:
      if (*comparator_dest != NULL &&
          *comparator_dest != graphd_comparator_unspecified) {
        graphd_request_errprintf(greq, false, "SEMANTICS more than one %s=...",
                                 name);
        return GRAPHD_ERR_SEMANTICS;
      }
      *comparator_dest = cc->cc_data.cd_comparator;
      break;

    case GRAPHD_CC_COUNT:
      clause_merge_count(greq, con, cc);
      break;

    case GRAPHD_CC_COUNTLIMIT:
      if (con->con_countlimit_valid) {
        graphd_request_errprintf(greq, false,
                                 "SEMANTICS more than one countlimit=...");
        return GRAPHD_ERR_SEMANTICS;
      }
      con->con_countlimit = cc->cc_data.cd_limit;
      con->con_countlimit_valid = true;
      break;

    case GRAPHD_CC_CURSOR:
      /* cursor already defined? */
      if (con->con_cursor_s) {
        graphd_request_errprintf(greq, false,
                                 "SEMANTICS more than one cursor=...");
        return GRAPHD_ERR_SEMANTICS;
      }
      con->con_cursor_s = cc->cc_data.cd_cursor.cursor_s;
      con->con_cursor_e = cc->cc_data.cd_cursor.cursor_e;
      break;

    case GRAPHD_CC_DATELINE:
      err = clause_merge_dateline(greq, con, cc);
      if (err != 0) return err;
      break;

    case GRAPHD_CC_FALSE:
      con->con_false = true;
      break;

    case GRAPHD_CC_GUID:
      err = graphd_guid_constraint_merge(greq, con, &con->con_guid,
                                         cc->cc_data.cd_guidcon.guidcon_op,
                                         cc->cc_data.cd_guidcon.guidcon_set);
      if (err != 0) return err;
      break;

    case GRAPHD_CC_GUIDLINK:
      err = graphd_guid_constraint_merge(
          greq, con, con->con_linkcon + cc->cc_data.cd_guidcon.guidcon_linkage,
          cc->cc_data.cd_guidcon.guidcon_op,
          cc->cc_data.cd_guidcon.guidcon_set);
      if (err != 0) return err;
      break;

    case GRAPHD_CC_LINKAGE:
      if (con->con_linkage != 0) {
        graphd_request_errprintf(greq, false,
                                 "SEMANTICS more than one linkage connection");
        return GRAPHD_ERR_SEMANTICS;
      }
      con->con_linkage = cc->cc_data.cd_linkage;
      break;

    case GRAPHD_CC_LIVE:
      fl = &con->con_live;
      name = "live";
      goto have_flag;

    case GRAPHD_CC_NAME:
      q = &con->con_name;
      name = "name";
    /* FALL THROUGH */
    have_strconqueue:
      strcon = cc->cc_data.cd_strcon;

      /* Add strcon to the queue.
       */
      *q->strqueue_tail = strcon;
      q->strqueue_tail = &strcon->strcon_next;
      break;

    case GRAPHD_CC_NEWEST:
      clause_merge_gencon(greq, con, &con->con_newest, cc);
      break;

    case GRAPHD_CC_NEXT:
      err = graphd_guid_constraint_merge(greq, con, &con->con_version_next,
                                         cc->cc_data.cd_guidcon.guidcon_op,
                                         cc->cc_data.cd_guidcon.guidcon_set);
      if (err != 0) return err;
      break;

    case GRAPHD_CC_OLDEST:
      clause_merge_gencon(greq, con, &con->con_oldest, cc);
      break;

    case GRAPHD_CC_BOR:
    case GRAPHD_CC_LOR:

      /*  Chain the "or" into its parent constraint's
       * "or" chain.
       */
      cor = *con->con_or_tail = cc->cc_data.cd_or;
      con->con_or_tail = &cor->or_next;
      cor->or_prototype = con;

      /*  Merge the two or halves.
       */
      err = graphd_constraint_clause_merge_or_brach(greq, cor, &cor->or_head);
      if (err != 0) return err;

      if (cor->or_tail != NULL) {
        err = graphd_constraint_clause_merge_or_brach(greq, cor, cor->or_tail);
        if (err != 0) return err;
      }
      break;

    case GRAPHD_CC_PAGESIZE:
      if (con->con_pagesize_valid) {
        graphd_request_errprintf(greq, false,
                                 "SEMANTICS more than one pagesize");
        return GRAPHD_ERR_SEMANTICS;
      }
      con->con_pagesize = cc->cc_data.cd_limit;
      con->con_pagesize_valid = true;
      break;

    case GRAPHD_CC_PREV:
      err = graphd_guid_constraint_merge(greq, con, &con->con_version_previous,
                                         cc->cc_data.cd_guidcon.guidcon_op,
                                         cc->cc_data.cd_guidcon.guidcon_set);
      if (err != 0) return err;
      break;

    case GRAPHD_CC_RESULT:
      if (con->con_result != NULL) {
        graphd_request_errprintf(greq, false,
                                 "SEMANTICS more than one value for result");
        return GRAPHD_ERR_SEMANTICS;
      }
      con->con_result = cc->cc_data.cd_pattern;
      break;

    case GRAPHD_CC_RESULTPAGESIZE:
      if (con->con_resultpagesize_parsed_valid) {
        graphd_request_errprintf(greq, false,
                                 "SEMANTICS more than one resultpagesize");
        return GRAPHD_ERR_SEMANTICS;
      }
      con->con_resultpagesize_parsed = cc->cc_data.cd_limit;
      con->con_resultpagesize_parsed_valid = true;
      break;

    case GRAPHD_CC_SEQUENCE:
      for (sub_cc = cc->cc_data.cd_sequence; sub_cc != NULL;
           sub_cc = sub_cc_next) {
        sub_cc_next = sub_cc->cc_next;
        err = graphd_constraint_clause_merge(greq, con, sub_cc);
        if (err != 0) return err;
      }
      break;

    case GRAPHD_CC_SORT:
      if (con->con_sort != NULL && con->con_sort_valid) {
        graphd_request_errprintf(greq, false,
                                 "SEMANTICS more than one value for sort");
        return GRAPHD_ERR_SEMANTICS;
      }
      con->con_sort = cc->cc_data.cd_pattern;
      break;

    case GRAPHD_CC_SORTCOMPARATOR:
      if (con->con_sort_comparators.gcl_used) {
        graphd_request_errprintf(greq, false,
                                 "SEMANTICS more than one sortcomparator=...");
        return GRAPHD_ERR_SEMANTICS;
      }
      con->con_sort_comparators = cc->cc_data.cd_sortcomparators;
      return 0;

    case GRAPHD_CC_START:
      if (con->con_start != 0) {
        graphd_request_errprintf(greq, false, "SEMANTICS more than one start");
        return GRAPHD_ERR_SEMANTICS;
      }
      con->con_start = cc->cc_data.cd_start;
      break;

    case GRAPHD_CC_SUBCON:
      sub = cc->cc_data.cd_subcon;
      sub->con_parent = con;
      *con->con_tail = sub;
      con->con_tail = &(sub->con_next);
      con->con_subcon_n++;
      break;

    case GRAPHD_CC_TIMESTAMP:
      clause_merge_timestamp(greq, con, cc);
      break;

    case GRAPHD_CC_TYPE:
      q = &con->con_type;
      name = "type";
      goto have_strconqueue;

    case GRAPHD_CC_VALUE:
      q = &con->con_value;
      name = "value";
      goto have_strconqueue;

    case GRAPHD_CC_VALTYPE:
      if (con->con_valuetype != GRAPH_DATA_UNSPECIFIED) {
        graphd_request_errprintf(greq, false,
                                 "SEMANTICS more than one valuetype");
        return GRAPHD_ERR_SEMANTICS;
      }
      con->con_valuetype = cc->cc_data.cd_valtype;
      break;

    case GRAPHD_CC_VALUECOMPARATOR:
      comparator_dest = &con->con_value_comparator;
      name = "value-comparator";
      goto have_comparator_dest;

    case GRAPHD_CC_META:
      if (con->con_meta != GRAPHD_META_UNSPECIFIED) {
        graphd_request_errprintf(greq, false,
                                 "SEMANTICS more than one meta-type");
        return GRAPHD_ERR_SEMANTICS;
      }
      con->con_meta = cc->cc_data.cd_meta;
      break;

    default:
      cl_notreached(cl,
                    "graphd_constraint_clause_merge: unexpected "
                    "cc->cc_type %d",
                    (int)cc->cc_type);
  }

  con->con_title = NULL;
  return 0;
}

int graphd_constraint_clause_merge_all(graphd_request *greq,
                                       graphd_constraint *con) {
  int err;
  graphd_constraint_clause *cc;

  /*  Merge the parsed subclauses into the semantic constraint
   */
  for (cc = con->con_cc_head; cc != NULL; cc = cc->cc_next) {
    err = graphd_constraint_clause_merge(greq, con, cc);
    if (err != 0) {
      graphd_request_errprintf(greq, 0, "SEMANTIC error merging subclause: %s",
                               graphd_strerror(err));
      return err;
    }
  }

  /*  Zero out the chain; the clauses themselves will be freed
   *  with the request heap.
   */
  con->con_cc_head = NULL;
  con->con_cc_tail = &con->con_cc_head;

  return 0;
}
