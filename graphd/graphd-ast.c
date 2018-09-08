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
#include "graphd/graphd-ast.h"
#include "graphd/graphd.h"

#include <ctype.h>
#include <errno.h>

#include "libcm/cm.h"
#include "libgdp/gdp.h"

static int graph_err_to_graphd(int err) {
  switch (err) {
    case GRAPH_ERR_LEXICAL:
      return GRAPHD_ERR_LEXICAL;
    case GRAPH_ERR_SEMANTICS:
      return GRAPHD_ERR_SEMANTICS;
    case GRAPH_ERR_NO:
      return GRAPHD_ERR_NO;
    default:
      break;
  }
  return err;
}

static int validate_conlist(gdp_output *out, graphd_constraint *gcon) {
  graphd_request *greq = out->out_private;
  graphd_semantic_constraint_complete_parse(greq, gcon);

  return 0;
}

static int validate_request(gdp_output *out) {
  graphd_request *greq = out->out_private;
  graphd_constraint *gcon = greq->greq_constraint;

  cl_assert(out->out_cl, greq->greq_request != GRAPHD_REQUEST_UNSPECIFIED);

  graphd_semantic_constraint_complete(greq, gcon);

  return 0;
}

static int ast_request_new(gdp_output *out, graphd_command kind,
                           gdp_modlist_t *modlist, gdp_conlist_t *conlist) {
  graphd_request *greq = out->out_private;
  graphd_constraint *gcon = conlist;
  int err;
  cl_handle *const cl = graphd_request_cl(greq);

  cl_assert(cl, greq->greq_request != GRAPHD_REQUEST_UNSPECIFIED);

  if (greq->greq_request == GRAPHD_REQUEST_ERROR) return 0;

  cl_assert(cl, greq->greq_request == kind);

  greq->greq_constraint = gcon;
  greq->greq_constraint_n = gcon ? 1 : 0;

  if (gcon) {
    if ((err = validate_conlist(out, gcon)) != 0) return err;
    if ((err = validate_request(out)) != 0) return err;
  }
  return 0;
}

static int ast_request_initialize(gdp_output *out, graphd_command kind) {
  graphd_request *greq = out->out_private;
  int err = 0;

  if (greq->greq_request == GRAPHD_REQUEST_UNSPECIFIED) {
    cl_assert(out->out_cl, kind != GRAPHD_REQUEST_UNSPECIFIED);
    err = graphd_request_become(greq, kind);
  }
  return err;
}

static int ast_request_new_dump(gdp_output *out, gdp_modlist_t *modlist,
                                unsigned long long start,
                                unsigned long long end,
                                unsigned long long pagesize) {
  graphd_request *greq = out->out_private;

  greq->greq_request = GRAPHD_REQUEST_DUMP;
  greq->greq_pagesize = pagesize;
  greq->greq_start = start;
  greq->greq_end = end;

  return 0;
}

static void ast_request_new_error(gdp_output *out, gdp_modlist_t *modlist,
                                  int err, char const *msg) {
  graphd_request *greq = out->out_private;
  graphd_request_error(greq, msg);
}

static int ast_request_new_replica(gdp_output *out, gdp_modlist_t *modlist,
                                   unsigned long long start_id,
                                   unsigned long long version,
                                   bool check_master) {
  graphd_request *greq = out->out_private;

  graphd_replica_initialize(greq);

  greq->greq_data.gd_replica.gdrep_start_id = start_id;
  greq->greq_data.gd_replica.gdrep_version = version;
  greq->greq_data.gd_replica.gdrep_master = check_master;

  return 0;
}

static int ast_request_new_replica_write(gdp_output *out,
                                         gdp_modlist_t *modlist,
                                         gdp_recordlist_t *records, size_t n,
                                         unsigned long long start,
                                         unsigned long long end) {
  graphd_request *greq = out->out_private;

  graphd_request_become(greq, GRAPHD_REQUEST_REPLICA_WRITE);

  greq->greq_restore_version = 6;
  greq->greq_restore_base = records;
  greq->greq_restore_n = n;
  greq->greq_start = start;
  greq->greq_end = end;

  return 0;
}

static int ast_request_new_response(gdp_output *out, gdp_modlist_t *modlist,
                                    bool ok) {
  graphd_request *greq = out->out_private;
  char buf[200];

  (void)modlist;

  switch (greq->greq_request) {
    case GRAPHD_REQUEST_SMP_FORWARD:
    case GRAPHD_REQUEST_SMP_OUT:
    case GRAPHD_REQUEST_REPLICA:
    case GRAPHD_REQUEST_WRITETHROUGH:
    case GRAPHD_REQUEST_CLIENT_READ:
    case GRAPHD_REQUEST_PASSTHROUGH:
      break;
    default:
      cl_notreached(graphd_request_cl(greq),
                    "ast_request_new_response: unexpected "
                    "source request %s (%d)",
                    graphd_request_to_string(greq, buf, sizeof buf),
                    greq->greq_request);
  }
  greq->greq_response_ok = ok;

  return 0;
}

static int ast_request_new_restore(gdp_output *out, gdp_modlist_t *modlist,
                                   gdp_recordlist_t *records, size_t n,
                                   unsigned char version,
                                   unsigned long long start,
                                   unsigned long long end) {
  graphd_request *greq = out->out_private;
  /* set `restore'-specific fields in the request */
  greq->greq_request = GRAPHD_REQUEST_RESTORE;

  greq->greq_restore_version = version;
  greq->greq_restore_base = records;
  greq->greq_restore_n = n;
  greq->greq_start = start;
  greq->greq_end = end;
  return 0;
}

/* Called after parsing a response to a "CLIENT_REPLICA" request.
 *
 *  If the request failed (the response was ERROR), the address is NULL.
 *  Otherwise, the response was OK, and version and address are the
 *  parsed components.
 */
static int ast_request_new_rok(gdp_output *out, gdp_modlist_t *modlist,
                               unsigned int version, gdp_token const *address) {
  graphd_request *greq = out->out_private;
  cl_handle *const cl = graphd_request_cl(greq);

  cl_assert(cl, greq->greq_request == GRAPHD_REQUEST_CLIENT_REPLICA);

  cl_log(cl, CL_LEVEL_VERBOSE, "ast_request_new_rok(address=%p)", address);

  if ((greq->greq_data.gd_client_replica.gdcrep_ok = (address != NULL))) {
    if (gdp_token_matches(address, "archive"))
      greq->greq_data.gd_client_replica.gdcrep_write_url_s =
          greq->greq_data.gd_client_replica.gdcrep_write_url_e = NULL;
    else {
      greq->greq_data.gd_client_replica.gdcrep_write_url_s = address->tkn_start;
      greq->greq_data.gd_client_replica.gdcrep_write_url_e = address->tkn_end;
    }
  }
  return 0;
}

static int ast_request_new_set(gdp_output *out, gdp_modlist_t *modlist,
                               gdp_proplist_t *proplist) {
  graphd_request *greq = out->out_private;
  greq->greq_request = GRAPHD_REQUEST_SET;
  return 0;
}

static int ast_request_new_smp(gdp_output *out, gdp_modlist_t *modlist,
                               gdp_smpcmd_t *smpcmd) {
  graphd_request *greq = out->out_private;
  greq->greq_request = GRAPHD_REQUEST_SMP;

  return 0;
}

static int ast_request_new_status(gdp_output *out, gdp_modlist_t *modlist,
                                  gdp_statlist_t *statlist) {
  (void)out;
  (void)modlist;
  (void)statlist;

  return 0;
}

static int ast_request_new_verify(gdp_output *out, gdp_modlist_t *modlist,
                                  graph_guid const *low, graph_guid const *high,
                                  unsigned long long pagesize) {
  graphd_request *greq = out->out_private;
  int err;

  greq->greq_request = GRAPHD_REQUEST_VERIFY;
  greq->greq_verifyquery = (graphd_verify_query){.verify_guid_low = *low,
                                                 .verify_guid_high = *high,
                                                 .verify_pagesize = pagesize};

  /* setup verify request */
  if ((err = graphd_verify_setup(greq)))
    graphd_request_errprintf(greq, 0, "SYSTEM %s", graphd_strerror(err));

  return err;
}

static int ast_modlist_new(gdp_output *out, gdp_modlist_t **modlist) {
  /* NOTE: Nothing to do: the request modifiers are declared statically
   * in the graphd_request structure */
  *modlist = NULL;
  return 0;
}

static int ast_modlist_add_asof(gdp_output *out, gdp_modlist_t *modlist,
                                gdp_token const *tok) {
  graph_dateline *asof = NULL;
  graphd_value *asof_value = NULL;
  char const *s = tok->tkn_start;
  char const *e = tok->tkn_end;
  graphd_request *greq = out->out_private;
  graph_guid guid;
  graph_timestamp_t timestamp;
  int err = 0;

  /* [ adapted from graphd-parse.c, parse_request_modifier_asof() ] */

  if ((asof = graph_dateline_create(out->out_cm)) == NULL) goto fail_nomem;

  switch (tok->tkn_kind) {
    case TOK_ATOM:
      /* maybe a GUID value? */
      if (!graph_guid_from_string(&guid, s, e)) {
        /* if the caller supplies an ID, use that ID plus one
         * as the "odometer reading" for the access */
        const uint64_t dbid = GRAPH_GUID_DB(guid);
        const uint64_t count = GRAPH_GUID_SERIAL(guid) + 1;
        if ((err = graph_dateline_add(&asof, dbid, count, NULL)) != 0)
          goto fail_asof;
      }
      /* maybe a timestamp value? */
      else if ((err = graph_timestamp_from_string(&timestamp, s, e)) != 0)
        goto fail_asof;
      else {
        asof_value = cm_malloc(out->out_cm, sizeof(graphd_value));
        if (asof_value == NULL) goto fail_nomem;

        graphd_value_timestamp_set(asof_value, timestamp, PDB_ID_NONE);

        /* graphd_read() will evaluate this one for us, given a
         * non-NULL asof_value...
         */
      }
      break;
    case TOK_STR:
      if ((err = graph_dateline_from_string(&asof, s, e)) != 0) goto fail_asof;
      break;
    default:
      gdp_bug(out->out_cl);
  }

  greq->greq_asof = asof;
  greq->greq_asof_value = asof_value;
  return 0;

fail_asof:
  graph_dateline_destroy(asof);
  return graph_err_to_graphd(err);

fail_nomem:
  cl_log(out->out_cl, CL_LEVEL_ERROR, "insufficient memory");
  return ENOMEM;
}

static int ast_modlist_add_cost(gdp_output *out, gdp_modlist_t *modlist,
                                graphd_operator op, gdp_token const *tok) {
  graphd_request *greq = out->out_private;
  graphd_request_parameter *par;

  /* [ adapted from graphd-parse.c, parse_request_modifier_cost() ] */

  if (op == GRAPHD_OP_MATCH) greq->greq_soft_timeout = true;

  par = graphd_request_parameter_append(greq, graphd_format_request_cost,
                                        sizeof(*par));
  if (par == NULL) {
    cl_log(out->out_cl, CL_LEVEL_ERROR, "insufficient memory");
    return ENOMEM;
  }

  graphd_cost_parse(greq, tok, &greq->greq_runtime_statistics_allowance);
  if (greq->greq_error_message != NULL) return GRAPHD_ERR_SEMANTICS;

  return 0;
}

static int ast_modlist_add_dateline(gdp_output *out, gdp_modlist_t *modlist,
                                    gdp_token const *tok) {
  graphd_request *greq = out->out_private;
  graphd_request_parameter *par;

  /*
   * NOTE: Adapted from graphd-parse.c, parse_request_modifier_dateline()
   */
  cl_handle *cl = graphd_request_cl(greq);
  cl_log(cl, CL_LEVEL_VERBOSE, "ast_modlist_add_dateline %p", (void *)greq);

  par = graphd_request_parameter_append(greq, graphd_format_request_dateline,
                                        sizeof(*par));
  if (par == NULL) {
    cl_log(out->out_cl, CL_LEVEL_ERROR, "insufficient memory");
    return ENOMEM;
  }

  greq->greq_dateline_wanted = true;

  if (gdp_token_len(tok) == 0)
    greq->greq_dateline = NULL;
  else {
    /* the caller did not just ask for a dateline to be returned,
     * they're also asking that we be at least _this_ caught up
     */

    graph_dateline *dl;
    int err;

    if ((dl = graph_dateline_create(out->out_cm)) == NULL) return ENOMEM;

    if ((err = graph_dateline_from_string(&dl, tok->tkn_start, tok->tkn_end))) {
      if (err == GRAPH_ERR_SEMANTICS)
        err = GRAPHD_ERR_SEMANTICS;
      else if (err == GRAPH_ERR_LEXICAL)
        err = GRAPHD_ERR_LEXICAL;
      return err;
    }
    greq->greq_dateline = dl;
  }

  return 0;
}

static int ast_modlist_add_id(gdp_output *out, gdp_modlist_t *modlist,
                              gdp_token const *tok) {
  graphd_request *greq = out->out_private;
  graphd_request_parameter *par;
  graphd_request_parameter_id *id;
  const char *s;
  const char *e;
  size_t len;

  s = tok->tkn_start;
  e = tok->tkn_end;
  len = e - s;

  /*
   * NOTE: Adapted from graphd-parse.c, parse_request_modifier_id()
   */

  par = graphd_request_parameter_append(greq, graphd_format_request_id,
                                        sizeof(*id) + 1 + len);
  if (par == NULL) {
    cl_log(out->out_cl, CL_LEVEL_ERROR, "insufficient memory");
    return ENOMEM;
  }

  id = (graphd_request_parameter_id *)par;
  id->id_s = (char *)(id + 1);
  id->id_e = (char *)(id->id_s + len);
  memcpy(id->id_s, s, len);
  id->id_s[len] = '\0';

  /* set a pointer to the ID; logging code will use it */
  greq->greq_req.req_display_id = id->id_s;

  return 0;
}

static int ast_modlist_add_heatmap(gdp_output *out, gdp_modlist_t *modlist,
                                   gdp_token const *tok) {
  graphd_request *greq = out->out_private;
  graphd_request_parameter *par;
  graphd_request_parameter_heatmap *heatmap;
  const char *s;
  const char *e;
  size_t len;

  s = tok->tkn_start;
  e = tok->tkn_end;
  len = e - s;

  par = graphd_request_parameter_append(greq, graphd_format_request_heatmap,
                                        sizeof(*heatmap) + 1 + len);
  if (par == NULL) {
    cl_log(out->out_cl, CL_LEVEL_ERROR, "insufficient memory");
    return ENOMEM;
  }

  heatmap = (graphd_request_parameter_heatmap *)par;
  heatmap->hm_s = (char *)(heatmap + 1);
  heatmap->hm_e = (char *)(heatmap->hm_s + len);
  memcpy(heatmap->hm_s, s, len);
  heatmap->hm_s[len] = '\0';

  greq->greq_heatmap = true;

  return 0;
}

static int ast_modlist_add_loglevel(gdp_output *out, gdp_modlist_t *modlist,
                                    gdp_token const *tok) {
  graphd_request *greq = out->out_private;
  const char *s;
  const char *e;

  s = tok->tkn_start;
  e = tok->tkn_end;

  // NOTE: Adapted from parse_request_modifier_loglevel()

  cl_loglevel_configuration clc;
  if (cl_loglevel_configuration_from_string(s, e, graphd_facilities, &clc))
    return GRAPHD_ERR_SYNTAX;
  cl_loglevel_configuration_max(&clc, &greq->greq_loglevel,
                                &greq->greq_loglevel);
  greq->greq_loglevel_valid = true;

  return 0;
}

static int ast_modlist_add_timeout(gdp_output *out, gdp_modlist_t *modlist,
                                   unsigned long long timeout) {
  graphd_request *greq = out->out_private;
  greq->greq_timeout = timeout;
  return 0;
}

static int ast_conlist_new(gdp_output *out, gdp_conlist_t **conlst) {
  graphd_request *greq = out->out_private;
  graphd_handle *g = graphd_request_graphd(greq);
  graphd_constraint *gcon;

  if ((gcon = cm_malloc(out->out_cm, sizeof(*gcon))) == NULL) return ENOMEM;
  graphd_constraint_initialize(g, gcon);
  *conlst = gcon;

  return 0;
}

static int ast_conlist_new_sortcomparator(gdp_output *out,
                                          gdp_conlist_t *where) {
  graphd_constraint *con = where;
  graphd_request *greq = out->out_private;

  if (con->con_sort_comparators.gcl_used) {
    graphd_request_errprintf(greq, 0,
                             "SEMANTICS only one"
                             " sort-comparator clause per constraint");
    return GRAPHD_ERR_SEMANTICS;
  } else {
    con->con_sort_comparators.gcl_used = true;
    return 0;
  }
}

static int ast_conlist_add_sortcomparator(gdp_output *out, gdp_conlist_t *where,
                                          gdp_token *cname) {
  graphd_request *greq = out->out_private;
  graphd_comparator const *cmp;
  graphd_constraint *con = where;
  graphd_comparator_list *gcl;

  /*  We're just using this as a temporary.
   *  Once we're done parsing this list of
   *  sortcomparators, it'll turn into a
   *  constraint_clause.
   */
  gcl = &con->con_sort_comparators;

  cmp = graphd_comparator_from_string(cname->tkn_start, cname->tkn_end);

  if (cmp == NULL) {
    graphd_request_errprintf(greq, 0,
                             "SEMANTICS '%.*s' is not a"
                             "comparator",
                             (int)(cname->tkn_end - cname->tkn_start),
                             cname->tkn_start);

    return GRAPHD_ERR_SEMANTICS;
  }
  if (gcl->gcl_n == gcl->gcl_m) {
    if (gcl->gcl_m == 0)
      gcl->gcl_m = 1;
    else
      gcl->gcl_m *= 2;

    gcl->gcl_comp = cm_realloc(out->out_cm, gcl->gcl_comp,
                               sizeof(graphd_comparator *) * gcl->gcl_m);
    if (gcl->gcl_comp == NULL) return ENOMEM;
  }
  cl_assert(out->out_cl, gcl->gcl_n < gcl->gcl_m);

  gcl->gcl_comp[gcl->gcl_n] = cmp;
  gcl->gcl_n++;

  return 0;
}

static int ast_conlist_add_value_comparator(
    gdp_output *out, gdp_conlist_t *where,
    gdp_token const *name  // comparator name
    ) {
  graphd_constraint *gcon = where;
  graphd_comparator const *cmp;
  char const *s;
  char const *e;

  s = name->tkn_start;
  e = name->tkn_end;
  if ((cmp = graphd_comparator_from_string(s, e)) == NULL)
    return GRAPHD_ERR_SEMANTICS;

  if (gcon->con_value_comparator != NULL &&
      gcon->con_value_comparator != graphd_comparator_unspecified)
    return EBUSY;

  gcon->con_value_comparator = cmp;
  return 0;
}

static int ast_conlist_add_comparator(gdp_output *out, gdp_conlist_t *where,
                                      gdp_token const *name  // comparator name
                                      ) {
  graphd_constraint_clause *cc;
  graphd_request *greq = out->out_private;
  graphd_constraint *gcon = where;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_COMPARATOR);
  if (cc == NULL) return ENOMEM;

  cc->cc_data.cd_comparator =
      graphd_comparator_from_string(name->tkn_start, name->tkn_end);
  if (cc->cc_data.cd_comparator == NULL) {
    graphd_request_errprintf(greq, false,
                             "SYNTAX "
                             "unknown comparator \"%.*s\"",
                             (int)(name->tkn_end - name->tkn_start),
                             name->tkn_start);
    return GRAPHD_ERR_SEMANTICS;
  }

  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_conlist_add_count(gdp_output *out, gdp_conlist_t *where,
                                 graphd_operator op, unsigned long long val) {
  graphd_constraint *gcon = where;
  graphd_request *greq = out->out_private;
  graphd_constraint_clause *cc;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_COUNT);
  if (cc == NULL) return ENOMEM;

  cc->cc_data.cd_count.count_value = val;
  cc->cc_data.cd_count.count_op = op;

  graphd_constraint_clause_append(gcon, cc);

  return 0;
}

static int ast_conlist_add_cursor(gdp_output *out, gdp_conlist_t *where,
                                  gdp_token const *value  // a string, or NULL
                                  ) {
  graphd_constraint *gcon = where;
  graphd_request *greq = out->out_private;
  graphd_constraint_clause *cc;

  if (value->tkn_start == NULL) return 0;

  cc = graphd_constraint_clause_alloc_cursor(greq, value->tkn_start,
                                             value->tkn_end);
  if (cc == NULL) return ENOMEM;

  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_conlist_add_dateline(gdp_output *out, gdp_conlist_t *where,
                                    graphd_operator op,     // `<' or `>'
                                    gdp_token const *value  // a string or atom
                                    ) {
  graphd_constraint *gcon = where;
  graphd_request *greq = out->out_private;
  graphd_constraint_clause *cc;
  graph_dateline *dl = NULL;
  int err;

  if ((dl = graph_dateline_create(out->out_cm)) == NULL) return ENOMEM;

  err = graph_dateline_from_string(&dl, value->tkn_start, value->tkn_end);
  if (err != 0) {
    graphd_request_errprintf(greq, false,
                             "SYNTAX "
                             "invalid dateline \"%.*s\"",
                             (int)(value->tkn_end - value->tkn_start),
                             value->tkn_start);
    return err;
  }

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_DATELINE);
  if (cc == NULL) {
    graph_dateline_destroy(dl);
    return ENOMEM;
  }

  cc->cc_data.cd_dateline.dateline_op = op;
  cc->cc_data.cd_dateline.dateline_value = dl;

  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_conlist_add_false(gdp_output *out, gdp_conlist_t *where) {
  graphd_constraint *gcon = where;
  graphd_request *greq = out->out_private;
  graphd_constraint_clause *cc;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_FALSE);
  if (cc == NULL) return ENOMEM;

  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_conlist_add_flag(
    gdp_output *out, gdp_conlist_t *where,
    gdp_token const *name,      // flag name ("live" or "archival")
    graphd_flag_constraint val  // flag value
    ) {
  graphd_constraint *gcon = where;
  graphd_request *greq = out->out_private;
  graphd_constraint_clause *cc;
  graphd_constraint_clause_type which;

  switch (tolower(name->tkn_start[0])) {
    case 'a':
      if (name->tkn_start[1] == 'n') /* anchor */
        which = GRAPHD_CC_ANCHOR;
      else
        /* archival */
        which = GRAPHD_CC_ARCHIVAL;
      break;

    case 'l': /* live */
      which = GRAPHD_CC_LIVE;
      break;

    default:
      cl_notreached(out->out_cl, "unexpected flag \"%.*s\"",
                    (int)(name->tkn_end - name->tkn_start), name->tkn_start);
      /* NOTREACHED */
      return GRAPHD_ERR_SEMANTICS;
  }
  cc = graphd_constraint_clause_alloc(greq, which);
  if (cc == NULL) return ENOMEM;
  cc->cc_data.cd_flag = val;

  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_conlist_add_gen(gdp_output *out, gdp_conlist_t *where,
                               gdp_gencon_kind kind, graphd_operator op,
                               unsigned long long ull) {
  graphd_constraint *gcon = where;
  graphd_request *greq = out->out_private;
  graphd_constraint_clause *cc;
  graphd_constraint_clause_type which;

  switch (kind) {
    case GDP_GENCON_NEWEST:
      which = GRAPHD_CC_NEWEST;
      break;
    case GDP_GENCON_OLDEST:
      which = GRAPHD_CC_OLDEST;
      break;
    default:
      cl_notreached(out->out_cl, "ast_conlist_add_gen: unexpected kind %d",
                    kind);
      return GRAPHD_ERR_SEMANTICS;
  }

  cc = graphd_constraint_clause_alloc(greq, which);
  if (cc == NULL) return ENOMEM;

  cc->cc_data.cd_gencon.gencon_value = ull;
  cc->cc_data.cd_gencon.gencon_op = op;

  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_conlist_add_guid(
    gdp_output *out,
    gdp_conlist_t *where,   // current constraint list
    gdp_guidcon_kind kind,  // this, next, prev
    graphd_operator op,     // =, !=, ~=
    gdp_guidset_t *guidset  // constraint values
    ) {
  graphd_request *greq = out->out_private;
  graphd_constraint *gcon = where;
  graphd_constraint_clause *cc;
  graphd_constraint_clause_type which;

  switch (kind) {
    case GDP_GUIDCON_THIS:
      which = GRAPHD_CC_GUID;
      break;
    case GDP_GUIDCON_NEXT:
      which = GRAPHD_CC_NEXT;
      break;
    case GDP_GUIDCON_PREV:
      which = GRAPHD_CC_PREV;
      break;
    default:
      cl_notreached(out->out_cl, "ast_conlist_add_guid: unexpected kind %d",
                    (int)kind);
      return GRAPHD_ERR_SEMANTICS;
  }

  cc = graphd_constraint_clause_alloc(greq, which);
  if (cc == NULL) return ENOMEM;

  cc->cc_data.cd_guidcon.guidcon_linkage = PDB_LINKAGE_N;
  cc->cc_data.cd_guidcon.guidcon_set = guidset;
  cc->cc_data.cd_guidcon.guidcon_op = op;

  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_conlist_add_key(gdp_output *out, gdp_conlist_t *where,
                               gdp_pattern_t *pat) {
  graphd_constraint *gcon = where;
  graphd_pattern *gpat = pat;
  int err;

  switch (gpat->pat_type) {
    case GRAPHD_PATTERN_DATATYPE:
    case GRAPHD_PATTERN_LEFT:
    case GRAPHD_PATTERN_NAME:
    case GRAPHD_PATTERN_RIGHT:
    case GRAPHD_PATTERN_SCOPE:
    case GRAPHD_PATTERN_TIMESTAMP:
    case GRAPHD_PATTERN_TYPEGUID:
    case GRAPHD_PATTERN_VALUE:
      gcon->con_key |= 1 << gpat->pat_type;
      break;
    case GRAPHD_PATTERN_LIST:
      if (gpat->pat_parent != NULL)
        return GRAPHD_ERR_SYNTAX;  // cannot nest list within lists
      gcon->con_key |= 1 << GRAPHD_PATTERN_LIST;
      /* iterate through the pattern elements */
      gpat = gpat->pat_data.data_list.list_head;
      while (gpat != NULL) {
        if ((err = ast_conlist_add_key(out, where, gpat))) return err;
        gpat = gpat->pat_next;
      }
      break;
    default:
      return GRAPHD_ERR_SEMANTICS;  // invalid key
  }

  return 0;
}

static int ast_conlist_add_linkage(
    gdp_output *out,
    gdp_conlist_t *where,   // current constraint list
    unsigned int linkage,   // left, right, type, or scope
    graphd_operator op,     // =, !=, ~=
    gdp_guidset_t *guidset  // constraint values
    ) {
  graphd_request *greq = out->out_private;
  graphd_constraint *gcon = where;
  graphd_constraint_clause *cc;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_GUIDLINK);
  if (cc == NULL) return ENOMEM;

  cc->cc_data.cd_guidcon.guidcon_linkage = linkage;
  cc->cc_data.cd_guidcon.guidcon_set = guidset;
  cc->cc_data.cd_guidcon.guidcon_op = op;

  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_conlist_add_resultpagesize(gdp_output *out, gdp_conlist_t *where,
                                          size_t size) {
  graphd_request *greq = out->out_private;
  graphd_constraint *gcon = where;
  graphd_constraint_clause *cc;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_RESULTPAGESIZE);
  if (cc == NULL) return ENOMEM;

  cc->cc_data.cd_limit = size;

  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_conlist_add_countlimit(gdp_output *out, gdp_conlist_t *where,
                                      size_t size) {
  graphd_request *greq = out->out_private;
  graphd_constraint *gcon = where;
  graphd_constraint_clause *cc;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_COUNTLIMIT);
  if (cc == NULL) return ENOMEM;

  cc->cc_data.cd_limit = size;

  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_conlist_add_pagesize(gdp_output *out, gdp_conlist_t *where,
                                    size_t size) {
  graphd_request *greq = out->out_private;
  graphd_constraint *gcon = where;
  graphd_constraint_clause *cc;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_PAGESIZE);
  if (cc == NULL) return ENOMEM;

  cc->cc_data.cd_limit = size;

  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_conlist_add_result(gdp_output *out, gdp_conlist_t *where,
                                  gdp_pattern_t *pat) {
  graphd_request *greq = out->out_private;
  graphd_constraint *gcon = where;
  graphd_constraint_clause *cc;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_RESULT);
  if (cc == NULL) return ENOMEM;

  cc->cc_data.cd_pattern = (graphd_pattern *)pat;

  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_conlist_add_sort(gdp_output *out, gdp_conlist_t *where,
                                gdp_pattern_t *pat) {
  graphd_request *greq = out->out_private;
  graphd_constraint *gcon = where;
  graphd_constraint_clause *cc;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_SORT);
  if (cc == NULL) return ENOMEM;

  cc->cc_data.cd_pattern = (graphd_pattern *)pat;

  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_conlist_add_start(gdp_output *out, gdp_conlist_t *where,
                                 size_t offset) {
  graphd_request *greq = out->out_private;
  graphd_constraint *gcon = where;
  graphd_constraint_clause *cc;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_START);
  if (cc == NULL) return ENOMEM;

  cc->cc_data.cd_start = offset;
  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_conlist_add_string(
    gdp_output *out,
    gdp_conlist_t *where,  // con list
    gdp_token *name,       // con name (value, name, or type)
    graphd_operator op,    // con operator
    gdp_strset_t *values,  // string values
    bool allow_multi       // allow multiple values
    ) {
  graphd_request *greq = out->out_private;
  graphd_constraint *gcon = where;
  graphd_string_constraint *strcon = values;

  graphd_constraint_clause *cc;
  graphd_constraint_clause_type type;

  switch (tolower(*name->tkn_start)) {
    case 'n':
      type = GRAPHD_CC_NAME;
      break;
    case 't':
      type = GRAPHD_CC_TYPE;
      break;
    case 'v':
      type = GRAPHD_CC_VALUE;
      break;
    default:
      gdp_bug(out->out_cl);
  }

  cc = graphd_constraint_clause_alloc(greq, type);
  if (cc == NULL) return ENOMEM;

  cc->cc_data.cd_strcon = strcon;
  strcon->strcon_op = op;

  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_conlist_add_sublist(gdp_output *out, gdp_conlist_t *where,
                                   gdp_conlist_t *list) {
  graphd_request *greq = out->out_private;
  graphd_constraint *parent = where;
  graphd_constraint *gcon = list;
  graphd_constraint_clause *cc;
  int err;

  gcon->con_parent = where;

  err = validate_conlist(out, gcon);
  if (err != 0) return err;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_SUBCON);
  if (cc == NULL) return ENOMEM;

  cc->cc_data.cd_subcon = gcon;
  graphd_constraint_clause_append(parent, cc);

  return 0;
}

static int clause_flip_constraint(graphd_request *greq,
                                  graphd_constraint_clause *cc,
                                  graphd_constraint *old_con,
                                  graphd_constraint *new_con) {
  int err = 0;

  /*  Flip the loyalty of the declarations below <cc> to <con>,
   *  instead of the temporary constraint they were allocated with.
   */
  switch (cc->cc_type) {
    case GRAPHD_CC_SEQUENCE:
      for (cc = cc->cc_data.cd_sequence; cc != NULL; cc = cc->cc_next) {
        err = clause_flip_constraint(greq, cc, old_con, new_con);
        if (err != 0) return err;
      }
      break;

    case GRAPHD_CC_ASSIGNMENT:
      err = graphd_pattern_move_declaration_target(
          greq, cc->cc_data.cd_assignment.asn_pattern, old_con, new_con);
      break;

    case GRAPHD_CC_RESULT:
    case GRAPHD_CC_SORT:
      err = graphd_pattern_move_declaration_target(greq, cc->cc_data.cd_pattern,
                                                   old_con, new_con);
      break;

    default:
      break;
  }
  return err;
}

static int ast_conlist_add_sequence(gdp_output *out, gdp_conlist_t *where,
                                    gdp_conlist_t *input) {
  graphd_constraint *gcon = where;
  graphd_constraint *sub = input;
  graphd_request *greq = out->out_private;
  graphd_constraint_clause *cc;
  int err;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_SEQUENCE);

  if (cc == NULL) return ENOMEM;
  if (sub == NULL) {
    // This is intended to be an empty sublist.
    cc->cc_data.cd_sequence = NULL;
    return 0;
  }

  cc->cc_data.cd_sequence = sub->con_cc_head;
  sub->con_cc_head = NULL;
  sub->con_cc_tail = &sub->con_cc_head;

  graphd_constraint_clause_append(gcon, cc);
  for (cc = cc->cc_data.cd_sequence; cc != NULL; cc = cc->cc_next) {
    err = clause_flip_constraint(greq, cc, sub, gcon);
    if (err != 0) return err;
  }
  return 0;
}

static int ast_conlist_add_timestamp(gdp_output *out, gdp_conlist_t *where,
                                     graphd_operator op, graph_timestamp_t ts) {
  graphd_constraint *gcon = where;
  graphd_request *greq = out->out_private;
  graphd_constraint_clause *cc;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_TIMESTAMP);
  if (cc == NULL) return ENOMEM;

  cc->cc_data.cd_timestamp.timestamp_op = op;
  cc->cc_data.cd_timestamp.timestamp_value = ts;

  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_conlist_add_unique(gdp_output *out, gdp_conlist_t *where,
                                  gdp_pattern_t *pat) {
  graphd_constraint *gcon = where;
  graphd_pattern *gpat = pat;
  int err;

  /*  Since key and unique patterns are local
   *  to write requests only, this does not
   *  use the constraint-clause mechanism.
   */

  switch (gpat->pat_type) {
    case GRAPHD_PATTERN_DATATYPE:
    case GRAPHD_PATTERN_LEFT:
    case GRAPHD_PATTERN_NAME:
    case GRAPHD_PATTERN_RIGHT:
    case GRAPHD_PATTERN_SCOPE:
    case GRAPHD_PATTERN_TIMESTAMP:
    case GRAPHD_PATTERN_TYPEGUID:
    case GRAPHD_PATTERN_VALUE:
      gcon->con_unique |= 1 << gpat->pat_type;
      break;
    case GRAPHD_PATTERN_LIST:
      if (gpat->pat_parent != NULL)
        return GRAPHD_ERR_SYNTAX;  // cannot nest list within lists
      gcon->con_unique |= 1 << GRAPHD_PATTERN_LIST;
      /* iterate through the pattern elements */
      gpat = gpat->pat_data.data_list.list_head;
      while (gpat != NULL) {
        if ((err = ast_conlist_add_unique(out, where, gpat))) return err;
        gpat = gpat->pat_next;
      }
      break;
    default:
      return GRAPHD_ERR_SYNTAX;
  }

  return 0;
}

static int ast_conlist_add_valuetype(gdp_output *out, gdp_conlist_t *where,
                                     graph_datatype valuetype) {
  graphd_constraint *gcon = where;
  graphd_request *greq = out->out_private;
  graphd_constraint_clause *cc;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_VALTYPE);
  if (cc == NULL) return ENOMEM;
  cc->cc_data.cd_valtype = valuetype;
  graphd_constraint_clause_append(gcon, cc);

  return 0;
}

static int ast_conlist_add_variable(
    gdp_output *out,
    gdp_conlist_t *where,  // the scope of the variable
    gdp_token const *var,  // token containing the variable name
    gdp_pattern_t *pat     // pattern to be assigned to the var
    ) {
  graphd_request *greq = out->out_private;
  graphd_constraint *gcon = where;
  graphd_constraint_clause *cc;

  cc = graphd_constraint_clause_alloc_assignment(greq, var->tkn_start,
                                                 var->tkn_end, pat);
  if (cc == NULL) return ENOMEM;

  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static bool cc_okay_in_or(graphd_request *greq,
                          graphd_constraint_clause const *cc) {
  char const *unwanted = NULL;
  if (cc == NULL) return true;

  switch (cc->cc_type) {
    case GRAPHD_CC_SEQUENCE:
      for (cc = cc->cc_data.cd_sequence; cc != NULL; cc = cc->cc_next)
        if (!cc_okay_in_or(greq, cc)) return false;
      return true;

    case GRAPHD_CC_COMPARATOR:
      unwanted = "set-level comparator";
      break;
    case GRAPHD_CC_COUNT:
      unwanted = "set-level count";
      break;
    case GRAPHD_CC_COUNTLIMIT:
      unwanted = "set-level countlimit";
      break;
    case GRAPHD_CC_CURSOR:
      unwanted = "set-level cursor";
      break;
    case GRAPHD_CC_LINKAGE:
      unwanted = "linkage";
      break;
    case GRAPHD_CC_PAGESIZE:
      unwanted = "set-level pagesize";
      break;
    case GRAPHD_CC_RESULT:
      unwanted = "set-level result";
      break;
    case GRAPHD_CC_RESULTPAGESIZE:
      unwanted = "resultpagesize=...";
      break;
    case GRAPHD_CC_SORT:
      unwanted = "sort=...";
      break;
    case GRAPHD_CC_SORTCOMPARATOR:
      unwanted = "sortcomparator=...";
      break;
    case GRAPHD_CC_START:
      unwanted = "start=...";
      break;
    case GRAPHD_CC_VALUECOMPARATOR:
      unwanted = "valuecomparator=...";
      break;
    default:
      return true;
  }

  graphd_request_errprintf(
      greq, 0, "SEMANTICS can't have %s inside a primitive-level OR-branch",
      unwanted);
  return false;
}

static int ast_conlist_add_or(gdp_output *out,
                              gdp_conlist_t *where,  // container + lhs
                              gdp_conlist_t *rhs,    // rhs
                              bool short_circuit     // || vs. |
                              ) {
  graphd_request *greq = out->out_private;
  graphd_constraint *con = where;
  graphd_constraint_clause *cc_or, *cc_lhs;
  graphd_constraint_clause **cc_prev;
  graphd_constraint_or *cor;
  int err;

  if (con->con_cc_head == NULL)

    /*  Nothing or nothing -> nothing.
     */
    return 0;

  /*  Set cc_prev to the address of the pointer
   *  to the last element.
   */
  for (cc_prev = &con->con_cc_head; (*cc_prev)->cc_next != NULL;
       cc_prev = &(*cc_prev)->cc_next)
    ;

  cc_lhs = *cc_prev;

  if (!cc_okay_in_or(greq, cc_lhs) || !cc_okay_in_or(greq, rhs))
    return GRAPHD_ERR_SEMANTICS;

  /*  Make an "or" whose first part ("head")
   *  is cc_prev, and whose other half ("tail")
   *  is rhs.
   *
   *  The "or" is not yet active in the sense that
   *  it doesn't show up in its parent's or chain;
   *  it'll take a constraint_clause merge to do
   *  that.
   */
  cc_or = graphd_constraint_clause_alloc(
      greq, short_circuit ? GRAPHD_CC_LOR : GRAPHD_CC_BOR);
  if (cc_or == NULL) return ENOMEM;

  cc_or->cc_data.cd_or = cor =
      graphd_constraint_or_create(greq, con, short_circuit);
  if (cor == NULL) return ENOMEM;
  cor->or_short_circuit = short_circuit;
  cor->or_tail = rhs;

  /*  Replace cc_lhs with cc_or.
   */
  *cc_prev = cc_or;
  con->con_cc_tail = &cc_or->cc_next;

  err = graphd_constraint_clause_merge(greq, &cor->or_head, cc_lhs);
  if (err != 0) return err;

  err = graphd_constraint_clause_merge_all(greq, rhs);
  if (err != 0) return err;

  return 0;
}

static bool ast_conlist_has_meta(gdp_output *out, gdp_conlist_t *list) {
  graphd_constraint const *gcon = list;

  if (gcon->con_meta == GRAPHD_META_UNSPECIFIED) {
    graphd_constraint_clause const *cc;
    for (cc = gcon->con_cc_head; cc != NULL; cc = cc->cc_next)
      if (cc->cc_type == GRAPHD_CC_META) return true;
  }
  return false;
}

static gdp_meta ast_conlist_get_meta(gdp_output *out,
                                     gdp_conlist_t const *list) {
  graphd_constraint const *gcon = list;

  if (gcon->con_meta == GRAPHD_META_UNSPECIFIED) {
    graphd_constraint_clause const *cc;
    for (cc = gcon->con_cc_head; cc != NULL; cc = cc->cc_next)
      if (cc->cc_type == GRAPHD_CC_META) return cc->cc_data.cd_meta;
  }
  return gcon->con_meta;
}

static int ast_conlist_set_meta(gdp_output *out, gdp_conlist_t *list,
                                gdp_meta meta) {
  graphd_request *greq = out->out_private;
  graphd_constraint *gcon = list;
  graphd_constraint_clause *cc;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_META);
  if (cc == NULL) return ENOMEM;

  cc->cc_data.cd_meta = meta;
  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static bool ast_conlist_has_linkage(gdp_output *out, gdp_conlist_t *list) {
  graphd_constraint const *gcon = list;
  graphd_constraint_clause const *cc;

  for (cc = gcon->con_cc_head; cc != NULL; cc = cc->cc_next)
    if (cc->cc_type == GRAPHD_CC_LINKAGE) return true;

  return false;
}

static int ast_conlist_set_linkage(gdp_output *out, gdp_conlist_t *list,
                                   unsigned int linkage) {
  graphd_request *greq = out->out_private;
  graphd_constraint *gcon = list;
  graphd_constraint_clause *cc;

  cc = graphd_constraint_clause_alloc(greq, GRAPHD_CC_LINKAGE);
  if (cc == NULL) return ENOMEM;

  cc->cc_data.cd_linkage = linkage;
  graphd_constraint_clause_append(gcon, cc);
  return 0;
}

static int ast_recordlist_new(gdp_output *out, size_t n,
                              gdp_recordlist_t **records) {
  gdp_record *rs;

  const size_t size = sizeof(gdp_record) * n;
  if ((rs = cm_malloc(out->out_cm, size)) == NULL) {
    cl_log(out->out_cl, CL_LEVEL_ERROR, "insufficient memory");
    return ENOMEM;
  }

  memset(rs, 0, size);

  *records = rs;
  return 0;
}

static int ast_recordlist_set(gdp_output *out, unsigned int version,
                              gdp_recordlist_t *records, unsigned int index,
                              gdp_record const *rec) {
  gdp_record *greq_recordset = records;

  greq_recordset[index] = *rec;

  return 0;
}

static int ast_smpcmd_new(gdp_output *out, gdp_smpcmd_t **smpcmd,
                          unsigned long long **smppid) {
  graphd_request *greq = out->out_private;

  *smpcmd = &greq->greq_data.gd_smp.gds_smpcmd;
  *smppid = &greq->greq_data.gd_smp.gds_smppid;

  return 0;
}

/*  Translate the token into a GRAPHD_SMP_.... command,
 *  and assign it to the passed-in SMP command output
 *  structure.
 */
static int ast_smpcmd_set(gdp_output *out, gdp_smpcmd_t *smpcmd,
                          gdp_token const *tok) {
  if (gdp_token_matches(tok, "post-write")) {
    *(graphd_smp_command *)smpcmd = GRAPHD_SMP_POSTWRITE;
  } else if (gdp_token_matches(tok, "pre-write")) {
    *(graphd_smp_command *)smpcmd = GRAPHD_SMP_PREWRITE;
  } else if (gdp_token_matches(tok, "connect")) {
    *(graphd_smp_command *)smpcmd = GRAPHD_SMP_CONNECT;
  } else if (gdp_token_matches(tok, "paused")) {
    *(graphd_smp_command *)smpcmd = GRAPHD_SMP_PAUSED;
  } else if (gdp_token_matches(tok, "running")) {
    *(graphd_smp_command *)smpcmd = GRAPHD_SMP_RUNNING;
  } else if (gdp_token_matches(tok, "status")) {
    *(graphd_smp_command *)smpcmd = GRAPHD_SMP_STATUS;
  } else
    return GRAPHD_ERR_LEXICAL;

  return 0;
}

static int ast_proplist_new(gdp_output *out, gdp_proplist_t **proplist) {
  graphd_request *greq = out->out_private;
  *proplist = &greq->greq_data.gd_set.gds_setqueue;
  return 0;
}

static int ast_proplist_add(gdp_output *out, gdp_proplist_t *proplist,
                            const char *name_s, const char *name_e,
                            const char *value_s, const char *value_e) {
  graphd_set_subject *su;
  graphd_set_queue *setq = proplist;

  su = cm_malloc(out->out_cm, sizeof(*su));

  if (!su) return ENOMEM;

  su->set_name_s = name_s;
  su->set_name_e = name_e;
  su->set_value_s = value_s;
  su->set_value_e = value_e;
  su->set_next = NULL;

  *setq->setqueue_tail = su;
  setq->setqueue_tail = &su->set_next;

  return 0;
}

static int ast_statlist_new(gdp_output *out, gdp_statlist_t **statlist) {
  *statlist = NULL; /* unused */
  return 0;
}

static int ast_statlist_add(gdp_output *out, gdp_statlist_t *statlist,
                            gdp_token const *tok, unsigned long long num) {
  graphd_request *greq = out->out_private;
  graphd_property const *prop;
  graphd_status_subject *subj;
  graphd_status_queue *queue;
  char const *s;
  char const *e;
  int id = GRAPHD_STATUS_UNSPECIFIED;
  cl_handle *const cl = graphd_request_cl(greq);

  if (greq->greq_request != GRAPHD_REQUEST_STATUS) {
    graphd_request_errprintf(greq, 0,
                             "SYNTAX status argument "
                             "outside a status request");
    return GRAPHD_ERR_SYNTAX;
  }
  cl_assert(cl, greq->greq_request == GRAPHD_REQUEST_STATUS);
  queue = &greq->greq_data.gd_status.gds_statqueue;

  /* identify subject */
  s = tok->tkn_start;
  e = tok->tkn_end;
  if ((prop = graphd_property_by_name(s, e)) != NULL)
    id = GRAPHD_STATUS_PROPERTY;
  else {
    switch (*s) {
      case 'i':
        if (gdp_token_matches(tok, "islink")) id = GRAPHD_STATUS_ISLINK;
        break;

      case 'c':
        if (gdp_token_matches(tok, "conn") ||
            gdp_token_matches(tok, "connection") ||
            gdp_token_matches(tok, "connections"))
          id = GRAPHD_STATUS_CONNECTION;
        break;

      case 'd':
        if (gdp_token_matches(tok, "db") ||
            gdp_token_matches(tok, "database") ||
            gdp_token_matches(tok, "databases"))
          id = GRAPHD_STATUS_DATABASE;
        else if (gdp_token_matches(tok, "diary"))
          id = GRAPHD_STATUS_DIARY;
        break;

      case 'm':
        if (gdp_token_matches(tok, "mem") || gdp_token_matches(tok, "memory"))
          id = GRAPHD_STATUS_MEMORY;
        break;

      case 'r':
        if (gdp_token_matches(tok, "rep") ||
            gdp_token_matches(tok, "replica-details"))
          id = GRAPHD_STATUS_REPLICA;
        else if (gdp_token_matches(tok, "ru") ||
                 gdp_token_matches(tok, "rusage"))
          id = GRAPHD_STATUS_RUSAGE;
        break;

      case 't':
        if (gdp_token_matches(tok, "tiles")) id = GRAPHD_STATUS_TILES;
        break;
    }
  }
  if (id == GRAPHD_STATUS_UNSPECIFIED) return GRAPHD_ERR_LEXICAL;

  /* add subject to status list
   */
  if ((subj = cm_talloc(out->out_cm, graphd_status_subject, 1)) == NULL)
    return ENOMEM;

  subj->stat_subject = id;
  subj->stat_number = num;
  subj->stat_property = prop;
  subj->stat_next = NULL;

  *queue->statqueue_tail = subj;
  queue->statqueue_tail = &subj->stat_next;

  return 0;
}

static int ast_guidset_new(gdp_output *out, gdp_guidset_t **new_set) {
  graphd_request *greq = out->out_private;
  graphd_guid_set *gs = cm_malloc(greq->greq_req.req_cm, sizeof(*gs));

  if (gs == NULL) return ENOMEM;

  graphd_guid_set_initialize(gs);
  *new_set = gs;

  return 0;
}

static int ast_guidset_add(gdp_output *out, gdp_guidset_t *set,
                           graph_guid const *guid) {
  graphd_guid_set *gs = set;

  /* a null GUID value */
  if (guid == NULL) {
    gs->gs_null = true;
    return 0;
  }

  /* any other GUID value */
  return graphd_guid_set_add(out->out_private, set, guid);
}

static int ast_strset_new(gdp_output *out, gdp_strset_t **strset) {
  graphd_string_constraint *strcon;

  if ((strcon = cm_malloc(out->out_cm, sizeof(*strcon))) == NULL) return ENOMEM;

  memset(strcon, 0, sizeof(*strcon));
  strcon->strcon_tail = &strcon->strcon_head;

  *strset = strcon;
  return 0;
}

static int ast_strset_add(gdp_output *out, gdp_strset_t *strset,
                          gdp_token *tok) {
  graphd_string_constraint *strcon = strset;
  graphd_string_constraint_element *strcel;

  if ((strcel = cm_malloc(out->out_cm, sizeof(*strcel))) == NULL) return ENOMEM;

  strcel->strcel_next = NULL;
  strcel->strcel_s = tok->tkn_start;
  strcel->strcel_e = tok->tkn_end;

  *strcon->strcon_tail = strcel;
  strcon->strcon_tail = &strcel->strcel_next;

  return 0;
}

static int ast_pattern_new(
    gdp_output *out,
    gdp_conlist_t *scope,      // context, if needed (for variables)
    gdp_token const *tok,      // token, if needed
    graphd_pattern_type kind,  // pattern kind
    bool forward,              // (used for sorting)
    gdp_pattern_t *ppat,       // parent pattern, or NULL if none
    gdp_pattern_t **new_pat    // the new pattern
    ) {
  graphd_request *greq = out->out_private;
  char const *s = tok ? tok->tkn_start : NULL;
  char const *e = tok ? tok->tkn_end : NULL;
  graphd_pattern *pat;
  char const *nada = "";

  switch (kind) {
    case GRAPHD_PATTERN_LITERAL:
      gdp_assert(out->out_cl, tok != NULL);
      pat = graphd_pattern_alloc_string(greq, ppat, kind, s, e);
      break;
    case GRAPHD_PATTERN_NONE:
      gdp_assert(out->out_cl, tok != NULL);
      pat = graphd_pattern_alloc_string(greq, ppat, kind, nada, nada);
      break;
    case GRAPHD_PATTERN_VARIABLE:
      gdp_assert(out->out_cl, (scope != NULL) && (tok != NULL));
      pat = graphd_variable_declare(greq, scope, ppat, s, e);
      break;
    default:
      pat = graphd_pattern_alloc(greq, ppat, kind);
      break;
  }

  if (pat == NULL) {
    cl_log(out->out_cl, CL_LEVEL_ERROR, "insufficient memory");
    return ENOMEM;
  }

  pat->pat_sort_forward = forward;

  *new_pat = pat;
  return 0;
}

static void _init_ast_ops(gdp_ast_ops *ops) {
  *ops = (gdp_ast_ops){
      .request_initialize = ast_request_initialize,
      .request_new = ast_request_new,
      .request_new_dump = ast_request_new_dump,
      .request_new_error = ast_request_new_error,
      .request_new_replica = ast_request_new_replica,
      .request_new_replica_write = ast_request_new_replica_write,
      .request_new_restore = ast_request_new_restore,
      .request_new_response = ast_request_new_response,
      .request_new_rok = ast_request_new_rok,
      .request_new_set = ast_request_new_set,
      .request_new_smp = ast_request_new_smp,
      .request_new_status = ast_request_new_status,
      .request_new_verify = ast_request_new_verify,
      .modlist_new = ast_modlist_new,
      .modlist_add_asof = ast_modlist_add_asof,
      .modlist_add_cost = ast_modlist_add_cost,
      .modlist_add_dateline = ast_modlist_add_dateline,
      .modlist_add_id = ast_modlist_add_id,
      .modlist_add_heatmap = ast_modlist_add_heatmap,
      .modlist_add_loglevel = ast_modlist_add_loglevel,
      .modlist_add_timeout = ast_modlist_add_timeout,
      .conlist_new = ast_conlist_new,
      .conlist_add_comparator = ast_conlist_add_comparator,
      .conlist_add_count = ast_conlist_add_count,
      .conlist_add_cursor = ast_conlist_add_cursor,
      .conlist_add_dateline = ast_conlist_add_dateline,
      .conlist_add_false = ast_conlist_add_false,
      .conlist_add_flag = ast_conlist_add_flag,
      .conlist_add_gen = ast_conlist_add_gen,
      .conlist_add_guid = ast_conlist_add_guid,
      .conlist_add_key = ast_conlist_add_key,
      .conlist_add_linkage = ast_conlist_add_linkage,
      .conlist_add_or = ast_conlist_add_or,
      .conlist_add_pagesize = ast_conlist_add_pagesize,
      .conlist_add_resultpagesize = ast_conlist_add_resultpagesize,
      .conlist_add_countlimit = ast_conlist_add_countlimit,
      .conlist_add_result = ast_conlist_add_result,
      .conlist_add_sort = ast_conlist_add_sort,
      .conlist_add_start = ast_conlist_add_start,
      .conlist_add_sequence = ast_conlist_add_sequence,
      .conlist_add_string = ast_conlist_add_string,
      .conlist_add_sublist = ast_conlist_add_sublist,
      .conlist_add_timestamp = ast_conlist_add_timestamp,
      .conlist_add_unique = ast_conlist_add_unique,
      .conlist_add_valuetype = ast_conlist_add_valuetype,
      .conlist_add_variable = ast_conlist_add_variable,
      .conlist_get_meta = ast_conlist_get_meta,
      .conlist_set_meta = ast_conlist_set_meta,
      .conlist_has_meta = ast_conlist_has_meta,
      .conlist_set_linkage = ast_conlist_set_linkage,
      .conlist_has_linkage = ast_conlist_has_linkage,
      .conlist_add_valuecomparator = ast_conlist_add_value_comparator,
      .conlist_new_sortcomparator = ast_conlist_new_sortcomparator,
      .conlist_add_sortcomparator = ast_conlist_add_sortcomparator,
      .proplist_new = ast_proplist_new,
      .proplist_add = ast_proplist_add,
      .recordlist_new = ast_recordlist_new,
      .recordlist_set = ast_recordlist_set,
      .statlist_new = ast_statlist_new,
      .statlist_add = ast_statlist_add,
      .smpcmd_new = ast_smpcmd_new,
      .smpcmd_set = ast_smpcmd_set,
      .guidset_new = ast_guidset_new,
      .guidset_add = ast_guidset_add,
      .strset_new = ast_strset_new,
      .strset_add = ast_strset_add,
      .pattern_new = ast_pattern_new,
  };
}

static void _init_greq(graphd_request *greq) {
  /* parameter queue */
  greq->greq_parameter_head = NULL;
  greq->greq_parameter_tail = &greq->greq_parameter_head;
}

int graphd_ast_parse(graphd_request *greq) {
  cm_handle *cm = graphd_request_cm(greq);
  cl_handle *cl = graphd_request_cl(greq);
  gdp parser;
  gdp_output out;
  gdp_input in;
  int err;

  /* set output specs */
  out.out_private = greq;
  out.out_cm = cm;
  out.out_cl = cl;
  _init_ast_ops(&out.out_ops);

  if (greq->greq_request == GRAPHD_REQUEST_UNSPECIFIED) _init_greq(greq);

  /* set input specs */
  if ((err = gdp_input_init_chain_part(
           &in, greq->greq_req.req_first, greq->greq_req.req_first_offset,
           greq->greq_req.req_last, greq->greq_req.req_last_n,
           greq->greq_req.req_cm, cl)) != 0)
    return err;

  /* initialize parser */
  if ((err = gdp_init(&parser, cm, cl))) return err;

  /* parse ! */
  if (greq->greq_request == GRAPHD_REQUEST_UNSPECIFIED)
    return gdp_parse(&parser, &in, &out);
  else
    return gdp_parse_reply(&parser, greq->greq_request, &in, &out);
}
