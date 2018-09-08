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
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

static int seperate(graphd_handle* g, cm_buffer* buf) {
  if (buf->buf_n == 0) return 0;

  switch (buf->buf_s[buf->buf_n - 1]) {
    case '(':
    case '=':
    case ' ':
    case '\t':
      return 0;
    default:
      break;
  }

  return cm_buffer_add_string(buf, " ");
}

static bool is_vip_guid(graphd_handle* const g, graph_guid const* const guid) {
  int linkage;
  pdb_id id;
  int err;

  err = pdb_id_from_guid(g->g_pdb, &id, guid);
  if (err != 0) return false;

  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++) {
    unsigned long long n;
    err = pdb_linkage_count(g->g_pdb, linkage, id, 0, PDB_ITERATOR_HIGH_ANY,
                            PDB_VIP_MIN, &n);

    if (err == PDB_ERR_MORE || n >= PDB_VIP_MIN) return true;
  }
  return false;
}

static char const* vip_guid(graphd_handle* g, graph_guid const* guid,
                            unsigned int flags, char* buf, size_t size) {
  if (!(flags & GRAPHD_SIGNATURE_OMIT_COMMON_GUID) || is_vip_guid(g, guid))
    return graph_guid_to_string(guid, buf, size);

  return "#...";
}

static int signature_guid_set(graphd_handle* g, graphd_guid_set const* gs,
                              unsigned int flags, cm_buffer* sig) {
  size_t i;
  int err;
  char buf[GRAPH_GUID_SIZE];

  if ((err = seperate(g, sig)) != 0) return err;

  switch (gs->gs_n) {
    case 0:
      return cm_buffer_add_string(sig, "()");

    case 1:
      return cm_buffer_sprintf(
          sig, "%s", vip_guid(g, gs->gs_guid, flags, buf, sizeof buf));

    default:
      if ((err = cm_buffer_add_string(sig, "(")) != 0) return err;

      for (i = 0; i < gs->gs_n; i++) {
        char buf[GRAPH_GUID_SIZE];

        if ((err = seperate(g, sig)) != 0 ||
            (err = cm_buffer_sprintf(
                 sig, "%s",
                 vip_guid(g, gs->gs_guid + i, flags, buf, sizeof buf))))

          return err;
      }

      if ((err = cm_buffer_add_string(sig, ")")) != 0) return err;
      break;
  }
  return 0;
}

static int signature_guid_constraint(graphd_handle* g, char const* name,
                                     graphd_guid_constraint const* guidcon,
                                     unsigned int flags, cm_buffer* sig) {
  graphd_guid_set const* gs;
  int err;

  if (!guidcon->guidcon_match_valid && !guidcon->guidcon_include_valid &&
      !guidcon->guidcon_exclude_valid)
    return 0;

  if (guidcon->guidcon_match_valid) {
    for (gs = &guidcon->guidcon_match; gs != NULL; gs = gs->gs_next)

      if ((err = seperate(g, sig)) != 0 ||
          (err = cm_buffer_sprintf(sig, "%s~=", name)) != 0 ||
          (err = signature_guid_set(g, gs, flags, sig)) != 0)

        return err;
  }
  if (guidcon->guidcon_include_valid)

    if ((err = seperate(g, sig)) != 0 ||
        (err = cm_buffer_sprintf(sig, "%s=", name)) != 0 ||
        (err = signature_guid_set(g, &guidcon->guidcon_include, flags, sig)) !=
            0)

      return err;

  if (guidcon->guidcon_exclude_valid)

    if ((err = seperate(g, sig)) != 0 ||
        (err = cm_buffer_sprintf(sig, "%s!=", name)) != 0 ||
        (err = signature_guid_set(g, &guidcon->guidcon_exclude, flags, sig)) !=
            0)

      return err;

  return 0;
}

static int signature_gencon(graphd_handle* const g, char const* const name,
                            graphd_generational_constraint const* gencon,
                            cm_buffer* const sig) {
  int err;

  if ((err = seperate(g, sig)) != 0) return err;

  if (!gencon->gencon_valid) return cm_buffer_sprintf(sig, "%s=*", name);

  if (gencon->gencon_min == 0)
    return cm_buffer_sprintf(sig, "%s<=%lu", name, gencon->gencon_max);

  if (gencon->gencon_max == ULONG_MAX)
    return cm_buffer_sprintf(sig, "%s>=%lu", name, gencon->gencon_min);

  if (gencon->gencon_max == gencon->gencon_min)
    return cm_buffer_sprintf(sig, "%s=%lu", name, gencon->gencon_min);

  return cm_buffer_sprintf(sig, "%s~=%lu..%lu", name, gencon->gencon_min,
                           gencon->gencon_max);
}

static int signature_string_constraint_queue(
    graphd_handle* const g, char const* const name,
    graphd_string_constraint_queue const* const q, cm_buffer* const sig,
    bool const write_value) {
  graphd_string_constraint const* strcon;
  int err;

  if (q->strqueue_head == NULL) return 0;

  for (strcon = q->strqueue_head; strcon != NULL;
       strcon = strcon->strcon_next) {
    if ((err = seperate(g, sig)) != 0) return err;

    err = cm_buffer_sprintf(sig, "%s", name);
    if (err != 0) return err;

    err = graphd_string_constraint_to_signature(strcon, sig, write_value);
    if (err != 0) return err;
  }
  return 0;
}

static int signature_flag(graphd_handle* g, char const* name,
                          graphd_flag_constraint fl,
                          graphd_flag_constraint deflt, cm_buffer* sig) {
  char const* str;
  char buf[42];
  int err;

  if (fl == deflt || fl == GRAPHD_FLAG_UNSPECIFIED) return 0;

  if ((err = seperate(g, sig)) != 0) return err;
  switch (fl) {
    case GRAPHD_FLAG_FALSE:
      str = "false";
      break;
    case GRAPHD_FLAG_TRUE:
      str = "true";
      break;
    case GRAPHD_FLAG_DONTCARE:
      str = "*";
      break;
    default:
      snprintf(buf, sizeof buf, "%d", (int)fl);
      str = buf;
      break;
  }
  return cm_buffer_sprintf(sig, "%s=%s", name, str);
}

static int signature_byte_pattern(graphd_handle* g, char const* name_s,
                                  char const* name_e,
                                  graphd_pattern const* pattern,
                                  cm_buffer* sig) {
  if (pattern != NULL && pattern != graphd_pattern_read_default() &&
      pattern != graphd_pattern_write_default()) {
    char bigbuf[1024];
    int err;
    char const* s;

    s = graphd_pattern_to_string(pattern, bigbuf, sizeof bigbuf);
    if (s != NULL) {
      if ((err = seperate(g, sig)) != 0 ||
          (err = cm_buffer_sprintf(sig, "%.*s=%s", (int)(name_e - name_s),
                                   name_s, s)) != 0)
        return err;
    }
  }
  return 0;
}

static int signature_pattern(graphd_handle* g, char const* name,
                             char const* fallback,
                             graphd_pattern const* pattern, cm_buffer* sig) {
  if (pattern == NULL && fallback != NULL) {
    int err;

    if ((err = seperate(g, sig)) != 0 ||
        (err = cm_buffer_sprintf(sig, "%s=%s", name, fallback)) != 0)
      return err;
    return 0;
  }
  if (pattern != NULL && pattern != graphd_pattern_read_default() &&
      pattern != graphd_pattern_write_default()) {
    char bigbuf[1024];
    int err;
    char const* s;

    s = graphd_pattern_to_string(pattern, bigbuf, sizeof bigbuf);
    if (s != NULL) {
      if ((err = seperate(g, sig)) != 0 ||
          (err = cm_buffer_sprintf(sig, "%s=%s", name, s)) != 0)
        return err;
    }
  }
  return 0;
}

static int signature_code(graphd_handle* g, char const* name, int code,
                          cm_buffer* sig) {
  if (code != 0) {
    char bigbuf[1024];
    int err;
    char const* s;

    s = graphd_unique_to_string(code, bigbuf, sizeof bigbuf);
    if (s != NULL) {
      if ((err = seperate(g, sig)) != 0 ||
          (err = cm_buffer_sprintf(sig, "%s=%s", name, s)) != 0)
        return err;
    }
  }
  return 0;
}

static int signature_count(graphd_handle* const g, bool const lo_valid,
                           size_t const lo, bool const hi_valid,
                           size_t const hi, cm_buffer* const sig) {
  int err;

  if (!lo_valid && !hi_valid) return 0;

  if (!hi_valid && lo_valid && lo == 1) return 0;

  if ((err = seperate(g, sig)) != 0) return err;

  if (lo_valid && lo == 0 && !hi_valid)
    err = cm_buffer_add_string(sig, "optional");
  else {
    if (lo_valid) {
      err = cm_buffer_sprintf(sig, "count>=%zu", lo);
      if (err != 0) return err;
    }
    if (hi_valid) {
      if ((err = seperate(g, sig)) != 0 ||
          (err = cm_buffer_sprintf(sig, "count>=%zu", lo)) != 0)
        return err;
    }
  }
  return 0;
}

static int signature_timestamp(graphd_handle* const g, char const* const name,
                               bool const valid, graph_timestamp_t const lo,
                               graph_timestamp_t const hi,
                               cm_buffer* const sig) {
  int err;

  if (!valid || (lo == GRAPH_TIMESTAMP_MIN && hi == GRAPH_TIMESTAMP_MAX))
    return 0;

  if ((err = seperate(g, sig)) != 0) return err;

  if (lo == GRAPH_TIMESTAMP_MIN)
    err = cm_buffer_sprintf(sig, "%s<=#...", name);
  else if (hi == GRAPH_TIMESTAMP_MAX)
    err = cm_buffer_sprintf(sig, "%s>=#...", name);
  else if (hi == lo)
    err = cm_buffer_sprintf(sig, "%s=#...", name);
  else
    err = cm_buffer_sprintf(sig, "%s~=#..#", name);
  return err;
}

static int signature_dateline(graphd_handle* const g, char const* const name,
                              graphd_dateline_constraint const* const condat,
                              cm_buffer* const sig) {
  int err;

  if (condat->dateline_min != 0) {
    if ((err = seperate(g, sig)) != 0 ||
        (err = cm_buffer_sprintf(sig, "%s>=#...", name)))
      return err;
  }
  if (condat->dateline_max != 0) {
    if ((err = seperate(g, sig)) != 0 ||
        (err = cm_buffer_sprintf(sig, "%s<=#...", name)))
      return err;
  }
  return 0;
}

static int signature_assignments(graphd_handle* const g,
                                 graphd_assignment const* a,
                                 cm_buffer* const sig) {
  int err;
  char const *name_s, *name_e;

  for (; a != NULL; a = a->a_next) {
    graphd_variable_declaration_name(a->a_declaration, &name_s, &name_e);

    if ((err = signature_byte_pattern(g, name_s, name_e, a->a_result, sig)) !=
        0)
      return err;
  }
  return 0;
}

/**
 * @brief Write a signature for a constraint subtree.
 *
 *  Constraints with similar structure have the same signature.
 *  GUIDs with VIP-or-greater fan-in stand for themselves;
 *  other specific GUIDs, datelines, and timestamps are omitted.
 *
 * @param cl	log and assert through here.
 * @param con	a constraint
 * @param buf	append the signature to this.
 */
static int signature(graphd_handle* g, graphd_constraint const* con,
                     unsigned int flags, cm_buffer* sig) {
  int err;
  int linkage;
  char const* s;
  graphd_constraint const* sub;

  cl_assert(g->g_cl, sig != NULL);

  /*  The constraint can be null if the
   *  request aborted with a parser failure.
   */
  if (con == NULL) return cm_buffer_sprintf(sig, "null");

  if (con->con_parent != NULL) {
    if ((err = seperate(g, sig)) != 0) return err;

    if (graphd_linkage_is_i_am(con->con_linkage)) {
      err = cm_buffer_sprintf(
          sig, "%s->(",
          pdb_linkage_to_string(graphd_linkage_i_am(con->con_linkage)));
    } else
      err = cm_buffer_sprintf(
          sig, "(<-%s",
          pdb_linkage_to_string(graphd_linkage_my(con->con_linkage)));
    if (err != 0) return err;
  }

  if ((err = signature_string_constraint_queue(g, "type", &con->con_type, sig,
                                               true)) != 0 ||
      (err = signature_string_constraint_queue(g, "name", &con->con_name, sig,
                                               true)) != 0 ||
      (err = signature_string_constraint_queue(g, "value", &con->con_value, sig,
                                               false)) != 0)
    return err;

  if (con->con_valuetype != GRAPH_DATA_UNSPECIFIED) {
    if ((err = seperate(g, sig)) != 0) return err;

    s = graph_datatype_to_string(con->con_valuetype);
    if (s != NULL)
      err = cm_buffer_sprintf(sig, "datatype=%s", s);
    else
      err = cm_buffer_sprintf(sig, "datatype=%d", (int)con->con_valuetype);
    if (err != 0) return err;
  }

  err = signature_flag(g, "archive", con->con_archival, GRAPHD_FLAG_DONTCARE,
                       sig);
  if (err != 0) return err;

  err = signature_flag(g, "live", con->con_live, GRAPHD_FLAG_TRUE, sig);
  if (err != 0) return err;

  if ((err = signature_code(g, "key", con->con_key, sig)) != 0 ||
      (err = signature_code(g, "unique", con->con_unique, sig)) != 0 ||
      (err = signature_pattern(g, "result", "()", con->con_result, sig)) != 0 ||
      (err = signature_pattern(g, "sort", NULL, con->con_sort, sig)) != 0)
    return err;

  if (con->con_countlimit_valid && con->con_countlimit != con->con_pagesize &&
      (con->con_countlimit != 1 || !graphd_linkage_is_i_am(con->con_linkage))) {
    if ((err = seperate(g, sig)) != 0 ||
        (err = cm_buffer_sprintf(sig, "countlimit=%llu",
                                 con->con_countlimit)) != 0)
      return err;
  }

  if (con->con_resultpagesize_parsed_valid &&
      ((con->con_resultpagesize_parsed != 1 ||
        !graphd_linkage_is_i_am(con->con_linkage)) &&
       con->con_resultpagesize_parsed != con->con_pagesize &&
       con->con_resultpagesize_parsed != GRAPHD_RESULT_PAGE_SIZE_DEFAULT)) {
    if ((err = seperate(g, sig)) != 0 ||
        (err = cm_buffer_sprintf(sig, "resultpagesize=%zu",
                                 con->con_resultpagesize_parsed)) != 0)
      return err;
  }

  if (con->con_pagesize_valid &&
      (con->con_pagesize != 1 || !graphd_linkage_is_i_am(con->con_linkage))) {
    if ((err = seperate(g, sig)) != 0 ||
        (err = cm_buffer_sprintf(sig, "pagesize=%zu", con->con_pagesize)) != 0)
      return err;
  }

  if (con->con_start != 0) {
    if ((err = seperate(g, sig)) != 0 ||
        (err = cm_buffer_sprintf(sig, "start=%zu", con->con_start)) != 0)
      return err;
  }

  if (con->con_newest.gencon_valid) {
    err = signature_gencon(g, "newest", &con->con_newest, sig);
    if (err != 0) return 0;
  }
  if (con->con_oldest.gencon_valid) {
    err = signature_gencon(g, "oldest", &con->con_oldest, sig);
    if (err != 0) return err;
  }

  if ((err = signature_guid_constraint(g, "guid", &con->con_guid, flags,
                                       sig)) != 0 ||
      (err = signature_guid_constraint(g, "next", &con->con_version_next, flags,
                                       sig)) != 0 ||
      (err = signature_guid_constraint(
           g, "previous", &con->con_version_previous, flags, sig)) != 0)
    return err;

  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++)
    if ((err = signature_guid_constraint(g, pdb_linkage_to_string(linkage),
                                         con->con_linkcon + linkage, flags,
                                         sig)) != 0)
      return err;

  if (!(flags & GRAPHD_SIGNATURE_OMIT_CURSOR) && con->con_cursor_s != NULL) {
    if ((err = seperate(g, sig)) != 0) return err;

    err = cm_buffer_add_string(sig, con->con_cursor_s < con->con_cursor_e
                                        ? "cursor=\"...\""
                                        : "cursor=\"\"");
    if (err != 0) return err;
  }

  if ((err = signature_timestamp(g, "timestamp", con->con_timestamp_valid,
                                 con->con_timestamp_min, con->con_timestamp_max,
                                 sig)) != 0)
    return err;

  if ((err = signature_count(g, con->con_count.countcon_min_valid,
                             con->con_count.countcon_min,
                             con->con_count.countcon_max_valid,
                             con->con_count.countcon_max, sig)) != 0)
    return err;

  err = signature_dateline(g, "dateline", &con->con_dateline, sig);
  if (err != 0) return err;

  err = signature_assignments(g, con->con_assignment_head, sig);
  if (err != 0) return err;

  for (sub = con->con_head; sub != NULL; sub = sub->con_next)
    if ((err = signature(g, sub, flags, sig)) != 0) return err;

  if (con->con_parent != NULL)
    if ((err = cm_buffer_add_string(sig, ")")) != 0) return err;

  return 0;
}

/**
 * @brief Write a signature for a constraint subtree.
 *
 *  Constraints with similar structure have the same signature.
 *  GUIDs with VIP-or-greater fan-in stand for themselves;
 *  other specific GUIDs, datelines, and timestamps are omitted.
 *
 *  In case of a successful call, the passed-in buffer has been
 *  dynamically allocated and filled; it must be freed by the caller.
 *  In case of an error result, the buffer must not be freed.
 *
 * @param g	graphd handle for allocations and logging
 * @param con	the constraint subtree
 * @param out	uninitialized buffer storage
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_constraint_signature(graphd_handle* g, graphd_constraint const* con,
                                unsigned int flags, cm_buffer* out) {
  int err;

  cm_buffer_initialize(out, g->g_cm);
  err = signature(g, con, flags, out);
  if (err != 0) cm_buffer_finish(out);
  cl_log(g->g_cl, CL_LEVEL_VERBOSE, "graphd_constraint_signature(%.*s)",
         (int)cm_buffer_length(out), cm_buffer_memory(out));
  return err;
}
