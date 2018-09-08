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

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>

static char* graphd_constraint_format(char* s, char* e,
                                      graphd_constraint const* con);

static char* graphd_constraint_format_or(char* s, char* e,
                                         graphd_constraint_or const* cor);

char const* graphd_constraint_flag_to_string(int flag, char* buf, size_t size) {
  switch (flag) {
    case GRAPHD_FLAG_UNSPECIFIED:
      return "unspecified";
    case GRAPHD_FLAG_FALSE:
      return "false";
    case GRAPHD_FLAG_TRUE:
      return "true";
    case GRAPHD_FLAG_DONTCARE:
      return "dontcare";
    case GRAPHD_FLAG_TRUE_LOCAL:
      return "local";
    default:
      break;
  }
  snprintf(buf, size, "unexpected flag %x", flag);
  return buf;
}

char const* graphd_constraint_meta_to_string(int meta, char* buf, size_t size) {
  switch (meta) {
    case GRAPHD_META_UNSPECIFIED:
      return "";
    case GRAPHD_META_ANY:
      return "any";
    case GRAPHD_META_NODE:
      return "node";
    case GRAPHD_META_LINK_TO:
      return "<-";
    case GRAPHD_META_LINK_FROM:
      return "->";
    default:
      break;
  }
  snprintf(buf, size, "unexpected meta-flags %x", meta);
  return buf;
}

char const* graphd_constraint_linkage_to_string(int linkage, char* buf,
                                                size_t size) {
  if (linkage == 0) return "none";

  if (linkage < 1 || linkage > 9) {
    snprintf(buf, size, "unexpected linkage %x", linkage);
    return buf;
  }
  if (graphd_linkage_is_my(linkage))
    snprintf(buf, size, "(<-%s",
             pdb_linkage_to_string(graphd_linkage_my(linkage)));
  else
    snprintf(buf, size, "%s->(",
             pdb_linkage_to_string(graphd_linkage_i_am(linkage)));
  return buf;
}

static char const* generational_to_string(
    graphd_generational_constraint const* gencon, char* buf, size_t bufsize) {
  if (!gencon->gencon_valid) return "...";

  if (gencon->gencon_min == gencon->gencon_max) {
    snprintf(buf, bufsize, "%lu", gencon->gencon_min);
  } else if (gencon->gencon_min <= 0) {
    if (gencon->gencon_max >= (unsigned long)-1) return "...";
    snprintf(buf, bufsize, "... %lu", gencon->gencon_max);
  } else if (gencon->gencon_max >= (unsigned long)-1)
    snprintf(buf, bufsize, "%lu ...", gencon->gencon_min);
  else
    snprintf(buf, bufsize, "%lu ... %lu", gencon->gencon_min,
             gencon->gencon_max);
  return buf;
}

static char* graphd_constraint_format_bytes(char* s, char* e,
                                            char const* bytes_s,
                                            char const* bytes_e) {
  while (bytes_s < bytes_e && e - s > 1) *s++ = *bytes_s++;
  if (s < e) *s = '\0';
  return s;
}

static char* graphd_constraint_format_string(char* s, char* e,
                                             char const* string) {
  while (*string != '\0' && e - s > 1) *s++ = *string++;
  if (s < e) *s = '\0';
  return s;
}

static char* graphd_constraint_format_prompt_bytes(char* s, char* e,
                                                   char const* name_s,
                                                   char const* name_e) {
  if (e - s <= 1) return s;

  if (name_s < name_e) {
    if (e - s > 1 && s[-1] != ' ' && s[-1] != '(') *s++ = ' ';

    s = graphd_constraint_format_bytes(s, e, name_s, name_e);
    if (e - s > 1) *s++ = '=';
    if (s < e) *s = '\0';
  } else {
    if (s[-1] != ' ' && s[-1] != '(') *s++ = ' ';
  }

  return s;
}

static char* graphd_constraint_format_prompt(char* s, char* e,
                                             char const* name) {
  if (e - s <= 1) return s;

  if (name != NULL) {
    if (e - s > 1 && s[-1] != ' ' && s[-1] != '(') *s++ = ' ';

    s = graphd_constraint_format_string(s, e, name);
    if (e - s > 1) *s++ = '=';
    if (s < e) *s = '\0';
  } else {
    if (s[-1] != ' ' && s[-1] != '(') *s++ = ' ';
  }

  return s;
}

char const* graphd_constraint_guidset_to_string(graphd_guid_set const* gs,
                                                char* buf, size_t size) {
  char guidbuf[GRAPH_GUID_SIZE];
  char const* guid_ptr;

  if (gs->gs_n == 0)
    return "()";
  else if (gs->gs_n == 1 && !gs->gs_null)
    return graph_guid_to_string(gs->gs_guid, buf, size);
  else {
    char* w = buf;
    char const* sep = "";
    size_t i;

    if (size <= 10) return "(...)";

    *w++ = '(';
    for (i = 0; i < 4 && i < gs->gs_n; i++) {
      snprintf(w, size - (w - buf), "%s%s", sep,
               graph_guid_to_string(gs->gs_guid + i, guidbuf, sizeof guidbuf));
      sep = " ";
      if (w < buf + size)
        ;
      w += strlen(w);
    }
    if (gs->gs_null) {
      snprintf(w, size - (w - buf), "%snull", sep);

      if (w < buf + size)
        ;
      w += strlen(w);
    }

    if (w < buf + size - 4 && i < gs->gs_n)
      memcpy(w, "..)", 4);
    else if (w < buf + size - 2)
      memcpy(w, ")", 2);
    else
      memcpy(w - 4, "..)", 4);

    guid_ptr = buf;
  }
  return guid_ptr;
}

static char* graphd_constraint_format_guidset(char* s, char* e,
                                              graphd_guid_set const* gs) {
  char buf[200];
  char guidbuf[GRAPH_GUID_SIZE];
  char const* guid_ptr;

  if (gs->gs_n == 0)
    guid_ptr = "()";
  else if (gs->gs_n == 1 && !gs->gs_null)
    guid_ptr = graph_guid_to_string(gs->gs_guid, guidbuf, sizeof guidbuf);
  else {
    char* w = buf;
    char const* sep = "";
    size_t i;

    *w++ = '(';
    for (i = 0; i < 4 && i < gs->gs_n; i++) {
      snprintf(w, sizeof(buf) - (w - buf), "%s%s", sep,
               graph_guid_to_string(gs->gs_guid + i, guidbuf, sizeof guidbuf));
      sep = " ";
      if (w < buf + sizeof buf)
        ;
      w += strlen(w);
    }
    if (gs->gs_null) {
      snprintf(w, sizeof(buf) - (w - buf), "%snull", sep);

      if (w < buf + sizeof buf)
        ;
      w += strlen(w);
    }

    if (w < buf + sizeof(buf) - 4 && i < gs->gs_n)
      memcpy(w, "..)", 4);
    else if (w < buf + sizeof(buf) - 2)
      memcpy(w, ")", 2);
    else
      memcpy(w - 4, "..)", 4);

    guid_ptr = buf;
  }
  s = graphd_constraint_format_string(s, e, guid_ptr);
  return s;
}

static char* graphd_constraint_format_guidcon(
    char* s, char* e, graphd_guid_constraint const* guidcon, char const* name) {
  if (!guidcon->guidcon_include_valid && !guidcon->guidcon_match_valid &&
      !guidcon->guidcon_exclude_valid)
    return s;

  if (s < e && s[-1] != ' ' && s[-1] != '(') *s++ = ' ';

  if (name != NULL) s = graphd_constraint_format_string(s, e, name);

  if (guidcon->guidcon_include_valid) {
    s = graphd_constraint_format_string(s, e, "=");
    s = graphd_constraint_format_guidset(s, e, &guidcon->guidcon_include);
  }
  if (guidcon->guidcon_match_valid) {
    s = graphd_constraint_format_string(s, e, "~=");
    s = graphd_constraint_format_guidset(s, e, &guidcon->guidcon_match);
  }
  if (guidcon->guidcon_exclude_valid) {
    s = graphd_constraint_format_string(s, e, "!=");
    s = graphd_constraint_format_guidset(s, e, &guidcon->guidcon_exclude);
  }
  return s;
}

static char* graphd_constraint_format_timerange(char* s, char* e,
                                                graph_timestamp_t tmin,
                                                graph_timestamp_t tmax,
                                                char const* name) {
  char bmin[200], bmax[200];
  char buf[200];

  if (tmin == GRAPH_TIMESTAMP_MIN && tmax >= GRAPH_TIMESTAMP_MAX) return s;

  s = graphd_constraint_format_prompt(s, e, name);
  if (tmin == GRAPH_TIMESTAMP_MIN)
    snprintf(buf, sizeof buf, "...%s",
             graph_timestamp_to_string(tmax, bmax, sizeof bmax));
  else if (tmax >= GRAPH_TIMESTAMP_MAX)
    snprintf(buf, sizeof buf, "%s...",
             graph_timestamp_to_string(tmin, bmin, sizeof bmin));
  else
    snprintf(buf, sizeof buf, "%s...%s",
             graph_timestamp_to_string(tmax, bmax, sizeof bmax),
             graph_timestamp_to_string(tmax, bmax, sizeof bmax));
  return graphd_constraint_format_string(s, e, buf);
}

static char* graphd_constraint_format_strqueue(
    char* s, char* e, graphd_string_constraint_queue const* q,
    char const* name) {
  graphd_string_constraint* strcon;
  char const* sep = "";

  if (q->strqueue_head == NULL) {
    if (s < e) *s = '\0';
    return s;
  }

  if (s < e && s[-1] != ' ' && s[-1] != '(') *s++ = ' ';

  if (name != NULL) s = graphd_constraint_format_string(s, e, name);

  for (strcon = q->strqueue_head; strcon != NULL;
       strcon = strcon->strcon_next) {
    char buf[200];

    s = graphd_constraint_format_string(s, e, sep);
    s = graphd_constraint_format_string(
        s, e, graphd_string_constraint_to_string(strcon, buf, sizeof buf));
    sep = "/";
  }
  return s;
}

char const* graphd_constraint_strqueue_to_string(
    graphd_string_constraint_queue const* q, char const* name, char* buf,
    size_t size) {
  char* s;

  if (q == NULL || q->strqueue_head == NULL) return "";

  buf[0] = ' ';
  s = graphd_constraint_format_strqueue(buf + 1, buf + size, q, name);
  if (s != NULL && s < buf + size) *s = '\0';
  return buf + 1;
}

static char* graphd_constraint_format_generational(
    char* s, char* e, graphd_generational_constraint const* gencon,
    char const* name) {
  char buf[100];

  if (!gencon->gencon_valid) return s;

  if (gencon->gencon_min <= 0)
    if (gencon->gencon_max >= (unsigned long)-1) return s;

  s = graphd_constraint_format_prompt(s, e, name);
  s = graphd_constraint_format_string(
      s, e, generational_to_string(gencon, buf, sizeof buf));
  return s;
}

static char* graphd_constraint_format_with_borders(char* s, char* e,
                                                   graphd_constraint const* con,
                                                   char const* borders) {
  graphd_assignment* a;
  graphd_constraint* sub;
  char buf[200];
  char abuf[200], lbuf[200];
  graphd_constraint_or* cor;

#define SP                                           \
  if (s >= e || s[-1] == ' ' || s[-1] == borders[0]) \
    ;                                                \
  else                                               \
  *s++ = ' '

  if (e - s < 4) {
    while (e - s > 1) *s++ = '.';
    if (s < e) *s = '\0';
    return s;
  }

  /* Reserve space for a closing { ')', '\0' }
   */
  e -= 2;

  /*  Make sure idioms like
   * 	snprintf(s, e - s, ...)
   *	s += strlen(s)
   *
   *  will do the right thing, even if s == e
   *  and snprintf didn't do anything.
   */
  *e = '\0';

  if (graphd_linkage_is_i_am(con->con_linkage)) {
    snprintf(s, (size_t)(e - s), "%s->",
             pdb_linkage_to_string(graphd_linkage_i_am(con->con_linkage)));
    s += strlen(s);
  }
  if (s < e && borders[0] != '\0') *s++ = borders[0];

  if (graphd_linkage_is_my(con->con_linkage)) {
    snprintf(s, (size_t)(e - s), "<-%s",
             pdb_linkage_to_string(graphd_linkage_my(con->con_linkage)));
    s += strlen(s);
  } else if (!graphd_linkage_is_i_am(con->con_linkage)) {
    char const* meta;

    meta = graphd_constraint_meta_to_string(con->con_meta, buf, sizeof buf);
    if (meta == NULL) meta = "???";

    snprintf(s, (size_t)(e - s), "%s", meta);
    s += strlen(s);
  }

  if (con->con_false) {
    SP;
    s = graphd_constraint_format_string(s, e, "FALSE");
  }

  if (con->con_archival != GRAPHD_FLAG_DONTCARE &&
      con->con_archival != GRAPHD_FLAG_UNSPECIFIED) {
    SP;
    s = graphd_constraint_format_string(s, e, "ARCHIVAL=");
    s = graphd_constraint_format_string(
        s, e,
        graphd_constraint_flag_to_string(con->con_archival, abuf, sizeof abuf));
  }
  if (con->con_live != GRAPHD_FLAG_TRUE &&
      con->con_live != GRAPHD_FLAG_UNSPECIFIED) {
    SP;
    s = graphd_constraint_format_string(s, e, "LIVE=");
    s = graphd_constraint_format_string(
        s, e,
        graphd_constraint_flag_to_string(con->con_live, lbuf, sizeof lbuf));
  }
  s = graphd_constraint_format_strqueue(s, e, &con->con_type, NULL);
  s = graphd_constraint_format_strqueue(s, e, &con->con_name, "NAME");
  s = graphd_constraint_format_strqueue(s, e, &con->con_value, "VALUE");

  if (con->con_timestamp_valid)
    s = graphd_constraint_format_timerange(s, e, con->con_timestamp_min,
                                           con->con_timestamp_max, "timestamp");

  if (con->con_dateline.dateline_max != NULL) {
    SP;
    s = graphd_constraint_format_string(s, e, "DATELINE<");
    s = graphd_constraint_format_string(
        s, e, graph_dateline_to_string(con->con_dateline.dateline_max, lbuf,
                                       sizeof lbuf));
  }
  if (con->con_dateline.dateline_min != NULL) {
    SP;
    s = graphd_constraint_format_string(s, e, "DATELINE>");
    s = graphd_constraint_format_string(
        s, e, graph_dateline_to_string(con->con_dateline.dateline_min, lbuf,
                                       sizeof lbuf));
  }
  if (con->con_newest.gencon_valid &&
      (con->con_newest.gencon_min != 0 || con->con_newest.gencon_max != 0)) {
    SP;
    if (con->con_newest.gencon_min == 0 &&
        con->con_newest.gencon_max >= (size_t)-1)
      s = graphd_constraint_format_string(s, e, "NEWEST=*");
    else {
      s = graphd_constraint_format_string(s, e, "NEWEST=");
      s = graphd_constraint_format_string(
          s, e, generational_to_string(&con->con_newest, lbuf, sizeof lbuf));
    }
  }
  s = graphd_constraint_format_generational(s, e, &con->con_oldest, "OLDEST");

  if (con->con_count.countcon_min_valid && con->con_count.countcon_min != 1) {
    SP;
    s = graphd_constraint_format_string(s, e, "COUNT>=");
    snprintf(s, e - s, "%lu", (unsigned long)con->con_count.countcon_min);
    s += strlen(s);
  }
  if (con->con_count.countcon_max_valid) {
    SP;
    s = graphd_constraint_format_string(s, e, "COUNT<=");
    snprintf(s, e - s, "%lu", (unsigned long)con->con_count.countcon_max);
    s += strlen(s);
  }

  s = graphd_constraint_format_guidcon(s, e, &con->con_guid, "GUID");
  s = graphd_constraint_format_guidcon(s, e, &con->con_left, "LEFT");
  s = graphd_constraint_format_guidcon(s, e, &con->con_right, "RIGHT");
  s = graphd_constraint_format_guidcon(s, e, &con->con_scope, "SCOPE");
  s = graphd_constraint_format_guidcon(s, e, &con->con_typeguid, "TYPEGUID");

  if (con->con_valuetype != GRAPH_DATA_UNSPECIFIED) {
    s = graphd_constraint_format_prompt(s, e, "VALUETYPE");
    s = graphd_constraint_format_string(
        s, e, graph_datatype_to_string(con->con_valuetype));
  }
  if (con->con_unique != 0) {
    s = graphd_constraint_format_prompt(s, e, "UNIQUE");
    s = graphd_constraint_format_string(
        s, e, graphd_unique_to_string(con->con_unique, buf, sizeof buf));
  }

  if (con->con_result != graphd_pattern_write_default() &&
      con->con_result != graphd_pattern_read_default() &&
      con->con_result != NULL) {
    s = graphd_constraint_format_prompt(s, e, "RESULT");
    s = graphd_constraint_format_string(
        s, e, graphd_pattern_to_string(con->con_result, buf, sizeof buf));
  }

  for (a = con->con_assignment_head; a != NULL; a = a->a_next) {
    char const *name_s, *name_e;

    graphd_variable_declaration_name(a->a_declaration, &name_s, &name_e);
    if (a->a_declaration->vdecl_constraint == con->con_parent) {
      static char const dots[] = "$..";

      if (s < e && s[-1] != ' ' && s[-1] != '(') *s++ = ' ';
      s = graphd_constraint_format_string(s, e, dots);
      s = graphd_constraint_format_bytes(s, e, name_s + (*name_s == '$'),
                                         name_e);
      s = graphd_constraint_format_string(s, e, "=");
    } else {
      s = graphd_constraint_format_prompt_bytes(s, e, name_s, name_e);
    }
    s = graphd_constraint_format_string(
        s, e, graphd_pattern_to_string(a->a_result, buf, sizeof buf));
  }

  cor = con->con_or_head;
  sub = con->con_head;

  /*  Subconstraints in "or" clauses interleave with
   *  subconstraints outside of "or" clauses.  It's important
   *  to get the order right.
   *
   *  If we're an "or" head or tail, we just want to print those
   *  subconstraints that are actually directly part of us.
   *  They point to us with their con_parent pointer.
   *
   *  If we're the or prototype (if any), we print subconstraints
   *  that are part of an "or" by printing the "or" up to them.
   */
  while (sub != *con->con_tail) {
    graphd_constraint_or* target;

    /*  sub is the subconstraint we're currently
     *  deciding whether or not to print.
     */
    if (sub->con_parent == con) {
      if (s < e && s[-1] != ' ' && s[-1] != '(' /*)*/) *s++ = ' ';

      s = graphd_constraint_format(s, e, sub);
      sub = sub->con_next;

      continue;
    }

    assert(cor != NULL);
    assert(sub->con_parent->con_or != NULL);

    /* Which of our alternatives is this subconstraint under?
     */
    target = graphd_constraint_or_below(con, sub->con_parent);
    assert(target != NULL);
    assert(target->or_prototype == con);

    /* Print the ones before the target alternatives.
     */
    while (cor != target) {
      SP;

      s = graphd_constraint_format_or(s, e, cor);
      cor = cor->or_next;
      assert(cor != NULL);
    }

    /* Print the target alternative.
     */
    assert(cor == target);
    assert(cor != NULL);

    SP;
    s = graphd_constraint_format_or(s, e, cor);

    /*  This implicitly printed at least sub.
     */
    sub = sub->con_next;

    /*  Advance our current subconstraint pointer
     *  until we're out of "cor" land. (Or out
     *  of subconstraints!)
     */
    while (sub != *con->con_tail) {
      if (sub->con_parent == con) break;

      target = graphd_constraint_or_below(con, sub->con_parent);
      assert(target != NULL);

      if (target != cor) break;

      sub = sub->con_next;
    }
    cor = cor->or_next;
  }

  /*  If we have "or"s left to print that didn't involve subconstraints,
   *  print them now.
   */
  while (cor != NULL) {
    SP;

    s = graphd_constraint_format_or(s, e, cor);
    cor = cor->or_next;
  }

  /*  We know we have this space because we reserved it on entry.
   */
  if (borders[1]) *s++ = borders[1];
  *s = '\0';

  return s;
}

static char* graphd_constraint_format_or(char* s, char* e,
                                         graphd_constraint_or const* cor) {
  if (cor == NULL) return s;

  if (e - s < 10) {
    if (e - s < 4) return s;

    memcpy(s - 4, "...", 4);
    return s - 1;
  }

  s = graphd_constraint_format_with_borders(s, e, &cor->or_head, "{}");
  if (e - s >= 5) {
    if (cor->or_short_circuit) {
      memcpy(s, " || ", 5);
      s += 4;
    } else {
      memcpy(s, " | ", 4);
      s += 3;
    }

    if (e - s < 20) {
      if (e - s > 4) {
        memcpy(s, "...", 4);
        return s + 3;
      } else {
        memcpy(s - 4, "...", 4);
        return s - 1;
      }
    }
    s = graphd_constraint_format_with_borders(s, e, cor->or_tail, "{}");
  }
  if (s < e)
    *s = '\0';
  else
    *--s = '\0';

  return s;
}

static char* graphd_constraint_format(char* s, char* e,
                                      graphd_constraint const* con) {
  return graphd_constraint_format_with_borders(s, e, con, "()");
}

/**
 * @brief Render a constraint as a string, for debugging.
 *
 *  If the buffer is smaller than the constraint's rendering,
 *  the call returns an abbreviated rendering - it never
 *  fails altogether, and it's safe to pass the result to
 *  e.g. printf("%s").
 *
 * @param con	the constraint to be rendered
 * @return A non-null string rendering of the constraint.
 */
char const* graphd_constraint_to_string(graphd_constraint* con) {
  if (con == NULL) return "null";

  if (con->con_title == NULL) {
    char* s = graphd_constraint_format(
        con->con_title_buf, con->con_title_buf + sizeof con->con_title_buf,
        con);
    if (s < con->con_title_buf + sizeof con->con_title_buf)
      *s = '\0';
    else
      memcpy(con->con_title_buf + sizeof con->con_title_buf - 4, "..)", 4);

    con->con_title = con->con_title_buf;
  }
  return con->con_title;
}
