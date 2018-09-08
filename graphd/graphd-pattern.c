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

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

static struct graphd_pattern empty_pattern[] = {
    /* link only? / contents only? / forward? */
    /* root */
    {NULL, NULL, GRAPHD_PATTERN_LIST, false, false, true}};

static struct graphd_pattern default_write_pattern[] = {
    /* link only? / contents only? / forward? */
    /* list */
    {NULL, NULL, GRAPHD_PATTERN_LIST, false, false, true},
    /* elements */
    {NULL, NULL, GRAPHD_PATTERN_GUID, false, false, true},
    {NULL, NULL, GRAPHD_PATTERN_CONTENTS, false, true, true},
    {NULL, NULL, GRAPHD_PATTERN_UNSPECIFIED} /* sentinel */
};

static struct graphd_pattern default_read_pattern[] = {
    /* link only? / contents only? / forward? */
    /* root */
    {NULL, NULL, GRAPHD_PATTERN_LIST, false, false, true},
    /* list inside root */
    {NULL, NULL, GRAPHD_PATTERN_LIST, false, false, true},
    /* elements */
    {NULL, NULL, GRAPHD_PATTERN_META, true, false, true},
    {NULL, NULL, GRAPHD_PATTERN_GUID, false, false, true},
    {NULL, NULL, GRAPHD_PATTERN_TYPE, false, false, true},
    {NULL, NULL, GRAPHD_PATTERN_NAME, false, false, true},
    {NULL, NULL, GRAPHD_PATTERN_DATATYPE, false, false, true},
    {NULL, NULL, GRAPHD_PATTERN_VALUE, false, false, true},
    {NULL, NULL, GRAPHD_PATTERN_SCOPE, false, false, true},
    {NULL, NULL, GRAPHD_PATTERN_LIVE, false, false, true},
    {NULL, NULL, GRAPHD_PATTERN_ARCHIVAL, false, false, true},
    {NULL, NULL, GRAPHD_PATTERN_TIMESTAMP, false, false, true},
    {NULL, NULL, GRAPHD_PATTERN_RIGHT, true, false, true},
    {NULL, NULL, GRAPHD_PATTERN_LEFT, true, false, true},
    {NULL, NULL, GRAPHD_PATTERN_CONTENTS, false, true, true},
    {NULL, NULL, GRAPHD_PATTERN_UNSPECIFIED} /* sentinel */
};

graphd_pattern *graphd_pattern_empty(void) {
  if (empty_pattern->pat_data.data_list.list_tail == NULL) {
    empty_pattern->pat_data.data_list.list_tail =
        &empty_pattern->pat_data.data_list.list_head;
  }
  return empty_pattern;
}

graphd_pattern const *graphd_pattern_write_default(void) {
  if (default_write_pattern->pat_data.data_list.list_head == NULL) {
    graphd_pattern *pat = default_write_pattern;
    graphd_pattern *p0 = pat;

    pat->pat_data.data_list.list_head = pat + 1;
    pat++;
    while (pat->pat_type != GRAPHD_PATTERN_UNSPECIFIED) {
      pat->pat_parent = p0;
      pat->pat_next = pat + 1;

      pat++;
    }
    pat[-1].pat_next = NULL;

    p0->pat_data.data_list.list_tail = &pat[-1].pat_next;
    p0->pat_data.data_list.list_n = pat - (p0 + 1);
  }
  return default_write_pattern;
}

graphd_pattern const *graphd_pattern_read_default(void) {
  if (default_read_pattern->pat_data.data_list.list_head == NULL) {
    graphd_pattern *pat = default_read_pattern;
    graphd_pattern *p0 = pat;

    pat->pat_data.data_list.list_head = pat + 1;
    pat->pat_data.data_list.list_tail = &pat[1].pat_next;
    pat->pat_data.data_list.list_n = 1;

    pat++;
    pat->pat_data.data_list.list_head = pat + 1;
    pat->pat_parent = p0;

    pat++;
    while (pat->pat_type != GRAPHD_PATTERN_UNSPECIFIED) {
      pat->pat_parent = p0 + 1;
      pat->pat_next = pat + 1;

      pat++;
    }
    pat[-1].pat_next = NULL;

    p0[1].pat_data.data_list.list_tail = &pat[-1].pat_next;
    p0[1].pat_data.data_list.list_n = pat - (p0 + 2);
  }
  return default_read_pattern;
}

/**
 * @brief Turn a single pattern node into a list.
 * @param greq allocate memory here
 * @param child	Wrap this pattern into a list
 * @return NULL on allocation error, otherwise a list pattern th at
 * 	contains the third argument as its only element.
 */
graphd_pattern *graphd_pattern_wrap(graphd_request *greq,
                                    graphd_pattern *child) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_pattern *pat;

  cl_assert(cl, child != NULL);

  if (greq->greq_pattern_n <
      sizeof(greq->greq_pattern_buf) / sizeof(*greq->greq_pattern_buf)) {
    pat = greq->greq_pattern_buf + greq->greq_pattern_n++;
  } else {
    pat = cm_malloc(greq->greq_req.req_cm, sizeof(*pat));
    if (pat == NULL) {
      cl_log(cl, CL_LEVEL_ERROR,
             "graphd: failed to allocate %lu bytes for "
             "pattern: %s",
             (unsigned long)sizeof(*pat), strerror(errno));
      return NULL;
    }
  }

  memset(pat, 0, sizeof(*pat));

  pat->pat_type = GRAPHD_PATTERN_LIST;
  pat->pat_parent = NULL;
  pat->pat_next = NULL;
  pat->pat_sort_forward = 1;
  pat->pat_list_head = child;
  pat->pat_list_tail = &child->pat_next;
  *pat->pat_list_tail = NULL;
  pat->pat_list_n = 1;

  child->pat_parent = pat;

  return pat;
}

graphd_pattern *graphd_pattern_dup(graphd_request *greq, graphd_pattern *parent,
                                   graphd_pattern const *source) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_pattern *pat;

  cl_assert(cl, parent == NULL || parent->pat_type == GRAPHD_PATTERN_LIST);

  if (greq->greq_pattern_n <
      sizeof(greq->greq_pattern_buf) / sizeof(*greq->greq_pattern_buf)) {
    pat = greq->greq_pattern_buf + greq->greq_pattern_n++;
  } else {
    pat = cm_malloc(greq->greq_req.req_cm, sizeof(*pat));
    if (pat == NULL) {
      cl_log(cl, CL_LEVEL_ERROR,
             "graphd: failed to allocate %lu bytes for "
             "result instruction: %s",
             (unsigned long)sizeof(*pat), strerror(errno));
      return NULL;
    }
  }

  *pat = *source;
  pat->pat_parent = parent;
  pat->pat_next = NULL;

  if (pat->pat_type == GRAPHD_PATTERN_LIST) {
    graphd_pattern *sc;

    pat->pat_list_n = 0;
    pat->pat_list_head = NULL;
    pat->pat_list_tail = &pat->pat_list_head;

    /* Recursively dup the list elements. */
    for (sc = source->pat_list_head; sc != NULL; sc = sc->pat_next) {
      /* No cleanup - it's all on the request heap. */
      if (graphd_pattern_dup(greq, pat, sc) == NULL) return NULL;
    }
  }

  if (parent != NULL) {
    cl_assert(cl, parent->pat_type == GRAPHD_PATTERN_LIST);
    cl_assert(cl, parent->pat_list_tail != NULL);

    *parent->pat_list_tail = pat;
    parent->pat_list_tail = &pat->pat_next;
    parent->pat_list_n++;
  }

  if (pat->pat_type == GRAPHD_PATTERN_VARIABLE)
    pat->pat_variable_declaration->vdecl_linkcount++;

  return pat;
}

void graphd_pattern_append(graphd_request *greq, graphd_pattern *parent,
                           graphd_pattern *child) {
  cl_handle *cl = graphd_request_cl(greq);

  cl_assert(cl, parent->pat_type == GRAPHD_PATTERN_LIST ||
                    parent->pat_type == GRAPHD_PATTERN_PICK);

  *parent->pat_list_tail = child;
  parent->pat_list_tail = &child->pat_next;
  child->pat_parent = parent;

  parent->pat_list_n++;
}

graphd_pattern *graphd_pattern_alloc(graphd_request *greq,
                                     graphd_pattern *parent, int type) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_pattern *pat;

  cl_assert(cl, parent == NULL || GRAPHD_PATTERN_IS_COMPOUND(parent->pat_type));

  if (greq->greq_pattern_n <
      sizeof(greq->greq_pattern_buf) / sizeof(*greq->greq_pattern_buf)) {
    pat = greq->greq_pattern_buf + greq->greq_pattern_n++;
  } else {
    pat = cm_malloc(greq->greq_req.req_cm, sizeof(*pat));
    if (pat == NULL) {
      cl_log(cl, CL_LEVEL_ERROR,
             "graphd: failed to allocate %lu bytes for "
             "result instruction: %s",
             (unsigned long)sizeof(*pat), strerror(errno));
      return NULL;
    }
  }

  memset(pat, 0, sizeof(*pat));
  pat->pat_type = type;
  pat->pat_parent = parent;
  pat->pat_next = NULL;
  pat->pat_sort_forward = 1;

  if (GRAPHD_PATTERN_IS_COMPOUND(type)) {
    pat->pat_list_head = NULL;
    pat->pat_list_tail = &pat->pat_list_head;
  }

  if (parent != NULL) {
    cl_assert(cl, GRAPHD_PATTERN_IS_COMPOUND(parent->pat_type));
    cl_assert(cl, parent->pat_list_tail != NULL);

    *parent->pat_list_tail = pat;
    parent->pat_list_tail = &pat->pat_next;
    parent->pat_list_n++;
  }
  return pat;
}

graphd_pattern *graphd_pattern_alloc_string(graphd_request *greq,
                                            graphd_pattern *parent, int type,
                                            char const *s, char const *e) {
  graphd_pattern *pat;

  pat = graphd_pattern_alloc(greq, parent, type);
  if (!pat) return NULL;

  pat->pat_string_s = s;
  pat->pat_string_e = e;

  return pat;
}

/**
 * @brief  Allocate a "return the value of local variable $x" instruction.
 *
 *  Called by graphd_variable_declare().
 *
 * @param greq		request for whom this is happening
 * @param parent	NULL or pattern to add the variable to
 * @param decl		declaration record for the same variable
 *
 * @return NULL on allocation failure, otherwise a newly allocated
 *	variable pattern.
 */
graphd_pattern *graphd_pattern_alloc_variable(
    graphd_request *greq, graphd_pattern *parent,
    graphd_variable_declaration *vdecl) {
  graphd_pattern *pat;

  pat = graphd_pattern_alloc(greq, parent, GRAPHD_PATTERN_VARIABLE);
  if (pat == NULL) return NULL;

  pat->pat_variable_declaration = vdecl;
  return pat;
}

/**
 * @brief Extract a named value from a primitive under control of a pattern.
 */
int graphd_pattern_from_primitive(graphd_request *greq,
                                  graphd_pattern const *pat,
                                  pdb_primitive const *pr,
                                  graphd_constraint const *con,
                                  graphd_value *val_out) {
  char *str = NULL;
  size_t str_n;
  graph_guid guidval, guid;
  graph_timestamp_t tsval;
  int err = 0, dt;
  graphd_session *gses = graphd_request_session(greq);
  graphd_handle *g = gses->gses_graphd;
  cl_handle *cl = gses->gses_cl;
  pdb_handle *pdb = g->g_pdb;
  pdb_id id;

  switch (pat->pat_type) {
    case GRAPHD_PATTERN_COUNT:
    case GRAPHD_PATTERN_CONTENTS:
    case GRAPHD_PATTERN_CURSOR:
    case GRAPHD_PATTERN_TIMEOUT:
    case GRAPHD_PATTERN_LIST:
    case GRAPHD_PATTERN_PICK:
    case GRAPHD_PATTERN_ESTIMATE:
    case GRAPHD_PATTERN_ESTIMATE_COUNT:
    case GRAPHD_PATTERN_ITERATOR:
      return GRAPHD_ERR_NO;

    case GRAPHD_PATTERN_ARCHIVAL:
      cl_cover(cl);
      graphd_value_boolean_set(val_out, pdb_primitive_is_archival(pr));
      break;

    case GRAPHD_PATTERN_VALUETYPE:
      cl_cover(cl);
      dt = pdb_primitive_valuetype_get(pr);
      graphd_value_number_set(val_out, dt);
      break;

    case GRAPHD_PATTERN_DATATYPE:

      /*  Difference between a datatype and a valuetype:
       *
       *  - the datatype prints as a datatype atom where possible,
       *    as a number otherwise.
       *
       *  - the valuetype always prints as a number.
       */
      cl_cover(cl);
      dt = pdb_primitive_valuetype_get(pr);
      cl_assert(cl, dt >= 1 && dt <= 255);

      graphd_value_datatype_set(cl, val_out, dt);
      break;

    case GRAPHD_PATTERN_GUID:
      cl_cover(cl);
      pdb_primitive_guid_get(pr, guidval);
    have_guid:
      graphd_value_guid_set(val_out, &guidval);
      break;

    case GRAPHD_PATTERN_LEFT:
      if (pdb_primitive_has_left(pr)) {
        cl_cover(cl);
        pdb_primitive_left_get(pr, guidval);
        goto have_guid;
      }
      cl_cover(cl);
    have_null:
      graphd_value_null_set(val_out);
      break;

    case GRAPHD_PATTERN_TYPEGUID:
      if (pdb_primitive_has_typeguid(pr)) {
        cl_cover(cl);
        pdb_primitive_typeguid_get(pr, guidval);
        goto have_guid;
      }
      cl_cover(cl);
      goto have_null;

    case GRAPHD_PATTERN_NONE:
      graphd_value_atom_set_constant(val_out, "", 0);
      break;

    case GRAPHD_PATTERN_LITERAL:

      /*  The literal has the lifetime of the result instructions.
       *  The result instructions last at least until the request
       *  has been replied to - for our purposes, it's constant.
       */
      cl_cover(cl);
      graphd_value_atom_set_constant(val_out,
                                     pat->pat_data.data_string.string_s,
                                     pat->pat_data.data_string.string_e -
                                         pat->pat_data.data_string.string_s);
      break;

    case GRAPHD_PATTERN_LIVE:
      cl_cover(cl);
      graphd_value_boolean_set(val_out, pdb_primitive_is_live(pr));
      break;

    case GRAPHD_PATTERN_META:
      if (pdb_primitive_is_node(pr)) {
        graphd_value_atom_set_constant(val_out, "node", 4);
        cl_cover(cl);
      } else {
        if (con->con_parent != NULL && graphd_linkage_is_my(con->con_linkage) &&
            graphd_linkage_my(con->con_linkage) == PDB_LINKAGE_RIGHT) {
          cl_cover(cl);
          graphd_value_atom_set_constant(val_out, "<-", 2);
        } else {
          cl_cover(cl);
          graphd_value_atom_set_constant(val_out, "->", 2);
        }
      }
      break;

    case GRAPHD_PATTERN_NAME:
      str_n = pdb_primitive_name_get_size(pr); /* Includes 0 */
      if (str_n > 0) {
        str = pdb_primitive_name_get_memory(pr);
        cl_cover(cl);
      }

    have_string:
      if (str_n == 0) {
        cl_cover(cl);
        graphd_value_null_set(val_out);
      } else {
        graphd_value_text_set(val_out, GRAPHD_VALUE_STRING, str,
                              (str + str_n) - 1, pr);
        cl_cover(cl);
      }
      break;

    case GRAPHD_PATTERN_PREVIOUS:
      cl_log(cl, CL_LEVEL_SPEW, "%s:%d: pdb_primitive_has_previous: %d",
             __FILE__, __LINE__, pdb_primitive_has_previous(pr));

      if (pdb_primitive_previous_guid(pdb, pr, &guidval) == 0) goto have_guid;
      cl_cover(cl);
      goto have_null;

    case GRAPHD_PATTERN_NEXT:
      if (pdb_primitive_has_previous(pr)) {
        unsigned long long gen;

        pdb_primitive_guid_get(pr, guid);
        gen = pdb_primitive_generation_get(pr);

        err = pdb_generation_nth(pdb, greq->greq_asof, &guid,
                                 false /* oldest */, gen + 1, NULL, &guidval);
        cl_cover(cl);
      } else {
        /* Either an original or unversioned. */

        pdb_primitive_guid_get(pr, guid);
        err = pdb_generation_nth(pdb, greq->greq_asof, &guid,
                                 false /* oldest */, 1, NULL, &guidval);

        cl_cover(cl);
      }
      if (err == GRAPHD_ERR_NO) {
        cl_cover(cl);
        err = 0;
        goto have_null;
      }
      if (err == 0) {
        cl_cover(cl);
        goto have_guid;
      }
      return err;

    case GRAPHD_PATTERN_GENERATION:
      graphd_value_number_set(val_out, pdb_primitive_has_previous(pr)
                                           ? pdb_primitive_generation_get(pr)
                                           : 0);
      cl_cover(cl);
      return 0;

    case GRAPHD_PATTERN_RIGHT:
      if (pdb_primitive_has_right(pr)) {
        pdb_primitive_right_get(pr, guidval);
        cl_cover(cl);
        goto have_guid;
      }
      cl_cover(cl);
      goto have_null;

    case GRAPHD_PATTERN_SCOPE:
      if (pdb_primitive_has_scope(pr)) {
        pdb_primitive_scope_get(pr, guidval);
        cl_cover(cl);
        goto have_guid;
      }
      cl_cover(cl);
      goto have_null;

    case GRAPHD_PATTERN_TIMESTAMP:
      tsval = pdb_primitive_timestamp_get(pr);
      pdb_primitive_guid_get(pr, guidval);
      err = pdb_id_from_guid(pdb, &id, &guidval);
      if (err != 0) id = PDB_ID_NONE;
      graphd_value_timestamp_set(val_out, tsval, id);
      cl_cover(cl);
      break;

    case GRAPHD_PATTERN_TYPE:
      if (pdb_primitive_has_typeguid(pr)) {
        pdb_primitive_typeguid_get(pr, guid);
        err = graphd_type_value_from_guid(g, greq->greq_asof, &guid, val_out);
        cl_cover(cl);
        break;
      }
      cl_cover(cl);
      goto have_null;

    case GRAPHD_PATTERN_VALUE:
      str_n = pdb_primitive_value_get_size(pr); /* Includes 0 */
      if (str_n > 0) {
        str = pdb_primitive_value_get_memory(pr);
        cl_cover(cl);
      }
      goto have_string;

    case GRAPHD_PATTERN_VARIABLE:
      graphd_value_atom_set_constant(val_out, "?v?", 3);
      cl_cover(cl);
      break;

    default:
      cl_notreached(cl, "unexpected result instruction type %d", pat->pat_type);
      break;
  }
  return err;
}

/**
 * @brief Set a pattern to "()"
 * @param pat pattern lvalue to assign in.
 */
void graphd_pattern_null(graphd_pattern *pat) {
  memset(pat, 0, sizeof(*pat));

  pat->pat_parent = NULL;
  pat->pat_next = NULL;
  pat->pat_type = GRAPHD_PATTERN_LIST;

  pat->pat_list_head = NULL;
  pat->pat_list_tail = &pat->pat_list_head;
  pat->pat_list_n = 0;
  pat->pat_sort_forward = true;
}

/**
 * @brief Fill in a pattern for no primitives
 * @param cl log through this
 * @param pat shape the result should have
 * @param val value to fill in
 */
int graphd_pattern_from_null(cl_handle *cl, graphd_pattern const *pat,
                             graphd_value *val) {
  int err = 0;

  switch (pat->pat_type) {
    case GRAPHD_PATTERN_COUNT:
    case GRAPHD_PATTERN_CURSOR:
    case GRAPHD_PATTERN_TIMEOUT:
    case GRAPHD_PATTERN_ESTIMATE:
    case GRAPHD_PATTERN_ESTIMATE_COUNT:
    case GRAPHD_PATTERN_ITERATOR:
    case GRAPHD_PATTERN_LIST:
      return GRAPHD_ERR_NO;

    case GRAPHD_PATTERN_PICK:
    case GRAPHD_PATTERN_GENERATION:
    case GRAPHD_PATTERN_ARCHIVAL:
    case GRAPHD_PATTERN_DATATYPE:
    case GRAPHD_PATTERN_VALUETYPE:
    case GRAPHD_PATTERN_GUID:
    case GRAPHD_PATTERN_LEFT:
    case GRAPHD_PATTERN_LIVE:
    case GRAPHD_PATTERN_META:
    case GRAPHD_PATTERN_NAME:
    case GRAPHD_PATTERN_NEXT:
    case GRAPHD_PATTERN_PREVIOUS:
    case GRAPHD_PATTERN_RIGHT:
    case GRAPHD_PATTERN_SCOPE:
    case GRAPHD_PATTERN_TIMESTAMP:
    case GRAPHD_PATTERN_TYPE:
    case GRAPHD_PATTERN_TYPEGUID:
    case GRAPHD_PATTERN_VALUE:
    case GRAPHD_PATTERN_VARIABLE:
    case GRAPHD_PATTERN_CONTENTS:
      graphd_value_atom_set_constant(val, "null", 4);
      cl_cover(cl);
      break;

    case GRAPHD_PATTERN_NONE:
      graphd_value_atom_set_constant(val, "", 0);
      cl_cover(cl);
      break;

    case GRAPHD_PATTERN_LITERAL:
      graphd_value_atom_set_constant(val, pat->pat_data.data_string.string_s,
                                     pat->pat_data.data_string.string_e -
                                         pat->pat_data.data_string.string_s);
      cl_cover(cl);
      break;

    default:
      cl_notreached(cl, "unexpected result instruction type %d", pat->pat_type);
      break;
  }
  return err;
}

/**
 * @brief Return a human-readable (partial) string representation of a pattern.
 * @param pat 	NULL or the pattern to format
 * @param buf 	use this buffer to format
 * @param size	number of  usable bytes pointed ot by buf
 * @return a pointer to the beginning of the NUL-terminated string.
 */
char const *graphd_pattern_type_to_string(graphd_pattern_type pat, char *buf,
                                          size_t size) {
  switch (pat) {
    case GRAPHD_PATTERN_PICK:
      return "pick";
    case GRAPHD_PATTERN_LIST:
      return "list";
    case GRAPHD_PATTERN_VARIABLE:
      return "variable";
    case GRAPHD_PATTERN_LITERAL:
      return "literal";
    case GRAPHD_PATTERN_UNSPECIFIED:
      return "unspecified";
    case GRAPHD_PATTERN_ARCHIVAL:
      return "archival";
    case GRAPHD_PATTERN_DATATYPE:
      return "datatype";
    case GRAPHD_PATTERN_VALUETYPE:
      return "valuetype";
    case GRAPHD_PATTERN_GENERATION:
      return "generation";
    case GRAPHD_PATTERN_GUID:
      return "guid";
    case GRAPHD_PATTERN_LEFT:
      return "left";
    case GRAPHD_PATTERN_LIVE:
      return "live";
    case GRAPHD_PATTERN_META:
      return "meta";
    case GRAPHD_PATTERN_NAME:
      return "name";
    case GRAPHD_PATTERN_NEXT:
      return "next";
    case GRAPHD_PATTERN_RIGHT:
      return "right";
    case GRAPHD_PATTERN_PREVIOUS:
      return "previous";
    case GRAPHD_PATTERN_SCOPE:
      return "scope";
    case GRAPHD_PATTERN_ESTIMATE:
      return "estimate";
    case GRAPHD_PATTERN_ESTIMATE_COUNT:
      return "estimate-count";
    case GRAPHD_PATTERN_ITERATOR:
      return "iterator";
    case GRAPHD_PATTERN_TIMESTAMP:
      return "timestamp";
    case GRAPHD_PATTERN_TYPE:
      return "type";
    case GRAPHD_PATTERN_TYPEGUID:
      return "typeguid";
    case GRAPHD_PATTERN_VALUE:
      return "value";
    case GRAPHD_PATTERN_COUNT:
      return "count";
    case GRAPHD_PATTERN_CURSOR:
      return "cursor";
    case GRAPHD_PATTERN_TIMEOUT:
      return "timeout";
    case GRAPHD_PATTERN_CONTENTS:
      return "contents";
    case GRAPHD_PATTERN_NONE:
      return "none";
  }

  snprintf(buf, size, "unexpected pattern %x", pat);
  return buf;
}

/**
 * @brief Return a human-readable (partial) string representation of a pattern.
 * @param pat 	NULL or the pattern to format
 * @param buf 	use this buffer to format
 * @param size	number of  usable bytes pointed ot by buf
 * @return a pointer to the beginning of the NUL-terminated string.
 */
char const *graphd_pattern_to_string(graphd_pattern const *pat, char *buf,
                                     size_t size) {
  char *w, *e;
  char const *sep = "";
  char const *b0 = buf;
  char const *label = NULL;
  char const *decoration;
  graphd_pattern const *sub;

  if (pat == NULL) return "null";

  if (size > 1 && !pat->pat_sort_forward) {
    *buf++ = '-';
    size--;
  }
  switch (pat->pat_type) {
    case GRAPHD_PATTERN_LIST:
      decoration = "()";
      break;
    case GRAPHD_PATTERN_PICK:
      decoration = "<>";
      break;
      snprintf(buf, size, "%.*s", (int)(pat->pat_data.data_string.string_e -
                                        pat->pat_data.data_string.string_s),
               pat->pat_data.data_string.string_s);
      return b0;

    case GRAPHD_PATTERN_VARIABLE: {
      char const *name_s, *name_e;
      graphd_variable_declaration_name(pat->pat_variable_declaration, &name_s,
                                       &name_e);
      snprintf(buf, size, "%.*s", (int)(name_e - name_s), name_s);
      return buf;
    }

    case GRAPHD_PATTERN_NONE:
      snprintf(buf, size, "\"\"");
      return b0;

    case GRAPHD_PATTERN_LITERAL:
      snprintf(buf, size, "\"%.*s\"", (int)(pat->pat_data.data_string.string_e -
                                            pat->pat_data.data_string.string_s),
               pat->pat_data.data_string.string_s);
      return b0;

    case GRAPHD_PATTERN_UNSPECIFIED:
      label = "unspecified";
      break;
    case GRAPHD_PATTERN_ARCHIVAL:
      label = "archival";
      break;
    case GRAPHD_PATTERN_DATATYPE:
      label = "datatype";
      break;
    case GRAPHD_PATTERN_VALUETYPE:
      label = "valuetype";
      break;
    case GRAPHD_PATTERN_GENERATION:
      label = "generation";
      break;
    case GRAPHD_PATTERN_GUID:
      label = "guid";
      break;
    case GRAPHD_PATTERN_LEFT:
      label = "left";
      break;
    case GRAPHD_PATTERN_LIVE:
      label = "live";
      break;
    case GRAPHD_PATTERN_META:
      label = "meta";
      break;
    case GRAPHD_PATTERN_NAME:
      label = "name";
      break;
    case GRAPHD_PATTERN_NEXT:
      label = "next";
      break;
    case GRAPHD_PATTERN_RIGHT:
      label = "right";
      break;
    case GRAPHD_PATTERN_PREVIOUS:
      label = "previous";
      break;
    case GRAPHD_PATTERN_SCOPE:
      label = "scope";
      break;
    case GRAPHD_PATTERN_ESTIMATE:
      label = "estimate";
      break;
    case GRAPHD_PATTERN_ESTIMATE_COUNT:
      label = "estimate-count";
      break;
    case GRAPHD_PATTERN_ITERATOR:
      label = "iterator";
      break;
    case GRAPHD_PATTERN_TIMESTAMP:
      label = "timestamp";
      break;
    case GRAPHD_PATTERN_TYPE:
      label = "type";
      break;
    case GRAPHD_PATTERN_TYPEGUID:
      label = "typeguid";
      break;
    case GRAPHD_PATTERN_VALUE:
      label = "value";
      break;
    case GRAPHD_PATTERN_COUNT:
      label = "count";
      break;
    case GRAPHD_PATTERN_CURSOR:
      label = "cursor";
      break;
    case GRAPHD_PATTERN_TIMEOUT:
      label = "timeout";
      break;
    case GRAPHD_PATTERN_CONTENTS:
      label = "contents";
      break;
    default:
      snprintf(buf, size, "unexpected result instruction %x", pat->pat_type);
      return b0;
  }

  if (label != NULL) {
    snprintf(buf, size, "%s", label);
    return b0;
  }

  if (size <= 10)
    return pat->pat_type == GRAPHD_PATTERN_PICK ? "<...>" : "(...)";

  e = buf + size - 5;
  w = buf;
  *w++ = decoration[0];

  for (sub = pat->pat_list_head; sub != NULL; sub = sub->pat_next) {
    char const *ptr;
    char b[200];

    ptr = graphd_pattern_to_string(sub, b, sizeof b);
    snprintf(w, (int)(e - w), "%s%s", sep, ptr);
    sep = ", ";
    if (w < e) w += strlen(w);

    if (w < e && pat->pat_type == GRAPHD_PATTERN_PICK) {
      snprintf(w, (int)(e - w), "@%zu", sub->pat_or_index);
      w += strlen(w);
    }

    if (e - w <= 5) {
      break;
    }
  }
  *w++ = decoration[1];
  *w = '\0';

  return b0;
}

/**
 * @brief Like graphd_pattern_to_string, but with more detail.
 * @param pat 	NULL or the pattern to format
 * @param buf 	use this buffer to format
 * @param size	number of  usable bytes pointed ot by buf
 * @return a pointer to the beginning of the NUL-terminated string.
 */
char const *graphd_pattern_dump(graphd_pattern const *pat, char *buf,
                                size_t size) {
  char *w, *e;
  char const *sep = "";
  char const *b0 = buf;
  char const *label = NULL;
  char const *decoration, *name_s, *name_e;
  graphd_pattern const *sub;

  if (pat == NULL) return "null";

  if (size > 1 && !pat->pat_sort_forward) {
    *buf++ = '-';
    size--;
  }

  if (size > 1 && pat->pat_sample) {
    *buf++ = '^';
    size--;
  }

  if (size > 2 && pat->pat_collect) {
    *buf++ = '[';
    size -= 2;
  }

  if (size >= 20 && pat->pat_or_index != 0) {
    snprintf(buf, size, "{%zu}", pat->pat_or_index);

    size -= strlen(buf);
    buf += strlen(buf);
  }

  switch (pat->pat_type) {
    case GRAPHD_PATTERN_VARIABLE:
      graphd_variable_declaration_name(pat->pat_variable_declaration, &name_s,
                                       &name_e);
      snprintf(buf, size, "%.*s%s", (int)(name_e - name_s), name_s,
               pat->pat_collect ? "]" : "");
      return b0;

    case GRAPHD_PATTERN_LIST:
      decoration = "()";
      break;
    case GRAPHD_PATTERN_PICK:
      decoration = "<>";
      break;
      snprintf(buf, size, "%.*s%s", (int)(pat->pat_data.data_string.string_e -
                                          pat->pat_data.data_string.string_s),
               pat->pat_data.data_string.string_s, pat->pat_collect ? "]" : "");
      return b0;

    case GRAPHD_PATTERN_NONE:
      snprintf(buf, size, "\"\"%s", pat->pat_collect ? "]" : "");
      return b0;

    case GRAPHD_PATTERN_LITERAL:
      snprintf(buf, size, "\"%.*s\"%s",
               (int)(pat->pat_data.data_string.string_e -
                     pat->pat_data.data_string.string_s),
               pat->pat_data.data_string.string_s, pat->pat_collect ? "]" : "");
      return b0;

    case GRAPHD_PATTERN_UNSPECIFIED:
      label = "unspecified";
      break;
    case GRAPHD_PATTERN_ARCHIVAL:
      label = "archival";
      break;
    case GRAPHD_PATTERN_DATATYPE:
      label = "datatype";
      break;
    case GRAPHD_PATTERN_VALUETYPE:
      label = "valuetype";
      break;
    case GRAPHD_PATTERN_GENERATION:
      label = "generation";
      break;
    case GRAPHD_PATTERN_GUID:
      label = "guid";
      break;
    case GRAPHD_PATTERN_LEFT:
      label = "left";
      break;
    case GRAPHD_PATTERN_LIVE:
      label = "live";
      break;
    case GRAPHD_PATTERN_META:
      label = "meta";
      break;
    case GRAPHD_PATTERN_NAME:
      label = "name";
      break;
    case GRAPHD_PATTERN_NEXT:
      label = "next";
      break;
    case GRAPHD_PATTERN_RIGHT:
      label = "right";
      break;
    case GRAPHD_PATTERN_PREVIOUS:
      label = "previous";
      break;
    case GRAPHD_PATTERN_SCOPE:
      label = "scope";
      break;
    case GRAPHD_PATTERN_ESTIMATE:
      label = "estimate";
      break;
    case GRAPHD_PATTERN_ESTIMATE_COUNT:
      label = "estimate-count";
      break;
    case GRAPHD_PATTERN_ITERATOR:
      label = "iterator";
      break;
    case GRAPHD_PATTERN_TIMESTAMP:
      label = "timestamp";
      break;
    case GRAPHD_PATTERN_TYPE:
      label = "type";
      break;
    case GRAPHD_PATTERN_TYPEGUID:
      label = "typeguid";
      break;
    case GRAPHD_PATTERN_VALUE:
      label = "value";
      break;
    case GRAPHD_PATTERN_COUNT:
      label = "count";
      break;
    case GRAPHD_PATTERN_CURSOR:
      label = "cursor";
      break;
    case GRAPHD_PATTERN_TIMEOUT:
      label = "timeout";
      break;
    case GRAPHD_PATTERN_CONTENTS:
      label = "contents";
      break;
    default:
      snprintf(buf, size, "unexpected result instruction %x%s", pat->pat_type,
               pat->pat_collect ? "]" : "");
      return b0;
  }

  if (label != NULL) {
    snprintf(buf, size, "%s%s", label, pat->pat_collect ? "]" : "");
    return b0;
  }

  if (size <= 10)
    return pat->pat_type == GRAPHD_PATTERN_PICK ? "<...>" : "(...)";

  e = buf + size - 5;
  w = buf;
  *w++ = decoration[0];

  for (sub = pat->pat_list_head; sub != NULL; sub = sub->pat_next) {
    char const *ptr;
    char b[200];

    ptr = graphd_pattern_dump(sub, b, sizeof b);
    snprintf(w, (int)(e - w), "%s%s", sep, ptr);
    sep = ", ";
    if (w < e) w += strlen(w);

    if (w < e && pat->pat_type == GRAPHD_PATTERN_PICK) {
      snprintf(w, (int)(e - w), "@%zu", sub->pat_or_index);
      w += strlen(w);
    }

    if (e - w <= 5) {
      break;
    }
  }
  *w++ = decoration[1];
  if (pat->pat_collect) *w++ = ']';
  *w = '\0';

  return b0;
}

/**
 * @brief How deeply nested is this pattern?
 *
 * @param pat	The pattern we're looking into
 * @return the maximum number of nested parentheses in this pattern.
 */
int graphd_pattern_depth(graphd_pattern const *pat) {
  int depth, d, best_depth = -1;

  if (pat == NULL || !GRAPHD_PATTERN_IS_COMPOUND(pat->pat_type)) return 0;

  best_depth = depth = pat->pat_type == GRAPHD_PATTERN_PICK;
  for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next)
    if (GRAPHD_PATTERN_IS_COMPOUND(pat->pat_type)) {
      d = depth + graphd_pattern_depth(pat);
      if (d > best_depth) best_depth = d;
    }

  return best_depth;
}

/**
 * @brief Return that part of a result pattern that is
 *	repeated for each matching alternative.
 *
 *  That's the part of the pattern that's nested two levels deep.
 *  For example, in (cursor count (name value guid)), it would be
 *  (name value guid).
 *  (Only one such pattern can exist per result or assignment clause.)
 *
 * @param pat	The pattern we're looking into
 * @return NULL if nothing is repeated for each alternative, otherwise
 *	a pointer to the list of subvalues.
 */
graphd_pattern *graphd_pattern_per_match(graphd_pattern const *pat) {
  if (pat == NULL || pat->pat_type != GRAPHD_PATTERN_LIST) return NULL;

  for (pat = pat->pat_data.data_list.list_head; pat != NULL;
       pat = pat->pat_next)

    if (pat->pat_type == GRAPHD_PATTERN_LIST) break;

  return (graphd_pattern *)pat;
}

unsigned long long graphd_pattern_spectrum(graphd_pattern const *pat) {
  unsigned long long res = 0;

  if (pat == NULL) return 0;

  res |= 1ull << pat->pat_type;
  if (!GRAPHD_PATTERN_IS_COMPOUND(pat->pat_type)) return res;

  for (pat = pat->pat_data.data_list.list_head; pat != NULL;
       pat = pat->pat_next) {
    if (GRAPHD_PATTERN_IS_COMPOUND(pat->pat_type))
      res |= graphd_pattern_spectrum(pat);
    else
      res |= 1ull << pat->pat_type;
  }
  return res;
}

graphd_pattern *graphd_pattern_lookup(graphd_pattern const *pat, int type) {
  if (pat == NULL) return NULL;

  if (pat->pat_type == type) return (graphd_pattern *)pat;

  if (!GRAPHD_PATTERN_IS_COMPOUND(pat->pat_type)) return NULL;

  for (pat = pat->pat_data.data_list.list_head; pat != NULL;
       pat = pat->pat_next) {
    if (pat->pat_type == type) return (graphd_pattern *)pat;

    if (GRAPHD_PATTERN_IS_COMPOUND(pat->pat_type)) {
      graphd_pattern *p2 = graphd_pattern_lookup(pat, type);
      if (p2 != NULL) return p2;
    }
  }
  return NULL;
}

/**
 * @param Return the next node in a graphd_pattern tree, in preorder.
 *
 *  Use: for (pat = ...; pat != NULL; pat = graphd_pattern_preorder_next(pat))
 *
 * @param pat	Last node that this function returned, or the root.
 * @return NULL if the previous node was the last
 * @return otherwise, the next node in preorder (node, child 1, child 2, ..)
 */
graphd_pattern *graphd_pattern_preorder_next(graphd_pattern const *pat) {
  if (GRAPHD_PATTERN_IS_COMPOUND(pat->pat_type) && pat->pat_list_head != NULL)
    return pat->pat_list_head;
  do {
    if (pat->pat_next != NULL) return pat->pat_next;

  } while ((pat = pat->pat_parent) != NULL);

  return NULL;
}

/* Copy <from> to <to>.
 */
int graphd_pattern_dup_in_place(cm_handle *cm, graphd_pattern *to,
                                graphd_pattern const *from) {
  graphd_pattern tmp = *to;
  graphd_pattern **to_child, *p;

  *to = *from;

  /*  Restore next and parent pointer of
   *  the destination.
   */
  to->pat_next = tmp.pat_next;
  to->pat_parent = tmp.pat_parent;

  if (GRAPHD_PATTERN_IS_COMPOUND(to->pat_type)) {
    /*  Duplicate the element list.
     *  Elements point to their parent;
     *  without the duplication, these
     *  elements would have two parents.
     */
    to_child = &to->pat_list_head;
    for (p = from->pat_list_head; p != NULL; p = p->pat_next) {
      graphd_pattern *p_new;
      int err;

      p_new = cm_zalloc(cm, sizeof(*p_new));
      if (p_new == NULL) return errno ? errno : ENOMEM;

      err = graphd_pattern_dup_in_place(cm, p_new, p);
      if (err != 0) {
        /*  Allocations are all on heap;
         *  no need to free them individually.
         */
        return err;
      }
      p_new->pat_parent = to;

      *to_child = p_new;
      to_child = &p_new->pat_next;
    }
    *to_child = NULL;
  }
  return 0;
}

/**
 * @brief Do these two patterns refer to the same value?
 *
 *  Unlike graphd_pattern_equal(), sort order doesn't matter here.
 *
 * @param cl 	Log through here
 * @param a 	pattern
 * @param b 	another pattern
 *
 * @return true if they're equal, false otherwise.
 */
bool graphd_pattern_equal_value(cl_handle *cl, graphd_constraint const *a_con,
                                graphd_pattern const *a,
                                graphd_constraint const *b_con,
                                graphd_pattern const *b) {
  char abuf[200], bbuf[200];
  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_pattern_equal_value(%s, %s)",
         graphd_pattern_to_string(a, abuf, sizeof abuf),
         graphd_pattern_to_string(b, bbuf, sizeof bbuf));

  if (a == NULL && b == NULL) return true;

  if (a == NULL || b == NULL) return false;

  cl_assert(cl, a != NULL);
  cl_assert(cl, b != NULL);

  if (a->pat_type != b->pat_type) return false;

  switch (a->pat_type) {
    default:
      break;

    case GRAPHD_PATTERN_NONE:
      return true;

    case GRAPHD_PATTERN_LITERAL:
      if (a->pat_string_e - a->pat_string_s !=
          b->pat_string_e - b->pat_string_s)
        return false;
      return memcmp(a->pat_string_s, b->pat_string_s,
                    (size_t)(b->pat_string_s - a->pat_string_s)) == 0;

    case GRAPHD_PATTERN_VARIABLE:
      return a->pat_variable_declaration == b->pat_variable_declaration;

    case GRAPHD_PATTERN_PICK:
      for (a = a->pat_list_head, b = b->pat_list_head; a != NULL && b != NULL;
           a = a->pat_next, b = b->pat_next) {
        if (!graphd_pattern_equal_value(cl, a_con, a, b_con, b)) return false;
        if (a->pat_or_index != b->pat_or_index) return false;
      }
      return a == NULL && b == NULL;

    case GRAPHD_PATTERN_LIST:
      for (a = a->pat_list_head, b = b->pat_list_head; a != NULL && b != NULL;
           a = a->pat_next, b = b->pat_next) {
        if (!graphd_pattern_equal_value(cl, a_con, a, b_con, b)) return false;
      }
      return a == NULL && b == NULL;
  }
  return true;
}

/**
 * @brief Are these two patterns equal?
 *
 *  False negatives are okay.
 *
 * @param cl 	Log through here
 * @param a 	pattern
 * @param b 	another pattern
 *
 * @return true if they're equal, false otherwise.
 */
bool graphd_pattern_equal(cl_handle *cl, graphd_constraint const *a_con,
                          graphd_pattern const *a,
                          graphd_constraint const *b_con,
                          graphd_pattern const *b) {
  if (a == NULL && b == NULL) return true;

  if (a == NULL || b == NULL) return false;

  cl_assert(cl, a != NULL);
  cl_assert(cl, b != NULL);

  if (a->pat_type != b->pat_type || a->pat_sort_forward != b->pat_sort_forward)
    return false;

  switch (a->pat_type) {
    default:
      break;

    case GRAPHD_PATTERN_NONE:
      return true;

    case GRAPHD_PATTERN_LITERAL:
      if (a->pat_string_e - a->pat_string_s !=
          b->pat_string_e - b->pat_string_s)
        return false;
      return memcmp(a->pat_string_s, b->pat_string_s,
                    (size_t)(b->pat_string_s - a->pat_string_s)) == 0;

    case GRAPHD_PATTERN_VARIABLE:
      return graphd_variable_declaration_equal(
          cl, a_con, a->pat_variable_declaration, b_con,
          b->pat_variable_declaration);

    case GRAPHD_PATTERN_PICK:
    case GRAPHD_PATTERN_LIST:
      for (a = a->pat_list_head, b = b->pat_list_head; a != NULL && b != NULL;
           a = a->pat_next, b = b->pat_next) {
        if (!graphd_pattern_equal(cl, a_con, a, b_con, b)) return false;
      }
      return a == NULL && b == NULL;
  }
  return true;
}

/**
 * @brief Add a pattern to a hash.
 *
 * @param cl 		Log through here
 * @param pat 		pattern
 * @param hash_inout 	hash accumulator
 */
void graphd_pattern_hash(cl_handle *const cl, graphd_pattern const *pat,
                         unsigned long *const hash_inout) {
  char const *name_s, *name_e;

  if (pat == NULL) return;

  GRAPHD_HASH_VALUE(*hash_inout, pat->pat_type);
  GRAPHD_HASH_BIT(*hash_inout, pat->pat_sort_forward);

  switch (pat->pat_type) {
    default:
      break;

    case GRAPHD_PATTERN_LITERAL:
      GRAPHD_HASH_BYTES(*hash_inout, pat->pat_string_s, pat->pat_string_e);
      break;

    case GRAPHD_PATTERN_VARIABLE:
      cl_assert(cl, pat->pat_variable_declaration != NULL);
      graphd_variable_declaration_name(pat->pat_variable_declaration, &name_s,
                                       &name_e);
      GRAPHD_HASH_BYTES(*hash_inout, name_s, name_e);
      break;

    case GRAPHD_PATTERN_LIST:
    case GRAPHD_PATTERN_PICK:
      for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next)
        graphd_pattern_hash(cl, pat, hash_inout);
      break;
  }
}

/**
 * @brief Return the first non-list element of a pattern.
 */
bool graphd_pattern_head(graphd_pattern const *pat, graphd_pattern *out) {
  bool forward;

  if (pat == NULL) return false;

  if (pat->pat_type != GRAPHD_PATTERN_LIST) {
    *out = *pat;
    return true;
  }

  forward = pat->pat_sort_forward;
  for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next)
    if (graphd_pattern_head(pat, out)) {
      out->pat_sort_forward ^= !forward;
      return true;
    }

  return false;
}

bool graphd_pattern_inside(graphd_pattern const *pat, int type) {
  while (pat->pat_parent != NULL) {
    if (pat->pat_parent->pat_type == type) return true;
    pat = pat->pat_parent;
  }
  return false;
}

bool graphd_pattern_is_set_dependent(cl_handle *cl,
                                     graphd_constraint const *con,
                                     graphd_pattern const *pat) {
  graphd_assignment *a;

  if (GRAPHD_PATTERN_IS_SET_VALUE(pat->pat_type)) return true;

  if (GRAPHD_PATTERN_IS_PRIMITIVE_VALUE(pat->pat_type)) return false;

  switch (pat->pat_type) {
    case GRAPHD_PATTERN_PICK:
    case GRAPHD_PATTERN_LIST:
      for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next)
        if (graphd_pattern_is_set_dependent(cl, con, pat)) return true;
      return false;

    case GRAPHD_PATTERN_VARIABLE:
      a = graphd_assignment_by_declaration(con, pat->pat_variable_declaration);
      if (a == NULL) return false;

      return graphd_pattern_is_set_dependent(cl, con, a->a_result);

    default:
      break;
  }
  return false;
}

bool graphd_pattern_is_sort_dependent(cl_handle *cl,
                                      graphd_constraint const *con,
                                      graphd_pattern const *pat) {
  graphd_assignment *a;
  graphd_pattern const *p0 = pat;

  if (GRAPHD_PATTERN_IS_PRIMITIVE_VALUE(pat->pat_type)) return true;

  switch (pat->pat_type) {
    case GRAPHD_PATTERN_CONTENTS:
      return true;

    case GRAPHD_PATTERN_PICK:
    case GRAPHD_PATTERN_LIST:
      for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next)
        if (graphd_pattern_is_sort_dependent(cl, con, pat)) return true;

      char buf[200];
      cl_log(cl, CL_LEVEL_DEBUG, "graphd_pattern_is_sort_dependent: %s isn't.",
             graphd_pattern_to_string(p0, buf, sizeof buf));
      return false;

    case GRAPHD_PATTERN_VARIABLE:
      a = graphd_assignment_by_declaration(con, pat->pat_variable_declaration);

      /*  If we're not assigning to it ourselves, someone
       *  below us is - and that means that its value likely
       *  depends on the specific ID whose subconstraints
       *  we're looking at.
       */
      if (a == NULL) return true;

      return graphd_pattern_is_sort_dependent(cl, con, a->a_result);

    default:
      break;
  }

  char buf[200];
  cl_log(cl, CL_LEVEL_DEBUG,
         "graphd_pattern_is_sort_dependent: %s isn't (default case)",
         graphd_pattern_to_string(pat, buf, sizeof buf));
  return false;
}

bool graphd_pattern_is_primitive_dependent(cl_handle *cl,
                                           graphd_constraint const *con,
                                           graphd_pattern const *pat) {
  graphd_assignment *a;

  if (GRAPHD_PATTERN_IS_SET_VALUE(pat->pat_type)) return false;

  if (GRAPHD_PATTERN_IS_PRIMITIVE_VALUE(pat->pat_type)) return true;

  switch (pat->pat_type) {
    case GRAPHD_PATTERN_PICK:
      /* Even though the patterns may not be
       * primitive dependent, the "or" distribution
       * in them is.
       */
      return true;

    case GRAPHD_PATTERN_LIST:
      for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next)
        if (graphd_pattern_is_primitive_dependent(cl, con, pat)) return true;
      return false;

    case GRAPHD_PATTERN_VARIABLE:
      a = graphd_assignment_by_declaration(con, pat->pat_variable_declaration);
      if (a == NULL) return false;
      return graphd_pattern_is_primitive_dependent(cl, con, a->a_result);

    default:
      break;
  }
  return false;
}

void graphd_pattern_variable_rename(graphd_pattern *pat,
                                    graphd_variable_declaration *source,
                                    graphd_variable_declaration *dest) {
  if (pat == NULL) return;

  if (pat->pat_type == GRAPHD_PATTERN_VARIABLE &&
      pat->pat_variable_declaration == source)
    pat->pat_variable_declaration = dest;

  else if (GRAPHD_PATTERN_IS_COMPOUND(pat->pat_type))
    for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next)
      graphd_pattern_variable_rename(pat, source, dest);
}

int graphd_pattern_move_declaration_target(graphd_request *greq,
                                           graphd_pattern *pat,
                                           graphd_constraint *old_con,
                                           graphd_constraint *new_con) {
  int err;
  char const *name_s, *name_e;
  graphd_variable_declaration *new_vdecl;

  if (pat == NULL) return 0;

  if (GRAPHD_PATTERN_IS_COMPOUND(pat->pat_type)) {
    for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next) {
      err = graphd_pattern_move_declaration_target(greq, pat, old_con, new_con);
      if (err != 0) return err;
    }
    return 0;
  } else if (pat->pat_type != GRAPHD_PATTERN_VARIABLE ||
             pat->pat_variable_declaration == NULL ||
             pat->pat_variable_declaration->vdecl_constraint != old_con)
    return 0;

  /* Unify the variable declaration here with one
   * in <new_con>; create one there if it doesn't exist yet.
   */
  graphd_variable_declaration_name(pat->pat_variable_declaration, &name_s,
                                   &name_e);

  new_vdecl = graphd_variable_declaration_by_name(new_con, name_s, name_e);
  if (new_vdecl == NULL) {
    new_vdecl = graphd_variable_declaration_add(greq->greq_req.req_cm,
                                                graphd_request_cl(greq),
                                                new_con, name_s, name_e);
    if (new_vdecl == NULL) return errno ? errno : ENOMEM;
  }

  pat->pat_variable_declaration = new_vdecl;
  return 0;
}
