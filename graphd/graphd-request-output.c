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
#include <stdio.h>
#include <string.h>

#define NUMBER_MAX_SIZE 42

static int format_value(graphd_handle *g, graphd_session *gses,
                        graphd_request *greq, char **s, char *e);

typedef struct {
  size_t frc_i;
  pdb_primitive frc_pr;
  int frc_field;
  int frc_err;
  unsigned int frc_quoted : 1;

} graphd_format_records_context;

static void format_error(void *data, srv_handle *srv, void *session_data,
                         void *request_data, char **s, char *e);

static graphd_value *format_stack_top(graphd_request *greq) {
  if (greq->greq_format_stack_n == 0) return NULL;
  return greq->greq_format_stack[greq->greq_format_stack_n - 1];
}

static graphd_value *format_stack_pop(graphd_request *greq) {
  if (greq->greq_format_stack_n == 0) return NULL;
  greq->greq_format_list_first = 0;
  return greq->greq_format_stack[--greq->greq_format_stack_n];
}

int graphd_format_stack_push(graphd_session *gses, graphd_request *greq,
                             graphd_value *t) {
  cl_assert(gses->gses_cl, t->val_type != GRAPHD_VALUE_UNSPECIFIED);
  if (greq->greq_format_stack_n >= greq->greq_format_stack_m) {
    graphd_value **tmp;

    tmp = cm_trealloc(greq->greq_req.req_cm, graphd_value *,
                      greq->greq_format_stack, greq->greq_format_stack_m + 32);
    if (tmp == NULL) return ENOMEM;

    greq->greq_format_stack = tmp;
    greq->greq_format_stack_m += 32;
  }
  greq->greq_format_stack[greq->greq_format_stack_n++] = t;
  if (t->val_type == GRAPHD_VALUE_LIST) {
    greq->greq_format_list_first = 1;
  }
  return 0;
}

static int format_value_finish(graphd_handle *g, graphd_session *gses,
                               graphd_request *greq, char **s, char *e) {
  /*  Clear the temporary state atom formatters use.
   */
  greq->greq_format_s = NULL;
  greq->greq_format_list_finishing = 0;

  while (e - *s >= 2) {
    graphd_value *t = format_stack_pop(greq);
    graphd_value *parent = format_stack_top(greq);

    cl_assert(gses->gses_cl, s != NULL);
    cl_assert(gses->gses_cl, *s != NULL);
    cl_assert(gses->gses_cl, e != NULL);
    cl_assert(gses->gses_cl, e - *s >= 2);
    cl_assert(gses->gses_cl, t != NULL);

    if (parent == NULL) {
      *(*s)++ = '\n';
      **s = '\0';

      return 0;
    }
    if (1 + (t - parent->val_array_contents) == parent->val_array_n) {
      /* Last of its list, close the list. */
      if (parent->val_type == GRAPHD_VALUE_LIST) {
        *(*s)++ = ')';
        greq->greq_format_list_sep = 0;
      }
    } else {
      /* Go to the next in the list. */
      ++t;
      cl_assert(gses->gses_cl, t->val_type != GRAPHD_VALUE_UNSPECIFIED);
      return graphd_format_stack_push(gses, greq, t);
    }
  }

  /*  We'll need to get called back to finish closing all
   *  those lists we're popping off the stack.
   */
  greq->greq_format_list_finishing = 1;
  return GRAPHD_ERR_MORE;
}

/**
 * @brief Format an unquoted value into pushed space.
 *
 *  We have [*s...e) to write into, and would like to write the
 *  value on top of the format stack as unquoted text.
 *  Do all or part of that, with greq->greq_format_s available
 *  to keep temporary state.
 *
 * @param g 	the graphd environment
 * @param gses	current session
 * @param greq	within that, the current request
 * @param s	beginning of space to write into, advanced with writnig
 * @param e	pointer just after the end of available space
 *
 * @return	0 if the stack top was successfull written
 * @return	GRAPHD_ERR_MORE if the formatter ran out of space
 * @return 	ENOMEM for allocation errors.
 */
static int format_value_atom(graphd_handle *g, graphd_session *gses,
                             graphd_request *greq, char **s, char *e) {
  graphd_value *t = format_stack_top(greq);
  cl_handle *cl = gses->gses_cl;
  size_t n;

  cl_assert(cl, greq != NULL);
  cl_assert(cl, t != NULL);
  cl_assert(cl, s != NULL);
  cl_assert(cl, *s != NULL);
  cl_assert(cl, e != NULL);

  /*  greq->greq_format_s -- a piece of generic state for
   *  the current formatter, NULL after a stack push.
   *  Initialize it to a pointer into the value's state,
   *  and advance it as the value's text is written.
   */
  if (greq->greq_format_s == NULL) {
    if (t->val_text_s < t->val_text_e) {
      if (!greq->greq_format_list_sep) {
        if (e - *s <= 2) return GRAPHD_ERR_MORE;
        *(*s)++ = ' ';
        greq->greq_format_list_sep = 1;
      }
    }
    greq->greq_format_s = t->val_text_s;
  }

  cl_assert(cl, greq->greq_format_s >= t->val_text_s);
  cl_assert(cl, greq->greq_format_s <= t->val_text_e);

  /*  n -- How much space do we want to copy into?
   */
  n = e - *s;

  if (t->val_text_e - greq->greq_format_s < n)
    n = t->val_text_e - greq->greq_format_s;

  memcpy(*s, greq->greq_format_s, n);
  greq->greq_format_s += n;
  *s += n;

  /*  Before we hand off to format_value_finish, there needs
   *  to be room for at least 2 bytes to close a parenthesized
   *  list and remove ourselves from the stack.
   *  Ask for more space if we don't have those.
   */
  if (e - *s < 2) return GRAPHD_ERR_MORE;

  greq->greq_format_list_sep = 0;
  greq->greq_format_s = NULL;

  cl_assert(cl, e - *s >= 2);

  return format_value_finish(g, gses, greq, s, e);
}

/**
 * @brief Format an constant text literal into pushed space.
 *
 *  We have [*s...e) to write into, and would like to write the
 *  value on top of the format stack as unquoted text.  The
 *  value isn't stored in an atom, it's just a string literal
 *  the caller passed in.  (The value is derived from the atom,
 *  somehow, but it doesn't matter how.)
 *
 *  Do all or part of that, with greq->greq_format_s available
 *  to keep temporary state.
 *
 * @param g 	the graphd environment
 * @param gses	current session
 * @param greq	within that, the current request
 * @param lit	the string literal to write.
 * @param s	beginning of space to write into, advanced with writnig
 * @param e	pointer just after the end of available space
 *
 * @return	0 if the stack top was successfull written
 * @return	GRAPHD_ERR_MORE if the formatter ran out of space
 * @return 	ENOMEM for allocation errors.
 */
static int format_value_literal(graphd_handle *g, graphd_session *gses,
                                graphd_request *greq, char const *lit, char **s,
                                char *e) {
  graphd_value *t = format_stack_top(greq);
  char const *r;
  char *w;
  cl_handle *cl = gses->gses_cl;

  cl_assert(cl, greq != NULL);
  cl_assert(cl, t != NULL);
  cl_assert(cl, s != NULL);
  cl_assert(cl, *s != NULL);
  cl_assert(cl, e != NULL);

  /*  greq->greq_format_s -- a piece of generic state for
   *  the current formatter, NULL after a stack push.
   *  Initialize it to a pointer into the literal if NULL,
   *  and advance it as the value's text is written.
   */
  if (greq->greq_format_s == NULL) {
    if (lit && *lit != '\0') {
      if (!greq->greq_format_list_sep) {
        if (e - *s <= 2) return GRAPHD_ERR_MORE;
        *(*s)++ = ' ';
        greq->greq_format_list_sep = 1;
      }
    }
    greq->greq_format_s = lit;
  }

  /*  Copy; stop if we run out of space or hit a
   *  the terminating \0 of the literal.
   */
  r = greq->greq_format_s;
  w = *s;

  while (w < e && (*w = *r++) != '\0') w++;

  greq->greq_format_s = r;
  *s = w;

  /*  Before we hand off to format_value_finish, there needs
   *  to be room for at least 2 bytes.  Ask for more space
   *  if we don't have those.
   */
  if (e - *s < 2) {
    return GRAPHD_ERR_MORE;
  }

  greq->greq_format_list_sep = 0;
  greq->greq_format_s = NULL;

  cl_assert(cl, e - *s >= 2);
  return format_value_finish(g, gses, greq, s, e);
}

/**
 * @brief Format a double-quoted and escape-quoted string into pushed space.
 *
 *  We have [*s...e) to write into, and would like to write the
 *  value on top of the format stack as a quoted text.   There need
 *  to be "" around the text, and " and \ inside of the string need
 *  to be escaped.
 *
 *  Do all or part of that, with greq->greq_format_s available
 *  to keep temporary state.
 *
 * @param g 	the graphd environment
 * @param gses	current session
 * @param greq	within that, the current request
 * @param s	beginning of space to write into, advanced with writnig
 * @param e	pointer just after the end of available space
 *
 * @return	0 if the stack top was successfull written
 * @return	GRAPHD_ERR_MORE if the formatter ran out of space
 * @return 	ENOMEM for allocation errors.
 */
static int format_value_string(graphd_handle *g, graphd_session *gses,
                               graphd_request *greq, char **s, char *e) {
  graphd_value *t = format_stack_top(greq);
  char const *r;
  char *w;

  cl_assert(gses->gses_cl, t != NULL);
  if (greq->greq_format_s == NULL) {
    /* Write the beginning of the string.
     */
    if (e - *s <= 2) {
      return GRAPHD_ERR_MORE;
    }
    if (!greq->greq_format_list_sep) {
      *(*s)++ = ' ';
      greq->greq_format_list_sep = 1;
    }
    *(*s)++ = '"';
    greq->greq_format_s = t->val_text_s;
  }

  r = greq->greq_format_s;
  w = *s;

  while (r < t->val_text_e && w + 2 < e) {
    switch (*r) {
      case '\n':
        *w++ = '\\';
        *w++ = 'n';
        break;
      case '"':
      case '\\':
        *w++ = '\\';
      /* FALL THROUGH */
      default:
        *w++ = *r;
        break;
    }
    r++;
  }

  greq->greq_format_s = r;
  *s = w;

  /*  Before we hand off to format_value_finish, there needs
   *  to be room for at least 2 bytes, plus one byte for our
   *  closing ".
   */
  if (r < t->val_text_e || e - w < 3) {
    return GRAPHD_ERR_MORE;
  }

  *(*s)++ = '"';
  cl_assert(gses->gses_cl, e - *s >= 2);

  greq->greq_format_list_sep = 0;
  return format_value_finish(g, gses, greq, s, e);
}

static void format_value_append_string(cl_handle *cl, char **s, char *e,
                                       char const *p) {
  cl_assert(cl, strlen(p) <= e - *s);
  while ((*(*s) = *p++) != '\0') ++*s;
}

void graphd_format_value_records_finish(graphd_request *greq) {
  graphd_format_records_context *frc = greq->greq_format_records_context;

  if (frc != NULL) {
    pdb_primitive_finish(graphd_request_graphd(greq)->g_pdb, &frc->frc_pr);
    pdb_primitive_initialize(&frc->frc_pr);
  }
}

static int format_value_records(graphd_handle *g, graphd_session *gses,
                                graphd_request *greq, char **s, char *e) {
  graphd_format_records_context *frc;
  graphd_value *t = format_stack_top(greq);

  graph_guid guid;

  char guid_buf[GRAPH_GUID_SIZE];
  char ts_buf[GRAPH_TIMESTAMP_SIZE];

  size_t string_size;
  char const *string_mem;
  int err;

  if ((frc = greq->greq_format_records_context) == NULL) {
    frc = cm_malloc(greq->greq_req.req_cm, sizeof(*frc));
    if (frc == NULL) return ENOMEM;

    memset(frc, 0, sizeof(*frc));
    pdb_primitive_initialize(&frc->frc_pr);

    greq->greq_format_records_context = frc;
  }

/*  If we're in the middle of formatting a string -
 *  double-quoted or not - continue formatting the string.
 */
have_string:
  if (greq->greq_format_s != NULL) {
    if (frc->frc_quoted) {
      while (e - *s >= 3 && *greq->greq_format_s != '\0')
        switch (*greq->greq_format_s) {
          case '\n':
            *(*s)++ = '\\';
            *(*s)++ = 'n';
            greq->greq_format_s++;
            break;

          case '\\':
          case '"':
            *(*s)++ = '\\';
          /* Fall through */
          default:
            *(*s)++ = *greq->greq_format_s++;
            break;
        }

      if (*greq->greq_format_s != '\0') {
        return GRAPHD_ERR_MORE;
      }

      *(*s)++ = '"';
      greq->greq_format_s = NULL;
      greq->greq_format_list_sep = 0;

      frc->frc_quoted = false;
    } else {
      size_t n = strlen(greq->greq_format_s);
      if (e - *s < n) {
        n = e - *s;
      }
      memcpy(*s, greq->greq_format_s, n);
      *s += n;
      if (*(greq->greq_format_s += n) == '\0') {
        greq->greq_format_s = NULL;
        greq->greq_format_list_sep = 0;
      } else {
        return GRAPHD_ERR_MORE;
      }
    }
  }

  /*  If we arrive here, we're done formatting the string
   *  and will continue with the next field.
   */
  cl_assert(gses->gses_cl, greq->greq_format_s == NULL);

  while (frc->frc_i < t->val_records_n) {
    /* space, opening (, leading " */

    if (e - *s <= 3) return GRAPHD_ERR_MORE;
    if (frc->frc_field < 14 && !greq->greq_format_list_sep) {
      *(*s)++ = ' ';
      greq->greq_format_list_sep = 1;
    }

    switch (frc->frc_field) {
      case 0:
        *(*s)++ = '(';
        greq->greq_format_list_sep = 1;

        /* Read this record. */
        frc->frc_err = pdb_id_read(t->val_records_pdb,
                                   t->val_records_i + frc->frc_i, &frc->frc_pr);
        frc->frc_field++;

      /* FALL THROUGH */
      case 1:
        if (frc->frc_err != 0) {
          *(*s)++ = '"';
          frc->frc_quoted = 1;
          frc->frc_field = 14;
          greq->greq_format_s = graphd_strerror(frc->frc_err);
          goto have_string;
        }

        if (e - *s < 2 + GRAPH_GUID_SIZE) return GRAPHD_ERR_MORE;

        pdb_primitive_guid_get(&frc->frc_pr, guid);
      have_guid:
        format_value_append_string(
            gses->gses_cl, s, e,
            graph_guid_to_string(&guid, guid_buf, sizeof(guid_buf)));
        cl_assert(gses->gses_cl, *s <= e);
        greq->greq_format_list_sep = 0;
        frc->frc_field++;
        break;

      case 2:
        if (e - *s < 2 + GRAPH_GUID_SIZE) return GRAPHD_ERR_MORE;
        if (pdb_primitive_has_typeguid(&frc->frc_pr)) {
          pdb_primitive_typeguid_get(&frc->frc_pr, guid);
          goto have_guid;
        }
        greq->greq_format_s = "null";
        frc->frc_quoted = 0;
        frc->frc_field++;
        goto have_string;

      case 3:
        string_size = pdb_primitive_name_get_size(&frc->frc_pr);
        string_mem = pdb_primitive_name_get_memory(&frc->frc_pr);
      have_bytes:
        frc->frc_field++;
        if (string_size != 0) {
          *(*s)++ = '"';
          greq->greq_format_s = string_mem;
          cl_assert(gses->gses_cl, string_mem[string_size - 1] == '\0');
          frc->frc_quoted = 1;
        } else {
          greq->greq_format_s = "null";
          frc->frc_quoted = 0;
        }
        goto have_string;

      case 4:
        if (e - *s < 4) return GRAPHD_ERR_MORE;

        greq->greq_format_s =
            graph_datatype_to_string(pdb_primitive_valuetype_get(&frc->frc_pr));
        if (greq->greq_format_s != NULL) {
          /* If it has a string name, print the
           * string name.
           */
          frc->frc_quoted = 0;
          frc->frc_field++;
          goto have_string;
        } else {
          /* Otherwise, print a number.
           */
          char buf[200];

          snprintf(buf, sizeof buf, "%hu",
                   pdb_primitive_valuetype_get(&frc->frc_pr));
          format_value_append_string(gses->gses_cl, s, e, buf);
          cl_assert(gses->gses_cl, *s < e);
          *(*s)++ = ' ';
          greq->greq_format_list_sep = 1;
          frc->frc_quoted = 0;
          frc->frc_field++;
        }
      /* FALL THROUGH */

      case 5:
        string_size = pdb_primitive_value_get_size(&frc->frc_pr);
        string_mem = pdb_primitive_value_get_memory(&frc->frc_pr);
        goto have_bytes;

      case 6:
        if (e - *s < 2 + GRAPH_GUID_SIZE) return GRAPHD_ERR_MORE;
        if (pdb_primitive_has_scope(&frc->frc_pr))
          pdb_primitive_scope_get(&frc->frc_pr, guid);
        else
          GRAPH_GUID_MAKE_NULL(guid);
        goto have_guid;

      case 7:
        greq->greq_format_s =
            pdb_primitive_is_live(&frc->frc_pr) ? "true" : "false";
        frc->frc_quoted = 0;
        frc->frc_field++;
        goto have_string;

      case 8:
        greq->greq_format_s =
            pdb_primitive_is_archival(&frc->frc_pr) ? "true" : "false";
        frc->frc_quoted = 0;
        frc->frc_field++;
        goto have_string;

      case 9:
        greq->greq_format_s =
            pdb_primitive_is_txstart(&frc->frc_pr) ? "true" : "false";
        frc->frc_quoted = 0;
        frc->frc_field++;
        goto have_string;

      case 10:
        if (e - *s < 2 + GRAPH_TIMESTAMP_SIZE) return GRAPHD_ERR_MORE;
        frc->frc_field++;
        format_value_append_string(
            gses->gses_cl, s, e,
            graph_timestamp_to_string(pdb_primitive_timestamp_get(&frc->frc_pr),
                                      ts_buf, sizeof(ts_buf)));
        *(*s)++ = ' ';
        greq->greq_format_list_sep = 1;
      /* FALL THROUGH */

      case 11:
        if (e - *s < 2 + GRAPH_GUID_SIZE) return GRAPHD_ERR_MORE;
        if (pdb_primitive_has_left(&frc->frc_pr))
          pdb_primitive_left_get(&frc->frc_pr, guid);
        else
          GRAPH_GUID_MAKE_NULL(guid);
        goto have_guid;

      case 12:
        if (e - *s < 2 + GRAPH_GUID_SIZE) return GRAPHD_ERR_MORE;
        if (pdb_primitive_has_right(&frc->frc_pr))
          pdb_primitive_right_get(&frc->frc_pr, guid);
        else
          GRAPH_GUID_MAKE_NULL(guid);
        goto have_guid;

      case 13:
        if (e - *s < 2 + GRAPH_GUID_SIZE) return GRAPHD_ERR_MORE;
        if (!pdb_primitive_has_previous(&frc->frc_pr))
          GRAPH_GUID_MAKE_NULL(guid);
        else if ((err = pdb_primitive_previous_guid(
                      t->val_records_pdb, &frc->frc_pr, &guid)) != 0) {
          char buf[200];
          cl_log_errno(gses->gses_cl, CL_LEVEL_ERROR,
                       "pdb_primitive_previous_guid", err,
                       "unable to get previous GUID "
                       "for %s",
                       pdb_primitive_to_string(&frc->frc_pr, buf, sizeof buf));
          GRAPH_GUID_MAKE_NULL(guid);
        }
        goto have_guid;

      case 14:
        *(*s)++ = ')';

        pdb_primitive_finish(t->val_records_pdb, &frc->frc_pr);
        pdb_primitive_initialize(&frc->frc_pr);

        if (++frc->frc_i < t->val_records_n) {
          *(*s)++ = ' ';
          frc->frc_field = 0;
          greq->greq_format_list_sep = 1;
        }
        break;

      default:
        cl_notreached(gses->gses_cl, "unexpected field value %d",
                      frc->frc_field);
    }
  }

  cl_assert(gses->gses_cl, frc->frc_i == t->val_records_n);

  if (e - *s <= 2) return GRAPHD_ERR_MORE;

  /*  Free and reset the local formatter state.
   */
  cm_free(greq->greq_req.req_cm, frc);

  greq->greq_format_list_sep = 0;
  greq->greq_format_records_context = NULL;

  return format_value_finish(g, gses, greq, s, e);
}

/**
 * @brief Push formatter for the top of the formatting stack
 *
 *  The space [*s ... e) is available to write into.
 *  There is at least one object on top of the formatting stack.
 *
 * @return 0 for success, and call me again
 * @return GRAPHD_ERR_MORE if we need the caller to make more space, then call
 *	us again.
 *
 */
static int format_value(graphd_handle *g, graphd_session *gses,
                        graphd_request *greq, char **s, char *e) {
  graphd_value *t = format_stack_top(greq);
  int err;
  char buf[GRAPH_TIMESTAMP_SIZE];
  char guidbuf[GRAPH_GUID_SIZE];
  char const *guid, *val;
  cl_handle *cl = gses->gses_cl;

  cl_assert(cl, t != NULL);
  if (e - *s <= 2) {
    return GRAPHD_ERR_MORE;
  }

#define SEPARATE                               \
  do                                           \
    if (!greq->greq_format_list_sep) {         \
      if (e - *s <= 2) return GRAPHD_ERR_MORE; \
      *(*s)++ = ' ';                           \
      greq->greq_format_list_sep = 1;          \
    }                                          \
  while (0)

  switch (t->val_type) {
    default:
      cl_notreached(cl, "unexpected value type %d", t->val_type);
      break;

    case GRAPHD_VALUE_STRING:
      return format_value_string(g, gses, greq, s, e);

    case GRAPHD_VALUE_ATOM:
      return format_value_atom(g, gses, greq, s, e);

    case GRAPHD_VALUE_NUMBER:
      SEPARATE;
      if (e - *s <= NUMBER_MAX_SIZE + 2) {
        return GRAPHD_ERR_MORE;
      }
      snprintf(*s, e - *s, "%llu", (unsigned long long)t->val_number);
      *s += strlen(*s);
      greq->greq_format_list_sep = 0;
      cl_assert(cl, e - *s >= 2);
      return format_value_finish(g, gses, greq, s, e);

    case GRAPHD_VALUE_BOOLEAN:
      return format_value_literal(g, gses, greq,
                                  t->val_boolean ? "true" : "false", s, e);

    case GRAPHD_VALUE_DATATYPE:
      val = graph_datatype_to_string(t->val_datatype);
      if (val == NULL) {
        snprintf(buf, sizeof buf, "%hu", t->val_datatype);
        val = buf;
      }
      return format_value_literal(g, gses, greq, val, s, e);

    case GRAPHD_VALUE_GUID:
      SEPARATE;
      if (e - *s <= GRAPH_GUID_SIZE + 4) return GRAPHD_ERR_MORE;

      guid = graph_guid_to_string(&t->val_guid, guidbuf, sizeof guidbuf);
      if (guid == *s) {
        *s += strlen(*s);
      } else {
        size_t n = strlen(guid);
        memmove(*s, guid, n);
        *s += n;
      }
      greq->greq_format_list_sep = 0;
      cl_assert(cl, e - *s >= 2);

      return format_value_finish(g, gses, greq, s, e);

    case GRAPHD_VALUE_LIST:
      SEPARATE;
      if (e - *s <= 5) {
        return GRAPHD_ERR_MORE;
      }
      if (greq->greq_format_list_first) {
        greq->greq_format_list_sep = 1;
        greq->greq_format_list_first = 0;
        *(*s)++ = '(';
      }

    /* FALL THROUGH */

    case GRAPHD_VALUE_SEQUENCE:
      if (e - *s <= 1) return GRAPHD_ERR_MORE;

      if (t->val_array_n == 0) {
        cl_assert(cl, e - *s >= 1);
        if (t->val_type == GRAPHD_VALUE_LIST) {
          *(*s)++ = ')';
          greq->greq_format_list_sep = 0;
        }
        err = format_value_finish(g, gses, greq, s, e);
      } else {
        graphd_value *val = t->val_array_contents;

        cl_assert(cl, val != NULL);
        cl_assert(cl, val->val_type != GRAPHD_VALUE_UNSPECIFIED);
        err = graphd_format_stack_push(gses, greq, val);
      }
      return err;

    case GRAPHD_VALUE_TIMESTAMP:
      SEPARATE;
      if (e - *s <= GRAPH_TIMESTAMP_SIZE + 2) {
        return GRAPHD_ERR_MORE;
      }
      snprintf(*s, e - *s, "%s",
               graph_timestamp_to_string(t->val_timestamp, buf, sizeof buf));
      *s += strlen(*s);
      greq->greq_format_list_sep = 0;
      cl_assert(cl, e - *s >= 2);
      return format_value_finish(g, gses, greq, s, e);

    case GRAPHD_VALUE_NULL:
      return format_value_literal(g, gses, greq, "null", s, e);

    case GRAPHD_VALUE_RECORDS:
      return format_value_records(g, gses, greq, s, e);

    case GRAPHD_VALUE_DEFERRED:
      cl_notreached(cl, "attempt to format deferred records.");
  }
  return 0;
}

void graphd_format_result(void *data, srv_handle *srv, void *session_data,
                          void *request_data, char **s, char *e) {
  graphd_handle *g = data;
  graphd_session *gses = session_data;
  graphd_request *greq = request_data;
  cl_handle *cl = srv_log(srv);

  cl_assert(cl, gses != NULL);
  cl_assert(cl, greq != NULL);
  cl_assert(cl, g != NULL);

  /* (Continue to...) format the reply. */
  while (greq->greq_format_stack_n != 0) {
    if (e - *s < SRV_MIN_BUFFER_SIZE) {
      return;
    }
    if ((greq->greq_format_list_finishing ? format_value_finish : format_value)(
            g, gses, greq, s, e)) {
      return;
    }
  }
  srv_request_output_done(&greq->greq_req);
}

/**
 * @brief Format the request paramter "id".
 *
 *  This is the protocol-level graphd request ID, a string sent by the
 *  client; not the numeric internal srv_request_id handed out by the
 *  interface.
 *
 *  Caller must initially set greq->greq_format_s = NULL.
 *
 * @param grp		request parameter
 * @param greq		graphd request
 * @param s		in/out: beginning of available buffer
 * @param e		end of available buffer
 *
 * @return 0 		on completion
 * @return GRAPHD_ERR_MORE	after running out of output buffer
 */
int graphd_format_request_id(graphd_request_parameter *grp,
                             graphd_request *greq, char **s, char *e) {
  graphd_request_parameter_id *id = (void *)grp;
  char *w = *s;

  if (greq->greq_format_s == NULL) {
    if (e - w <= sizeof("id=\"")) return GRAPHD_ERR_MORE;

    greq->greq_format_s = id->id_s;
    memcpy(w, "id=\"", sizeof("id=\"") - 1);
    w += sizeof("id=\"") - 1;
  }

  while (w < e && greq->greq_format_s < id->id_e) {
    switch (*greq->greq_format_s) {
      case '\n':
        *w++ = '\\';
        *w++ = 'n';
        break;

      case '"':
      case '\\':
        *w++ = '\\';
      /* FALL THROUGH */
      default:
        *w++ = *greq->greq_format_s;
        break;
    }
    greq->greq_format_s++;
  }

  if (e - (*s = w) < 3) return GRAPHD_ERR_MORE;

  *(*s)++ = '"';
  greq->greq_format_s = NULL;

  return 0;
}

/**
 * @brief Format a dateline
 *
 *  This is called by format_request_parameter via the
 *  grp_format member of a request parameter structure.
 *
 * @param grp		request parameter
 * @param greq		graphd request
 * @param s		in/out: beginning of available buffer
 * @param e		end of available buffer
 *
 * @return 0 			on completion
 * @return GRAPHD_ERR_MORE	after running out of output buffer
 */
int graphd_format_request_dateline(graphd_request_parameter *grp,
                                   graphd_request *greq, char **s, char *e) {
  cl_handle *cl = graphd_request_cl(greq);
  char *w = *s;
  int err;
  graphd_handle *g;

  g = graphd_request_graphd(greq);

  cl_assert(cl, greq->greq_dateline_wanted);
  if (greq->greq_dateline == NULL) {
    graphd_request_served(greq);
    if (greq->greq_dateline == NULL) return 0;
  }
  cl_assert(cl, greq->greq_dateline != NULL);

  if (greq->greq_format_s == NULL) {
    greq->greq_format_s = "dateline=\"";
    greq->greq_format_dateline_state = NULL;
    greq->greq_format_dateline_offset = 0;
    greq->greq_format_dateline_io = 0;
    greq->greq_format_dateline_io_done = false;
  }
  while (*greq->greq_format_s != '\0' && w < e) *w++ = *greq->greq_format_s++;

  /* We ran out of space?
   */
  if (*greq->greq_format_s != '\0') {
    *s = w;
    return GRAPHD_ERR_MORE;
  }

  err = 0;
  while (w < e) {
    err = graph_dateline_format(greq->greq_dateline, &w, e,
                                &greq->greq_format_dateline_state,
                                &greq->greq_format_dateline_offset);
    if (err != 0) {
      /*  Unexpected error.
       */
      if (err != GRAPH_ERR_DONE) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graph_dateline_format", err,
                     "unexpected error");
        return err;
      }
      break;
    }
  }

  *s = w;
  if (err == 0)
    /* We ran out of space.
     */
    return GRAPHD_ERR_MORE;

  /* graph_dateline_format ran out of stuff to do.
   */
  cl_assert(cl, err == GRAPH_ERR_DONE);

  *(*s)++ = '"';
  greq->greq_format_s = NULL;

  return 0;
}

/**
 * @brief Format a heatmap
 *
 *  Caller must initially set greq->greq_format_s = NULL.
 *
 * @param grp		request parameter
 * @param greq		graphd request
 * @param s		in/out: beginning of available buffer
 * @param e		end of available buffer
 *
 * @return 0 			on completion
 * @return GRAPHD_ERR_MORE	after running out of output buffer
 */
int graphd_format_request_heatmap(graphd_request_parameter *heatmap,
                                  graphd_request *greq, char **s, char *e) {
  char *w = *s;
  cm_buffer buf;

  static char const out_of_memory[] =
      "*** ERROR: out of memory while determining heatmap ***";

  if (greq->greq_format_s == NULL) {
    int err;

    if (e - w <= sizeof("heatmap=\"")) return GRAPHD_ERR_MORE;

    cm_buffer_initialize(&buf, greq->greq_req.req_cm);
    err = graphd_constraint_get_heatmap(greq, greq->greq_constraint, &buf);
    if (err != 0)
      greq->greq_format_s = out_of_memory;
    else {
      /* Don't free the buffer - freeing the
       * request heap will take care of that.
       */
      greq->greq_format_s = cm_buffer_memory(&buf);
      if (greq->greq_format_s == NULL) greq->greq_format_s = "";
    }

    memcpy(w, "heatmap=\"", sizeof("heatmap=\"") - 1);
    w += sizeof("heatmap=\"") - 1;
  }

  while (w < e && *greq->greq_format_s != '\0') {
    switch (*greq->greq_format_s) {
      case '\n':
        *w++ = '\\';
        *w++ = 'n';
        break;

      case '"':
      case '\\':
        *w++ = '\\';
      /* FALL THROUGH */
      default:
        *w++ = *greq->greq_format_s;
        break;
    }
    greq->greq_format_s++;
  }

  *s = w;
  if (w >= e) {
    return GRAPHD_ERR_MORE;
  }

  *(*s)++ = '"';
  greq->greq_format_s = NULL;

  return 0;
}

/**
 * @brief Format a cost
 *
 *  Caller must initially set greq->greq_format_s = NULL.
 *
 * @param grp		request parameter
 * @param greq		graphd request
 * @param s		in/out: beginning of available buffer
 * @param e		end of available buffer
 *
 * @return 0 			on completion
 * @return GRAPHD_ERR_MORE	after running out of output buffer
 */
int graphd_format_request_cost(graphd_request_parameter *cost,
                               graphd_request *greq, char **s, char *e) {
  char bigbuf[1024];
  char *w = *s;
  char *buf;

  static char const out_of_memory[] =
      "*** ERROR: out of memory while determining cost ***";

  if (greq->greq_format_s == NULL) {
    graphd_runtime_statistics st;

    if (e - w <= sizeof("cost=\"")) return GRAPHD_ERR_MORE;

    graphd_runtime_statistics_publish(&greq->greq_runtime_statistics, &st);

    snprintf(
        bigbuf, sizeof bigbuf,
        "tu=%llu " /* time/user			*/
        "ts=%llu " /* time/system			*/
        "tr=%llu " /* time/real			*/
        "te=%llu " /* time/end-to-end		*/
        "pr=%llu " /* page reclaims		*/
        "pf=%llu " /* page faults			*/
        "dw=%llu " /* primitive writes 		*/
        "dr=%llu " /* primitive reads 		*/
        "in=%llu " /* gmap size reads		*/
        "ir=%llu " /* gmap reads			*/
        "iw=%llu " /* gmap writes			*/
        "va=%llu", /* values allocated		*/

        st.grts_user_millis,
        st.grts_system_millis, st.grts_wall_millis, st.grts_endtoend_millis,
        st.grts_minflt, st.grts_majflt, st.grts_pdb.rts_primitives_written,
        st.grts_pdb.rts_primitives_read, st.grts_pdb.rts_index_extents_read,
        st.grts_pdb.rts_index_elements_read,
        st.grts_pdb.rts_index_elements_written, st.grts_values_allocated);

    buf = cm_bufmalcpy(greq->greq_req.req_cm, bigbuf);
    if (buf == NULL) buf = (char *)out_of_memory;
    greq->greq_format_s = buf;

    memcpy(w, "cost=\"", sizeof("cost=\"") - 1);
    w += sizeof("cost=\"") - 1;
  }

  while (w < e && *greq->greq_format_s != '\0') {
    switch (*greq->greq_format_s) {
      case '\n':
        *w++ = '\\';
        *w++ = 'n';
        break;

      case '"':
      case '\\':
        *w++ = '\\';
      /* FALL THROUGH */
      default:
        *w++ = *greq->greq_format_s;
        break;
    }
    greq->greq_format_s++;
  }

  *s = w;
  if (w >= e) {
    return GRAPHD_ERR_MORE;
  }

  *(*s)++ = '"';
  greq->greq_format_s = NULL;

  return 0;
}

static void format_request_parameter(void *data, srv_handle *srv,
                                     void *session_data, void *request_data,
                                     char **s, char *e) {
  graphd_request *greq = request_data;
  graphd_request_parameter *grp;
  int err;

  while ((grp = greq->greq_parameter_head) != NULL) {
    if (grp->grp_format != NULL) {
      err = (*grp->grp_format)(grp, greq, s, e);
      if (err == GRAPHD_ERR_MORE) return;

      /* We're done with the param-specific formatting. */

      grp->grp_format = NULL;
      greq->greq_format_s = NULL;
    }
    if (*s >= e) return;

    *(*s)++ = ' ';
    if ((greq->greq_parameter_head = grp->grp_next) == NULL)
      greq->greq_parameter_tail = &greq->greq_parameter_head;

    /*  Don't free the parameter "grp" here -- leave if for the
     *  collective heap free.  For example, an ID parameter
     *  may have gotten used as the request's displayname,
     *  and we still need that for logging.
     */
  }

  greq->greq_format =
      greq->greq_error_message != NULL ? format_error : graphd_format_result;
  (*greq->greq_format)(data, srv, session_data, request_data, s, e);
}

static void format_error(void *data, srv_handle *srv, void *session_data,
                         void *request_data, char **s, char *e) {
  graphd_handle *g = data;
  graphd_session *gses = session_data;
  graphd_request *greq = request_data;
  cl_handle *cl = srv_log(srv);
  char *p = *s;

  cl_assert(cl, gses != NULL);
  cl_assert(cl, greq != NULL);
  cl_assert(cl, greq->greq_error_message != NULL);
  cl_assert(cl, g != NULL);

  /*  Formatting: At the beginning of the error message pointed to
   *  by greq_error_message, there's a single token - followed by
   *  SPACE or '\0' - that's intended to be machine-readable.
   *  Everything after that is sent wrapped into a string,
   *  enclosed in "" and with " and \ quoted.
   *
   *  Internal state:
   *	greq_error_message: SYNTAX expected "foo", got "bar"
   *
   *  Actually sent:
   *	error SYNTAX "expected \"foo\", got \"bar\""
   *
   *  If there are id= or source= modifiers, they appear after the
   *  error key ("SYNTAX" in the example above), but before
   *  the error message string.
   *
   *	error SYNTAX id="foo" source="bar.xml" "d'oh!"
   */

  switch (greq->greq_error_state) {
    case GRAPHD_ERRORSTATE_INITIAL:

      /*  To state-transition, we need at least six free bytes
       *  to store e r r o r <space>.
       */
      if (e - p < 6) {
        break;
      }

      greq->greq_error_state = GRAPHD_ERRORSTATE_KEYWORD;
      memcpy(p, "error ", 6);
      *s = p += 6;

    /* fall through */

    case GRAPHD_ERRORSTATE_KEYWORD:

      /*  Copy the first token literally.  We rely on the
       *  application to not include %t or quotes in it -
       *  it's something like SYNTAX or SEMANTICS or MEMORY.
       */
      for (; p < e; p++)
        if (*greq->greq_error_message == '\0' ||
            (*p = *greq->greq_error_message++) == ' ') {
          break;
        }

      /*  To state-transition, we need at most two free
       *  bytes to store a <space> <quote>.
       */
      if (e - p < 1) {
        break;
      }

      if (p[-1] != ' ') *p++ = ' ';

      greq->greq_error_state = GRAPHD_ERRORSTATE_QUOTE;
      greq->greq_format_s = NULL;

      if (greq->greq_parameter_head != NULL) {
        greq->greq_format_s = NULL;
        greq->greq_format = format_request_parameter;
        break;
      }
    /* fall through */

    case GRAPHD_ERRORSTATE_QUOTE:
      if (e - p < 1) {
        break;
      }
      greq->greq_error_state = GRAPHD_ERRORSTATE_MESSAGE;
      *p++ = '"';

    case GRAPHD_ERRORSTATE_MESSAGE:

      while (e - p >= 2 && *greq->greq_error_message != '\0') {
        int ch;

        /*  At the end of this block, <ch> contains the
         *  literal character we want to write.  We'll need
         *  at most 2 bytes to write it -- an optional \
         *  followed by the character itself.
         */

        if (greq->greq_error_message[0] != '%' ||
            !greq->greq_error_substitute) {
          ch = *greq->greq_error_message++;
        } else if (greq->greq_error_message[1] == 't') {
          if (greq->greq_error_token.tkn_start <
              greq->greq_error_token.tkn_end) {
            ch = *greq->greq_error_token.tkn_start++;
          } else {
            greq->greq_error_message += 2;
            continue;
          }
        } else if (greq->greq_error_message[1] == '%') {
          greq->greq_error_message += 2;
          ch = '%';
        } else {
          ch = *greq->greq_error_message++;
        }

        /*  Quote " or \ with \, \n as \n.
         */
        switch (ch) {
          case '\n':
            *p++ = '\\';
            *p++ = 'n';
            break;

          case '"':
          case '\\':
            *p++ = '\\';
          /* FALL THROUGH */
          default:
            *p++ = ch;
            break;
        }
      }

      /*  To state-transition, we need three free bytes
       *  to store " \n \0.  (The \0 is not strictly needed,
       *  but may help with casual logging later on.)
       */
      if (e - p < 3) {
        break;
      }

      *p++ = '"';
      *p++ = '\n';
      *p = '\0';

      greq->greq_error_message = NULL;
      greq->greq_error_state = 0;
      greq->greq_format = NULL;

      srv_request_complete(&greq->greq_req);

      break;
  }
  *s = p;
}

static int graphd_format_error(graphd_handle *g, srv_handle *srv,
                               graphd_session *gses, graphd_request *greq,
                               char **s, char *e) {
  cl_assert(g->g_cl, greq != NULL);
  cl_assert(g->g_cl, greq->greq_error_message != NULL);

  greq->greq_format = format_error;
  (*greq->greq_format)(g, srv, gses, greq, s, e);

  return 0;
}

typedef struct graphd_format_checkpoint {
  graphd_handle *check_g;
  pdb_id check_horizon;
} graphd_format_checkpoint;

/* This is a callback which is called immediately before writing
 * a response to a command which wrote primitives
 * We ensure that all primitives have been committed to disk.
 *
 * @return SRV_ERR_MORE to say "we need more time to run."
 */
static int graphd_format_sync_horizon(void *data, bool block, bool *any) {
  graphd_format_checkpoint *const cpd = data;
  graphd_handle *const g = cpd->check_g;
  pdb_id marker_id;
  int err = 0;

  if (cpd->check_horizon > pdb_primitive_n(g->g_pdb))
    return 0; /* we've had an intervening restore */

  cl_log(g->g_cl, CL_LEVEL_DEBUG, "Mandatory checkpoint to %llx",
         (unsigned long long)cpd->check_horizon);

  marker_id = pdb_checkpoint_id_on_disk(g->g_pdb);
  if (cpd->check_horizon > marker_id) {
    pdb_id next_id = (pdb_id)pdb_primitive_n(g->g_pdb);

    err = pdb_checkpoint_mandatory(g->g_pdb, block);

    /*
     * Start replicating these primitives as soon as they hit disk
     */
    if (err == 0 || err == PDB_ERR_ALREADY)
      graphd_replicate_primitives(g, marker_id, next_id);
    if (err && err != PDB_ERR_ALREADY && err != GRAPHD_ERR_MORE) return err;
    if (err == GRAPHD_ERR_MORE) err = SRV_ERR_MORE;
  }
  return err;
}

/**
 * @brief Fill output space with a request result's value.
 *
 *  This includes first running the requst.
 *
 *  The caller has provided the space for us to fill,
 *  and will guarantee a minimal available size of
 *  SRV_MINIMUM_SIZE.   We format as long as we have space,
 *  then return.
 *
 * @param data	opaque application data; in our case, the graphd handle
 * @param srv	service module pointer
 * @param session_data	opaque application per-session data, gses
 * @param request_data	opaque application per-request data, greq
 * @param s		in/out: begin of unfilled output buffer
 * @param e		in: pointer just after the last available byte
 * @param deadline	after running this long, return 0
 *			and continue when called back.
 * @return 0 on success, nonzero error codes on unexpected
 * 	system errors.
 */
int graphd_request_output(void *data, srv_handle *srv, void *session_data,
                          void *request_data, char **s, char *e,
                          srv_msclock_t deadline) {
  graphd_handle *g = data;
  graphd_session *gses = session_data;
  graphd_request *greq = request_data;
  cl_handle *cl = gses->gses_cl;
  char const *leave_msg = 0;
  char *p = s ? *s : NULL;
  char buf[200];
  int err;

  /* Error - there is no output capacity.
   */
  if (s == NULL) {
    /*  Say we're done, reset our formatter, and free
     *  our stuff.
     */
    srv_request_complete(&greq->greq_req);
    greq->greq_format = NULL;
    graphd_request_free_specifics(greq);

    return 0;
  }

  cl_enter(cl, CL_LEVEL_VERBOSE, "req=%s",
           graphd_request_to_string(greq, buf, sizeof buf));

  p = *s;
  cl_assert(cl, gses != NULL);
  cl_assert(cl, greq != NULL);
  cl_assert(cl, g != NULL);
  cl_assert(cl, e - p >= SRV_MIN_BUFFER_SIZE);

  gses->gses_time_active = g->g_now;

  if (g->g_diary_cl != NULL) graphd_request_diary_log(greq, 0, "FORMAT");

  /*  If we're in the middle of actually producing output,
   *  keep on doing that.
   */
  if (greq->greq_format != NULL) {
    (*greq->greq_format)(data, srv, gses, greq, s, e);
    cl_leave(cl, CL_LEVEL_VERBOSE, "-> greq->greq_format");
    return 0;
  }

  cl_assert(cl, greq->greq_request != GRAPHD_REQUEST_UNSPECIFIED);
  if (greq->greq_horizon == 0) greq->greq_horizon = pdb_primitive_n(g->g_pdb);

  /*  If the request is based on state that hasn't hit
   *  the disk yet, make sure that that'll have happened
   *  by the time its results are sent.
   */
  if (greq->greq_horizon > pdb_checkpoint_id_on_disk(g->g_pdb)) {
    graphd_format_checkpoint *checkpoint;

    checkpoint = srv_session_allocate_pre_hook(
        &gses->gses_ses, (void *)graphd_format_sync_horizon,
        sizeof(*checkpoint));
    if (checkpoint == NULL) {
      if (greq->greq_error_message == NULL)
        greq->greq_error_message = "MEMORY server out of memory";
      cl_leave(cl, CL_LEVEL_VERBOSE, "-> graphd_format_error");
      return graphd_format_error(g, srv, gses, greq, s, e);
    }

    checkpoint->check_g = g;
    checkpoint->check_horizon = greq->greq_horizon;
  }

  if ((GRAPHD_VALUE_UNSPECIFIED == greq->greq_reply.val_type) &&
      (greq->greq_request == GRAPHD_REQUEST_READ ||
       greq->greq_request == GRAPHD_REQUEST_ITERATE ||
       (greq->greq_request == GRAPHD_REQUEST_WRITE &&
        g->g_access != GRAPHD_ACCESS_REPLICA &&
        g->g_access != GRAPHD_ACCESS_REPLICA_SYNC) ||
       greq->greq_request == GRAPHD_REQUEST_STATUS ||
       greq->greq_request == GRAPHD_REQUEST_SYNC)) {
    if (greq->greq_error_message == NULL)
      greq->greq_error_message = "EMPTY not found";
    cl_leave(cl, CL_LEVEL_SPEW, "no results");

    return graphd_format_error(g, srv, gses, greq, s, e);
  }

  if (greq->greq_request == GRAPHD_REQUEST_ERROR ||
      greq->greq_error_message != NULL) {
    if (greq->greq_error_message == NULL)
      greq->greq_error_message = "SYSTEM unexpected error";

    /*  Some types of sessions are asynchronous (or move
     *  into asynchronous mode at some point) and don't send
     *  replies; their only means of reporting an error is to
     *  drop the connection.
     */
    if (g->g_rep_master == gses || g->g_rep_write == gses ||
        gses->gses_type == GRAPHD_SESSION_REPLICA_MASTER) {
      char const *const msg = greq->greq_error_message;
      greq->greq_error_message = NULL;
      srv_request_complete(&greq->greq_req);

      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "Replication protocol error on "
             "session %s: %s [aborting session]",
             gses->gses_ses.ses_displayname, msg);
      cl_leave(cl, CL_LEVEL_VERBOSE,
               "done; error on replica/import connection");
      srv_session_abort(&gses->gses_ses);

      return 0;
    }
    cl_leave(cl, CL_LEVEL_VERBOSE, "done; formatting error message");
    return graphd_format_error(g, srv, gses, greq, s, e);
  }

  if (g->g_rep_master == gses || g->g_rep_write == gses) {
    srv_request_complete(&greq->greq_req);
    leave_msg = "-- replica never says \"ok\"";

    goto leave_format;
  }

  if (GRAPHD_REQUEST_WRITE == greq->greq_request &&
      (GRAPHD_ACCESS_REPLICA == g->g_access ||
       GRAPHD_ACCESS_REPLICA_SYNC == g->g_access)) {
    leave_msg = "-- forwarding write";
    goto leave_format;
  }

  cl_assert(cl, e - *s >= 16);

  /* Write the "ok" portion of the response except for
   * restores on the replica session.
   */

  /* Deprecated - for now, send "rok" instead of "ok" if the
   * request was a "replica" request.
   */
  if (greq->greq_request == GRAPHD_REQUEST_REPLICA) *(*s)++ = 'r';

  *(*s)++ = 'o';
  *(*s)++ = 'k';

  if (greq->greq_request == GRAPHD_REQUEST_READ ||
      greq->greq_request == GRAPHD_REQUEST_ITERATE ||
      greq->greq_request == GRAPHD_REQUEST_ISLINK ||
      greq->greq_request == GRAPHD_REQUEST_WRITE ||
      greq->greq_request == GRAPHD_REQUEST_VERIFY ||
      greq->greq_request == GRAPHD_REQUEST_DUMP ||
      greq->greq_request == GRAPHD_REQUEST_STATUS ||
      greq->greq_request == GRAPHD_REQUEST_SYNC ||
      greq->greq_request == GRAPHD_REQUEST_REPLICA) {
    *(*s)++ = ' ';
    greq->greq_format_list_sep = 1;

    cl_assert(gses->gses_cl,
              greq->greq_reply.val_type != GRAPHD_VALUE_UNSPECIFIED);
    err = graphd_format_stack_push(gses, greq, &greq->greq_reply);
    if (err) {
      cl_leave(cl, CL_LEVEL_SPEW,
               "-- "
               "error from format_stack_push");
      return err;
    }

    greq->greq_format_s = NULL;
    if (greq->greq_parameter_head != NULL) {
      greq->greq_format_s = NULL;
      greq->greq_format = format_request_parameter;
    } else {
      greq->greq_format = graphd_format_result;
    }
    (*greq->greq_format)(data, srv, session_data, request_data, s, e);
  } else {
    *(*s)++ = '\n';
    srv_request_complete(&greq->greq_req);
  }

leave_format:
  if (!leave_msg) leave_msg = "";

  gses->gses_time_active = g->g_now;
  cl_leave(cl, CL_LEVEL_SPEW, "%s", leave_msg);

  return 0;
}

static void graphd_request_output_text_callback(void *data, srv_handle *srv,
                                                void *session_data,
                                                void *request_data, char **s,
                                                char *e) {
  graphd_request *greq = request_data;
  char const *p = greq->greq_format_s;

  if (s == NULL) {
    if (greq->greq_format_s_buf != NULL) {
      cm_free(greq->greq_format_s_cm, greq->greq_format_s_buf);
      greq->greq_format_s_cm = NULL;
      greq->greq_format_s_buf = NULL;
    }
    return;
  }

  while (*p != '\0' && *s < e) *(*s)++ = *p++;

  if (*p == '\0') {
    greq->greq_format_s = NULL;
    if (greq->greq_format_s_buf != NULL) {
      cm_free(greq->greq_format_s_cm, greq->greq_format_s_buf);
      greq->greq_format_s_cm = NULL;
      greq->greq_format_s_buf = NULL;
    }
    greq->greq_format = NULL;

    srv_request_output_done(&greq->greq_req);

    /*  If we haven't read input yet, but want to,
     *  start waiting for a reply.
     */
    if (!(greq->greq_req.req_done & (1 << SRV_INPUT)))
      srv_request_input_ready(&greq->greq_req);
    else
      graphd_request_served(greq);
  } else {
    greq->greq_format_s = p;
  }
}

/**
 * @brief Send a literal reply to a request.
 *
 * @param greq		the request
 * @param cm		NULL or an allocator the text was
 *			allocated in and should be free'd in.
 * @param text		the reply to send.
 *
 * @return 0 on success, nonzero error codes on unexpected
 * 	system errors.
 */
int graphd_request_output_text(graphd_request *greq, cm_handle *cm,
                               char const *text) {
  /* allocation failed? */
  if (text == NULL) return errno ? errno : ENOMEM;

  greq->greq_format = graphd_request_output_text_callback;
  greq->greq_format_s = text;
  greq->greq_format_s_buf = cm ? (char *)text : NULL;
  greq->greq_format_s_cm = cm;

  return 0;
}
