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
// NOTE(rtp): These two need to be in this order for now.
#include "libgraphdb/graphdbp.h"
#include "libgraphdb/graphdb-args.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>
#include <stdarg.h>

#include "libgraph/graph.h"


#define IS_LIT(lit, s, e) \
  ((e) - (s) == sizeof(lit) - 1 && !strncasecmp((s), lit, sizeof(lit) - 1))

/*  Replace \x with x, overwriting the leading bytes of s...e;
 *  return a pointer to the new e.
 */
static char const *unquote_in_place(char *s, char const *e) {
  char const *r = s;

  while (r < e) {
    if (*r == '\\' && r + 1 < e) r++;
    *s++ = *r++;
  }
  *s = '\0';
  return s;
}

/*  If <s...e> is a string (enclosed in double quotes), return
 *  its string contents.  Otherwise, just return it.
 *
 *  If the string contains escape characters, make a mutable
 *  copy on the heap and remove the escape characters in that copy.
 *
 *  The mutable copy, if needed, is allocated on <heap> and is
 *  \0-terminated.
 *
 *  The call returns ENOMEM on error, otherwise 0 on success.
 */
static int text_content(cm_handle *heap, char const *s, char const *e,
                        char **s_buf_out, char const **s_out,
                        char const **e_out) {
  *s_buf_out = NULL;

  if (s < e && *s == '"') {
    s++;

    /* Needs copying? */
    if (memchr(s, '\\', (int)(e - s)) == NULL) {
      /* No, just omit the surrounding "" */
      if (e > s && e[-1] == '"') --e;
    } else {
      /* Yes, make a modifiable copy. */
      *s_buf_out = cm_substr(heap, s, e);
      if (*s_buf_out == NULL) return ENOMEM;

      e = unquote_in_place(*s_buf_out, *s_buf_out + (e - s));
      s = *s_buf_out;
    }
  }
  *s_out = s;
  *e_out = e;

  return 0;
}

/*  Get a token from iterator <it>.  It's supposed to be an atom
 *  or string; return an error (EILSEQ) if it isn't.
 *
 *  If it is a string, decode it (stripping the other "" characters).
 *  Return the contents in <*s_out..*e_out>.  If a mutable copy was
 *  allocated somewhere along the way,	*s_buf_out will be set to its
 *  base, and it will be \0-terminated.	 (Otherwise, the string need
 *  not be \0-terminated.)
 *
 *  If we run out of memory along the way, return ENOMEM; otherwise,
 *  return 0 on success.
 */
static int get_atom(graphdb_handle *graphdb, graphdb_iterator *it,
                    char **s_buf_out, char const **s_out, char const **e_out) {
  int t;

  *s_buf_out = NULL;

  /* skip commas */
  do {
    t = graphdb_iterator_token(graphdb, it, s_out, e_out);

    graphdb_assert(graphdb, t != GRAPHDB_TOKENIZE_MORE);
    if (t == GRAPHDB_TOKENIZE_ERROR_MEMORY) return ENOMEM;
    if (t == GRAPHDB_TOKENIZE_EOF) {
      graphdb_iterator_error_set(graphdb, it, ENOENT,
                                 "short reply: expected atom");
      return ENOENT;
    }
  } while (t == ',');

  /* expecting an atom, looking at a list boundary? */
  if (t == '(' || t == ')') {
    graphdb_iterator_token_unget(graphdb, it, t, *s_out, *e_out);
    graphdb_iterator_error_set(graphdb, it, EILSEQ,
                               "unexpected punctuation: expected atom, got %c",
                               t);
    return EILSEQ;
  }
  return t == '"' ? text_content(it->it_request->req_heap, *s_out, *e_out,
                                 s_buf_out, s_out, e_out)
                  : 0;
}

/**
 * @brief Get english text for an error that occurred while parsing
 *	query results, and reset the iterator error status.
 *
 * @param graphdb	module handle, created with graphdb_create()
 *			and used in call to graphdb_query()
 * @param it		iterator used in call to graphdb_query() or
 *			graphdb_vquery() that returned the error.
 * @param err		error number returned by graphdb_query_next()
 *			or graphdb_query_vnext()
 */
char const *graphdb_query_error(graphdb_handle *graphdb, graphdb_iterator *it,
                                int err) {
  char const *result;

  if (it == NULL || it->it_error_number == 0) {
    switch (err) {
      case 0:
        break;
      default:
        return strerror(err);
    }
    return NULL;
  }

  it->it_error_number = 0;

  result = it->it_error_text;
  it->it_error_text = NULL;

  return result ? result : strerror(err);
}

/**
 * @brief Free a query iterator.
 *
 * Iterators must be freed explicitly.	Once all iterators
 * associated with a query are freed, the query data itself
 * is freed and becomes unavailable.
 *
 * @param graphdb	module handle, created with graphdb_create()
 *			and used in call to graphdb_query()
 * @param it		iterator returned by a call to graphdb_query() or
 *			graphdb_vquery(), or graphdb_query_next()
 *			or graphdb_query_vnext().
 */
void graphdb_query_free(graphdb_handle *graphdb, graphdb_iterator *it) {
  if (graphdb != NULL && it != NULL) graphdb_iterator_free(graphdb, it);
}

static int graphdb_query_scan_bool(graphdb_handle *graphdb,
                                   graphdb_iterator *it, char const *s,
                                   char const *e, int *ptr) {
  int err = 0;

  if (ptr == NULL) {
    graphdb_iterator_error_set(graphdb, it, EINVAL,
                               "NULL pointer "
                               "parameter for %%b in "
                               "graphdb_query_(v)next");
    return EINVAL;
  }

  err = 0;
  if (e - s == 4) {
    if (strncasecmp(s, "null", 4) == 0)
      *ptr = 0;
    else if (strncasecmp(s, "true", 4) == 0)
      *ptr = 1;
    else
      err = EILSEQ;
  } else if (e - s == 5) {
    if (strncasecmp(s, "false", 5) == 0)
      *ptr = 0;
    else
      err = EILSEQ;
  }

  if (err) {
    if (err == EILSEQ)
      graphdb_iterator_token_unget(graphdb, it, (unsigned char)*s, s, e);

    graphdb_iterator_error_set(
        graphdb, it, err,
        "syntax error: expected boolean (true or false), "
        "got \"%.*s%s\"",
        e - s > 80 ? 80 : (int)(e - s), s, e - s > 80 ? "..." : "");
    return err;
  }
  return 0;
}

static int graphdb_query_scan_unsigned(graphdb_handle *graphdb,
                                       graphdb_iterator *it, char const *s,
                                       char const *e, unsigned long long *ptr) {
  int err = 0;

  if (ptr == NULL) {
    graphdb_iterator_error_set(graphdb, it, EINVAL,
                               "NULL pointer "
                               "parameter for %%u in "
                               "graphdb_query_(v)next");
    return EINVAL;
  }

  if (e - s == 4 && strncasecmp(s, "null", 4) == 0)
    *ptr = 0;
  else {
    int sign = 1;
    char const *p = s;

    *ptr = 0;
    if (p < e) {
      if (*p == '-')
        sign = -1, p++;
      else if (*p == '+')
        p++;
    }
    while (p < e && isascii(*p) && isdigit(*p)) {
      if (*ptr > (unsigned long long)-1 / 10 - (*p - '0')) {
        err = ERANGE;
        break;
      }
      *ptr = *ptr * 10 + *p++ - '0';
    }
    if (err == ERANGE) {
      graphdb_iterator_error_set(
          graphdb, it, err, "overflow: integer \"%.*s%s\" out of range",
          e - s > 80 ? 80 : (int)(e - s), s, e - s > 80 ? "..." : "");
      return err;
    } else if (p < e) {
      graphdb_iterator_token_unget(graphdb, it, (unsigned char)*s, s, e);
      graphdb_iterator_error_set(graphdb, it, EILSEQ,
                                 "syntax error: expected integer, "
                                 "got \"%.*s%s\"",
                                 e - s > 80 ? 80 : (int)(e - s), s,
                                 e - s > 80 ? "..." : "");
      return EILSEQ;
    }
    if (sign == -1) *ptr = -*ptr;
  }
  return 0;
}

graphdb_iterator *graphdb_query_dup_loc(graphdb_handle *graphdb,
                                        graphdb_iterator *it, char const *file,
                                        int line) {
  if (it == NULL) return NULL;
  return graphdb_iterator_alloc_loc(it->it_request, it, file, line);
}

/**
 * @brief Return graph query result as (almost) unparsed bytes.
 *
 * @param graphdb		module handle
 * @param it			iterator returned by graphdb_query() or
 *				graphdb_vquery().
 * @param s_out			out: result chunk
 * @param n_out			out: number of bytes pointed to by *s_out
 *
 * @returns 0 on success
 * @returns ENOENT when all result chunks have been returned
 */
int graphdb_query_read_bytes(graphdb_handle *graphdb, graphdb_iterator *it,
                             char const **s_out, size_t *n_out) {
  if (it == NULL || graphdb == NULL) return EINVAL;
  return graphdb_iterator_read(graphdb, it, s_out, n_out);
}

int graphdb_query_pnext_loc(graphdb_handle *graphdb, graphdb_iterator *it,
                            char const *fmt, graphdb_arg_pusher *pusher,
                            char const *file, int line) {
  int err = 0, t;

  char const *s, *e;
  char *buf;

  char *fmt_atom_buf;
  char const *fmt_atom_s, *fmt_atom_e;
  char const *fmt_e;

  size_t depth;
  int do_assign;
  int eof_error = ENOENT;

  if (it == NULL || graphdb == NULL || fmt == NULL) return EINVAL;

  if (it->it_request != NULL && it->it_request->req_errno != 0) {
    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "graphdb_query_pnext(fmt=%s): stored request error: %s", fmt,
                strerror(it->it_request->req_errno));

    return it->it_request->req_errno;
  }

  errno = 0;

  graphdb_log(graphdb, CL_LEVEL_DEBUG, "graphdb_query_pnext(fmt=%s)", fmt);

  for (; *fmt != '\0'; fmt++) {
    /* Skip space, tab, and commas in the format. */
    while (*fmt == ',' || *fmt == ' ' || *fmt == '\t') fmt++;
    if (*fmt == '\0') break;

    /* Skip commas in the arriving reply data. */
    while ((t = graphdb_iterator_peek(graphdb, it)) == ',')
      (void)graphdb_iterator_token(graphdb, it, &s, &e);

    switch (*fmt) {
      case '(':
        t = graphdb_iterator_token(graphdb, it, &s, &e);
        graphdb_assert(graphdb, t != GRAPHDB_TOKENIZE_MORE);
        if (t == '(') continue;

        if (t == GRAPHDB_TOKENIZE_ERROR_MEMORY)
          err = ENOMEM;
        else if (t == GRAPHDB_TOKENIZE_EOF)
          graphdb_iterator_error_set(graphdb, it, err = eof_error,
                                     "short reply: expected \"(\", got EOF");
        else {
          graphdb_iterator_token_unget(graphdb, it, (unsigned char)*s, s, e);
          graphdb_iterator_error_set(graphdb, it, err = EILSEQ,
                                     "not a list: expected \"(\", "
                                     "got \"%.*s%s\"",
                                     e - s > 80 ? 80 : (int)(e - s), s,
                                     e - s > 80 ? "..." : "");
        }
        return err;

      case ')':
        t = graphdb_iterator_token(graphdb, it, &s, &e);
        graphdb_assert(graphdb, t != GRAPHDB_TOKENIZE_MORE);
        if (t == ')') continue;

        if (t == GRAPHDB_TOKENIZE_ERROR_MEMORY)
          err = ENOMEM;
        else if (t == GRAPHDB_TOKENIZE_EOF)
          graphdb_iterator_error_set(graphdb, it, err = eof_error,
                                     "end-of-input in list: expected \")\"");
        else
          graphdb_iterator_error_set(graphdb, it, err = ENOTEMPTY,
                                     "list too long: expected \")\", "
                                     "got \"%.*s%s\"",
                                     e - s > 80 ? 80 : (int)(e - s), s,
                                     e - s > 80 ? "..." : "");
        return err;

      case '.':
        depth = 0;

        /* In fmt, skip all but the last dot. */
        while (*fmt == '.') fmt++;
        fmt--;

        /* In the list, skip the rest of this list. */
        while (depth > 0 || ((t = graphdb_iterator_peek(graphdb, it)) != ')' &&
                             t != GRAPHDB_TOKENIZE_EOF)) {
          t = graphdb_iterator_token(graphdb, it, &s, &e);
          if (t == GRAPHDB_TOKENIZE_ERROR_MEMORY)
            return ENOMEM;
          else if (t == GRAPHDB_TOKENIZE_EOF) {
            graphdb_iterator_error_set(graphdb, it, EILSEQ,
                                       "EOF in list: "
                                       "expected \")\" or elements");
            return EILSEQ;
          } else if (t == '(')
            depth++;
          else if (t == ')')
            depth--;
        }
        continue;

      case '%':
        break;

      default:
        /*  <fmt> spells out a specific atom or string that
         *  we're expecting here.  Verify that it's there.
         */
        err = get_atom(graphdb, it, &buf, &s, &e);
        if (err) {
          if (err == ENOENT) err = eof_error;
          return err;
        }

        fmt_e = graphdb_token_atom_end(fmt);
        err = text_content(it->it_request->req_heap, fmt, fmt_e, &fmt_atom_buf,
                           &fmt_atom_s, &fmt_atom_e);
        if (err != 0) return err;

        if (e - s != fmt_atom_e - fmt_atom_s ||
            strncasecmp(s, fmt_atom_s, e - s) != 0) {
          graphdb_iterator_token_unget(graphdb, it, t, s, e);
          graphdb_iterator_error_set(
              graphdb, it, EILSEQ, "expected \"%.*s\", got \"%.*s%s\"",
              (int)(fmt_atom_e - fmt_atom_s), fmt_atom_s,
              e - s > 80 ? 80 : (int)(e - s), s, e - s > 80 ? "..." : "");

          return EILSEQ;
        }
        fmt = fmt_e - 1;
        continue;
    }

    graphdb_assert(graphdb, *fmt == '%');
    do_assign = 1;
    fmt++;

    /* Assignment supression character '*' as in real scanf:
     */
    if (*fmt == '*' && fmt[1] != '\0') {
      do_assign = 0;
      fmt++;
    }

    switch (*fmt) {
      case '\0':
        graphdb_iterator_error_set(graphdb, it, EILSEQ,
                                   "format string ends in single %%");
        return EINVAL;

      case 'o':
        /* octets: base + length */
        t = graphdb_iterator_token(graphdb, it, &s, &e);
        graphdb_assert(graphdb, t != GRAPHDB_TOKENIZE_MORE);

        if (t == GRAPHDB_TOKENIZE_EOF) {
          graphdb_iterator_error_set(graphdb, it, eof_error,
                                     "short reply: "
                                     "expected atom or string");
          return eof_error;
        }
        if (t == GRAPHDB_TOKENIZE_ERROR_MEMORY) return ENOMEM;

        if (do_assign) {
          char const *_s;
          size_t _n;

          if ((t == 'n' || t == 'N') && e - s == 4 &&
              strncasecmp(s, "null", 4) == 0) {
            _s = NULL;
            _n = 0;
          } else {
            _s = s;
            _n = (size_t)(e - s);
          }

          if ((err = graphdb_push_string(pusher, _s)) ||
              (err = graphdb_push_size(pusher, _n)))
            return err;
        }
        break;

      case 's': /* string	*/
        t = graphdb_iterator_token(graphdb, it, &s, &e);

        graphdb_assert(graphdb, t != GRAPHDB_TOKENIZE_MORE);

        if (t == GRAPHDB_TOKENIZE_EOF) {
          graphdb_iterator_error_set(graphdb, it, eof_error,
                                     "short reply: expected atom or string"
                                     ", got end-of-data");
          return eof_error;
        }
        if (t == GRAPHDB_TOKENIZE_ERROR_MEMORY) return ENOMEM;

        if (do_assign) {
          char *_s;

          if (t == 'n' && e - s == 4 && strncasecmp(s, "null", 4) == 0)
            _s = NULL;
          else {
            _s = cm_substr(it->it_request->req_heap, s, e);
            if (_s == NULL) return ENOMEM;
          }

          if ((err = graphdb_push_string(pusher, _s))) return err;
        }
        break;

      case 'u': /* unsigned long long  */
        t = graphdb_iterator_token(graphdb, it, &s, &e);
        graphdb_assert(graphdb, t != GRAPHDB_TOKENIZE_MORE);

        if (t == GRAPHDB_TOKENIZE_EOF) {
          graphdb_iterator_error_set(graphdb, it, eof_error,
                                     "short reply: expected integer"
                                     ", got end-of-data");
          return eof_error;
        }
        if (t == GRAPHDB_TOKENIZE_ERROR_MEMORY) return ENOMEM;

        {
          unsigned long long _v;

          err = graphdb_query_scan_unsigned(graphdb, it, s, e, &_v);
          if (!err && do_assign) err = graphdb_push_ull(pusher, _v);
          if (err != 0) return err;
          break;
        }

      case 't': /* timestamp	*/
      {
        graph_timestamp_t ts;

        err = get_atom(graphdb, it, &buf, &s, &e);
        if (err != 0) {
          if (err == ENOENT) err = eof_error;
          return err;
        }

        if (s == e || IS_LIT("null", s, e))
          ts = 0;
        else {
          err = graph_timestamp_from_string(&ts, s, e);
          if (err != 0) {
            graphdb_iterator_token_unget(graphdb, it, (unsigned char)*s, s, e);
            graphdb_iterator_error_set(graphdb, it, err,
                                       "syntax error: "
                                       "expected timestamp, got "
                                       "\"%.*s%s\"",
                                       e - s > 80 ? 80 : (int)(e - s), s,
                                       e - s > 80 ? "..." : "");
            return err;
          }
        }

        if (do_assign && (err = graphdb_push_timestamp(pusher, ts))) return err;
        break;
      }

      case 'm': /* meta		*/
      {
        int meta;

        err = get_atom(graphdb, it, &buf, &s, &e);
        if (err != 0) {
          if (err == ENOENT) err = eof_error;
          return err;
        }

        if (s == e || IS_LIT("node", s, e))
          meta = GRAPHDB_META_NODE;
        else if (IS_LIT("<-", s, e))
          meta = GRAPHDB_META_LINK_TO;
        else if (IS_LIT("->", s, e))
          meta = GRAPHDB_META_LINK_FROM;
        else {
          graphdb_iterator_token_unget(graphdb, it, (unsigned char)*s, s, e);
          graphdb_iterator_error_set(graphdb, it, err,
                                     "syntax error: expected "
                                     "\"node\", \"->\", or \"<-\","
                                     " got \"%.*s%s\"",
                                     e - s > 80 ? 80 : (int)(e - s), s,
                                     e - s > 80 ? "..." : "");
          return err;
        }
        if (do_assign && (err = graphdb_push_int(pusher, meta))) return err;
        break;
      }

      case 'b': /* bool		*/
        t = graphdb_iterator_token(graphdb, it, &s, &e);
        graphdb_assert(graphdb, t != GRAPHDB_TOKENIZE_MORE);

        if (t == GRAPHDB_TOKENIZE_EOF) {
          graphdb_iterator_error_set(graphdb, it, eof_error,
                                     "short reply: expected boolean"
                                     ", got end-of-data");
          return eof_error;
        }
        if (t == GRAPHDB_TOKENIZE_ERROR_MEMORY) return ENOMEM;

        {
          int _v = 0;

          err = graphdb_query_scan_bool(graphdb, it, s, e, &_v);
          if (err != 0) return err;
          if (do_assign && (err = graphdb_push_int(pusher, _v))) return err;
          break;
        }

      case 'd': /* datatype	*/
        t = graphdb_iterator_token(graphdb, it, &s, &e);
        graphdb_assert(graphdb, t != GRAPHDB_TOKENIZE_MORE);

        if (t == GRAPHDB_TOKENIZE_EOF) {
          graphdb_iterator_error_set(graphdb, it, eof_error,
                                     "short reply: expected datatype"
                                     ", got EOF");
          return eof_error;
        }
        if (t == GRAPHDB_TOKENIZE_ERROR_MEMORY) return ENOMEM;

        {
          graph_datatype _v;

          err = graph_datatype_from_string(&_v, s, e);
          if (err != 0) {
            graphdb_iterator_token_unget(graphdb, it, (unsigned char)*s, s, e);
            graphdb_iterator_error_set(graphdb, it, err,
                                       "syntax error: expected datatype, "
                                       "got \"%.*s%s\"",
                                       e - s > 80 ? 80 : (int)(e - s), s,
                                       e - s > 80 ? "..." : "");
            return err;
          }
          if (do_assign && (err = graphdb_push_datatype(pusher, _v)))
            return err;
          break;
        }

      case 'g': /* guid		*/
      {
        graph_guid guid;

        err = get_atom(graphdb, it, &buf, &s, &e);
        if (err != 0) {
          if (err == ENOENT) err = eof_error;
          return err;
        }

        if (s == e || IS_LIT("null", s, e) || IS_LIT("0", s, e))
          GRAPH_GUID_MAKE_NULL(guid);
        else if ((err = graph_guid_from_string(&guid, s, e)) != 0) {
          graphdb_iterator_token_unget(graphdb, it, (unsigned char)*s, s, e);
          graphdb_iterator_error_set(graphdb, it, err,
                                     "syntax error: "
                                     "expected GUID, got "
                                     "\"%.*s%s\"",
                                     e - s > 80 ? 80 : (int)(e - s), s,
                                     e - s > 80 ? "..." : "");
          return err;
        }
        if (do_assign && (err = graphdb_push_guid(pusher, guid))) return err;
        break;
      }

      case '.':
        /* skip rest of the ellipsis */

        if (do_assign) {
          graphdb_iterator *_it;
          _it = graphdb_iterator_alloc_loc(it->it_request, it, file, line);
          if (_it == NULL) return ENOMEM;
          if ((err = graphdb_push_iterator(pusher, _it))) return err;
        }
        depth = 0;

        /* In fmt, skip all but the last dot. */
        while (*fmt == '.') fmt++;
        fmt--;

        /* In the list, skip the rest of this list. */
        while (depth > 0 || ((t = graphdb_iterator_peek(graphdb, it)) != ')' &&
                             t != GRAPHDB_TOKENIZE_EOF)) {
          t = graphdb_iterator_token(graphdb, it, &s, &e);
          if (t == GRAPHDB_TOKENIZE_EOF) {
            /* If this EOF were on the outermost "
             * level, the peek in the loop head
             * would have caught it.  Conseqeuently,
             * this is always a sequencing error,
             * never just an EOF.
             */
            graphdb_iterator_error_set(graphdb, it, EILSEQ,
                                       "short reply: "
                                       "end-of-data in list");
            return EILSEQ;
          } else if (t == GRAPHDB_TOKENIZE_ERROR_MEMORY)
            return ENOMEM;
          else if (t == '(')
            depth++;
          else if (t == ')')
            depth--;
        }
        break;

      case 'n':

        /* like '%l', but null counts, too
         */
        t = graphdb_iterator_peek(graphdb, it);
        if (tolower(t) == 'n') {
          t = graphdb_iterator_token(graphdb, it, &s, &e);

          if (tolower(t) == 'n' && IS_LIT("null", s, e)) {
            if (do_assign) {
              err = graphdb_push_iterator(pusher, NULL);
              if (err != 0) return err;
            }
            break;
          }

          /*  We were expecting null, but got
           *  something else - put it back and
           *  complain in the next round.
           */
          graphdb_iterator_token_unget(graphdb, it, t, s, e);
        }

      /* FALL THROUGH */

      case 'l':
        if ((t = graphdb_iterator_peek(graphdb, it)) != '(') {
          /*  Get the token anwyay, so
           *  we can complain about what it is.
           */
          t = graphdb_iterator_token(graphdb, it, &s, &e);
          if (t == GRAPHDB_TOKENIZE_ERROR_MEMORY)
            return ENOMEM;
          else if (t == GRAPHDB_TOKENIZE_EOF)
            graphdb_iterator_error_set(graphdb, it, err = eof_error,
                                       "short reply: expected "
                                       "\"(\", got EOF");
          else {
            graphdb_iterator_token_unget(graphdb, it, (unsigned char)*s, s, e);
            graphdb_iterator_error_set(graphdb, it, err = EILSEQ,
                                       "not a list: expected \"(\", "
                                       "got \"%.*s%s\"",
                                       e - s > 80 ? 80 : (int)(e - s), s,
                                       e - s > 80 ? "..." : "");
          }
          return err;
        }
        if (do_assign) {
          graphdb_iterator *_it;
          _it = graphdb_iterator_alloc_loc(it->it_request, it, file, line);
          if (_it == NULL) return ENOMEM;
          if ((err = graphdb_push_iterator(pusher, _it))) return err;
        }
        depth = 0;

        /* skip the list ahead. */
        for (;;) {
          t = graphdb_iterator_token(graphdb, it, &s, &e);
          if (t == ')') {
            if (depth-- == 1) break;
          } else if (t == '(')
            depth++;
          else if (t == GRAPHDB_TOKENIZE_EOF) {
            graphdb_iterator_error_set(graphdb, it, eof_error,
                                       "short reply: "
                                       "expected \")\" or more list "
                                       "elements");
            return eof_error;
          } else if (t == GRAPHDB_TOKENIZE_ERROR_MEMORY)
            return ENOMEM;
        }
        break;

      default:
        graphdb_iterator_error_set(graphdb, it, EINVAL,
                                   "unexpected format sequence %%%c", *fmt);
        return EINVAL;
    }

    eof_error = EILSEQ;
  }
  return 0;
}

int graphdb_query_vnext_loc(graphdb_handle *graphdb, graphdb_iterator *it,
                            char const *fmt, va_list ap, char const *file,
                            int line) {
  graphdb_va_arg_pusher pusher;
  int err;

  pusher.ga_generic.ga_fns = &graphdb_va_arg_pusher_fns;
  va_copy(pusher.ga_ap, ap);

  err = graphdb_query_pnext_loc(graphdb, it, fmt, (graphdb_arg_pusher *)&pusher,
                                file, line);
  va_end(pusher.ga_ap);
  return err;
}

#ifndef GRAPHDB_HAVE_C9X_VA_ARGS

/**
 * @brief Scan results of a graph query.
 *
 * The results of a graph query - a sequence of nested atoms or strings
 * and lists of atoms or strings - is scanned according to a
 * format string.  Different elements of the format string match
 * different elements of the result.
 *
 * @param graphdb		module handle
 * @param it			iterator returned by graphdb_query() or
 *				graphdb_vquery().
 * @param fmt			format string for scanning results.
 *
 * @returns 0 on success
 * @returns ENOENT after encountering EOF at the beginning
 * @returns EILSEQ after a syntax error or EOF in the middle
 * @returns ENOMEM if running out of memory.
 * @returns EINVAL if invoked with a NULL handle or text pointer and
 *	non-NULL text size
 *
 * @par Formats
 * @par
 * tab, space, @b , (comma) \n
 * Ignored.\n
 * @par
 * @b ( \n
 * The current token must be the beginning of a list.
 * @par
 * @b ) \n
 * The current token must be the end of a list.
 * @par
 * @b ... \n
 * Skip all following atoms in this list (or request), up to the
 * end of the list (or request).
 * @par
 * atom-or-string\n
 * The given atom or string, with or without quotes.
 * @par
 * @b %%o \n
 * Octets.  The parameters are a char ** and a size_t *.
 * The char ** is assigned a pointer to the parsed atom or string,
 * or NULL if the parsed atom was the unquoted word null.
 * The size_t * is assigned the number of bytes pointed to by the char **,
 * or 0 if the parsed atom was the unquoted word null.
 * @par
 * @b %%*o \n
 * Octets, but don't assign.  An arbitrary string or atom is skipped.
 * No arguments are popped off the call stack or assigned to.
 * (This convention is taken from C scanf, where * functions as
 * the "assignment suppression character".)
 * @par
 * @b %%s \n
 * String. The parameter is a char **.
 * The char ** is assigned a pointer to the parsed atom or string,
 * or NULL if the parsed atom was the unquoted word null.
 * The string is null-terminated, and is allocated in the per-request
 * memory (and will be free()d once the last iterator on the request
 * is free()d.)
 * @par
 * @b %%*s \n
 * String, but don't assign.  Has the same effect as %%*o.
 * @par
 * @b %%t \n
 * Timestamp.  The argument is a graph_timestamp_t *.  If the
 * token is the unquoted word null, the timestamp is assigned
 * the value zero.  Otherwise, the token must match a timestamp;
 * it is parsed and assigned to the argument as graph_timestamp_t.
 * @par
 * @b %%*t \n
 * Timestamp, but don't assign.	 A token that must be either
 * the unquoted word null or a valid timestamp is skipped.
 * @b %%b \n
 * Boolean.  The parameter is an int *.
 * The int * is assigned 1 or 0 whether the parsed atom
 * was the unquoted word "true" or either the unquoted word "false" or "null".
 * @par
 * @b %%*b \n
 * Boolean, but don't assign.  An arbitrary boolean is skipped.
 * No arguments are popped off the call stack or assigned to.
 * @b %%d \n
 * Datatype.  The parameter is a graph_datatype *.
 * The pointed-to-location is assigned an value corresponding to
 * the unquoted word in the enumeration.
 * @par
 * @b %%*d \n
 * Datatype, but don't assign.	An arbitrary datatype is skipped.
 * No arguments are popped off the call stack or assigned to.
 * @par
 * @b %%m \n
 * Meta.  The current input token must be one of "node",
 * "->", or "<-".  The corresponding parameter is an int *.  It is
 * assigned #GRAPHDB_META_NODE,
 * #GRAPHDB_META_LINK_FROM, or
 * #GRAPHDB_META_LINK_TO, respectively.
 * @par
 * @b %%m \n
 * Meta, but don't assign.  The current input token must be one of "node",
 * "->", or "<-", and is skipped.
 * @par
 * @b %%g \n
 * GUID.  The argument is a graph_guid *.  If the
 * token is the unquoted word null, a null GUID is assigned
 * to the pointer, the token must match a GUID;
 * it is parsed and assigned to the argument.
 * @par
 * @b %%*g \n
 * GUID, but don't assign.  A token that must be either
 * the unquoted word null or a valid GUID is skipped.
 * @b %%u \n
 * unsigned.  The argument is an unsigned long long *.
 * If the token is the unquoted word null, zero is assigned.
 * Otherwise, the token must match a signed or unsigned
 * integer; it is parsed and assigned to the argument.
 * (A negative integer is converted to the argument type
 * according to the identity -X = (MAX + 1) + X.)
 * @par
 * @b %%*u \n
 * Unsigned, but don't assign.	A token that must be either
 * the unquoted word null or a valid signed or unsigned
 * integer is skipped.
 * @par
 * @b %... \n
 * Rest of the list, as an iterator.  The argument is
 * a graphdb_iterator **.  It is assigned an iterator
 * over the remainder of the current list; the calling
 * iterator will skip to the end, to just before the closing ")".
 * @par
 * @b %%*... \n
 * Like %..., but don't assign.	 Same as just "...".
 * @par
 * @b %%l \n
 * A full parenthesized list.  Same as (%...).
 * @par
 * @b %%*l \n
 * A full parenthesized list, but don't assign.	 Same as (...).
 */
int graphdb_query_next(graphdb_handle *graphdb, graphdb_iterator *it,
                       char const *fmt, ...) {
  int err;
  graphdb_va_arg_pusher pusher;

  pusher.ga_generic.ga_fns = &graphdb_va_arg_pusher_fns;
  va_start(pusher.ga_ap, fmt);
  err = graphdb_query_pnext_loc(graphdb, it, fmt, (graphdb_arg_pusher *)&pusher,
                                __FILE__, __LINE__);
  va_end(pusher.ga_ap);

  return err;
}

#endif /* GRAPHDB_HAVE_C9X_VA_ARGS */

int graphdb_query_next_loc(graphdb_handle *graphdb, graphdb_iterator *it,
                           char const *file, int line, char const *fmt, ...) {
  int err;
  graphdb_va_arg_pusher pusher;

  va_start(pusher.ga_ap, fmt);
  pusher.ga_generic.ga_fns = &graphdb_va_arg_pusher_fns;
  err = graphdb_query_pnext_loc(graphdb, it, fmt, (graphdb_arg_pusher *)&pusher,
                                file, line);
  va_end(pusher.ga_ap);
  return err;
}

int graphdb_pquery_loc(graphdb_handle *graphdb, graphdb_iterator **it_out,
                       long timeout, char const *file, int line,
                       char const *fmt, graphdb_arg_popper *popper) {
  graphdb_buffer *buf;
  graphdb_request *req, *wait_req;
  int err = 0;
  unsigned long long deadline;

  graphdb_log(graphdb, CL_LEVEL_SPEW, "graphdb_(v)query: timeout is %ld",
              timeout);

  if (graphdb == NULL) return EINVAL;
  if (it_out == NULL) {
    graphdb_log(graphdb, CL_LEVEL_ERROR,
                "graphdb_(v)query: null iterator result pointer");
    return EINVAL;
  }
  *it_out = NULL;

  if (fmt == NULL) {
    graphdb_log(graphdb, CL_LEVEL_ERROR,
                "graphdb_(v)query: null format string");
    return EINVAL;
  }
  if (timeout < -1) {
    graphdb_log(graphdb, CL_LEVEL_ERROR,
                "graphdb_(v)query: unexpected timeout %ld (expecting "
                "value >= -1)",
                timeout);
    return EINVAL;
  }

  errno = 0;

  deadline = timeout <= 0 ? timeout : graphdb_time_millis() + timeout;

  req = graphdb_request_alloc(graphdb);
  if (req == NULL) return errno ? errno : ENOMEM;

  /*  The handling code now holds one reference to
   *  req; we must free it if we abort.
   */

  buf = graphdb_buffer_alloc_heap(graphdb, req->req_heap, 4 * 1024);
  if (buf == NULL) {
    graphdb_request_unlink_req(graphdb, req);
    return errno ? errno : ENOMEM;
  }

  if ((err = graphdb_buffer_pformat(graphdb, buf, fmt, popper)) != 0 ||
      (err = graphdb_request_send_buffer_req(graphdb, req, buf)) != 0) {
    graphdb_log(graphdb, CL_LEVEL_SPEW,
                "graphdb_vquery: error; free request %p", (void *)req);
    graphdb_request_unlink_req(graphdb, req);
    return err;
  }

  wait_req = req;
  if ((err = graphdb_request_wait_req(graphdb, &wait_req, deadline)) != 0) {
    graphdb_log(graphdb, CL_LEVEL_SPEW,
                "graphdb_vquery: error; discard reference to %p", (void *)req);
    req->req_cancelled = 1;
    graphdb_request_unlink_req(graphdb, req);

    return err;
  }

  *it_out = graphdb_iterator_alloc_loc(req, NULL, file, line);
  if (*it_out == NULL) {
    graphdb_log(graphdb, CL_LEVEL_SPEW,
                "graphdb_vquery: iterator alloc fails; free request %p",
                (void *)req);
    graphdb_request_unlink_req(graphdb, req);
    return ENOMEM;
  }

  /* The iterator we're returning now holds a reference
   * to the request.  The one we're freeing here is the
   * code reference we acquired in graphdb_request_alloc().
   */
  graphdb_request_unlink_req(graphdb, req);
  return 0;
}

int graphdb_vquery_loc(graphdb_handle *graphdb, graphdb_iterator **it_out,
                       long timeout, char const *file, int line,
                       char const *fmt, va_list ap) {
  graphdb_va_arg_popper popper;
  int err;

  popper.ga_generic.ga_fns = &graphdb_va_arg_popper_fns;
  va_copy(popper.ga_ap, ap);

  err = graphdb_pquery_loc(graphdb, it_out, timeout, file, line, fmt,
                           (graphdb_arg_popper *)&popper);

  va_end(popper.ga_ap);
  return err;
}

#if !GRAPHDB_HAVE_C9X_VA_ARGS

/**
 * @brief Format and send a graph query.
 * This sends a command or other piece of text to a server.
 * If the text doesn't end in a newline, one is supplied by the call.
 * The result of sending the request can be obtained by calling
 * graphdb_query_next() on an iterator assigned by a successful call.
 *
 * @param graphdb		handle created with graphdb_create() and
 *				connected to a server with graphdb_connect().
 * @param it_out		location to which an iterator is assigned.
 * @param timeout		timeout in milliseconds;
 *				-1 means infinity.
 * @param fmt			format string for request; followed by
 *			arguments for the format sequences in the string.
 *
 * @par Formats
 * In the format string, literal % must be escaped
 * as %%.  Other than that, the following format
 * sequences have the following meaning:\n
 * @par
 * @b %%q \n
 * The argument, a char const *, is either NULL
 * or points to a '\\0'-terminated string.  NULL is sent as the word null.
 * Everything else is sent as a quoted strings, with contained \\ or "
 * or newline characters properly escaped.\n
 * @par
 * @b %%*q \n
 * Like %%q, but the arguments are a size_t
 * and a char const *, and the string need not be '\\0'-terminated.
 * A NULL string pointer is sent as the word null, regardless of the size_t.
 * @par
 * @b %%s \n
 * Arbitrary text, included literally.
 * The argument is a char const * pointing to a '\\0'-terminated string.
 * A NULL pointer is sent as the word null.
 * @par
 * @b %%*s \n
 * Like %%s, but the arguments are a size_t and a char const *,
 * and the string need not be '\\0'-terminated.
 * A NULL pointer with non-zero size is sent as the word null.
 * @par
 * @b %%g \n
 * A GUID.  The argument is NULL or a graph_guid const *.  Both
 * NULL and a NULL graph guid are sent as the word null.
 * @par
 * @b %%b \n
 * A boolean.  The argument is an int.	If it is zero, the word
 * false is sent; otherwise, the word true.
 * @par
 * @b %%d \n
 * A datatype.	The argument is an int.	 Its value is interpreted
 * as a graph_datatype and sent as a word.
 * @par
 * @b %%u \n
 * An unsigned number.	The argument is an unsigned long long; its
 * numeric value is sent as decimal digits.
 * @par
 * @b %%t \n
 * A time stamp.  The argument is a graph_timestamp_t.	A
 *	zero timestamp is sent as the word null.
 *
 * @returns 0 on success
 * @returns ENOMEM if running out of memory.
 * @returns EINVAL if invoked with a NULL handle or text pointer and
 *	non-NULL text size
 */
int graphdb_query(graphdb_handle *graphdb, graphdb_iterator **it_out,
                  long timeout, char const *fmt, ...) {
  int err;
  graphdb_va_arg_popper popper;

  popper.ga_generic.ga_fns = &graphdb_va_arg_popper_fns;
  va_start(popper.ga_ap, fmt);
  err = graphdb_pquery_loc(graphdb, it_out, timeout, __FILE__, __LINE__, fmt,
                           (graphdb_arg_popper *)&popper);
  va_end(popper.ga_ap);

  return err;
}

#endif /* ! GRAPHDB_HAVE_C9X_VA_ARGS */

int graphdb_query_loc(graphdb_handle *graphdb, graphdb_iterator **it_out,
                      long timeout, char const *file, int line, char const *fmt,
                      ...) {
  int err;
  graphdb_va_arg_popper popper;

  popper.ga_generic.ga_fns = &graphdb_va_arg_popper_fns;
  va_start(popper.ga_ap, fmt);
  err = graphdb_pquery_loc(graphdb, it_out, timeout, file, line, fmt,
                           (graphdb_arg_popper *)&popper);
  va_end(popper.ga_ap);
  return err;
}
