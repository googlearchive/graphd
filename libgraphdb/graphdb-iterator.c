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
#include "libgraphdb/graphdbp.h"

#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>

#define TOK_IS_LIT(s, e, lit) \
  ((e) - (s) == sizeof(lit) - 1 && strncasecmp(s, lit, sizeof(lit) - 1) == 0)

/**
 * @brief Read bytes of a token out of an iterator.
 *
 *  The bytes are interpreted within the iterator to determine
 *  the end of the iterator, but are not transformed for the caller.
 *
 *  The call terminates when either the output buffer is full
 *  or the iterator is empty.
 *
 * @param graphdb 	opaque module handle
 * @param it 		iterator whose memory is returned
 * @param s_out		out: output data
 * @param n_out 	out: number of bytes pointed to by *s_out
 *
 * @return 0 on success, ENOENT once we run out.
 */
int graphdb_iterator_read(graphdb_handle *graphdb, graphdb_iterator *it,
                          char const **s_out, size_t *n_out) {
  char const *s, *e;
  int res = 0;

  *s_out = NULL;
  *n_out = 0;

  if (it == NULL || it->it_buffer == NULL ||
      (it->it_depth == 0 &&
       (res = graphdb_iterator_peek(graphdb, it)) == ')')) {
    return ENOENT;
  }

  for (;;) {
    /*  Iterator is at the end of its buffer chain?
     */
    if (it->it_buffer == it->it_request->req_in_tail &&
        it->it_offset == it->it_request->req_in_tail_n) {
      break;
    }

    s = it->it_buffer->buf_data + it->it_offset;
    e = it->it_buffer->buf_data + (it->it_buffer == it->it_request->req_in_tail
                                       ? it->it_request->req_in_tail_n
                                       : it->it_buffer->buf_data_n);

    if (s < e) {
      graphdb_assert(graphdb, *s_out == NULL);

      res = graphdb_token_peek(graphdb, &it->it_tokenizer, s, e);
      if (res == ')' && it->it_depth == 0) {
        *s_out = NULL;
        *n_out = 0;

        return ENOENT;
      }

      *s_out = s;

      res = graphdb_token_skip(graphdb, &it->it_tokenizer, &s, e);
      it->it_offset = s - it->it_buffer->buf_data;

      if (res == GRAPHDB_TOKENIZE_EOF) {
        /* Didn't find anything? */

        *s_out = NULL;
        break;
      }

      *n_out = s - *s_out;

      graphdb_assert(graphdb, res != GRAPHDB_TOKENIZE_ERROR_MEMORY);
      graphdb_assert(graphdb, res != GRAPHDB_TOKENIZE_MORE);

      if (res == '(')
        it->it_depth++;
      else if (res == ')')
        it->it_depth--;

      return 0;
    }

    /*  Was this the last buffer?
     */
    if (it->it_buffer == it->it_request->req_in_tail) {
      /*  Yes.
       */
      graphdb_assert(graphdb, it->it_offset == it->it_request->req_in_tail_n);
      break;
    }

    /*  No -> go to the next one.
     */
    it->it_buffer = it->it_buffer->buf_next;
    graphdb_assert(graphdb, it->it_buffer != NULL);
    it->it_offset = 0;
  }
  return ENOENT;
}

/*  Allocate a  query iterator; if <it_parent> is non-NULL,
 *  as a child of (and incrementing the linkcount of) <it_parent>.
 *  The returned child has a linkcount of 1.
 */
graphdb_iterator *graphdb_iterator_alloc_loc(
    graphdb_request *req, graphdb_iterator *it_parent, /* can be NULL */
    char const *file, int line) {
  graphdb_iterator *it;

  it = (*req->req_heap->cm_realloc_loc)(req->req_heap, NULL, sizeof(*it), file,
                                        line);
  if (it == NULL) return NULL;

  memset(it, 0, sizeof(*it));

  it->it_request = req;
  if ((it->it_parent = it_parent) != NULL) {
    it_parent->it_refcount++;

    it->it_buffer = it_parent->it_buffer;
    it->it_offset = it_parent->it_offset;
  } else {
    req->req_refcount++;

    it->it_buffer = req->req_in_head;
    it->it_offset = req->req_in_head_i;
  }
  it->it_refcount = 1;
  it->it_depth = 0;

  graphdb_token_initialize(&it->it_tokenizer);

  return it;
}

/**
 * @brief Drop a link to an iterator.
 *
 *  If that was the last one, free the iterator's parent - or
 *  the request itself, if there is no parent.
 *
 * @param graphdb	The module handle.
 * @param it		NULL or the iterator to free.
 */
void graphdb_iterator_free(graphdb_handle *graphdb, graphdb_iterator *it) {
  if (it == NULL) return;

  for (;;) {
    graphdb_assert(graphdb, it == NULL || it->it_refcount > 0);

    if (it == NULL || it->it_refcount-- > 1) break;

    if (it->it_parent == NULL) {
      graphdb_log(graphdb, CL_LEVEL_SPEW,
                  "graphdb_iterator_free: "
                  "freeing iterator request %p (%zu)",
                  (void *)it->it_request,
                  it->it_request ? it->it_request->req_refcount : 0);

      if (it->it_request != NULL) {
        graphdb_request *req;

        req = it->it_request;
        it->it_request = NULL;

        graphdb_request_unlink_req(graphdb, req);
        graphdb_log(graphdb, CL_LEVEL_SPEW,
                    "graphdb_iterator_free: done unlinking.");
      }
      break;
    }
    it = it->it_parent;
  }
  graphdb_log(graphdb, CL_LEVEL_SPEW, "graphdb_iterator_free: done.");
}

/*  Pull the next token out of an iterator.
 *
 *  Evaluate (, ), and "" to keep track of the nesting depth.
 *  A closing parenthesis at depth 0 is treated like EOF.
 *
 *  Strings that span multiple buffers are glued together in
 *  the iterator's buffer.
 */
int graphdb_iterator_token(graphdb_handle *graphdb, graphdb_iterator *it,
                           char const **tok_s_out, char const **tok_e_out) {
  char const *s, *e;
  int res = 0;

  if (it == NULL || it->it_buffer == NULL ||
      (it->it_depth == 0 &&
       (res = graphdb_iterator_peek(graphdb, it)) == ')')) {
    graphdb_log(graphdb, CL_LEVEL_ULTRA, "IT(%p): EOF (%s)", (void *)it,
                it == NULL ? "null iterator"
                           : (it->it_buffer == NULL ? "null buffer"
                                                    : "close at zero depth"));

    return GRAPHDB_TOKENIZE_EOF;
  }
  for (;;) {
    /*  Iterator is at the end of its buffer chain?
     */
    if (it->it_buffer == it->it_request->req_in_tail &&
        it->it_offset == it->it_request->req_in_tail_n) {
      /* We're done.  Give the tokenizer a chance to
       * finish by passing NULL <s>, <e>.
       */
      res = graphdb_token(graphdb, &it->it_tokenizer, it->it_request->req_heap,
                          NULL, NULL, tok_s_out, tok_e_out);

      graphdb_assert(graphdb, res != GRAPHDB_TOKENIZE_MORE);
    } else {
      s = it->it_buffer->buf_data + it->it_offset;
      e = it->it_buffer->buf_data +
          (it->it_buffer == it->it_request->req_in_tail
               ? it->it_request->req_in_tail_n
               : it->it_buffer->buf_data_n);

      if (s >= e)
        res = GRAPHDB_TOKENIZE_MORE;
      else {
        res =
            graphdb_token(graphdb, &it->it_tokenizer, it->it_request->req_heap,
                          &s, e, tok_s_out, tok_e_out);
        it->it_offset = s - it->it_buffer->buf_data;
      }
    }

    if (res != GRAPHDB_TOKENIZE_MORE) {
      if (res == GRAPHDB_TOKENIZE_EOF)
        *tok_e_out = (*tok_s_out = "EOF") + 3;
      else if (res == GRAPHDB_TOKENIZE_ERROR_MEMORY)
        *tok_e_out = *tok_s_out = "";
      break;
    }

    /*  If we have more, go to the next buffer.
     */
    if (it->it_buffer == it->it_request->req_in_tail) {
      graphdb_assert(graphdb, it->it_offset == it->it_request->req_in_tail_n);
    } else {
      it->it_buffer = it->it_buffer->buf_next;
      graphdb_assert(graphdb, it->it_buffer != NULL);
      it->it_offset = 0;
    }
  }

  graphdb_assert(graphdb, res != GRAPHDB_TOKENIZE_MORE);

  if (res == GRAPHDB_TOKENIZE_EOF)
    graphdb_log(graphdb, CL_LEVEL_ULTRA, "IT(%p): EOF", (void *)it);
  else
    graphdb_log(graphdb, CL_LEVEL_ULTRA, "IT(%p @ %d): %c \"%.*s\"", (void *)it,
                (int)it->it_depth, res, (int)(*tok_e_out - *tok_s_out),
                *tok_s_out);

  if (res == '(')
    it->it_depth++;
  else if (res == ')') {
    if (it->it_depth == 0) {
      graphdb_iterator_token_unget(graphdb, it, res, *tok_s_out, *tok_e_out);
      return GRAPHDB_TOKENIZE_EOF;
    }
    it->it_depth--;
  }
  return res;
}

/*  Pull the next token out of an iterator, and throw it away.
 *
 *  Evaluate (, ), and "" to keep track of the nesting depth.
 *  A closing parenthesis at depth 0 is treated like EOF.
 */
int graphdb_iterator_token_skip(graphdb_handle *graphdb, graphdb_iterator *it) {
  char const *s, *e;
  char const *tok_s, *tok_e;
  int res = 0;

  if (it == NULL || it->it_buffer == NULL ||
      (it->it_depth == 0 &&
       (res = graphdb_iterator_peek(graphdb, it)) == ')')) {
    graphdb_log(graphdb, CL_LEVEL_ULTRA, "IT(%p): EOF (%s)", (void *)it,
                it == NULL ? "null iterator"
                           : (it->it_buffer == NULL ? "null buffer"
                                                    : "close at zero depth"));

    return GRAPHDB_TOKENIZE_EOF;
  }
  for (;;) {
    /*  Iterator is at the end of its buffer chain?
     */
    if (it->it_buffer == it->it_request->req_in_tail &&
        it->it_offset == it->it_request->req_in_tail_n) {
      /* We're done.  Give the tokenizer a chance to
       * finish by passing NULL <s>, <e>.
       */
      res = graphdb_token(graphdb, &it->it_tokenizer, it->it_request->req_heap,
                          NULL, NULL, &tok_s, &tok_e);

      graphdb_assert(graphdb, res != GRAPHDB_TOKENIZE_MORE);
    } else {
      s = it->it_buffer->buf_data + it->it_offset;
      e = it->it_buffer->buf_data +
          (it->it_buffer == it->it_request->req_in_tail
               ? it->it_request->req_in_tail_n
               : it->it_buffer->buf_data_n);

      if (s >= e)
        res = GRAPHDB_TOKENIZE_MORE;
      else {
        /* Push the data into the tokenizer. */

        res = graphdb_token(graphdb, &it->it_tokenizer,
                            it->it_request->req_heap, &s, e, &tok_s, &tok_e);
        it->it_offset = s - it->it_buffer->buf_data;
      }
    }

    if (res != GRAPHDB_TOKENIZE_MORE) break;

    /*  If we have more, go to the next buffer.
     */
    if (it->it_buffer == it->it_request->req_in_tail)
      graphdb_assert(graphdb, it->it_offset == it->it_request->req_in_tail_n);
    else {
      it->it_buffer = it->it_buffer->buf_next;
      graphdb_assert(graphdb, it->it_buffer != NULL);
      it->it_offset = 0;
    }
  }

  graphdb_assert(graphdb, res != GRAPHDB_TOKENIZE_MORE);

  if (res == '(')
    it->it_depth++;
  else if (res == ')') {
    if (it->it_depth == 0) {
      graphdb_iterator_token_unget(graphdb, it, res, tok_s, tok_e);
      return GRAPHDB_TOKENIZE_EOF;
    }
    it->it_depth--;
  }
  return res;
}

/*  Push a token back into the iterator.
 */
void graphdb_iterator_token_unget(graphdb_handle *graphdb, graphdb_iterator *it,
                                  int tok, char const *tok_s,
                                  char const *tok_e) {
  graphdb_assert(graphdb, it != NULL);

  if (tok == ')')
    it->it_depth++;
  else if (tok == '(' && it->it_depth > 0)
    it->it_depth--;

  graphdb_token_unget(graphdb, &it->it_tokenizer, tok, tok_s, tok_e);
}

/*  What's the upcoming token?
 */
int graphdb_iterator_peek(graphdb_handle *graphdb, graphdb_iterator const *it) {
  char const *s, *e;
  int err;
  graphdb_buffer *buf;
  unsigned long long off;

  graphdb_request *req = it->it_request;

  if (it->it_buffer == NULL) return GRAPHDB_TOKENIZE_EOF;

  buf = it->it_buffer;
  off = it->it_offset;

  for (;;) {
    if (buf == req->req_in_tail && off == req->req_in_tail_n) {
      err = graphdb_token_peek(graphdb, &it->it_tokenizer, NULL, NULL);
      graphdb_assert(graphdb, err != GRAPHDB_TOKENIZE_MORE);
      return err;
    }

    s = buf->buf_data + off;
    e = buf->buf_data +
        ((buf == req->req_in_tail) ? req->req_in_tail_n : buf->buf_data_n);

    if (s >= e)
      err = GRAPHDB_TOKENIZE_MORE;
    else {
      err = graphdb_token_peek(graphdb, &it->it_tokenizer, s, e);
      if (err == GRAPHDB_TOKENIZE_MORE) off = e - buf->buf_data;
    }

    if (err != GRAPHDB_TOKENIZE_MORE) return err;

    if (buf == req->req_in_tail)
      graphdb_assert(graphdb, off == req->req_in_tail_n);
    else {
      off = 0;
      buf = buf->buf_next;

      graphdb_assert(graphdb, buf != NULL);
    }
  }
}

/*  Set the error state of an iterator.
 */
static char const out_of_memory[] = "out of memory while copying error text";

void graphdb_iterator_error_set(graphdb_handle *graphdb, graphdb_iterator *it,
                                int err, char const *fmt, ...) {
  va_list ap;
  char bigbuf[1024 * 4];

  va_start(ap, fmt);
  vsnprintf(bigbuf, sizeof bigbuf, fmt, ap);
  va_end(ap);

  graphdb_log(graphdb, CL_LEVEL_DEBUG, "graphdb_iterator_error_set: %s (%s)",
              bigbuf, strerror(err));

  it->it_error_number = err;

  if (it->it_error_text != NULL && it->it_error_text != out_of_memory)
    cm_free(it->it_request->req_heap, (char *)it->it_error_text);

  it->it_error_text = cm_bufmalcpy(it->it_request->req_heap, bigbuf);
  if (it->it_error_text == NULL) it->it_error_text = out_of_memory;
}

/**
 * @brief Are we at EOF?
 *
 * @param graphdb	module handle
 * @param it		Iterator
 *
 * @return true if the iterator is at the end of its data, false otherwise.
 */
bool graphdb_iterator_eof(graphdb_handle *graphdb, graphdb_iterator *it) {
  return graphdb_iterator_peek(graphdb, it) == GRAPHDB_TOKENIZE_EOF;
}

/**
 * @brief Pull a byte sequence out of an iterator.
 *
 *  This is much like graphbd_iterator_string, but with less copying.
 *
 * @param graphdb	module handle
 * @param it		Iterator for a request.
 * @param s_out		Out: first byte of the string
 * @param e_out		Out: pointer just after last byte of the string.
 *
 * @return 0 on success, a nonzero error code on error
 */
int graphdb_iterator_bytes(graphdb_handle *graphdb, graphdb_iterator *it,
                           char const **s_out, char const **e_out) {
  graphdb_request *req;
  char const *s, *e;
  int res;

  if (graphdb == NULL || it == NULL) return ENOENT;

  req = it->it_request;

  res = graphdb_iterator_token(graphdb, it, &s, &e);
  if (res == '(' || res == ')' || res == GRAPHDB_TOKENIZE_EOF || res == '\n') {
    graphdb_iterator_token_unget(graphdb, it, res, s, e);
    return ENOENT;
  }

  if (TOK_IS_LIT(s, e, "null")) {
    *s_out = *e_out = NULL;
    return 0;
  }

  if (res == '"' && e - s >= 2 && s[0] == '"' && e[-1] == '"') {
    char *buf, *w;

    if (memchr(s, e - s, '\\') == 0) {
      *s_out = s + 1;
      *e_out = e - 1;

      return 0;
    }

    if ((buf = cm_malloc(req->req_heap, 1 + (e - s))) == NULL)
      return errno ? errno : ENOMEM;

    *s_out = w = buf;
    s++;
    e--;
    while (s < e) {
      if (*s == '\\' && s + 1 < e && *++s == 'n') {
        *w++ = '\n';
        s++;
      } else
        *w++ = *s++;
    }
    *w = '\0';
    *e_out = w;
  } else {
    *s_out = s;
    *e_out = e;
  }
  return 0;
}

/**
 * @brief Pull a string out of an iterator.
 *
 *  If successful, the iterator's state is advanced past the string.
 *
 *  The call fails if  the next token at the iterator is something
 *  other than a string or an atom.
 *
 *  An atom that is "null" converts to a null pointer.
 *  All other atoms convert to their spelling.
 *  Strings convert to their contents.
 *
 * @param graphdb	module handle
 * @param it		Iterator for a request.
 *
 * @return NULL if there was no string or if the next token was the
  *  atom "null", otherwise a \\0-terminated string.
 */
char const *graphdb_iterator_string(graphdb_handle *graphdb,
                                    graphdb_iterator *it) {
  graphdb_request *req;
  char const *s, *e;
  char *buf;
  int res;

  if (graphdb == NULL || it == NULL) return NULL;

  req = it->it_request;

  res = graphdb_iterator_token(graphdb, it, &s, &e);
  if (res == '(' || res == ')' || res == GRAPHDB_TOKENIZE_EOF || res == '\n') {
    graphdb_iterator_token_unget(graphdb, it, res, s, e);
    return NULL;
  }

  if (TOK_IS_LIT(s, e, "null")) return NULL;

  if ((buf = cm_malloc(req->req_heap, 1 + (e - s))) == NULL) return NULL;

  if (res == '"' && s < e && s[0] == '"' && e[-1] == '"') {
    char *w = buf;

    s++;
    e--;
    while (s < e) {
      if (*s == '\\' && s + 1 < e && *++s == 'n') {
        *w++ = '\n';
        s++;
      } else
        *w++ = *s++;
    }
    *w = '\0';
  } else {
    memcpy(buf, s, e - s);
    buf[e - s] = '\0';
  }
  return buf;
}

/**
 * @brief Pull a GUID out of an iterator.
 *
 *  If successful, the iterator's state is advanced past the GUID.
 *
 *  The call fails if  the next token at the iterator is something
 *  other than a string or an atom.
 *
 *  An atom that is "null" or "0" converts to a null GUID.
 *  All other atoms or strings convert to their contents,
 *  interpreted as a GUID.
 *
 * @param graphdb	module handle
 * @param it		Iterator for a request.
 * @param guid_out	Assing the GUID to this.
 *
 * @return 0 if a GUID was found and converted
 * @return ENOENT if we're out of tokens
 * @return EILSEQ if there is something, but it's not a token.
 */
int graphdb_iterator_guid(graphdb_handle *graphdb, graphdb_iterator *it,
                          graph_guid *guid_out) {
  char const *s, *e;
  int res;

  if (graphdb == NULL || it == NULL) return ENOENT;

  res = graphdb_iterator_token(graphdb, it, &s, &e);
  if (res == '(' || res == ')' || res == GRAPHDB_TOKENIZE_EOF || res == '\n') {
    graphdb_iterator_token_unget(graphdb, it, res, s, e);
    return ENOENT;
  }

  if (TOK_IS_LIT(s, e, "null")) {
    GRAPH_GUID_MAKE_NULL(*guid_out);
    return 0;
  }

  if (graph_guid_from_string(guid_out, s, e) != 0) {
    graphdb_iterator_token_unget(graphdb, it, res, s, e);
    return EILSEQ;
  }
  return 0;
}

/**
 * @brief Pull a list out of an iterator.
 *
 * @param graphdb	module handle
 * @param it		Iterator for a request.
 *
 * @return NULL if there is no list, otherwise an iterator for
 *  	the elements of the list. (E.g., as one would get for "(%...)")
 */
graphdb_iterator *graphdb_iterator_list(graphdb_handle *graphdb,
                                        graphdb_iterator *it) {
  char const *s, *e;
  int res;
  graphdb_iterator *result_it;
  unsigned int depth = 1;

  if (graphdb == NULL || it == NULL) return NULL;

  if (graphdb_iterator_peek(graphdb, it) != '(') return NULL;

  res = graphdb_iterator_token(graphdb, it, &s, &e);
  result_it =
      graphdb_iterator_alloc_loc(it->it_request, it, __FILE__, __LINE__);
  if (result_it == NULL) return NULL;

  /* skip past the list in <it>. */
  for (;;) {
    res = graphdb_iterator_token(graphdb, it, &s, &e);
    if (res == ')') {
      if (depth-- == 1) break;
    } else if (res == '(')
      depth++;
    else if (res == GRAPHDB_TOKENIZE_EOF) {
      graphdb_iterator_error_set(graphdb, it, ENOENT,
                                 "short reply: expected \")\" or more list "
                                 "elements");
      return result_it;
    } else if (res == GRAPHDB_TOKENIZE_ERROR_MEMORY)
      return result_it;
  }
  return result_it;
}

/**
 * @brief Pull a list or null out of an iterator.
 *
 * @param graphdb	module handle
 * @param it		Iterator for a request.
 *
 * @return NULL if there is no list, otherwise an iterator for
 *  	the elements of the list. (E.g., as one would get for "(%...)")
 */
int graphdb_iterator_list_or_null(graphdb_handle *graphdb, graphdb_iterator *it,
                                  graphdb_iterator **it_out) {
  char const *s, *e;
  int res;
  graphdb_iterator *result_it;
  unsigned int depth = 1;
  int glimpse;

  *it_out = NULL;

  if (graphdb == NULL || it == NULL) return ENOENT;

  glimpse = graphdb_iterator_peek(graphdb, it);
  if (tolower(glimpse) == 'n') {
    res = graphdb_iterator_token(graphdb, it, &s, &e);
    if (tolower(res) == 'n' && TOK_IS_LIT(s, e, "null")) return 0;

    /*  We were expecting null, but got something else -
     *  put it back and complain.
     */
    graphdb_iterator_token_unget(graphdb, it, res, s, e);
    return EILSEQ;
  } else if (glimpse != '(')
    return EILSEQ;

  res = graphdb_iterator_token(graphdb, it, &s, &e);
  *it_out = result_it =
      graphdb_iterator_alloc_loc(it->it_request, it, __FILE__, __LINE__);
  if (result_it == NULL) return errno ? errno : ENOMEM;

  /* skip past the list in <it>. */
  for (;;) {
    res = graphdb_iterator_token(graphdb, it, &s, &e);
    if (res == ')') {
      if (depth-- == 1) break;
    } else if (res == '(')
      depth++;
    else if (res == GRAPHDB_TOKENIZE_EOF) {
      graphdb_iterator_error_set(graphdb, it, ENOENT,
                                 "short reply: expected \")\" or more list "
                                 "elements");
      graphdb_iterator_free(graphdb, *it_out);
      return ENOENT;
    } else if (res == GRAPHDB_TOKENIZE_ERROR_MEMORY) {
      graphdb_iterator_free(graphdb, *it_out);
      return errno ? errno : ENOMEM;
    }
  }
  return 0;
}

/**
 * @brief Decode the server error code returned by a request.
 *
 * @param graphdb	module handle
 * @param it		Toplevel iterator for the request's text,
 *			e.g. as returned by graphdb_request_wait_iterator().
 *
 * @return 0 if there was no error
 * @return GRAPHDB_ERROR_EMPTY	if the request started with EMPTY
 * @return GRAPHDB_ERROR_EXISTS if the request started with EXISTS
 * @return GRAPHDB_ERROR_INTERNAL if the system couldn't decode the error.
 */
graphdb_server_error graphdb_iterator_server_error(graphdb_handle *graphdb,
                                                   graphdb_iterator *it) {
  char const *tok_s, *tok_e;
  int res;

  /*  We're looking for (optionally) the word "error",
   *  followed by the code.
   */
  res = graphdb_iterator_token(graphdb, it, &tok_s, &tok_e);

  if (!isascii(res) || !isalnum(res) ||
      (tolower(res) == 'o' && TOK_IS_LIT(tok_s, tok_e, "ok"))) {
    graphdb_iterator_token_unget(graphdb, it, res, tok_s, tok_e);
    return 0;
  }

  if ((res == 'e' || res == 'E') && TOK_IS_LIT(tok_s, tok_e, "error"))
    res = graphdb_iterator_token(graphdb, it, &tok_s, &tok_e);

  if (res == GRAPHDB_TOKENIZE_EOF) return GRAPHDB_SERVER_ERROR_INTERNAL;

  return graphdb_server_error_hash_bytes(tok_s, tok_e);
}

/**
 * @brief Decode the error message that was returned by a request.
 *
 * @param graphdb	module handle
 * @param it		Toplevel iterator for the request's text,
 *			e.g. as returned by graphdb_request_wait_iterator().
 *
 * @return NULL if there was no error
 * @return a decoded (and unqouted) error message in the request.
 */
char const *graphdb_iterator_server_error_string(graphdb_handle *graphdb,
                                                 graphdb_iterator *it) {
  /*  We're looking for (optionally) the word "error",
   *  followed by (optionally) the code,
   *  followed by the string.  Return that string.
   */
  char const *tok_s, *tok_e;
  char const *result;
  int res;

  /*  Are we already positioned on a string?  If yes, just use that.
   */
  res = graphdb_iterator_peek(graphdb, it);
  if (res != '"') {
    int prev = 0;

    /*  Move to the first string that isn't preceded
     *  by an equal sign.
     */
    for (;;) {
      prev = res;
      res = graphdb_iterator_token(graphdb, it, &tok_s, &tok_e);

      if (res == EOF || !isascii(res)) break;

      if (tolower(res) == 'o' && TOK_IS_LIT(tok_s, tok_e, "ok")) {
        graphdb_iterator_token_unget(graphdb, it, res, tok_s, tok_e);
        return NULL;
      } else if (res == '"' && prev != '=')
        break;
    }
    if (res != '"')
      return "error while parsing error result - "
             "expected string";

    /*  Push back the string we just positioned on.
     */
    graphdb_iterator_token_unget(graphdb, it, res, tok_s, tok_e);
  }

  /* If we're not on a string at this point, something is wrong.
   */
  result = graphdb_iterator_string(graphdb, it);
  if (result == NULL)
    return "error while parsing error result - expected string";
  return result;
}
