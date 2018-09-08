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
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "libgraph/graph.h"

#define GRAPHDB_MIN_FORMATTING_SPACE 10

static char const *graphdb_meta_to_string(int meta) {
  switch (meta) {
    case GRAPHDB_META_NODE:
      return "node";
    case GRAPHDB_META_LINK_TO:
      return "<-";
    case GRAPHDB_META_LINK_FROM:
      return "->";
    default:
      break;
  }
  return NULL;
}

/*  Remove newlines outside of parentheses or strings
 *  (by replacing them with spaces)
 *  from the passed-in buffer or buffer chain.
 *  If needed, add or convert a newline at the end.
 */
void graphdb_buffer_format_dwim(graphdb_handle *graphdb, graphdb_buffer *buf) {
  bool in_string = false;
  bool escaped = false;
  unsigned int parens = 0;

  while (buf != NULL) {
    size_t i;

    for (i = buf->buf_data_i; i < buf->buf_data_n; i++) {
      if (escaped) {
        escaped = false;
        continue;
      }

      switch (buf->buf_data[i]) {
        case '(':
          if (!in_string) parens++;
          break;

        case ')':
          if (!in_string) parens--;
          break;

        case '"':
          in_string = !in_string;
          break;

        case '\n':
          if (!parens && !in_string) buf->buf_data[i] = ' ';
          break;

        case '\\':
          if (in_string) escaped = true;
          break;

        default:
          break;
      }
    }

    /*  If this is the last buffer in the chain, make
     *  sure it ends with a newline.
     */
    if (buf->buf_next == NULL) {
      i = buf->buf_data_n;
      while (i > 0 && isascii(buf->buf_data[i - 1]) &&
             isspace(buf->buf_data[i - 1]))
        i--;

      /*  We can do this exacty once--
       *  we allocated a spare byte
       *  originally.
       */
      if (i >= buf->buf_data_m) buf->buf_data_m++;
      buf->buf_data[i] = '\n';
      buf->buf_data_n = i + 1;
    }
    buf = buf->buf_next;
  }
}

/**
 * @brief Format into a buffer, appending new buffers as needed.
 *
 * @param graphdb	handle created with graphdb_create()
 * @param buf		buffer to format into
 * @param fmt		%%x formatting sequence
 * @param popper	the abstract argument list
 *
 * @return EINVAL for invalid formatting sequences
 * @return ENOMEM on allocation error
 * @return 0 on success.
 */

int graphdb_buffer_pformat(graphdb_handle *graphdb, graphdb_buffer *buf,
                           char const *fmt, graphdb_arg_popper *popper) {
  graphdb_buffer *last;
  char *s, *e, valbuf[200];
  char const *val_s = NULL, *val_e = NULL;
  char const *qval_s = NULL, *qval_e = NULL;
  int qval_state = 0, have_size = 0;
  size_t n, val_size = 0;
  unsigned char last_ch = 0;
  char const *const fmt0 = fmt;
  int err = 0;

  if (buf == NULL) return EINVAL;

  /*  Make sure there's at least one buffer in the chain -- itself!
   */
  if (buf->buf_tail == NULL || buf->buf_tail == &buf->buf_head) {
    buf->buf_head = buf;
    buf->buf_tail = &buf->buf_next;
    buf->buf_next = NULL;
  }

  while (*fmt != '\0' || qval_s != NULL || val_s != NULL) {
    /*  Set <last> to the last buffer in the chain, and
     *  s..e to the free buffer space in it.
     */

    graphdb_assert(graphdb, buf->buf_tail != NULL);
    graphdb_assert(graphdb, buf->buf_tail != &buf->buf_head);

    last = (void *)((char *)buf->buf_tail - offsetof(graphdb_buffer, buf_next));

    s = last->buf_data + last->buf_data_n;
    e = last->buf_data + last->buf_data_m;

    /*  Format into this buffer.
     */
    while (s < e && (val_s != NULL || qval_s != NULL || *fmt != '\0')) {
    /* Finish writing a literal value, if we have one.
     */
    have_val:
      if (val_s != NULL) {
        n = val_e - val_s;
        if (n > e - s) n = e - s;

        if (n > 0) {
          memcpy(s, val_s, n);

          s += n;
          val_s += n;

          last_ch = s[-1];
        }
        if (val_s >= val_e)
          val_s = val_e = NULL;
        else {
          graphdb_assert(graphdb, s >= e);
          break;
        }
      }

    have_qval:
      if (qval_s != NULL) {
        char const *p;

        /* Leading " */
        if (qval_state == 0) {
          qval_state++;
          val_e = (val_s = "\"") + 1;
          goto have_val;
        }

        /*  Seqeuence of unescaped characters */
        p = qval_s;
        while (p < qval_e && *p != '\\' && *p != '\n' && *p != '\"') p++;
        if (p > qval_s) {
          val_s = qval_s;
          val_e = p;
          qval_s = p;
          goto have_val;
        }
        if (qval_s >= qval_e) {
          /*  Closing " */

          graphdb_assert(graphdb, qval_state == 1);
          qval_state++;

          val_e = (val_s = "\"") + 1;
          qval_s = qval_e = NULL;

          goto have_val;
        }
        qval_s++;
        val_s = valbuf;
        *valbuf = '\\';
        switch (*p) {
          case '\n':
            valbuf[1] = 'n';
            val_e = valbuf + 2;
            break;
          case '"':
          case '\\':
            valbuf[1] = *p;
            val_e = valbuf + 2;
            break;
          default:
            /* This can't happen, right? */
            snprintf(valbuf + 1, sizeof(valbuf) - 1, "x%2.2x",
                     (unsigned char)*p);
            val_e = val_s + 4;
        }
        goto have_val;
      }

      if (*fmt != '%') {
        if (*fmt == '\0') break;

        val_s = fmt;
        for (; *fmt != '\0' && *fmt != '%'; fmt++)
          ;
        val_e = fmt;
        goto have_val;
      }

      if (e - s < GRAPHDB_MIN_FORMATTING_SPACE) break;

      graphdb_assert(graphdb, *fmt == '%');
      fmt++;

      if ((have_size = (*fmt == '*')) != 0) {
        have_size = 1;
        if ((err = graphdb_pop_size(popper, &val_size))) return err;
        fmt++;
      }

      switch (*fmt++) {
        case '\0':
          last_ch = *s++ = '%';
          fmt--;
          break;

        case '%':
          last_ch = *s++ = '%';
          break;

        case 'q': /* "\-quoted string" */
          if ((err = graphdb_pop_string(popper, &qval_s))) return err;
          if (qval_s != NULL)
            qval_e = qval_s + (have_size ? val_size : strlen(qval_s));
          else {
            val_s = "null";
            val_e = val_s + strlen(val_s);
            goto have_val;
          }
          qval_state = 0;
          goto have_qval;

        case 's': /* string */
          if ((err = graphdb_pop_string(popper, &val_s))) return err;
          if (val_s != NULL)
            val_e = val_s + (have_size ? val_size : strlen(val_s));
          else {
            val_s = "null";
            val_e = (have_size && val_size == 0 ? val_s : val_s + 4);
          }
          goto have_val;

        case 'g': /* guid */
        {
          graph_guid *ptr = NULL, value;
          if ((err = graphdb_pop_guid(popper, &ptr, &value))) return err;
          if (ptr == NULL) {
            val_s = "null";
            val_e = val_s + 4;
            goto have_val;
          }
          val_s = graph_guid_to_string(ptr, valbuf, sizeof valbuf);
        }
          val_e = val_s + strlen(val_s);
          goto have_val;

        case 't': /* timestamp */
        {
          graph_timestamp_t ts;
          if ((err = graphdb_pop_timestamp(popper, &ts))) return err;
          val_s = graph_timestamp_to_string(ts, valbuf, sizeof(valbuf));
        }
          val_e = val_s + strlen(val_s);
          goto have_val;

        case 'u': /* unsigned */
        {
          unsigned long long u;
          if ((err = graphdb_pop_ull(popper, &u))) return err;
          snprintf(valbuf, sizeof valbuf, "%llu", u);
          val_s = valbuf;
        }
          val_e = val_s + strlen(val_s);
          goto have_val;

        case 'm': /* meta */
        {
          int m;
          if ((err = graphdb_pop_int(popper, &m))) return err;
          val_s = graphdb_meta_to_string(m);
          if (val_s == NULL) {
            graphdb_notreached(graphdb,
                               "unexpected meta value "
                               "%d (while formatting \"%s\")",
                               m, fmt0);
            return EINVAL;
          }
          val_e = val_s + strlen(val_s);
          goto have_val;
        }

        case 'b': /* bool */
        {
          int b;
          if ((err = graphdb_pop_int(popper, &b))) return err;
          val_s = b ? "true" : "false";
          val_e = val_s + (b ? 4 : 5);
        }
          goto have_val;

        case 'd': /* datatype */
        {
          graph_datatype d;
          if ((err = graphdb_pop_datatype(popper, &d))) return err;
          val_s = graph_datatype_to_string(d);
        }
          if (val_s == NULL) return EINVAL;
          val_e = val_s + strlen(val_s);
          goto have_val;
        default:
          graphdb_notreached(graphdb,
                             "unexpected format sequence "
                             "%%%c in \"%s\"",
                             fmt[-1], fmt0);
          return EINVAL;
      }
    }

    /*  Update the buffer fill level depending on what we just
     *  wrote.
     */
    last->buf_data_n = s - last->buf_data;

    /*  Append a \n, if we're done and the buffer didn't end
     *  with one.
     */
    if (*fmt == '\0' && qval_s == NULL && val_s == NULL && last_ch != '\n') {
      val_s = "\n";
      val_e = val_s + 1;

      continue;
    }

    /*  If we have more to write, create another buffer for it.
     */
    if (*fmt != '\0' || val_s != NULL || qval_s != NULL) {
      *buf->buf_tail = graphdb_buffer_alloc_heap(graphdb, buf->buf_heap, 4096);
      if (*buf->buf_tail == NULL) return ENOMEM;

      buf->buf_tail = &(*buf->buf_tail)->buf_next;
    }
  }

  return 0;
}

int graphdb_buffer_vformat(graphdb_handle *graphdb, graphdb_buffer *buf,
                           char const *fmt, va_list ap) {
  graphdb_va_arg_popper popper;
  int err;

  popper.ga_generic.ga_fns = &graphdb_va_arg_popper_fns;

  va_copy(popper.ga_ap, ap);
  err =
      graphdb_buffer_pformat(graphdb, buf, fmt, (graphdb_arg_popper *)&popper);
  va_end(popper.ga_ap);

  return err;
}

/**
 * @brief Format into a buffer, appending new buffers as needed.
 *
 *  Uses #graphdb_buffer_format() to do the real work.
 *
 * @param graphdb	handle created with graphdb_create()
 * @param buf		buffer to format into
 * @param fmt		%%x formatting sequence
 * @param ...		variable arguments.
 *
 * @return EINVAL for invalid formatting sequences
 * @return ENOMEM on allocation error
 * @return 0 on success.
 */

int graphdb_buffer_format(graphdb_handle *graphdb, graphdb_buffer *buf,
                          char const *fmt, ...) {
  int err;
  graphdb_va_arg_popper popper;

  popper.ga_generic.ga_fns = &graphdb_va_arg_popper_fns;
  va_start(popper.ga_ap, fmt);
  err =
      graphdb_buffer_pformat(graphdb, buf, fmt, (graphdb_arg_popper *)&popper);
  va_end(popper.ga_ap);

  return err;
}
