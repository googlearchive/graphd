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
#include "libgdp/gdp.h"

#include <ctype.h>
#include <errno.h>

/* A dummy "null" string value for graphd functions that need it */
static char const nil[] = "null";
#define NIL_S nil
#define NIL_E (nil + sizeof(nil) - 1)

/**
 * Token names.
 */
static const char *gdp_token_names[] = {
        [TOK_END] = "END",   [TOK_ATOM] = "ATOM",   [TOK_VAR] = "VAR",
        [TOK_STR] = "STR",   [TOK_NULL] = "NULL",   [TOK_OPAR] = "OPAR",
        [TOK_CPAR] = "CPAR", [TOK_LARR] = "LARR",   [TOK_RARR] = "RARR",
        [TOK_EQ] = "EQ",     [TOK_NE] = "NE",       [TOK_FE] = "FE",
        [TOK_LT] = "LT",     [TOK_LE] = "LE",       [TOK_GT] = "GT",
        [TOK_GE] = "GE",     [TOK_MINUS] = "MINUS", [TOK_PLUS] = "PLUS",
        [TOK_OBRC] = "OBRC", [TOK_CBRC] = "CBRC",   [TOK_BOR] = "BOR",
        [TOK_LOR] = "LOR",
};

bool gdp_token_matches(const gdp_token *tok, const char *img) {
  const char *s = tok->tkn_start;
  const char *e = tok->tkn_end;
  size_t len;

  switch (tok->tkn_kind) {
    case TOK_ATOM:
    case TOK_STR:
      len = e - s;
      break;
    default:
      return false;
  }

  return !strncasecmp(img, s, len) && (img[len] == '\0');
}

size_t gdp_token_len(const gdp_token *tok) {
  const char *s = tok->tkn_start;
  const char *e = tok->tkn_end;

  switch (tok->tkn_kind) {
    case TOK_ATOM:
    case TOK_STR:
      return e - s;
      break;
    default:
      return 0;
  }
}

int gdp_token_toull(const gdp_token *tok, unsigned long long *val) {
  const char *s = tok->tkn_start;
  const char *e = tok->tkn_end;
  unsigned long long u;

  switch (tok->tkn_kind) {
    case TOK_ATOM:
    case TOK_STR:
      break;
    default:
      return EINVAL;
  }

  if (e == s) return EINVAL;

  u = 0;
  for (; s < e; s++) {
    const unsigned char ch = *s;
    unsigned long long t;

    if (!isdigit(ch)) return EINVAL;

    t = (u * 10) + (ch - '0');
    if (t < u) return EINVAL;
    u = t;
  }

  *val = u;

  return 0;
}

int gdp_token_toguid(const gdp_token *tok, graph_guid *guid) {
  char const *s;
  char const *e;

  switch (tok->tkn_kind) {
    case TOK_ATOM:
    case TOK_STR:
      s = tok->tkn_start;
      e = tok->tkn_end;
      break;
    case TOK_NULL:
      s = NIL_S;
      e = NIL_E;
      break;
    default:
      return EINVAL;
  }

  return graph_guid_from_string(guid, s, e);
}

int gdp_token_totype(const gdp_token *tok, graph_datatype *type) {
  const char *s;
  const char *e;

  switch (tok->tkn_kind) {
    case TOK_ATOM:
    case TOK_STR:
      s = tok->tkn_start;
      e = tok->tkn_end;
      break;
    case TOK_NULL:
      s = NIL_S;
      e = NIL_E;
      break;
    default:
      return EINVAL;
  }

  if (graph_datatype_from_string(type, s, e)) return EINVAL;

  return 0;
}

int gdp_token_totime(const gdp_token *tok, graph_timestamp_t *ts) {
  const char *s = tok->tkn_start;
  const char *e = tok->tkn_end;

  switch (tok->tkn_kind) {
    case TOK_ATOM:
    case TOK_STR:
      break;
    default:
      return EINVAL;
  }

  if (graph_timestamp_from_string(ts, s, e)) return EINVAL;

  return 0;
}

int gdp_token_tobool(const gdp_token *tok, bool *val) {
  int err = 0;

  switch (tok->tkn_kind) {
    case TOK_ATOM:
    case TOK_STR:
      if (gdp_token_matches(tok, "true"))
        *val = true;
      else if (gdp_token_matches(tok, "false"))
        *val = false;
      else
        err = EINVAL;
      break;
    default:
      err = EINVAL;
  }

  return err;
}

void gdp_token_image(const gdp_token *tok, char *buf, size_t size) {
  const char *s = tok->tkn_start;
  const char *e = tok->tkn_end;
  char *p;

  if (s && e) {
    for (p = buf; (size > 1) && (s < e); s++) {
      const char ch = *s;
      if (isprint(ch)) {
        *p = ch;
        size--;
        p++;
      } else {
        int n = snprintf(p, size, "\\%03hho", ch);
        if ((n > 0) && ((size_t)n < size)) {
          size -= n;
          p += n;
        } else
          size = 0;
      }
    }
    *p = '\0';
  } else if (tok->tkn_kind == TOK_NULL)
    snprintf(buf, size, "null");
  else if (tok->tkn_kind == TOK_END)
    snprintf(buf, size, "<EOF>");
  else
    snprintf(buf, size, "<UNK>");
}

void gdp_token_printf(FILE *f, const char *fmt, const gdp_token *tok) {
  const char *p;
  const char *name;
  char image[128];

  cl_assert(NULL, tok->tkn_kind <
                      (sizeof(gdp_token_names) / sizeof(gdp_token_names[0])));

  name = gdp_token_names[tok->tkn_kind];
  gdp_token_image(tok, image, sizeof(image));

  p = fmt;
  while (1) {
    const char *q;
    if ((q = strchr(p, '$'))) {
      fwrite(p, q - p, 1, f);
      switch (q[1]) {
        case 0:
          fputc('$', f);
          return;
        case 'n':
          fputs(name, f);
          break;
        case 'i':
          fputs(image, f);
          break;
        default:
          fwrite(q, 2, 1, f);
      }
      p = q + 2;
    } else {
      fputs(p, f);
      return;
    }
  }
}
