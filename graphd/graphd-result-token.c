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

void graphd_result_token_initialize(graphd_result_token* rtok) {
  memset(rtok, 0, sizeof(*rtok));
}

void graphd_result_token_finish(graphd_request* greq,
                                graphd_result_token* rtok) {
  graphd_result_token *s, *e;
  switch (rtok->rtok_type) {
    case GRAPHD_RESULT_TOKEN_LIST:
    case GRAPHD_RESULT_TOKEN_SEQUENCE:

      /* Free array elements. */
      if (rtok->rtok_array_m > 0) {
        s = rtok->rtok_array_token;
        e = s + rtok->rtok_array_n;
        while (s < e) graphd_result_token_finish(greq, s++);
        cm_free(greq->greq_req.req_cm, rtok->rtok_array_token);
      }
      break;

    case GRAPHD_RESULT_TOKEN_STRING:
    case GRAPHD_RESULT_TOKEN_ATOM:
      /* There may be a primitive underlying the text.  Free it. */
      pdb_primitive_reference_free(&rtok->rtok_text_ref);
      break;

    default:
      break;
  }
}

int graphd_result_token_alloc_list(graphd_session* gses, graphd_request* greq,
                                   graphd_result_token* rtok, size_t n) {
  rtok->rtok_type = GRAPHD_RESULT_TOKEN_LIST;
  if ((rtok->rtok_array_n = n) == 0) {
    rtok->rtok_array_m = 0;
    rtok->rtok_array_token = NULL;
  } else {
    rtok->rtok_array_token =
        cm_talloc(greq->greq_req.req_cm, graphd_result_token, n);
    if (rtok->rtok_array_token == NULL) {
      cl_log(gses->gses_cl, CL_LEVEL_ERROR,
             "failed to allocate %zu slots in "
             "result token array",
             n);
      return ENOMEM;
    }
    memset(rtok->rtok_array_token, 0, sizeof(graphd_result_token) * n);
    rtok->rtok_array_m = n;
  }
  return 0;
}

void graphd_result_token_set_constant(graphd_result_token* rtok,
                                      char const* lit, size_t n) {
  rtok->rtok_type = GRAPHD_RESULT_TOKEN_ATOM;
  rtok->rtok_text_s = lit;
  rtok->rtok_text_e = lit + n;
  pdb_primitive_reference_initialize(&rtok->rtok_text_ref);
}

void graphd_result_token_set_text(graphd_result_token* rtok, int type,
                                  char const* s, char const* e,
                                  pdb_primitive const* pr) {
  rtok->rtok_type = type;
  rtok->rtok_text_s = s;
  rtok->rtok_text_e = e;

  pdb_primitive_reference_from_primitive(&rtok->rtok_text_ref, pr);
}

int graphd_result_token_alloc_text(graphd_request* greq,
                                   graphd_result_token* rtok, int type,
                                   char const* s, char const* e) {
  char* str_dup;

  str_dup = cm_substr(greq->greq_req.req_cm, s, e);
  if (str_dup == NULL) return ENOMEM;

  rtok->rtok_type = type;
  rtok->rtok_text_s = str_dup;
  rtok->rtok_text_e = str_dup + (e - s);

  return 0;
}

void graphd_result_token_set_number(graphd_result_token* rtok,
                                    unsigned long long num) {
  rtok->rtok_type = GRAPHD_RESULT_TOKEN_NUMBER;
  rtok->rtok_data.data_number = num;
}

void graphd_result_token_set_timestamp(graphd_result_token* rtok,
                                       graph_timestamp_t ts) {
  rtok->rtok_type = GRAPHD_RESULT_TOKEN_TIMESTAMP;
  rtok->rtok_data.data_timestamp = ts;
}

void graphd_result_token_clear(graphd_result_token* rtok) {
  rtok->rtok_type = GRAPHD_RESULT_TOKEN_UNSPECIFIED;
}

void graphd_result_token_set_guid(graphd_result_token* rtok,
                                  graph_guid const* guid) {
  rtok->rtok_type = GRAPHD_RESULT_TOKEN_GUID;
  rtok->rtok_data.data_guid = *guid;
}

void graphd_result_token_set_sequence(graphd_request* greq,
                                      graphd_result_token* rtar) {
  rtar->rtok_type = GRAPHD_RESULT_TOKEN_SEQUENCE;
  rtar->rtok_data.data_array.array_n = 0;
  rtar->rtok_data.data_array.array_m = 0;
  rtar->rtok_data.data_array.array_token = NULL;
}

int graphd_result_token_array_grow(cl_handle* cl, graphd_request* greq,
                                   graphd_result_token* rtar, size_t n) {
  cl_assert(cl, rtar != NULL);
  cl_assert(cl, greq != NULL);
  cl_assert(cl, (long long)n > 0);
  cl_assert(cl, rtar->rtok_array_n <= rtar->rtok_array_m);

  if ((rtar->rtok_array_n + n) > rtar->rtok_array_m) {
    graphd_result_token* tmp;

    tmp = cm_trealloc(greq->greq_req.req_cm, graphd_result_token,
                      rtar->rtok_data.data_array.array_token,
                      rtar->rtok_data.data_array.array_m + n);
    if (tmp == NULL) return ENOMEM;

    rtar->rtok_array_m += n;
    rtar->rtok_array_token = tmp;
  }

  cl_assert(cl, rtar != NULL);
  cl_assert(cl, (long long)n > 0);
  cl_assert(cl, rtar->rtok_array_n <= rtar->rtok_array_m);
  cl_assert(cl, rtar->rtok_array_n + n <= rtar->rtok_array_m);

  return 0;
}

int graphd_result_token_array_add(cl_handle* cl, graphd_request* greq,
                                  graphd_result_token* rtar,
                                  graphd_result_token const* rtok) {
  int err;

  if (rtar->rtok_array_n >= rtar->rtok_array_m) {
    err = graphd_result_token_array_grow(cl, greq, rtar, 64);
    if (err) return err;
  }

  cl_assert(cl, rtar != NULL);
  cl_assert(cl, rtar->rtok_array_n + 1 <= rtar->rtok_array_m);

  rtar->rtok_array_token[rtar->rtok_array_n++] = *rtok;

  cl_assert(cl, rtar != NULL);
  cl_assert(cl, rtar->rtok_array_n <= rtar->rtok_array_m);

  return 0;
}

graphd_result_token* graphd_result_token_array_alloc(cl_handle* cl,
                                                     graphd_request* greq,
                                                     graphd_result_token* rtar,
                                                     size_t n) {
  cl_assert(cl, rtar != NULL);
  cl_assert(cl, (long long)n > 0);

  if (rtar->rtok_array_n + n >= rtar->rtok_array_m &&
      graphd_result_token_array_grow(cl, greq, rtar, n))
    return NULL;

  cl_assert(cl, rtar->rtok_array_n + n <= rtar->rtok_array_m);
  return rtar->rtok_array_token + rtar->rtok_array_n;
}

void graphd_result_token_array_alloc_commit(cl_handle* cl, graphd_request* greq,
                                            graphd_result_token* rtar,
                                            size_t n) {
  cl_assert(cl, rtar != NULL);
  cl_assert(cl, (long long)n > 0);
  cl_assert(cl, rtar->rtok_array_n + n <= rtar->rtok_array_m);

  rtar->rtok_array_n += n;
}

int graphd_result_token_array_append(cl_handle* cl, graphd_request* greq,
                                     graphd_result_token* dst,
                                     graphd_result_token const* src) {
  int err = 0;

  if (src == NULL || src->rtok_type == GRAPHD_RESULT_TOKEN_UNSPECIFIED)
    return 0;

  if (src->rtok_type != GRAPHD_RESULT_TOKEN_SEQUENCE)
    err = graphd_result_token_array_add(cl, greq, dst, src);
  else {
    graphd_result_token* t_dst;
    graphd_result_token const* t_src;
    size_t n;

    n = src->rtok_data.data_array.array_n;
    if (n == 0) return 0;

    t_dst = graphd_result_token_array_alloc(cl, greq, dst, n);
    if (t_dst == NULL) {
      /* XXX error */
      return ENOMEM;
    }
    t_src = src->rtok_data.data_array.array_token;
    memcpy(t_dst, t_src, sizeof(*t_src) * n);
    graphd_result_token_array_alloc_commit(cl, greq, dst, n);
  }

  return err;
}

void graphd_result_token_array_truncate(graphd_request* greq,
                                        graphd_result_token* rtar, size_t len) {
  if (rtar->rtok_array_n >= len) {
    graphd_result_token* el = rtar->rtok_array_token + len;
    graphd_result_token* end = rtar->rtok_array_token + rtar->rtok_array_n;

    for (; el < end; el++) graphd_result_token_finish(greq, el);
    rtar->rtok_array_n = len;
  }
}

/*  Move el to rtar[i].  Free the previous rtar[i].
 *  The old *el is destroyed in the process.
 */
int graphd_result_token_array_set(cl_handle* cl, graphd_request* greq,
                                  graphd_result_token* rtar, size_t i,
                                  graphd_result_token* el) {
  if (i >= rtar->rtok_array_n) {
    graphd_result_token* rtok;
    rtok = graphd_result_token_array_alloc(cl, greq, rtar,
                                           (i + 1) - rtar->rtok_array_n);
    if (rtok == NULL) return ENOMEM;
    graphd_result_token_array_alloc_commit(cl, greq, rtar,
                                           (i + 1) - rtar->rtok_array_n);
  }

  graphd_result_token_finish(greq, rtar->rtok_array_token + i);
  rtar->rtok_array_token[i] = *el;
  graphd_result_token_initialize(el);

  return 0;
}

char const* graphd_result_token_to_string(graphd_result_token* t, char* buf,
                                          size_t size) {
  size_t n;
  char const* el;
  char elbuf[80];
  char const* q;

  if (t == NULL) return "null";
  switch (t->rtok_type) {
    case GRAPHD_RESULT_TOKEN_UNSPECIFIED:
      return "unspecified";

    case GRAPHD_RESULT_TOKEN_ATOM:
    case GRAPHD_RESULT_TOKEN_STRING:
      n = t->rtok_data.data_text.text_e - t->rtok_data.data_text.text_s;
      if (n > 60)
        n = 80, el = "...";
      else
        el = "";

      q = (t->rtok_type == GRAPHD_RESULT_TOKEN_STRING) ? "\"" : "'";
      snprintf(buf, size, "%s%.*s%s%s[%zu]", q, (int)n,
               t->rtok_data.data_text.text_s, el, q,
               (size_t)(t->rtok_data.data_text.text_e -
                        t->rtok_data.data_text.text_s));
      return buf;

    case GRAPHD_RESULT_TOKEN_NUMBER:
      snprintf(buf, size, "#%llu",
               (unsigned long long)t->rtok_data.data_number);
      return buf;

    case GRAPHD_RESULT_TOKEN_TIMESTAMP:
      snprintf(buf, size, "%s",
               graph_timestamp_to_string(t->rtok_data.data_timestamp, elbuf,
                                         sizeof(elbuf)));
      return buf;

    case GRAPHD_RESULT_TOKEN_GUID:
      snprintf(buf, size, "%lu-%lu-%lu",
               (unsigned long)GRAPH_GUID_APPLICATION_ID(t->rtok_data.data_guid),
               (unsigned long)GRAPH_GUID_DB(t->rtok_data.data_guid),
               (unsigned long)GRAPH_GUID_SERIAL(t->rtok_data.data_guid));
      return buf;

    case GRAPHD_RESULT_TOKEN_LIST:
    case GRAPHD_RESULT_TOKEN_SEQUENCE:
      q = t->rtok_type == GRAPHD_RESULT_TOKEN_LIST ? "()" : "{}";
      snprintf(
          buf, size, "%c%lu%s%s%s%c", q[0],
          (unsigned long)t->rtok_data.data_array.array_n,
          t->rtok_data.data_array.array_n > 0 ? " " : "",
          t->rtok_data.data_array.array_n > 0
              ? graphd_result_token_to_string(
                    t->rtok_data.data_array.array_token, elbuf, sizeof elbuf)
              : "",
          t->rtok_data.data_array.array_n > 1 ? "..." : "", q[1]);
      return buf;

    case GRAPHD_RESULT_TOKEN_NULL:
      return "<null>";

    default:
      break;
  }

  snprintf(buf, size, "<unexpected reply token type %d>", (int)t->rtok_type);
  return buf;
}
