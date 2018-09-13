/*
Copyright 2018 Google Inc. All rights reserved.
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

#include <ctype.h>
#include <limits.h>
#include "gld.h"

/* results: "e" for error, "o" for ok, "(", ")" for themselves, "#" for GUID,
 *  	0 if we're done.
 */
static int scan_reply_token(char const **s, char const *e,
                            graph_guid *guid_out) {
  char const *r = *s, *p;

  /* Skip leading white space or commas.
   */
  while (r < e && isascii(*r) && (isspace(*r) || *r == ',')) r++;

  if (r >= e) return 0;
  if (*r == '(' || *r == ')') {
    *s = r + 1;
    return *r;
  }

  if (r + 1 < e && *r == 'o') /* ok */
  {
    *s = r + 2;
    return 'o';
  }

  if (e - r >= 5 && *r == 'e') /* error */
  {
    *s = r + 5;
    return 'e';
  }

  for (p = r; p < e && isascii(*p) && isalnum(*p); p++)
    ;
  if (graph_guid_from_string(guid_out, r, p) != 0) {
    fprintf(stderr, "Unexpected result text %.*s\n", (int)(e - r), r);
    exit(1);
  }
  *s = p;
  return '#';
}

/*  The caller has consumed the leading ( of a list.  Read the rest
 *  of the list elements up to, including, the closing ).  Advance *s.
 */
static void scan_reply_list(gld_handle *gld, gld_primitive *pr, char const **s,
                            char const *e) {
  int tok;
  graph_guid guid;
  char const *s0 = *s;

  while ((tok = scan_reply_token(s, e, &guid)) != ')') {
    gld_primitive *new_pr;

    if ((new_pr = gld_primitive_alloc(gld)) == NULL) {
      fprintf(stderr,
              "out of memory while allocating "
              "result primitive: %s\n",
              strerror(errno));
      exit(1);
    }

    gld_primitive_append(gld, pr, new_pr);

    if (tok == '(')
      scan_reply_list(gld, new_pr, s, e);
    else if (tok == '#')
      gld_primitive_set_guid(gld, new_pr, &guid);
    else {
      fprintf(stderr, "expected list elements, got %.*s\n", (int)(e - s0), s0);
      exit(1);
    }
  }
}

/* results: "e" for error, "o" for ok, "(", ")" for themselves, "#" for GUID,
 *  	0 if we're done.
 */
static int scan_reply(gld_handle *gld, gld_primitive *pr, char const *s,
                      size_t n, bool empty_is_ok) {
  int tok;
  graph_guid guid;
  char const *s0 = s;
  char const *e = s + n;

  tok = scan_reply_token(&s, e, &guid);
  if (tok == 'e') {
    if (!empty_is_ok || strncasecmp(s, "EMPTY", 5) != 0)
      fprintf(stderr, "gld: error:%.*s (empty: %d)\n", (int)(e - s), s,
              empty_is_ok);
    return 0;
  } else if (tok != 'o') {
    fprintf(stderr,
            "scan_reply: unexpected reply token '%c' "
            "(want o(k) or e(rror), got %.*s!\n",
            tok, (int)(e - s0), s0);
    exit(1);
  }

  s0 = s;
  tok = scan_reply_token(&s, e, &guid);
  if (tok != '(') {
    fprintf(stderr, "scan_reply: expected \"(\", got %.*s\n", (int)(e - s0),
            s0);
    exit(1);
  }
  scan_reply_list(gld, pr, &s, e);
  return 0;
}

/*  Read request results until we see the result we're waiting for.
 */
static int gld_request_is_outstanding(gld_handle *gld, char const *name_s,
                                      char const *name_e) {
  return gld->gld_request != NULL &&
         cm_haccess(gld->gld_request, gld_request_data, name_s,
                    name_e - name_s) != NULL;
}

/*  Read request results until we see the result we're waiting for.
 */
void gld_request_wait_any(gld_handle *gld) {
  int err;
  graphdb_request_id request_id;
  void *my_app_data;
  size_t my_text_size;
  char const *my_text;

  request_id = GRAPHDB_REQUEST_ANY;
  err = graphdb_request_wait(gld->gld_graphdb, &request_id, -1, /* timeout */
                             &my_app_data, &my_text, &my_text_size);
  if (err != 0) {
    fprintf(stderr, "gld: graphdb_request_wait: %s\n", strerror(err));
    exit(1);
  }

  cl_assert(gld->gld_cl, gld->gld_outstanding > 0);
  gld->gld_outstanding--;

  if (my_app_data == NULL) {
    if (gld->gld_print_answers)
      fwrite(my_text, my_text_size, 1, stdout);

    else if (my_text_size < 2 || *my_text != 'o')
      fwrite(my_text, my_text_size, 1, stderr);
  } else {
    gld_primitive *pr;
    gld_request_data *d;
    char const *var_name;
    size_t var_name_size;

    d = (gld_request_data *)my_app_data;

    if (d->d_most_recent_id != request_id) {
      /* This isn't the reply we are
       * waiting for - it's a previous
       * instance that never actually
       * was used (or waited for).
       *
       * We can just ignore it.
       */
      cl_log(gld->gld_cl, CL_LEVEL_VERBOSE,
             "ignore reply %llu (still "
             "waiting for %llu)",
             (unsigned long long)request_id,
             (unsigned long long)d->d_most_recent_id);
    } else {
      /*  Scan the resulting text into a variable.
       */
      var_name = cm_hmem(gld->gld_request, gld_request_data, my_app_data);
      var_name_size = cm_hsize(gld->gld_request, gld_request_data, my_app_data);

      pr = gld_var_create(gld, var_name, var_name + var_name_size);
      if (pr == NULL) {
        fprintf(stderr, "failed to create variable %.*s\n", (int)var_name_size,
                var_name);
        exit(1);
      }

      scan_reply(gld, pr, my_text, my_text_size, d->d_can_be_empty);

      cl_assert(gld->gld_cl, gld->gld_request != NULL);

      /* This is no longer outstanding. */
      cm_hdelete(gld->gld_request, gld_request_data, my_app_data);
    }
  }

  graphdb_request_free(gld->gld_graphdb, request_id);
}

/*  Read request results until we see the result we're waiting for.
 */
void gld_request_wait(gld_handle *gld, char const *name_s, char const *name_e) {
  int err;
  graphdb_request_id request_id;
  void *my_app_data;
  size_t my_text_size;
  char const *my_text;

  while (gld_request_is_outstanding(gld, name_s, name_e)) {
    request_id = GRAPHDB_REQUEST_ANY;
    err = graphdb_request_wait(gld->gld_graphdb, &request_id, -1, &my_app_data,
                               &my_text, &my_text_size);
    if (err) {
      fprintf(stderr,
              "gld: graphdb_request_wait fails: "
              "%s\n",
              strerror(err));
      exit(1);
    }
    if (my_app_data == NULL) {
      if (my_text_size < 2 || *my_text != 'o')
        fprintf(stderr, "gld: %.*s", (int)my_text_size, my_text);
      gld->gld_outstanding--;
    } else {
      gld_request_data *d = my_app_data;
      gld_primitive *pr;

      char const *var_name;
      size_t var_name_size;

      cl_log(gld->gld_cl, CL_LEVEL_VERBOSE,
             "gld_request_wait: got back \"%llu\" (%.*s)",
             (unsigned long long)request_id,
             (int)cm_hsize(gld->gld_request, gld_request_data, d),
             (char const *)cm_hmem(gld->gld_request, gld_request_data, d));

      gld->gld_outstanding--;

      if (d->d_most_recent_id != request_id) {
        cl_log(gld->gld_cl, CL_LEVEL_VERBOSE,
               "No longer waiting for %llu, waiting for %llu",
               (unsigned long long)request_id,
               (unsigned long long)d->d_most_recent_id);
      } else if (my_text_size < 2 || *my_text != 'o') {
        if (d->d_can_be_empty && strncasecmp(my_text, "error EMPTY", 11) == 0) {
          var_name = cm_hmem(gld->gld_request, gld_request_data, d);
          var_name_size = cm_hsize(gld->gld_request, gld_request_data, d);
          pr = gld_var_create(gld, var_name, var_name + var_name_size);
          if (pr == NULL) {
            fprintf(stderr,
                    "failed to create "
                    "variable %.*s\n",
                    (int)var_name_size, var_name);
            exit(1);
          }

          cl_log(gld->gld_cl, CL_LEVEL_VERBOSE,
                 "gld_request_wait: delete \"%llu\"",
                 (unsigned long long)d->d_most_recent_id);

          /* This is no longer outstanding. */
          cm_hdelete(gld->gld_request, gld_request_data, d);
        } else {
          fprintf(stderr, "(failed request) %.*s", (int)my_text_size, my_text);
          exit(0);
        }
      } else {
        /* Request is checked in under variable name. */

        var_name = cm_hmem(gld->gld_request, gld_request_data, my_app_data);
        var_name_size =
            cm_hsize(gld->gld_request, gld_request_data, my_app_data);

        pr = gld_var_create(gld, var_name, var_name + var_name_size);
        if (pr == NULL) {
          fprintf(stderr,
                  "failed to create "
                  "variable %.*s\n",
                  (int)var_name_size, var_name);
          exit(1);
        }

        scan_reply(gld, pr, my_text, my_text_size, d->d_can_be_empty);

        /* This variable is no longer outstanding. */
        cm_hdelete(gld->gld_request, gld_request_data, my_app_data);
      }
    }
    graphdb_request_free(gld->gld_graphdb, request_id);
  }
}

/*  Send out a request.  If it's being assigned to a variable,
 *  store the fact that it's been sent.
 */
int gld_request_send(gld_handle *gld, gld_request_data *d,
                     char const *request_s, char const *request_e) {
  int err;
  graphdb_request_id id;

  err = graphdb_request_send(gld->gld_graphdb, &id, d, request_s,
                             request_e - request_s);
  if (err) {
    if (d != NULL) cm_hdelete(gld->gld_request, gld_request_data, d);
  } else {
    if (d != NULL) {
      cl_log(gld->gld_cl, CL_LEVEL_VERBOSE,
             "gld_request_send: %llu for \"%.*s\"", (unsigned long long)id,
             (int)cm_hsize(gld->gld_request, gld_request_data, d),
             (char const *)cm_hmem(gld->gld_request, gld_request_data, d));

      d->d_most_recent_id = id;
      d->d_sent = true;
    }

    gld->gld_outstanding++;
  }
  return err;
}

/*  Send out a request.  If it's being assigned to a variable,
 *  store the fact that it's been sent.
 */
gld_request_data *gld_request_alloc(gld_handle *gld, char const *name_s,
                                    char const *name_e) {
  if (gld->gld_request == NULL) {
    gld->gld_request = cm_hcreate(gld->gld_cm, gld_request_data, 256);
    if (gld->gld_request == NULL) return NULL;
  }

  cl_log(gld->gld_cl, CL_LEVEL_VERBOSE, "allocate request \"%.*s\"",
         (int)(name_e - name_s), name_s);

  return cm_hnew(gld->gld_request, gld_request_data, name_s, name_e - name_s);
}

size_t gld_request_outstanding(gld_handle *gld) { return gld->gld_outstanding; }
