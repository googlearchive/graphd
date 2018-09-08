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
#include "graphd/graphd-ast-debug.h"

#include <stdio.h>

/** The name of the file to be used for logging */
#define DEBUG_FILE_NAME "graphd-debug.log"

static char const *get_greq_id(graphd_request const *const greq) {
  graphd_request_parameter const *grp;

  for (grp = greq->greq_parameter_head; grp; grp = grp->grp_next) {
    if (grp->grp_format == graphd_format_request_id) {
      graphd_request_parameter_id const *id =
          (graphd_request_parameter_id const *)grp;
      return id->id_s;
    }
  }

  return "unknown";
}

static FILE *get_debug_file(void) {
  static FILE *f = NULL;   // debug file handle
  static time_t last = 0;  // the last time the handle was used

  if (f == NULL) {
    const unsigned long long pid = getpid();
    char path[128];
    sprintf(path, "%s.%llu", DEBUG_FILE_NAME, pid);
    if ((f = fopen(path, "w")))
      setvbuf(f, NULL, _IOLBF, 0);  // line buffered
    else
      f = stderr;
  }

  /* print current time if more than 1 second has elapsed since the last
   * time we wrote to the file */
  const time_t now = time(NULL);
  if ((now - last) >= 1) {
    char buf[64];
    struct tm tm;
    localtime_r(&now, &tm);
    strftime(buf, sizeof(buf), "%F %T", &tm);
    fprintf(f, "============== Time: %s ==============\n", buf);
    last = now;
  }

  return f;
}

void graphd_ast_debug_reading(graphd_request const *greq) {
  /* current row and column number (in graphd-micro.c) */
  extern unsigned int gdp_micro_row, gdp_micro_col;
  /* the numerical identifier of the request */
  const unsigned long long reqno = greq->greq_req.req_id;

  FILE *f = get_debug_file();

  fprintf(f, "[R%llu] @L%u,C%u\n", reqno, gdp_micro_row, gdp_micro_col);
}

void graphd_ast_debug_received(graphd_request const *greq, bool eof) {
  /* a potentially malformed request? */
  const bool malformed = greq->greq_micro.micro_malformed;
  /* the numerical identifier of the request */
  const unsigned long long reqno = greq->greq_req.req_id;
  /* request size */
  const size_t reqsz = greq->greq_request_size;

  FILE *f = get_debug_file();

  fprintf(f, "[R%llu] Microparsed (%zu B)\n", reqno, reqsz);

  if (malformed) fprintf(f, "[R%llu] Possibly malformed\n", reqno);
  if (eof) fprintf(f, "[R%llu] EOF\n", reqno);
}

void graphd_ast_debug_parsed(graphd_request const *greq, bool has_errors) {
  const unsigned long long reqno = greq->greq_req.req_id;

  FILE *f = get_debug_file();

  if (has_errors)
    fprintf(f, "[R%llu] Has errors\n", reqno);
  else
    fprintf(f, "[R%llu] Ok\n", reqno);

  fprintf(f, "[R%llu] Id: %s\n", reqno, get_greq_id(greq));
}

void graphd_ast_debug_serving(graphd_request const *greq) {
  FILE *f = get_debug_file();

  fprintf(f, "[R%llu] Serving\n", greq->greq_req.req_id);
}

void graphd_ast_debug_finished(graphd_request const *greq) {
  FILE *f = get_debug_file();

  fprintf(f, "[R%llu] Finished\n", greq->greq_req.req_id);
}
