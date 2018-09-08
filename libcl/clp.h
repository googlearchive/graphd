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
#ifndef CLP_H
#define CLP_H

#include "libcl/cl.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>


struct cl_handle {
  /*  These two must be first -- externally visible macros
   *  type-pun cl_handle* to cl_loglevel*.
   */
  cl_loglevel cl_level;
  cl_loglevel cl_diary_trigger;

  /*  netlog interface
   */
  char *cl_netlog_host;      /* malloc'ed */
  char *cl_netlog_ciid;      /* malloc'ed */
  unsigned long cl_netlog_n; /* running number. */

  /*  syslog interface
   */
  int cl_syslog_facility;
  char *cl_syslog_ident;
  unsigned int cl_syslog_open;

  /*  FILE logging interface
   */
  char *cl_file_name_fmt; /* malloc'ed */
  char *cl_file_name;     /* malloc'ed */
  time_t cl_file_minute;
  pid_t cl_file_pid;

  int cl_file_dup_buf[5];
  size_t cl_file_dup_n;

  /*  Redirect into a diary.
   */
  cl_diary_handle *cl_diary;

  void (*cl_write)(void *, cl_loglevel, char const *);
  void *cl_write_data;

  void (*cl_siphon)(void *, cl_loglevel, char const *);
  void *cl_siphon_data;
  cl_loglevel cl_siphon_level;

  char const *(*cl_strerror)(void *, int);
  void *cl_strerror_data;

  void (*cl_abort)(void *);
  void *cl_abort_data;

  void (*cl_hard_error)(void *);
  void *cl_hard_error_data;

  void (*cl_destroy)(void *);
  char *cl_coverage_path;

  size_t cl_indent; /* indentation level */

  /*  Shared between netlog and file logging
   */
  FILE *cl_fp;
  cl_flush_policy cl_flush;

  /*
   * Include stacktrace on errors?
   */
  bool cl_stacktrace;
};

/*  Any write function that optionally works with a diary should include
 *  an invocation of this macro (without trailing semicolon) at the
 *  beginning of its execution path, before printing anything.
 */
#define CL_DIARY_CHECK(cl, lev, str)                                  \
  if ((cl) != NULL && (cl)->cl_diary != NULL) {                       \
    cl_loglevel _sav;                                                 \
    cl_diary_handle *_d = (cl)->cl_diary;                             \
    if (_d != NULL && !CL_IS_LOGGED((cl)->cl_diary_trigger, (lev))) { \
      /* Save it for later. */                                        \
      unsigned char _l4[4];                                           \
      _l4[0] = (unsigned char)(0xFF & (lev >> 24));                   \
      _l4[1] = (unsigned char)(0xFF & (lev >> 16));                   \
      _l4[2] = (unsigned char)(0xFF & (lev >> 8));                    \
      _l4[3] = (unsigned char)(0xFF & lev);                           \
      cl_diary_entry_create(_d, (char const *)_l4, sizeof _l4);       \
      cl_diary_entry_add(_d, (str), strlen(str));                     \
      return;                                                         \
    }                                                                 \
    if (_d != NULL) {                                                 \
      _sav = (cl)->cl_level;                                          \
      (cl)->cl_level |= CL_FACILITY_DIARY;                            \
      (cl)->cl_diary = NULL;                                          \
      cl_diary_relog(_d, (cl));                                       \
      cl_diary_truncate(_d);                                          \
      (cl)->cl_diary = _d;                                            \
      (cl)->cl_level = _sav;                                          \
    }                                                                 \
  }

/* cl-dup2.c */

int cl_dup2_install(cl_handle *cl);

/* cl-netlog.c */

int cl_write_netlog_quoted(FILE *fp, char const *s, char const *e);
int cl_write_netlog_trim(char const **s, char const **e);

/* write callbacks */

void cl_write_file(void *, cl_loglevel, char const *);
void cl_write_netlog(void *, cl_loglevel, char const *);
void cl_write_netlog3(void *, cl_loglevel, char const *);
void cl_write_stderr(void *, cl_loglevel, char const *);
void cl_write_syslog(void *, cl_loglevel, char const *);

/* abort callback */

void cl_abort_c(void *p);

/* log rotation support */

bool cl_timer_check(cl_handle *cl, time_t now);
bool cl_pid_check(cl_handle *cl);

int cl_file_rotate(cl_handle *cl, time_t now);

/* vlog with function helper */

void cl_vlog_func(cl_handle *cl, cl_loglevel level, const char *func,
                  bool entry, char const *fmt, va_list ap);
#endif /* CLP_H */
