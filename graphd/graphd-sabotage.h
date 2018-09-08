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
#ifndef GRAPHD_RESUME_SABOTAGE_H
#define GRAPHD_RESUME_SABOTAGE_H

#include "libsrv/srv.h"

#ifndef SABOTAGE
#define SABOTAGE 1 /* Enable sabotage testing. */
#endif             /* SABOTAGE */

/*  Report acts of sabotage at this loglevel.
 */
#define GRAPHD_SABOTAGE_LOGLEVEL CL_LEVEL_ERROR

/*
 *  The array tracks whether or not a suspend statement has been
 *  executed.  Indexed with __LINE__. (This assumes we never have
 *  more than 10,000 lines in a resumable translation unit, but
 *  will abort with an assertion failure if we do.)
 */
#define GRAPHD_SABOTAGE_DECL static unsigned char graphd_sabotage_buffer[10000]

#define GRAPHD_SABOTAGE(g, cond)                                           \
  ((cond) ||                                                               \
   ((g)->g_sabotage && (g)->g_sabotage->gs_countdown > 0 &&                \
    sizeof(char[sizeof graphd_sabotage_buffer - __LINE__]) &&              \
    graphd_sabotage_buffer[__LINE__] < (g)->g_sabotage->gs_target &&       \
    --((g)->g_sabotage->gs_countdown) == 0 &&                              \
    ++(graphd_sabotage_buffer[__LINE__]) &&                                \
    (graphd_sabotage_report((g)->g_sabotage, __FILE__, __LINE__, __func__, \
                            #cond, graphd_sabotage_buffer[__LINE__]),      \
     0)))

struct graphd_handle;

typedef struct graphd_sabotage_config {
  /*  What was our initial level?
   */
  unsigned long gsc_countdown_initial;

  /*  Should we restart counting after triggering?
   */
  unsigned int gsc_cycle : 1;

  /*  Should we increment the initial countdown timer
   *	after triggering?
   */
  unsigned int gsc_increment : 1;

  /*  Mess with the stack area
   *  between each call to graphd_serve()?
   */
  unsigned int gsc_deadbeef : 1;

  /*  What loglevel are we logging at?
   */
  cl_loglevel gsc_loglevel;

  /*  How many times should each location fire?
   */
  unsigned char gsc_target;

} graphd_sabotage_config;

typedef struct graphd_sabotage_handle {
  /*  Log through this to report that
   *  we're breaking something deliberately.
   */
  cl_handle* gs_cl;

  /*  What level are we logging at?
   */
  cl_loglevel gs_loglevel;

  /*  Count this down to 0 before striking.
   *  An initial value of 0 is safe and never triggers.
   */
  unsigned long gs_countdown;

  /*  What was our initial level?
   */
  unsigned long gs_countdown_initial;

  /*  What's our total age in ticks?
   */
  unsigned long gs_countdown_total;

  /*  Should we restart counting after triggering?
   */
  unsigned int gs_cycle : 1;

  /*  Should we increment the initial countdown timer
   *	after triggering?
   */
  unsigned int gs_increment : 1;

  /*  Mess with the stack area
   *  between each call to graphd_serve()?
   */
  unsigned int gs_deadbeef : 1;

  /*  How many times should each location fire?
   */
  unsigned char gs_target;

} graphd_sabotage_handle;

void graphd_sabotage_report(graphd_sabotage_handle* gs, char const* file,
                            int line, char const* func, char const* cond,
                            int local_count);

int graphd_sabotage_option_set(void* data, srv_handle* srv, cm_handle* cm,
                               int opt, char const* opt_arg);

int graphd_sabotage_option_configure(void* data, srv_handle* srv,
                                     void* config_data,
                                     srv_config* srv_config_data);

void graphd_sabotage_initialize(graphd_sabotage_handle* gs, cl_handle* cl);

#endif /* GRAPHD_RESUME_SABOTAGE_H */
