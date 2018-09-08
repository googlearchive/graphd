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
#include "libes/esp.h"

/*  Add <events> to the events that a session is waiting for.
 */
void es_subscribe_loc(es_handle *es, es_descriptor *ed, unsigned int events,
                      char const *file, int line) {
  if (es == NULL) return;

  cl_assert(es->es_cl, ed != NULL);
  cl_assert(es->es_cl, !(events & ~(ES_INPUT | ES_OUTPUT)));
  cl_assert(es->es_cl, ed->ed_poll >= 0);
  cl_assert(es->es_cl, ed->ed_poll < es->es_poll_n);

  if ((es->es_poll[ed->ed_poll].events & events) != events)
    cl_log(es->es_cl, CL_LEVEL_DEBUG, "%p: subscribe +%s [%s:%d]", (void *)ed,
           events == (ES_INPUT | ES_OUTPUT)
               ? "input+output"
               : (events == ES_INPUT ? "input" : "output"),
           file, line);

  es->es_poll[ed->ed_poll].events |= events;

  cl_cover(es->es_cl);
}
