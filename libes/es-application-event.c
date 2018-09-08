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

/*  Schedule a call with ES_APPLICATION set during the next
 *  round of es_loop().
 */
void es_application_event_loc(es_handle *es, es_descriptor *ed,
                              char const *file, int line) {
  if (es == NULL || ed == NULL) return;

  if (!ed->ed_application_event) {
    cl_log(es->es_cl, CL_LEVEL_DEBUG, "%p: application event [%s:%d]",
           (void *)ed, file, line);
    ed->ed_application_event = true;
    es->es_application_event_n++;
  }
}

/*  If this descriptor had an ES_APPLICATION event pending,
 *  free that event.
 *
 *  (It's a good idea to keep that counter updated to avoid
 *  making unnecessary sweeps through the descriptors.)
 */
void es_application_event_clear(es_handle *es, es_descriptor *ed) {
  if (es != NULL && ed != NULL && ed->ed_application_event) {
    ed->ed_application_event = false;
    cl_assert(es->es_cl, es->es_application_event_n > 0);
    es->es_application_event_n--;
  }
}
