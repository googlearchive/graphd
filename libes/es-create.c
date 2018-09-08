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

#include <string.h>

/**
 * @brief Create an opaque module handle.
 *
 * @param cm an allocation manager, e.g. cm_c()
 * @param cl a log manager, allocated with cl_create()
 * @return NULL on allocation error, otherwise a module handle.
 *	The handle must be destroyed with es_destroy().
 */
es_handle *es_create(cm_handle *cm, cl_handle *cl) {
  es_handle *es;

  if (!(es = cm_talloc(cm, es_handle, 1))) return NULL;

  memset(es, 0, sizeof(*es));
  es->es_cm = cm;
  es->es_cl = cl;
  es->es_poll_free = -1;
  es->es_poll = NULL;
  es->es_desc = NULL;
  time(&es->es_now);
  es->es_timeout_head = NULL;

  es->es_idle_callback_head = NULL;
  es->es_idle_callback_tail = &es->es_idle_callback_head;

  cl_cover(cl);

  return es;
}
