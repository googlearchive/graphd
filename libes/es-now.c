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
 * @brief What's the current time?
 *
 * If @b es is null, the call simply returns the current time,
 * as seen by the C runtime library call time().
 * Otherwise, it returns libes's internal idea of what
 * the current time is.
 *
 * @param es NULL or an opaque module handle, as created by es_create()
 * @return the current time, as seen by the system.
 */
time_t es_now(es_handle *es) {
  if (!es) return time((time_t *)0);

  cl_cover(es->es_cl);
  return es->es_now;
}
