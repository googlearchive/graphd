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
#include "libcl/clp.h"

/**
 * @brief Install a function that is called
 * 	before the library terminates.
 *
 * The callback function is invoked just before the log library
 * terminates as result of a cl_assert() or cl_notreached() call.
 *
 * Only one such function is stored in the library - if a second
 * callback is installed, the previous callback is overwritten.
 *
 * @param cl the log module handle
 * @param callback call this function before the library aborts.
 * @param data first (and only) argument passed to the function.
 */
void cl_set_abort(cl_handle *cl, cl_abort_callback *callback, void *data) {
  if (cl != NULL) {
    cl->cl_abort = callback;
    cl->cl_abort_data = data;
  }
}
