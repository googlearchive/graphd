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
 * @brief Install a function that writes text output.
 *
 * The callback function is invoked with a preformatted,
 * '\\0'-terminated string to write log output.
 *
 * Only one such function is stored in the library - if a second
 * callback is installed, the previous callback is overwritten.
 *
 * Setting the callback pointer to NULL reverts to the default
 * (an internal, stderr-wrting function).
 *
 * @param cl the log module handle
 * @param callback function to call
 * @param data first argument passed to the function (followed by
 *	loglevel and the string data.)
 */
void cl_set_write(cl_handle* cl, cl_write_callback* callback, void* data) {
  if (callback != NULL) {
    cl->cl_write = callback;
    cl->cl_write_data = data;
  } else {
    cl->cl_write = cl_write_stderr;
    cl->cl_write_data = cl;
  }
}
