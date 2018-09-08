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
 * @brief Install a function that is called in case of a hard error.
 *
 *  Hard errors are errors that don't happen in the course of normal
 *  operation, but may not have been directly caused by programmer
 *  errors.  For example, errors due to misconfigured databases,
 *  debris from previous database instances lying around, write errors,
 *  etc.
 *
 * @param cl 		the log module handle
 * @param callback 	call this function in case of a hard error.
 * @param data		argument passed to the function.
 */
void cl_set_hard_error(cl_handle* cl, cl_hard_error_callback* callback,
                       void* data) {
  if (cl != NULL) {
    cl->cl_hard_error = callback;
    cl->cl_hard_error_data = data;
  }
}
