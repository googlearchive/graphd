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
 * @brief Call the currently installed hard error handler.
 *
 *   A hard error may or may not be fatal.  What happens in response
 *   is centrally configured.
 *
 * @param cl	The log handle through which to log
 */
void cl_hard_error(cl_handle* cl) {
  if (cl->cl_hard_error != NULL)
    (*cl->cl_hard_error)(cl->cl_hard_error_data);
  else
    cl_notreached(cl,
                  "cl_hard_error: unexpected hard error -- no handler "
                  "installed.  [aborting]");
}
