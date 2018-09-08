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

/* Attach a siphon write function to a cl handle.
 *
 *	The siphon write will be called whenever
 *	a message matching the passed level is
 *	logged.
 *
 *	This mechanism is used to forward ERRORs
 *	to the netlog.
 */
void cl_set_siphon(cl_handle* cl, cl_write_callback* callback, void* data,
                   cl_loglevel level) {
  cl->cl_siphon = callback;
  cl->cl_siphon_data = data;
  cl->cl_siphon_level = level;
}
