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
#include "libcm/cm.h"
#include "srvp.h"
#include <errno.h>

/**
 * @brief Call a callback for all memory fragments in the server
 *
 *  See cm_trace_list and cm_trace_set_log_callback for details
 *  about the callback format.
 *
 * @param srv	server module handle
 * @param callback	application callback to call
 * @param data		opaque first parameter for callback
 *
 * @return 0 on success.
 * @return ENOTSUP if the application isn't running with
 * 	memory tracing enabled (runtime -t option).
 */
int srv_memory_list(srv_handle* srv, cm_log_callback* callback, void* data) {
  cm_log_callback* sav;
  void* sav_data;

  if (!srv->srv_trace) return SRV_ERR_NOT_SUPPORTED;

  cm_trace_get_log_callback(srv->srv_cm, &sav, &sav_data);
  cm_trace_set_log_callback(srv->srv_cm, callback, data);

  cm_trace_list(srv->srv_cm);

  cm_trace_set_log_callback(srv->srv_cm, sav, sav_data);
  return 0;
}
