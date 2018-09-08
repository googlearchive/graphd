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
#include <errno.h>
#include <stdbool.h>
#include "srvp.h"

/**
 * @brief Associate a session with a timeout.
 *
 *  If the session's interface doesn't support timeout, the
 *  call is silently ignored.
 *
 * @param ses		session handle
 * @param timeout	a timeout allocated with srv_timeout_create()
 */
void srv_session_set_timeout(srv_session *ses, srv_timeout *timeout) {
  if (ses->ses_interface_type != NULL &&
      ses->ses_interface_type->sit_set_timeout != NULL)
    (*ses->ses_interface_type->sit_set_timeout)(ses->ses_interface_data,
                                                timeout);
}

/**
 * @brief Allocate a timeout.
 *
 *  A timeout can be associated with any number of sessions
 *  using srv_session_set_timeout(), above.
 *
 * @param srv		server handle
 * @param seconds	if there's no activity on the session after this
 *			many seconds, wake it up with a "timeout" event.
 *
 * @return NULL on allocation failure, otherwise a session pointer.
 */
srv_timeout *srv_timeout_create(srv_handle *srv, unsigned long seconds) {
  return (srv_timeout *)es_timeout_create(srv->srv_es, seconds);
}
