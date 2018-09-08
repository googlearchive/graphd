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
 * @file srv-idle.c -- schedule an idle callback.
 */

/*  Translate an ES idle callback into a SRV idle callback.
 *
 *  For SRV, we just want *one* callback - either we're idle or not
 *  - and we want to prevent multiple callbacks from pending at the
 *  same time, and remember whether we've set a callback that hasn't
 *  triggered yet.
 */
static void srv_idle_callback(void* data, es_idle_callback_timed_out mode) {
  srv_idle_context* const con = data;

  con->idle_es = NULL;
  if (con->idle_callback != NULL) (*con->idle_callback)(con, mode);
}

/**
 * @brief Set a single "idle" callback.
 *
 * @param srv		opaque server handle
 * @param con 		the callback we'd like to set
 * @param seconds	how many seconds should we wait before
 *			executing this anyway (even if not idle)?
 *
 * @return SRV_ERR_ALREADY if there's already an idle callback installed
 * @return ENOMEM 	if we're out of memory
 * @return 0 		if the callback was installed successfully.
 */
int srv_idle_set(srv_handle* srv, srv_idle_context* con,
                 unsigned long seconds) {
  if (con->idle_es != NULL) return SRV_ERR_ALREADY;

  con->idle_es =
      es_idle_callback_create(srv->srv_es, seconds, srv_idle_callback, con);
  if (con->idle_es == NULL) return ENOMEM;

  return 0;
}

/**
 * @brief 	Is there an "idle" callback installed in the server?
 * @param srv	opaque server handle
 * @return true if there's already an idle callback installed, false if not.
 */
bool srv_idle_test(srv_handle* srv, srv_idle_context const* ic) {
  return ic->idle_es != NULL;
}

/**
 * @brief Remove the idle callback from the server's polling module
 * @param srv	opaque server handle
 * @param callback_out	out: callback that was installed
 * @param callback_data_out	out: its closure
 *
 * @return 0 		on success
 * @return SRV_ERR_NO 	if there's no idle callback installed.
 */
int srv_idle_delete(srv_handle* srv, srv_idle_context* con) {
  es_idle_callback* ecb;

  if ((ecb = con->idle_es) == NULL) return SRV_ERR_NO;
  con->idle_es = NULL;

  es_idle_callback_cancel(srv->srv_es, ecb);

  return 0;
}

/**
 * @brief Remove the idle callback from the server's polling module
 * @param srv	opaque server handle
 * @param callback_out	out: callback that was installed
 * @param callback_data_out	out: its closure
 *
 * @return 0 		on success
 * @return SRV_ERR_NO 	if there's no idle callback installed.
 */
void srv_idle_initialize(srv_handle* srv, srv_idle_context* con,
                         srv_idle_callback_func* callback) {
  memset(con, 0, sizeof(*con));

  con->idle_callback = callback;
  con->idle_es = NULL;
}
