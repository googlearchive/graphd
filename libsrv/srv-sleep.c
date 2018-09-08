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

static void srv_sleep_callback(void* data, es_idle_callback_timed_out mode) {
  srv_handle* srv = data;
  srv_session *ses, *ses_next;
  struct timeval tv;
  unsigned long long now = 0;

  if (mode == ES_IDLE_CANCEL || srv->srv_app->app_request_sleep == NULL) return;

  cl_log(srv->srv_cl, CL_LEVEL_VERBOSE, "srv_sleep_callback (%s; data=%p)",
         (mode == ES_IDLE_IDLE
              ? "idle"
              : (mode == ES_IDLE_TIMED_OUT ? "timed out" : "cancel")),
         data);

  for (ses = srv->srv_session_head; ses != NULL; ses = ses_next) {
    srv_request *req, *req_next;

    ses_next = ses->ses_next;
    req_next = ses->ses_request_head;
    srv_request_link(req_next);

    while ((req = req_next) != NULL) {
      req_next = req->req_next;
      srv_request_link(req_next);

      /*  If we haven't needed the time in micros
       *  until now, get it.
       */
      if (now == 0) {
        if (gettimeofday(&tv, NULL) == 0)
          now = tv.tv_sec * (1000ull * 1000) + tv.tv_usec;
      }

      (*srv->srv_app->app_request_sleep)(srv->srv_app_data, srv, now, ses, req);

      srv_request_unlink(req);

      /*  If our next request just got killed,
       *  don't go on past it - just drop out.
       *  We'll get the survivors next time.
       */
      if (req_next != NULL && req_next->req_refcount <= 1) {
        srv_request_unlink(req_next);
        break;
      }
    }
  }

  /*  Repost the callback.
   */
  srv->srv_sleep_delay =
      srv_delay_create(srv, 1, 1, srv_sleep_callback, srv, "srv sleep delay");
  if (srv->srv_sleep_delay == NULL)
    cl_log(srv->srv_cl, CL_LEVEL_ERROR, "srv_sleep_callback: repost failed: %s",
           strerror(errno ? errno : ENOMEM));
}

int srv_sleep_initialize(srv_handle* srv) {
  if (srv->srv_app->app_request_sleep == NULL || srv->srv_sleep_delay != NULL)
    return 0;

  srv->srv_sleep_delay = srv_delay_create(srv, 1, 1, srv_sleep_callback, srv,
                                          "srv sleep delay init");
  if (srv->srv_sleep_delay == NULL) return errno ? errno : ENOMEM;

  return 0;
}

void srv_sleep_finish(srv_handle* srv) {
  srv_delay* del;

  if ((del = srv->srv_sleep_delay) != NULL) {
    srv->srv_sleep_delay = NULL;
    srv_delay_destroy(del);
  }
}
