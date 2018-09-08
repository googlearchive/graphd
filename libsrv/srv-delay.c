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
 * @file srv-delay.c -- schedule an delayed callback.
 *
 * A delayed callback is similar to an idle callback,
 * but happens no sooner than P milliseconds after
 * it has been set, but no later than E milliseconds.
 *
 * If the server is idle, the delay callback happens
 * at the promised time t0+P
 *
 *        ,--------------.
 *        |              V
 * |------|--------------x--------------- server activity
 *                       p        e
 *
 * But if the server is active, the delay callback happens
 * either once the server is idle, or at a future
 * emergency time e.
 *        ,--------------.
 *        |              `----.
 *        |                   V
 * |------|-------~||||||||||-x---------- server activity
 *                       p        e
 *
 *        ,--------------.
 *        |              `--------.
 *        |                       V
 * |------|-------~|||||||||||||||x|||--- server activity
 *                       p        e
 *
 */

/*  We're idle, or our timer has elapsed.
 *  Call the application callback.
 */
static void srv_delay_idle_callback(void *data,
                                    es_idle_callback_timed_out mode) {
  srv_delay *del = (srv_delay *)data;
  srv_handle *srv = del->del_srv;
  void (*callback)(void *, es_idle_callback_timed_out);
  void *callback_data;

  cl_enter(srv->srv_cl, CL_LEVEL_VERBOSE, "+++ (%s; data=%p) +++",
           (mode == ES_IDLE_IDLE
                ? "idle"
                : (mode == ES_IDLE_TIMED_OUT ? "timed out" : "cancel")),
           data);

  cl_assert(srv->srv_cl, SRV_IS_DELAY(del));

  callback = del->del_callback;
  callback_data = del->del_callback_data;

  /*  Destroy the delay prior to calling the callback
   *  so the callback can re-institute the delay
   *  without getting in our way.
   */
  del->del_es_idle_callback = NULL;
  srv_delay_destroy(del);

  (*callback)(callback_data, mode);

  cl_leave(srv->srv_cl, CL_LEVEL_VERBOSE, "---");
  cl_cover(srv->srv_cl);
}

/*  The initial delay has elapsed.
 *  Create an idle callback.
 */
static void srv_delay_ed_callback(es_descriptor *ed, int minus_one,
                                  unsigned timed_out) {
  srv_delay *del = (srv_delay *)ed;
  srv_handle *srv = del->del_srv;

  cl_enter(srv->srv_cl, CL_LEVEL_VERBOSE, "+++ del=%p +++", (void *)del);

  /* Cancel the timeout that just elapsed.
   */
  if (del->del_es_timeout != NULL) {
    /* Disassociate the descriptor from the timeout.
    */
    es_timeout_delete(srv->srv_es, &del->del_ed);

    /*  Destroy the timeout.
     */
    es_timeout_destroy(srv->srv_es, del->del_es_timeout);
    del->del_es_timeout = NULL;
  }

  if (timed_out == ES_EXIT) {
    cl_log(srv->srv_cl, CL_LEVEL_VERBOSE,
           "srv_delay_ed_callback: exiting - calling the "
           "delayed callback _now_.");

    /*  If we already had an idle callback installed,
     *  cancel it.
     */
    if (del->del_es_idle_callback != NULL) {
      es_idle_callback_cancel(del->del_srv->srv_es, del->del_es_idle_callback);
      del->del_es_idle_callback = NULL;
    } else {
      /*  Otherwise, just call the result function directly.
       */
      srv_delay_idle_callback(del, ES_IDLE_CANCEL);
    }
  } else {
    /*  Normal operation.  The timer has run off,
     *  but nothing else terrible has happened.
     *
     *  Install an idle callback.
     */
    del->del_es_idle_callback = es_idle_callback_create(
        srv->srv_es, del->del_max_seconds > del->del_min_seconds
                         ? del->del_max_seconds - del->del_min_seconds
                         : 0,
        srv_delay_idle_callback, del);

    if (del->del_es_idle_callback == NULL)

      /* Ick.  Allocation error.
       * Just call the result callback directly.
       */
      srv_delay_idle_callback(del, ES_IDLE_CANCEL);
  }

  cl_leave(srv->srv_cl, CL_LEVEL_VERBOSE, "---");
  cl_cover(srv->srv_cl);
}

/**
 * @brief Create, and install, a "delay" callback.
 *
 *  The delay callback will uninstall and delete
 *  itself prior to triggering.  It is safe for
 *  the callback to repost itself from within
 *  the call.
 *
 *  This isn't very efficient, but more flexible;
 *  the system can have more than one srv_delay
 *  structure.
 *
 * @param srv		opaque server handle
 * @param min_seconds	call back no sooner than this
 *			many seconds in the future.
 * @param max_seconds	call back no later than this many
 *			seconds in the future.
 * @param callback	call this callback
 * @param callback_data	with this argument.
 *
 * @return NULL on allocation error, otherwise
 *  	a pointer to the srv_delay structure.
 */
srv_delay *srv_delay_create(srv_handle *srv, unsigned long min_seconds,
                            unsigned long max_seconds,
                            srv_delay_callback_func *callback,
                            void *callback_data, const char *displayname) {
  srv_delay *del;
  int err;

  del = cm_zalloc(srv->srv_cm, sizeof(*del));
  if (del == NULL) return del;

  SRV_SET_DELAY(del);
  del->del_ed.ed_callback = srv_delay_ed_callback;
  if (displayname != NULL)
    del->del_ed.ed_displayname = displayname;
  else
    del->del_ed.ed_displayname = "libsrv delay timeout";

  err = es_open_null(srv->srv_es, &del->del_ed);
  if (err != 0) {
    cm_free(srv->srv_cm, del);
    return NULL;
  }

  del->del_srv = srv;
  del->del_callback = callback;
  del->del_callback_data = callback_data;
  del->del_min_seconds = min_seconds;
  del->del_max_seconds = max_seconds;

  /*  Install an "es" callback to wake us
   *  when min-seconds have passed.
   */
  del->del_es_timeout = es_timeout_create(srv->srv_es, min_seconds);
  if (del->del_es_timeout == NULL) {
    es_close(srv->srv_es, &del->del_ed);
    cm_free(srv->srv_cm, del);

    return NULL;
  }
  es_timeout_add(srv->srv_es, del->del_es_timeout, &del->del_ed);
  cl_cover(srv->srv_cl);

  cl_assert(srv->srv_cl, SRV_IS_DELAY(del));
  cl_log(srv->srv_cl, CL_LEVEL_VERBOSE,
         "srv_delay_create(min=%lu, max=%lu) -> %p (timeout: %p)", min_seconds,
         max_seconds, (void *)del, (void *)del->del_es_timeout);

  return del;
}

/**
 * @brief Remove a specific delay callback from the server.
 * @param del	delay callback to remove
 */
void srv_delay_destroy(srv_delay *del) {
  srv_handle *srv;

  if (del == NULL) return;

  srv = del->del_srv;

  /*  If we have one, call the idle callback associated
   *  with the delay. This will recursively call
   *  srv_delay_destroy again.
   */
  if (del->del_es_idle_callback != NULL) {
    es_idle_callback *ecb;

    /*
     *  es_idle_callback_cancel() will call the associated
     *  callback with ES_IDLE_CANCEL.
     *
     *  srv_delay_idle_callback() with mode of
     *  ES_IDLE_CANCEL will in turn, destroy the
     *  delay the callback is associated with,
     *  landing us in srv_delay_destroy again.
     *
     *  But because we no longer have a del->del_es_idle_callback,
     *  on the delay, we'll proceed that time.
     */
    ecb = del->del_es_idle_callback;
    del->del_es_idle_callback = NULL;

    es_idle_callback_cancel(srv->srv_es, ecb);
    return;
  }

  cl_assert(srv->srv_cl, SRV_IS_DELAY(del));
  cl_log(srv->srv_cl, CL_LEVEL_VERBOSE,
         "srv_delay_destroy (%p: timeout=%p min=%lu, max=%lu)", (void *)del,
         (void *)del->del_es_timeout, del->del_min_seconds,
         del->del_max_seconds);

  /*  Destroy the timeout.
   */
  if (del->del_es_timeout != NULL) {
    es_timeout_delete(srv->srv_es, &del->del_ed);
    es_timeout_destroy(srv->srv_es, del->del_es_timeout);

    del->del_es_timeout = NULL;
  }

  /*  Destroy the null descriptor that carried
   *  this timeout.
   */
  es_close(srv->srv_es, &del->del_ed);
  cm_free(srv->srv_cm, del);
}
