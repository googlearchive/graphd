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
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <stdarg.h>

#include "srvp.h"

/*
 * srv-settle.c -- signal that a child has settled in,
 *  	and everything is actually working.
 *
 *  	This is only used when starting graphd in server mode.
 */

/*  Utility: close a file descriptor (if it's not -1 already),
 *  and replace it with -1.
 *
 *  If there is an error, complain and ignore it.
 */
static void noisy_close(cl_handle* cl, int* fd, char const* name) {
  if (*fd != -1) {
    if (close(*fd) != 0)
      cl_log_errno(cl, CL_LEVEL_ERROR, "close", errno, "%s=%d (ignored)", name,
                   *fd);
    *fd = -1;
  }
}

/*
 * @brief This process is not participating in the settling protocol.
 * @param srv	server handle
 */
void srv_settle_close(srv_handle* srv) {
  noisy_close(srv->srv_cl, srv->srv_settle_pipe, "srv_settle_pipe[0]");
  noisy_close(srv->srv_cl, srv->srv_settle_pipe + 1, "srv_settle_pipe[1]");
}

/*
 * @brief Delay settling-in until I say so.
 * @param srv	server handle
 *
 *  Without this call, the server considers itself settled-in after
 *  the startup() application callback returns OK (0).
 */
void srv_settle_delay(srv_handle* srv) { srv->srv_settle_application = true; }

/* @brief Report a startup failure.
 *
 *  This is called by the starting child when something goes
 *  wrong.  If srv_epitaph is called while there is still a line
 *  open, we prefer srv_settle_error() and exit over writing an
 *  epitaph file.
 */
void srv_settle_error(srv_handle* srv, char const* fmt, ...) {
  char bigbuf[1024 * 16];
  va_list ap;
  int cc;
  size_t i, n;

  if (srv->srv_settle_pipe[1] == -1) return;

  /* Format the error message.
   */
  va_start(ap, fmt);
  vsnprintf(bigbuf, sizeof bigbuf, fmt, ap);
  va_end(ap);

  bigbuf[sizeof bigbuf - 1] = '\0';

  /* Write the formatted message to the pipe.
   */
  n = strlen(bigbuf);
  for (i = 0; i < n; i += cc) {
    cc = write(srv->srv_settle_pipe[1], bigbuf + i, n - i);
    if (cc < 0) break;
  }

  /* Close the pipe's write-end.
   */
  noisy_close(srv->srv_cl, srv->srv_settle_pipe + 1, "srv_settle_pipe[1]");
}

/* @brief Report a correct start-up.
 *
 *  This is called by the starting child when it has started up
 *  successfully.  It closes the communication line to the parent,
 *  if one is still open, signalling that everything is okay.
 */
void srv_settle_ok(srv_handle* srv) {
  noisy_close(srv->srv_cl, srv->srv_settle_pipe + 1, "srv_settle_pipe[1]");
}

/*  @brief Receive results of a child's startup.
 *
 *  We're the parent; we're waiting for a response from our child
 *  that either says "I've started; everything's okay" (return 0)
 *  or "There was a problem, here's the text of an error message."
 *
 *  @param 	srv 	-- the server handle
 *  @param	err_out -- on return, NULL or an error message
 *		in storage cm_malloc()'ed in srv->srv_cm.
 *
 *  @return
 *	0          -- child settled in successfully
 *	SRV_ERR_NO -- child terminated with an error message.
 *		*err_out contains a malloc'ed pointer to that
 *		error message (in srv->srv_cm).
 *	other nonzero error pointers: unexpected system errors.
 *
 */
int srv_settle_wait(srv_handle* srv, char** err_out) {
  char bigbuf[1024 * 16];
  size_t i;
  int cc;

  *err_out = NULL;

  /*  Close the write end of the pipe.  If we're
   *  the only process owning it, that'll allow
   *  the read to come back immediately.
   */
  noisy_close(srv->srv_cl, srv->srv_settle_pipe + 1, "srv_settle_pipe[1]");

  /*  If there is no read end, we're done.
   */
  if (srv->srv_settle_pipe[0] == -1) return 0;

  /* Block until the other side is done.
   */
  i = 0;
  while (i < sizeof bigbuf) {
    cc = read(srv->srv_settle_pipe[0], bigbuf + i, sizeof bigbuf - i);
    if (cc <= 0) break;

    i += cc;
  }

  /*  Close the read end.
   */
  noisy_close(srv->srv_cl, srv->srv_settle_pipe + 0, "srv_settle_pipe[0]");

  /*  No news is good news.
   */
  if (i == 0) return 0;

  /*  Return a copy of the error message we just received.
   */
  bigbuf[i >= sizeof bigbuf ? sizeof bigbuf - 1 : i] = '\0';
  *err_out = cm_strmalcpy(srv->srv_cm, bigbuf);
  if (*err_out == NULL) return ENOMEM;

  /* Meaning, the start-up has failed; please print this and exit.
   */
  return SRV_ERR_NO;
}
