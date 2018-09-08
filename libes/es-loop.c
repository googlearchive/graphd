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
#include "libes/esp.h"

#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>

#if ES_EMULATE_POLL_WITH_SELECT

/*  In Tiger (MacOS 10.4), a previously emulated poll was moved
 *  into the kernel ... breaking it for, oh, named pipes and terminal I/O.
 *  See: http://marc.theaimsgroup.com/?l=log&m=111515776629581&w=2
 */
#include <sys/types.h>
#include <sys/select.h>

int es_emulate_poll(struct pollfd *pfd, int n_pfd, int millis) {
  fd_set rfds, wfds;
  int i, max_fd = -1, total;
  struct timeval tv, *tvp = NULL;

  FD_ZERO(&rfds);
  FD_ZERO(&wfds);

  for (i = 0; i < n_pfd; i++) {
    if (pfd[i].events & POLLIN) FD_SET(pfd[i].fd, &rfds);
    if (pfd[i].events & POLLOUT) FD_SET(pfd[i].fd, &wfds);
    if (pfd[i].fd > max_fd) max_fd = pfd[i].fd;
  }
  if (millis >= 0) {
    tvp = &tv;
    tv.tv_sec = millis / 1000;
    tv.tv_usec = 1000 * (millis % 1000);
  }

  i = select(max_fd + 1, &rfds, &wfds, NULL, tvp);
  if (i < 0) return i;

  for (i = 0, total = 0; i < n_pfd; i++) {
    pfd[i].revents = 0;

    if (FD_ISSET(pfd[i].fd, &rfds)) pfd[i].revents |= POLLIN;
    if (FD_ISSET(pfd[i].fd, &wfds)) pfd[i].revents |= POLLOUT;

    if (pfd[i].revents != 0) total++;
  }
  return total;
}

#endif /* ES_EMULATE_POLL_WITH_SELECT */

static char const *pollfd_dump_results(struct pollfd *pfd, int n, char *buf,
                                       size_t size) {
  char *w = buf, *e = buf + size;
  int i;

  if (n <= 0) return "{}";
  if (size <= 2) return "{...}";

  *w++ = '{';
  *w = '\0';

  for (i = 0; i < n && e - w >= 10; i++) {
    if (pfd[i].revents != 0) {
      snprintf(w, (size_t)(e - w), " %d%s%s%s%s", pfd[i].fd,
               pfd[i].revents & POLLIN ? "r" : "",
               pfd[i].revents & POLLOUT ? "w" : "",
               pfd[i].revents & POLLERR ? "e" : "",
               pfd[i].revents & POLLNVAL ? "n" : "");
      w += strlen(w);
    }
  }

  if (i < n && e - w >= 5)
    memcpy(w, "...}", 5);
  else if (e - w >= 3)
    memcpy(w, " }", 3);

  return buf;
}

static char const *pollfd_dump(struct pollfd *pfd, int n, char *buf,
                               size_t size) {
  char *w = buf, *e = buf + size;
  int i;

  if (n == 0) return "{}";
  if (size <= 2) return "{...}";

  *w++ = '{';
  *w = '\0';

  for (i = 0; i < n && e - w >= 10; i++) {
    snprintf(w, (size_t)(e - w), " %d%s%s", pfd[i].fd,
             pfd[i].events & POLLIN ? "r" : "",
             pfd[i].events & POLLOUT ? "w" : "");
    w += strlen(w);
  }

  if (i < n && e - w >= 5)
    memcpy(w, "...}", 5);
  else if (e - w >= 3)
    memcpy(w, " }", 3);

  return buf;
}

/**
 * @brief Dispatch events to descriptors.
 *
 * Once the application has set up its descriptors, it calls
 * es_loop() and does all its remaining work from within callbacks
 * by es_loop().  The call runs until all descriptors have been
 * deleted, or the poll() call itself returns an error.
 *
 * If the loop terminates due to an error, it first
 * calls all callback with an ES_EXIT event mask.
 *
 * @param es handle allocated by es_create()
 * @return 0 on success.
 * @return EALREADY if another call to es_loop() is
 * 	already in progress.
 * @return EINTR if someone called es_break().
 * @return errno after a failing poll() call.
 */
int es_loop(es_handle *es) {
  if (es->es_looping) return EALREADY;

  es->es_looping = true;
  es->es_interrupted = false;

  while (es->es_desc_n > es->es_demon_n && !es->es_destroyed &&
         !es->es_interrupted) {
    int n_poll_events;
    int millis;
    time_t next_timeout;
    es_descriptor *ed;
    char buf[200];
    int err = 0;
    int i = 0;
    size_t end_i;

    time(&es->es_now);

    /*  Get the next timeout value.  Time out
     *  descriptors with negative timeout values.
     */
    millis = -1;
    while ((next_timeout = es_timeout_wakeup(es, &ed)) != 0) {
      struct pollfd *pfd;

      double dt = difftime(next_timeout, es->es_now);
      if (dt > 0.) {
        if (dt >= INT_MAX / 1000)
          millis = INT_MAX;
        else
          millis = dt * 1000;

        break;
      }

      if (ed->ed_poll == (size_t)-1) {
        cl_log(es->es_cl, CL_LEVEL_VERBOSE, "es: virtual timeout");

        ed->ed_activity = es->es_now;
        (*ed->ed_callback)(ed, -1, ES_TIMEOUT);
        cl_cover(es->es_cl);
      } else {
        cl_assert(es->es_cl, ed->ed_poll >= 0);
        cl_assert(es->es_cl, ed->ed_poll < es->es_poll_n);
        pfd = es->es_poll + ed->ed_poll;

        cl_log(es->es_cl, CL_LEVEL_DEBUG, "es: timeout on fd %d", pfd->fd);

        ed->ed_activity = es->es_now;
        (*ed->ed_callback)(ed, pfd->fd, ES_TIMEOUT);
        cl_cover(es->es_cl);
      }
    }

    /*  Pack pollfd structs tightly into es->es_poll.
     *
     *  Free slots form a chain via their fd members.
     *  <es->es_pool_free> holds the chain's head
     *  (i.e., the index of the first unoccupied slot),
     *  or -1 if all gaps are filled.
     */
    while (es->es_poll_free != -1) {
      int dst, src;

      /*  Unhook this free structure from the chain.
       */
      dst = es->es_poll_free;
      es->es_poll_free = es->es_poll[dst].fd;

      /*  Find a non-empty source structure to slide
       *  down to fill this hole.
       */
      for (src = es->es_poll_n - 1; src > dst; src--) {
        if (es->es_poll[src].fd != -1 &&
            es->es_desc[es->es_poll[src].fd] != NULL &&
            es->es_desc[es->es_poll[src].fd]->ed_poll == src)

          break;
      }

      /*  Found one?
       */
      if (src > dst) {
        es->es_poll[dst] = es->es_poll[src];
        es->es_desc[es->es_poll[dst].fd]->ed_poll = dst;

        /*  The original slot is now empty.  Because it
         *  is the highest non-empty slot we found,
         *  it will be cut off once es->es_poll_free
         *  becomes -1; but until then, we must mark
         *  it as empty, or it might be reused as filling.
         */
        es->es_poll[src].fd = -1;
        cl_cover(es->es_cl);
      }

      /*  If we didn't slide anything down into <dst>,
       *  <dst> is part of a sequence of empty struct pollfd
       *  at the end of our array.  We discard one of
       *  these - not necessarily dst - by cutting it off.
       *
       *  This means that es->es_poll_free can, during
       *  this loop, point *beyond* es->es_poll_n.
       */
      es->es_poll_n--;
    }

    /*  Get events from the operating system.
     */
    cl_log(es->es_cl, CL_LEVEL_VERBOSE, "es: poll [%lu] %s timeout=%d",
           (unsigned long)es->es_poll_n,
           pollfd_dump(es->es_poll, es->es_poll_n, buf, sizeof buf),
           (es->es_idle_callback_head != NULL || es->es_application_event_n > 0)
               ? 0
               : millis);

    n_poll_events =
        poll(es->es_poll, es->es_poll_n,
             es->es_idle_callback_head != NULL || es->es_application_event_n > 0
                 ? 0
                 : millis);
    if (n_poll_events < 0) err = errno;

    cl_log(es->es_cl, CL_LEVEL_VERBOSE, "es: poll [%lu] results: %d %s%s%s",
           (unsigned long)es->es_poll_n, n_poll_events,
           pollfd_dump_results(es->es_poll, es->es_poll_n, buf, sizeof buf),
           n_poll_events < 0 ? ": " : "",
           n_poll_events < 0 ? strerror(err) : "");

    if (n_poll_events < 0) {
      size_t i;

      if (err == EINTR) continue;

      cl_log_errno(es->es_cl, CL_LEVEL_ERROR, "poll", err,
                   "es_loop: catastrophic poll failure");

      /*  Pass an error event to all descriptors,
       *  then terminate.
       */
      for (i = 0; i < es->es_poll_n; i++) {
        struct pollfd *pfd;

        pfd = es->es_poll + i;
        if (pfd->fd < 0) continue;

        cl_assert(es->es_cl, pfd->fd >= 0);
        cl_assert(es->es_cl, pfd->fd < es->es_desc_m);

        ed = es->es_desc[pfd->fd];
        cl_assert(es->es_cl, ed != NULL);

        ed->ed_activity = es->es_now;
        (*ed->ed_callback)(ed, pfd->fd, ES_EXIT);
      }
      return err;
    }

    if (es->es_pre_dispatch != NULL) {
      (*es->es_pre_dispatch)(es->es_pre_dispatch_data, es);
      cl_cover(es->es_cl);
    }
    es->es_dispatching = 1;

    /*  Dispatch events.
     */
    if (n_poll_events == 0 && es->es_application_event_n == 0) {
      es_idle(es);
      cl_cover(es->es_cl);
    }

    end_i = es->es_poll_n;

    while (i < end_i && i < es->es_poll_n &&
           n_poll_events + es->es_application_event_n > 0 &&
           !es->es_destroyed) {
      struct pollfd const *const pfd = es->es_poll + i++;

      if (pfd->fd < 0 || es->es_desc[pfd->fd] == NULL ||
          (!pfd->revents && !es->es_desc[pfd->fd]->ed_application_event))

        continue;

      cl_assert(es->es_cl, pfd->fd >= 0);
      cl_assert(es->es_cl, pfd->fd < es->es_desc_m);

      ed = es->es_desc[pfd->fd];
      cl_assert(es->es_cl, ed != NULL);

      /*  Decrement the network event counter,
       *  if there were network events.
       */
      if (pfd->revents) n_poll_events--;

      if (pfd->events || ed->ed_application_event ||
          (~(POLLIN | POLLOUT) & pfd->revents)) {
        unsigned int ev = pfd->revents;

        if (ed->ed_application_event) {
          /* Decrement es->es_application_event_n,
           * if there was an application event.
           */
          es_application_event_clear(es, ed);
          ev |= ES_APPLICATION;
        }

        /*  We handle errors by forcing ES_INPUT or
         *  ES_OUTPUT.  The application will attempt
         *  to read or write and react appropriately,
         *  typically by closing the socket.
         *
         *  POLLHUP is set when neither reading nor
         *  writing can occur on a socket.  A
         *  half-close condition is indicated by POLLIN.
         *  POLLERR is set for ill-defined error
         *  conditions which should be handled by
         *  closing the socket.
         *
         */
        if (pfd->revents & (POLLERR | POLLHUP)) {
          if (pfd->revents & POLLERR)
            cl_log(es->es_cl, CL_LEVEL_FAIL, "POLLERR on %d", pfd->fd);

          if (pfd->events & POLLIN) ev |= ES_INPUT;
          if (pfd->events & POLLOUT) ev |= ES_OUTPUT;
        }
        ed->ed_activity = es->es_now;
        (*ed->ed_callback)(ed, pfd->fd, ev);
      }
    }

    /* Time-out idle events that have been pending for
     * too long without the system actually *being* idle.
     */
    es_idle_timeout(es);

    /*  Clear the "dispatching" flag before calling
     *  the post-dispatch callback so that an es_destroy()
     *  from within it will not call it again.
     */
    es->es_dispatching = 0;
    if (es->es_post_dispatch != NULL) {
      (*es->es_post_dispatch)(es->es_post_dispatch_data, es);
      cl_cover(es->es_cl);
    }
  }

  es->es_looping = false;

  if (es->es_destroyed)
    es_destroy(es);

  else if (es->es_interrupted)
    return EINTR;

  return 0;
}

/**
 * @brief Stop dispatching events to descriptors.
 *
 *  This breaks out of an ongoing call to es_loop(), with a view
 *  towards resuming the call - returning to the event dispatch loop -
 *  at a later time.
 *
 *  The loop stops only once its present iteration has completed - once
 *  all events from the current poll round have been dispatched,
 *  and the post-dispatch callback, if any, has been called.
 *
 * @param es handle allocated by es_create()
 * @return 0 on success.
 * @return EINVAL if no loop is going on
 * @return EALREADY if the loop has already been interrupted.
 */
int es_break_loc(es_handle *es, char const *file, int line) {
  if (!es->es_looping) return EINVAL;
  if (es->es_interrupted) return EALREADY;

  cl_log(es->es_cl, CL_LEVEL_DEBUG,
         "es_break: interrupting the ongoing es_loop [from %s:%d]", file, line);

  es->es_interrupted = true;
  return 0;
}
