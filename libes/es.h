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
#ifndef ES_H
#define ES_H 1

/**
 * @file es.h
 * @brief Event Server
 *
 * The libes library is an event dispatch layer used to
 * build event-based servers and other systems that use
 * a central dispatch loop wrapped around poll().
 *
 * @warning
 * While this module does not require its functions to be
 * called from a single thread, it does not lock against
 * parallel accesses. If a multithreaded application makes
 * libes calls from multiple threads, it needs to add locking
 * around the accesses to the same es_handle.
 */

#include <poll.h>    /* poll events */
#include <stdbool.h> /* bool */
#include <time.h>    /* time_t */

#include "libcm/cm.h" /* Memory 	*/
#include "libcl/cl.h" /* Logging 	*/

#ifdef DOCUMENTATION_GENERATOR_ONLY
/**
 * @brief Opaque pointer to an idle callback structure.
 *
 *  Created by es_idle_create()
 */
typedef void *es_idle_callback;
#else

typedef struct es_idle_callback es_idle_callback;

#endif

/**
 * @brief Possible call reasons for an idle callback.
 */
typedef enum {
  /**
   * @brief The system is idle.
   */
  ES_IDLE_IDLE = 0,

  /**
   * @brief The maximum delay time specified when installing the
   *	idle callback has passed without an idle moment.
   */
  ES_IDLE_TIMED_OUT = 1,

  /**
   * @brief The idle callback is being cancelled externally (for
   *	example, as part of shutting down the system.)
   */
  ES_IDLE_CANCEL = 2

} es_idle_callback_timed_out;

/**
 * @brief Idle callback function pointer, invoked with its callback data.
 * @param _callback_data  	opaque closure
 * @param _timed_out		circumstances of the call
 */
typedef void es_idle_callback_func(void *_callback_data,
                                   es_idle_callback_timed_out _timed_out);

#if __APPLE__

/* __APPLE__ MacOS 10.4 poll(2) doesn't work on /etc/tty.
 */

#define ES_EMULATE_POLL_WITH_SELECT 1
#endif

#ifdef ES_EMULATE_POLL_WITH_SELECT
extern int es_emulate_poll(struct pollfd *, int, int);
#undef poll
#define poll(a, b, c) es_emulate_poll(a, b, c)
#endif

/**
 * @brief CVS information of the most recently checked-in file of the library.
 */
extern char const es_build_version[];

/**
 * @brief Flag used in the third parameter of an es_descriptor_callback()
 * 	and with es_subscribe() and es_unsubscribe().
 *
 * The using application sets this flag to wait for incoming data.
 * It is set in the event results if data is available, or if
 * a read would terminate immediately for other reasons (e.g., EOF).
 */
#define ES_INPUT POLLIN

/**
 * @brief Flag used in the third parameter of an es_descriptor_callback()
 * 	and with es_subscribe() and es_unsubscribe().
 *
 * The caller sets this event flag to wait for outgoing capacity.
 * It is set in the event results if capacity is available, or
 * if a write would terminate immediately for other reasons.
 */
#define ES_OUTPUT POLLOUT

/**
 * @brief Flag used in the third parameter of an es_descriptor_callback().
 *
 * The calling application never sets this flag, but it can
 * always be set in the results.
 * If any bit of this mask is set in the events delivered to the
 * application, an error has happened on the line.
 */
#define ES_ERROR (POLLERR | POLLHUP | POLLNVAL)

/**
 * @brief Flag used in the third parameter of an es_descriptor_callback().
 *
 * The calling application never sets this flag.
 * If it is set in the results delivered to the application,
 * the poll call has timed out.
 */
#define ES_TIMEOUT 0x00100000

/**
 * @brief Flag used in the third parameter of an es_descriptor_callback().
 *
 * The calling application never sets this flag.
 * If it is set in the results delivered to the application,
 * the es layer is asked to shut down its connections.
 * It is up to the application to free the resources allocated
 * for the connection.  It must not block in response.
 */
#define ES_EXIT 0x00200000

/**
 * @brief Flag used in the third parameter of an es_descriptor_callback().
 *
 * The calling application never sets this flag.
 * If it is set in the results delivered to the application,
 * the application has called es_application_event() on the
 * connection.
 */
#define ES_APPLICATION 0x00400000

/**
 * @brief Opaque timeout data structure, used to manage
 * 	timeouts associated with a descriptor.
 */
typedef struct es_timeout es_timeout;

/**
 * @brief Opaque module handle, allocated with es_create(),
 * 	freed with es_destroy().
 */
typedef struct es_handle es_handle;

struct es_descriptor;

/**
 * @brief Callback for a single descriptor
 *
 * The events indicated by @b events have happened on @b fd,
 * one or more of a binary @em OR of #ES_EXIT, #ES_TIMEOUT,
 * #ES_INPUT, #ES_OUTPUT, or #ES_ERROR.
 * The application reacts to that, but mustn't
 * block.
 *
 * @param descriptor	es_descriptor and the
 * 			surrounding application structure
 * @param fd	file descriptor the event happened on
 * @param events binary OR of the events that have happened
 */
typedef void es_descriptor_callback(struct es_descriptor *descriptor, int fd,
                                    unsigned events);

/**
 * @brief Pre/Post dispatch callback
 *
 * Just after the inner es_loop() wakes up from having waited for
 * new events, the pre-dispatch callback is called; once the inner
 * loop has dispatched all its events, the post-dispatch callback is
 * called.  Between the two, timeouts are processed, and the actual
 * wait for new events (usually using poll()) takes pace.
 *
 * @param callback_data	opaque application data, passed to
 *			es_set_pre_dispatch(), es_set_post_dispatch().
 * @param es		opaque module handle, created with es_create()
 */
typedef void es_iteration_callback(void *callback_data, es_handle *es);

/**
 * @brief Descriptor for a single connection.
 *
 *  Do not access or edit members other than #es_descriptor::ed_callback
 *  and #es_descriptor::ed_displayname
 *  directly; use the functions in this module instead.
 *
 *  Usually, the descriptor is embedded in an application
 *  data structure that manages application session state.
 */
typedef struct es_descriptor {
  /**
   * @brief Application callback used to deliver data
   * 	to the session.
   *
   *  Before an application installs a descriptor using
   *  es_open(), it must set this member to the address of
   *  the descriptor callback that will handle events
   *  for the descriptor.
   */
  es_descriptor_callback *ed_callback;

  /**
   * @brief Short name for the connection, used in
   *	human-readable error messages.
   *
   *  Before an application installs a descriptor using
   *  es_open(), it must set this member to a string that
   *  can be used to describe the session or connection
   *  in error messages.
   */
  char const *ed_displayname;

  /**
   * @brief Private index of the pollfd structure for the
   * 	descriptor.
   */
  size_t ed_poll;

  /**
   * @brief Private indicator for the most recent acvitiy
   * 	on the descriptor, used by the timeout.
   */
  time_t ed_activity;

  /**
   * @brief Private pointer to the timeout responsible for
   * 	this descriptor.
   *
   * Use es_timeout_add() to modify a descriptor's timeout.
   */
  es_timeout *ed_timeout;

  /**
   *  @brief Private previous pointer in a doubly linked list
   * 	of descriptors with the same timeout as this one.
   *
   *  Whenever timeout-preventing activity takes place, a
   *  descriptor moves to the head of that chain; its tail
   *  is polled to yield the next timeout.
   */
  struct es_descriptor *ed_timeout_prev;

  /**
   *  @brief Private next pointer in a doubly linked list of descriptors
   *  with the same timeout as this one.
   */
  struct es_descriptor *ed_timeout_next;

  /**
   * @brief Is this a "demon" descriptor that just remains
   * 	open in the background, or does it count for purposes
   *  	of keeping the poll loop running?
   */
  unsigned int ed_demon : 1;

  /**
   * @brief If this bit is set, there is an external event on
   *	this connection that requires es_poll() to return an
   * 	application event.
   */
  unsigned int ed_application_event : 1;

  /* More descriptor-specific data in the surrounding struct. */

} es_descriptor;

es_handle *es_create(cm_handle *, cl_handle *);
void es_destroy(es_handle *);

#ifdef DOCUMENTATION_GENERATOR_ONLY

/**
 * @brief Subscribe to events on a descriptor.
 *
 * @param es opaque module handle, created with es_create()
 * @param ed descriptor, initialized with es_open()
 * @param mask events the application is interested in.
 */

void es_subscribe(es_handle *es, es_descriptor *ed, unsigned int mask) {}

#else
void es_subscribe_loc(es_handle *_es, es_descriptor *_ed, unsigned int _mask,
                      char const *_file, int _line);
#define es_subscribe(a, b, c) es_subscribe_loc(a, b, c, __FILE__, __LINE__)
#endif

#ifdef DOCUMENTATION_GENERATOR_ONLY

/**
 * @brief Stop receciving specific events for a descriptor.
 * @param es opaque module handle, created with es_create()
 * @param ed descriptor, initialized with es_open()
 * @param mask events the application is no longer interested in.
 */

void es_unsubscribe(es_handle *es, es_descriptor *ed, unsigned int mask) {}
#else
void es_unsubscribe_loc(es_handle *_es, es_descriptor *_ed, unsigned int _mask,
                        char const *_file, int _line);
#define es_unsubscribe(a, b, c) es_unsubscribe_loc(a, b, c, __FILE__, __LINE__)
#endif

void es_application_event_loc(es_handle *_es, es_descriptor *_ed,
                              char const *_file, int _line);
#define es_application_event(a, b) \
  es_application_event_loc(a, b, __FILE__, __LINE__)

void es_close(es_handle *, es_descriptor *);
int es_open(es_handle *, int, unsigned int, es_descriptor *);
int es_open_null(es_handle *, es_descriptor *);
void es_demon(es_handle *, es_descriptor *, bool);
void es_set_pre_dispatch(es_handle *, es_iteration_callback *, void *);
void es_set_post_dispatch(es_handle *, es_iteration_callback *, void *);

/* es-idle.c */

void es_idle_callback_cancel(es_handle *, es_idle_callback *);
es_idle_callback *es_idle_callback_create(es_handle *_es,
                                          unsigned long _seconds,
                                          es_idle_callback_func *_callback,
                                          void *_callback_data);

/* es-now.c */

time_t es_now(es_handle *);

/* es-loop.c */

int es_loop(es_handle *es);
#define es_break(es) es_break_loc((es), __FILE__, __LINE__)
int es_break_loc(es_handle *, char const *, int);

/* es-timeout.c */

es_timeout *es_timeout_create(es_handle *, unsigned long);
void es_timeout_destroy(es_handle *, es_timeout *);
void es_timeout_delete(es_handle *, es_descriptor *);
void es_timeout_add(es_handle *, es_timeout *, es_descriptor *);

#endif /* ES_H */
