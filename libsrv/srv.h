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
#ifndef SRV_H

/**
 * @brief Guard against multiple includes.
 */
#define SRV_H 1

#include <stdlib.h>     /* size_t */
#include <stdbool.h>    /* bool */
#include <time.h>       /* clock_t */
#include <sys/time.h>   /* struct timeval */
#include <netinet/in.h> /* sockaddr_in */

#include "libcm/cm.h"
#include "libcl/cl.h"
#include "libes/es.h"

#define SRV_ERR_BASE (-6000)
#define SRV_ERR_NO (SRV_ERR_BASE + 1)
#define SRV_ERR_ALREADY (SRV_ERR_BASE + 2)
#define SRV_ERR_SYNTAX (SRV_ERR_BASE + 3)
#define SRV_ERR_SEMANTICS (SRV_ERR_BASE + 4)
#define SRV_ERR_MORE (SRV_ERR_BASE + 5)
#define SRV_ERR_NOT_SUPPORTED (SRV_ERR_BASE + 6)
#define SRV_ERR_REQUEST_TOO_LONG (SRV_ERR_BASE + 7)
#define SRV_ERR_ADDRESS (SRV_ERR_BASE + 8)

/*  Maslow's hierarchy of needs, but for sessions.
 */
typedef enum srv_need {
  SRV_INPUT = 0,
  SRV_OUTPUT,
  SRV_RUN,
  SRV_BUFFER,
  SRV_EXTERNAL

} srv_need;

/**
 * @file srv.h -- libsrv API.
 *
 * The libsrv library implements methods for an abstract
 * TCP- or interactive server.  Commands are issued by a user;
 * replies come back.
 */

/**
 * @brief NULL-terminated array of version strings for this library
 */
extern char const srv_build_version[];

/**
 * @brief debug facility for scheduler-related data
 *
 *  (Redefined as GRAPHD_FACILITY_SCHEDULER in ../graph/src.)
 */
#define SRV_FACILITY_SCHEDULER (1ul << 5)

/**
 * @brief Number of seconds to wait for the application while
 *  shutting down the server.
 *
 * If shutting down takes longer than this, the service
 * just exits, and relies on the application to clean things
 * up during restart.
 *
 * Can be configured with shutdown-delay in the server
 * configuration file.
 */
#define SRV_SHUTDOWN_DELAY_SECONDS_DEFAULT (60 * 5)

/**
 * @brief Oh my, look at the time.
 *
 *  This macro deals correctly with overflow.
 *
 * @param now		The time now, in milliseconds since the epoch.
 * @param deadline	A deadline, in milliseconds since the epoch.
 */
#define SRV_PAST_DEADLINE(now, deadline)                                  \
  ((deadline) != 0 && ((srv_msclock_t)(now) - (srv_msclock_t)(deadline) < \
                       (srv_msclock_t)(deadline) - (srv_msclock_t)(now)))

/**
 * @brief Time in milliseconds since Jan 1st, 1970.
 */
typedef unsigned long long srv_msclock_t;

/**
 * @brief Request and session IDs, unique per srv_handle.
 */
typedef unsigned long long srv_unique_id;

/**
 * @brief Global module handle.
 */
typedef struct srv_handle srv_handle;

/**
 * @brief Global module configuration.
 */
typedef struct srv_config srv_config;

/**
 * @brief A single buffered connection, part of a srv_sesion.
 */
typedef struct srv_buffered_connection srv_buffered_connection;

/**
 * @brief A buffer pool, used by the buffered connections.
 */
typedef struct srv_buffer_pool srv_buffer_pool;

/**
 * @brief An application-specified option.
 */
typedef struct srv_option srv_option;

/**
 * @brief An application-specified configuration file entry.
 */
typedef struct srv_config_parameter srv_config_parameter;

/**
 * @brief A session; usually placed at the beginning of a
 * 	larger, application-specific session struct.
 */
typedef struct srv_session srv_session;

/* @brief A descriptor structure for delayed invocation.
 */
typedef struct srv_delay srv_delay;

/* @brief Make libsrv understand es_timeouts
 */
typedef es_timeout srv_timeout;

/**
 * @brief A descriptor structure for idle callbacks.
 */
typedef void srv_idle_callback_func(void *_data,
                                    es_idle_callback_timed_out _timed_out);

typedef struct srv_idle_context {
  srv_idle_callback_func *idle_callback;
  es_idle_callback *idle_es;

  /* Application-specific details go here. */

} srv_idle_context;

/* A network address
 *
 *	The url, host and port are always piggybacked on the
 *	allocation of the srv_address and thus never need to
 *	be freed.
 */
typedef struct srv_address {
  cm_handle *addr_cm;
  char const *addr_url;  /* the tcp:// url form of the address */
  char const *addr_host; /* the host, numeric or symbolic */
  char const *addr_port; /* the port */
} srv_address;

/**
 * @brief Pre-write callback; passed an opaque pointer on invocation.
 *
 * @param 	pointer	Application-specific pointer
 * @param 	block	If true, please return after completion only
 * @param	any	out: set if the call did anything useful
 * @return 	0 on succcess, a nonzero error code on error.
 */
typedef int srv_pre_callback(void *pointer, bool block, bool *any);

/**
 * @brief Run a session for an interface
 *
 * @param app_data	opaque application data
 * @param srv		libsrv module handle
 * @param ses		session we're adjusting events for.
 * @param deadline	run at most until this time
 *			(in milliseconds since 1970.)
 */
typedef bool srv_session_interface_run_callback(void *app_data, srv_handle *srv,
                                                srv_session *ses,
                                                srv_msclock_t deadline);

/**
 * @brief Adjust which events we're listening for.
 *
 * @param app_data	opaque application data
 * @param srv		libsrv module handle
 * @param ses		session we're adjusting events for.
 */
typedef void srv_session_interface_listen_callback(void *app_data,
                                                   srv_handle *srv,
                                                   srv_session *ses);

/**
 * @brief Install a timeout on an interface
 *
 * @param app_data	opaque application data
 * @param timeout	the timeout to set
 */
typedef void srv_session_interface_set_timeout_callback(void *app_data,
                                                        srv_timeout *timeout);

/**
 * @brief Per-session, per-interface type data.
 */
typedef struct srv_session_interface_type {
  /**
   * @brief Callback: Run the session.
   */
  srv_session_interface_run_callback *sit_run;

  /**
   * @brief Callback: Adjust the session's link to any central
   * 	file descriptors so that the next wait of the
   *  	central event loop waits for the right events.
   */
  srv_session_interface_listen_callback *sit_listen;

  /* @brief Callback: set an es_timeout structure for the es_descriptor(s)
  */
  srv_session_interface_set_timeout_callback *sit_set_timeout;

} srv_session_interface_type;

/**
 * @brief List of modules and their versions, typically generated by
 *	build-version.sh.
 *
 *  This is used to print the list of modules of the application
 *  when the executable is invoked with -m.
 */
typedef struct srv_build_version_reference {
  /**
   * @brief Name of the source module (e.g, "libsrv")
   */
  char const *vr_module;

  /**
   * @brief Version of the named module, based on CVS file mod times
   */
  char const *vr_version;

} srv_build_version_reference;

/**
 * @brief Method: the parent (monitoring) process has changed PIDs.
 * 	Please adjust your set.
 *
 * @param 	data	opaque application data
 * @param 	srv	module handle
 * @param	pid	new pid.
 *
 * @return 0 on success, a nonzero error code on error.
 */
typedef int srv_app_spawn(void *data, srv_handle *srv, pid_t pid);

/**
 * @brief Method: Begin serving requests.
 *
 *   This callback is optional; if the application doesn't have
 *   state to clean up prior to shutdown, it can be left NULL.
 *
 * @param 	data	opaque application data
 * @param 	srv	module handle
 *
 * @return	0 on success, a nonzero error code on error.
 */
typedef int srv_app_startup(void *data, srv_handle *srv);

/**
 * @brief Method: Startup is now complete. Finish setup.
 *
 * @param 	data	opaque application data
 * @param 	srv	module handle
 *
 * @return	0 on success, a nonzero error code on error.
 */
typedef int srv_app_startup_complete(void *data, srv_handle *srv);

/**
 * @brief Method: Spawned a new SMP process, do something with it
 *
 * @param 	data	opaque application data
 * @param 	srv	module handle
 * @param 	index	index of the process, from 0
 *			to number of configured processes
 *
 * @return	0 on success, a nonzero error code on error.
 */
typedef int srv_app_smp_startup(void *data, srv_handle *srv, size_t index);
/**
 * @brief Method: An SMP process returned from wait(2), what should I do?
 *
 * @param 	data	opaque application data
 * @param 	srv	module handle
 * @param 	index	index of the process, from 0
 *			to number of configured processes
 * @param	status	status returned from wait(2)
 *
 * @return	0 on successful completion, SRV_ERR_MORE to respawn
 *		and SRV_ERR_NOT_SUPPORTED to fail, epically
 */
typedef int srv_app_smp_finish(void *data, srv_handle *srv, size_t index,
                               int status);
/**
 * @brief Method: Shut down the application.
 *
 *   This callback is optional; if the application doesn't have
 *   state to clean up prior to shutdown, it can be left NULL.
 *
 *   The shutdown function can be called even if srv_app_startup()
 *   wasn't called or returned an error code.
 *
 * @param 	data	opaque application data
 * @param 	srv	module handle
 */
typedef void srv_app_shutdown(void *data, srv_handle *srv);

/**
 * @brief Method: Shut down a session.
 *
 *   This callback is optional; if the application doesn't have
 *   state to clean up prior to shutdown, it can be left NULL.
 *
 * @param 	data		opaque application data
 * @param 	srv		module handle
 * @param 	session_data	application's per-session data
 *				(first part is a srv_session)
 */
typedef void srv_app_session_shutdown(void *data, srv_handle *srv,
                                      void *session_data);

/**
 * @brief Method: Initialize a session.
 *
 *   The srv_session part is initialized prior to this callback.
 *   (Don't bzero the session data!)
 *
 * @param 	data		opaque application data
 * @param 	srv		module handle
 * @param 	session_data	application's per-session data
 *
 * @return 0 on success, a nonzero error code on error.
 */
typedef int srv_app_session_initialize(void *data, srv_handle *srv,
                                       void *session_data);

/**
 * @brief Method: Return the prompt for a session.
 *
 *  This is optional; if there is no meaningful prompt,
 *  just return NULL.
 *
 * @param 	data		opaque application data
 * @param 	srv		module handle
 * @param 	session_data	application's per-session data
 * @param 	buf		buffer to compose a prompt in
 * @param	size		# of bytes pointed to by buf.
 *
 * @return NULL or a prompt for use by a human-facing interactive session.
 */
typedef char const *srv_app_session_interactive_prompt(void *data,
                                                       srv_handle *srv,
                                                       void *session_data,
                                                       char *buf, size_t size);

/**
 * @brief Method: Initialize a request.
 *
 *  The generic parts of the request have been initialized by
 *  the session at this point - don't bzero it!
 *
 *  The call is executed when a request structure is at first
 *  created, and before the request knows what command it'll
 *  get to execute.
 *
 * @param 	data		opaque application data
 * @param 	srv		module handle
 * @param 	session_data	application's per-session data
 * @param 	request_data	application's per-request data
 *
 * @return 0 on success, a nonzero error code on error.
 */
typedef int srv_app_request_initialize(void *data, srv_handle *srv,
                                       void *session_data, void *request_data);

/**
 * @brief Method: Parse a command for a request.
 *
 *   The text between *s and e may be less than, more than,
 *   or exactly one command or response.  It's up to the
 *   application to detect command boundaries and parameterize
 *   the existing ses->ses_request with the parsed command data.
 *
 *   If s and e are NULL, the connection has been closed, and
 *   any partial state parsed so far needs to be taken on its
 *   own merits rather than waiting for completion.  (I.e.,
 *   if you want to complain about trailing garbage, do it
 *   there.)
 *
 * @param 	data		opaque application data
 * @param 	srv		module handle
 * @param 	session_data	application's per-session data
 * @param 	request_data	application's per-request data
 * @param 	s		NULL or in/out: not-yet-parsed bytes
 * @param 	e		NULL or pointer just past the last
 *				available byte.
 * @param 	deadline	run at most for this long.
 *
 * @return 0 on success, a nonzero error code on error.
 *	A nonzero error code is an unexpected error and fatal for
 * 	the connection.
 */
typedef int srv_app_request_input(void *data, srv_handle *srv,
                                  void *session_data, void *request_data,
                                  char **s, char *e, srv_msclock_t deadline);

/**
 * @brief Method: Execute a request
 *
 *  "Execution" is any sort of processing that doesn't
 *  involve input or output.  All it needs is time; the
 *  system does not wait for capacity before calling this.
 *
 * @param 	data		opaque application data
 * @param 	srv		module handle
 * @param 	session_data	application's per-session data
 * @param 	request_data	application's per-request data
 * @param	deadline 	run at most until this time,
 *				in milliseconds since 1970.
 *
 * @return 	0 on success, a nonzero error code on (fatal) error.
 */
typedef int srv_app_request_run(void *data, srv_handle *srv, void *session_data,
                                void *request_data, srv_msclock_t deadline);

/**
 * @brief Method: Format output for a request.
 *
 *  The formatting process also includes computing the result
 *  of a request.
 *
 * @param 	data		opaque application data
 * @param 	srv		module handle
 * @param 	session_data	application's per-session data
 * @param 	request_data	application's per-request data
 * @param 	s		in/out: space available for output
 * @param 	e		end of available space
 * @param	deadline 	run at most until this time,
 *				in milliseconds since 1970.
 *
 * @return 0 on success, a nonzero error code on error.
 * @return SRV_ERR_MORE if there's more output waiting, but
 * 			the call ran out of space.
 */
typedef int srv_app_request_output(void *data, srv_handle *srv,
                                   void *session_data, void *request_data,
                                   char **s, char *e, srv_msclock_t deadline);

/**
 * @brief Method: Handle timeouts on a suspended session
 *
 * @param 	data		opaque application data
 * @param 	srv		module handle
 * @param 	session_data	application's per-session data
 * @param 	request_data	application's per-request data
 *
 * @return 0 on success, a nonzero error code on error.
 */
typedef int srv_app_request_sleep(void *data, srv_handle *srv,
                                  unsigned long long micros_now,
                                  void *session_data, void *request_data);

/**
 * @brief Method: Free resources allocated for ar equest.
 *
 *  Don't free the request itself - the server library handles that.
 *
 * @param 	data		opaque application data
 * @param 	srv		module handle
 * @param 	session_data	application's per-session data
 * @param 	request_data	application's per-request data
 *
 * @return 0 on success, a nonzero error code on error.
 */
typedef void srv_app_request_finish(void *data, srv_handle *srv,
                                    void *session_data, void *request_data);

/**
 * @brief Method: Begin dispatching events for an application.
 *
 *  This function, if non-NULL, is called before every round of
 *  events are centrally dispatched to the sessions of an application.
 *
 * @param 	data		opaque application data
 * @param 	srv		module handle
 */
typedef void srv_app_pre_dispatch(void *data, srv_handle *srv);

/**
 * @brief Per-application callback structure.
 */
typedef struct srv_application {
  /**
   * @brief Application's error log name.
   */
  char const *app_name;

  /**
   * @brief Version string of the application, e.g. "0.1"
   *  This is printed as part of the usage command.
   */
  char const *app_version;

  /**
   * @brief Build version reference - use -m to dump it.
   */
  srv_build_version_reference const *app_build_version_reference;

  /**
   * @brief Method: adjust pid of the monitoring (parent) process.
   */
  srv_app_spawn *app_spawn;

  /**
   * @brief Method: start accepting incoming connections
   */
  srv_app_startup *app_startup;

  /**
   * @brief Method: shut down the entire application.
   */
  srv_app_shutdown *app_shutdown;

  /**
   * @brief Method: shut down a session.
   */
  srv_app_session_shutdown *app_session_shutdown;

  /**
   * @brief Method: initialize a session.
   */
  srv_app_session_initialize *app_session_initialize;

  /**
   * @brief Method: prompt within an interactive session.
   */
  srv_app_session_interactive_prompt *app_session_interactive_prompt;

  /**
   * @brief Method: begin a request.
   */
  srv_app_request_initialize *app_request_initialize;

  /**
   * @brief Method: interpret input to a request.
   */
  srv_app_request_input *app_request_input;

  /**
   * @brief Method: execute a request.
   */
  srv_app_request_run *app_request_run;

  /**
   * @brief Method: format results of executing a request.
   */
  srv_app_request_output *app_request_output;

  /**
   * @brief Method: sleep while a session is suspended.
   */
  srv_app_request_sleep *app_request_sleep;

  /**
   * @brief Method: free application-specific resources for a request.
   */
  srv_app_request_finish *app_request_finish;

  /**
   * @brief Method: begin dispatching events.
   */
  srv_app_pre_dispatch *app_pre_dispatch;

  /**
   * @brief Method: we have finished starting up, and are happy
   */
  srv_app_startup_complete *app_startup_complete;

  /**
   * @brief Method: we have forked a new process, initialize it
   */
  srv_app_smp_startup *app_smp_startup;

  /**
   * @brief Method: a forked process has died, abort, retry or fail
   */
  srv_app_smp_finish *app_smp_finish;

  /**
   * @brief File name of the default location of the server's
   *  "PID file", a file used for logging and self-termination.
   */
  char const *app_default_pid_file;

  /**
   * @brief Application's default port number.
   */
  unsigned short app_default_port;

  /**
   * @brief File name of the application's default configuration file.
   */
  char const *app_default_conf_file;

  /**
   * @brief Array of application-specific command line options.
   *	Terminated by an option with a NULL name.
   */
  srv_option const *app_options;

  /**
   * @brief Size of application's configuration data structure.
   */
  size_t app_config_size;

  /**
   * @brief Array of application-specific parameter structures,
   * 	terminated by a parameter with a NULL name.
   */
  srv_config_parameter const *app_config_parameters;

  /**
   * @brief Size of the application's per-session state.
   *  (The structure starts with an embedded srv_session.)
   */
  size_t app_session_size;

  /**
   * @brief Size of the application's request structure.
   *  (The structure starts with an embedded srv_request.)
   */
  size_t app_request_size;

  /**
   * @brief List of application-specific logging facilities.
   */
  cl_facility const *app_facilities;

} srv_application;

struct srv_buffer;

/**
 * @brief Buffer for incoming and outgoing data.
 */
typedef struct srv_buffer {
  /**
   * @brief Next buffer in the chain.
   */
  struct srv_buffer *b_next;

  /**
   * @brief Heap the buffer is allocated on.
   */
  cm_handle *b_cm;

  /**
   * @brief Log through here.
   */
  cl_handle *b_cl;

  /**
   * @brief Number of references to the buffer.
   *
   *  This is important - multiple requests can share one
   *  buffer, and the whole buffer only gets freed when the
   *  last request that had a reference to it gets freed.
   */
  size_t b_refcount;

  /**
   * @brief Beginning of buffered data.
   */
  char *b_s;

  /**
   * @brief Number of valid bytes in the buffered data.
   *  	On input, data is read into the space between
   *	b_n and b_m.  On output, data is formatted into
   *	the space between b_n and b_m.
   */
  size_t b_n;

  /**
   * @brief Number of allocated bytes in the buffered data.
   */
  size_t b_m;

  /**
   * @brief Pointer into the buffered data.
   *
   *	On input, it marks the boundary between data that has
   *	been parsed and data that has only been copied.
   *	On output, it marks the boundary between data that has
   *	been written to the outgoing connection and data
   *	still waiting to be written.
   */
  size_t b_i;

  /**
   * @brief Opaque data for the pre-write callback.
   */
  void *b_pre_callback_data;

  /**
   * @brief Pre-write callback.  Invoked just before writing
   * 	data in the buffer to an outside connection.
   *
   *	Graphd uses this to guarantee that database
   *	state has been flushed to disk when an "OK" reply
   *	is sent to a client.
   */
  int (*b_pre_callback)(void *, bool, bool *);

} srv_buffer;

/**
 * @brief Tail queue of buffers.
 */
typedef struct srv_buffer_queue {
  /**
   * @brief First buffer in the queue, or NULL if the queue is empty.
   */
  srv_buffer *q_head;

  /**
   * @brief Append new elements here.
   *  If the queue is empty, this points to q_head.
   */
  srv_buffer **q_tail;

  /**
   * @brief Number of entries in the queue.
   */
  size_t q_n;

} srv_buffer_queue;

/**
 * @brief Generic server request - common head of all application requests.
 */
typedef struct srv_request {
  /**
   * @brief Request's session.
   */
  srv_session *req_session;

  /**
   * @brief Memory heap for the request.  Memory allocated here
   *	is guaranteed to be free'd when the request is freed.
   */
  cm_handle *req_cm;

  /**
   * @brief Next request in the session's request chain.
   */
  struct srv_request *req_next;

  /**
   * @brief First buffer that contains input text for the request.
   */
  srv_buffer *req_first;

  /**
   * @brief Last buffer that contains text for the request.
   */
  srv_buffer *req_last;

  /**
   * @brief beginning of the request's text.
   *
   *  The request's full text runs from req_first_offset in
   *  req_first to req_last_n in req_last.
   *
   *  This is stored here for documentation while the request
   *  is running; during the actual parse, the srv_buffered_connection
   *  keeps track of the offsets.
   */
  size_t req_first_offset;

  /**
   * @brief End of the request's text.
   */
  size_t req_last_n;

  /**
   * @brief Request's unique ID.
   *
   *   Pulled from srv_id at time of creation.
   */
  srv_unique_id req_id;

  /**
   * @brief Request's displayname/ID; may be NULL.
   */
  char const *req_display_id;

  /**
  * @brief  Log this request's output.
  * 	True by default, can be cleared.
  */
  unsigned int req_log_output : 1;

  /**
   * @brief Number of references held on this request.
   */
  size_t req_refcount;

  /**
   * @brief Number of timeslices "spent" working on this
   *	request
   */
  int req_n_timeslices;

  /**
   * @brief req_ready flag
   *
   *   The "ready" flag holds at most one of
   *
   *	(1 << SRV_INPUT)
   *	(1 << SRV_OUTPUT)
   *	(1 << SRV_RUN)
   *
   *   It indicates that the request is ready to
   *   actually do something useful, given CPU time.
   *   Incremental calls to app->app_request_run()
   *   must eventually result in the ready flag being
   *   either
   *
   *    - cleared (because the request has suspended
   *      itself while waiting for something else)
   *
   *    - or changed (e.g. to 1 << SRV_OUTPUT
   *	after 1 << SRV_DONE).
   *
   *   A request must not be marked both ready and done
   *   for the same thing.
   */
  unsigned int req_ready;

  /*
   *  @brief req_done flag.
   *
   *   The "done" flag holds between zero and three of
   *
   *	(1 << SRV_INPUT)
   *	(1 << SRV_OUTPUT)
   *	(1 << SRV_RUN)
   *
   *   If a bit is clear, the request expresses intention
   *   to at some time become ready to perform the
   *   specified operation.
   *
   *   If a bit is set, the request guarantees that it will
   *   never again parse input, never write output, or never
   *   merely consume CPU time, respectively.
   *
   *   A bit in req_done can only be set, not cleared.
   *
   *   If the whole request is done - all three bits are
   *   set - it can be removed from its session queue.
   */
  unsigned int req_done;

  /* @brief Queue of requests that block on buffer space.
   * 	When buffers become available, they are re-woken
   *	as "ready for output" or "ready for input",
   *	depending on req_buffer_waiting's value of
   *	(1 << SRV_INPUT) or (1 << SRV_OUTPUT).
   */
  struct srv_request *req_buffer_waiting_next;
  struct srv_request *req_buffer_waiting_prev;
  unsigned int req_buffer_waiting;

  /* @brief This request depends on me.  So, if it (or
   *	one of its ancestors) has priority, and I want
   *	priority, I get to take it from it!
   */
  struct srv_request *req_dependent;

} srv_request;

/**
 * @brief A buffered connection - common abstraction mechanism
 *  	for application connections.
 */
struct srv_buffered_connection {
  /**
   * @brief Log through this.
   */
  cl_handle *bc_cl;

/**
 * @brief Error status of the connection: write failed.  Don't
 * 	write any more output.
 */
#define SRV_BCERR_WRITE 0x01

/**
 * @brief Error status of the connection: read failed.  Don't
 * 	read any more input.
 */
#define SRV_BCERR_READ 0x02

/**
 * @brief Error status of the connection: something is wrong with
 * 	the connection itself.
 */
#define SRV_BCERR_SOCKET (SRV_BCERR_WRITE | SRV_BCERR_READ)

  /**
   * @brief Error status of the connection -- which parts have failed?
   */
  unsigned int bc_error : 2;

  /**
   * @brief The session I/O processing wants to run.
   * 	The interface code causes itself to run again soon if this is
   *  	set, typically by sending itself an ES_APPLICATION event.
   */
  unsigned int bc_processing : 1;

  /**
   * @brief If set, the OS indicates that reading would not block.
   */
  unsigned int bc_data_waiting_to_be_read : 1;

  /**
   * @brief If set, the OS indicates that writing would not
   * 	block (i.e., poll() set this bit for this file descriptor)
   */
  unsigned int bc_write_capacity_available : 1;

  /**
   * @brief system errno of most recent failed access, for
   * 	informational purposes only.
   */
  int bc_errno;

  /**
   * @brief If set, there is room in the input buffers that
   * 	input data could be read into.
   */
  unsigned int bc_input_buffer_capacity_available : 1;

  /**
   * @brief If set, there is input in the input buffers that
   * 	hasn't yet been parsed by the application.
   */
  unsigned int bc_input_waiting_to_be_parsed : 1;

  /**
   * @brief If set, there is space in the output buffers that
   *  	an application could write to.
   */
  unsigned int bc_output_buffer_capacity_available : 1;

  /**
   * @brief If set, there is data in the output buffers.
   */
  unsigned int bc_output_waiting_to_be_written : 1;

  /**
   * @brief If set, the connection has priority.  A connection that
   *  	has priority wins when competing with other connections
   * 	over buffers.
   */
  unsigned int bc_have_priority : 1;

  /**
   * @brief Buffers used to write output.
   */
  srv_buffer_queue bc_output;

  /**
   * @brief Buffers used to read input.  Buffers stay valid
   * 	at least until their requests have completed.
   */
  srv_buffer_queue bc_input;

  /**
   * @brief Pool of buffers for use in this connection
   *  (and perhaps others).
   */
  srv_buffer_pool *bc_pool;

  /**
   * @brief Statistics: how many bytes have been read?
   */
  unsigned long long bc_total_bytes_in;

  /**
   * @brief Statistics: how many bytes have been written?
   */
  unsigned long long bc_total_bytes_out;
};

/**
 * @brief Generic session; at the head of all server application sessions.
 */
struct srv_session {
  /**
   * @brief Generic module handle.
   */
  srv_handle *ses_srv;

  /**
   * @brief Previous session in the chain of all sessions.
   */
  struct srv_session *ses_prev;

  /**
   * @brief Next session in the chain of all sessions.
   */
  struct srv_session *ses_next;

  /**
   * @brief Session-local heap.  This isn't used much -- most
   * 	things are request-local.
   */
  cm_handle *ses_cm;

  /**
   * @brief Session's buffered connection.
   */
  srv_buffered_connection ses_bc;

  /**
   * @brief Tail queue of requests, to be executed in order.
   */
  srv_request *ses_request_head;

  /**
   * @brief Tail of a tail-queue of requests.
   */
  srv_request **ses_request_tail;

  /**
   * @brief The first request in ses_request_head[..]
   * 	that doesn't have req_input_done set.  If we
   *  	read new input, it's for this one.
   */
  srv_request **ses_request_input;

  /**
   * @brief The first request in ses_request_head[..]
   * 	that doesn't have req_output_done set.  If we
   * 	write output, it's for this one.
   */
  srv_request **ses_request_output;

  /**
   * @brief Session's display name, for debugging.
   */
  char const *ses_displayname;

  /**
   * @brief Session's netlog header with client and
   * 	server IP and port.
   */
  char const *ses_netlog_header;

  /**
   * @brief Session's interface type; NULL when it no
   * 	longer has an interface.
   */
  srv_session_interface_type const *ses_interface_type;

  /**
   * @brief Opaque per-interface/per-session data, managed by
   * 	the interface type.
   */
  void *ses_interface_data;

  /**
   * @brief Session's interface name ("which port did
   *  	this client connect to"), managed by the
   * 	interface type.
   */
  char const *ses_interface_name;

  /**
   * @brief Reference count, protects session from being
   * 	freed in mid-execution.
   */
  size_t ses_refcount;

  /**
   * @brief Number of milliseconds this session gets to
   * 	run before giving up the CPU.
   */
  srv_msclock_t ses_timeslice;

  /**
   * @brief What kind of input/output/run is the session
   * 	 waiting for?  Values: binary or of
   *	1 << SRV_RUN, 1 << SRV_OUTPUT, 1 << SRV_INPUT.
   */
  unsigned int ses_want;

  /**
   * @brief Statistics: Time spent computing the answers to requests.
   *
   * 	The per-request costs are calculated by computing
   *	the difference of session costs before and after
   *	the request executes.
   */
  unsigned long long ses_requests_millis;
  struct timeval ses_requests_millis_before;

  /**
   * @brief Statistics: numbers of requests parsed.
   */
  unsigned long long ses_requests_in;

  /**
   * @brief Statistics: numbers of requests answered.
   *
   *  The difference between this number and ses_requests_in
   *  is the size of the request queue for a session.  (Bigger
   *  means more backlogged.)
   */
  unsigned long long ses_requests_out;

  /**
   * @brief Statistics: numbers of requests made.
   */
  unsigned long long ses_requests_made;

  /**
   * @brief Statistics: numbers of replies received.
   */
  unsigned long long ses_replies_received;

  /**
   * @brief A unique session ID.
   *
   *	For convenience' sake, we just use the same number space
   *	that is already handing out the request IDs - as if the
   *	creation of a session were an invisible request.
   */
  srv_unique_id ses_id;

  /* @brief Is there an asynchronous connect pending?
   *
   *	This is set when creating an outbound session
   *	and cleared after tcp_run has verified a successful
   *	connection.
   */
  unsigned int ses_pending_connect : 1;

  /* @brief Is this a retry?
   *
   *  	Is this session retrying the connect, rather than
   * 	attempting it the first?  If yes, it shouldn't be
   *	surprised if the connection attempt fails.
   */
  unsigned int ses_retry_connect : 1;

  /**
   * @brief Is this a server session?
   *
   * 	A server session creates empty requests if there are
   *	none to read into, and not enough requests waiting already.
   */
  unsigned int ses_server : 1;

  /**
   * @brief Has anything changed with this session?
   * 	Set when the session gets events or runs requests.
   *	Causes srv_session_process_events() to get called,
   *	and ses_needs_interface_update to get set.
   */
  unsigned int ses_changed : 1;

  /* @brief Has anything changed about what events the session
   *  	is interested in?
   */
  unsigned int ses_needs_interface_update : 1;
};

/**
 * @brief 	Set a command line option.
 * @param data		opaque application data pointer
 * @param srv		server module handle
 * @param cm		allocate data here
 * @param opt		command-line option (from getopt)
 * @param arg		command-line option argument (getopt's optarg)
 *
 * @return 0 on success, a nonzero error code on error.
 */
typedef int srv_option_set(void *data, srv_handle *srv, cm_handle *cm, int opt,
                           char const *arg);

/**
 * @brief 	Override configuration data with a command line option.
 *
 *	This method is executed after the config file has been
 *	parsed, but before it is instantiated.
 *
 * @param data		opaque application data pointer
 * @param srv		server module handle
 * @param config_data	opaque application configuration data
 * @param srv_config	server configuration data
 *
 * @return 0 on success, a nonzero error code on error.
 */
typedef int srv_option_configure(void *data, srv_handle *srv, void *config_data,
                                 srv_config *srv_config);

/**
 * @brief Application option.
 *
 *  A libsrv application declares zero or more of these to extend the
 *  command line options of a server with its application-specific options.
 *
 *  Available letters:
 * 	a   b   d   e   j   k   n   o   r   s   w
 */
struct srv_option {
  /**
   * @brief Name of the option, a single character, optionally
   *	followed by ":" if the option has an argument.
   *	This is embedded into a getopt argument string.
   */
  char const *option_name;

  /**
   * @brief Description of the option, printed as part of the
   * 	server executable's usage.
   */
  char const *option_description;

  /**
   * @brief Method: parse the option, given the command line arguments.
   */
  srv_option_set *option_set;

  /**
   * @brief Method: override configuration with command-line option.
   */
  srv_option_configure *option_configure;

  /**
   * @brief NULL or static string to print when encountering
   * 	this option (don't start the server).
   */
  char const *option_static;
};

/**
 * @brief Parse an application parameter from the configuration file.
 *
 *  This is a method supplied by the application for the generic
 *  libsrv configuration framework.
 *
 *  When the generic libsrv configuration parser encounters the keyword
 *  the application has reserved for itself, the generic parser passes
 *  control to the application.
 *
 *  The application parses the text following its keyword,
 *  adjusts the beginning of the unread text to point after the
 *  consumed data, and fills in the application's configuration data
 *  to represent the parsed text.
 *
 * @param data			opaque application module handle
 * @param srv			libsrv module handle
 * @param config_data		opaque application per-configuration handle
 * @param srv_config_data	libsrv per-configuration handle
 * @param s			In/Out: Beginning of unparsed configuration
 *				file text.
 * @param e			End of unparsed configuration file text
 *
 * @return 0 on success, a nonzero error code on error.
 */
typedef int srv_config_parameter_read(void *data, srv_handle *srv,
                                      void *config_data,
                                      srv_config *srv_config_data, char **s,
                                      char const *e);

/**
 * @brief Initiate use of a configured resource, parent process.
 *
 *  This is called once for its application-specific resource
 *  after a complete configuration file has been parsed,
 *  and after those configuration settings have been overridden by
 *  the command line.
 *
 * @param data			opaque application module handle
 * @param srv			libsrv module handle
 * @param config_data		opaque application per-configuration handle
 * @param srv_config_data	libsrv per-configuration handle
 *
 * @return 0 on success, a nonzero error code on error.
 */
typedef int srv_config_parameter_open(void *data, srv_handle *srv,
                                      void *config_data,
                                      srv_config *srv_config_data);
/**
 * @brief Initiate use of a configured resource, worker process
 *
 *  This is called just before the worker process is starting to
 *  accept calls.  This is not a good time to check user configuration --
 *  if the process is running in TCP mode, it's in the background by now.
 *
 * @param data			opaque application module handle
 * @param srv			libsrv module handle
 * @param config_data		opaque application per-configuration handle
 * @param srv_config_data	libsrv per-configuration handle
 *
 * @return 0 on success, a nonzero error code on error.
 */
typedef int srv_config_parameter_run(void *data, srv_handle *srv,
                                     void *config_data,
                                     srv_config *srv_config_data);

/**
 * @brief A configuration parameter.
 *
 *  Something with a name that can be set in a configuration file.
 */
struct srv_config_parameter {
  /**
   * @brief The parameter's name; case-insensitive.
   */
  char const *config_name;

  /**
   * @brief Read method, called while parsing the configuration file.
   */
  srv_config_parameter_read *config_read;

  /**
   * @brief Open method, called after optionally overriding
   *  with command line arguments.
   */
  srv_config_parameter_open *config_open;

  /**
   * @brief Run method, called in the worker thread, before
   *	opening the outside interface.
   */
  srv_config_parameter_run *config_run;
};

/**
 * @brief Minimum buffer size that should be available for wirting
 * 	before calling any formatting code.
 *
 *  That doesn't mean that formatting code can rely on this much
 *  space to be available - this still needs checking.
 */
#define SRV_MIN_BUFFER_SIZE 128
struct srv_config;

/* srv-address.c */

void srv_address_ip_port(char const *s, char const **_ip_s, char const **_ip_e,
                         char const **_port_s, char const **_port_e);

int srv_address_read_token(srv_address **_addr_inout, cl_handle *_cl,
                           char const *_tok_s, char const *_tok_e, char **_s,
                           char const *_e);

int srv_address_create_url(cm_handle *cm, cl_handle *cl, char const *url_s,
                           char const *url_e, srv_address **sa);

int srv_address_create_host_port(cm_handle *cm, cl_handle *cl,
                                 char const *host_s, char const *host_e,
                                 char const *port_s, char const *port_e,
                                 srv_address **sa_out);

void srv_address_destroy(srv_address *);

char *srv_address_fully_qualified_domainname(cm_handle *);

int srv_address_copy(cm_handle *cm, cl_handle *cl, srv_address *from,
                     srv_address **to);

/* srv-config.c */

bool srv_config_is_name(char const *lit, char const *s, char const *e);

int srv_config_get_token(char **_s, char const *_e, char const **_tok_s,
                         char const **_tok_e);

void srv_config_link(srv_config *);
void srv_config_unlink(srv_config *);
int srv_config_read_number(srv_config *_cf, cl_handle *_cl, char const *_what,
                           char **_s, char const *_e, unsigned long long *_out);
char *srv_config_read_string(srv_config *_cf, cl_handle *_cl, char const *_what,
                             char **_s, char const *_e);
int srv_config_read_boolean(srv_config *_cf, cl_handle *_cl, char **_s,
                            char const *_e, bool *_out);
char const *srv_config_file_name(srv_config const *);
int srv_config_line_number(srv_config const *, char const *);
cm_handle *srv_config_mem(srv_config *);

/* srv-delay.c */

typedef void srv_delay_callback_func(void *_data,
                                     es_idle_callback_timed_out _mode);

srv_delay *srv_delay_create(srv_handle *_srv, unsigned long _min_seconds,
                            unsigned long _max_seconds,
                            srv_delay_callback_func *_callback,
                            void *_callback_data, char const *displayname);

void srv_delay_destroy(srv_delay *del);

/* srv-epitaph.c */

int srv_epitaph_print(srv_handle *_srv, int _exit_code, char const *_fmt, ...);

/* srv-idle.c */

/**
 * @brief Idle callback function
 *
 * @param 	data		opaque data pointer
 * @param 	timed_out 	true if the server hasn't fallen idle
 *				within number of seconds specified with
 *				srv_idle_set().
 */
int srv_idle_set(srv_handle *_srv, srv_idle_context *_context,
                 unsigned long _seconds);

int srv_idle_delete(srv_handle *_srv, srv_idle_context *_context);

bool srv_idle_test(srv_handle *_srv, srv_idle_context const *_ic);

void srv_idle_initialize(srv_handle *_srv, srv_idle_context *_con,
                         srv_idle_callback_func *_callback);

/* srv-interface.c */

char const *srv_interface_to_string(srv_handle *_srv, char *_buf, size_t _size);

int srv_interface_connect(srv_handle *_srv, char const *_destination,
                          srv_session **_ses_out);

int srv_interface_add_and_listen(srv_handle *srv, char const *address);

void srv_interface_balance(srv_handle *srv, bool activate);
/* srv-main.c */

char const *srv_xstrerror(int);
char const *srv_strerror(int);

cl_loglevel_configuration const *srv_loglevel_configuration(srv_handle *);
cl_handle *srv_log(srv_handle *);
cl_handle *srv_netlog(srv_handle *);
cm_handle *srv_mem(srv_handle *);
es_handle *srv_events(srv_handle *);

unsigned long long srv_smp_processes(srv_handle *srv);

void srv_set_smp_processes(srv_handle *srv, unsigned long long processes);

char const *srv_program_name(srv_handle *h);
int srv_main(int _argc, char **_argv, void *_data,
             srv_application const *_settings);

int srv_netlog_set_filename(srv_handle *, char const *);
void srv_run_startup_complete_callback(srv_handle *);
void srv_open_interfaces(srv_handle *srv);

void srv_finish(srv_handle *srv, bool child);
void srv_shutdown_now(srv_handle *srv);

void srv_shutdown_now_loc(srv_handle *srv, char const *file, int line);

#define srv_shutdown_now(srv) srv_shutdown_now_loc(srv, __FILE__, __LINE__)

void srv_startup_now_complete(srv_handle *);

void srv_cleanup_and_finish(srv_handle *);

int srv_child_smp(srv_handle *srv);

bool srv_is_shutting_down(srv_handle *srv);

int srv_log_set_filename(srv_handle *, char const *);

int srv_netlog_set_level(srv_handle *_srv,
                         cl_loglevel_configuration const *_clc);

int srv_log_set_level(srv_handle *_srv, cl_loglevel_configuration const *_clc);

void srv_set_max_restart_count(srv_handle *srv, int count);

/* Do we want core files?
*/
void srv_set_want_core(srv_handle *srv, bool want_core);
void srv_set_diary(srv_handle *srv, cl_handle *diary);
bool srv_want_core(srv_handle *srv);

/* srv-memory-list.c */

int srv_memory_list(srv_handle *_srv, cm_log_callback *_callback, void *_data);

/* srv-msclock.c */

srv_msclock_t srv_msclock(srv_handle *srv);

/* srv-request.c */

/**
 * @brief Try to acquire priority for a request.
 *
 *	Requests that have priority get more buffers than those who
 *	don't.  If not enough memory is available, this request can
 *	be denied.
 *
 * @param req	request that wants priority.
 * @return true if the request now has priority, false if not.
 */
#define srv_request_priority_get(ses) \
  srv_request_priority_get_loc((ses), __FILE__, __LINE__)
bool srv_request_priority_get_loc(srv_request *_ses, char const *_file,
                                  int _line);

/**
 * @brief Give up priority for a request.
 *
 *	Requests give up priority when they wait for something.
 *
 * @param req	request that wants priority.
 */
#define srv_request_priority_release(ses) \
  srv_request_priority_release_loc((ses), __FILE__, __LINE__)
void srv_request_priority_release_loc(srv_request *_ses, char const *_file,
                                      int _line);

void srv_request_depend(srv_request *dep, srv_request *req);
bool srv_request_is_complete(srv_request const *req);
bool srv_request_error(srv_request const *req);

void srv_request_run_ready(srv_request *req);
void srv_request_output_ready(srv_request *req);
void srv_request_input_ready(srv_request *req);
void srv_request_run_done(srv_request *req);
void srv_request_input_done(srv_request *req);
void srv_request_output_done(srv_request *req);

#define srv_request_complete(req) \
  srv_request_complete_loc(req, __FILE__, __LINE__)

void srv_request_complete_loc(srv_request *_req, char const *_file, int _line);

srv_request *srv_request_create_outgoing(srv_session *ses);
srv_request *srv_request_create_incoming(srv_session *);

void srv_request_ready(srv_request *req, unsigned int flags);
void srv_request_sent(srv_request *req);
void srv_request_arrived(srv_request *);

void srv_request_reply_sent(srv_request *req);
void srv_request_reply_received(srv_request *req);

void srv_request_unlink(srv_request *);
void srv_request_link(srv_request *);
srv_request *srv_request_create_asynchronous(srv_session *);
int srv_request_text_next(srv_request *_req, char const **_s_out,
                          size_t *_n_out, void **_state);

char const *srv_request_to_string(srv_request const *req, char *buf,
                                  size_t size);

void srv_request_suspend(srv_request *req);

/* srv-session.c */

#define srv_session_change(a, b, c) \
  srv_session_change_loc(a, b, c, __FILE__, __LINE__)

void srv_session_change_loc(srv_session *ses, bool value, char const *what,
                            char const *file, int line);

void srv_session_set_server(srv_session *ses, bool new_value);
char const *srv_session_to_string(srv_session const *ses, char *buf,
                                  size_t size);
char const *srv_session_chain_name(srv_session const *ses);
size_t srv_session_n_requests(srv_session const *ses);
void srv_session_input_commit(srv_session *, char const *);

void srv_session_abort(srv_session *);
void srv_session_quit(srv_session *);

void srv_session_link_loc(srv_session *, char const *, int);

/**
 * @brief Add a link to a session
 *
 *	This mechanism allows sessions to "destroy themselves" without
 * 	having their surrounding event loop collapse on them.
 *
 * @param ses	session we'd like to have a reference to.
 */
#define srv_session_link(ses) srv_session_link_loc((ses), __FILE__, __LINE__)

bool srv_session_unlink_loc(srv_session *, char const *, int);
/**
 * @brief Remove a link from a session
 *
 *	Only after the last link to a session is removed does the
 * 	session actually get freed.
 *
 * @param ses	session to unlink.
 */
#define srv_session_unlink(ses) \
  srv_session_unlink_loc((ses), __FILE__, __LINE__)

void srv_session_schedule(srv_session *);
void srv_session_suspend(srv_session *);
void srv_session_resume(srv_session *);
bool srv_any_sessions_ready_for(srv_handle *srv, int flag);

void *srv_session_allocate_pre_hook(srv_session *ses,
                                    srv_pre_callback *callback,
                                    size_t callback_data_size);

/**
 * @brief List a session.
 *
 *  This callback is invoked by srv_session_list() once for each
 *  session.
 *
 * @param closure	caller-supplied opaque data pointer
 * @param ses		session to list
 *
 * @return 0 on success, a nonzero error code on error.
 */
typedef int srv_session_list_callback(void *closure, srv_session *ses);

int srv_session_list(srv_handle *_srv, srv_session_list_callback *_callback,
                     void *_callback_data);

/* srv-shared.c */

void srv_shared_set_restart(srv_handle *srv, bool can_restart);

bool srv_shared_can_restart(srv_handle const *srv);

void srv_shared_set_safe(srv_handle *srv, bool is_safe);

bool srv_shared_is_safe(srv_handle const *srv);

void srv_shared_set_crashed(srv_handle *srv);

bool srv_shared_has_crashed(srv_handle const *srv);

void srv_shared_set_time(srv_handle const *srv, time_t start_time);

time_t srv_shared_get_time(srv_handle const *srv);

void srv_shared_connection_activate_index(srv_handle *srv, int index,
                                          bool valid);

void srv_shared_connection_increment(srv_handle *srv, int index);

void srv_shared_connection_decrement(srv_handle *srv, int index);

int srv_shared_get_connection_count(srv_handle *srv, int index);

/* srv-settle.c */

void srv_settle_error(srv_handle *, char const *, ...);
void srv_settle_ok(srv_handle *);
int srv_settle_wait(srv_handle *, char **);
void srv_settle_delay(srv_handle *srv);
void srv_settle_close(srv_handle *srv);

/* srv-timeout.c */

void srv_session_set_timeout(srv_session *ses, srv_timeout *timeout);
srv_timeout *srv_timeout_create(srv_handle *srv, unsigned long seconds);

#endif /* SRV_H */
