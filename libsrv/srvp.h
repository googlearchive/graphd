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
#ifndef SRVP_H
#define SRVP_H

#include <sys/types.h> /* uid_t, gid_t */
#include <unistd.h>
#include <stdlib.h>  /* size_t */
#include <stdbool.h> /* bool */

#include "libcm/cm.h"
#include "libcl/cl.h"
#include "libes/es.h"
#include "srv.h"

#define SRV_PIDFILE_DEFAULT "/var/run/srv.pid"
#define SRV_MIN_BUFFER_SIZE 128
#define SRV_MAX_PROCESS_COUNT 256

extern cm_handle *srv_trace_me;

#define CHECK()      \
  if (!srv_trace_me) \
    ;                \
  else               \
    cm_trace_check(srv_trace_me);

struct srv_config;
struct srv_interface_type;
typedef struct srv_interface_type srv_interface_type;

struct srv_buffer_pool {
  cl_handle *pool_cl;
  cm_handle *pool_cm;

  unsigned long long pool_min;
  unsigned long long pool_max;
  unsigned long long pool_available;
  size_t pool_size;

  srv_buffer_queue pool_q;
  enum {
    SRV_POOL_REPORT_LOW,
    SRV_POOL_REPORT_OK,
    SRV_POOL_REPORT_FULL
  } pool_report;
};

typedef struct srv_interface_config {
  srv_interface_type const *icf_type;
  struct srv_interface_config *icf_next;
  struct srv_config *icf_config;
  char const *icf_address;

} srv_interface_config;

typedef struct srv_interface {
  struct srv_interface *i_next;
  srv_interface_config *i_config;
  void *i_data;

} srv_interface;

struct srv_config {
  cm_handle *cf_cm;

  /* What's the filename, if any, it is based on?
   */
  char const *cf_file;

  /* If this drops to 0, we can free this configuration.
   */
  size_t cf_link;

  /* Log settings.
   */
  cl_loglevel_configuration cf_log_level;
  int cf_log_facility;
  char *cf_log_ident;
  char *cf_log_file;
  cl_flush_policy cf_log_flush;

  char *cf_netlog_file;
  cl_loglevel_configuration cf_netlog_level;
  cl_flush_policy cf_netlog_flush;

  /**
   * Do we want a core file?
   */
  bool cf_want_core;

  /**
   * Timeslices
   */
  unsigned long long cf_short_timeslice_ms;
  unsigned long long cf_long_timeslice_ms;

  /* Bind to this CPU
   */
  unsigned long long cf_cpu;

  /* Fork this many processes
   */
  unsigned long long cf_processes;

  /**
   *  How many seconds do we wait during shutdown
   *  before we just up?
   */
  unsigned long cf_shutdown_delay;

  /* Default buffer pool settings.
   */
  unsigned long long cf_pool_min;
  unsigned long long cf_pool_max;
  size_t cf_pool_page_size;

  /* Run as this user/group.
   */
  uid_t cf_user_id;
  gid_t cf_group_id;

  /* Unless interactive, lock against this pid file.
   */
  char *cf_pid_file;

  /* Open these interfaces.
   */
  srv_interface_config *cf_interface_head, **cf_interface_tail;

  /* Opaque application data
   */
  void *cf_app_data;
};

/**
 * Information shared between parent and child process.
 */
typedef struct {
  /* We can safely restart the child process after a crash
   */
  volatile bool ssh_safe;

  /* Wether or not we want to restart the child process after a crash
   */
  volatile bool ssh_restart;

  /* Have we crashed before?
   */
  volatile bool ssh_crashed;

  /* Child (re)start time
   */
  volatile time_t ssh_time;

  /* Load balancing across processes
   */
  volatile int ssh_connections[SRV_MAX_PROCESS_COUNT];

} srv_shared;

struct srv_handle {
  /* Must be first; we use it to wait for outside
  *  notifications in a server process.
   */
  es_descriptor srv_ed;

  srv_config *srv_config;
  cm_handle *srv_cm;
  cl_handle *srv_cl;

  /* Stuff shared between parent and child
   */
  srv_shared *srv_shared;

  es_handle *srv_es;
  es_idle_callback *srv_es_idle_head;
  srv_idle_callback_func *srv_es_idle_callback;
  void *srv_es_idle_callback_data;

  char const *srv_progname;
  srv_buffer_pool srv_pool;

  void *srv_app_data;
  srv_application const *srv_app;

  /*  Has shutdown been called for this application yet?
   */
  unsigned int srv_app_shutdown;

  /*  Tail queue of instantiated interfaces based on the
   *  runtime configuration.
   */
  srv_interface *srv_if_head;
  srv_interface **srv_if_tail;

  struct srv_request *srv_priority;

  /*  Should we stay in the foreground, rather than
   *  backgrounding ourselves?
   */
  unsigned int srv_foreground : 1;

  /*  Are we shutting down?
   */
  unsigned int srv_shutdown_begun : 1;

  /*  How many times should we restart before giving up?
      This is usually 3, but a libsrv app can change this.
      -1 to *always* restart.
  */

  int srv_max_restarts;

  /*  Is this the first call in an es-dispatch round?
   */
  unsigned int srv_first_es_dispatch : 1;

  /*  Is this a single-threaded interactive server?
   */
  unsigned int srv_interactive : 1;
  unsigned int srv_trace : 1;
  unsigned int srv_settle_application : 1;

  unsigned int srv_startup_is_complete : 1;
  unsigned int srv_startup_complete_has_run : 1;

  int srv_shutdown_pipe[2];
  int srv_settle_pipe[2];

  /*  Is any session waiting for buffers?
   */
  unsigned int srv_requests_waiting_for_buffers : 1;

  /*  A delay for polling inactive sessions for updates.
   */
  srv_delay *srv_sleep_delay;

  /*  The process ID of the monitoring process.  The monitoring
   *  process is the one whose pid is written in the pid file.
   *  It is not supposed to crash, ever, and doesn't get restarted;
   *  killing it serves as a shutdwon signal.
   */
  pid_t srv_pid;

  /*
   *  The index of the forked worker I currently am. -1 if SMP is
   *  disabled.
   */

  int srv_smp_index;

  /*
   * Am I an SMP manager?
   */

  unsigned int srv_smp_manager : 1;

  /**
   * @brief The unique ID of the next request to be created.
   */
  srv_unique_id srv_id;

  /**
   * @brief A diary to log events through.
   */
  cl_handle *srv_diary;

  /**
   * @brief NULL or the netlog file for the application.
   */
  cl_handle *srv_netlog;

  /**
   * @brief For documentation: NULL or request that's currently running.
   */
  srv_request *srv_request;

  /**
   * @brief Chain of all sessions in the system.
   */
  srv_session *srv_session_head;
  srv_session *srv_session_tail;

  /**
   * @brief Chain of requests that are waiting for a buffer.
   */
  srv_request *srv_buffer_waiting_head;
  srv_request *srv_buffer_waiting_tail;
  /**
   * @brief For documentation: NULL or session that's currently running.
   */
  srv_session *srv_session;
};

typedef struct srv_epitaph {
  pid_t epi_pid;
  int epi_exit;
  time_t epi_time;
  char const *epi_message;

} srv_epitaph;

struct srv_delay {
  es_descriptor del_ed;
  unsigned long del_magic;
  srv_handle *del_srv;
  srv_delay_callback_func *del_callback;
  void *del_callback_data;
  unsigned long del_min_seconds;
  unsigned long del_max_seconds;

  es_timeout *del_es_timeout;
  es_idle_callback *del_es_idle_callback;
};

#define SRV_DELAY_MAGIC 0x87654321

#define SRV_SET_DELAY(ptr) ((ptr)->del_magic = SRV_DELAY_MAGIC)
#define SRV_IS_DELAY(ptr) ((ptr)->del_magic == SRV_DELAY_MAGIC)

/* srv-config.c */

srv_config *srv_config_default(srv_handle *srv, cm_handle *, cl_handle *);
void srv_config_link(srv_config *);
void srv_config_unlink(srv_config *);
int srv_config_read(srv_handle *_srv, char const *_filename, cm_handle *_cm_env,
                    cl_handle *_cl, srv_config **_config_out);
int srv_config_read_number(srv_config *_cf, cl_handle *_cl, char const *_what,
                           char **_s, char const *_e, unsigned long long *_out);

char *srv_config_read_string(srv_config *_cf, cl_handle *_cl, char const *_what,
                             char **_s, char const *_e);

/* srv-interface.c */

srv_interface_config *srv_interface_config_alloc(srv_config *_srv,
                                                 cl_handle *_cl,
                                                 char const *_spec_s,
                                                 char const *_spec_e);

int srv_interface_config_add(srv_config *_srv, cl_handle *_cl,
                             char const *_address);

void srv_interface_config_chain_in(srv_config *_srv,
                                   srv_interface_config *_icf);

int srv_interface_config_read(srv_config *_cf, cl_handle *_cl,
                              srv_interface_config *_icf, char **_s,
                              char const *_e);

int srv_interface_create(srv_handle *_cf, srv_interface_config *_icf);

srv_interface_type const *srv_interface_type_match(char const *, char const *);

void srv_interface_shutdown(srv_handle *);

/* srv-shared.c */

int srv_shared_initialize(srv_handle *srv);

void srv_shared_finish(srv_handle *srv);

/* srv-unixid.c */

int srv_unixid_name_to_uid(char const *name, uid_t *uid);
int srv_unixid_name_to_gid(char const *name, gid_t *gid);
int srv_unixid_become(uid_t uid, gid_t gid);

/* srv-buffer.c */

srv_buffer *srv_buffer_alloc_loc(cm_handle *, cl_handle *, size_t, char const *,
                                 int);
#define srv_buffer_alloc(cm, cl, size) \
  srv_buffer_alloc_loc(cm, cl, size, __FILE__, __LINE__)

void srv_buffer_link_loc(srv_buffer *, char const *, int);
#define srv_buffer_link(buf) srv_buffer_link_loc(buf, __FILE__, __LINE__)

bool srv_buffer_unlink_loc(srv_buffer *, char const *, int);
#define srv_buffer_unlink(buf) srv_buffer_unlink_loc(buf, __FILE__, __LINE__)

void srv_buffer_reinitialize(srv_buffer *);
void srv_buffer_free(srv_buffer *);

void srv_buffer_queue_initialize(srv_buffer_queue *);
void srv_buffer_queue_append(srv_buffer_queue *, srv_buffer *);
srv_buffer *srv_buffer_queue_remove(srv_buffer_queue *);
srv_buffer *srv_buffer_queue_tail(srv_buffer_queue *);
size_t srv_buffer_queue_tail_size(srv_buffer_queue *);

void srv_buffer_check(cl_handle *, const srv_buffer *);
void srv_buffer_queue_check(cl_handle *, const srv_buffer_queue *);

/* srv-buffered-connection.c */

void srv_buffered_connection_clear_unparsed_input(srv_handle *_srv,
                                                  srv_buffered_connection *_bc);

char const *srv_buffered_connection_to_string(
    srv_buffered_connection const *_bc, char *_buf, size_t _size);

bool srv_buffered_connection_input_waiting_to_be_parsed(
    srv_handle *_srv, srv_buffered_connection *_bc);

void srv_buffered_connection_initialize(srv_buffered_connection *_bc,
                                        cl_handle *_cl, srv_buffer_pool *_pool);

void srv_buffered_connection_shutdown(srv_handle *_srv,
                                      srv_buffered_connection *_bc);

void srv_buffered_connection_have_priority(srv_buffered_connection *_bc,
                                           bool _val);

void srv_buffered_connection_status(srv_handle *_srv,
                                    srv_buffered_connection *_bc);

int srv_buffered_connection_write_ready(srv_buffered_connection *_bc,
                                        es_descriptor *_ed, bool *_any_out);

int srv_buffered_connection_write(srv_handle *_srv,
                                  srv_buffered_connection *_bc, int _fd,
                                  es_handle *_es, es_descriptor *_ed_out,
                                  bool *_any_out);

srv_buffer *srv_buffered_connection_policy_alloc(srv_buffered_connection *bc,
                                                 int priority,
                                                 char const *what_kind,
                                                 int line);
bool srv_buffered_connection_read(srv_session *_ses, int _fd,
                                  es_descriptor *_ed_in);

int srv_buffered_connection_input_lookahead(srv_buffered_connection *_bc,
                                            char **_s_out, char **_e_out,
                                            srv_buffer **_b_out);

void srv_buffered_connection_input_commit(srv_handle *_srv,
                                          srv_buffered_connection *_bc,
                                          char const *_e);

int srv_buffered_connection_output_lookahead(srv_session *_ses,
                                             size_t _min_size, char **_s_out,
                                             char **_e_out);

void srv_buffered_connection_output_commit(srv_buffered_connection *_bc,
                                           char const *_e);

void srv_buffered_connection_check(srv_buffered_connection *);

void *srv_buffered_connection_output_alloc_pre_hook(
    srv_buffered_connection *_bc, srv_pre_callback *_callback,
    size_t _callback_data_size);

/* srv-buffer-pool.c */

void srv_buffer_pool_free(srv_handle *, srv_buffer_pool *, srv_buffer *);
srv_buffer *srv_buffer_pool_alloc(srv_buffer_pool *);
void srv_buffer_pool_initialize(srv_buffer_pool *_pool, cm_handle *_cm,
                                cl_handle *_cl, unsigned long long _min_level,
                                unsigned long long _max_level, size_t _size);
void srv_buffer_pool_finish(srv_buffer_pool *);

/**
 * @brief If there's more than this much buffer space available (in %),
 *	give out buffer to anyone who wants one.  (Let's trade off all this
 *	space for speed.)
 */
#define SRV_BUFFER_POOL_MIN_GENEROUS (50.)

/**
 * @brief If there's at least this much buffer available (in %),
 * 	let customers have one buffer in use each (but do expect them
 * 	to empty out write buffers before getting new ones).
 */
#define SRV_BUFFER_POOL_MIN_FAIR (10.)

double srv_buffer_pool_available(srv_buffer_pool const *);

/* srv-epitaph.c */

int srv_epitaph_read(srv_handle *, cm_handle *, srv_epitaph **);
int srv_epitaph_clear(srv_handle *);

/* srv-pidfile.c */

int srv_pidfile_create(char const *, cl_handle *, char const *);
int srv_pidfile_read(cl_handle *, char const *, pid_t *);

int srv_pidfile_update(char const *_progname, cl_handle *_cl, char const *_path,
                       pid_t _pid);

int srv_pidfile_kill(char const *_progname, cl_handle *_cl, char const *_path,
                     int _sig);

int srv_pidfile_test(char const *_progname, cl_handle *_cl,
                     char const *_pidfile_path);

/* srv-request.c */

void srv_request_buffer_wait(srv_request *req);
void srv_request_buffer_wakeup(srv_request *req);
void srv_request_buffer_wakeup_all(srv_handle *);

void srv_request_done(srv_request *req, unsigned int flags);

void srv_request_run_start(srv_request *req);
void srv_request_run_stop(srv_request *req);

void srv_request_attach(srv_session *_ses, srv_request *_req, srv_buffer *_buf);

/* srv-session.c */

bool srv_session_status(srv_session *);

int srv_session_output_priority(srv_session const *);
bool srv_session_output_error(srv_session *, srv_msclock_t);
bool srv_session_input_error(srv_session *, srv_msclock_t);

bool srv_session_is_suspended(srv_session const *ses);
bool srv_session_run(srv_session *_ses, unsigned long long _deadline);

bool srv_session_ready_to_format(srv_session *);
bool srv_session_ready_to_parse(srv_session *);

void srv_session_link_request(srv_session *_ses, srv_request *_req);

void srv_session_unlink_request(srv_session *_ses, srv_request *_req);

typedef void srv_session_io_callback(void *, srv_handle *, srv_session *);

srv_session *srv_session_create(cm_handle *_cm, srv_handle *_srv,
                                srv_session_interface_type const *_if,
                                void *_data, bool _is_server,
                                char const *_displayname, char const *_ifname);
void srv_session_process_events(srv_session *);
void srv_session_schedule(srv_session *);
bool srv_session_output(srv_session *, srv_msclock_t);
bool srv_session_input(srv_session *, srv_msclock_t);

void srv_session_process_start(srv_session *);
void srv_session_process_stop(srv_session *);
void srv_session_update(srv_session *ses);

/* srv-sleep.c */

int srv_sleep_initialize(srv_handle *srv);
void srv_sleep_finish(srv_handle *srv);

/* srv-stat.c */

void srv_stat_sessions(srv_handle *);

/* srv-timeout.c */

int srv_session_timeout_destroy(srv_handle *, srv_session *);
srv_timeout *srv_timeout_create(srv_handle *, unsigned long);

#endif /* SRVP_H */
