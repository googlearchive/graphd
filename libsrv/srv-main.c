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
/*
 * srv-main.c -- srv main()
 *
 *	Startup and signal-handling.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <netdb.h>
#include <limits.h>
#include <sysexits.h>
#include <syslog.h>
#include <errno.h>
#include <signal.h>

#ifdef __linux__

#include <sys/sysinfo.h>

/* The declaration for sched_setaffinity seems badly messed up.  The following
 * is copied from the man page and seems to work...
 */
#include <sched.h>
extern int sched_setaffinity(pid_t pid, unsigned int len, unsigned long *mask);

#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <sys/mman.h>

#ifdef USE_GPROF
#include <sys/gmon.h>
void moncontrol(int mode);
#endif

#include "libcm/cm.h"
#include "srvp.h"
#include "srv-interface.h"

static char const *srv_executable;
static char const srv_default_interface[] = "tcp:";

static cl_loglevel_configuration const srv_clc_default = {
    CL_LEVEL_DETAIL, CL_LEVEL_OPERATOR_ERROR};

/* debugging */

int srv_syslog_crash_priority = LOG_USER | LOG_DEBUG;

/* global signal flags */

sig_atomic_t srv_reread_configuration = 0;
sig_atomic_t srv_terminate = 0;
sig_atomic_t srv_child_pid = 0;
sig_atomic_t srv_manager_terminate = 0;
srv_handle *srv_srv = NULL;

#define SRV_MAX_RESTARTS 3
#define SRV_MIN_RESTART_INTERVAL (5 * 60)
#define SRV_EX_MISCONFIGURE (EX__MAX + 1)

static void configure_netlog(srv_handle *srv) {
  char buf[1024], ciid_buf[1024], port_buf[42];
  char const *sip_s, *sip_e;
  char const *sport_s, *sport_e;
  char const *ifs;
  char *long_host_buf = NULL;
  char const *long_host, *long_dot = NULL, *r;

  if (srv->srv_netlog == NULL) return;

  long_host_buf = srv_address_fully_qualified_domainname(srv->srv_cm);

  ifs = srv_interface_to_string(srv, buf, sizeof buf);
  srv_address_ip_port(ifs, &sip_s, &sip_e, &sport_s, &sport_e);

  if ((sport_s == NULL || *sport_s == '\0') &&
      srv->srv_app->app_default_port != 0) {
    snprintf(port_buf, sizeof port_buf, "%hu", srv->srv_app->app_default_port);
    sport_s = port_buf;
    sport_e = port_buf + strlen(port_buf);
  }

  if ((long_host = long_host_buf) == NULL) long_host = "localhost";

  r = long_dot = long_host + strlen(long_host);

  /*  Cut off two domain name segments, if we
   *  have them.  Otherwise, stick with the FQDN.
   */
  while (r > long_host && r[-1] != '.') r--;
  if (r > long_host && r[-1] == '.') r--;
  while (r > long_host && r[-1] != '.') r--;
  if (r > long_host && r[-1] == '.') long_dot = r - 1;

  if (sport_s != NULL && *sport_s != '\0')
    snprintf(ciid_buf, sizeof ciid_buf, "%s:%.*s:%.*s", srv->srv_progname,
             (int)(long_dot - long_host), long_host, (int)(sport_e - sport_s),
             sport_s);
  else
    snprintf(ciid_buf, sizeof ciid_buf, "%s:%.*s", srv->srv_progname,
             (int)(long_dot - long_host), long_host);

  (void)cl_netlog_set_ciid(srv->srv_netlog, ciid_buf);

  cm_free(srv->srv_cm, long_host_buf);
}

static char const *srv_used_version(srv_handle const *srv) {
  srv_build_version_reference const *vr;
  char const *best;

  vr = srv->srv_app->app_build_version_reference;
  best = vr->vr_version;

  for (vr++; vr->vr_module != NULL; vr++)
    if (strcmp(vr->vr_version, best) > 0) best = vr->vr_version;

  return best;
}

static void srv_list_modules(srv_handle *srv) {
  srv_build_version_reference const *vr;
  vr = srv->srv_app->app_build_version_reference;

  for (; vr->vr_module != NULL; vr++) {
    printf("%s:%*s%s\n", vr->vr_module,
           (strlen(vr->vr_module) > 10 ? 0 : (int)(10 - strlen(vr->vr_module))),
           "", vr->vr_version);
  }
  exit(0);
}

static void srv_usage(char const *progname, srv_handle *srv) {
  srv_option const *option_ptr;

  fprintf(stderr,
          "Usage: %s [options...] (version: %s)\n"
          "Options are:\n"
          "  -c dirname       log code coverage to <dirname>\n"
          "  -f config-file   read configuration from config-file\n"
          "  -g name          become group <name>\n"
          "  -h               print this usage and exit\n"
          "  -i address       listen at interface <address>\n"
          "  -l pathname      log to file <pathname>\n"
          "  -L pathname      netlog to file <pathname>\n"
          "  -m               print module versions and exit\n"
          "  -n               run in foreground\n"
          "  -p pid-file      use <pid-file> to lock\n"
          "  -P processes     spawn <processes> workers (default: 1)\n"
          "  -q               query whether the server is running\n"
          "  -t               trace allocations\n"
          "  -u name          become user <name>\n"
          "  -v loglevel      set loglevel (verbosity) to loglevel\n"
          "  -V loglevel      set netloglevel (verbosity) to loglevel\n"
          "  -W               do NOT produce cores\n"
          "  -x pathname      set execuctable pathname (for debugger)\n"
          "  -y               run interactively\n"
          "  -z               shut down an existing server\n",
          progname,
          srv->srv_app->app_version ? srv->srv_app->app_version : "1.0");

  for (option_ptr = srv->srv_app->app_options;
       option_ptr != NULL && option_ptr->option_name != NULL; option_ptr++)
    if (option_ptr->option_description != NULL) {
      fputs(option_ptr->option_description, stderr);
    }

  putc('\n', stderr);

  exit(EX_USAGE);
}

static void srv_reread_configuration_set(int dummy) {
  (void)dummy;
  srv_reread_configuration = 1;
}

static void srv_manager_SIGTERM_or_SIGINT(int dummy) {
  (void)dummy;

  /* This is a human generated signal, so the intent is
   * to shutdown
   */
  srv_terminate = 1;

  /* And we're a terminating manager */
  srv_manager_terminate = 1;
}

static void srv_parent_SIGTERM_or_SIGINT(int dummy) {
  (void)dummy;

  /*  A human user is expressing an intention to terminate.
   */
  srv_terminate = 1;

  /*  If we don't have a child, just exit.
   */
  if (srv_child_pid == 0) exit(0);

  /* send a signal to the child by closing the
   * pipe to it.
   */
  if (srv_srv != NULL && srv_srv->srv_shutdown_pipe[1] != -1) {
    close(srv_srv->srv_shutdown_pipe[1]);
    srv_srv->srv_shutdown_pipe[1] = -1;
  }
}

void srv_finish(srv_handle *srv, bool child) {
  cl_enter(srv->srv_cl, CL_LEVEL_SPEW, "%s%s",
           srv->srv_interactive ? "(interactive) " : "",
           child ? "(child)" : "(parent)");

  if (!srv->srv_interactive) {
    char const *pid_file;

    if (!(pid_file = srv->srv_config->cf_pid_file)) {
      pid_file = SRV_PIDFILE_DEFAULT;
    }

    (void)unlink(pid_file);
  }

  /*  Terminate the input event handlers.
   */
  if (srv->srv_es != NULL) {
    es_handle *es;

    es = srv->srv_es;
    srv->srv_es = NULL;

    es_destroy(es);
  }

  srv_buffer_pool_finish(&srv->srv_pool);
  if (srv->srv_config != NULL) {
    srv_config_unlink(srv->srv_config);
    srv->srv_config = NULL;
  }

  /*  Call the final application callback to, e.g.,
   *  sync the database.
   */
  if (child) {
    if (srv->srv_app != NULL && srv->srv_app->app_shutdown != NULL &&
        !srv->srv_app_shutdown) {
      cl_log(srv->srv_cl, CL_LEVEL_SPEW, "Calling app_shutdown callback");
      srv->srv_app_shutdown = true;
      (*srv->srv_app->app_shutdown)(srv->srv_app_data, srv);
    }
  }

  /* Unmap shared memory area
   */
  srv_shared_finish(srv);
  cl_leave(srv->srv_cl, CL_LEVEL_SPEW, "leave");
}

static void srv_update(srv_handle *srv) {
  srv_session *ses, *next;

  for (ses = srv->srv_session_head; ses != NULL; ses = next) {
    next = ses->ses_next;

    if (ses->ses_changed || (ses->ses_want & (1 << SRV_RUN)) ||
        ses->ses_needs_interface_update) {
      ses->ses_needs_interface_update = false;
      srv_session_schedule(ses);
    }
  }
}

static void srv_es_post_dispatch(void *app_data, es_handle *es) {
  srv_session *ses, *next;
  srv_handle *srv = app_data;

  (void)es;

  /*  Process events in all sessions.
   */
  for (ses = srv->srv_session_head; ses != NULL; ses = next) {
    next = ses->ses_next;

    if (ses->ses_changed || (ses->ses_want & (1 << SRV_RUN))) {
      ses->ses_needs_interface_update = true;
      srv_session_process_events(ses);
    }
  }

  /*  Schedule all changed sessions.  This is separate from
   *  the first pass so that processing in one session can
   *  wake up another (earlier in the chain) - without us
   *  constantly rescheduling runs.
   */
  srv_update(srv);
}

static void srv_es_pre_dispatch(void *app_data, es_handle *es) {
  srv_handle *srv = app_data;

  if (srv->srv_app->app_pre_dispatch)
    (*srv->srv_app->app_pre_dispatch)(srv->srv_app_data, srv);
}

static int srv_initialize(srv_handle *srv) {
  int err;
  char const *pid_file = NULL;
  srv_config_parameter const *app_cf;

  /* Default to not running in SMP mode */
  if (srv->srv_config->cf_processes <= 0) {
    srv->srv_config->cf_processes = 1;
  }

  srv->srv_smp_index = -1;
  srv->srv_smp_manager = false;

  /* How many times should we restart in quick succession?
     This used to be hardcoded, but can now be configured */

  srv->srv_max_restarts = 3;

  /* Create shared memory area for parent and child process */

  if (srv_shared_initialize(srv)) return EX_OSERR;

  /* Create a pid file. */

  if (!srv->srv_interactive) {
    pid_file = srv->srv_config->cf_pid_file ? srv->srv_config->cf_pid_file
                                            : SRV_PIDFILE_DEFAULT;
    err = srv_pidfile_create(srv_program_name(srv), srv->srv_cl, pid_file);
    if (err != 0) {
      return EX_DATAERR;
    }

    /*  Remove leftover epitaphs so we can write one
     *  at shutdown time.
     */
    if ((err = srv_epitaph_clear(srv)) != 0) {
      return EX_DATAERR;
    }
  }
  srv->srv_pid = getpid();

  /* Create the event server handle.  */

  if (!(srv->srv_es = es_create(srv->srv_cm, srv->srv_cl))) {
    fprintf(stderr,
            "%s: failed to allocate event "
            "server handle: %s",
            srv->srv_progname, strerror(errno));
    if (pid_file) (void)unlink(pid_file);
    return EX_OSERR;
  }

  es_set_pre_dispatch(srv->srv_es, srv_es_pre_dispatch, srv);
  es_set_post_dispatch(srv->srv_es, srv_es_post_dispatch, srv);

  /*
   *  Finish the application-specific file configuration by
   *  calling the "config_open" callback for each configurable.
   *
   *  This also opens the database!
   */
  app_cf = srv->srv_app->app_config_parameters;
  if (app_cf)

    for (; app_cf->config_name != NULL; app_cf++) {
      if (app_cf->config_open != NULL) {
        err = (*app_cf->config_open)(srv->srv_app_data, srv,
                                     srv->srv_config->cf_app_data,
                                     srv->srv_config);
        if (err != 0) return err;
      }
    }

  /* Default missing buffer pool parameters,
   * and initialize buffer pool.
   */
  if (srv->srv_config->cf_pool_page_size == 0) {
    srv->srv_config->cf_pool_page_size = 1024 * 4;
  }

  if (!srv->srv_config->cf_pool_max) {
    srv->srv_config->cf_pool_max =
        srv->srv_config->cf_pool_min + 64 * srv->srv_config->cf_pool_page_size;
  }
  srv_buffer_pool_initialize(
      &srv->srv_pool, srv->srv_cm, srv->srv_cl, srv->srv_config->cf_pool_min,
      srv->srv_config->cf_pool_max, srv->srv_config->cf_pool_page_size);

  return 0;
}

static int srv_initialize_runtime_logging(srv_handle *srv, char const *log_name,
                                          char const *netlog_name) {
  int err;
  char *best_netlog_filename;

  if (!srv->srv_interactive) {
    /* Divert log to file or syslog. */
    if (!log_name) {
      log_name = srv->srv_config->cf_log_file;
    }
    if (log_name != NULL) {
      if ((err = cl_file(srv->srv_cl, log_name))) {
        fprintf(stderr,
                "%s: failed to open or "
                "create main log file\n",
                srv->srv_progname);
        return err;
      }
      cl_set_flush_policy(srv->srv_cl, srv->srv_config->cf_log_flush);
    } else {
      cl_syslog(srv->srv_cl,
                srv->srv_config->cf_log_ident ? srv->srv_config->cf_log_ident
                                              : srv->srv_progname,
                srv->srv_config->cf_log_facility
                    ? srv->srv_config->cf_log_facility
                    : LOG_USER);
    }

    /*  Set the loglevel on the diversion to the configured one.
     */
    cl_set_loglevel_configuration(srv->srv_cl, &srv->srv_config->cf_log_level);
  }

  /*  If the configuration supports that, create
   *  a netlog identity.
   */
  best_netlog_filename = (netlog_name == NULL) ? srv->srv_config->cf_netlog_file
                                               : (char *)netlog_name;

  if (best_netlog_filename != NULL) {
    /* set netlog file name */

    char *filename = best_netlog_filename;
    bool free_filename = false;
    if ((err = srv_netlog_set_filename(srv, filename))) {
      fprintf(stderr,
              "%s: failed to open or "
              "create netlog file\n",
              srv->srv_progname);
      return err;
    }

    if (free_filename) cm_free(srv->srv_cm, filename);

    /* set netlog level and flush policy */
    if (srv->srv_netlog != NULL) {
      cl_set_loglevel_configuration(srv->srv_netlog,
                                    &srv->srv_config->cf_netlog_level);
      cl_set_flush_policy(srv->srv_netlog, srv->srv_config->cf_netlog_flush);

      /*
       * Don't stacktrace the netlog. We already have
       * the stack trace from when cl_vlog formatted the
       * message the first time
       */
      cl_set_stacktrace(srv->srv_netlog, false);
    }
  }
  return 0;
}

static void srv_set_parent_signal_handlers(srv_handle *srv, pid_t child_pid) {
  /*  Set the global variables the signal handlers
   *  may use.  ("srv" obviously doesn't have type
   *  sigatomic_t, but it also doesn't change, so
   *  that's okay... Sort of...)
   */
  srv_child_pid = child_pid;
  srv_srv = srv;

  /* Ignore SIGPIPE.  We just use poll() */
  signal(SIGPIPE, SIG_IGN);

  signal(SIGUSR1, srv_reread_configuration_set);
  signal(SIGHUP, srv_reread_configuration_set);
  signal(SIGINT, srv_parent_SIGTERM_or_SIGINT);
  signal(SIGTERM, srv_parent_SIGTERM_or_SIGINT);
}

static void srv_manager_set_signal_handlers(srv_handle *srv) {
  signal(SIGTERM, srv_manager_SIGTERM_or_SIGINT);
  signal(SIGINT, srv_manager_SIGTERM_or_SIGINT);
}

static void srv_set_child_signal_handlers(void) {
  /* Ignore SIGPIPE.  We just use poll() */
  signal(SIGPIPE, SIG_IGN);

  signal(SIGUSR1, srv_reread_configuration_set);
  signal(SIGHUP, srv_reread_configuration_set);
}

static void just_exit(int unused) {
  signal(SIGABRT, SIG_DFL);
  abort();
}

/**
 * @brief Terminate now.
 *
 *  This function must be idempotent - it may be called more
 *  than once in the course of a termination.
 *
 * @param srv module handle
 */
#define srv_shutdown_now(srv) srv_shutdown_now_loc(srv, __FILE__, __LINE__)
void srv_shutdown_now_loc(srv_handle *srv, char const *file, int line) {
  cl_enter(srv->srv_cl, CL_LEVEL_SPEW, "[from %s:%d]", file, line);

  srv->srv_shutdown_begun = 1;

  /* If we get stuck somewhere, we'll exit after 5 minutes. */
  signal(SIGALRM, just_exit);
  alarm((unsigned int)(srv->srv_config->cf_shutdown_delay > UINT_MAX
                           ? UINT_MAX
                           : srv->srv_config->cf_shutdown_delay));

  /*  Close the pipe descriptor connected to
   *  the parent.
   */
  if (srv->srv_es != NULL && srv->srv_ed.ed_callback != NULL) {
    es_close(srv->srv_es, &srv->srv_ed);
    srv->srv_ed.ed_callback = NULL;
  }

  if (srv->srv_shutdown_pipe[0] != -1) {
    close(srv->srv_shutdown_pipe[0]);
  }
  if (srv->srv_shutdown_pipe[1] != -1) {
    close(srv->srv_shutdown_pipe[1]);
  }
  srv->srv_shutdown_pipe[0] = srv->srv_shutdown_pipe[1] = -1;

  /*  Log that we're exiting.  This is happening
   *  relatively early, but I want to format the
   *  interface names while we still *have*
   *  interfaces.
   */
  if (srv->srv_netlog != NULL) {
    char const *sip_s, *sip_e;
    char const *sport_s, *sport_e;
    char const *ifs;
    char buf[1024];

    ifs = srv_interface_to_string(srv, buf, sizeof buf);

    srv_address_ip_port(ifs, &sip_s, &sip_e, &sport_s, &sport_e);

    cl_log(srv->srv_netlog, CL_LEVEL_OVERVIEW,
           "%s.end "
           "(s)%s.version: %s "
           "%s%.*s "
           "%s%.*s "
           "%s.interface:: %s",

           srv->srv_progname, srv->srv_progname,
           srv->srv_app->app_version ? srv->srv_app->app_version : "1.0",

           *sip_s ? "server.ip: " : "", *sip_s ? (int)(sip_e - sip_s) : 0,
           sip_s,

           *sport_e ? "server.port: " : "",
           *sport_e ? (int)(sport_e - sport_s) : 0, sport_s,

           srv->srv_progname, ifs);
  }
  srv_interface_shutdown(srv);
  srv_sleep_finish(srv);

  /*  Terminate the input event handlers.  This sends
   *  EXIT events to any interfaces that still may be
   *  open, so we want it to happen before the
   *  application shutdown.
   */
  if (srv->srv_es != NULL) {
    es_destroy(srv->srv_es);
    srv->srv_es = NULL;
  }

  /*  Call the final application callback to, e.g.,
   *  sync the database.
   */
  if (srv->srv_app != NULL && srv->srv_app->app_shutdown != NULL &&
      !srv->srv_app_shutdown) {
    srv->srv_app_shutdown = true;
    (*srv->srv_app->app_shutdown)(srv->srv_app_data, srv);
  }

  if (srv->srv_netlog != NULL) {
    cl_destroy(srv->srv_netlog);
    srv->srv_netlog = NULL;
  }

  cl_leave(srv->srv_cl, CL_LEVEL_SPEW, "leave");
}

void srv_open_interfaces(srv_handle *srv) {
  int err = 0;
  srv_interface_config *icf;
  int any = 0;
  srv_config_parameter const *app_cf;
  app_cf = srv->srv_app->app_config_parameters;

  /*  If there's no interface configured, and we've
   *  got a default port, default to <tcp::port>.
   */
  if (srv->srv_config->cf_interface_head == NULL && srv->srv_app != NULL &&
      srv->srv_app->app_default_port != 0) {
    err = srv_interface_config_add(srv->srv_config, srv->srv_cl,
                                   srv_default_interface);
    if (err) {
      cl_log_errno(srv->srv_cl, CL_LEVEL_FAIL, "srv_interface_config_add", err,
                   "config=%s", app_cf->config_name);

      srv_shutdown_now(srv);
      srv_epitaph_print(srv, EX_SOFTWARE,
                        "srv_open_interfaces configuration fails "
                        "for %s: %s",
                        app_cf->config_name, strerror(err));

      srv_finish(srv, true);
      exit(EX_SOFTWARE);
    }
  }

  for (icf = srv->srv_config->cf_interface_head; icf; icf = icf->icf_next) {
    if ((err = srv_interface_create(srv, icf)) != 0) {
      cl_log_errno(srv->srv_cl, CL_LEVEL_FAIL, "srv_interface_create", err,
                   "app %s, interface %s", app_cf->config_name,
                   icf->icf_type->sit_type);

      srv_shared_set_restart(srv, false);
      srv_shutdown_now(srv);

      srv_epitaph_print(srv, EX_SOFTWARE, "cannot open interface %s: %s",
                        icf->icf_address, strerror(err));

      srv_finish(srv, true);
      exit(EX_SOFTWARE);
    } else
      any++;
  }
  if (!any) {
    if (srv->srv_interactive) {
      fprintf(stderr,
              "%s: could not open terminal interface "
              "- abort\n",
              srv->srv_progname);
      srv_shutdown_now(srv);
      srv_epitaph_print(srv, EX_OSERR,
                        "could not open terminal interface "
                        "for %s: %s",
                        app_cf->config_name, strerror(err));

      srv_finish(srv, true);
      exit(EX_OSERR);
    } else {
      fprintf(
          stderr,
          "%s: missing configuration - which interfaces should %s listen on?\n"
          "\tTo specify an interface on the command line, use \"-i ADDRESS\";\n"
          "\tin the configuration file, use \"listen ADDRESS\".\n"
          "\tADDRESS syntax:           EXAMPLES\n"
          "\t    TCP: \"[IP]:PORT\"      \"127.0.0.1:8100\" or \":80\"\n"
          "\t  Local: \"PATH\"           \"/var/run/srv\"\n"
          "\tTo interact on stdin/stdout instead, use \"-y\".\n",
          srv->srv_progname, srv->srv_progname);
      srv_shutdown_now(srv);
      srv_epitaph_print(srv, EX_SOFTWARE,
                        "could not open any interfaces "
                        "for %s: %s",
                        app_cf->config_name, strerror(err));

      srv_finish(srv, true);
      exit(EX_SOFTWARE);
    }
  }
}

/* Write ERRORs to the netlog system
 */
static void srv_netlog_siphon_write(void *data, cl_loglevel level,
                                    char const *text) {
  srv_handle *srv = (srv_handle *)data;

  if (!srv->srv_netlog) return;

  if (srv->srv_request != NULL) {
    cl_log(srv->srv_netlog, level,
           "%s.error "
           "TID: %s "
           "%s"
           "error.level: %s "
           "error.msg:: %s",
           srv->srv_app->app_name,
           srv->srv_request->req_display_id ? srv->srv_request->req_display_id
                                            : "???",
           srv->srv_request->req_session->ses_netlog_header
               ? srv->srv_request->req_session->ses_netlog_header
               : "",
           CL_IS_LOGGED(CL_LEVEL_FATAL, level) ? "fatal" : "error", text);
  } else if (srv->srv_session != NULL) {
    cl_log(
        srv->srv_netlog, level,
        "%s.session.error %s (l)%s.sesid: %llu error.level: %s error.msg:: %s",
        srv->srv_app->app_name, srv->srv_session->ses_netlog_header
                                    ? srv->srv_session->ses_netlog_header
                                    : "",
        srv->srv_app->app_name, (unsigned long long)srv->srv_session->ses_id,
        CL_IS_LOGGED(CL_LEVEL_FATAL, level) ? "fatal" : "error", text);
  } else {
    cl_log(srv->srv_netlog, level,
           "%s.error "
           "error.level: %s "
           "error.msg:: %s",
           srv->srv_app->app_name,
           CL_IS_LOGGED(CL_LEVEL_FATAL, level) ? "fatal" : "error", text);
  }
}

/**
 * @brief terminate in response to a parent terminating.
 */
static void srv_pipe_es_callback(es_descriptor *ed, int fd,
                                 unsigned int events) {
  srv_handle *srv = (srv_handle *)ed;

  cl_log(srv->srv_cl, CL_LEVEL_DETAIL,
         "%s %s (%.17s) work process %lu shutting down "
         "(parent terminated; events: %x)",
         srv->srv_progname,
         srv->srv_app->app_version ? srv->srv_app->app_version : "1.0",
         srv_used_version(srv), (unsigned long)getpid(), (unsigned int)events);
  srv_shutdown_now(srv);

  /*  Return to the calling es_loop().  If everything
   *  went right, it knows its supposed to terminate and
   *  return us to the calling srv_child().
   */
}

static void srv_child_setup_pipe(srv_handle *srv) {
  int err;

  /*  Child.  Close the write end of the pipe; the child
   *  will listen to the read end for signals.
   *
   *  When the parent terminates, the read end is
   *  signalled (because it would yield an error).
   *  The child will terminate in response.
   *
   *  (Because the parent doesn't do anything but wait for
   *  the child, the parent is likely to die in response to
   *  being killed by a human, and the likely cause of that
   *  is a wish for the database server to terminate.)
   */
  close(srv->srv_shutdown_pipe[1]);
  srv->srv_shutdown_pipe[1] = -1;

  srv->srv_ed.ed_callback = srv_pipe_es_callback;
  srv->srv_ed.ed_displayname = "server process";

  err = es_open(srv->srv_es, srv->srv_shutdown_pipe[0], ES_INPUT, &srv->srv_ed);
  if (err != 0) {
    cl_log(srv->srv_cl, CL_LEVEL_ERROR, "%s: es_open fails: %s [%s:%d]",
           srv->srv_progname, strerror(err), __FILE__, __LINE__);
    exit(EX_OSERR);
  }
}

static void srv_child(srv_handle *srv) {
  srv_config_parameter const *app_cf;
  int err;

  cl_assert(srv->srv_cl, srv->srv_es != NULL);
  cl_enter(srv->srv_cl, CL_LEVEL_SPEW, "enter");

#ifdef USE_GPROF
  moncontrol(1);
#endif

  if (!srv->srv_interactive) {
    srv_child_setup_pipe(srv);
    srv_set_child_signal_handlers();
  }

  cl_set_siphon(srv->srv_cl, srv_netlog_siphon_write, srv,
                CL_LEVEL_OPERATOR_ERROR);

  if (srv->srv_netlog != NULL) {
    char buf[1024], port_buf[42];
    char const *sip_s, *sip_e;
    char const *sport_s, *sport_e;
    char const *ifs;
    char *long_host_buf = NULL;
    char const *long_host, *long_dot = NULL, *r;

    long_host_buf = srv_address_fully_qualified_domainname(srv->srv_cm);

    ifs = srv_interface_to_string(srv, buf, sizeof buf);
    srv_address_ip_port(ifs, &sip_s, &sip_e, &sport_s, &sport_e);

    if ((sport_s == NULL || *sport_s == '\0') &&
        srv->srv_app->app_default_port != 0) {
      snprintf(port_buf, sizeof port_buf, "%hu",
               srv->srv_app->app_default_port);
      sport_s = port_buf;
      sport_e = port_buf + strlen(port_buf);
    }

    if ((long_host = long_host_buf) == NULL) long_host = "localhost";

    r = long_dot = long_host + strlen(long_host);

    /*  Cut off two domain name segments, if we
     *  have them.  Otherwise, stick with the FQDN.
     */
    while (r > long_host && r[-1] != '.') r--;
    if (r > long_host && r[-1] == '.') r--;
    while (r > long_host && r[-1] != '.') r--;
    if (r > long_host && r[-1] == '.') long_dot = r - 1;

    cl_log(srv->srv_netlog, CL_LEVEL_OVERVIEW,
           "%s.start "
           "(s)HOST: %s "
           "(s)%s.version: %s "
           "%s%.*s "
           "%s%.*s "
           "%s.interface:: %s",

           srv->srv_progname, long_host, srv->srv_progname,
           srv->srv_app->app_version ? srv->srv_app->app_version : "1.0",

           *sip_s ? "server.ip: " : "", *sip_s ? (int)(sip_e - sip_s) : 0,
           sip_s,

           *sport_s ? "server.port: " : "",
           *sport_s ? (int)(sport_e - sport_s) : 0, sport_s,

           srv->srv_progname, ifs);

    cm_free(srv->srv_cm, long_host_buf);
  }
#if 0
#ifdef __linux__
	{
		unsigned long  mask = 0x1 << srv->srv_config->cf_cpu;

		cl_log(
			srv->srv_cl,
			CL_LEVEL_DETAIL,
			"srv_child: locking process to CPU %d",
			(int)srv->srv_config->cf_cpu);
		if (sched_setaffinity( 0, sizeof mask, &mask ))
			cl_log_errno( srv->srv_cl, CL_LEVEL_ERROR,
				"sched_setaffinity", errno,
				"Unable to set process affinity");
	}
#endif
#endif

  /*
   *  Call the "run" callback for any configurables.
   */
  app_cf = srv->srv_app->app_config_parameters;
  if (app_cf != NULL)
    for (; app_cf->config_name != NULL; app_cf++)
      if (app_cf->config_run != NULL) {
        int err;

        err = (*app_cf->config_run)(srv->srv_app_data, srv,
                                    srv->srv_config->cf_app_data,
                                    srv->srv_config);
        if (err != 0) {
          cl_leave(srv->srv_cl, CL_LEVEL_SPEW,
                   "config_run "
                   "callback fails for %s: %s",
                   app_cf->config_name, strerror(err));

          srv_epitaph_print(srv, SRV_EX_MISCONFIGURE,
                            "configuration callback fails "
                            "for %s: %s",
                            app_cf->config_name, strerror(err));

          srv_finish(srv, true);
          exit(EX_SOFTWARE);
        }
      }

  /*  Call global "everything's ready to go" callback.
   */
  if (srv->srv_app->app_startup != NULL) {
    err = (*srv->srv_app->app_startup)(srv->srv_app_data, srv);
    if (err != 0) {
      cl_leave(srv->srv_cl, CL_LEVEL_SPEW,
               "app_startup callback "
               "fails for %s: %s",
               app_cf && app_cf->config_name ? app_cf->config_name
                                             : srv->srv_app->app_name,
               cl_strerror(srv->srv_cl, err));

      srv_epitaph_print(srv, SRV_EX_MISCONFIGURE,
                        "startup callback fails for %s: %s",
                        app_cf && app_cf->config_name ? app_cf->config_name
                                                      : srv->srv_app->app_name,
                        cl_strerror(srv->srv_cl, err));
      srv_finish(srv, true);
      exit(EX_SOFTWARE);
    }
  }

  /*  Start sleep callbacks.
   */
  if ((err = srv_sleep_initialize(srv)) != 0) {
    cl_leave(srv->srv_cl, CL_LEVEL_SPEW,
             "srv_sleep_initialize "
             "fails for %s: %s",
             app_cf && app_cf->config_name ? app_cf->config_name
                                           : srv->srv_app->app_name,
             cl_strerror(srv->srv_cl, err));

    srv_shutdown_now(srv);
    srv_epitaph_print(srv, EX_SOFTWARE, "startup callback fails for %s: %s",
                      app_cf && app_cf->config_name ? app_cf->config_name
                                                    : srv->srv_app->app_name,
                      cl_strerror(srv->srv_cl, err));

    srv_finish(srv, true);
    exit(EX_SOFTWARE);
  }

  if (!srv->srv_startup_is_complete &&
      srv->srv_app->app_startup_complete != NULL) {
    /* We are not fully started. This usually means
     * some external connections and handshaking need
     * to happen. Let's run a bit so that we can
     * become complete
     */

    srv_update(srv);
    err = es_loop(srv->srv_es);

    /* The way to continue this process is
     * to call srv_startup_now_complete, which calls
     * es_break() which makes es_loop return EINTR.
     */
    if (err != EINTR) {
      cl_log(srv->srv_cl, CL_LEVEL_VERBOSE, "srv_child: es_loop: %s",
             err ? cl_strerror(srv->srv_cl, err) : "ok");
      goto srv_child_done;
    }
  }
  srv_open_interfaces(srv);

  if (!srv->srv_settle_application) srv_settle_ok(srv);

  srv_run_startup_complete_callback(srv);

  if (srv->srv_config->cf_processes > 1 && !srv->srv_interactive) {
    /* We become the manager */

    err = srv_child_smp(srv);
    if (err != 0) {
      cl_log(srv->srv_cl, CL_LEVEL_ERROR,
             "srv_child: SMP manager died with error code %d: %s", err,
             cl_strerror(srv->srv_cl, err));
      srv_shutdown_now(srv);
      srv_finish(srv, true);
      exit(EX_SOFTWARE);
    }
    goto srv_child_done;
  }

  srv_update(srv);
  err = es_loop(srv->srv_es);

srv_child_done:

  srv_shutdown_now(srv);
  srv_finish(srv, true);
  cl_leave(srv->srv_cl, CL_LEVEL_VERBOSE, "done");

  if (srv->srv_trace) cm_trace_list(srv->srv_cm);

  exit(EX_OK);
}

static int smp_spawn(srv_handle *srv, size_t index) {
  pid_t new_pid;
  int smp_pipe_fds[2];
  int err;
  int write_err;

  if (pipe(smp_pipe_fds) == -1) {
    err = errno ? errno : ENOTCONN;
    return err;
  }

  if ((new_pid = fork()) == -1) {
    err = errno ? errno : EAGAIN;
    return err;
  }

  if (new_pid == 0) {
    /* This is the child of the fork */
    srv->srv_smp_index = (int)index;

    /* Close the read end of the startup pipe */
    close(smp_pipe_fds[0]);

    if (srv->srv_app->app_smp_startup != NULL) {
      err = (*srv->srv_app->app_smp_startup)(srv->srv_app_data, srv, index);
      if (err != 0) {
        cl_log(srv->srv_cl, CL_LEVEL_ERROR,
               "smp_startup callback failed, "
               "pid %d index %d: %s",
               getpid(), (int)index, cl_strerror(srv->srv_cl, err));
        /* Write something non-zero to the pipe to cause an error */
        write_err = write(smp_pipe_fds[1], "e", 1);
        exit(EX_SOFTWARE);
      }
    }
    /* Close the write end of the startup pipe */
    close(smp_pipe_fds[1]);

/* The entire lifecycle of a worker */
#ifdef USE_GPROF
    moncontrol(1);
#endif

    srv_update(srv);
    err = es_loop(srv->srv_es);
    srv_shutdown_now(srv);
    srv_finish(srv, true);

    if (srv->srv_trace) cm_trace_list(srv->srv_cm);

    exit(EX_OK);
  }

  close(smp_pipe_fds[1]);

  char buf;
  if ((read(smp_pipe_fds[0], &buf, 1)) != 0) {
    waitpid(new_pid, &err, 0);
    close(smp_pipe_fds[0]);
    return err;
  }

  close(smp_pipe_fds[0]);
  return new_pid;
}

int srv_child_smp(srv_handle *srv) {
  pid_t *worker_pids;
  int status;
  pid_t dead_proc = 0;
  int err;
  pid_t new_pid;
  pid_t killed = 0;
  unsigned int running_process_n = 0;
  size_t i = 0;

  size_t process_num = (size_t)srv->srv_config->cf_processes;

  /* We get called if and only if we are supposed to be in
     SMP mode. */
  cl_assert(srv->srv_cl, process_num > 1);

  worker_pids = (pid_t *)malloc(sizeof(pid_t) * process_num);

  if (worker_pids == NULL) {
    err = errno ? errno : ENOMEM;
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "malloc fails for worker_pids array: %s",
           cl_strerror(srv->srv_cl, err));
    return err;
  }

  srv_manager_set_signal_handlers(srv);

  for (i = 0; i < process_num; i++) {
    new_pid = smp_spawn(srv, i);

    if (new_pid < 1) {
      err = new_pid;
      cl_log(srv->srv_cl, CL_LEVEL_ERROR, "smp_spawn failed: %s",
             cl_strerror(srv->srv_cl, err));
      kill(0, SIGABRT);
      return err;
    }

    cl_assert(srv->srv_cl, new_pid > 0);

    worker_pids[i] = new_pid;
    running_process_n++;
  }

  srv->srv_smp_manager = true;
  srv_settle_close(srv);

  while (running_process_n > 0) {
    if (srv_terminate) {
      for (i = 0; i < process_num; i++) {
        if (kill(worker_pids[i], SIGTERM)) {
          if (errno != ESRCH) {
            err = errno;
            cl_log(srv->srv_cl, CL_LEVEL_ERROR,
                   "Cannot kill child process %d in orderly shutdown. "
                   "Unorderly shutdown is now in progress. kill(2) err: %s",
                   (int)worker_pids[i], cl_strerror(srv->srv_cl, err));
            goto killall_err;
          }
        }
      }
    }

    dead_proc = wait(&status);
    if (dead_proc < 0) {
      err = errno;
      if (srv_manager_terminate) {
        cl_log(srv->srv_cl, CL_LEVEL_INFO,
               "Manager caught SIGTERM or SIGINT, "
               "shutting down workers normally.");
      }

      /* Go back to the top. If we've got a signal,
       * this will shut things down normally. If not,
       * we'll just loop and wait again
       */

      if (err == EINTR) continue;

      cl_log(srv->srv_cl, CL_LEVEL_ERROR,
             "wait(2) failed -- either I have no children or caught "
             "a signal. Error: %s",
             cl_strerror(srv->srv_cl, err));
      goto killall_err;
    }
    running_process_n--;
    for (i = 0; i < process_num; i++) {
      if (worker_pids[i] == dead_proc) {
        break;
      }
    }
    cl_assert(srv->srv_cl, i < process_num);

    err = 0;
    if (srv->srv_app->app_smp_finish != NULL) {
      err = (*srv->srv_app->app_smp_finish)(srv->srv_app_data, srv, i, status);
    }

    if (err == 0)
      continue;
    else if (err == SRV_ERR_MORE) {
      new_pid = smp_spawn(srv, i);

      cl_assert(srv->srv_cl, new_pid > 0);

      worker_pids[i] = new_pid;
      running_process_n++;
    } else {
      cl_log(srv->srv_cl, CL_LEVEL_ERROR,
             "app_smp_finish instructed us to kill everyone due to error");
      goto killall_err;
    }
  }

  return 0;

killall_err:
  for (i = 0; i < process_num; i++) {
    if (worker_pids[i] != dead_proc) {
      kill(worker_pids[i], SIGKILL);

      killed = waitpid(worker_pids[i], &status, 0);
    }
  }
  return SRV_ERR_NO;
}

static int srv_parent(srv_handle *srv, pid_t child_pid) {
  size_t n_adjacent = 0;
  char buf[1024 * 8];
  int err = 0;

  cl_assert(srv->srv_cl, !srv->srv_interactive);

  close(srv->srv_shutdown_pipe[0]);
  srv->srv_shutdown_pipe[0] = -1;
  srv_set_parent_signal_handlers(srv, child_pid);

  /* set child start-time */
  srv_shared_set_time(srv, time(NULL));

  if (srv->srv_netlog) {
    /*
     * Send out errors to the netlog
     */
    cl_set_siphon(srv->srv_cl, srv_netlog_siphon_write, srv,
                  CL_LEVEL_OPERATOR_ERROR);
  }

  cl_log(srv->srv_cl, CL_LEVEL_OVERVIEW, "%s %s starting up on %s.",
         srv->srv_progname,
         srv->srv_app->app_version ? srv->srv_app->app_version : "1.0",
         srv_interface_to_string(srv, buf, sizeof buf));

  for (;;) {
    int status = 0;
    bool restart = false;
    pid_t res;

    res = waitpid(child_pid, &status, 0);
    if (res == child_pid) {
      char *err_string;

      /* Clear the global varialble that the
       * signal handler sends to.
       */
      srv_child_pid = 0;

      /*  If we have an error message from the child,
       *  print it.
       */
      if (srv_settle_wait(srv, &err_string) && err_string != NULL) {
        fprintf(stderr, "%s: %s\n", srv->srv_progname, err_string);
        cm_free(srv->srv_cm, err_string);

        cl_log(srv->srv_cl, CL_LEVEL_DEBUG,
               "Error message from child printed to stderr");
        /* A positive response from srv_settle_wait means
         * the application itself wants to shut down.
         */
        return EX_SOFTWARE;
      }
      if (WIFEXITED(status)) {
        if (WEXITSTATUS(status) == EX_OK && srv_terminate) {
          /*  Correct shutdown.  We wanted
           *  to terminate; we told the child
           *  to terminate; it terminated.
           *
           *  Whee.
           */
          cl_log(srv->srv_cl, CL_LEVEL_OVERVIEW, "%s %s shutting down.",
                 srv->srv_progname,
                 srv->srv_app->app_version ? srv->srv_app->app_version : "1.0");

          /*  In this case, the netlog message
           *  has been sent by the application.
           */
          return EX_OK;
        }
        if (WEXITSTATUS(status) == EX_SOFTWARE) {
          cl_log(srv->srv_cl, CL_LEVEL_OPERATOR_ERROR,
                 "%s: child process %lu died on "
                 "EX_SOFTWARE. Assuming a "
                 "misconfiguration and exiting.",
                 srv->srv_progname, (unsigned long)child_pid);
          return EX_SOFTWARE;
        }

        cl_log(srv->srv_cl, CL_LEVEL_OPERATOR_ERROR,
               "%s: engine process %lu exited "
               "unexpectedly with exit code %d",
               srv->srv_progname, (unsigned long)child_pid,
               WEXITSTATUS(status));

        if (srv->srv_netlog != NULL)
          cl_log(srv->srv_netlog, CL_LEVEL_ERROR,
                 "%s.process.abort pid: %lu, "
                 "exit: %d",
                 srv->srv_progname, (unsigned long)child_pid,
                 WEXITSTATUS(status));

        restart =
            !(srv_terminate || WEXITSTATUS(status) == SRV_EX_MISCONFIGURE);
        cl_log(srv->srv_cl, CL_LEVEL_SPEW, "srv_parent (restart): restart = %s",
               restart ? "true" : "false");
      } else if (WIFSIGNALED(status)) {
        /* Not really an operator error, but
         * the stack trace really doesn't help
         * us here.
         */
        cl_log(srv->srv_cl, CL_LEVEL_OPERATOR_ERROR,
               "%s: engine process %lu exited "
               "with signal %d%s",
               srv->srv_progname, (unsigned long)child_pid, WTERMSIG(status),
               WCOREDUMP(status) ? " (core dumped)" : "");

        if (WTERMSIG(status) == SIGKILL && srv->srv_config->cf_processes > 1) {
          /* We're in SMP mode, someone has sent a SIGKILL.
           * There's no way we can recover the follower processes.
           * We want to die. Now, and with a lockfile in place
           * beacuse this is a very intentional death.
           */

          srv_epitaph_print(srv, EX_SOFTWARE,
                            "%s: SMP manager with PID %lu died with signal 9. "
                            "Oh the humanity! "
                            "In the future, kill a child with SIGKILL!",
                            srv->srv_progname, (unsigned long)child_pid);

          kill(0, SIGKILL);
          /* And since this signals ourselves, we're dead */
        }

        restart = !srv_terminate;
      } else {
        cl_notreached(srv->srv_cl, "child %lu: unexpected status %x",
                      (unsigned long)child_pid, status);
        return EX_SOFTWARE;
      }

      if (srv_terminate)
        /*  The child crashed during shutdown.
         *  May be coincidence, may be causal.
         *  Worry.
         */
        return EX_SOFTWARE;

      if (!srv_shared_can_restart(srv)) {
        cl_log(srv->srv_cl, CL_LEVEL_OPERATOR_ERROR,
               "%s: child does not want to "
               "be restarted. Quitting.",
               srv->srv_progname);
        return EX_SOFTWARE;
      }
    } else if (errno != EINTR)
      cl_notreached(srv->srv_cl, "waitpid %lu: unexpected error: %s",
                    (unsigned long)child_pid, cl_strerror(srv->srv_cl, err));
    else {
      if (srv_reread_configuration) {
        /* XXX */
      } else if (srv_terminate)
        ;
      else {
        cl_log(srv->srv_cl, CL_LEVEL_ERROR,
               "%s: server waitpid() call "
               "interrupted, restarting",
               srv->srv_progname);
      }
    }

    if (restart) {
      time_t last_time;  // child start-time
      time_t this_time;  // child term-time

      /* remember that we crashed */
      srv_shared_set_crashed(srv);

      /* crashing too often? */
      last_time = srv_shared_get_time(srv);
      this_time = time(NULL);
      if (difftime(this_time, last_time) > SRV_MIN_RESTART_INTERVAL)
        n_adjacent = 0;
      else if (++n_adjacent >= srv->srv_max_restarts &&
               srv->srv_max_restarts >= 0) {
        cl_log(srv->srv_cl, CL_LEVEL_FATAL,
               "%s: %d restarts in short succession "
               "-- giving up",
               srv->srv_progname, SRV_MAX_RESTARTS);
        return EX_SOFTWARE;
      }
      srv_shared_set_time(srv, this_time);

      if (pipe(srv->srv_shutdown_pipe)) {
        fprintf(stderr, "%s: pipe: %s\n", srv->srv_progname, strerror(errno));
        srv_finish(srv, false);

        exit(EX_OSERR);
      }

      if (srv_terminate)

        /*  A shutdown signal reaches us as we are
         *  planning to restart a crashed child.
         */
        return EX_SOFTWARE;

      /* Make sure the pidfile (which should point to us,
       * the parent) is in place
       */
      char const *pid_file;
      pid_file = srv->srv_config->cf_pid_file ? srv->srv_config->cf_pid_file
                                              : SRV_PIDFILE_DEFAULT;

      err = srv_pidfile_test(srv_program_name(srv), srv->srv_cl, pid_file);
      if (err == ENOENT) {
        err = srv_pidfile_create(srv_program_name(srv), srv->srv_cl, pid_file);
        if (err != 0) {
          return EX_DATAERR;
        }
      } else if (err != 0) {
        return err;
      }

#ifdef USE_GPROF
      moncontrol(0);
#endif
      if ((child_pid = fork()) == (pid_t)-1) {
        cl_log(srv->srv_cl, CL_LEVEL_FATAL,
               "%s: "
               "fork in monitoring parent fails: %s",
               srv->srv_progname, strerror(errno));
        return EX_OSERR;
      }
      if (child_pid == 0) {
        srv_child(srv);
        /* NOTREACHED */
      }

      /* parent */

      srv_child_pid = child_pid;

      if (srv->srv_app->app_spawn != NULL) {
        err = (*srv->srv_app->app_spawn)(srv->srv_app_data, srv, child_pid);
        if (err != 0) {
          cl_log(srv->srv_cl, CL_LEVEL_FATAL,
                 "%s: error preparing "
                 "restarted subprocesses: %s\n",
                 srv->srv_progname, cl_strerror(srv->srv_cl, err));
          (void)kill(child_pid, SIGTERM);
          srv_finish(srv, false);
          return err;
        }
      }

      close(srv->srv_shutdown_pipe[0]);
      srv->srv_shutdown_pipe[0] = -1;

      /*  If a signal hit while we were
       *  still setting up this pipe and
       *  the child pid, the child may not
       *  have gotten killed because the
       *  signal handler didn't know the child id.
       *  So, invoke the handler again.
       */
      if (srv_terminate) srv_parent_SIGTERM_or_SIGINT(0);
    } else {
      /* restart == false */
      return EX_SOFTWARE;
    }
  }
}

static void srv_trace_log_callback(void *callback_data, int level,
                                   char const *str, ...) {
  static const int loglevels[10] = {[CM_LOG_LIST] =
                                        CL_LEVEL_ERROR | CM_FACILITY_MEMORY,
                                    [CM_LOG_ERROR] =
                                        CL_LEVEL_ERROR | CM_FACILITY_MEMORY,
                                    [CM_LOG_ALLOC] =
                                        CL_LEVEL_ULTRA | CM_FACILITY_MEMORY};
  cl_handle *const cl = (cl_handle *)callback_data;
  va_list ap;

  if (!cl_is_logged(cl, loglevels[level])) return;

  va_start(ap, str);
  cl_vlog(cl, loglevels[level], str, ap);
  va_end(ap);
}

static void srv_cl_abort_callback(void *data) {
  srv_handle *srv = data;
  if (srv_want_core(srv)) {
    signal(SIGABRT, SIG_DFL);
    abort();
  }
}

/**
 * @brief Gimme a C!  Gimme an O!  Gimme an R!  Gimme an E!
 *
 *  If the underlying system calls fail, this function
 *  logs an error message to CL_LEVEL_ERROR.
 *
 * @param srv	global module handle
 * @param want_core	true if the caller would like the application
 * 	to leave a core file when crashing, false if not.
 */
void srv_set_want_core(srv_handle *srv, bool want_core) {
  cl_handle *cl = srv->srv_cl;
  struct rlimit rl;

  srv->srv_config->cf_want_core = want_core;

  if (getrlimit(RLIMIT_CORE, &rl)) {
    cl_log(cl, CL_LEVEL_ERROR, "getrlimit failed: %s", strerror(errno));
    return;
  }

  if (want_core) {
    rl.rlim_cur = rl.rlim_max;
    signal(SIGABRT, SIG_DFL);
    signal(SIGSEGV, SIG_DFL);
    signal(SIGBUS, SIG_DFL);
  } else {
    rl.rlim_cur = 0;
  }

  if (setrlimit(RLIMIT_CORE, &rl))
    cl_log(cl, CL_LEVEL_ERROR, "setrlimit failed: %s", strerror(errno));
}

/**
 * @brief Return the current core setting for the server.
 *
 * @param srv	global module handle
 * @return whether the system is configured to dump a core file
 */
bool srv_want_core(srv_handle *srv) { return srv->srv_config->cf_want_core; }

/**
 * @brief Keep a diary.
 *
 * @param srv		global module handle
 * @param diary		NULL or cl_handle to log formalized events to
 */
void srv_set_diary(srv_handle *srv, cl_handle *diary) {
  srv->srv_diary = diary;
}

char const *srv_xstrerror(int err) {
  char const *str;
  if ((str = srv_strerror(err)) != NULL) return str;
  return strerror(err);
}

char const *srv_strerror(int err) {
  switch (err) {
    case SRV_ERR_NO:
      return "no";
    case SRV_ERR_MORE:
      return "more...";
    case SRV_ERR_ALREADY:
      return "operation already completed";
    case SRV_ERR_SYNTAX:
      return "syntax error";
    case SRV_ERR_SEMANTICS:
      return "semantics error";
    case SRV_ERR_NOT_SUPPORTED:
      return "option not supported";
    case SRV_ERR_REQUEST_TOO_LONG:
      return "request too long";
    case SRV_ERR_ADDRESS:
      return "error in address";
    default:
      break;
  }
  return NULL;
}

/**
 * @brief Run an application
 *
 * @param argc	number of command line arguments, plus one, as passed to main()
 * @param argv	command line arguments and executable, as passed to main()
 * @param data	opaque application data pointer, passed to methods
 * @param app	application callback and descriptor structure
 *
 * @return an exit status, fit for use with "return" from main().
 */
int srv_main(int argc, char **argv, void *data, srv_application const *app) {
  cm_handle *cm;
  cl_handle *cl;
  srv_handle srv[1];
  struct stat st;
  int opt;
  cl_loglevel_configuration *clc = NULL, clc_buf;
  cl_loglevel_configuration *cnlc = NULL, cnlc_buf;
  char const *user_name = NULL;
  char const *group_name = NULL;
  char const *log_name = NULL;
  char const *netlog_name = NULL;
  char const *config_name = NULL;
  char const *pid_file = NULL;
  int proc_num = 0;
  char const *coco_dir = NULL;
  cm_handle *arg_cm;
  char **arg_i = NULL;
  char **a;
  char *w;
  int err = 0;
  pid_t pid;
  char option_buf[1024];
  srv_option const *option_ptr;
  int do_stop = 0;
  int do_query = 0;
  cl_handle *diary_cl;
  cl_diary_handle *d;
  bool want_core = true;

  memset(srv, 0, sizeof(srv));

  srv_executable = argv[0];

  if ((srv->srv_progname = strrchr(argv[0], '/')) != NULL)
    srv->srv_progname++;
  else
    srv->srv_progname = argv[0];

  /*  Initialize the interface tail queue.
   */
  srv->srv_if_head = NULL;
  srv->srv_if_tail = &srv->srv_if_head;
  srv->srv_session_head = NULL;
  srv->srv_session_tail = srv->srv_session_head = NULL;
  srv->srv_app_data = data;
  srv->srv_app = app;
  srv->srv_settle_pipe[0] = srv->srv_settle_pipe[1] = -1;
  srv->srv_shutdown_pipe[0] = srv->srv_shutdown_pipe[1] = -1;

  arg_cm = cm_c();
  if (arg_cm == NULL) {
    /* This can't actually happen -- cm_c() is static. */
    fprintf(stderr, "%s: failed to allocate allocator!\n", srv->srv_progname);
    exit(EX_OSERR);
  }

  /*  Merge application-supplied options with libsrv's.
   */
  strcpy(option_buf, "c:f:g:hi:l:L:mnp:P:qtu:v:V:Wyx:z");
  w = option_buf + strlen(option_buf);
  for (option_ptr = srv->srv_app->app_options;
       option_ptr && option_ptr->option_name; option_ptr++) {
    char const *r = option_ptr->option_name;
    if (strchr(option_buf, *r)) {
      fprintf(stderr,
              "libsrv: option %s is already taken, "
              "sorry! (Used: %s)\n",
              option_ptr->option_name, option_buf);
      exit(EX_SOFTWARE);
    }
    while ((*w = *r) != '\0') w++, r++;
  }
  *w = '\0';

  while ((opt = getopt(argc, argv, option_buf)) != EOF) switch (opt) {
      case 'c':
        if (coco_dir != NULL) {
          fprintf(stderr,
                  "%s: cannot log to more than "
                  "one code-coverage directory! "
                  "(first %s, now %s)\n",
                  srv->srv_progname, coco_dir, optarg);
          srv_usage(srv->srv_progname, srv);
        }
        coco_dir = optarg;
        break;

      case 'f':
        if (config_name != NULL) {
          fprintf(stderr,
                  "%s: cannot read more than "
                  "one configuration file! (first %s, "
                  "now %s)\n",
                  srv->srv_progname, config_name, optarg);
          srv_usage(srv->srv_progname, srv);
        }
        config_name = optarg;
        break;

      case 'g':
        group_name = optarg;
        break;
      case 'h':
        srv_usage(srv->srv_progname, srv);
        break;

      case 'i':
        if (!(arg_i = cm_argvadd(arg_cm, arg_i, optarg))) {
          fprintf(stderr,
                  "%s: failed to allocate "
                  "command line parameter data: %s\n",
                  srv->srv_progname, srv_xstrerror(errno));
          exit(EX_OSERR);
        }
        break;

      case 'l':
        if (log_name != NULL) {
          fprintf(stderr,
                  "%s: cannot write to more than "
                  "one logfile! (first %s, now %s)\n",
                  srv->srv_progname, log_name, optarg);
          srv_usage(srv->srv_progname, srv);
        }
        log_name = optarg;
        break;

      case 'L':
        if (netlog_name != NULL) {
          fprintf(stderr,
                  "%s: cannot write to more than "
                  "one netlogfile! (first %s, now %s)\n",
                  srv->srv_progname, netlog_name, optarg);
          srv_usage(srv->srv_progname, srv);
        }
        netlog_name = optarg;
        break;

      case 'm':
        srv_list_modules(srv);
      /* NOTREACHED */

      case 'n':
        srv->srv_foreground = 1;
        break;
      case 'p':
        pid_file = optarg;
        break;
      case 'P':
        /* atoi() returns 0 if the string does not convert, which
         * is fine -- 0 is the default (disabled, single process)
         * value. 1 is effectively the same. See the top of
         * srv_initialize
         */
        errno = 0;
        proc_num = atoi(optarg);
        if (proc_num <= 0 || errno) {
          fprintf(stderr,
                  "%s: invalid number of processes: %s."
                  "Try a positive integer.\n",
                  srv->srv_progname, optarg);

          srv_usage(srv->srv_progname, srv);
        }
        break;
      case 'q':
        do_query = 1;
        break;
      case 't':
        srv->srv_trace = 1;
        break;
      case 'u':
        user_name = optarg;
        break;
      case 'V':
        if (cl_loglevel_configuration_from_string(
                optarg, optarg + strlen(optarg), srv->srv_app->app_facilities,
                &cnlc_buf)) {
          fprintf(stderr,
                  "%s: unexpected loglevel "
                  "in -v \"%s\"\n",
                  srv->srv_progname, optarg);
          srv_usage(srv->srv_progname, srv);
        } else {
          cnlc = &cnlc_buf;
        }
        break;

      case 'v': /*
                 * 	Basic syntax:
                 *  -v loglevel [ "[" loglevel "]" ]
                 *  First level triggers logging; second
                 *  is the "diary" that is sent along when
                 *  first is logged.
                 *
                 *  Shorthand:
                 *  -vv	detail
                 *  -vvv 	debug
                 *  -vvvv 	despair
                 */
        if (strcasecmp(optarg, "v") == 0) {
          clc = &clc_buf;
          clc->clc_full = clc->clc_trigger = CL_LEVEL_DETAIL;
        } else if (strcasecmp(optarg, "vv") == 0) {
          clc = &clc_buf;
          clc->clc_full = clc->clc_trigger = CL_LEVEL_DEBUG;
        } else if (strcasecmp(optarg, "vvv") == 0) {
          clc = &clc_buf;
          clc->clc_full = clc->clc_trigger = CL_LEVEL_VERBOSE;
        } else if (cl_loglevel_configuration_from_string(
                       optarg, optarg + strlen(optarg),
                       srv->srv_app->app_facilities, &clc_buf)) {
          fprintf(stderr,
                  "%s: unexpected loglevel "
                  "in -v \"%s\"\n",
                  srv->srv_progname, optarg);
          srv_usage(srv->srv_progname, srv);
        } else {
          clc = &clc_buf;
        }
        break;

      case 'W':
        want_core = false;
        break;
      case 'y':
        srv->srv_interactive = true;
        break;
      case 'x':
        srv_executable = optarg;
        break;
      case 'z':
        do_stop = 1;
        break;
      default:
        for (option_ptr = srv->srv_app->app_options;
             option_ptr && option_ptr->option_name; option_ptr++) {
          if (*option_ptr->option_name == opt) break;
        }

        if (!option_ptr || !option_ptr->option_name)
          srv_usage(srv->srv_progname, srv);

        if (option_ptr->option_static != NULL) {
          puts(option_ptr->option_static);
          exit(0);
        }

        err =
            option_ptr->option_set(srv->srv_app_data, srv, arg_cm, opt, optarg);

        if (err) srv_usage(srv->srv_progname, srv);
        break;
    }

  if (do_stop && do_query) {
    fprintf(stderr,
            "%s: specify at most one of -z "
            "(shutdown) or -q (query)\n",
            srv->srv_progname);
    exit(EX_USAGE);
  }

  /*  If trace is on, switch to tracer memory.
   */
  if ((cm = cm_c()) == NULL ||
      (srv->srv_trace && (cm = cm_trace(cm)) == NULL)) {
    fprintf(stderr, "%s: failed to allocate allocator!\n", srv->srv_progname);
    exit(EX_OSERR);
  }
  srv->srv_cm = cm;

  /* Initialize default logging to stderr.
   */
  srv->srv_cl = cl = cl_create();
  cl_set_loglevel_configuration(cl, clc ? clc : &srv_clc_default);

  /*  Create the (relatively unimportant) log stream on which
   *  the _diary itself_ logs.  (The diary can't log into cl,
   *  or we're looking at an endless loop...)
   */
  diary_cl = cl_create();
  if (diary_cl == NULL) {
    fprintf(stderr, "%s: can't create diary stream: %s\n", srv->srv_progname,
            srv_xstrerror(errno));
    return 0;
  }

  /*  If we leave the diary pointing to stderr, some of our
   *  output later may end up in the server database files.
   *  Not good.
   */
  cl_syslog(diary_cl, "libsrv/diary", LOG_USER);

  /*  We only ever want this for assertions and death rattles,
   *  so CL_LEVEL_ERROR is plenty.
   */
  cl_set_loglevel_full(diary_cl, CL_LEVEL_OPERATOR_ERROR);
  cl_set_loglevel_trigger(diary_cl, CL_LEVEL_ERROR);

  /*  Now that we have a debug stream for the diary to log
   *  to, create the diary.  Let's keep 64k of local memory.
   */
  d = cl_diary_create(diary_cl);
  if (d == NULL) {
    fprintf(stderr, "%s: can't create diary: %s\n", srv->srv_progname,
            srv_xstrerror(errno));
    return 0;
  }
  cl_diary_set_size(d, 64 * 1024);

  /*  Attach the diary as the short-term memory to our
   *  main log stream cl.
   */
  cl_set_diary(cl, d);

  /* Make the memory tracer log via <cl>, rather than stderr.
   */
  if (srv->srv_trace) cm_trace_set_log_callback(cm, srv_trace_log_callback, cl);

  /*  Configure code coverage logging.
   */
  if (coco_dir != NULL) {
    err = cl_set_coverage(cl, coco_dir);
    if (err != 0) {
      fprintf(stderr,
              "%s: failed to set code coverage"
              " directory to \"%s\": %s",
              srv->srv_progname, coco_dir, srv_xstrerror(err));
      exit(EX_OSERR);
    }
  }

  cl_assert(cl, app != NULL);
  cl_assert(cl, app->app_session_size >= sizeof(srv_session));
  cl_assert(cl, app->app_request_size >= sizeof(srv_request));
  cl_assert(cl, app->app_session_shutdown != NULL);
  cl_assert(cl, app->app_session_initialize != NULL);
  cl_assert(cl, app->app_request_input != NULL);
  cl_assert(cl, app->app_request_initialize != NULL);
  cl_assert(cl, app->app_request_output != NULL);
  cl_assert(cl, app->app_request_finish != NULL);

  if (config_name == NULL && app->app_default_conf_file != NULL &&
      (stat(app->app_default_conf_file, &st) == 0 || errno != ENOENT))
    config_name = app->app_default_conf_file;

  if (config_name != NULL && strcmp(config_name, "-") == 0) {
    config_name = NULL;
  }

  /* Read, or default, the configuration
   */
  if (config_name ? srv_config_read(srv, config_name, cm, cl, &srv->srv_config)
                  : !(srv->srv_config = srv_config_default(srv, cm, cl))) {
    exit(EX_OSERR);
  }

  /*  Override the log configuration with command line parameters,
   *  if we have some.
   */
  if (clc != NULL) srv->srv_config->cf_log_level = *clc;

  if (cnlc != NULL) {
    srv->srv_config->cf_netlog_level = *cnlc;
    srv_netlog_set_level(srv, cnlc);
  }

  /*  Make us log at the installed loglevel.
   */
  cl_set_loglevel_configuration(srv->srv_cl, &srv->srv_config->cf_log_level);

  if (!do_stop && !do_query) {
    /*  Override/add to the configuration from the command line.
     */
    if (srv->srv_interactive) {
      /*  Throw out whatever interfaces were defined
       *  in the configuration file, and replace them
       *  with the command line interface.
       */
      srv->srv_config->cf_interface_head = NULL;
      srv->srv_config->cf_interface_tail = &srv->srv_config->cf_interface_head;

      if (srv_interface_config_add(srv->srv_config, cl, NULL)) exit(EX_OSERR);
    } else if (arg_i) {
      for (a = arg_i; *a; a++) {
        if (srv_interface_config_add(srv->srv_config, cl, *a)) exit(EX_OSERR);
      }
      cm_argvfree(arg_cm, arg_i);
      arg_i = NULL;
    }

    srv_set_want_core(srv, want_core);
    cl_set_abort(cl, srv_cl_abort_callback, srv);
  }

  if (pid_file) {
    srv->srv_config->cf_pid_file = (char *)pid_file;
  }

  if (do_stop) {
    srv_epitaph *e = 0;

    err = srv_pidfile_kill(srv_program_name(srv), cl,
                           srv->srv_config->cf_pid_file
                               ? srv->srv_config->cf_pid_file
                               : SRV_PIDFILE_DEFAULT,
                           SIGTERM);
    if (err != 0) {
      if (err == EPERM) exit(EX_NOPERM);
      exit(EX_DATAERR);
    }

    if ((err = srv_epitaph_read(srv, arg_cm, &e)) == 0) {
      cl_assert(cl, e != NULL);

      if (e->epi_message != NULL && e->epi_message[0] != '\0') {
        char time_buf[200];
        struct tm loctm, *loctm_ptr;

        loctm_ptr = localtime_r(&e->epi_time, &loctm);
        if (loctm_ptr == NULL ||
            strftime(time_buf, sizeof time_buf, "%Y-%m-%d %H:%M:%S",
                     loctm_ptr) == 0)
          strcpy(time_buf, "???");

        fprintf(stderr, "%s[%lu]: %s (%s)\n", srv->srv_progname,
                (unsigned long)e->epi_pid, e->epi_message, time_buf);
      }
      exit(e->epi_exit);
    }
    exit(0);
  }
  if (do_query) {
    err = srv_pidfile_test(srv_program_name(srv), cl,
                           srv->srv_config->cf_pid_file
                               ? srv->srv_config->cf_pid_file
                               : SRV_PIDFILE_DEFAULT);

    if (err == ENOENT) {
      exit(1);
    }
    if (err == 0) {
      exit(0);
    }

    /*  In this case, srv_pidfile_test() logged an error message.
     */
    exit(2);
  }
  if (proc_num) {
    srv->srv_config->cf_processes = proc_num;
  }

  if (user_name) {
    err = srv_unixid_name_to_gid(user_name, &srv->srv_config->cf_user_id);
    if (err) {
      fprintf(stderr,
              "-u \"%s\": can't get Unix "
              "user ID for \"%s\": %s",
              user_name, user_name, srv_xstrerror(err));
      srv_usage(srv->srv_progname, srv);
      /* NOTREACHED */
    }
  }
  if (group_name) {
    err = srv_unixid_name_to_gid(group_name, &srv->srv_config->cf_group_id);
    if (err) {
      fprintf(stderr,
              "-g \"%s\": can't get Unix "
              "group ID for \"%s\": %s",
              group_name, group_name, srv_xstrerror(err));
      srv_usage(srv->srv_progname, srv);
      /* NOTREACHED */
    }
  }

  /*  For the application-defined command-line options,
   *  override the configuration value with the command
   *  line value, if one was defined.
   */
  for (option_ptr = srv->srv_app->app_options;
       option_ptr && option_ptr->option_name; option_ptr++) {
    if (option_ptr->option_configure != NULL) {
      err = (*option_ptr->option_configure)(srv->srv_app_data, srv,
                                            srv->srv_config->cf_app_data,
                                            srv->srv_config);
      if (err) exit(err);
    }
  }

  /*  Initialize all the sub-srv things (eg, pidfiles)
   *  and the database!
   */
  if ((err = srv_initialize(srv)) != 0) {
    exit(err);
  }

  if ((err = srv_initialize_runtime_logging(srv, log_name, netlog_name))) {
    srv_finish(srv, true);
    exit(err);
  }

  if (srv->srv_interactive) {
    srv_child(srv);
    /* NOTREACHED */
  }

  if (pipe(srv->srv_shutdown_pipe) || pipe(srv->srv_settle_pipe)) {
    fprintf(stderr, "%s: pipe: %s\n", srv->srv_progname, srv_xstrerror(errno));
    srv_finish(srv, true);
    exit(EX_OSERR);
  }

  /*  Configure the "netlog" log file with our
   *  host name prefix.
   */
  if (srv->srv_netlog != NULL) configure_netlog(srv);

  if (!srv->srv_foreground) {
    /* Background ourselves -- detach from the terminal.
     */
    if ((pid = fork()) == (pid_t)-1) {
      fprintf(stderr, "%s: fork failed: %s\n", srv->srv_progname,
              srv_xstrerror(errno));
      srv_finish(srv, true);
      exit(EX_OSERR);
    }
    if (pid != 0) {
      char *errstr = NULL;

      /*  Parent.  Move the pidfile to the child,
       *  wait for confirmation that the child has
       *  settled, then exit.
       */
      err = srv_pidfile_update(srv_program_name(srv), cl,
                               srv->srv_config->cf_pid_file
                                   ? srv->srv_config->cf_pid_file
                                   : SRV_PIDFILE_DEFAULT,
                               pid);

      if (err) {
        (void)kill(pid, SIGTERM);

        fprintf(stderr,
                "%s: could not update "
                "pid file: %s\n",
                srv->srv_progname, srv_xstrerror(err));

        (void)kill(pid, SIGKILL);
        srv_finish(srv, false);
        exit(EX_OSERR);
      }
      srv->srv_pid = pid;
      if (srv->srv_app->app_spawn != NULL) {
        err = (*srv->srv_app->app_spawn)(srv->srv_app_data, srv, pid);
        if (err != 0) {
          fprintf(stderr,
                  "%s: error preparing "
                  "subprocesses: %s\n",
                  srv->srv_progname, srv_xstrerror(err));
          (void)kill(pid, SIGTERM);
          srv_finish(srv, false);
          return err;
        }
      }

      if (srv_settle_wait(srv, &errstr) != 0) {
        fprintf(stderr, "%s: %s\n", srv->srv_progname,
                errstr ? errstr : "(null)");
        exit(EX_SOFTWARE);
      }
      exit(EX_OK);
    }

    /*  Child.
     */
    if (setpgid(getpid(), getpid())) {
      cl_log(cl, CL_LEVEL_ERROR, "%s: setpgid fails: %s", srv->srv_progname,
             srv_xstrerror(errno));
      srv_finish(srv, true);
      exit(EX_OSERR);
    }

    /*  Background.  Close TTY file descriptors; fork again, parent
     *  watches the child; child waits for parent to terminate.
     */
    (void)close(0);
    (void)close(1);
    (void)close(2);
  }

  pid = fork();
  if (pid == (pid_t)-1) {
    cl_log(cl, CL_LEVEL_ERROR, "%s: fork%s failed: %s", srv->srv_progname,
           srv->srv_foreground ? "" : "#2", srv_xstrerror(errno));
    srv_finish(srv, true);
    exit(EX_OSERR);
  } else if (pid == 0) {
    srv->srv_pid = getppid();
    srv_child(srv);
    /* NOTREACHED */
  }

/*  As a parent, we don't produce monitoring output
 *  - let the child do that.
 */
#ifdef USE_GPROF
  moncontrol(0);
#endif

  /*  Parent.  Close the read end of the shutdown pipe.  The write
   *  end will stay open as long as this process is awake.
   */
  srv->srv_pid = getpid();
  close(srv->srv_shutdown_pipe[0]);
  srv->srv_shutdown_pipe[0] = -1;

  /*  If we're in background mode, close the parent's settle pipe
   *  descriptors; the pipe is for communication between the child
   *  and the outermost layer.
   */
  if (!srv->srv_foreground) srv_settle_close(srv);

  err = srv_parent(srv, pid);
  srv_finish(srv, false);

  exit(err);
}

/**
 * @brief Return the program name of the executing server program
 * @param srv module handle
 * @return the program name, as a \\0-terminated string.
 */
char const *srv_program_name(srv_handle *srv) { return srv->srv_progname; }

/**
 * @brief Return the logging module for this handle.
 * @param srv module handle
 * @return the logging module for this handle.
 */
cl_handle *srv_log(srv_handle *srv) { return srv->srv_cl; }

cl_loglevel_configuration const *srv_loglevel_configuration(srv_handle *srv) {
  return &srv->srv_config->cf_log_level;
}

unsigned long long srv_smp_processes(srv_handle *srv) {
  return srv->srv_config->cf_processes;
}

void srv_set_smp_processes(srv_handle *srv, unsigned long long processes) {
  srv->srv_config->cf_processes = processes;
}
/**
 * @brief Return the netlog logging module for this handle.
 * @param srv module handle
 * @return the netlog module for this handle.
 */
cl_handle *srv_netlog(srv_handle *srv) { return srv->srv_netlog; }

/**
 * @brief Return the global memory manager for this handle.
 * @param srv module handle
 * @return the global memory manager for this handle.
 */
cm_handle *srv_mem(srv_handle *srv) { return srv->srv_cm; }

/**
 * @brief Return the file descriptor event manager for this handle.
 * @param srv Server.
 * @return the file descriptor event manager for this handle.
 */
es_handle *srv_events(srv_handle *srv) { return srv->srv_es; }

int srv_netlog_set_filename(srv_handle *srv, char const *filename) {
  int err = 0;

  if (srv->srv_netlog == NULL) {
    if (filename == NULL) return 0;

    if ((srv->srv_netlog = cl_create()) == NULL)
      cl_log(srv->srv_cl, CL_LEVEL_ERROR,
             "%s: cannot allocate cl-handle for "
             "netlog stream \"%s\": %s",
             srv->srv_progname, filename, srv_xstrerror(errno));
    else {
      char *filename_format = (char *)filename;
      bool free_filename = false;
      if (srv->srv_config->cf_processes > 1) {
        if (strstr(filename, "%$") == NULL) {
          filename_format = cm_sprintf(srv->srv_cm, "%s.%%$", filename);
          if (filename_format == NULL) {
            err = ENOMEM;
            cl_log(srv->srv_cl, CL_LEVEL_ERROR,
                   "%s: cannot open netlog "
                   "stream \"%s\": %s",
                   srv->srv_progname, filename, strerror(err));
            cl_destroy(srv->srv_netlog);
            srv->srv_netlog = NULL;
            return err;
          }

          free_filename = true;

          cl_log(srv->srv_cl, CL_LEVEL_OPERATOR_ERROR,
                 "Running an SMP server without netlogs "
                 "in multiple files. Appending the pid. ");
        }
      }

      if ((err = cl_netlog3(srv->srv_netlog, filename_format)) == 0) {
        cl_log(srv->srv_cl, CL_LEVEL_INFO,
               "%s: logging to netlog stream \"%s\"", srv->srv_progname,
               filename_format);

        cl_set_loglevel_configuration(srv->srv_netlog,
                                      &srv->srv_config->cf_netlog_level);
        cl_set_flush_policy(srv->srv_netlog, srv->srv_config->cf_log_flush);
        configure_netlog(srv);
      } else {
        cl_log(srv->srv_cl, CL_LEVEL_ERROR,
               "%s: cannot open netlog "
               "stream \"%s\": %s",
               srv->srv_progname, filename_format, strerror(err));

        cl_destroy(srv->srv_netlog);
        srv->srv_netlog = NULL;
      }
      if (free_filename) cm_free(srv->srv_cm, filename_format);
    }
  } else {
    if (filename == NULL) {
      cl_destroy(srv->srv_netlog);
      srv->srv_netlog = NULL;
    } else {
      err = cl_netlog_set_filename(srv->srv_netlog, filename);
      if (err != 0)
        cl_log(srv->srv_cl, CL_LEVEL_ERROR,
               "%s: cannot switch netlog stream to "
               "\"%s\": %s",
               srv->srv_progname, filename, strerror(err));
    }
  }
  return err;
}

/* Run the callback to the application for startup complete */

void srv_run_startup_complete_callback(srv_handle *srv) {
  int err = 0;
  srv_config_parameter const *app_cf;

  cl_assert(srv->srv_cl, srv->srv_startup_is_complete);

  app_cf = srv->srv_app->app_config_parameters;
  if (srv->srv_app->app_startup_complete != NULL) {
    err = (*srv->srv_app->app_startup_complete)(srv->srv_app_data, srv);
    if (err != 0) {
      cl_log_errno(srv->srv_cl, CL_LEVEL_FAIL, "%s->app_startup_complete", err,
                   app_cf && app_cf->config_name ? app_cf->config_name
                                                 : srv->srv_app->app_name,
                   cl_strerror(srv->srv_cl, err));

      srv_shutdown_now(srv);
      srv_epitaph_print(srv, EX_SOFTWARE,
                        "startup_complete callback fails for %s: %s",
                        app_cf && app_cf->config_name ? app_cf->config_name
                                                      : srv->srv_app->app_name,
                        cl_strerror(srv->srv_cl, err));
      srv_finish(srv, true);
      exit(EX_SOFTWARE);
    }
  }
}

/* Indicate to libsrv if we are ready to run
   our startup complete callback
 */
void srv_startup_now_complete(srv_handle *srv) {
  if (!srv->srv_startup_is_complete) {
    srv->srv_startup_is_complete = true;

    /*  Break our es_loop to complete -- does nothing
     *  if we're not in one.
     */
    if (srv->srv_es != NULL) es_break(srv->srv_es);
  }
}

void srv_cleanup_and_finish(srv_handle *srv) {
  srv_shutdown_now(srv);
  srv_finish(srv, true);

  if (srv->srv_trace) cm_trace_list(srv->srv_cm);
}

int srv_log_set_filename(srv_handle *srv, char const *filename) {
  int err = 0;

  if (srv->srv_cl == NULL) {
    if (filename == NULL) return 0;

    if ((srv->srv_cl = cl_create()) == NULL) return errno ? errno : ENOMEM;

    cl_set_loglevel_configuration(srv->srv_cl, &srv->srv_config->cf_log_level);
    cl_set_flush_policy(srv->srv_cl, srv->srv_config->cf_log_flush);
  }

  if ((err = cl_file_set_name(srv->srv_cl, filename)) != 0)
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "%s: cannot set log filename to \"%s\": %s", srv->srv_progname,
           filename, strerror(err));
  return err;
}

int srv_netlog_set_level(srv_handle *srv,
                         cl_loglevel_configuration const *clc) {
  int err = 0;

  srv->srv_config->cf_netlog_level = *clc;

  if (srv->srv_netlog != NULL)
    cl_set_loglevel_configuration(srv->srv_netlog, clc);
  return err;
}

bool srv_is_shutting_down(srv_handle *srv) {
  return srv->srv_shutdown_begun == 1;
}

void srv_set_max_restart_count(srv_handle *srv, int count) {
  srv->srv_max_restarts = count;
}

int srv_log_set_level(srv_handle *srv, cl_loglevel_configuration const *clc) {
  int err = 0;

  srv->srv_config->cf_log_level = *clc;
  if (srv->srv_cl != NULL) cl_set_loglevel_configuration(srv->srv_cl, clc);

  return err;
}
