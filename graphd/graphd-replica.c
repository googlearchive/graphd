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
#include "graphd/graphd.h"

#include <errno.h>
#include <stdio.h>    /* EOF */
#include <sysexits.h> /* EX_OSERR */

#include "libsrv/srv.h"

/*  Up to this many sessions can be queued waiting for their write-through
 *  to go through before we stop creating new requests.
 */
#define MAX_WRITETHROUGH_PENDING 8

/*  Attempt to reconnect to a missing master in this many seconds.
 */
#define REPLICA_RECONNECT_DELAY 10

/* Timeout an incoming replica stream after inactivity for this many seconds
 */
#define GRAPHD_REPLICA_TIMEOUT_SECONDS (10 * 60)

#define IS_LIT(lit, s, e) \
  ((e) - (s) == sizeof lit - 1 && !strncasecmp(lit, s, sizeof lit - 1))

static const cm_list_offsets graphd_ses_offsets =
    CM_LIST_OFFSET_INIT(graphd_session, gses_data.gd_rep_client.gdrc_next,
                        gses_data.gd_rep_client.gdrc_prev);

#define GRAPHD_RESTORE_LAG_MAX (128 * 1024)

static int enqueue_catch_up(graphd_session* gses);

static graphd_replica_config* graphd_replica_config_alloc(
    cm_handle* cm, cl_handle* cl, char const* address_s,
    char const* address_e) {
  graphd_replica_config* rcf = cm_malloc(cm, sizeof *rcf);
  size_t const address_n = address_s ? (size_t)(address_e - address_s) : 0;

  if (!rcf) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "cm_malloc", errno,
                 "%.*s: failed to allocate %lu bytes for "
                 "replica configuration structure",
                 (int)address_n, address_s, (unsigned long)sizeof *rcf);

    return NULL;
  }

  if (0 == address_n)
    rcf->rcf_master_address = NULL;
  else {
    int err = srv_address_create_url(cm, cl, address_s, address_e,
                                     &rcf->rcf_master_address);
    if (err) {
      cm_free(cm, rcf);
      return NULL;
    }
  }
  rcf->rcf_archive = false;

  return rcf;
}

int graphd_archive_config_read(void* data, srv_handle* srv, void* config_data,
                               srv_config* srv_cf, char** s, char const* e) {
  cl_handle* const cl = srv_log(srv);
  graphd_config* const gcf = config_data;
  int err;

  err = graphd_replica_config_read(data, srv, config_data, srv_cf, s, e);
  if (!err) {
    cl_assert(cl, gcf->gcf_replica_cf);
    gcf->gcf_replica_cf->rcf_archive = true;
  }

  return err;
}

/**
 * @brief Parse an option from the configuration file.  (Method.)
 *
 *  This is a method of the generic libsrv parameter mechanism,
 *  passed in via a srv_config_parameter[] structure declared in graphd.c.
 *
 * @param data		opaque application data handle (i.e., graphd)
 * @param srv 		generic libsrv handle
 * @param config_data	opaque application config data (i.e., graphd_config)
 * @param srv_cf	generic libsrv parameters
 * @param s		in/out: current position in the configuration file
 * @param e		in: end of the buffered configuration file
 *
 * @return 0 on success, a nonzero errno on error.
 */
int graphd_replica_config_read(void* data, srv_handle* srv, void* config_data,
                               srv_config* srv_cf, char** s, char const* e) {
  cm_handle* const cm = srv_config_mem(srv_cf);
  cl_handle* const cl = srv_log(srv);
  graphd_config* const gcf = config_data;
  graphd_replica_config* rcf;
  char const* tok_s;
  char const* tok_e;
  char const* host_s = 0;
  char const* host_e = 0;
  char const* port_s = 0;
  char const* port_e = 0;
  int tok;
  int err;

  cl_assert(cl, data);
  cl_assert(cl, config_data);
  cl_assert(cl, srv_cf);
  cl_assert(cl, gcf);

  tok = srv_config_get_token(s, e, &tok_s, &tok_e);
  if ('{' != tok) {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "configuration file \"%s\", line %d: expected "
           "address to replicate from, got \"%.*s\"\n",
           srv_config_file_name(srv_cf), srv_config_line_number(srv_cf, e),
           (int)(tok_e - tok_s), tok_s);

    return GRAPHD_ERR_SYNTAX;
  }

  tok = srv_config_get_token(s, e, &tok_s, &tok_e);
  while ('}' != tok && EOF != tok) {
    switch (*tok_s) {
      case 'H':
      case 'h':
        if (!IS_LIT("host", tok_s, tok_e)) {
        unknown_identifier:
          cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
                 "configuration file \"%s\", "
                 "line %d: expected \"host\" or "
                 "\"port\" in replica statement, "
                 "got \"%.*s\"",
                 srv_config_file_name(srv_cf),
                 srv_config_line_number(srv_cf, *s), (int)(tok_e - tok_s),
                 tok_s);
          return GRAPHD_ERR_SYNTAX;
        }
        if (host_s) {
          cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
                 "configuration file \"%s\", "
                 "line %d: duplicate \"host\" "
                 "(%.*s and %.*s) ",
                 srv_config_file_name(srv_cf),
                 srv_config_line_number(srv_cf, *s), (int)(host_e - host_s),
                 host_s, (int)(tok_e - tok_s), tok_s);

          return GRAPHD_ERR_SYNTAX;
        }

        /* Get the actual hostname. */
        tok = srv_config_get_token(s, e, &tok_s, &tok_e);
        if (tok != '"' && tok != 'a') {
          cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
                 "configuration file \"%s\", "
                 "line %d: expected "
                 "IP address or host name, "
                 "got \"%.*s\"\n",
                 srv_config_file_name(srv_cf),
                 srv_config_line_number(srv_cf, *s), (int)(tok_e - tok_s),
                 tok_s);
          return GRAPHD_ERR_SYNTAX;
        }
        host_s = tok_s;
        host_e = tok_e;
        break;

      case 'P':
      case 'p':
        if (!IS_LIT("port", tok_s, tok_e)) goto unknown_identifier;
        if (port_s) {
          cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
                 "configuration file \"%s\", "
                 "line %d: duplicate \"port\" "
                 "(%.*s and %.*s) ",
                 srv_config_file_name(srv_cf),
                 srv_config_line_number(srv_cf, *s), (int)(port_e - port_s),
                 port_s, (int)(tok_e - tok_s), tok_s);

          return GRAPHD_ERR_SYNTAX;
        }

        /* Get the actual port. */
        tok = srv_config_get_token(s, e, &tok_s, &tok_e);
        if (tok != '"' && tok != 'a') {
          cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
                 "configuration file \"%s\", "
                 "line %d: expected port, "
                 "got \"%.*s\"\n",
                 srv_config_file_name(srv_cf),
                 srv_config_line_number(srv_cf, *s), (int)(tok_e - tok_s),
                 tok_s);
          return GRAPHD_ERR_SYNTAX;
        }
        port_s = tok_s;
        port_e = tok_e;
        break;

      default:
        goto unknown_identifier;
    }

    tok = srv_config_get_token(s, e, &tok_s, &tok_e);
  }

  if (EOF == tok) {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "configuration file \"%s\", "
           "line %d: EOF in replica {...} section",
           srv_config_file_name(srv_cf), srv_config_line_number(srv_cf, *s));
    return GRAPHD_ERR_SYNTAX;
  }

  rcf = graphd_replica_config_alloc(cm, cl, NULL, NULL);
  if (!rcf) return errno ? errno : ENOMEM;

  err = srv_address_create_host_port(cm, cl, host_s, host_e, port_s, port_e,
                                     &rcf->rcf_master_address);
  if (err) {
    cm_free(cm, rcf);
    return err;
  }
  gcf->gcf_replica_cf = rcf;

  return 0;
}

int graphd_replica_config_open(void* data, srv_handle* srv, void* config_data,
                               srv_config* srv_cf) {
  cl_handle* const cl = srv_log(srv);
  graphd_handle* const g = data;
  graphd_config* gcf = config_data;
  graphd_replica_config* rcf;

  cl_assert(cl, data);
  cl_assert(cl, config_data);

  rcf = gcf->gcf_replica_cf;
  if (!rcf) return 0;

  g->g_rep_master_address = rcf->rcf_master_address;

  return 0;
}

/* Start the configured replica
 */
int graphd_replica_config_run(void* data, srv_handle* srv, void* config_data,
                              srv_config* srv_cf) {
  graphd_handle* g = data;
  graphd_config* const gcf = config_data;

  if (!gcf->gcf_replica_cf) return 0; /* no replica configuration */

  /*  This sets the desired access.  The startup code
   *  in graphd_startup() will actually establish the
   *  connection.
   */
  g->g_access = gcf->gcf_replica_cf->rcf_archive ? GRAPHD_ACCESS_ARCHIVE
                                                 : GRAPHD_ACCESS_REPLICA;
  return 0;
}

/**
 * @brief Parse an option from the command line.  (Method.)
 *
 *  This is a method of the generic libsrv parameter mechanism,
 *  passed in via a srv_option[] structure declared in graphd.c.
 *
 * @param data		opaque application data handle (i.e., graphd)
 * @param srv 		generic libsrv handle
 * @param cm		allocate through this
 * @param opt		command line option ('d')
 * @param optarg	option's parameter
 *
 * @return 0 on success, a nonzero errno on error.
 */
int graphd_replica_option_set_required(void* data, srv_handle* srv,
                                       cm_handle* cm, int opt,
                                       char const* opt_arg) {
  graphd_handle* g = data;

  if (g->g_rep_arg) {
    fprintf(stderr, "%s: duplicate replica %s, original: %s",
            srv_program_name(srv), g->g_rep_arg, opt_arg);
    exit(EX_OSERR);
  }

  g->g_rep_arg = opt_arg;
  g->g_require_replica_connection_for_startup = true;
  return 0;
}

/**
 * @brief Parse an option from the command line.  (Method.)
 *
 *  This is a method of the generic libsrv parameter mechanism,
 *  passed in via a srv_option[] structure declared in graphd.c.
 *
 * @param data		opaque application data handle (i.e., graphd)
 * @param srv 		generic libsrv handle
 * @param cm		allocate through this
 * @param opt		command line option ('d')
 * @param optarg	option's parameter
 *
 * @return 0 on success, a nonzero errno on error.
 */
int graphd_replica_option_set_not_required(void* data, srv_handle* srv,
                                           cm_handle* cm, int opt,
                                           char const* opt_arg) {
  graphd_handle* g = data;

  if (g->g_rep_arg) {
    fprintf(stderr, "%s: duplicate replica %s, original: %s",
            srv_program_name(srv), g->g_rep_arg, opt_arg);
    exit(EX_OSERR);
  }

  g->g_rep_arg = opt_arg;
  g->g_require_replica_connection_for_startup = false;
  return 0;
}

/**
 * @brief Override a replica configuration with a command line option.
 *
 *  This is a method of the generic libsrv parameter mechanism,
 *  passed in via a srv_option[] structure declared in graphd.c.
 *
 * @param data		opaque application data handle (i.e., graphd)
 * @param srv 		generic libsrv handle
 * @param config_data	opaque application config data (i.e., graphd_config)
 * @param srv_config_data	generic libsrv config data
 *
 * @return 0 on success, a nonzero errno on error.
 */
int graphd_replica_option_configure(void* data, srv_handle* srv,
                                    void* config_data,
                                    srv_config* srv_config_data) {
  graphd_handle* const g = data;
  graphd_config* const gcf = config_data;

  if (g->g_rep_arg) {
    cm_handle* const cm = srv_config_mem(srv_config_data);
    cl_handle* const cl = srv_log(srv);
    int err;

    if (!gcf->gcf_replica_cf) {
      gcf->gcf_replica_cf = graphd_replica_config_alloc(
          cm, cl, g->g_rep_arg, g->g_rep_arg + strlen(g->g_rep_arg));
      if (!gcf->gcf_replica_cf) return ENOMEM;
    } else {
      if (gcf->gcf_replica_cf->rcf_master_address) {
        srv_address_destroy(gcf->gcf_replica_cf->rcf_master_address);
        gcf->gcf_replica_cf->rcf_master_address = NULL;
      }
      err = srv_address_create_url(cm, cl, g->g_rep_arg,
                                   g->g_rep_arg + strlen(g->g_rep_arg),
                                   &gcf->gcf_replica_cf->rcf_master_address);
      if (err) return err;
    }
  }

  return 0;
}

/**
 * @brief Parse an option from the command line.  (Method.)
 *
 *  This is a method of the generic libsrv parameter mechanism,
 *  passed in via a srv_option[] structure declared in graphd.c.
 *
 * @param data		opaque application data handle (i.e., graphd)
 * @param srv 		generic libsrv handle
 * @param cm		allocate through this
 * @param opt		command line option ('d')
 * @param optarg	option's parameter
 *
 * @return 0 on success, a nonzero errno on error.
 */

int graphd_write_master_option_set(void* data, srv_handle* srv, cm_handle* cm,
                                   int opt, char const* opt_arg) {
  graphd_handle* g = data;

  if (g->g_rep_write_arg) {
    fprintf(stderr, "%s: duplicate replica %s, original: %s",
            srv_program_name(srv), g->g_rep_write_arg, opt_arg);
    exit(EX_OSERR);
  }

  g->g_rep_write_arg = opt_arg;

  return 0;
}

static void graphd_replica_reconnect_callback(void* data,
                                              es_idle_callback_timed_out mode) {
  graphd_handle* g = (graphd_handle*)data;
  int err;

  g->g_rep_reconnect_delay = 0;

  if (ES_IDLE_CANCEL == mode) return;

  if (GRAPHD_ACCESS_REPLICA != g->g_access &&
      GRAPHD_ACCESS_ARCHIVE != g->g_access)
    return;

  if (g->g_rep_master) {
    cl_log(g->g_cl, CL_LEVEL_ERROR,
           "Replication: reconnect callback found existing connection");
    return;
  }

  cl_log(g->g_cl, CL_LEVEL_INFO, "Replication: reconnecting");

  err = graphd_replica_connect(g);
  if (err) {
    /* Our connect attempt failed, enqueue another one
     */
    graphd_replica_schedule_reconnect(g);
  }
}

void graphd_replica_schedule_reconnect(graphd_handle* g) {
  if (g->g_rep_reconnect_delay) return;

  g->g_rep_reconnect_delay = srv_delay_create(
      g->g_srv, REPLICA_RECONNECT_DELAY, REPLICA_RECONNECT_DELAY,
      graphd_replica_reconnect_callback, g, "replica reconnect delay");
  if (g->g_rep_reconnect_delay == NULL)
    cl_log_errno(g->g_cl, CL_LEVEL_ERROR, "srv_delay_create",
                 errno ? errno : ENOMEM,
                 "Unable to allocate reconnect delay,"
                 "replica will NOT reconnect.");
  else
    cl_log(g->g_cl, CL_LEVEL_INFO, "Replica reconnect scheduled in %d seconds",
           REPLICA_RECONNECT_DELAY);
}

/* A connection is going away.  If it is related to the replication system,
 * keep data structures consistent and schedule a reconnect attempt if
 * need be.
 */
void graphd_replica_session_shutdown(graphd_session* gses) {
  cl_handle* const cl = gses->gses_cl;
  graphd_handle* const g = gses->gses_graphd;

  /* a connection from a replica?
   */
  if (gses->gses_type == GRAPHD_SESSION_REPLICA_CLIENT) {
    cl_log(gses->gses_cl, CL_LEVEL_FAIL, "Shutdown replica connection: %s",
           gses->gses_ses.ses_displayname);
    cm_list_remove(graphd_session, graphd_ses_offsets,
                   &gses->gses_graphd->g_rep_sessions, 0, gses);
    gses->gses_type = GRAPHD_SESSION_UNSPECIFIED;
  } else if (g->g_rep_master == gses) {
    g->g_rep_master = 0;

    /* Disassociate the timeout with the session */
    srv_session_set_timeout(&gses->gses_ses, NULL);

    /* We would like to terminate graphd immediately
     * if it appears to be misconfigured.  Anytime
     * the replica server drops the connection immediately
     * after sending the "replica" command we suspect a
     * configuration problem.
     */
    if (!g->g_rep_ever_connected && /* we've never connected */
        g->g_rep_replica_sent &&    /* we sent the "replica" */
        !g->g_rep_write_address)    /* but got no response */
    {
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "Replication master %s dropped connection "
             "after \"replica\" command.  Configuration "
             "problem or incorrect start id",
             gses->gses_ses.ses_displayname);
      srv_shared_set_restart(gses->gses_ses.ses_srv, false);
      srv_epitaph_print(gses->gses_ses.ses_srv, EX_GRAPHD_REPLICA_MASTER,
                        "Replication master %s dropped connection "
                        "after \"replica\" command.  Configuration "
                        "problem or incorrect start id",
                        gses->gses_ses.ses_displayname);
      exit(EX_GRAPHD_REPLICA_MASTER);
    }

    if (GRAPHD_ACCESS_REPLICA != g->g_access &&
        GRAPHD_ACCESS_ARCHIVE != g->g_access)
      return;

    graphd_replica_schedule_reconnect(g);
  } else if (g->g_rep_write == gses) {
    cl_log(cl, CL_LEVEL_DEBUG, "Lost replica writethrough connection %s",
           gses->gses_ses.ses_displayname);
    graphd_writethrough_session_fail(g);
    g->g_rep_write = 0; /* (re-)connection happens on demand */
  }
}

/* Is this session being used to run the replication protocol?
 */
bool graphd_replica_protocol_session(graphd_session* gses) {
  return gses->gses_type == GRAPHD_SESSION_REPLICA_CLIENT ||
         gses->gses_graphd->g_rep_master == gses;
}

static void format_replica_write(void* data, srv_handle* srv,
                                 void* session_data, void* request_data,
                                 char** s, char* e) {
  static char const rw[] = "replica-write ";
  size_t const rw_size = sizeof rw - 1;

  graphd_handle* const g = data;
  graphd_session* const gses = session_data;
  graphd_request* const greq = request_data;
  cl_handle* const cl = srv_log(srv);

  char* p = *s;

  cl_assert(cl, gses);
  cl_assert(cl, greq);
  cl_assert(cl, g);

  if (e - p < rw_size) return;
  memcpy(p, rw, rw_size);
  p += rw_size;
  *s = p;

  greq->greq_format = graphd_format_result;
}

/* Enqueue a "replica-write" command
 */
static int enqueue_replica_write(graphd_handle* g, graphd_session* gses,
                                 pdb_id start, pdb_id end) {
  cl_handle* const cl = gses->gses_cl;
  graphd_value* val;
  graphd_request* rep_req;
  int err = 0;

  if (start == end) {
    cl_log(cl, CL_LEVEL_ERROR,
           "Replication session %s "
           " start == end (%llu).  That's odd.",
           gses->gses_ses.ses_displayname, (unsigned long long)start);

    return 0;
  }

  rep_req = graphd_request_create_asynchronous(
      gses, GRAPHD_REQUEST_ASYNC_REPLICA_WRITE, format_replica_write);
  if (!rep_req) return ENOMEM;
  rep_req->greq_request_start_hint = "[asynchronous replica write]";

  graphd_request_start(rep_req);
  graphd_runtime_statistics_start_request(rep_req);

  err = graphd_value_list_alloc(g, rep_req->greq_req.req_cm, cl,
                                &rep_req->greq_reply, 3);
  if (err) {
    cl_log_errno(g->g_cl, CL_LEVEL_ERROR, "graphd_value_list_alloc", err,
                 "Unable to allocate value for replica-write");
    graphd_request_served(rep_req);
    srv_session_abort(&gses->gses_ses);

    return err;
  }
  val = rep_req->greq_reply.val_list_contents;
  graphd_value_number_set(val++, start);
  graphd_value_number_set(val++, end);
  graphd_value_records_set(val, g->g_pdb, start, end - start);

  err = graphd_format_stack_push(gses, rep_req, &rep_req->greq_reply);

  graphd_request_served(rep_req);

  if (err) {
    cl_log_errno(g->g_cl, CL_LEVEL_ERROR, "graphd_format_stack_push", err,
                 "Unable to push replica-write on format stack");
    srv_session_abort(&gses->gses_ses);
    return err;
  }

  gses->gses_data.gd_rep_client.gdrc_next_id = end;
  return 0;
}

/*  Locate an enqueued replica-write which we can modify.
 *
 *  This is called from within a replica master.  We try
 *  to bundle writes together, to avoid flushing to disk
 *  between sequential writes.
 */
static graphd_request* find_modifiable_replica_write(graphd_session* gses,
                                                     pdb_id start, pdb_id end) {
  cl_handle* const cl = gses->gses_cl;
  graphd_request* req = (graphd_request*)gses->gses_ses.ses_request_head;
  graphd_request* last_req = 0;
  int r_count = 0;

  while (req) {
    if (GRAPHD_REQUEST_ASYNC_REPLICA_WRITE == req->greq_request) {
      r_count++;
      last_req = req;
    } else if (!GRAPHD_REQUEST_IS_REPLICA(req->greq_request)) {
      cl_log(cl, CL_LEVEL_ERROR,
             "Unexpected request (%d) in replication session. "
             "Odd request: <%s> Session: %s",
             (int)req->greq_request, req->greq_req.req_display_id,
             req->greq_req.req_session->ses_displayname);
    }
    req = (graphd_request*)req->greq_req.req_next;
  }
  if (r_count > 1) return last_req;

  return 0;
}

/* Ensure that a replica-write command is sent to the graphd
 * at the other end of, gses.  If there's already a replica-write
 * in the queue (and we're not in the process of sending it) we
 * can just modify its start/end.
 */
static int send_replica_write(graphd_handle* g, graphd_session* gses,
                              pdb_id start, pdb_id end) {
  cl_handle* const cl = gses->gses_cl;
  graphd_request* req = find_modifiable_replica_write(gses, start, end);
  pdb_id old_start;
  graphd_value* val;

  if (req == NULL) return enqueue_replica_write(g, gses, start, end);

  val = req->greq_reply.val_list_contents;

  /*  We made this request - so we can guarantee at least
   *  its shape.
   */
  cl_assert(cl, val != NULL && val->val_type == GRAPHD_VALUE_NUMBER);

  old_start = val->val_data.data_number;
  val++;

  if (old_start >= start) {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "send_replica_write %s: old start (%llu) >= new start (%llu)",
           gses->gses_ses.ses_displayname, (unsigned long long)old_start,
           (unsigned long long)start);

    return GRAPHD_ERR_SEMANTICS;
  }
  if (val->val_data.data_number != start) {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "send_replica_write %s: old end (%llu) != new start (%llu)",
           gses->gses_ses.ses_displayname,
           (unsigned long long)val->val_data.data_number,
           (unsigned long long)start);

    return GRAPHD_ERR_SEMANTICS;
  }
  graphd_value_number_set(val++, end);
  graphd_value_records_set(val, g->g_pdb, old_start, end - old_start);
  if (end - old_start > GRAPHD_RESTORE_LAG_MAX) {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "send_replica_write %s: has fallen %llu primitives behind. "
           "Call me back when you can listen!",
           gses->gses_ses.ses_displayname,
           (unsigned long long)(end - old_start));
    return GRAPHD_ERR_NO;
  }
  cl_log(cl, CL_LEVEL_INFO,
         "Coalescing replica-write(%llu, %llu) with existing "
         "replica-write(%llu, %llu)",
         (unsigned long long)start, (unsigned long long)end,
         (unsigned long long)old_start, (unsigned long long)start);
  return 0;
}

static int pr_starts_tx(graphd_handle* g, pdb_id id, bool* is_tx_start) {
  pdb_primitive pr;
  int err;

  pdb_primitive_initialize(&pr);
  err = pdb_id_read(g->g_pdb, id, &pr);
  if (err) return err;
  *is_tx_start = pdb_primitive_is_txstart(&pr);
  pdb_primitive_finish(g->g_pdb, &pr);

  return 0;
}

static void abort_all_rep_sessions(graphd_handle* g) {
  graphd_session* gses = g->g_rep_sessions;

  while (gses) {
    graphd_session* const next_ses = gses->gses_data.gd_rep_client.gdrc_next;

    srv_session_abort(&gses->gses_ses);
    gses = next_ses;
  }
}

/*	Called from graphd_replicate_primitives, below, actually sends the
 *	primitives to the replicas either now, or after the delay fires.
 *
 *	Since we keep no state on the replication session, any
 *	error will cause the session to be killed.  The replica is
 *	expected to reconnect.
 */

static void replicate_primitive_horizon_callback(
    void* data, es_idle_callback_timed_out mode) {
  bool is_tx_start = false;
  int err;
  graphd_handle* g = data;
  graphd_session* gses = g->g_rep_sessions;
  pdb_id start = g->g_rep_write_delay_horizon_start;
  pdb_id end = g->g_rep_write_delay_horizon_end;
  cl_log(g->g_cl, CL_LEVEL_DEBUG, "Replicating from %llu to %llu", start, end);

  if (!gses || start == end) return; /* no replicas or nothing to replicate */

  while (gses) {
    graphd_session* const next_ses = gses->gses_data.gd_rep_client.gdrc_next;

    cl_assert(g->g_cl, gses->gses_data.gd_rep_client.gdrc_next_id <= end);

    if (gses->gses_data.gd_rep_client.gdrc_next_id != start) {
      if (gses->gses_data.gd_rep_client.gdrc_next_id == end)
        goto next_session; /* nothing to replicate */

      err = pr_starts_tx(g, gses->gses_data.gd_rep_client.gdrc_next_id,
                         &is_tx_start);
      if (err || !is_tx_start) {
        if (err)
          cl_log_errno(
              g->g_cl, CL_LEVEL_ERROR, "pr_starts_tx", err,
              "Unable to determine if primitive %llx, next for %s,"
              "starts transaction (next_id=%llx)",
              (unsigned long long)gses->gses_data.gd_rep_client.gdrc_next_id,
              gses->gses_ses.ses_displayname,
              (unsigned long long)pdb_primitive_n(g->g_pdb));
        else
          cl_log(g->g_cl, CL_LEVEL_ERROR,
                 "Transaction boundary out of sync at %llu, "
                 "dumping replication session %s",
                 (unsigned long long)gses->gses_data.gd_rep_client.gdrc_next_id,
                 gses->gses_ses.ses_displayname);

        srv_session_abort(&gses->gses_ses);

        goto next_session;
      }
    }

    if (gses->gses_data.gd_rep_client.gdrc_next_id > start)
      cl_log(g->g_cl, CL_LEVEL_ERROR,
             "replication session %s: ignoring redundant write "
             "ses->next_id=%llx, start=%llx, end=%llx",
             gses->gses_ses.ses_displayname,
             (unsigned long long)gses->gses_data.gd_rep_client.gdrc_next_id,
             (unsigned long long)start, (unsigned long long)end);

    err = send_replica_write(g, gses,
                             gses->gses_data.gd_rep_client.gdrc_next_id, end);
    if (err) {
      cl_log_errno(
          g->g_cl, CL_LEVEL_ERROR, "send_replica_write", err,
          "Unable to write primitives [%llx, %llx) to %s",
          (unsigned long long)gses->gses_data.gd_rep_client.gdrc_next_id,
          (unsigned long long)end, gses->gses_ses.ses_displayname);

      srv_session_abort(&gses->gses_ses);

      goto next_session;
    }

    gses->gses_data.gd_rep_client.gdrc_next_id = end;

  next_session:
    gses = next_ses;
  }

  // Clear the delay, if any
  g->g_rep_write_delay = NULL;
}

/* Send the indicated range of primitives to all replicas, with delay
 *
 *	We verify that the first primitive starts a transaction, but
 *	we lump all transactions into a single write.  There doesn't
 *	seem to be any compelling reason to preserve individual
 *	transaction boundaries, conversely, lumping transactions together
 *	mimics the graphd write semantics and will be substantially faster.
 *
 *	After determining whether to delay the replica writes (batching them
 *	up for speed reasons, based on -Z) we then call the above function
 *	to actually do the sending.
 *
 *	Since we keep no state on the replication session, any
 *	error will cause the session to be killed.  The replica is
 *	expected to reconnect.
 */

void graphd_replicate_primitives(graphd_handle* g, pdb_id start, pdb_id end) {
  graphd_session* gses = g->g_rep_sessions;
  bool is_tx_start = false;
  int err;

  cl_assert(g->g_cl, start <= end);

  if (!gses || start == end) return; /* no replicas or nothing to replicate */

  err = pr_starts_tx(g, start, &is_tx_start);
  if (err || !is_tx_start) {
    /* In either case things are in pretty bad shape but
     * we might be able to continue if replicas re-attach.
     */
    if (err)
      cl_log_errno(g->g_cl, CL_LEVEL_ERROR, "pr_starts_tx", err,
                   "Unable to determine if primitive %llx, next for %s,"
                   "starts transaction",
                   (unsigned long long)start, gses->gses_ses.ses_displayname);
    else {
      cl_log(g->g_cl, CL_LEVEL_ERROR,
             "Transaction boundary out of sync at %llu, "
             "dumping replication sessions",
             (unsigned long long)start);
    }

    abort_all_rep_sessions(g);

    return;
  }

  if (g->g_should_delay_replica_writes) {
    // Delay it!

    if (g->g_rep_write_delay != NULL) {
      // We're already waiting. Update the range.
      if (g->g_rep_write_delay_horizon_end != start) {
        // Something is wrong.
        cl_notreached(g->g_cl, "Delay skipped a couple primitives. Crashing.");
      }
      g->g_rep_write_delay_horizon_end = end;
      cl_log(g->g_cl, CL_LEVEL_DETAIL,
             "graphd_replicate_primitives: "
             "Delaying replica write. Still within timer. new start=%llx"
             " end=%llx",
             (unsigned long long)g->g_rep_write_delay_horizon_start,
             (unsigned long long)g->g_rep_write_delay_horizon_end);
    } else {
      // Begin the delay
      g->g_rep_write_delay_horizon_start = start;
      g->g_rep_write_delay_horizon_end = end;
      g->g_rep_write_delay = srv_delay_create(
          g->g_srv, g->g_delay_replica_writes_secs,
          g->g_delay_replica_writes_secs, replicate_primitive_horizon_callback,
          g, "delay replica writes");
      cl_log(g->g_cl, CL_LEVEL_INFO,
             "graphd_replicate_primitives: "
             "Delaying replica write. Starting timer. start=%llx"
             " end=%llx",
             (unsigned long long)g->g_rep_write_delay_horizon_start,
             (unsigned long long)g->g_rep_write_delay_horizon_end);
    }

  } else {
    // Just write it now.
    g->g_rep_write_delay_horizon_start = start;
    g->g_rep_write_delay_horizon_end = end;
    replicate_primitive_horizon_callback(g, 0);
  }
}

static void format_replica_restore(void* data, srv_handle* srv,
                                   void* session_data, void* request_data,
                                   char** s, char* e) {
  static char const restore[] = "restore";
  graphd_request* const greq = request_data;

  if (e - *s < sizeof(restore) - 1) return;

  memcpy(*s, restore, sizeof(restore) - 1);
  *s += sizeof(restore) - 1;

  greq->greq_format = graphd_format_result;
}

static int push_replica_restore(graphd_handle* g, graphd_session* gses,
                                graphd_request* rep_req, pdb_id start,
                                pdb_id end) {
  static char const version[] = "6";
  graphd_value* val = &rep_req->greq_reply;
  int err = 0;

  err = graphd_value_list_alloc(g, rep_req->greq_req.req_cm, g->g_cl,
                                &rep_req->greq_reply, 4);
  if (err) return err;
  val = rep_req->greq_reply.val_list_contents;

  graphd_value_text_set(val++, GRAPHD_VALUE_STRING, version,
                        version + sizeof version - 1, NULL);
  graphd_value_number_set(val++, start);
  graphd_value_number_set(val++, end);
  graphd_value_records_set(val, g->g_pdb, start, end - start);

  err = graphd_format_stack_push(gses, rep_req, &rep_req->greq_reply);
  if (err) return err;

  rep_req->greq_format = format_replica_restore;

  return 0;
}

/*  If we have data to restore, send a "restore" command to the
 *  replica graphd at the other end of the passed session.
 *  Unlike the regular restore, this will not receive a reply.
 */
static int send_replica_restore(graphd_handle* g, graphd_session* gses,
                                pdb_id start, pdb_id end) {
  cl_handle* const cl = gses->gses_cl;
  graphd_request* rep_req;
  int err = 0;

  if (start == end) {
    cl_log(cl, CL_LEVEL_ERROR,
           "Replication session %s "
           " restore start == end (%llx).  That's odd.",
           gses->gses_ses.ses_displayname, (unsigned long long)start);
    return 0;
  }

  rep_req = graphd_request_create_asynchronous(
      gses, GRAPHD_REQUEST_ASYNC_REPLICA_RESTORE, format_replica_restore);
  if (!rep_req) return ENOMEM;
  rep_req->greq_request_start_hint = "restore [...]";

  graphd_request_start(rep_req);
  graphd_runtime_statistics_start_request(rep_req);

  err = push_replica_restore(g, gses, rep_req, start, end);
  if (err)
    cl_log_errno(gses->gses_cl, CL_LEVEL_ERROR, "push_replica_restore", err,
                 "%s Unable to push a replica restore "
                 "from %llx to %llx",
                 gses->gses_ses.ses_displayname, (unsigned long long)start,
                 (unsigned long long)end);

  graphd_request_served(rep_req);

  return err;
}

/* The master graph has just executed a restore command.  Forward the
 * restore command to all replicas.
 */
int graphd_replicate_restore(graphd_handle* g, pdb_id start, pdb_id end) {
  graphd_session* gses = g->g_rep_sessions;
  int err = 0;
  int e;

  if (!gses || start == end) return 0; /* no replicas or nothing to replicate */

  while (gses) {
    graphd_session* const next_ses = gses->gses_data.gd_rep_client.gdrc_next;

    /*  If the restore goes beyond what the client already knows,
     *  send it an update.
     *
     *  (This omits restores that result from this connection's server
     *  reconnecting to *its* server, receiving a small set of redundant
     *  updates from the server to ensure that they're talking about the
     *  same database.)
     */
    if (gses->gses_data.gd_rep_client.gdrc_next_id < end) {
      e = send_replica_restore(g, gses,
                               gses->gses_data.gd_rep_client.gdrc_next_id, end);
      if (e) {
        if (!err) err = e;

        cl_log_errno(
            g->g_cl, CL_LEVEL_ERROR, "send_replica_write", e,
            "Unable to write primitives [%llx, %llx) to %s",
            (unsigned long long)gses->gses_data.gd_rep_client.gdrc_next_id,
            (unsigned long long)end, gses->gses_ses.ses_displayname);

        srv_session_abort(&gses->gses_ses);
        goto next_session;
      }
      gses->gses_data.gd_rep_client.gdrc_next_id = end;
    }

  next_session:
    gses = next_ses;
  }

  return err;
}

/* "Format" a catch-up request
 */
static void format_replica_catch_up(void* data, srv_handle* srv,
                                    void* session_data, void* request_data,
                                    char** s, char* e) {
  graphd_handle* const g = data;
  graphd_session* const gses = session_data;
  graphd_request* const greq = request_data;
  pdb_id const start = gses->gses_data.gd_rep_client.gdrc_next_id;
  pdb_id end = pdb_primitive_n(g->g_pdb);
  unsigned long long const delta = end - start;
  int err;

  cl_assert(srv_log(srv), start <= end);
  if (delta) {
    /* Still more catching up to do.  Make this request
     * into a restore.
     */

    if (delta > GRAPHD_RESTORE_LAG_MAX) end = start + GRAPHD_RESTORE_LAG_MAX;
    gses->gses_data.gd_rep_client.gdrc_next_id += end - start;

    cl_log(gses->gses_cl, CL_LEVEL_INFO,
           "Sending replication restore: %llx %llx %llu",
           (unsigned long long)start, (unsigned long long)end, delta);
    err = push_replica_restore(g, gses, greq, start, end);
    if (err) {
      cl_log_errno(gses->gses_cl, CL_LEVEL_ERROR, "push_replica_restore", err,
                   "Unable to push a replica restore "
                   "from %llx to %llx",
                   (unsigned long long)start, (unsigned long long)end);
      srv_session_abort(&gses->gses_ses);
      return;
    }
  } else {
    srv_request_output_done(&greq->greq_req);
  }

  if (delta <= GRAPHD_RESTORE_LAG_MAX) {
    /* We're (almost) caught up.  Add the session to the list
     * of replication sessions so that upcoming writes
     * will be sent to the replica.
     */
    graphd_session* test_ses = g->g_rep_sessions;
    while (test_ses != NULL) {
      cl_assert(gses->gses_cl, gses != test_ses);
      test_ses = test_ses->gses_data.gd_rep_client.gdrc_next;
    }

    gses->gses_type = GRAPHD_SESSION_REPLICA_CLIENT;
    cm_list_push(graphd_session, graphd_ses_offsets, &g->g_rep_sessions, 0,
                 gses);
  } else {
    /* The replica is at least one page behind the master.
     * Enqueue another catch-up request to continue the process
     * of catching up.
     */
    err = enqueue_catch_up(gses);
    if (err) {
      cl_log_errno(gses->gses_cl, CL_LEVEL_ERROR, "enqueue_catch_up", err,
                   "Unable to enqueue catch-up request");
      srv_session_abort(&gses->gses_ses);
      return;
    }
  }
}

static int enqueue_catch_up(graphd_session* gses) {
  graphd_request* greq;

  greq = graphd_request_create_asynchronous(
      gses, GRAPHD_REQUEST_ASYNC_REPLICA_CATCH_UP, format_replica_catch_up);
  if (greq == NULL) return ENOMEM;

  greq->greq_request_start_hint = "[asynchronous catch-up]";

  graphd_request_start(greq);
  graphd_runtime_statistics_start_request(greq);
  graphd_request_served(greq);

  return 0;
}

/* A replica request has arrived in the master.
 *
 * The "REPLICA" request is sent by the replicant to its master.
 * It means "please send me data!".
 *
 * After preparing the response to the replica request itself ("rok"),
 * we enqueue a catch-up request whose job it is to ensure that
 * the replica is caught up with the master.
 */
int graphd_replica(graphd_request* greq) {
  static char const version[] = "1";

  graphd_session* const gses = graphd_request_session(greq);
  graphd_handle* const g = gses->gses_graphd;
  cl_handle* const cl = gses->gses_cl;
  graphd_value* val = &greq->greq_reply;
  unsigned long long const prim_n = pdb_primitive_n(g->g_pdb);
  char const* const write_master_url =
      GRAPHD_ACCESS_ARCHIVE == g->g_access
          ? "archive"
          : (g->g_rep_write_address ? g->g_rep_write_address->addr_url : "");
  int err = 0;

  cl_assert(cl, greq->greq_request == GRAPHD_REQUEST_REPLICA);

  if (greq->greq_data.gd_replica.gdrep_start_id == PDB_ID_NONE) {
    graphd_request_errprintf(greq, 0, "SEMANTIC missing 'start-id' parameter");
    goto err;
  }
  if (greq->greq_data.gd_replica.gdrep_start_id > prim_n) {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "Replication session, %s, wants to start at: "
           "%llx > %llx",
           greq->greq_req.req_session->ses_displayname,
           greq->greq_data.gd_replica.gdrep_start_id, prim_n);
    graphd_request_errprintf(greq, 0,
                             "SEMANTIC cannot start replication at %llx; "
                             "this server only has primitives up to %llx",
                             greq->greq_data.gd_replica.gdrep_start_id, prim_n);
    goto err;
  }

  cl_log(
      cl, CL_LEVEL_INFO,
      "New replication session, %s (id=%llx), starting at: %llx, %s",
      greq->greq_req.req_session->ses_displayname, greq->greq_req.req_id,
      greq->greq_data.gd_replica.gdrep_start_id,
      greq->greq_data.gd_replica.gdrep_master ? "check master" : "same master");

  /*  We start replication 256 primitives behind the requested
   *  start.  If the replica is different from the master (ie
   *  not a replica), the restore will fail.  We never restore
   *  from zero unless the replica explicitly requests it, as
   *  restore-from-zero overwrites primitives instead of verifying
   *  that they are identical.
   */
  gses->gses_data.gd_rep_client.gdrc_next_id =
      greq->greq_data.gd_replica.gdrep_start_id > 256
          ? greq->greq_data.gd_replica.gdrep_start_id - 256
          : (greq->greq_data.gd_replica.gdrep_start_id > 0 ? 1 : 0);

  err = graphd_value_list_alloc(g, greq->greq_req.req_cm, cl, val, 2);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_value_list_alloc", err,
                 "failed to allocate 2 list elements");
    graphd_request_errprintf(greq, 0,
                             "SYSTEM out of memory while allocating "
                             "replication command result!");
    goto err;
  }

  graphd_value_text_set(val->val_list_contents, GRAPHD_VALUE_STRING, version,
                        version + sizeof(version) - 1, NULL);

  err = graphd_value_text_strdup(
      greq->greq_req.req_cm, val->val_list_contents + 1,
      GRAPHD_ACCESS_ARCHIVE == g->g_access ? GRAPHD_VALUE_ATOM
                                           : GRAPHD_VALUE_STRING,
      write_master_url, write_master_url + strlen(write_master_url));
  if (err) {
    graphd_value_finish(cl, val);
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_value_text_strdup", err,
                 "can't duplicate \"%s\"", write_master_url);
    graphd_request_errprintf(greq, 0,
                             "SYSTEM out of memory while allocating "
                             "replication command result!");
    goto err;
  }

  if ((err = enqueue_catch_up(graphd_request_session(greq))) != 0) {
    graphd_value_finish(cl, val);

    cl_log_errno(cl, CL_LEVEL_FAIL, "enqueue_catch_up", err,
                 "Unable to enqueue catch-up request");
    graphd_request_errprintf(greq, 0,
                             "SYSTEM out of memory while allocating "
                             "replication catch-up request");
  }
err:
  return err;
}

/*  I am a replica or an importer.  Here is an incoming replica-write.
 *  Run it, and any that may be queued behind it.
 */
int graphd_replica_write(graphd_request* greq) {
  graphd_session* const gses = graphd_request_session(greq);
  graphd_handle* const g = gses->gses_graphd;
  cl_handle* const cl = gses->gses_cl;
  unsigned long long next_id = pdb_primitive_n(g->g_pdb);
  unsigned long long const start = next_id;
  unsigned long long end = start + (greq->greq_end - greq->greq_start);
  int err, rollback_err;

  if ((err = graphd_smp_pause_for_write(greq)) != 0) return err;

  if ((err = graphd_defer_write(greq)) != 0) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_defer_write", err,
                 "refusing to write while no disk is available");

    goto disconnect;
  }

  /*  a crash here is fatal for non-transactional databases
   */
  if (!pdb_transactional(g->g_pdb)) srv_shared_set_safe(g->g_srv, false);

  err = graphd_restore_create_primitives(greq);
  if (err) goto rollback_and_disconnect;

  /* Now, check the incoming request queue for additional writes...
   */
  srv_request* sr = gses->gses_ses.ses_request_head->req_next;
  int count = 0;
  while (sr) {
    srv_request* const next = sr->req_next;
    graphd_request* gr = (graphd_request*)sr;

    /* The generic request that the system creates to have
     * something to read input into.
     */
    if (gr->greq_request == GRAPHD_REQUEST_UNSPECIFIED) break;

    cl_assert(cl, GRAPHD_REQUEST_REPLICA_WRITE == gr->greq_request);

    err = graphd_restore_create_primitives(gr);
    if (err) goto rollback_and_disconnect;

    end = end + (gr->greq_end - gr->greq_start);
    srv_request_complete(&gr->greq_req);

    sr = next;
    count++;
  }
  if (count)
    cl_log(cl, CL_LEVEL_INFO, "Coalesced %d replica-writes: %llx - %llx",
           count + 1, start, end);

  err = graphd_restore_checkpoint(cl, g, gses);
  if (err) goto disconnect;

  if (!pdb_transactional(g->g_pdb)) srv_shared_set_safe(g->g_srv, true);

  graphd_replicate_primitives(g, start, end);

  if (err == 0) return err;

rollback_and_disconnect:
  rollback_err = graphd_checkpoint_rollback(g, start);
  if (rollback_err) {
    char bigbuf[1024 * 8];
    char const* req_s;
    int req_n;
    bool incomplete;

    graphd_request_as_string(greq, bigbuf, sizeof bigbuf, &req_s, &req_n,
                             &incomplete);

    cl_log_errno(cl, CL_LEVEL_FATAL, "graphd_checkpoint_rollback", rollback_err,
                 "failed to roll back to horizon=%llx", start);

    srv_epitaph_print(gses->gses_ses.ses_srv, EX_GRAPHD_DATABASE,
                      "graphd: failed to roll back changes after "
                      "a restore error: "
                      "session=%s (SID=%lu, RID=%lu), "
                      "error=\"%s\" (%d), "
                      "rollback error=\"%s\" (%d), "
                      "request: %.*s%s",
                      gses->gses_ses.ses_displayname, gses->gses_ses.ses_id,
                      greq->greq_req.req_id, graphd_strerror(err), err,
                      graphd_strerror(rollback_err), rollback_err, (int)req_n,
                      req_s, incomplete ? "..." : "");

    exit(EX_GRAPHD_DATABASE);
  }

disconnect:

  /*  There was a problem, but we're still in a well-defined state.
   */
  if (!pdb_transactional(g->g_pdb)) srv_shared_set_safe(g->g_srv, true);

  cl_assert(cl, err);

  /* In case of any error which caused us not to commit primitives
   * we disconnect from the master graph.  Upon (automatic) reconnection
   * we'll get another try.
   */

  int e = graphd_replica_disconnect(g);
  if (e)
    cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_replica_disconnect", e,
                 "Unable to disconnect from master");
  return err;
}

/* Try to establish a connection to the replica server
 */
int graphd_replica_connect(graphd_handle* g) {
  int err;

  if (GRAPHD_ACCESS_REPLICA != g->g_access &&
      GRAPHD_ACCESS_ARCHIVE != g->g_access)
    return 0;

  if (!g->g_rep_master) {
    /*  Make sure we're getting a reasonable
     *  start time for this session - it often
     *  is created before even the first session.
     */
    graphd_set_time(g);

    cl_assert(g->g_cl, g->g_rep_master_address != NULL);
    cl_log(g->g_cl, CL_LEVEL_INFO, "Initiating replica connection to: %s",
           g->g_rep_master_address->addr_url);

    g->g_rep_replica_sent = 0;

    cl_assert(g->g_cl, g->g_srv != NULL);
    err = srv_interface_connect(g->g_srv, g->g_rep_master_address->addr_url,
                                (void*)&g->g_rep_master);
    if (err) return err;

    g->g_rep_master->gses_type = GRAPHD_SESSION_REPLICA_MASTER;

    /* Create the timeout handle if one doesn't yet exist */
    if (!g->g_rep_master_timeout) {
      g->g_rep_master_timeout =
          srv_timeout_create(g->g_srv, GRAPHD_REPLICA_TIMEOUT_SECONDS);
    }

    err = graphd_client_replica_send(g, g->g_rep_master);
    if (err) {
      srv_session_abort(&g->g_rep_master->gses_ses);
      g->g_rep_master = NULL;

      return err;
    }

    /* Associate the timeout */
    srv_session_set_timeout(&g->g_rep_master->gses_ses,
                            g->g_rep_master_timeout);
  }

  return 0;
}

int graphd_replica_disconnect(graphd_handle* g) {
  (void)graphd_replica_disconnect_oneway(g);

  cl_log(g->g_cl, CL_LEVEL_VERBOSE, "grapdh_replica_disconnect");

  if (g->g_rep_write) {
    cl_log(g->g_cl, CL_LEVEL_INFO,
           "Terminating write-through connection to: %s",
           g->g_rep_write_address->addr_url);

    srv_session_abort(&g->g_rep_write->gses_ses);

    if (g->g_smp_leader_passthrough == g->g_rep_write) {
      g->g_smp_leader_passthrough = 0;
    }

    g->g_rep_write = 0;

    if (g->g_rep_write_address) {
      srv_address_destroy(g->g_rep_write_address);
      g->g_rep_write_address = 0;
    }
  }

  return 0;
}

/* Drop replica connection, keep write-through link (if any)
 */
int graphd_replica_disconnect_oneway(graphd_handle* g) {
  cl_log(g->g_cl, CL_LEVEL_VERBOSE, "grapdh_replica_disconnect_oneway");

  if (g->g_rep_master) {
    cl_assert(g->g_cl, g->g_rep_master_address != NULL);
    cl_log(g->g_cl, CL_LEVEL_INFO, "Terminating replica connection to: %s",
           g->g_rep_master_address->addr_url);

    srv_session_abort(&g->g_rep_master->gses_ses);
    g->g_rep_master = 0;
  }

  return 0;
}

static int graphd_replica_run(graphd_request* greq,
                              unsigned long long deadline) {
  int err;

  err = graphd_replica(greq);
  if (err != GRAPHD_ERR_MORE) graphd_request_served(greq);
  return err;
}

static graphd_request_type graphd_replica_type = {
    "replica",
    /* graphd_replica_input_arrived */ NULL,
    /* graphd_replica_output_sent */ NULL, graphd_replica_run,
    /* graphd_replica_free */ NULL};

/*  The replica request inside the receiving master.
 */
void graphd_replica_initialize(graphd_request* greq) {
  greq->greq_request = GRAPHD_REQUEST_REPLICA;
  greq->greq_type = &graphd_replica_type;

  greq->greq_data.gd_replica.gdrep_start_id = PDB_ID_NONE;
  greq->greq_data.gd_replica.gdrep_start = NULL;
  greq->greq_data.gd_replica.gdrep_version = 0;
  greq->greq_data.gd_replica.gdrep_master = false;
}
