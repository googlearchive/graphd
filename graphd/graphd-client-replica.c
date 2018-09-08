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

static void graphd_client_replica_input_arrived(graphd_request* greq) {
  graphd_handle* const g = graphd_request_graphd(greq);
  graphd_session* const gses = graphd_request_session(greq);
  cl_handle* const cl = graphd_request_cl(greq);
  cm_handle* const cm = srv_mem(g->g_srv);
  srv_address* sa = NULL;
  int err;

  char const* url_s = greq->greq_data.gd_client_replica.gdcrep_write_url_s;
  char const* url_e = greq->greq_data.gd_client_replica.gdcrep_write_url_e;

  if (g->g_rep_write_arg) {
    url_s = g->g_rep_write_arg;
    url_e = url_s + strlen(g->g_rep_write_arg);
  }

  cl_assert(cl, !url_s == !url_e);

  if (!greq->greq_data.gd_client_replica.gdcrep_ok) {
    char buf[1024 * 16];
    char const* reply_s;
    int reply_n;
    bool reply_incomplete = false;

    graphd_request_reply_as_string(greq, buf, sizeof buf, &reply_s, &reply_n,
                                   &reply_incomplete);

    /*  Error.  This server doesn't like us.
     *
     *  If we've not started up yet, print an
     *  error message an die; that'll come out
     *  on stderr.
     *
     *  Otherwise, shut down this connection and
     *  try to reconnect a bit later.
     */
    if (g->g_startup_want_replica_connection) {
      srv_epitaph_print(
          g->g_srv, EX_GRAPHD_REPLICA_MASTER,
          "graphd: error connecting to replica master \"%s\": %.*s%s",
          g->g_rep_master_address && g->g_rep_master_address->addr_url
              ? g->g_rep_master_address->addr_url
              : "(null)",
          reply_n, reply_s, reply_incomplete ? "..." : "");

      /* NOTREACHED */
      exit(EX_GRAPHD_REPLICA_MASTER);
    }

    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "graphd: error connecting to replica master \"%s\": %.*s%s",
           g->g_rep_master_address && g->g_rep_master_address->addr_url
               ? g->g_rep_master_address->addr_url
               : "(null)",
           reply_n, reply_s, reply_incomplete ? "..." : "");

    goto retry_replication;
  }

  if (url_s != NULL) {
    int len = url_e - url_s;
    if (len) {
      err = srv_address_create_url(cm, g->g_cl, url_s, url_e, &sa);
      if (err) {
        cl_log_errno(cl, CL_LEVEL_ERROR, "srv_address_create_url", err,
                     "Unable to create address for %.*s", len, url_s);
        goto retry_replication;
      }
    } else if (GRAPHD_ACCESS_ARCHIVE != g->g_access) {
      cl_assert(g->g_cl, g->g_rep_master_address != NULL);
      err = srv_address_copy(cm, g->g_cl, g->g_rep_master_address, &sa);
      if (err) {
        cl_log_errno(cl, CL_LEVEL_ERROR, "srv_address_create_url", err,
                     "Unable to copy master address: %.*s", len,
                     g->g_rep_master_address->addr_url);

        goto retry_replication;
      }
    }
  } else {
    /* We connected to an archive.  If we are not in archive
     * mode ourselves, write an error to the log and enter
     * archive mode.
     */
    if (GRAPHD_ACCESS_ARCHIVE != g->g_access) {
      /* Tell people what to do.
       */
      cl_log(g->g_cl, CL_LEVEL_OPERATOR_ERROR,
             "WARNING: Replica connected to archive "
             "(read-only) server, switching to archive "
             "mode. To avoid this warning message, use "
             "\"archive\", rather than \"replica\", in "
             "graphd's configuration file.");
      g->g_access = GRAPHD_ACCESS_ARCHIVE;
    }

    cl_assert(g->g_cl, g->g_rep_master_address != NULL);
    cl_log(g->g_cl, CL_LEVEL_INFO, "Connected to archive server: %s",
           g->g_rep_master_address->addr_url);
  }
  if (g->g_rep_write_address) srv_address_destroy(g->g_rep_write_address);

  g->g_rep_write_address = sa;
  g->g_rep_ever_connected = true;

  /*  We've settled in.
   */
  if (g->g_startup_want_replica_connection) {
    g->g_startup_want_replica_connection = false;
    graphd_startup_todo_complete(g, &g->g_startup_todo_replica_connection);
  }

  srv_request_reply_received(&greq->greq_req);
  srv_request_complete(&greq->greq_req);
  graphd_request_served(greq);

  /*  Switch the session into "server" mode.  From now
   *  on, the replica will send us commands, and we'll
   *  react to them - rather than us sending commands and
   *  the server sending replies.
   */
  srv_session_set_server(&gses->gses_ses, true);

  cl_assert(g->g_cl, g->g_rep_master_address != NULL);
  cl_log(g->g_cl, CL_LEVEL_INFO,
         "Connected to replication server: "
         "%s, write master: %s, access: %s",
         g->g_rep_master_address->addr_url,
         g->g_rep_write_address ? g->g_rep_write_address->addr_url : "none",
         graphd_access_global_to_string(g->g_access));

  return;

retry_replication : {
  cl_log(cl, CL_LEVEL_VERBOSE, "%s:%d", __FILE__, __LINE__);
  int e = graphd_replica_disconnect(g);
  if (e)
    cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_replica_disconnect", e,
                 "Unable to disconnect from master");
}

  /* Even though things didn't work out,
   * this request is now done.
   */
  srv_request_reply_received(&greq->greq_req);
  srv_request_complete(&greq->greq_req);

  /*  Set up a retry, REPLICA_RECONNECT_DELAY
   *  seconds from now.
   */
  graphd_replica_schedule_reconnect(g);
}

static int graphd_client_replica_output_sent(graphd_request* greq) {
  graphd_handle* g = graphd_request_graphd(greq);

  g->g_rep_replica_sent = true;
  srv_request_sent(&greq->greq_req);

  return 0;
}

static graphd_request_type graphd_client_replica_type = {
    "client_replica",
    graphd_client_replica_input_arrived,
    graphd_client_replica_output_sent,
    /* graphd_client_replica_run */ NULL,
    /* graphd_client_replica_cancel */ NULL,
    /* graphd_client_replica_free */ NULL};

/*  The replica request inside the client.
 */
static int graphd_client_replica_initialize(graphd_request* greq) {
  graphd_handle* g = graphd_request_graphd(greq);
  size_t const check_size = 0;

  /* TODO: figure out when check for new write master*/

  greq->greq_request = GRAPHD_REQUEST_CLIENT_REPLICA;
  greq->greq_type = &graphd_client_replica_type;

  greq->greq_data.gd_client_replica.gdcrep_start_id = PDB_ID_NONE;
  greq->greq_data.gd_client_replica.gdcrep_version = 0;
  greq->greq_data.gd_client_replica.gdcrep_master = false;
  greq->greq_data.gd_client_replica.gdcrep_ok = false;
  greq->greq_data.gd_client_replica.gdcrep_write_url_s = NULL;
  greq->greq_data.gd_client_replica.gdcrep_write_url_e = NULL;

  return graphd_request_output_text(
      greq, greq->greq_req.req_cm,
      greq->greq_request_start_hint = cm_sprintf(
          greq->greq_req.req_cm, "replica (version=\"1\" start-id=%llu%s)\n",
          pdb_primitive_n(g->g_pdb), check_size ? " check-master" : ""));
}

/* Send a "replica" command to the replica graphd at
 * the other end of the passed session
 */
int graphd_client_replica_send(graphd_handle* g, graphd_session* gses) {
  graphd_request* greq;
  int err;

  greq = graphd_request_create_outgoing(gses, GRAPHD_REQUEST_CLIENT_REPLICA);
  if (greq == NULL) return errno ? errno : ENOMEM;

  err = graphd_client_replica_initialize(greq);
  if (err != 0) {
    srv_request_unlink(&greq->greq_req);
    return err;
  }
  graphd_request_start(greq);

  return 0;
}
