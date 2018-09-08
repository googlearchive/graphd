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
#include <stdio.h>
#include <string.h>

static char const* const graphd_access_names[] =
    {[GRAPHD_ACCESS_READ_WRITE] = "read-write",
     [GRAPHD_ACCESS_READ_ONLY] = "read-only",
     [GRAPHD_ACCESS_REPLICA] = "replica",
     [GRAPHD_ACCESS_REPLICA_SYNC] = "replica-sync",
     [GRAPHD_ACCESS_ARCHIVE] = "archive",
     [GRAPHD_ACCESS_RESTORE] = "restore",
     [GRAPHD_ACCESS_SHUTDOWN] = "shutdown",
     [GRAPHD_ACCESS_LIMBO] = "limbo"};

size_t graphd_access_n =
    sizeof(graphd_access_names) / sizeof(*graphd_access_names);

char const* graphd_access_global_to_string(graphd_access_global acc) {
  static char const* const names[] =
      {[GRAPHD_ACCESS_READ_WRITE] = "read-write",
       [GRAPHD_ACCESS_READ_ONLY] = "read-only",
       [GRAPHD_ACCESS_REPLICA] = "replica",
       [GRAPHD_ACCESS_REPLICA_SYNC] = "replica-sync",
       [GRAPHD_ACCESS_ARCHIVE] = "archive",
       [GRAPHD_ACCESS_RESTORE] = "restore",
       [GRAPHD_ACCESS_SHUTDOWN] = "shutdown",
       [GRAPHD_ACCESS_LIMBO] = "limbo"};

  if (acc >= graphd_access_n || graphd_access_names[acc] == NULL) return "???";

  return names[acc];
}

graphd_access_global graphd_access_global_from_string(const char* s,
                                                      const char* e) {
  int i;
  for (i = 0; i < graphd_access_n; i++) {
    if (((e - s) == strlen(graphd_access_names[i])) &&
        !memcmp(s, graphd_access_names[i], e - s)) {
      return i;
    }
  }
  return -1;
}

int graphd_access_set_global(graphd_handle* g, graphd_access_global acc,
                             bool* error_is_retriable, char* error_buf,
                             size_t error_buf_size) {
  int const old_acc = g->g_access;
  int err;

  cl_log(g->g_cl, CL_LEVEL_VERBOSE, "graphd_access_set_global old: %s, new: %s",
         graphd_access_global_to_string(old_acc),
         graphd_access_global_to_string(acc));

  *error_is_retriable = false;
  *error_buf = '\0';

  /* If we haven't started, graphd_startup will call us again
   */
  if (g->g_srv == NULL) {
    /*  Set the access mode, but don't check.
     *  Startup will call us again.
     */
    g->g_access = acc;
    return 0;
  }

  /* If we're an SMP follower, this call is the result of the leader forwarding
   * The current access mode to us. To that end, we need only set our
   * access mode; all replica connections are managed by the leader,
   * obviating the rest of the function.
   */

  if (g->g_smp_proc_type == GRAPHD_SMP_PROCESS_FOLLOWER) {
    g->g_access = acc;
    return 0;
  }

  /*  If this is a replica-ish mode, we need to actually be configured
   *  (at startup) as a replica server.
   */
  if (GRAPHD_ACCESS_REPLICA == acc || GRAPHD_ACCESS_ARCHIVE == acc) {
    if (!g->g_rep_master_address) {
      snprintf(error_buf, error_buf_size,
               "this server is not configured as a replica!");
      return GRAPHD_ERR_NOT_A_REPLICA;
    }
  } else if (GRAPHD_ACCESS_REPLICA_SYNC == acc) {
    if (GRAPHD_ACCESS_REPLICA != g->g_access &&
        GRAPHD_ACCESS_ARCHIVE != g->g_access) {
      snprintf(error_buf, error_buf_size,
               "cannot move "
               "a server directly from %s into "
               "replica-sync mode - server must be in "
               "archive or replica mode first.",
               graphd_access_global_to_string(g->g_access));
      return GRAPHD_ERR_NOT_A_REPLICA;
    }
  }

  g->g_access = acc;

  /* If we're already in replica mode, the intention
   * is that resetting it will drop and re-establish
   * connections.
   */
  if (GRAPHD_ACCESS_REPLICA == old_acc || GRAPHD_ACCESS_ARCHIVE == old_acc) {
    if (GRAPHD_ACCESS_REPLICA_SYNC == acc) {
      /* disconnect replica link, keep write-though
       * sync database to disk
       */
      cl_log(g->g_cl, CL_LEVEL_VERBOSE, "%s:%d", __FILE__, __LINE__);
      err = graphd_replica_disconnect_oneway(g);
      if (err) {
        cl_log_errno(g->g_cl, CL_LEVEL_ERROR,
                     "graphd_replica_disconnect_oneway", err,
                     "Unable to disconnect replica for replica-sync mode");
        snprintf(error_buf, error_buf_size,
                 "error while "
                 "disconnecting from replica master: %s",
                 graphd_strerror(err));
        return err;
      }
    } else {
      /* disconnect replica and write-through links */
      cl_log(g->g_cl, CL_LEVEL_VERBOSE, "%s:%d", __FILE__, __LINE__);
      err = graphd_replica_disconnect(g);
      if (err) {
        snprintf(error_buf, error_buf_size,
                 "error while "
                 "disconnecting from replica/write servers: %s",
                 graphd_strerror(err));
        return err;
      }
    }
  }

  if (GRAPHD_ACCESS_REPLICA == acc || GRAPHD_ACCESS_ARCHIVE == acc) {
    cl_assert(g->g_cl, g->g_rep_master_address != NULL);

    /*  As a replica, we allow ourselves to restart as
     *  often as we please
     */
    srv_set_max_restart_count(g->g_srv, -1);

    /*  If we're still starting up, wait until we've got
     *  the OK to confirm startup.
     */
    srv_settle_delay(g->g_srv);

    if (g->g_started) {
      int err = graphd_replica_connect(g);
      if (err) return err;
    } else if (g->g_startup_want_replica_connection) {
      /* We have to do this before we finish starting up.
       */
      int err = graphd_replica_connect(g);
      if (err) {
        /*  If this is the first connect,
         *  terminate, rather than limping along in
         *  read-only mode.
         */
        if (err == SRV_ERR_ADDRESS) {
          snprintf(error_buf, error_buf_size,
                   "cannot resolve replication master "
                   "address \"%s\". Did you get the "
                   "name right?",
                   g->g_rep_master_address->addr_url);
          return err;
        } else if (err == SRV_ERR_NOT_SUPPORTED) {
          snprintf(error_buf, error_buf_size,
                   "the interface protocol for \"%s\" "
                   "does not support outgoing "
                   "connections.",
                   g->g_rep_master_address->addr_url);
          return err;
        } else if (!g->g_rep_reconnect_delay) {
          snprintf(error_buf, error_buf_size,
                   "Unable to connect to replication "
                   "server: %s. (Error: %s)",
                   g->g_rep_master_address->addr_url, graphd_strerror(err));
          *error_is_retriable = true;
          graphd_replica_schedule_reconnect(g);

          return err;
        }
      }
      if (err) return err;
    }
  }
  return 0;
}

/**
 * @brief Can this request start executing?
 *
 *  The request's verb must have been parsed by the calling code.
 *  If this call returns false, an error message is scheduled,
 *  and the request parser is moved to "skip".
 *
 * @param g 	graphd module handle
 * @param greq	request
 *
 * @return true if the request can start executing.
 * @return false if there's something about the global access
 *	policy that interferes with this request.
 */
bool graphd_access_allow_global(graphd_handle* g, graphd_request* greq) {
  cl_handle* cl = graphd_request_cl(greq);
  graphd_session* gses = graphd_request_session(greq);

  if (greq->greq_request == GRAPHD_REQUEST_ERROR) return true;

  if (g->g_access == GRAPHD_ACCESS_SHUTDOWN) {
    graphd_request_error(greq,
                         "SHUTDOWN shutdown in "
                         "progress - call back later");
    return false;
  }

  if (g->g_access == GRAPHD_ACCESS_LIMBO) {
    graphd_request_error(greq,
                         "SYSTEM hard system "
                         "error - contact tech support");
    return false;
  }

  switch (greq->greq_request) {
    case GRAPHD_REQUEST_UNSPECIFIED:
    case GRAPHD_REQUEST_ERROR:
    case GRAPHD_REQUEST_SKIP:
    case GRAPHD_REQUEST_SET:
    case GRAPHD_REQUEST_ISLINK:
    case GRAPHD_REQUEST_STATUS:
    case GRAPHD_REQUEST_SMP_FORWARD:
    case GRAPHD_REQUEST_SYNC:
    case GRAPHD_REQUEST_REPLICA:
    case GRAPHD_REQUEST_REPLICA_WRITE:
      break;

    case GRAPHD_REQUEST_RESTORE:
      if ((GRAPHD_ACCESS_REPLICA == g->g_access ||
           GRAPHD_ACCESS_REPLICA_SYNC == g->g_access ||
           GRAPHD_ACCESS_ARCHIVE == g->g_access) &&
          gses != g->g_rep_master) {
        graphd_request_error(greq,
                             "REPLICA A replica this server is. "
                             "Restore on the master, you must.");
        return false;
      }
      break;

    case GRAPHD_REQUEST_WRITE:
      switch (g->g_access) {
        default:
        case GRAPHD_ACCESS_READ_WRITE:
        case GRAPHD_ACCESS_REPLICA:
        case GRAPHD_ACCESS_REPLICA_SYNC:
          break;

        case GRAPHD_ACCESS_READ_ONLY:
          graphd_request_error(greq,
                               "READONLY this server is "
                               "read-only (use \"set (access=read-write)\""
                               " to unlock)");
          return false;

        case GRAPHD_ACCESS_ARCHIVE:
          graphd_request_error(greq,
                               "ARCHIVE this server is "
                               "a read-only replica");
          return false;

        case GRAPHD_ACCESS_RESTORE:
          graphd_request_error(greq,
                               "RESTORE cannot accept writes while restoring; "
                               "try again later");
          return false;
      }
      break;

    case GRAPHD_REQUEST_VERIFY:
    case GRAPHD_REQUEST_DUMP:
    case GRAPHD_REQUEST_READ:
    case GRAPHD_REQUEST_ITERATE:
      if (g->g_access == GRAPHD_ACCESS_READ_WRITE ||
          g->g_access == GRAPHD_ACCESS_READ_ONLY ||
          g->g_access == GRAPHD_ACCESS_REPLICA ||
          g->g_access == GRAPHD_ACCESS_REPLICA_SYNC ||
          g->g_access == GRAPHD_ACCESS_ARCHIVE)
        return true;

      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_access_allow: rejecting read or dump access "
             "from %s",
             gses->gses_ses.ses_displayname);
      graphd_request_error(greq,
                           "RESTORE restore in progress; "
                           "(use \"set (access=read-write)\" or \"set ("
                           "access=read-only)\" to unlock)");
      return false;

    case GRAPHD_REQUEST_SMP:
    case GRAPHD_REQUEST_SMP_OUT:
      // Here we limit it to run only on replicas, but for testing
      // let's let it go --BCM
      break;

    default:
      cl_notreached(cl, "unexpected request type %d", greq->greq_request);
  }

  return true;
}
