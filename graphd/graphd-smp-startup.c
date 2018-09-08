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
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sysexits.h>
#include <syslog.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>

#include "libsrv/srv.h"
#include "libsrv/srvp.h"

/*
 *	An SMP process has died. This is the libsrv callback to determine
 *	what our next step is, based on process index (as per spawn) and
 *	status (returned from wait(2))
 *
 *	Return 0 to continue as normal (with the dead process still dead)
 *	Return SRV_ERR_MORE to respawn the process
 *	Return anything else (e.g. SRV_ERR_NOT_SUPPORTED)
 *		to kill everybody (something bad happened)
*/
int graphd_smp_finish(void *data, srv_handle *srv, size_t index, int status) {
  graphd_handle *g = (graphd_handle *)data;

  if (index == 0) {
    /* Leader died. Terminate everybody, ideally. */
    if (WIFEXITED(status)) {
      if (WEXITSTATUS(status) == 0) return 0;
    } else {
      /* Killall! */
      cl_log(g->g_cl, CL_LEVEL_ERROR,
             "SMP leader died. Killing all followers. Exit status: %d",
             WEXITSTATUS(status));
      return SRV_ERR_NOT_SUPPORTED;
    }

  } else if (index > 0) {
    /* Follower died */
    if (WIFEXITED(status)) {
      if (WEXITSTATUS(status) == 0) {
        /* If we exited normally, then just
           ignore it and don't respawn
           (we'll die soon enough) */
        return 0;
      }

      else if (WEXITSTATUS(status) == EX_SOFTWARE) {
        /* Something bad occured. A follower
           explicitly exited with an error */
        cl_log(g->g_cl, CL_LEVEL_ERROR,
               "SMP follower exited with EX_SOFTWARE. Killing all followers.");
        return SRV_ERR_NOT_SUPPORTED;
      }

      else if (WEXITSTATUS(status) == EX_TEMPFAIL) {
        /* A temporary fail condition (such as one follower dying,
         * of it's own will.) Explicitly respawn.
         */
        cl_log(g->g_cl, CL_LEVEL_FAIL,
               "SMP follower killed itself, respawning");
        return SRV_ERR_MORE;
      }
    }
    if (WIFSIGNALED(status)) {
      cl_log(g->g_cl, CL_LEVEL_ERROR, "SMP index %d died with signal %d",
             (int)index, WTERMSIG(status));
      if (WTERMSIG(status) == SIGKILL || WTERMSIG(status) == SIGTERM) {
        /* We were intentionally killed
         * from the outside. We yearn for death */
        return SRV_ERR_NOT_SUPPORTED;
      }
    }

    /* Otherwise, respawn */
    return SRV_ERR_MORE;
  }
  return 0;
}

int graphd_smp_startup(void *data, srv_handle *srv, size_t index) {
  graphd_handle *g = (graphd_handle *)data;
  int err;

  cl_log(g->g_cl, CL_LEVEL_INFO, "Starting SMP process with index %d",
         (int)index);

  /*  If this isn't the lead thread, close
   *  the settlement pipe.
   */
  if (index != 0) srv_settle_close(g->g_srv);

  if (index == 0) {
    /* We are a leader */
    g->g_smp_proc_type = GRAPHD_SMP_PROCESS_LEADER;

    g->g_smp_follower_timeout = srv_timeout_create(srv, 5);
    g->g_smp_request = NULL;

    /* Close the open interfaces -- leaders do not accept
       connections from the outside world.
       The new interface for followers we've created
       is about to be opened
     */
    srv_interface_shutdown(srv);

    err = srv_interface_add_and_listen(srv, g->g_smp_leader_address);
    if (err) return err;

    if (!g->g_require_replica_connection_for_startup) {
      err = graphd_replica_connect(g);

      if (err) return err;
    }

    srv_settle_close(g->g_srv);
  } else {
    /* We are a follower. Begin the connection */
    g->g_smp_proc_type = GRAPHD_SMP_PROCESS_FOLLOWER;

    srv_interface_balance(srv, true);

    if (g->g_pdb) {
      err = pdb_refresh(g->g_pdb);
      if (err) {
        cl_log_errno(g->g_cl, CL_LEVEL_FAIL, "pdb_refresh", err,
                     "Can't refresh database after restart");
        return err;
      }
    }

    err = graphd_smp_connect(g);
    if (err) return err;

    cl_log(g->g_cl, CL_LEVEL_VERBOSE, "%s:%d", __FILE__, __LINE__);
    err = graphd_replica_disconnect_oneway(g);
    if (err) return err;
  }
  return 0;
}
