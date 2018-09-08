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
#include "graphd/graphd-ast-debug.h"
#include "graphd/graphd-ast.h"
#include "graphd/graphd.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

static const cm_list_offsets graphd_session_wait_offsets = CM_LIST_OFFSET_INIT(
    graphd_request, greq_session_wait_next, greq_session_wait_prev);

/*  Is this the type of session that can receive
 *  replica-write commands?
 */
bool graphd_session_receives_replica_write(graphd_session const *gses) {
  return gses->gses_type == GRAPHD_SESSION_REPLICA_MASTER;
}

void graphd_session_shutdown(void *data, srv_handle *srv, void *session_data) {
  graphd_session *gses = session_data;

  /*  If there is a delay timer, destroy it.
   */
  if (gses->gses_delay != NULL) {
    srv_delay *del;

    del = gses->gses_delay;
    gses->gses_delay = NULL;

    srv_delay_destroy(del);
  }

  graphd_replica_session_shutdown(gses);

  if (!srv_is_shutting_down(srv)) graphd_smp_session_shutdown(gses);
}

int graphd_session_initialize(void *data, srv_handle *srv, void *session_data) {
  graphd_handle *g = data;
  graphd_session *gses = session_data;

  gses->gses_graphd = g;
  gses->gses_cl = srv_log(srv);
  gses->gses_time_active = gses->gses_time_created = g->g_now;
  gses->gses_last_action = "connect";

  cl_cover(gses->gses_cl);

  return 0;
}

/**
 * @brief If this session had a prompt, what would it be?
 *
 *  This callback is invoked by the server library every time
 *  an interactive prompt is printed.  It allows the prompt
 *  to change according to the server state.
 *
 * @param data	opaque application data pointer, i.e. the graphd module handle
 * @param srv	libsrv module handle
 * @param session_data opaque per-session application data pointer,
 *		i.e. the graphd session
 * @param buf	copy the prompt here, if you need a place to copy it.
 * @param size	use up to size bytes in the buffer.
 * @return a pointer to an interactive session prompt.
 */
char const *graphd_session_interactive_prompt(void *data, srv_handle *srv,
                                              void *session_data, char *buf,
                                              size_t size) {
  graphd_handle *g = data;
  graphd_session *gses = session_data;

  char const *state_prompt;
  cl_assert(gses->gses_cl, data != NULL);

  gses->gses_time_active = g->g_now;

  switch (gses->gses_tokenizer.ts_state) {
    case GRAPHD_TS_INITIAL:
      cl_cover(gses->gses_cl);
      state_prompt = ">";
      break;

    case GRAPHD_TS_STRING:
      cl_cover(gses->gses_cl);
      state_prompt = "> \"";
      break;

    case GRAPHD_TS_CR:
    case GRAPHD_TS_SKIP:
      cl_cover(gses->gses_cl);
      return "[return] ";

    default:
      cl_log(gses->gses_cl, CL_LEVEL_FAIL,
             "graphd_session_interactive_prompt: unexpected "
             "tokenizer state %d",
             gses->gses_tokenizer.ts_state);
      return NULL;
  }
  if (gses->gses_tokenizer.ts_nesting_depth > 0) {
    cl_cover(gses->gses_cl);
    snprintf(buf, size, "%s <%d%s ", srv_program_name(srv),
             gses->gses_tokenizer.ts_nesting_depth, state_prompt);
  } else {
    cl_cover(gses->gses_cl);
    snprintf(buf, size, "%s%s ", srv_program_name(srv), state_prompt);
  }
  return buf;
}

static bool displayname_match(char const *a_s, char const *a_e, char const *b) {
  /* Skip a "tcp" prefix. */
  if (a_e - a_s >= 3 && strncasecmp(a_s, "tcp", 3) == 0) a_s += 3;
  if (strncasecmp(b, "tcp", 3) == 0) b += 3;

  /* skip one or more ":" or "/" */
  while (a_s < a_e && (*a_s == ':' || *a_s == '/')) a_s++;
  while (*b == ':' || *b == '/') b++;

  /* case-insensitively compare the rest. */
  while (a_s < a_e && *b != '\0' &&
         (isascii(*a_s) && isascii(*b) ? tolower(*a_s) == tolower(*b)
                                       : *a_s == *b)) {
    a_s++;
    b++;
  }
  return *b == '\0' && a_s >= a_e;
}

typedef struct displayname_context {
  srv_session const *dpy_session;
  gdp_token const *dpy_token;
} displayname_context;

static int displayname_callback(void *data, srv_session *ses) {
  displayname_context *ctx = data;

  if (ses->ses_displayname == NULL) return 0;

  if (displayname_match(ctx->dpy_token->tkn_start, ctx->dpy_token->tkn_end,
                        ses->ses_displayname)) {
    ctx->dpy_session = ses;

    /*  Not an error - just cause the traversal function
     *  to return early.
     */
    return GRAPHD_ERR_ALREADY;
  }
  return 0;
}

graphd_session *graphd_session_by_displayname(srv_handle *srv,
                                              gdp_token const *tok) {
  displayname_context ctx;

  ctx.dpy_session = NULL;
  ctx.dpy_token = tok;

  srv_session_list(srv, displayname_callback, &ctx);
  return (graphd_session *)ctx.dpy_session;
}

static void graphd_session_delay_callback(void *data,
                                          es_idle_callback_timed_out mode) {
  graphd_session *gses = data;

  gses->gses_delay = NULL;
  cl_log(gses->gses_cl, CL_LEVEL_VERBOSE,
         "graphd_session_delay_callback for %s (mode: %d).",
         gses->gses_ses.ses_displayname, (int)mode);

  srv_session_resume(&gses->gses_ses);
}

/**
 * @brief Suspend a session for a number of seconds.
 *
 *  This is like a "sleep" that doesn't prevent the other
 *  sessions from running.  If there's nothing else going on,
 *  the system will end up sitting in the central poll
 *  loop for a while, then come out of it.
 *
 * @param gses		Session to suspend
 * @param seconds	How many seconds to sleep for.
 *
 * @return 0 on success, otherwise a nonzero errno.
 */
int graphd_session_delay(graphd_session *gses, unsigned long seconds) {
  srv_handle *srv = gses->gses_ses.ses_srv;
  int err;

  /*  Have this session be called back in <seconds>.
   */
  srv_session_suspend(&gses->gses_ses);
  gses->gses_delay =
      srv_delay_create(srv, seconds, seconds, graphd_session_delay_callback,
                       gses, "graphd session delay");
  if (gses->gses_delay == NULL) {
    err = errno ? errno : ENOMEM;
    cl_log_errno(gses->gses_cl, CL_LEVEL_FAIL, "srv_delay_create", err,
                 "session=%s, seconds=%lu", gses->gses_ses.ses_displayname,
                 seconds);
    return err;
  }

  cl_log(gses->gses_cl, CL_LEVEL_VERBOSE,
         "graphd_session_delay %s for %lu seconds.",
         gses->gses_ses.ses_displayname, seconds);
  return 0;
}

static int resume_if_dateline(void *data, srv_session *ses) {
  graphd_session *const gses = (graphd_session *)ses;

  if (GRAPHD_SUSPEND_DATELINE == gses->gses_suspend_reason &&
      gses->gses_dateline_id <= *(pdb_id *)data) {
    cl_log(gses->gses_cl, CL_LEVEL_DEBUG,
           "resume_if_dateline: resuming session %llu",
           (unsigned long long)gses->gses_ses.ses_id);
    graphd_session_resume(gses);
  }

  return 0;
}

/*  Invoked by the PDB whenever a primitive is written.
 */
static int graphd_session_dateline_primitive_write_callback(
    void *callback_data, pdb_handle *handle, pdb_id id,
    pdb_primitive const *primitive) {
  graphd_handle *g = callback_data;

  /*  If we're resetting to an empty database, there's no need
   *  to see if someone was waiting for that...
   */
  if (id == PDB_ID_NONE) return 0;

  cl_log(g->g_cl, CL_LEVEL_DEBUG,
         "graphd_session_dateline_primitive_write_callback "
         "id=%llx, max=%lld",
         (unsigned long long)id, (long long)g->g_dateline_suspended_max);

  /*  If we know that nobody's waiting for a dateline,
   *  we don't have to scan suspended sessions.
   */
  if (g->g_dateline_suspended_max == PDB_ID_NONE) return 0;

  /*  Walk the session list, looking for sessions that
   *  waited for this event.
   */

  /*  Where actually comparing against the number of primitives, not
   *  against the highest id.
   */
  id++;
  (void)srv_session_list(g->g_srv, resume_if_dateline, &id);

  /*  Was that ID the last one anyone was waiting for?  If yes,
   *  reset the marker.
   */
  if (g->g_dateline_suspended_max <= id)
    g->g_dateline_suspended_max = PDB_ID_NONE;

  return 0;
}

/*  Subscribe to pdb's "I just created a primitive" callbacks.
 */
int graphd_session_dateline_monitor(graphd_handle *g) {
  g->g_dateline_suspended_max = PDB_ID_NONE;

  return pdb_primitive_alloc_subscription_add(
      g->g_pdb, graphd_session_dateline_primitive_write_callback, g);
}

/*  Defer writing if we're in disk trouble.
 */
int graphd_defer_write(graphd_request *greq) {
  graphd_session *gses = graphd_request_session(greq);
  graphd_handle *g = gses->gses_graphd;
  int err;

  if (!pdb_disk_is_available(g->g_pdb) &&
      (err = graphd_checkpoint_optional(g)) != 0) {
    if (g->g_rep_master == gses) {
      cl_assert(g->g_cl, g->g_rep_master_address != NULL);
      cl_log(gses->gses_cl, CL_LEVEL_ERROR,
             "Out of disk space, aborting replication on %s",
             g->g_rep_master_address->addr_url);

      srv_session_abort(&gses->gses_ses);
    } else if (err == PDB_ERR_MORE) {
      /*  We're writing, but it's taking some time.
       *  Suspend this session until we're done writing
       *  and have room to move in.
       */
      graphd_session_delay(gses, 1);
    } else {
      if (err == ENOSPC)
        graphd_request_errprintf(greq, 0, "SYSTEM out of disk space");
      else
        graphd_request_errprintf(greq, 0, "SYSTEM %s", strerror(err));
    }

    return err;
  }

  return 0;
}

static char const *const graphd_session_type_names[] = {

    /* GRAPHD_SESSION_UNSPECIFIED 	*/ "unspecified",
    /* GRAPHD_SESSION_SERVER 	*/ "server->client",
    /* GRAPHD_SESSION_SMP_FOLLOWER 	*/ "leader->follower",
    /* GRAPHD_SESSION_SMP_LEADER 	*/ "follower->leader",
    /* GRAPHD_SESSION_REPLICA_CLIENT */ "master->replica",
    /* GRAPHD_SESSION_REPLICA_MASTER */ "replica->master",
    /* GRAPHD_SESSION_IMPORT 	*/ "importer->master"};

static char const *graphd_session_type_name(graphd_session const *const gses) {
  if (gses->gses_type >=
      sizeof(graphd_session_type_names) / sizeof(*graphd_session_type_names))
    return "<unexpected sesion type>";

  return graphd_session_type_names[gses->gses_type];
}

/**
 * @brief For debugging, report this session's details
 *
 * @param greq	request to dump
 * @param buf	buffer to dump into
 * @param size	number of bytes pointed to by buf.
 *
 * @return a pointer to buf.
 */
char const *graphd_session_to_string(graphd_session const *const gses,
                                     char *const buf, size_t size) {
  char *w = buf;
  char const *sep = "";
  size_t len;
  graphd_request const *greq;
  int some = 3;

  snprintf(w, size, "%s %llu@%p %s [%zu] (", graphd_session_type_name(gses),
           gses->gses_ses.ses_id, (void *)gses,
           srv_session_chain_name(&gses->gses_ses),
           srv_session_n_requests(&gses->gses_ses));

  w += len = strlen(w);
  size -= len;

  for (greq = (graphd_request *)gses->gses_ses.ses_request_head; greq != NULL;
       greq = (graphd_request *)greq->greq_req.req_next) {
    char b2[200];

    snprintf(w, size, "%s%s", sep,
             graphd_request_to_string(greq, b2, sizeof b2));
    sep = ", ";

    w += len = strlen(w);
    size -= len;

    if (--some <= 0) break;
  }

  if (greq != NULL) {
    snprintf(w, size, "...");
    w += len = strlen(w);
    size -= len;
  }

  if (size > 1) *w++ = ')';
  if (size > 0) *w = '\0';

  return buf;
}

void graphd_session_resume(graphd_session *gses) {
  if (gses->gses_suspend_reason != GRAPHD_SUSPEND_NOTHING) {
    gses->gses_suspend_reason = GRAPHD_SUSPEND_NOTHING;
    srv_session_resume(&gses->gses_ses);
  }
}

/*  Hey, session gses -- can I create another request inside you,
 *  or should I wait for you to catch up with your existing
 *  workload, and queue myself into your wait queue instead?
 *
 * @return true   Yes, go ahead and create a request here.
 * @return false  No, queue yourself in.
 */
bool graphd_session_has_room_for_request(graphd_session const *gses) {
  /*  If there's already a line, tell the caller to
   *  join the line.
   */
  if (gses->gses_request_wait_head != NULL) return false;

  return srv_session_n_requests(&gses->gses_ses) < GRAPHD_OUTGOING_REQUESTS_MAX;
}

void graphd_session_request_wait_add(graphd_session *gses, graphd_request *greq,
                                     unsigned int wakeup_ready) {
  char b1[200], b2[200];
  cl_handle *cl = graphd_request_cl(greq);

  cl_log(cl, CL_LEVEL_VERBOSE, "request %s to sesssion %s",
         graphd_request_to_string(greq, b1, sizeof b1),
         graphd_session_to_string(gses, b2, sizeof b2));

  cm_list_enqueue(graphd_request, graphd_session_wait_offsets,
                  &gses->gses_request_wait_head, &gses->gses_request_wait_tail,
                  greq);

  greq->greq_session_wait = gses;
  greq->greq_session_wait_ready = greq->greq_req.req_ready;
  greq->greq_req.req_ready = 0;

  srv_session_change(greq->greq_req.req_session, true,
                     "graphd_session_request_wait_add");

  srv_session_link(&gses->gses_ses);
  srv_request_link(&greq->greq_req);
}

void graphd_session_request_wait_remove(graphd_request *greq) {
  graphd_session *gses = greq->greq_session_wait;
  cl_handle *cl = graphd_request_cl(greq);
  char b1[200], b2[200];

  if (gses == NULL) return;

  cl_log(cl, CL_LEVEL_VERBOSE, "request %s from sesssion %s",
         graphd_request_to_string(greq, b1, sizeof b1),
         graphd_session_to_string(gses, b2, sizeof b2));

  srv_request_ready(&greq->greq_req, greq->greq_session_wait_ready);

  cm_list_remove(graphd_request, graphd_session_wait_offsets,
                 &gses->gses_request_wait_head, &gses->gses_request_wait_tail,
                 greq);
  greq->greq_session_wait = NULL;
  greq->greq_session_wait_ready = 0;
  greq->greq_session_wait_next = NULL;
  greq->greq_session_wait_prev = NULL;

  srv_session_unlink(&gses->gses_ses);
  srv_request_unlink(&greq->greq_req);
}

void graphd_session_request_wait_abort(graphd_request *greq) {
  graphd_session *gses = greq->greq_session_wait;
  cl_handle *cl = graphd_request_cl(greq);
  char b1[200], b2[200];

  if (gses == NULL) return;

  cl_log(cl, CL_LEVEL_VERBOSE, "aborting waiting request %s for sesssion %s",
         graphd_request_to_string(greq, b1, sizeof b1),
         graphd_session_to_string(gses, b2, sizeof b2));

  cm_list_remove(graphd_request, graphd_session_wait_offsets,
                 &gses->gses_request_wait_head, &gses->gses_request_wait_tail,
                 greq);

  greq->greq_session_wait = NULL;
  greq->greq_session_wait_ready = 0;
  greq->greq_session_wait_next = NULL;
  greq->greq_session_wait_prev = NULL;

  srv_session_unlink(&gses->gses_ses);
  srv_request_unlink(&greq->greq_req);
}

void graphd_session_request_wait_wakeup(graphd_session *gses) {
  graphd_request *greq;
  char buf[200];

  if (gses->gses_request_wait_head != NULL) {
    cl_enter(gses->gses_ses.ses_bc.bc_cl, CL_LEVEL_VERBOSE, "%s",
             graphd_session_to_string(gses, buf, sizeof buf));

    while ((greq = gses->gses_request_wait_head) != NULL)
      graphd_session_request_wait_remove(greq);

    cl_leave(gses->gses_ses.ses_bc.bc_cl, CL_LEVEL_VERBOSE, "leave");
  }
}
