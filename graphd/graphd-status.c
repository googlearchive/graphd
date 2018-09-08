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
#include <sys/resource.h>
#include <sys/time.h>

/*  STATUS
 *  ==========
 *
 *  The client sends a list of status keywords like "memory" or "connections".
 *
 *  For each of these keywords, the server sends back a list of
 *  status values.  Details depend on the status in question.
 *
 *  MEMORY
 *	If we're working on top of a tracing allocator, a list of
 *		(file line pointer size)
 *	tuples.
 *
 *  CONNECTION
 *	A list of
 *		("client-address" "last-action", "queue"
 *			first-activity 	last-activity
 *			inbytes 	outbytes
 *			inqueries 	outqueries
 *			request-millis
 *			full-command session-id request-or-session-id)
 *	for each connection.
 *
 *  DATABASE
 *	A list of ("name" "value") pairs for the database.
 *
 *  RUSAGE
 *	A list of ("name" "value") pairs for resource usage
 *
 *  ... and more; see graphd-property.c for simple cases.
 */

#define GRAPHD_STATUS_CONN_VERSION 2

typedef struct graphd_status_memory_fragment {
  struct graphd_status_memory_fragment* frag_next;
  char* frag_text;

} graphd_status_memory_fragment;

typedef struct graphd_status_context {
  graphd_handle* gsc_g;
  graphd_request* gsc_greq;
  cl_handle* gsc_cl;

  graphd_value* gsc_callback_result;

  graphd_status_memory_fragment* gsc_memory_head;
  graphd_status_memory_fragment** gsc_memory_tail;

} graphd_status_context;

/*  Append a name/value pair to our list of database statistics.
 *
 *  This is used both for the "database" status and the "tiles" status.
 */
static int graphd_status_db_callback(void* callback_data, char const* name,
                                     char const* value) {
  cl_handle* cl;
  cm_handle* cm;
  graphd_status_context* gsc = callback_data;
  graphd_value *li, *pair;
  int err;

  li = gsc->gsc_callback_result;
  cl = gsc->gsc_cl;
  cm = gsc->gsc_greq->greq_req.req_cm;

  cl_assert(cl, li != NULL);
  cl_assert(cl, li->val_type == GRAPHD_VALUE_LIST);

  /*  Make space in the list for one more element.
   */
  errno = 0;
  if ((pair = graphd_value_array_alloc(gsc->gsc_g, cl, li, 1)) == NULL) {
    int err = errno ? errno : ENOMEM;
    cl_log(cl, CL_LEVEL_ERROR,
           "graphd_status_db_callback: "
           "graphd_value_array_alloc fails: %s",
           strerror(err));
    return err;
  }

  /*  The new element is a two-element list (a "pair")
   */
  if ((err = graphd_value_list_alloc(gsc->gsc_g, cm, cl, pair, 2)) != 0) {
    cl_log(cl, CL_LEVEL_ERROR,
           "graphd_status_db_callback: "
           "graphd_value_list_alloc fails: %s",
           graphd_strerror(err));
    return err;
  }
  cl_assert(cl, pair->val_type == GRAPHD_VALUE_LIST);

  graphd_value_array_alloc_commit(cl, li, 1);

  /*  Its first half is the name.
   */
  err =
      graphd_value_text_strdup(cm, pair->val_list_contents, GRAPHD_VALUE_STRING,
                               name, name + strlen(name));
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_value_text_strdup", err,
                 "can't duplicate name \"%s\"", name);
    return err;
  }

  /*  Its second half, the value.
   */
  err = graphd_value_text_strdup(cm, pair->val_list_contents + 1,
                                 GRAPHD_VALUE_STRING, value,
                                 value + strlen(value));
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_value_text_strdup", err,
                 "can't duplicate value \"%s\"", value);
    return err;
  }
  return 0;
}

/*
 *  Report on the database underlying the server.
 */
static int graphd_status_database(graphd_status_context* gsc,
                                  graphd_value* val) {
  int err;
  char const* checkpoint_state = "undefined";

  err = graphd_value_list_alloc(gsc->gsc_g, gsc->gsc_greq->greq_req.req_cm,
                                gsc->gsc_cl, val, 0);
  if (err != 0) {
    cl_log(gsc->gsc_cl, CL_LEVEL_FAIL,
           "graphd_status_database: failed to "
           "allocate 0 entries: %s",
           graphd_strerror(err));
    return err;
  }

  cl_cover(gsc->gsc_cl);

  gsc->gsc_callback_result = val;

  switch (gsc->gsc_g->g_checkpoint_state) {
    case GRAPHD_CHECKPOINT_CURRENT:
      checkpoint_state = "current";
      break;
    case GRAPHD_CHECKPOINT_PENDING:
      checkpoint_state = "pending";
      break;
    default:
      cl_notreached(gsc->gsc_cl, "unexpected g_checkpoint_state %d",
                    gsc->gsc_g->g_checkpoint_state);
      break;
  }

  err = graphd_status_db_callback(gsc, "database", checkpoint_state);
  if (err) {
    cl_log_errno(gsc->gsc_cl, CL_LEVEL_ERROR, "graphd_status_db_callback", err,
                 "can't report checkpoint_state");
    return err;
  }

  err = pdb_status(gsc->gsc_g->g_pdb, graphd_status_db_callback, gsc);
  if (err)
    cl_log(gsc->gsc_cl, CL_LEVEL_FAIL,
           "graphd_status_database: pdb_status fails: %s",
           graphd_strerror(err));
  return err;
}

/* Replica status consists of:
 *
 *   (("master" "peer") ("write" "peer") ("replica1" ... "replicaN"))
 *
 *   Where a "master" and "write" are the addresses of the
 *   master and write servers and an empty "peer", ie. "", indicates
 *   a lack of connection.
 */
static int graphd_status_replica(graphd_status_context* gsc,
                                 graphd_value* val) {
  cm_handle* const cm = gsc->gsc_greq->greq_req.req_cm;
  cl_handle* const cl = gsc->gsc_cl;
  graphd_handle* const g = gsc->gsc_g;
  graphd_value* master_val;
  graphd_value* write_val;
  graphd_value* replicas_val;
  graphd_value* replica_val;
  graphd_session* gses = g->g_rep_sessions;
  size_t n_replicas = 0;
  char const* nm;
  int err;

  while (gses) {
    n_replicas++;
    gses = gses->gses_data.gd_rep_client.gdrc_next;
  }

  err = graphd_value_list_alloc(gsc->gsc_g, cm, cl, val, 3);
  if (err) goto value_list_alloc_failed;

  master_val = val->val_list_contents;
  write_val = val->val_list_contents + 1;
  replicas_val = val->val_list_contents + 2;

  err = graphd_value_list_alloc(gsc->gsc_g, cm, cl, master_val, 2);
  if (err) goto value_list_alloc_failed;

  err = graphd_value_list_alloc(gsc->gsc_g, cm, cl, write_val, 2);
  if (err) goto value_list_alloc_failed;

  err = graphd_value_list_alloc(gsc->gsc_g, cm, cl, replicas_val, n_replicas);
  if (err) goto value_list_alloc_failed;

  /* The replica server
   */
  if (g->g_rep_master)
    if (g->g_rep_master->gses_ses.ses_displayname)
      nm = g->g_rep_master->gses_ses.ses_displayname;
    else
      nm = "unnamed session";
  else
    nm = "";
  err = graphd_value_text_strdup(cm, master_val->val_list_contents,
                                 GRAPHD_VALUE_STRING, nm, nm + strlen(nm));
  if (err) goto value_text_strdup_failed;

  if (g->g_rep_master_address)
    nm = g->g_rep_master_address->addr_url;
  else
    nm = "";
  err = graphd_value_text_strdup(cm, master_val->val_list_contents + 1,
                                 GRAPHD_VALUE_STRING, nm, nm + strlen(nm));
  if (err) goto value_text_strdup_failed;

  /* The write server
   */
  if (g->g_rep_write)
    if (g->g_rep_write->gses_ses.ses_displayname)
      nm = g->g_rep_write->gses_ses.ses_displayname;
    else
      nm = "unnamed session";
  else
    nm = "";
  err = graphd_value_text_strdup(cm, write_val->val_list_contents,
                                 GRAPHD_VALUE_STRING, nm, nm + strlen(nm));
  if (err) goto value_text_strdup_failed;

  if (g->g_rep_write_address)
    nm = g->g_rep_write_address->addr_url;
  else
    nm = "";
  err = graphd_value_text_strdup(cm, write_val->val_list_contents + 1,
                                 GRAPHD_VALUE_STRING, nm, nm + strlen(nm));
  if (err) goto value_text_strdup_failed;

  /* The replication sessions
   */
  replica_val = replicas_val->val_list_contents;
  gses = g->g_rep_sessions;
  while (gses) {
    if (gses->gses_ses.ses_displayname)
      nm = gses->gses_ses.ses_displayname;
    else
      nm = "unnamed session";

    err = graphd_value_text_strdup(cm, replica_val, GRAPHD_VALUE_STRING, nm,
                                   nm + strlen(nm));

    if (err) goto value_text_strdup_failed;

    gses = gses->gses_data.gd_rep_client.gdrc_next;
    replica_val++;
  }

  return 0;

value_list_alloc_failed:
  cl_log_errno(gsc->gsc_cl, CL_LEVEL_FAIL, "graphd_value_list_alloc", err,
               "graphd_status_replica: failed to allocate list");

  return err;

value_text_strdup_failed:
  cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_value_text_strdup", err,
               "can't duplicate \"%s\"", nm);

  return err;
}

/*
 *  Get tile statistics for the database
 */
static int graphd_status_tiles(graphd_status_context* gsc, graphd_value* val) {
  int err;

  err = graphd_value_list_alloc(gsc->gsc_g, gsc->gsc_greq->greq_req.req_cm,
                                gsc->gsc_cl, val, 0);
  if (err != 0) {
    cl_log(gsc->gsc_cl, CL_LEVEL_FAIL,
           "graphd_status_database: failed to "
           "allocate 0 entries: %s",
           graphd_strerror(err));
    return err;
  }

  cl_cover(gsc->gsc_cl);

  gsc->gsc_callback_result = val;

  return pdb_status_tiles(gsc->gsc_g->g_pdb, graphd_status_db_callback, gsc);
}

/*
 *  Dump the diary, starting with the first unread entry, if specified.
 */
static int graphd_status_diary(graphd_status_context* gsc,
                               unsigned long long first_unread,
                               graphd_value* val) {
  int err;
  size_t size, n_entries;
  char* heap = NULL;
  cm_handle* cm = gsc->gsc_greq->greq_req.req_cm;
  cl_handle* cl = gsc->gsc_cl;
  cl_diary_handle* d = gsc->gsc_g->g_diary;
  cl_diary_entry de;
  graphd_value* val_entry = NULL;
  unsigned long long first_sent = first_unread;

  /*  To avoid geometric " explosion, don't log the results of
   *  this in the diary.
   */
  gsc->gsc_greq->greq_req.req_log_output = false;

  /*  How many entries are there?
   */
  n_entries = cl_diary_entries(d);
  size = cl_diary_total_size(d);

  if (size > 0)
    if ((heap = cm_malloc(cm, size)) == NULL) return ENOMEM;

  memset(&de, 0, sizeof(de));
  while (n_entries > 0 && cl_diary_entry_next(d, &de) == 0) {
    size_t entry_size;
    unsigned long long serial;

    n_entries--;

    serial = cl_diary_entry_serial(d, &de);
    if (serial < first_unread) continue;

    /*  Make a buffer for the results in the request data space.
     */
    if (val_entry == NULL) {
      err = graphd_value_list_alloc(gsc->gsc_g, cm, cl, val, n_entries + 2);
      if (err != 0) {
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_status_diary: failed to "
               "allocate 0 entries: %s",
               graphd_strerror(err));
        return err;
      }
      cl_cover(cl);
      val_entry = val->val_list_contents + 1;
    }

    entry_size = cl_diary_entry_read(d, &de, heap, size);
    cl_assert(cl, entry_size <= size);
    graphd_value_text_set(val_entry, GRAPHD_VALUE_STRING, heap,
                          heap + entry_size, NULL);

    heap += entry_size;
    size -= entry_size;
    val_entry++;
  }

  /*  Fill in the first element of the resulting list,
   *  statistics about the diary itself.
   */
  if (val_entry == NULL) {
    err = graphd_value_list_alloc(gsc->gsc_g, cm, cl, val, 1);
    if (err != 0) {
      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_status_diary: failed to "
             "allocate a single entry: %s",
             graphd_strerror(err));
      return err;
    }
    cl_cover(cl);
  }
  val_entry = val->val_list_contents;

  err = graphd_value_list_alloc(gsc->gsc_g, cm, cl, val_entry, 3);
  if (err != 0) {
    cl_log(cl, CL_LEVEL_FAIL,
           "graphd_status_diary: failed to "
           "allocate a single entry: %s",
           graphd_strerror(err));
    return err;
  }
  val_entry = val_entry->val_list_contents;

  /*  First sent.
   */
  graphd_value_number_set(val_entry, first_sent);
  val_entry++;

  /*  Number of entries sent.
   */
  graphd_value_number_set(val_entry, val->val_list_n - 1);
  val_entry++;

  /*  Current time on the server.
   */
  graphd_value_number_set(val_entry, gsc->gsc_g->g_now);
  return 0;
}

static void memory_callback(void* callback_data, int level, char const* str,
                            ...) {
  graphd_status_context* gsc = callback_data;
  size_t str_n;
  graphd_status_memory_fragment* frag;
  char bigbuf[1024];
  va_list ap;

  if (str == NULL) return;

  va_start(ap, str);
  vsnprintf(bigbuf, sizeof bigbuf, str, ap);
  va_end(ap);

  str_n = strlen(bigbuf);
  frag = malloc(sizeof(*frag) + str_n + 1);
  if (frag == NULL)
    cl_log(gsc->gsc_cl, CL_LEVEL_ERROR,
           "failed to allocate %llu bytes for memory "
           "fragment report",
           (unsigned long long)(sizeof(*frag) + str_n + 1));
  else {
    cl_assert(gsc->gsc_cl, gsc->gsc_memory_tail != NULL);

    *gsc->gsc_memory_tail = frag;
    gsc->gsc_memory_tail = &frag->frag_next;

    frag->frag_next = NULL;
    frag->frag_text = (char*)(frag + 1);
    memcpy((char*)(frag + 1), bigbuf, str_n + 1);
  }
}

/*
 *  Report on the memory allocated by the server, as reported by
 *  the tracer module.
 */
static int graphd_status_memory(graphd_status_context* gsc, graphd_value* val) {
  graphd_status_memory_fragment *frag, *next;
  size_t n;
  int err;

  if (gsc->gsc_memory_head == NULL)
    if (srv_memory_list(gsc->gsc_greq->greq_req.req_session->ses_srv,
                        memory_callback, gsc)) {
      graphd_value_null_set(val);
      return 0;
    }

  n = 0;
  for (frag = gsc->gsc_memory_head; frag != NULL; frag = frag->frag_next) n++;

  /* Allocate <n> list slots. */

  err = graphd_value_list_alloc(gsc->gsc_g, gsc->gsc_greq->greq_req.req_cm,
                                gsc->gsc_cl, val, n);
  if (err != 0)
    val->val_type = GRAPHD_VALUE_NULL;
  else {
    for (frag = gsc->gsc_memory_head, n = 0; frag != NULL;
         frag = frag->frag_next, n++) {
      err = graphd_value_text_strdup(gsc->gsc_greq->greq_req.req_cm,
                                     val->val_list_contents + n,
                                     GRAPHD_VALUE_STRING, frag->frag_text,
                                     frag->frag_text + strlen(frag->frag_text));
      if (err) {
        val->val_type = GRAPHD_VALUE_NULL;
        goto err;
      }
    }
  }

err: /*  Free the temporaries returend by the fragmenter.
      */
  next = gsc->gsc_memory_head;
  while ((frag = next) != NULL) {
    next = frag->frag_next;
    free(frag);
  }
  gsc->gsc_memory_head = NULL;
  gsc->gsc_memory_tail = &gsc->gsc_memory_head;

  return 0;
}

static int connection_get_full_request(cl_handle* cl, cm_handle* cm,
                                       graphd_session* gses, char** s_out,
                                       size_t* n_out) {
  size_t need = 0;
  srv_request* req;
  void* state;
  char const* text_s;
  size_t text_n;
  char* w;

  /*  Which request is executing?
   */
  req = gses->gses_ses.ses_request_head;
  if (!req || !req->req_first) return GRAPHD_ERR_NO;

  /*  How many bytes do we need to store its text in one string?
   */
  state = NULL;
  while (srv_request_text_next(req, &text_s, &text_n, &state) == 0)
    need += text_n;

  /*  Allocate a string of that size (plus a '\0')
   */
  w = *s_out = cm_malloc(cm, need + 1);
  if (*s_out == NULL) return ENOMEM;

  /*  Copy into the string.
   */
  state = NULL;
  while (srv_request_text_next(req, &text_s, &text_n, &state) == 0) {
    cl_assert(cl, w + text_n <= *s_out + need);
    if (text_n != 0) {
      memcpy(w, text_s, text_n);
      w += text_n;
    }
  }

  cl_assert(cl, w == *s_out + need);
  *w = '\0';
  *n_out = w - *s_out;

  return 0;
}

/**
 * @brief Print data for a single connection
 * @param callback_data pointer to  graphd_status_context, opaque to libsrv
 * @param ses runtime context
 * @param queue name of the queue the session is in (I/O, MEM, or RUN)
 */
static int connection_callback(void* callback_data, srv_session* ses) {
  graphd_value *val, *li;
  graphd_status_context* gsc = callback_data;
  graphd_session* gses = (graphd_session*)ses;
  cl_handle* cl = gsc->gsc_cl;
  cm_handle* cm = gsc->gsc_greq->greq_req.req_cm;
  char* command_s;
  size_t command_n;
  int err;

  val = graphd_value_array_alloc(gsc->gsc_g, cl, gsc->gsc_callback_result, 1);
  if (val == NULL) return ENOMEM;

  err = graphd_value_list_alloc(gsc->gsc_g, cm, cl, val, 14);
  if (err) return err;

  li = val->val_list_contents;

  err = graphd_value_text_strdup(
      cm, li++, GRAPHD_VALUE_STRING, /* 1 */
      ses->ses_displayname,
      ses->ses_displayname + strlen(ses->ses_displayname));
  if (err) return err;

  graphd_value_text_strdup(
      cm, li++, GRAPHD_VALUE_STRING, /* 2 */
      gses->gses_last_action,
      gses->gses_last_action + strlen(gses->gses_last_action));

  graphd_value_timestamp_set(li++, gses->gses_time_created,
                             PDB_ID_NONE);                               /* 3 */
  graphd_value_timestamp_set(li++, gses->gses_time_active, PDB_ID_NONE); /* 4 */
  graphd_value_number_set(li++, ses->ses_bc.bc_total_bytes_in);          /* 5 */
  graphd_value_number_set(li++, ses->ses_bc.bc_total_bytes_out);         /* 6 */
  graphd_value_number_set(li++, ses->ses_requests_in);                   /* 7 */
  graphd_value_number_set(li++, ses->ses_requests_out);                  /* 8 */
  graphd_value_number_set(li++, ses->ses_requests_made);                 /* 9 */
  graphd_value_number_set(li++, ses->ses_replies_received); /* 10 */
  graphd_value_number_set(li++, ses->ses_requests_millis);  /* 11 */

  /* 12 */
  err = connection_get_full_request(cl, cm, gses, &command_s, &command_n);
  if (err != 0)
    graphd_value_null_set(li++);
  else {
    if (command_n >= 1 && command_s[command_n - 1] == '\n') command_n--;
    if (command_n >= 1 && command_s[command_n - 1] == '\r') command_n--;

    graphd_value_text_set_cm(li++, GRAPHD_VALUE_STRING, command_s, command_n,
                             cm);
  }

  graphd_value_number_set(li++, ses->ses_id);         /* 13 */
  graphd_value_number_set(li++, ses->ses_request_head /* 14 */
                                    ? ses->ses_request_head->req_id
                                    : ses->ses_id);

  graphd_value_array_alloc_commit(cl, gsc->gsc_callback_result, 1);
  return 0;
}

static int graphd_status_connection(graphd_status_context* gsc,
                                    graphd_value* val) {
  int err;
  graphd_value* vers;

  graphd_value_initialize(val);

  err = graphd_value_list_alloc(gsc->gsc_g, gsc->gsc_greq->greq_req.req_cm,
                                gsc->gsc_cl, val, 0);
  cl_assert(gsc->gsc_cl, err == 0);
  gsc->gsc_callback_result = val;

  vers = graphd_value_array_alloc(gsc->gsc_g, gsc->gsc_cl,
                                  gsc->gsc_callback_result, 1);
  if (vers == NULL) return ENOMEM;

  graphd_value_number_set(vers, GRAPHD_STATUS_CONN_VERSION);
  graphd_value_array_alloc_commit(gsc->gsc_cl, gsc->gsc_callback_result, 1);

  if (gsc->gsc_greq->greq_smp_request_collection_chain == NULL) {
    if (srv_session_list(gsc->gsc_greq->greq_req.req_session->ses_srv,
                         connection_callback, gsc)) {
      graphd_value_finish(gsc->gsc_cl, val);
      graphd_value_null_set(val);
      cl_cover(gsc->gsc_cl);
    }
  } else {
    err = graphd_smp_status_append_to_list(gsc->gsc_greq,
                                           gsc->gsc_callback_result);
    if (err) return err;
  }
  return 0;
}

static int named_number(graphd_handle* g, cm_handle* cm, cl_handle* cl,
                        graphd_value* val, char const* name,
                        unsigned long long number) {
  graphd_value* el;
  int err;

  err = graphd_value_list_alloc(g, cm, cl, val, 2);
  if (err != 0) return err;

  el = val->val_list_contents;
  graphd_value_atom_set_constant(el++, name, strlen(name));
  graphd_value_number_set(el, number);

  cl_cover(cl);
  return 0;
}

static int graphd_status_rusage(graphd_status_context* gsc, graphd_value* val) {
  struct rusage ru;
  graphd_value* pair;
  cm_handle* cm = gsc->gsc_greq->greq_req.req_cm;
  cl_handle* cl = gsc->gsc_cl;
  int err;

  if (getrusage(RUSAGE_SELF, &ru) ||
      graphd_value_list_alloc(gsc->gsc_g, cm, cl, val, 4) != 0) {
  err:
    graphd_value_finish(cl, val);
    graphd_value_null_set(val);
    cl_cover(cl);

    return 0;
  }
  pair = val->val_list_contents;
  err = named_number(
      gsc->gsc_g, cm, cl, pair++, "user-time-millis",
      ru.ru_utime.tv_sec * 1000ull + ru.ru_utime.tv_usec / 1000ull);
  if (err) goto err;

  err = named_number(
      gsc->gsc_g, cm, cl, pair++, "system-time-millis",
      ru.ru_stime.tv_sec * 1000ull + ru.ru_stime.tv_usec / 1000ull);
  if (err) goto err;

  err = named_number(gsc->gsc_g, cm, cl, pair++, "max-rss", ru.ru_maxrss);
  if (err) goto err;

  err = named_number(gsc->gsc_g, cm, cl, pair++, "data", ru.ru_idrss);
  if (err) goto err;

  cl_cover(cl);
  return 0;
}

static bool graphd_status_needs_forwarding(graphd_request* greq) {
  graphd_status_subject* su;
  for (su = greq->greq_data.gd_status.gds_statqueue.statqueue_head; su != NULL;
       su = su->stat_next) {
    switch (su->stat_subject) {
      case GRAPHD_STATUS_CONNECTION:
      case GRAPHD_STATUS_REPLICA:
        return true;
        break;
      default:
        break;
    }
  }
  return false;
}

int graphd_status(graphd_request* greq) {
  graphd_value* val = &greq->greq_reply;
  graphd_handle* g = graphd_request_graphd(greq);
  cl_handle* cl = graphd_request_cl(greq);
  graphd_status_context gsc;
  graphd_status_subject* su;
  int err;
  size_t n;

  cl_enter(cl, CL_LEVEL_VERBOSE, "statqueue_head=%p",
           (void*)greq->greq_data.gd_status.gds_statqueue.statqueue_head);

  /* Set up a context for this query.
   */
  memset(&gsc, 0, sizeof gsc);
  gsc.gsc_greq = greq;
  gsc.gsc_g = graphd_request_graphd(greq);
  gsc.gsc_cl = cl;
  gsc.gsc_memory_head = NULL;
  gsc.gsc_memory_tail = &gsc.gsc_memory_head;

  /* We'll return a list with as many elements as
   * our status queue.  Measure its length;
   */
  su = greq->greq_data.gd_status.gds_statqueue.statqueue_head;
  for (n = 0; su; n++) su = su->stat_next;

  /*  Allocate that many list slots.
   */
  err = graphd_value_list_alloc(g, greq->greq_req.req_cm, cl, val, n);
  if (err != 0) {
    cl_log(cl, CL_LEVEL_FAIL,
           "graphd_status: "
           "failed to allocate %llu list elements: %s",
           (unsigned long long)n, graphd_strerror(err));
    cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
    return err;
  }

  /* Compute the actual results.
   */
  for (su = greq->greq_data.gd_status.gds_statqueue.statqueue_head, n = 0;
       su != NULL; n++, su = su->stat_next) {
    err = graphd_smp_status_next_tokens(greq);
    if (err)
      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_status: "
             "graphd_smp_status_next_tokens fails: %s",
             graphd_strerror(err));

    switch (su->stat_subject) {
      case GRAPHD_STATUS_CONNECTION:
        cl_cover(cl);
        err = graphd_status_connection(&gsc, val->val_list_contents + n);
        if (err != 0)
          cl_log(cl, CL_LEVEL_FAIL,
                 "graphd_status: "
                 "graphd_status_connection fails: %s",
                 graphd_strerror(err));
        break;

      case GRAPHD_STATUS_DATABASE:
        cl_cover(cl);
        err = graphd_status_database(&gsc, val->val_list_contents + n);
        if (err != 0)
          cl_log(cl, CL_LEVEL_FAIL,
                 "graphd_status: "
                 "graphd_status_database fails: %s",
                 graphd_strerror(err));
        break;

      case GRAPHD_STATUS_TILES:
        cl_cover(cl);
        err = graphd_status_tiles(&gsc, val->val_list_contents + n);
        if (err != 0)
          cl_log(cl, CL_LEVEL_FAIL,
                 "graphd_status: "
                 "graphd_status_tiles fails: %s",
                 graphd_strerror(err));
        break;

      case GRAPHD_STATUS_DIARY:
        cl_cover(cl);
        err = graphd_status_diary(&gsc, su->stat_number,
                                  val->val_list_contents + n);
        if (err != 0)
          cl_log(cl, CL_LEVEL_FAIL,
                 "graphd_status: "
                 "graphd_status_diary fails: %s",
                 graphd_strerror(err));
        break;

      case GRAPHD_STATUS_MEMORY:
        cl_cover(cl);
        err = graphd_status_memory(&gsc, val->val_list_contents + n);
        if (err != 0)
          cl_log(cl, CL_LEVEL_FAIL,
                 "graphd_status: "
                 "graphd_status_memory fails: %s",
                 graphd_strerror(err));
        break;

      case GRAPHD_STATUS_RUSAGE:
        cl_cover(cl);
        err = graphd_status_rusage(&gsc, val->val_list_contents + n);
        if (err != 0)
          cl_log(cl, CL_LEVEL_FAIL,
                 "graphd_status: "
                 "graphd_status_rusage fails: %s",
                 graphd_strerror(err));
        break;

      case GRAPHD_STATUS_PROPERTY:
        cl_assert(cl, su->stat_property != NULL);
        if (su->stat_property->prop_status == NULL)
          graphd_request_errprintf(greq, 0,
                                   "SEMANTICS property \"%s\" "
                                   "cannot be queried",
                                   su->stat_property->prop_name);
        else
          err = su->stat_property->prop_status(su->stat_property, greq,
                                               val->val_list_contents + n);
        break;

      case GRAPHD_STATUS_REPLICA:
        err = graphd_status_replica(&gsc, val->val_list_contents + n);
        if (err)
          cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_status_replica", err,
                       "graphd_status: unable to get "
                       "replication status");
        break;

      case GRAPHD_STATUS_ISLINK:
        cl_cover(cl);
        err = graphd_islink_status(gsc.gsc_greq, val->val_list_contents + n);
        if (err != 0)
          cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_islink_status ", err,
                       "unexpected error");
        break;

      default:
        cl_notreached(cl, "unexpected status subject %d", su->stat_subject);
    }

    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
      return err;
    }
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "leave");
  return 0;
}

static int graphd_status_run(graphd_request* greq,
                             unsigned long long deadline) {
  graphd_session* gses = graphd_request_session(greq);
  graphd_handle* g = graphd_request_graphd(greq);
  int err;

  (void)deadline;

  if (g->g_smp_proc_type == GRAPHD_SMP_PROCESS_FOLLOWER &&
      gses != g->g_smp_leader && graphd_status_needs_forwarding(greq)) {
    /*  We're a follower, forwarding this request.
     *  We're done running, but we won't be ready for
     *  output until the passthrough request is.
     */

    err = graphd_leader_passthrough(greq);

    if (err != GRAPHD_ERR_MORE && err != GRAPHD_ERR_SUSPEND)
      srv_request_run_done(&greq->greq_req);

    else if (err == GRAPHD_ERR_SUSPEND) {
      srv_request_suspend(&greq->greq_req);
      err = GRAPHD_ERR_MORE;
    }
    return err;
  }

  if (g->g_smp_proc_type == GRAPHD_SMP_PROCESS_LEADER) {
    if (!greq->greq_smp_forward_started) {
      err = graphd_smp_start_forward_outgoing(greq);
      if (err) return err;
      /* suspend ourselves -- our subrequests will
       * wake us up
       */

      return GRAPHD_ERR_MORE;
    } else {
      if (!graphd_smp_finished_forward_outgoing(greq)) return GRAPHD_ERR_MORE;

      err = graphd_smp_status_init_tokens(greq);
      if (err) return err;
      /* fallthrough */
    }
  }

  /* Run the status command, and then we're done */

  err = graphd_status(greq);
  if (err != GRAPHD_ERR_MORE) {
    graphd_request_served(greq);
    graphd_smp_forward_unlink_all(greq);
  }

  return err;
}

static void graphd_status_input_arrived(graphd_request* greq) {
  srv_request_run_ready(&greq->greq_req);
}

static graphd_request_type graphd_status_request = {
    "status", graphd_status_input_arrived,
    /* graphd_status_output_sent */ NULL, graphd_status_run,
    /* graphd_status_free */ NULL};

int graphd_status_initialize(graphd_request* greq) {
  graphd_status_queue* q;
  cl_handle* cl = graphd_request_cl(greq);

  cl_assert(cl, greq->greq_type == GRAPHD_REQUEST_UNSPECIFIED);

  greq->greq_request = GRAPHD_REQUEST_STATUS;
  greq->greq_type = &graphd_status_request;

  q = &greq->greq_data.gd_status.gds_statqueue;

  q->statqueue_head = NULL;
  q->statqueue_tail = &q->statqueue_head;

  return 0;
}
