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

#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>

#define IS_LIT(s, e, lit)          \
  ((e) - (s) == sizeof(lit) - 1 && \
   strncasecmp((s), (lit), sizeof(lit) - 1) == 0)

/* graphd-property.c -- descriptor structures and implementations
 * 	for named configuration values that can be set and read.
 */

static int prop_this_loglevel_status(graphd_property const* prop,
                                     graphd_request* greq, graphd_value* val,
                                     cl_handle* cl) {
  char const* value = NULL;
  char bigbuf[1024];
  char* value_dup;
  cm_handle* cm = greq->greq_req.req_cm;
  size_t need;
  cl_loglevel_configuration clc;

  if (cl == NULL) {
    graphd_value_null_set(val);
    return 0;
  }

  cl_get_loglevel_configuration(cl, &clc);
  if ((value = cl_loglevel_configuration_to_string(
           &clc, graphd_facilities, bigbuf, sizeof bigbuf)) == NULL) {
    graphd_value_null_set(val);
    return 0;
  }
  need = strlen(value) + 3;
  if ((value_dup = cm_malloc(cm, need)) == NULL) return ENOMEM;
  snprintf(value_dup, need, "(%s)", value);
  graphd_value_text_set_cm(val, GRAPHD_VALUE_ATOM, value_dup, need - 1, cm);
  return 0;
}

static int prop_instanceid_set(graphd_property const* prop,
                               graphd_request* greq,
                               graphd_set_subject const* su) {
  graphd_handle* g = graphd_request_graphd(greq);

  /* Setting the instance-id to "" clears it entirely.  Otherwise...
   */
  if (su->set_value_s < su->set_value_e) {
    if (!graph_dateline_instance_verify(su->set_value_s, su->set_value_e)) {
      graphd_request_errprintf(greq, 0,
                               "SEMANTICS instance ID may only contain "
                               "letters and numbers, and must be between "
                               "1 and %d characters long (got \"%.*s\")",
                               GRAPH_INSTANCE_ID_SIZE,
                               (int)(su->set_value_e - su->set_value_s),
                               su->set_value_s);
      return GRAPHD_ERR_SEMANTICS;
    }
    memcpy(g->g_instance_id, su->set_value_s,
           su->set_value_e - su->set_value_s);
  }
  g->g_instance_id[su->set_value_e - su->set_value_s] = 0;

  /* Invalidate the global cached dateline.
   */
  graphd_dateline_expire(g);
  return 0;
}

/* ----------------------------------------------------------------------
   ACCESS -- enum or list of enums; graphd-private access          graphd
   ---------------------------------------------------------------------- */

static int prop_access_set(graphd_property const* prop, graphd_request* greq,
                           graphd_set_subject const* su) {
  graphd_handle* g = graphd_request_graphd(greq);
  int err;
  char buf[1024];
  bool err_retriable = false;

  graphd_access_global acc;

  acc = graphd_access_global_from_string(su->set_value_s, su->set_value_e);
  if ((int)acc < 0) {
    graphd_request_errprintf(
        greq, 0, "SYNTAX \"%.*s\" is not a valid access mode",
        (int)(su->set_value_e - su->set_value_s), su->set_value_s);
    return GRAPHD_ERR_SYNTAX;
  }

  err = graphd_access_set_global(g, acc, &err_retriable, buf, sizeof buf);
  if (err != 0) {
    graphd_request_errprintf(
        greq, 0, "%s cannot set access mode to \"%s\": %s",
        err == GRAPHD_ERR_NOT_A_REPLICA ? "NOTREPLICA" : "SYSTEM",
        graphd_access_global_to_string(acc), buf);
    return 0;
  }
  return 0;
}

static int prop_instanceid_status(graphd_property const* prop,
                                  graphd_request* greq, graphd_value* val) {
  graphd_handle* g = graphd_request_graphd(greq);
  cm_handle* cm = graphd_request_cm(greq);

  return graphd_value_text_strdup(cm, val, GRAPHD_VALUE_STRING,
                                  g->g_instance_id,
                                  g->g_instance_id + strlen(g->g_instance_id));
}

static int prop_access_status(graphd_property const* prop, graphd_request* greq,
                              graphd_value* val) {
  char const* value = NULL;
  graphd_handle* g = graphd_request_graphd(greq);

  switch (g->g_access) {
    case GRAPHD_ACCESS_READ_WRITE:
      value = "read-write";
      break;
    case GRAPHD_ACCESS_READ_ONLY:
      value = "read-only";
      break;
    case GRAPHD_ACCESS_REPLICA:
      value = "replica";
      break;
    case GRAPHD_ACCESS_REPLICA_SYNC:
      value = "replica-sync";
      break;
    case GRAPHD_ACCESS_ARCHIVE:
      value = "archive";
      break;
    case GRAPHD_ACCESS_RESTORE:
      value = "restore";
      break;
    case GRAPHD_ACCESS_SHUTDOWN:
      value = "shutdown";
      break;

    default:
      cl_notreached(graphd_request_cl(greq), "unexpected g_access %d",
                    g->g_access);
      /* NOTREACHED */
  }
  graphd_value_text_set(val, GRAPHD_VALUE_STRING, value, value + strlen(value),
                        NULL);
  return 0;
}

/* ----------------------------------------------------------------------
   CORE	-- boolean; dump core when crashing?                       libsrv
   ---------------------------------------------------------------------- */

static int prop_core_set(graphd_property const* prop, graphd_request* greq,
                         graphd_set_subject const* su) {
  if (IS_LIT(su->set_value_s, su->set_value_e, "true"))
    srv_set_want_core(greq->greq_req.req_session->ses_srv, true);
  else if (IS_LIT(su->set_value_s, su->set_value_e, "false"))
    srv_set_want_core(greq->greq_req.req_session->ses_srv, false);
  else {
    graphd_request_errprintf(greq, 0,
                             "SYNTAX \"core\" can be set "
                             "to \"true\" or \"false\", got \"%.*s\"",
                             (int)(su->set_value_e - su->set_value_s),
                             su->set_value_s);
    return GRAPHD_ERR_SYNTAX;
  }
  return 0;
}

static int prop_core_status(graphd_property const* prop, graphd_request* greq,
                            graphd_value* val) {
  graphd_value_boolean_set(val,
                           srv_want_core(greq->greq_req.req_session->ses_srv));
  return 0;
}

/* ----------------------------------------------------------------------
   COST -- maximum per-request cost	    		           graphd
   ---------------------------------------------------------------------- */

static int prop_cost_set(graphd_property const* prop, graphd_request* greq,
                         graphd_set_subject const* su) {
  char errbuf[200];

  int err;

  graphd_runtime_statistics rts;

  err = graphd_cost_from_string(&rts, su->set_value_s, su->set_value_e, errbuf,
                                sizeof errbuf);
  if (err) {
    graphd_request_errprintf(
        greq, 0, "SEMANTICS cannot parse cost string \"%.*s\"",
        (int)(su->set_value_e - su->set_value_s), su->set_value_s);
    return GRAPHD_ERR_SEMANTICS;
  }

  graphd_cost_set(graphd_request_graphd(greq), &rts);
  return 0;
}

static int prop_cost_status(graphd_property const* prop, graphd_request* greq,
                            graphd_value* val) {
  char buf[10 * 50];
  char const* cost;
  graphd_handle* g = graphd_request_graphd(greq);

  cost = graphd_cost_limit_to_string(&g->g_runtime_statistics_allowance, buf,
                                     sizeof buf);

  return graphd_value_text_strdup(greq->greq_req.req_cm, val,
                                  GRAPHD_VALUE_STRING, cost,
                                  cost + strlen(cost));
}

/* ----------------------------------------------------------------------
   LOGFLUSH -- flush policy for log files 		    	    libcl
   ---------------------------------------------------------------------- */

static int prop_logflush_set(graphd_property const* prop, graphd_request* greq,
                             graphd_set_subject const* su) {
  cl_handle* cl = srv_log(greq->greq_req.req_session->ses_srv);

  bool policy;
  if (IS_LIT(su->set_value_s, su->set_value_e, "true"))
    policy = true;
  else if (IS_LIT(su->set_value_s, su->set_value_e, "false"))
    policy = false;
  else {
    graphd_request_errprintf(greq, 0,
                             "SYNTAX \"logflush\" can be set "
                             "to \"true\" or \"false\", got \"%.*s\"",
                             (int)(su->set_value_e - su->set_value_s),
                             su->set_value_s);
    return GRAPHD_ERR_SYNTAX;
  }
  if (cl != NULL)
    cl_set_flush_policy(cl, policy ? CL_FLUSH_ALWAYS : CL_FLUSH_NEVER);
  return 0;
}

static int prop_logflush_status(graphd_property const* prop,
                                graphd_request* greq, graphd_value* val) {
  cl_handle* cl;
  char const* ptr;
  char buf[200];

  cl = graphd_request_cl(greq);
  if (cl == NULL) {
    graphd_value_null_set(val);
    return 0;
  }
  switch (cl_get_flush_policy(cl)) {
    case CL_FLUSH_NEVER:
      graphd_value_boolean_set(val, false);
      break;
    case CL_FLUSH_ALWAYS:
      graphd_value_boolean_set(val, true);
      break;
    default:
      ptr = cl_flush_policy_to_string(cl_get_flush_policy(cl), buf, sizeof buf);
      if (ptr == NULL) {
        graphd_value_null_set(val);
        return 0;
      }
      return graphd_value_text_strdup(greq->greq_req.req_cm, val,
                                      GRAPHD_VALUE_STRING, ptr,
                                      ptr + strlen(ptr));
  }
  return 0;
}

/* ----------------------------------------------------------------------
   LOGLEVEL -- enum or list of enums; graphd-private loglevel       libcl
   ---------------------------------------------------------------------- */

static int prop_loglevel_set(graphd_property const* prop, graphd_request* greq,
                             graphd_set_subject const* su) {
  graphd_handle* g = graphd_request_graphd(greq);
  cl_loglevel_configuration clc;

  if (cl_loglevel_configuration_from_string(su->set_value_s, su->set_value_e,
                                            graphd_facilities, &clc)) {
    graphd_request_errprintf(
        greq, 0, "SYNTAX can't parse \"%.*s\" as a loglevel",
        (int)(su->set_value_e - su->set_value_s), su->set_value_s);
    return GRAPHD_ERR_SYNTAX;
  }
  srv_log_set_level(g->g_srv, &clc);

  return 0;
}

static int prop_loglevel_status(graphd_property const* prop,
                                graphd_request* greq, graphd_value* val) {
  return prop_this_loglevel_status(prop, greq, val, graphd_request_cl(greq));
}

/* ----------------------------------------------------------------------
   LOGFILE -- pattern for log file names 			    libcl
   ---------------------------------------------------------------------- */

static int prop_logfile_set(graphd_property const* prop, graphd_request* greq,
                            graphd_set_subject const* su) {
  int err;
  cm_handle* cm = greq->greq_req.req_cm;
  char* f;
  // fprintf(stderr, "prop_logfile_set: setting filename to %s\n",
  // su->set_value.value_logfile ? su->set_value.value_logfile: "null");

  f = cm_substr(cm, su->set_value_s, su->set_value_e);
  if (!f) return ENOMEM;

  err = srv_log_set_filename(greq->greq_req.req_session->ses_srv, f);

  cm_free(cm, f);

  if (err != 0)
    graphd_request_errprintf(
        greq, 0, "%s error setting log filename to \"%s\": %s",
        err == GRAPHD_ERR_SEMANTICS ? "SEMANTICS" : "SYSTEM",
        f ? f : "*stderr*", graphd_strerror(err));
  return 0;
}
static int prop_logfile_status(graphd_property const* prop,
                               graphd_request* greq, graphd_value* val) {
  char const* value = NULL;
  srv_handle* srv;
  cl_handle* cl;

  srv = greq->greq_req.req_session->ses_srv;
  cl = srv_log(srv);

  if (cl == NULL || (value = cl_file_get_name(cl)) == NULL) {
    graphd_value_null_set(val);
    return 0;
  }
  return graphd_value_text_strdup(cm_c(), val, GRAPHD_VALUE_STRING, value,
                                  value + strlen(value));
}

/* ----------------------------------------------------------------------
   NETLOGFILE -- pattern for netlog filenames 			    libcl
   ---------------------------------------------------------------------- */

static int prop_netlogfile_set(graphd_property const* prop,
                               graphd_request* greq,
                               graphd_set_subject const* su) {
  int err;
  cm_handle* cm = greq->greq_req.req_cm;

  char* f;
  f = cm_substr(cm, su->set_value_s, su->set_value_e);
  err = srv_netlog_set_filename(greq->greq_req.req_session->ses_srv, f);
  if (err != 0)
    graphd_request_errprintf(
        greq, 0, "%s error setting netlog filename to \"%s\": %s",
        err == GRAPHD_ERR_SEMANTICS ? "SEMANTICS" : "SYSTEM", f,
        graphd_strerror(err));
  return 0;
}

static int prop_netlogfile_status(graphd_property const* prop,
                                  graphd_request* greq, graphd_value* val) {
  char const* value = NULL;
  srv_handle* srv;
  cl_handle* netlog;

  srv = greq->greq_req.req_session->ses_srv;
  netlog = srv_netlog(srv);

  if (netlog == NULL || (value = cl_netlog_get_filename(netlog)) == NULL) {
    graphd_value_null_set(val);
    return 0;
  }
  return graphd_value_text_strdup(cm_c(), val, GRAPHD_VALUE_STRING, value,
                                  value + strlen(value));
}

/* ----------------------------------------------------------------------
   NETLOGLEVEL -- loglevel for netlog files 			    libcl
   ---------------------------------------------------------------------- */

static int prop_netloglevel_set(graphd_property const* prop,
                                graphd_request* greq,
                                graphd_set_subject const* su) {
  cl_loglevel_configuration clc;

  if (cl_loglevel_configuration_from_string(su->set_value_s, su->set_value_e,
                                            graphd_facilities, &clc) != 0) {
    graphd_request_errprintf(
        greq, 0, "SYNTAX cannot parse netloglevel \"%.*s\"",
        (int)(su->set_value_e - su->set_value_s), su->set_value_s);
    return GRAPHD_ERR_SYNTAX;
  }
  srv_netlog_set_level(greq->greq_req.req_session->ses_srv, &clc);
  return 0;
}

static int prop_netloglevel_status(graphd_property const* prop,
                                   graphd_request* greq, graphd_value* val) {
  cl_handle* cl = srv_netlog(greq->greq_req.req_session->ses_srv);
  return prop_this_loglevel_status(prop, greq, val, cl);
}

/* ----------------------------------------------------------------------
   NETLOGFLUSH -- flush policy for netlog files 		    libcl
   ---------------------------------------------------------------------- */

static int prop_netlogflush_set(graphd_property const* prop,
                                graphd_request* greq,
                                graphd_set_subject const* su) {
  cl_handle* cl = srv_netlog(greq->greq_req.req_session->ses_srv);

  bool policy;

  if (IS_LIT(su->set_value_s, su->set_value_e, "true"))
    policy = true;
  else if (IS_LIT(su->set_value_s, su->set_value_e, "false"))
    policy = false;
  else {
    graphd_request_errprintf(greq, 0,
                             "SYNTAX \"netlogflush\" can be set "
                             "to \"true\" or \"false\", got \"%.*s\"",
                             (int)(su->set_value_e - su->set_value_s),
                             su->set_value_s);
    return GRAPHD_ERR_SYNTAX;
  }
  if (cl != NULL)
    cl_set_flush_policy(cl, policy ? CL_FLUSH_ALWAYS : CL_FLUSH_NEVER);
  return 0;
}

static int prop_netlogflush_status(graphd_property const* prop,
                                   graphd_request* greq, graphd_value* val) {
  cl_handle* cl;
  char const* ptr;
  char buf[200];

  cl = srv_netlog(greq->greq_req.req_session->ses_srv);
  if (cl == NULL) {
    graphd_value_null_set(val);
    return 0;
  }
  switch (cl_get_flush_policy(cl)) {
    case CL_FLUSH_NEVER:
      graphd_value_boolean_set(val, false);
      break;
    case CL_FLUSH_ALWAYS:
      graphd_value_boolean_set(val, true);
      break;
    default:
      ptr = cl_flush_policy_to_string(cl_get_flush_policy(cl), buf, sizeof buf);
      if (ptr == NULL) {
        graphd_value_null_set(val);
        return 0;
      }
      return graphd_value_text_strdup(greq->greq_req.req_cm, val,
                                      GRAPHD_VALUE_STRING, ptr,
                                      ptr + strlen(ptr));
  }
  return 0;
}

static int prop_refresh_set(graphd_property const* prop, graphd_request* greq,
                            graphd_set_subject const* su) {
  int err;

  err = pdb_refresh(graphd_request_graphd(greq)->g_pdb);
  return err;
}

/* ----------------------------------------------------------------------
   PID -- what pid am I connected to?
   ---------------------------------------------------------------------- */

static int prop_pid_status(graphd_property const* prop, graphd_request* greq,
                           graphd_value* val) {
  graphd_value_number_set(val, (unsigned long long)getpid());
  return 0;
}

/* ----------------------------------------------------------------------
   READ-SUSPENDS-PER-MINUTE -- how many read-suspends per minute?  graphd
   ---------------------------------------------------------------------- */

static int prop_read_suspends_per_minute_status(graphd_property const* prop,
                                                graphd_request* greq,
                                                graphd_value* val) {
  graphd_value_number_set(
      val, graphd_suspend_a_read(
               graphd_request_graphd(greq),
               srv_msclock(graphd_request_graphd(greq)->g_srv), false));
  return 0;
}

/* ----------------------------------------------------------------------
   SYNC -- really sync to disk?			    		   libpdb
   ---------------------------------------------------------------------- */

static int prop_sync_set(graphd_property const* prop, graphd_request* greq,
                         graphd_set_subject const* su) {
  bool policy;
  if (IS_LIT(su->set_value_s, su->set_value_e, "true"))
    policy = true;
  else if (IS_LIT(su->set_value_s, su->set_value_e, "false"))
    policy = false;
  else {
    graphd_request_errprintf(
        greq, 0,
        "SYNTAX \"sync\" can be set to \"true\" or \"false\", "
        "got \"%.*s\"",
        (int)(su->set_value_e - su->set_value_s), su->set_value_s);
    return GRAPHD_ERR_SYNTAX;
  }
  pdb_set_sync(graphd_request_graphd(greq)->g_pdb, policy);
  return 0;
}

static int prop_sync_status(graphd_property const* prop, graphd_request* greq,
                            graphd_value* val) {
  graphd_value_boolean_set(val, pdb_sync(graphd_request_graphd(greq)->g_pdb));
  return 0;
}

/* ----------------------------------------------------------------------
   TRANSACTIONAL -- really sync to disk?	    		   libpdb
   ---------------------------------------------------------------------- */

static int prop_transactional_status(graphd_property const* prop,
                                     graphd_request* greq, graphd_value* val) {
  graphd_value_boolean_set(
      val, pdb_transactional(graphd_request_graphd(greq)->g_pdb));
  return 0;
}

/* ----------------------------------------------------------------------
   VERSION -- software version			    		   graphd
   ---------------------------------------------------------------------- */

static int prop_version_status(graphd_property const* prop,
                               graphd_request* greq, graphd_value* val) {
  graphd_value_text_set(val, GRAPHD_VALUE_STRING, graphd_build_version,
                        graphd_build_version + strlen(graphd_build_version),
                        NULL);
  return 0;
}

/* ----------------------------------------------------------------------
   REPLICA -- replicate from where?			    	   graphd
   ---------------------------------------------------------------------- */

static int prop_replica_set(graphd_property const* prop, graphd_request* greq,
                            graphd_set_subject const* su) {
  graphd_handle* const g = graphd_request_graphd(greq);
  srv_address* sa;
  cl_handle* cl = graphd_request_cl(greq);
  int err;

  err =
      srv_address_create_url(cm_c(), cl, su->set_value_s, su->set_value_e, &sa);
  if (err) {
    graphd_request_errprintf(
        greq, 0, "SEMANTICS cannot parse replica address \"%.*s\"",
        (int)(su->set_value_e - su->set_value_s), su->set_value_s);
    return 0;
  }

  if (g->g_rep_master_address) srv_address_destroy(g->g_rep_master_address);
  g->g_rep_master_address = sa;

  if (GRAPHD_ACCESS_REPLICA == g->g_access ||
      GRAPHD_ACCESS_ARCHIVE == g->g_access) {
    char err_buf[1024];
    bool err_retriable;

    err = graphd_access_set_global(g, g->g_access, &err_retriable, err_buf,
                                   sizeof err_buf);
    if (err != 0)
      graphd_request_errprintf(
          greq, 0,
          "%s failed to reinitialize access mode "
          "to %s after setting replica master "
          "to \"%.*s\": %s",
          err == GRAPHD_ERR_NOT_A_REPLICA ? "NOTREPLICA" : "SYSTEM",
          graphd_access_global_to_string(g->g_access),
          (int)(su->set_value_e - su->set_value_s), su->set_value_s, err_buf);
  }
  return 0;
}

static int prop_replica_status(graphd_property const* prop,
                               graphd_request* greq, graphd_value* val) {
  graphd_handle* const g = graphd_request_graphd(greq);
  char const* const url =
      g->g_rep_master_address == NULL ? "" : g->g_rep_master_address->addr_url;

  graphd_value_text_set(val, GRAPHD_VALUE_STRING, url, url + strlen(url), NULL);

  return 0;
}

/* ----------------------------------------------------------------------
   HOSTNAME -- hostname where I'm running graphd                   libsrv
   ---------------------------------------------------------------------- */

static int prop_hostname_status(graphd_property const* prop,
                                graphd_request* greq, graphd_value* val) {
  char* hostname = NULL;
  cm_handle* cm = greq->greq_req.req_cm;

  hostname = srv_address_fully_qualified_domainname(cm);
  if (hostname == NULL) return ENOMEM;

  graphd_value_text_set_cm(val, GRAPHD_VALUE_STRING, hostname, strlen(hostname),
                           cm);
  return 0;
}

/* ----------------------------------------------------------------------
 *  End of individual properties.
 */

static graphd_property const graphd_properties[] = {
    {"access", prop_access_set, prop_access_status},
    {"core", prop_core_set, prop_core_status},
    {"cost", prop_cost_set, prop_cost_status},
    {"hostname", NULL, prop_hostname_status},
    {"instanceid", prop_instanceid_set, prop_instanceid_status},
    {"logflush", prop_logflush_set, prop_logflush_status},
    {"loglevel", prop_loglevel_set, prop_loglevel_status},
    {"logfile", prop_logfile_set, prop_logfile_status},
    {"netlogfile", prop_netlogfile_set, prop_netlogfile_status},
    {"netloglevel", prop_netloglevel_set, prop_netloglevel_status},
    {"netlogflush", prop_netlogflush_set, prop_netlogflush_status},
    {"pid", NULL, prop_pid_status},
    {"readsuspendsperminute", NULL, prop_read_suspends_per_minute_status},
    {"refresh", prop_refresh_set, NULL},
    {"replica", prop_replica_set, prop_replica_status},
    {"sync", prop_sync_set, prop_sync_status},
    {"transactional", NULL, prop_transactional_status},
    {"version", NULL, prop_version_status},
    {NULL}};

/**
 * Not counting dashes and underscores, is s...e pretty much name ?
 * The name is all-lowercase and doesn't contain dashes or underscores.
 */
static bool prop_is_name(char const* name, char const* s, char const* e) {
  while (s < e && *name != '\0')

    if (*s == '-' || *s == '_')
      s++;
    else if ((isascii(*s) ? tolower(*s) : *s) != *name)
      return false;
    else
      s++, name++;

  while (s < e && (*s == '-' || *s == '_')) s++;

  return s >= e && *name == '\0';
}

graphd_property const* graphd_property_by_name(char const* s, char const* e) {
  graphd_property const* p = graphd_properties;

  for (p = graphd_properties; p->prop_name != NULL; p++)
    if (prop_is_name(p->prop_name, s, e)) return p;
  return NULL;
}
