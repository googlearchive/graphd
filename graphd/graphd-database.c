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
#include "graphd/graphd-snapshot.h"
#include "graphd/graphd-version.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sysexits.h>
#include <sys/stat.h>

#include "libsrv/srv.h"

#define IS_LIT(lit, s, e) \
  (((e) - (s) == sizeof(lit) - 1) && strncasecmp(lit, s, sizeof(lit) - 1) == 0)

static graphd_database_config const* default_config(cl_handle* cl) {
  static graphd_database_config d;
  static int initialized = 0;
  int maxf;

  if (initialized) return &d;

  d.dcf_path = "/db/graphd";
  d.dcf_type = "addb";
  d.dcf_id = "devel";
  d.dcf_pdb_cf.pcf_sync = 1;
  d.dcf_pdb_cf.pcf_transactional = 1;
  d.dcf_pdb_cf.pcf_create_database = 1;
  d.dcf_pdb_cf.pcf_gcf.gcf_split_thr = 15;
  if ((maxf = pdb_get_max_files()) > 0)
    d.dcf_pdb_cf.pcf_gcf.gcf_max_lf = maxf / 6;
  else
    d.dcf_pdb_cf.pcf_gcf.gcf_max_lf = 256;

  d.dcf_pdb_cf.pcf_gcf.gcf_allow_bgmaps = true;

  if (sizeof(void*) < 8) {
    /* A 32-bit system: try and fit indexes into the
     * initial map
     */
    d.dcf_pdb_cf.pcf_icf.icf_init_map = 81 * 1024 * 1024;
    d.dcf_pdb_cf.pcf_gcf.gcf_init_map = 8 * 1024 * 1024;
    d.dcf_pdb_cf.pcf_hcf.hcf_init_map = 8 * 1024 * 1024;
    d.dcf_pdb_cf.pcf_hcf.hcf_gm_init_map = 8 * 1024 * 1024;
    d.dcf_pdb_cf.pcf_gcf.gcf_lf_init_map = 1;
  } else {
    /* A 64-bit system: defaults are larger than anything we've
     * observed
     */
    d.dcf_pdb_cf.pcf_icf.icf_init_map = 800 * 1024 * 1024;
    d.dcf_pdb_cf.pcf_gcf.gcf_init_map = 900 * 1024 * 1024;
    d.dcf_pdb_cf.pcf_hcf.hcf_init_map = 2ull * 1024ull * 1024ull * 1024ull;
    d.dcf_pdb_cf.pcf_hcf.hcf_gm_init_map = 2ull * 1024ull * 1024ull * 1024ull;
    d.dcf_pdb_cf.pcf_gcf.gcf_lf_init_map = 25 * 1024 * 1024;
    /* Largefile size is bounded by the size of a bitmap:
     * 200M / 8 = 25Mb
     */
  }

  initialized = 1;

  return &d;
}

/*  We're done configuring the database.   The files and
 *  command line arguments have been evaluated.
 */
static int graphd_database_configure_done(graphd_handle* g, srv_handle* srv,
                                          graphd_database_config* dcf) {
  cl_handle* cl = srv_log(srv);
  cm_handle* cm = srv_mem(srv);
  struct stat st;
  int err;

  cl_cover(cl);

  cl_enter(cl, CL_LEVEL_VERBOSE, " ");

  g->g_cl = cl;
  g->g_cm = cm;

  cl_assert(cl, atoi(GRAPHD_FORMAT_VERSION) > 0);

  g->g_pdb = pdb_create(cm, cl, atoi(GRAPHD_FORMAT_VERSION));
  if (g->g_pdb == NULL) return ENOMEM;

  if (g->g_database_must_exist) dcf->dcf_pdb_cf.pcf_create_database = false;

  pdb_configure(g->g_pdb, &dcf->dcf_pdb_cf);

  if (dcf->dcf_path != NULL) {
    if ((err = pdb_set_path(g->g_pdb, dcf->dcf_path)) != 0) {
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_ERROR,
             "failed to set database path "
             "to \"%s\": %s",
             dcf->dcf_path, strerror(err));

      goto err;
    }
    cl_cover(cl);
  }
  if (dcf->dcf_id != NULL) {
    char* ep;
    unsigned long long ull;

    cl_cover(cl);
    ull = strtoull(dcf->dcf_id, &ep, 0);
    if (ep && *ep == '\0') {
      cl_cover(cl);
      err = pdb_set_database_id(g->g_pdb, ull);
    } else {
      if (strlen(dcf->dcf_id) > 7) {
        cl_cover(cl);
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "non-numerical database IDs are "
               "limited to 7 characters - can't "
               "use \"%s\"",
               dcf->dcf_id);
        err = GRAPHD_ERR_LEXICAL;
        goto err;
      }
      cl_cover(cl);
      err = pdb_set_database_name(g->g_pdb, dcf->dcf_id);
    }

    if (err) goto err;
  }
  if (dcf->dcf_type != NULL && strcasecmp(dcf->dcf_type, "addb") != 0) {
    cl_cover(cl);
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "sorry, currently only addb databases are "
           "supported.");
    err = GRAPHD_ERR_SEMANTICS;
    goto err;
  }
  if (g->g_predictable) {
    cl_cover(cl);
    pdb_set_predictable(g->g_pdb, true);
  }

  bool try_snapshot =  //<- Should we try booting from a snapshot
                       //   if `pdb_initialize()' fails?
      dcf->dcf_snap != NULL;

  /* Boot from a snapshot if the the database directory does not exist,
   * and a snapshot directory is available. If we cannot boot from a
   * snapshot, then continue as normal -- an empty database directory
   * will be created by `pdb_initialize()'.
   */
  if (dcf->dcf_path != NULL && dcf->dcf_snap != NULL &&
      stat(dcf->dcf_path, &st) == -1 && stat(dcf->dcf_snap, &st) == 0) {
    char const* const prog = srv_program_name(srv);

    cl_log(cl, CL_LEVEL_INFO,
           "%s: no database found, attemping "
           "to load snapshot",
           prog);

    if ((err = graphd_snapshot_restore(g, srv, dcf))) {
      cl_log(cl, CL_LEVEL_ERROR, "%s: failed to load snapshot: %s", prog,
             strerror(err));
      cl_log(cl, CL_LEVEL_INFO, "%s: continuing without a snapshot", prog);
    } else {
      cl_log(cl, CL_LEVEL_INFO, "%s: booting from snapshot", prog);
    }

    try_snapshot = false;
  }

again:
  if ((err = pdb_configure_done(g->g_pdb)) != 0) {
    if (err == EBUSY) {
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "%s: another server is already "
             "accessing the database \"%s\".",
             srv_program_name(srv), dcf->dcf_path);
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "%s: shut down that "
             "process before starting a new one.",
             srv_program_name(srv));
    } else if (err == PDB_ERR_SYNTAX) {
      /* We told it not to create the database dir, so we
       * cancel startup here
       */
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "%s: no database at \"%s\": "
             "create/extract a database dir. ",
             srv_program_name(srv), dcf->dcf_path);
#if __FreeBSD__
    } else if (err == EIO && try_snapshot) {
#else
    } else if (err == ENODATA && try_snapshot) {
#endif
      /* A stale lock file was found in the database
       * directory. The database is probably unsafe to use,
       * so let's try to boot from a snapshot.
       */
      if ((err = graphd_snapshot_restore(g, srv, dcf))) {
        cl_log(cl, CL_LEVEL_ERROR, "%s: failed to restore snapshot: %s",
               srv_program_name(srv), strerror(err));
      } else {
        cl_log(cl, CL_LEVEL_INFO, "%s: rebooting now!", srv_program_name(srv));
        try_snapshot = false;
        goto again;
      }
    } else {
      cl_log(
          cl, err == ENOENT ? CL_LEVEL_OPERATOR_ERROR : CL_LEVEL_ERROR,
          "%s: failed to initialize database \"%s\": %s", srv_program_name(srv),
          dcf->dcf_path,
          err == ERANGE ? "premature EOF while reading" : pdb_xstrerror(err));
    }
    goto err;
  }

  if (g->g_force) {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "WARNING! Graphd has been started with the -C"
           " (continue) option. "
           " Graphd will ignore normally fatal errors "
           "during verification and re-indexing. ");
  }

  cl_log(cl, CL_LEVEL_DETAIL, "%s: open database path=%s, type=%s, id=%s",
         srv_program_name(srv), dcf->dcf_path ? dcf->dcf_path : "(default)",
         dcf->dcf_type ? dcf->dcf_type : "(default)",
         dcf->dcf_id ? dcf->dcf_id : "(default)");
  cl_cover(cl);
  cl_leave(cl, CL_LEVEL_VERBOSE, " ");

  return 0;

err:
  cl_assert(cl, err);
  (void)pdb_destroy(g->g_pdb);
  g->g_pdb = NULL;
  cl_leave(cl, CL_LEVEL_VERBOSE, "fails: %s errno=%d", graphd_strerror(err),
           err);

  return err;
}

static int graphd_database_initialize(graphd_handle* g, srv_handle* srv,
                                      graphd_database_config const* dcf) {
  cl_handle* cl = g->g_cl;
  int err;

  cl_enter(cl, CL_LEVEL_VERBOSE, " ");

  /*  Initialize the graphd handle enough to do some
   *  basic allocations...
   */
  if (g->g_cm == NULL) g->g_cm = srv_mem(srv);

  if (g->g_graph == NULL) {
    g->g_graph = graph_create(g->g_cm, cl);
    if (g->g_graph == NULL) return ENOMEM;
  }
  g->g_srv = srv;

  bool try_snapshot = true;  //<- Should we try booting from a snapshot
                             //   if `pdb_initialize()' fails?

again:
  if ((err = pdb_initialize(g->g_pdb)) != 0) {
    if (err == EBUSY) {
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "%s: another server is already "
             "accessing the database \"%s\".",
             srv_program_name(srv), dcf->dcf_path);
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "%s: shut down that "
             "process before starting a new one.",
             srv_program_name(srv));
#if __FreeBSD__
    } else if (err == EIO && try_snapshot) {
#else
    } else if (err == ENODATA && try_snapshot) {
#endif
      /* A stale lock file was found in the database
       * directory. The database is probably unsafe to use,
       * so let's try to boot from a snapshot.
       */
      if ((err = graphd_snapshot_restore(g, srv, dcf))) {
        cl_log(cl, CL_LEVEL_ERROR, "%s: failed to restore snapshot: %s",
               srv_program_name(srv), strerror(err));
      } else {
        cl_log(cl, CL_LEVEL_INFO, "%s: rebooting now!", srv_program_name(srv));
        try_snapshot = false;
        goto again;
      }
    } else {
      cl_log(
          cl, err == ENOENT ? CL_LEVEL_OPERATOR_ERROR : CL_LEVEL_ERROR,
          "%s: failed to initialize database \"%s\": %s", srv_program_name(srv),
          dcf->dcf_path,
          err == ERANGE ? "premature EOF while reading" : pdb_xstrerror(err));
    }
    goto err;
  }

  /*
   * If we need to reindex parts of the database, do that now.
   */
  err = pdb_initialize_checkpoint(g->g_pdb);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_OPERATOR_ERROR, "pdb_initialize_checkpoint", err,
                 "unexpected error");
    srv_epitaph_print(srv, EX_GRAPHD_DATABASE,
                      "Failed to initialize checkpoint on imported "
                      "database \"%s\": %s",
                      dcf->dcf_path, graphd_strerror(err));
    goto err;
  }

  /*
   * Verify the last 10,000 or so primitives in the database.
   */
  if (g->g_verify) {
    unsigned long long n = pdb_primitive_n(g->g_pdb);
    pdb_iterator_chain ch[1];

    memset(ch, 0, sizeof(ch));
    pdb_iterator_chain_clear(g->g_pdb, ch);
    pdb_iterator_chain_set(g->g_pdb, ch);

    err = pdb_verify_range(g->g_pdb, (n > 10000) ? (n - 10000) : 0, n, NULL);

    pdb_iterator_chain_finish(g->g_pdb, ch, "graphd_database_configure_done");
    pdb_iterator_chain_clear(g->g_pdb, ch);

    if (err) {
      if (err == PDB_ERR_NO)
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR, "Database verification failed%s",
               g->g_force ? " (but -C was specified - ignoring this error)"
                          : ".  Run with -C to force startup.");
      else
        cl_log_errno(cl, CL_LEVEL_OPERATOR_ERROR, "pdb_verify_range", err,
                     g->g_force
                         ? " (but -C was specified - ignoring this error)"
                         : "-- run with -C to force startup.");
      if (!g->g_force) {
        cl_log(cl, CL_LEVEL_ERROR,
               "Failed fo verify the last 10,000"
               " primitives. Your database is"
               " corrupt.");
        srv_epitaph_print(srv, EX_GRAPHD_DATABASE,
                          "Failed fo verify the last 10,000"
                          " primitives. Your database is"
                          " corrupt.");
        goto err;
      }
    }
  }

  /*  Read the primitives we need to bootstrap our type system.
   */
  err = graphd_type_bootstrap_read(g);
  if (err && err != GRAPHD_ERR_NO) {
    if (g->g_force) {
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "graphd_type_bootstrap_read: %s. "
             " (but -C was specified - ignoring this error)",
             graphd_strerror(err));
    } else {
      srv_epitaph_print(srv, EX_GRAPHD_DATABASE,
                        "Unexpected error: %s while reading the type"
                        " bootstrap. Corrupt database?",
                        graphd_strerror(err));
      goto err;
    }
  }

  /* Install the primitive write monitors. */
  err = graphd_session_dateline_monitor(g);
  if (err != 0) goto err;

  cl_log(cl, CL_LEVEL_DETAIL, "%s: open database path=%s, type=%s, id=%s",
         srv_program_name(srv), dcf->dcf_path ? dcf->dcf_path : "(default)",
         dcf->dcf_type ? dcf->dcf_type : "(default)",
         dcf->dcf_id ? dcf->dcf_id : "(default)");
  cl_cover(cl);
  cl_leave(cl, CL_LEVEL_SPEW, " ");

  return 0;

err:
  cl_assert(cl, err);
  (void)pdb_destroy(g->g_pdb);
  g->g_pdb = NULL;
  cl_leave(cl, CL_LEVEL_SPEW, "fails: errno=%d", err);

  return err;
}

static graphd_database_config* graphd_database_config_alloc(
    cm_handle* cm, cl_handle* cl, char const* progname, char const* path_s,
    char const* path_e, char const* snap_s, char const* snap_e,
    char const* type_s, char const* type_e, char const* id_s,
    char const* id_e) {
  graphd_database_config* dcf;
  char* heap;
  size_t need;

  size_t id_n = id_s ? (size_t)(1 + (id_e - id_s)) : 0;
  size_t path_n = path_s ? (size_t)(1 + (path_e - path_s)) : 0;
  size_t snap_n = snap_s ? (size_t)(1 + (snap_e - snap_s)) : 0;
  size_t type_n = type_s ? (size_t)(1 + (type_e - type_s)) : 0;

  cl_enter(cl, CL_LEVEL_SPEW, " ");

  need = sizeof(*dcf) + id_n + path_n + snap_n + type_n;

  if (!(dcf = cm_malloc(cm, need))) {
    cl_log(cl, CL_LEVEL_ERROR,
           "%s: failed to allocate %lu bytes for "
           "database \"%.*s\"'s configuration structure: %s",
           progname, (unsigned long)need, (int)(path_e - path_s), path_s,
           strerror(errno));
    cl_leave(cl, CL_LEVEL_SPEW, "malloc fails");
    return NULL;
  }
  dcf->dcf_pdb_cf = default_config(cl)->dcf_pdb_cf;
  heap = (char*)(dcf + 1);

  if (!path_n) {
    cl_cover(cl);
    dcf->dcf_path = NULL;
  } else {
    cl_cover(cl);
    dcf->dcf_path = memcpy(heap, path_s, path_n - 1);
    (heap += path_n)[-1] = '\0';
  }
  if (!snap_n) {
    cl_cover(cl);
    dcf->dcf_snap = NULL;
  } else {
    cl_cover(cl);
    dcf->dcf_snap = memcpy(heap, snap_s, snap_n - 1);
    (heap += snap_n)[-1] = '\0';
  }
  if (!type_n) {
    cl_cover(cl);
    dcf->dcf_type = NULL;
  } else {
    cl_cover(cl);
    dcf->dcf_type = memcpy(heap, type_s, type_n - 1);
    (heap += type_n)[-1] = '\0';
  }
  if (!id_n) {
    cl_cover(cl);
    dcf->dcf_id = NULL;
  } else {
    cl_cover(cl);
    dcf->dcf_id = memcpy(heap, id_s, id_n - 1);
    (heap += id_n)[-1] = '\0';
  }

  cl_leave(cl, CL_LEVEL_SPEW, " ");
  return dcf;
}

static int graphd_database_config_read_string(cl_handle* cl, srv_config* srv_cf,
                                              char** s, char const* e,
                                              char const* tok_s,
                                              char const* tok_e,
                                              char const** loc_s,
                                              char const** loc_e) {
  int tok;

  if (*loc_s != NULL) {
    cl_cover(cl);
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "configuration file \"%s\", line %d:  "
           "duplicate \"%.*s\" in database definition",
           srv_config_file_name(srv_cf), srv_config_line_number(srv_cf, e),
           (int)(tok_e - tok_s), tok_s);
    return GRAPHD_ERR_SYNTAX;
  }

  tok = srv_config_get_token(s, e, loc_s, loc_e);
  if (tok == EOF || tok == '}') {
    cl_cover(cl);
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "configuration file \"%s\", line %d:  "
           "unexpected %s in database definition",
           srv_config_file_name(srv_cf), srv_config_line_number(srv_cf, *s),
           tok == EOF ? "EOF" : "\"}\"");
    return GRAPHD_ERR_SYNTAX;
  }
  cl_cover(cl);
  return 0;
}

/**
 * @brief check for obsolete memory percentage literals.
 *
 * Check the token for a known obsolete keyword and fast-forward
 * past the obsolete percantage that should come after it.
 */
static bool graphd_database_obsolete_percentage(cl_handle* cl, char** s,
                                                const char* e,
                                                char const* tok_s,
                                                char const* tok_e) {
  if (srv_config_is_name("tilecache", tok_s, tok_e) ||
      srv_config_is_name("tilecacheprimitive", tok_s, tok_e) ||
      srv_config_is_name("tilecachehash", tok_s, tok_e) ||
      srv_config_is_name("tilecacheright", tok_s, tok_e) ||
      srv_config_is_name("tilecacheleft", tok_s, tok_e) ||
      srv_config_is_name("tilecachescope", tok_s, tok_e) ||
      srv_config_is_name("tilecachetype", tok_s, tok_e) ||
      srv_config_is_name("tilecachekey", tok_s, tok_e) ||
      srv_config_is_name("tilecachegeneration", tok_s, tok_e) ||
      srv_config_is_name("hmappercent", tok_s, tok_e)) {
    int tok;
    cl_log(cl, CL_LEVEL_FAIL, "Ignoring obsolete %.*s in database definition.",
           (int)(tok_e - tok_s), tok_s);
    tok = srv_config_get_token(s, e, &tok_s, &tok_e);
    if (tok == EOF || tok == /*{*/ '}') return GRAPHD_ERR_SYNTAX;

    return true;
  }
  return false;
}

/*
 * Emit a warning for an obsolete token. Fast forward past
 * the argument after it.
 */
static int graphd_database_obsolete_option(cl_handle* cl, const char* name,
                                           char** s, const char* e) {
  const char *tok_s, *tok_e;
  int tok;

  tok = srv_config_get_token(s, e, &tok_s, &tok_e);
  if (tok == EOF || tok == '}') return GRAPHD_ERR_SYNTAX;

  cl_log(cl, CL_LEVEL_FAIL,
         "Ignoring obsolete parameter %s in database definition.", name);

  return 0;
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
static int graphd_database_config_read_database(
    srv_handle* srv, srv_config* srv_cf, char** s, char const* e,
    char const** path_s, char const** path_e, char const** snap_s,
    char const** snap_e, char const** id_s, char const** id_e,
    char const** type_s, char const** type_e, pdb_configuration* pdb_cf) {
  cl_handle* cl = srv_log(srv);

  int tok;
  char const *tok_s, *tok_e;

  int err;

  cl_assert(cl, srv_cf != NULL);
  while ((tok = srv_config_get_token(s, e, &tok_s, &tok_e)) != '}') {
    err = 0;
    switch (tok) {
      case EOF:
        cl_cover(cl);
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "configuration file \"%s\", line %d: "
               "unexpected EOF in database definition",
               srv_config_file_name(srv_cf), srv_config_line_number(srv_cf, e));
        err = GRAPHD_ERR_SYNTAX;
        break;

      case 'a':
        if (IS_LIT("path", tok_s, tok_e))
          err = graphd_database_config_read_string(cl, srv_cf, s, e, tok_s,
                                                   tok_e, path_s, path_e);

        else if (IS_LIT("type", tok_s, tok_e))
          err = graphd_database_config_read_string(cl, srv_cf, s, e, tok_s,
                                                   tok_e, type_s, type_e);

        else if (IS_LIT("must-exist", tok_s, tok_e)) {
          err = srv_config_read_boolean(srv_cf, cl, s, e,
                                        &pdb_cf->pcf_create_database);

          /* Must exist means !create_database */
          pdb_cf->pcf_create_database = !pdb_cf->pcf_create_database;
        }

        else if (IS_LIT("id", tok_s, tok_e))
          err = graphd_database_config_read_string(cl, srv_cf, s, e, tok_s,
                                                   tok_e, id_s, id_e);

        else if (IS_LIT("sync", tok_s, tok_e))
          err = srv_config_read_boolean(srv_cf, cl, s, e, &pdb_cf->pcf_sync);

        else if (IS_LIT("istore-init-map-tiles", tok_s, tok_e))
          err = graphd_database_obsolete_option(cl, "istore-init-map-tiles", s,
                                                e);

        else if (IS_LIT("gmap-init-map-tiles", tok_s, tok_e))
          err =
              graphd_database_obsolete_option(cl, "gmap-init-map-tiles", s, e);

        else if (IS_LIT("hmap-init-map-tiles", tok_s, tok_e))
          err =
              graphd_database_obsolete_option(cl, "hmap-init-map-tiles", s, e);

        else if (IS_LIT("gmap-split-thr", tok_s, tok_e))
          err = srv_config_read_number(srv_cf, cl, "gmap-split-thr", s, e,
                                       &pdb_cf->pcf_gcf.gcf_split_thr);

        else if (IS_LIT("gmap-max-lf", tok_s, tok_e))
          err = srv_config_read_number(srv_cf, cl, "gmap-max-lf", s, e,
                                       &(pdb_cf->pcf_gcf.gcf_max_lf));

        else if (IS_LIT("snapshot", tok_s, tok_e))
          err = graphd_database_config_read_string(cl, srv_cf, s, e, tok_s,
                                                   tok_e, snap_s, snap_e);

        else if (IS_LIT("transactional", tok_s, tok_e))
          err = srv_config_read_boolean(srv_cf, cl, s, e,
                                        &pdb_cf->pcf_transactional);

        else if (graphd_database_obsolete_percentage(cl, s, e, tok_s, tok_e))
          err = 0;

        else if (IS_LIT("enable_bgmaps", tok_s, tok_e))
          err = srv_config_read_boolean(srv_cf, cl, s, e,
                                        &(pdb_cf->pcf_gcf.gcf_allow_bgmaps));

        else {
          cl_cover(cl);
          goto unknown;
        }
        break;

      default:
      unknown:
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "configuration file \"%s\", line %d: "
               "unexpected keyword \"%.*s\" in "
               "database definition",
               srv_config_file_name(srv_cf), srv_config_line_number(srv_cf, e),
               (int)(tok_e - tok_s), tok_s);
        return GRAPHD_ERR_SYNTAX;
    }
    if (err != 0) return err;
  }
  return 0;
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
int graphd_database_config_read(void* data, srv_handle* srv, void* config_data,
                                srv_config* srv_cf, char** s, char const* e) {
  cm_handle* cm = srv_config_mem(srv_cf);
  cl_handle* cl = srv_log(srv);
  graphd_config* gcf = config_data;
  graphd_handle* g = data;

  graphd_database_config* dcf;

  int tok;
  char const *tok_s, *tok_e;

  char const *path_s = NULL, *path_e = NULL;
  char const *snap_s = NULL, *snap_e = NULL;
  char const *id_s = NULL, *id_e = NULL;
  char const *type_s = NULL, *type_e = NULL;
  pdb_configuration pdb_cf = default_config(cl)->dcf_pdb_cf;
  int err;

  cl_enter(cl, CL_LEVEL_SPEW, "(%.*s)", (int)(s && *s ? e - *s : 4),
           s && *s ? *s : "null");

  cl_assert(cl, data != NULL);
  cl_assert(cl, config_data != NULL);
  cl_assert(cl, srv_cf != NULL);
  cl_assert(cl, gcf != NULL);

  if (gcf->gcf_database_cf) {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "configuration file \"%s\", line %d: "
           "only a single database may be configured",
           srv_config_file_name(srv_cf), srv_config_line_number(srv_cf, e));
    cl_leave(cl, CL_LEVEL_SPEW, "multiple database configurations");
    return GRAPHD_ERR_SEMANTICS;
  }

  if ((tok = srv_config_get_token(s, e, &tok_s, &tok_e)) == '{') {
    err = graphd_database_config_read_database(
        srv, srv_cf, s, e, &path_s, &path_e, &snap_s, &snap_e, &id_s, &id_e,
        &type_s, &type_e, &pdb_cf);
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_SPEW, "syntax error in {} section");
      return err;
    }
  } else {
    if (tok != 'a' && tok != '"') {
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "configuration file \"%s\", line %d: expected "
             "database path, got \"%.*s\"\n",
             srv_config_file_name(srv_cf), srv_config_line_number(srv_cf, e),
             (int)(tok_e - tok_s), tok_s);
      cl_leave(cl, CL_LEVEL_SPEW, "syntax error in database path");
      return GRAPHD_ERR_LEXICAL;
    }

    cl_cover(cl);

    path_s = tok_s;
    path_e = tok_e;
  }

  dcf = graphd_database_config_alloc(cm, cl, srv_program_name(srv), path_s,
                                     path_e, snap_s, snap_e, type_s, type_e,
                                     id_s, id_e);
  if (!dcf) {
    cl_leave(cl, CL_LEVEL_SPEW, "failed to allocate configuration");
    return ENOMEM;
  }

  dcf->dcf_pdb_cf = pdb_cf;
  gcf->gcf_database_cf = dcf;

  if (g->g_nosync) gcf->gcf_database_cf->dcf_pdb_cf.pcf_sync = false;

  if (g->g_notransactional)
    gcf->gcf_database_cf->dcf_pdb_cf.pcf_transactional = false;

  if (g->g_database_must_exist) {
    gcf->gcf_database_cf->dcf_pdb_cf.pcf_create_database = false;
  }

  if (gcf->gcf_database_cf->dcf_pdb_cf.pcf_transactional == false) {
    /* transactional false *implies* sync false
     *
     * This is not strictly required, but makes life simpler.
     * If we know the database is doomed on crash, then
     * why sync?
     */

    gcf->gcf_database_cf->dcf_pdb_cf.pcf_sync = false;
  }
  cl_leave(cl, CL_LEVEL_SPEW, " ");
  return 0;
}

/**
 * @brief Set an option as configured.  (Method.)
 *
 *  This is a method of the generic libsrv parameter mechanism,
 *  passed in via a srv_config_parameter[] structure declared in graphd.c.
 *
 *  This is the part where a configured database must be complete
 *  and consistent.
 *
 * @param data		opaque application data handle (i.e., graphd)
 * @param srv 		generic libsrv handle
 * @param config_data	opaque application config data (i.e., graphd_config)
 * @param srv_cf	generic libsrv parameters
 *
 * @return 0 on success, a nonzero errno on error.
 */
int graphd_database_config_open(void* data, srv_handle* srv, void* config_data,
                                srv_config* srv_cf) {
  cl_handle* cl = srv_log(srv);
  graphd_database_config* dcf;

  graphd_config* gcf = config_data;

  cl_assert(cl, data);
  cl_assert(cl, config_data);

  dcf = gcf->gcf_database_cf;
  if (!dcf) dcf = (graphd_database_config*)default_config(cl);

  cl_assert(cl, dcf);
  if (dcf->dcf_path == NULL) {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "%s: no database path in "
           "configuration file \"%s\" or "
           "command line",
           srv_program_name(srv), srv_config_file_name(srv_cf));

    return SRV_ERR_SYNTAX;
  }

  return graphd_database_configure_done(data, srv, dcf);
}

/**
 * @brief Begin executing with an option.
 *
 *  This is a method of the generic libsrv parameter mechanism,
 *  passed in via a srv_config_parameter[] structure declared in graphd.c.
 *
 * @param data		opaque application data handle (i.e., graphd)
 * @param srv 		generic libsrv handle
 * @param config_data	opaque application config data (i.e., graphd_config)
 * @param srv_cf	generic libsrv parameters
 *
 * @return 0 on success, a nonzero errno on error.
 */
int graphd_database_config_run(void* data, srv_handle* srv, void* config_data,
                               srv_config* srv_cf) {
  char const* const fn = __FUNCTION__;
  graphd_handle* const g = data;
  cl_handle* const cl = srv_log(srv);
  graphd_config const* gcf = config_data;
  graphd_database_config const* dcf = gcf->gcf_database_cf;
  int attempts = 0;
  int err;

  cl_assert(cl, data);
  cl_assert(cl, config_data);
  cl_assert(cl, srv_cf);

  /* Make sure we have enough processes */

  err = graphd_startup_check_max_procs(g);
  if (err) return err;

  /* If the database is not transactional, and we previously crashed in
   * the middle of an "unsafe" operation, then throw away the current
   * database and load a snapshot.
   */
  if (!pdb_transactional(g->g_pdb) && !srv_shared_is_safe(srv)) {
    cl_log(cl, CL_LEVEL_INFO,
           "graphd_database_config_run(): "
           "unsafe database; attempting to load a snapshot");

  retry:
    if ((err = graphd_snapshot_restore(g, srv, dcf))) {
      cl_log(cl, CL_LEVEL_ERROR, "%s(): failed to load snapshot: %s", fn,
             strerror(err));
      cl_log(cl, CL_LEVEL_ERROR, "%s(): cannot continue without a snapshot",
             fn);
      /* prevent child from being restarted */
      srv_shared_set_restart(srv, false);
      return err;
    }

    /* Update the start-time of the child (until now we have been
     * restoring a snashot, possibly for several minutes). This
     * value is used by srv_parent() to check whether the child is
     * crashing too often.
     */
    srv_shared_set_time(srv, time(NULL));
    srv_shared_set_safe(srv, true);

    attempts++;
  }

  /* Open databases.
   *
   * Notice that if pdb_reinitialize() fails, we try rebooting from a
   * snapshot. In theory we should be able to detect and discard unsafe
   * databases before reaching this point (see srv_shared_is_safe() check
   * above), but just in case...
   *
   * If graphd_database_initialize() does indeed fail,
   * chances are that we crashed in some "unsafe" operation that
   *  we are not aware of.
   */
  err = graphd_database_initialize(g, srv, dcf);
  if (err != 0 && attempts == 0 && dcf->dcf_snap != NULL) goto retry;

  return err;
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
 * @param opt_arg	option's parameter
 *
 * @return 0 on success, a nonzero errno on error.
 */
int graphd_database_option_set(void* data, srv_handle* srv, cm_handle* cm,
                               int opt, char const* opt_arg) {
  graphd_handle* g = data;

  if (g->g_dir_arg) {
    fprintf(stderr,
            "%s: more than one database specification "
            "- \"%s\" and \"%s\"\n",
            srv_program_name(srv), g->g_dir_arg, opt_arg);
    return SRV_ERR_SEMANTICS;
  }

  g->g_dir_arg = opt_arg;

  return 0;
}

/**
 * @brief Extend a database configuration with a command line option.
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
int graphd_database_option_configure(void* data, srv_handle* srv,
                                     void* config_data,
                                     srv_config* srv_config_data) {
  graphd_handle* const g = data;
  graphd_config* const gcf = config_data;

  if (g->g_dir_arg) {
    cm_handle* const cm = srv_config_mem(srv_config_data);
    cl_handle* const cl = srv_log(srv);

    if (!gcf->gcf_database_cf) {
      gcf->gcf_database_cf = graphd_database_config_alloc(
          cm, cl, srv_program_name(srv), g->g_dir_arg,
          g->g_dir_arg + strlen(g->g_dir_arg), NULL, NULL, NULL, NULL, NULL,
          NULL);
      if (!gcf->gcf_database_cf) return ENOMEM;
    } else {
      /* Override an existing path with the command line.
       */
      gcf->gcf_database_cf->dcf_path = g->g_dir_arg;
    }
  }
  if (g->g_nosync) gcf->gcf_database_cf->dcf_pdb_cf.pcf_sync = false;

  if (g->g_notransactional)
    gcf->gcf_database_cf->dcf_pdb_cf.pcf_transactional = false;

  if (g->g_total_memory > 0)
    gcf->gcf_database_cf->dcf_pdb_cf.pcf_total_memory = g->g_total_memory;

  return 0;
}

int graphd_nosync_option_set(void* data, srv_handle* srv, cm_handle* cm,
                             int opt, char const* opt_arg) {
  graphd_handle* g = data;

  g->g_nosync = true;
  return 0;
}

int graphd_notransactional_option_set(void* data, srv_handle* srv,
                                      cm_handle* cm, int opt,
                                      char const* opt_arg) {
  graphd_handle* g = data;

  g->g_notransactional = true;
  return 0;
}

int graphd_database_total_memory_set(void* data, srv_handle* srv, cm_handle* cm,
                                     int opt, char const* opt_arg) {
  graphd_handle* g = data;
  long long total_memory;

  if (sscanf(opt_arg, "%lld", &total_memory) != 1 || total_memory <= 0) {
    fprintf(stderr,
            "graphd: expected "
            "positive number with -K, got \"%s\"\n",
            opt_arg);
    exit(EX_USAGE);
  }
  g->g_total_memory = total_memory;
  return 0;
}
