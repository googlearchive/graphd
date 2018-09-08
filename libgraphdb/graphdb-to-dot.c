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
#include "libgraphdb/graphdb.h"

#include <errno.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sysexits.h>

#include "libcl/cl.h"
#include "libcm/cm.h"
#include "libgraph/graph.h"

/**
 * @file graphdb-to-dot.c
 * @brief Convert graphdb database contents to a .dot file, for use with
 *	the "graphviz" set of tools.
 */

/**
 * @brief Print a brief usage message and exit.
 * @param progname the basename of the program, for use in error messages.
 */
static void usage(char const *progname) {
  fprintf(
      stderr,
      "usage: %s options....\n"
      "Options:\n"
      "   -h                          print this brief message\n"
      "   -v                          increase verbosity of debug output\n"
      "   -t timeout-in-milliseconds	wait this long to connect or query\n"
      "   -q query                    include only nodes that match <query>\n"
      "   -s server-url		connect to <server-url>\n",
      progname);
  exit(EX_USAGE);
}

/**
 * @brief Print a label for use in "dot".
 *
 * Backqoutes, spaces, and double quotes must be escaped.
 *
 * @param s the beginning of the label as stored in the database
 * @param n length of the label as stored in the database
 * @param buf buffer for formatting the result.
 * @param size number of bytes pointed to by buf
 * @return a pointer to the formatted label.
 */
static char const *label(char const *s, size_t n, char *buf, size_t bufsize) {
  char *w = buf;
  char const *e;

  e = s + n;

  while (n > 0 && bufsize > 5) {
    n--;
    bufsize--;

    if (*s == ' ' || *s == '\\' || *s == '"') {
      *w++ = '\\';
      bufsize--;
    }
    *w++ = *s++;
  }
  if (n > 0) {
    *w++ = '.';
    *w++ = '.';
    *w++ = '.';
  }
  *w = '\0';

  return buf;
}

/**
 * @brief Print the last fragment of a hierarchical label.
 *
 * If the label is a sequence of dot-separated words, only the last
 * word is printed.
 *
 * @param s the beginning of the label as stored in the database
 * @param n length of the label as stored in the database
 * @param buf buffer for formatting the result.
 * @param size number of bytes pointed to by buf
 * @return a pointer to the formatted label.
 */
static char const *hierarchical_label(char const *s, size_t n, char *buf,
                                      size_t size) {
  char const *p;

  for (p = s + n; p > s; p--)
    if (p[-1] == '.') break;
  n -= p - s;
  s = p;

  return label(p, n - (p - s), buf, size);
}

static int print_label(char const *type, size_t type_n, char const *name,
                       size_t name_n, char const *value, size_t value_n) {
  char tbuf[1024], nbuf[1024], vbuf[1024];

  putchar('"');

  if (type != NULL && type_n != 0) {
    if (name != NULL && name_n != 0) {
      if (value != NULL && value_n != 0) {
        /*    type
         * ------------
         * name | value
         */
        printf("{%s|{%s|%s}}",
               hierarchical_label(type, type_n, tbuf, sizeof tbuf),
               hierarchical_label(name, name_n, nbuf, sizeof nbuf),
               label(value, value_n, vbuf, sizeof vbuf));
      } else {
        /* type
         * -------
         * name
         */
        printf("{%s|%s}", hierarchical_label(type, type_n, tbuf, sizeof tbuf),
               hierarchical_label(name, name_n, nbuf, sizeof nbuf));
      }
    } else {
      if (value != NULL && value_n != 0) {
        /*    type
         * ------------
         *    value
         */
        printf("{%s|%s}", hierarchical_label(type, type_n, tbuf, sizeof tbuf),
               label(value, value_n, vbuf, sizeof vbuf));
      } else {
        /* type
         */
        printf("%s", hierarchical_label(type, type_n, tbuf, sizeof tbuf));
      }
    }
  } else {
    if (name != NULL && name_n != 0) {
      if (value != NULL && value_n != 0) {
        /* name | value
         */
        printf("%s|%s", hierarchical_label(name, name_n, nbuf, sizeof nbuf),
               label(value, value_n, vbuf, sizeof vbuf));
      } else {
        /* name
         */
        printf("%s", hierarchical_label(name, name_n, nbuf, sizeof nbuf));
      }
    } else {
      if (value != NULL && value_n != 0) {
        /* value
         */
        printf("%s", label(value, value_n, vbuf, sizeof vbuf));
      } else {
        /* null
         */
      }
    }
  }
  putchar('"');
  return 0;
}

static int print_node(graph_guid const *guid, char const *type, size_t type_n,
                      char const *name, size_t name_n, char const *value,
                      size_t value_n) {
  char buf[200];

  printf("\"%s\" [shape=record, label=",
         graph_guid_to_string(guid, buf, sizeof buf));
  print_label(type, type_n, name, name_n, value, value_n);
  puts("];");

  return 0;
}

static int print_link(graph_guid const *guid, graph_guid const *left,
                      graph_guid const *right, char const *type, size_t type_n,
                      char const *name, size_t name_n, char const *value,
                      size_t value_n) {
  char lbuf[1024], rbuf[1024];

  /*  If the link has no endpoint, make one.
   *  (Conveniently, name it like the link.)
   */

  if (GRAPH_GUID_IS_NULL(*left))
    printf("%s [label=\"\"];\n", graph_guid_to_string(guid, lbuf, sizeof lbuf));

  printf("\"%s\" -> \"%s\" [shape=record,label=",

         GRAPH_GUID_IS_NULL(*left)
             ? graph_guid_to_string(guid, lbuf, sizeof lbuf)
             : graph_guid_to_string(left, lbuf, sizeof lbuf),

         graph_guid_to_string(right, rbuf, sizeof rbuf));

  print_label(type, type_n, name, name_n, value, value_n);
  puts("];");

  return 0;
}

/**
 * @brief Execution entry point
 * @param argc number of command line parameters
 * @param argv command line parameters
 * @return 0 on success, otherwise an error number
 * @return EX_USAGE\ (64) on usage error
 * @return EX_UNAVAILABLE\ (69) if the server didn't answer
 * @return EX_DATAERR\ (65)  if the query didn't match anything
 *
 */
int main(int argc, char **argv) {
  int opt, meta, err;
  cl_handle *cl;
  cm_handle *cm;
  long timeout_ms = -1;
  char const *progname;
  char const *query = NULL;
  char **s_arg = NULL;
  char sbuf[GRAPHDB_SERVER_NAME_SIZE];
  char ebuf[GRAPHDB_ERROR_SIZE];
  int status = 0;

  graphdb_handle *graphdb;
  graphdb_iterator *it;
  graph_guid guid, left, right;
  char const *type_s, *name_s, *value_s;
  size_t type_n, name_n, value_n;
  int verbose = 0;

  cl = cl_create();
  cm = cm_trace(cm_c());

  if ((progname = strrchr(argv[0], '/')) != NULL)
    progname++;
  else
    progname = argv[0];

  while ((opt = getopt(argc, argv, "hq:s:t:v")) != EOF) {
    switch (opt) {
      case 'q':
        query = optarg;
        break;

      case 's':
        s_arg = cm_argvadd(cm, s_arg, optarg);
        if (s_arg == NULL) {
          fprintf(stderr,
                  "%s: out of memory while "
                  "parsing command line arguments: %s\n",
                  progname, strerror(errno));
          exit(1);
        }
        break;

      case 't':
        if (sscanf(optarg, "%ld", &timeout_ms) != 1) {
          fprintf(stderr,
                  "%s: expected timeout "
                  "(in milliseconds), got \"%s\"\n",
                  progname, optarg);
          exit(64);
        }
        break;

      case 'v':
        verbose++;
        break;

      case 'h':
      case '?':
        usage(progname);
        break;

      default:
        break;
    }
  }

  /*  Create and parametrize a graphdb handle.
   */
  graphdb = graphdb_create();
  graphdb_set_memory(graphdb, cm);
  graphdb_set_logging(graphdb, cl);

  if (verbose) {
    cl_set_loglevel_full(cl, GRAPHDB_LEVEL_DEBUG);
    graphdb_set_loglevel(graphdb, GRAPHDB_LEVEL_DEBUG);
  }

  /* Connect to a server.
   */
  if ((err = graphdb_connect(graphdb, timeout_ms, (char const *const *)s_arg,
                             0)) != 0) {
    fprintf(stderr, "%s: failed to connect to %s: %s\n", progname,
            graphdb_server_name(graphdb, sbuf, sizeof sbuf),
            graphdb_strerror(err, ebuf, sizeof ebuf));

    status = EX_UNAVAILABLE;
  } else {
    if ((err = graphdb_query(graphdb, &it, timeout_ms,
                             "read (any %s result=((meta, guid, type, "
                             "name, value, right, left)))",
                             query ? query : "")) != 0) {
      fprintf(stderr, "%s: failed to query %s: %s\n", progname,
              graphdb_server_name(graphdb, sbuf, sizeof sbuf),
              graphdb_strerror(err, ebuf, sizeof ebuf));
      status = EX_UNAVAILABLE;
    } else {
      graphdb_iterator *elem_it;

      if ((err = graphdb_query_next(graphdb, it, "ok (%...)", &elem_it)) != 0) {
        printf("error in query response: %s\n",
               graphdb_query_error(graphdb, it, err));
        status = EX_DATAERR;
      } else {
        char buf[200];
        time_t tloc;

        time(&tloc);
        strftime(buf, sizeof buf, "%Y-%m-%dT%H:%M:%S", localtime(&tloc));

        printf("digraph \"%s-%s\" {\n",
               graphdb_server_name(graphdb, sbuf, sizeof sbuf), buf);

        while (!(err = graphdb_query_next(
                     graphdb, elem_it, "(%m %g %o %o %o %g %g)", &meta, &guid,
                     &type_s, &type_n, &name_s, &name_n, &value_s, &value_n,
                     &right, &left))) {
          switch (meta) {
            case GRAPHDB_META_NODE:
              print_node(&guid, type_s, type_n, name_s, name_n, value_s,
                         value_n);
              break;
            case GRAPHDB_META_LINK_TO:
            case GRAPHDB_META_LINK_FROM:
              print_link(&guid, &left, &right, type_s, type_n, name_s, name_n,
                         value_s, value_n);
              break;
            default:
              cl_notreached(cl,
                            "unexpected meta "
                            "result %d",
                            meta);
          }
        }

        printf("}\n");

        if (err != ENOENT) {
          printf(
              "error in graphdb_query_next: "
              "%s\n",
              graphdb_query_error(graphdb, elem_it, err));
        }
      }
    }
  }

  graphdb_destroy(graphdb);

  return status;
}
