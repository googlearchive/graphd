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
#include "libgraph/graph.h"

#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <stdio.h>
#include <string.h>
#include <sysexits.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "libcl/cl.h"
#include "libcm/cm.h"

/* Global variables implicit in commands. */

cl_handle *cl;
cm_handle *cm;
char const *executable = NULL;
int opt_time = 0;
graph_grmap grmap;
graph_handle *graph;

static char const *graph_xstrerror(int err) {
  char const *st = graph_strerror(err);
  if (st != NULL) return st;
  return strerror(err);
}

/*  Trap into gdb.
 */
static void stacktrace(int unused) {
  FILE *fp;

  char batch_file[200];
  char pid[200];
  pid_t child;
  int status;

  if (!isatty(0)) exit(1);

  /* Format $$ */
  snprintf(pid, sizeof pid, "%lu", (unsigned long)getpid());

  /* Create a batch script for GDB. */
  snprintf(batch_file, sizeof batch_file, "/tmp/%lu.gdb.bat",
           (unsigned long)getpid());
  (void)unlink(batch_file);
  if (!(fp = fopen(batch_file, "w"))) exit(1);
  fputs("bt\nquit\n", fp);
  fclose(fp);

  child = fork();
  if (child == (pid_t)-1) {
    perror("fork");
    exit(1);
  }
  if (child == 0) {
    /* child -- spawn the GDB. */
    execlp("gdb", "-batch", "-q", "-x", batch_file, executable, pid,
           (char *)NULL);
    perror("execlp");
    exit(1);
  }
  waitpid(child, &status, 0);
  unlink(batch_file);
  exit(1);
}

static void command_check(char const *progname, char const *const *argv,
                          char const *filename, int line) {
  graph_grmap_read_state read_state;
  FILE *fp;
  int err = 0;
  graph_grmap tmp;

  if (argv[1] != NULL && argv[2] != NULL) {
    fprintf(stderr, "%s:%d: usage: check [path]\n", filename, line);
    return;
  }

  if (argv[1] == NULL)
    fp = stdin;
  else {
    fp = fopen(argv[1], "r");
    if (fp == NULL) {
      fprintf(stderr,
              "%s, \"%s\":%d: cannot "
              "open \"%s\" for reading: %s\n",
              progname, filename, line, argv[1], strerror(errno));
      return;
    }
  }

  graph_grmap_initialize(grmap.grm_graph, &tmp);
  graph_grmap_read_initialize(&tmp, &read_state);

  for (;;) {
    char bigbuf[200];
    char const *s = bigbuf;
    size_t cc;

    cc = fread(bigbuf, 1, sizeof bigbuf, fp);
    if (cc == 0) {
      err = GRAPH_ERR_DONE;
      break;
    }

    err = graph_grmap_read_next(&tmp, &s, bigbuf + cc, &read_state);
    if (err != 0) break;
  }
  if (err != GRAPH_ERR_DONE) {
    fprintf(stderr,
            "%s, \"%s\":%d: error "
            "in graphd_grmap_read_next: %s\n",
            progname, filename, line, graph_xstrerror(err));
  } else {
    if (graph_grmap_equal(&grmap, &tmp))
      printf("ok - check \"%s\"\n", argv[1]);
    else
      printf("different - check \"%s\"\n", argv[1]);
  }
  if (fp != stdin) fclose(fp);
  graph_grmap_finish(&tmp);
  return;
}

static void command_read(char const *progname, char const *const *argv,
                         char const *filename, int line) {
  graph_grmap_read_state read_state;
  FILE *fp;
  int err = 0;

  if (argv[1] != NULL && argv[2] != NULL) {
    fprintf(stderr, "%s:%d: usage: read [path]\n", filename, line);
    return;
  }

  if (argv[1] == NULL)
    fp = stdin;
  else {
    fp = fopen(argv[1], "r");
    if (fp == NULL) {
      fprintf(stderr,
              "%s, \"%s\":%d: cannot "
              "open \"%s\" for reading: %s\n",
              progname, filename, line, argv[1], strerror(errno));
      return;
    }
  }
  graph_grmap_read_initialize(&grmap, &read_state);
  for (;;) {
    char bigbuf[200];
    char const *s = bigbuf;
    size_t cc;

    cc = fread(bigbuf, 1, sizeof bigbuf, fp);
    if (cc == 0) {
      err = GRAPH_ERR_DONE;
      break;
    }

    err = graph_grmap_read_next(&grmap, &s, bigbuf + cc, &read_state);
    if (err != 0) break;
  }
  if (err != GRAPH_ERR_DONE) {
    fprintf(stderr,
            "%s, \"%s\":%d: error "
            "in graphd_grmap_read_next: %s\n",
            progname, filename, line, graph_xstrerror(err));
  } else {
    printf("ok - read \"%s\"\n", argv[1]);
  }
  if (fp != stdin) fclose(fp);
  return;
}

static void command_write(char const *progname, char const *const *argv,
                          char const *filename, int line) {
  graph_grmap_write_state write_state;
  FILE *fp;
  int err;

  if (argv[1] != NULL && argv[2] != NULL) {
    fprintf(stderr, "%s:%d: usage: write [path]\n", filename, line);
    return;
  }

  if (argv[1] == NULL)
    fp = stdout;
  else {
    if ((fp = fopen(argv[1], "w")) == NULL) {
      fprintf(stderr,
              "%s, \"%s\":%d: cannot "
              "open \"%s\" for writing: %s\n",
              progname, filename, line, argv[1], strerror(errno));
      return;
    }
  }
  graph_grmap_write_initialize(&grmap, &write_state);
  for (;;) {
    char bigbuf[1024];
    char *s = bigbuf;

    err = graph_grmap_write_next(&grmap, &s, bigbuf + sizeof bigbuf,
                                 &write_state);

    if (fwrite(bigbuf, s - bigbuf, 1, fp) != 1) {
      fprintf(stderr,
              "%s, \"%s\":%d: error "
              "writing to \"%s\": %s\n",
              progname, filename, line, argv[1] ? argv[1] : "*stdout*",
              strerror(errno));
      err = GRAPH_ERR_DONE;
    }

    if (err != 0) break;
  }
  if (err != GRAPH_ERR_DONE) {
    fprintf(stderr,
            "%s, \"%s\":%d: error "
            "in graphd_grmap_write_next: %s\n",
            progname, filename, line, graph_xstrerror(err));
  }
  if (fp != stdout) fclose(fp);
  return;
}

static void command_map(char const *progname, char const *const *argv,
                        char const *filename, int line) {
  graph_guid source, dest;
  char b1[GRAPH_GUID_SIZE], b2[GRAPH_GUID_SIZE];
  int err;

  if (argv[1] == NULL || argv[2] != NULL) {
    fprintf(stderr, "%s:%d: usage: map GUID\n", filename, line);
    return;
  } else if (graph_guid_from_string(&source, argv[1],
                                    argv[1] + strlen(argv[1]))) {
    fprintf(stderr, "%s:%d: expected source GUID, got \"%s\"\n", filename, line,
            argv[1]);
    return;
  }

  err = graph_grmap_map(&grmap, &source, &dest);
  if (err != 0)
    printf("ERROR: %s\n", graph_xstrerror(err));
  else
    printf("ok: %s->%s\n", graph_guid_to_string(&source, b1, sizeof b1),
           graph_guid_to_string(&dest, b2, sizeof b2));
}

static void command_add(char const *progname, char const *const *argv,
                        char const *filename, int line) {
  graph_guid source, dest;
  unsigned long long n;
  char b1[GRAPH_GUID_SIZE], b2[GRAPH_GUID_SIZE];
  int err;

  if (argv[1] == NULL || argv[2] == NULL || argv[3] == NULL ||
      argv[4] != NULL) {
    fprintf(stderr, "%s:%d: usage: add G1 G2 N\n", filename, line);
    return;
  }

  else if (graph_guid_from_string(&source, argv[1],
                                  argv[1] + strlen(argv[1]))) {
    fprintf(stderr, "%s:%d: expected source GUID, got \"%s\"\n", filename, line,
            argv[1]);
    return;
  } else if (graph_guid_from_string(&dest, argv[2],
                                    argv[2] + strlen(argv[2]))) {
    fprintf(stderr, "%s:%d: expected destination GUID, got \"%s\"\n", filename,
            line, argv[2]);
    return;
  } else if (sscanf(argv[3], "%llu", &n) != 1) {
    fprintf(stderr, "%s:%d: expected count, got \"%s\"\n", filename, line,
            argv[3]);
    return;
  }

  err = graph_grmap_add_range(&grmap, &source, &dest, n);
  if (err != 0)
    printf("ERROR: %s\n", graph_xstrerror(err));
  else
    printf("ok: %s->%s[%llu]\n", graph_guid_to_string(&source, b1, sizeof b1),
           graph_guid_to_string(&dest, b2, sizeof b2), n);
}

static void command_tabsize(char const *progname, char const *const *argv,
                            char const *filename, int line) {
  size_t tabsize;
  int err;

  if (argv[1] == NULL || argv[2] != NULL) {
    fprintf(stderr, "%s:%d: usage: tabsize N\n", filename, line);
    return;
  }
  if (sscanf(argv[1], "%zu", &tabsize) != 1) {
    fprintf(stderr, "%s:%d: expected table size, got \"%s\"\n", filename, line,
            argv[1]);
    return;
  }
  err = graph_grmap_set_table_size(&grmap, tabsize);
  if (err != 0)
    printf("ERROR: %s\n", graph_xstrerror(err));
  else
    printf("ok: table size is now %zu\n", tabsize);
}

static void command_invariant(char const *progname, char const *const *argv,
                              char const *filename, int line) {
  graph_grmap_invariant(&grmap);
}

static void command_initialize(char const *progname, char const *const *argv,
                               char const *filename, int line) {
  graph_grmap_initialize(graph, &grmap);
}

static void command_finish(char const *progname, char const *const *argv,
                           char const *filename, int line) {
  graph_grmap_finish(&grmap);
}

static void command_help(char const *progname, char const *const *argv,
                         char const *filename, int line) {
  puts(

      "Graphgrmap is a test client for the graph_grmap data type in "
      "libgraph.a\n"
      "\n"
      "Quickreference:\n"
      "    initialize           finish           invariant\n"
      "    help                 add X Y N\n"
      "    quit                 read [path]      check [path]\n"
      "    tabsize              write [path]\n"
      "            \n");
}

static int command(char const *progname, char const *const *argv, FILE *fp,
                   char const *filename, int line) {
  if (strcasecmp(argv[0], "quit") == 0)
    return 1;
  else if (strcasecmp(argv[0], "add") == 0)
    command_add(progname, argv, filename, line);
  else if (strncasecmp(argv[0], "initialize", 3) == 0)
    command_initialize(progname, argv, filename, line);
  else if (strncasecmp(argv[0], "invariant", 3) == 0)
    command_invariant(progname, argv, filename, line);
  else if (strncasecmp(argv[0], "finish", 3) == 0)
    command_finish(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "tabsize") == 0)
    command_tabsize(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "help") == 0)
    command_help(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "write") == 0)
    command_write(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "read") == 0)
    command_read(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "check") == 0)
    command_check(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "map") == 0)
    command_map(progname, argv, filename, line);
  else {
    fprintf(stderr,
            "%s:%d: unknown command \"%s\" -- "
            "try \"help\"?\n",
            filename, line, argv[0]);
  }
  return 0;
}

static void usage(char const *progname) {
  fprintf(stderr,
          "Usage: %s [-hfv] [files...]\n"
          "Options:\n"
          "  -f		faster allocation (don't trace)\n"
          "  -h		print this message\n"
          "  -t		time command execution\n"
          "  -v		more verbose logging (v ... vvv)\n",
          progname);
  exit(EX_USAGE);
}

static void process(char const *progname, FILE *fp, char const *filename) {
  int line = 0;

  for (;;) {
    char buf[1024 * 8];
    size_t argc = 0;
    char const *argv[128];
    char *arg;

    line++;
    if (isatty(fileno(fp))) {
      fputs("graphgrmap? ", stderr);
      fflush(stderr);
    }

    if (fgets(buf, sizeof buf, fp) == NULL) break;

    arg = buf + strspn(buf, " \t");
    if (*arg == '#' || *arg == '\n') continue;

    for (argc = 0; argc < sizeof(argv) / sizeof(*argv) - 1;) {
      int cc;

      argv[argc++] = arg;
      cc = strcspn(arg, "\n \t\r\n");
      arg += cc;
      if (*arg == '\n' || *arg == '\0') {
        *arg = '\0';
        break;
      }
      *arg++ = '\0';
      arg += strspn(arg, " \t\n");
      if (*arg == '\0' || *arg == '\n') break;
    }
    argv[argc] = NULL;

    if (command(progname, argv, fp, filename, line)) return;
  }
}

int main(int argc, char **argv) {
  int opt;
  char const *progname;
  int opt_fast = 0;
  int opt_verbose = 0;
  char const *opt_coverage = NULL;
  size_t tab_size = 3;

  if ((progname = strrchr(argv[0], '/')) != NULL)
    progname++;
  else
    progname = argv[0];

  signal(SIGSEGV, stacktrace);
  signal(SIGBUS, stacktrace);
  signal(SIGABRT, stacktrace);

  executable = argv[0];
  while ((opt = getopt(argc, argv, "c:fhtvx:")) != EOF) switch (opt) {
      case 'c':
        opt_coverage = optarg;
        break;
      case 'f':
        opt_fast++;
        break;
      case 'n':
        if (sscanf(optarg, "%zu", &tab_size) != 1) {
          fprintf(stderr,
                  "%s: expected number of table "
                  "elements with -n, got \"%s\"\n",
                  progname, optarg);
          usage(progname);
        }
        break;
      case 't':
        opt_time = 1;
        break;
      case 'v':
        opt_verbose++;
        break;
      case 'x':
        executable = optarg;
        break;
      case 'h':
      default:
        usage(progname);
        /* NOTREACHED */
        break;
    }

  /* Create graph's execution environment.
   */
  cl = cl_create();
  if (opt_coverage) cl_set_coverage(cl, opt_coverage);

  cm = opt_fast ? cm_c() : cm_trace(cm_c());
  if (!cl || !cm) {
    fprintf(stderr, "%s: can't create %s environment: %s\n", progname,
            cl ? "memory" : "logging", strerror(errno));
    exit(EX_SOFTWARE);
  }
  if (opt_verbose)
    cl_set_loglevel_full(
        cl, opt_verbose >= 4
                ? CL_LEVEL_SPEW
                : (opt_verbose >= 3
                       ? CL_LEVEL_DEBUG
                       : (opt_verbose >= 2 ? CL_LEVEL_DETAIL : CL_LEVEL_INFO)));

  if ((graph = graph_create(cm, cl)) == NULL) {
    fprintf(stderr, "%s: can't create graph environment: %s\n", progname,
            strerror(errno));
    exit(EX_SOFTWARE);
  }

  if (optind >= argc)
    process(progname, stdin, "*standard input*");
  else
    for (; optind < argc; optind++) {
      FILE *fp;

      if ((fp = fopen(argv[optind], "r")) == NULL) {
        fprintf(stderr, "%s: can't open \"%s\" for input: %s\n", progname,
                argv[optind], strerror(errno));
        exit(EX_NOINPUT);
      }
      process(progname, fp, argv[optind]);
      fclose(fp);
    }

  graph_destroy(graph);
  return 0;
}
