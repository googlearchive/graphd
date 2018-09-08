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
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sysexits.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "libcm/cm.h"
#include "libcl/cl.h"

/* Global variables implicit in commands. */

cl_handle *cl;
cm_handle *cm;
char const *executable = NULL;
int opt_time = 0;
graph_dateline *var_sets[256] = {NULL};
graph_handle *graph;

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

static graph_dateline **var_set_loc(char const *arg, char const *file,
                                    int line) {
  unsigned char a;

  if (!arg) {
    fprintf(stderr, "%s:%d: expected set variable name\n", file, line);
    return NULL;
  }

  if (!*arg || arg[1]) {
    fprintf(stderr,
            "%s:%d: expected single-character set "
            "variable name, got \"%s\"\n",
            file, line, arg);
    return NULL;
  }

  a = isascii(*arg) ? tolower(*arg) : *arg;
  return var_sets + a;
}

static graph_dateline *var_set(char const *arg, char const *file, int line) {
  graph_dateline **loc;

  if (!(loc = var_set_loc(arg, file, line))) return NULL;

  if (!*loc && (*loc = graph_dateline_create(cm)) == NULL)
    fprintf(stderr, "%s:%d: can't allocate set \"%s\"!\n", file, line, arg);
  return *loc;
}

static int number_scan(unsigned long long *buf, char const *arg,
                       char const *file, int line) {
  if (sscanf(arg, "%llu", buf) != 1) {
    fprintf(stderr, "%s:%d: expected number, got \"%s\"\n", file, line, arg);
    return -1;
  }
  return 0;
}

static void command_add(char const *progname, char const *const *argv,
                        char const *filename, int line) {
  int err;

  unsigned long long dbid, n;
  graph_dateline *gs;

  if (!argv[1] || !argv[2] || !argv[3] || !argv[4] || argv[5]) {
    fprintf(stderr, "%s:%d: usage: add X dbid n iid\n", filename, line);
    return;
  }

  if (!(gs = var_set(argv[1], filename, line))) return;
  if (number_scan(&dbid, argv[2], filename, line)) return;
  if (number_scan(&n, argv[3], filename, line)) return;

  err = graph_dateline_add(&gs, dbid, n, argv[4]);

  if (err)
    printf("ERROR: %s\n", strerror(err));
  else
    printf("ok\n");
}

static void command_create(char const *progname, char const *const *argv,
                           char const *filename, int line) {
  graph_dateline **gsp;
  unsigned long n;

  if (!argv[1] || (argv[2] && argv[3])) {
    fprintf(stderr, "%s:%d: usage: create X [NELEMS]\n", filename, line);
    return;
  }

  if (!(gsp = var_set_loc(argv[1], filename, line))) return;
  if (!argv[2])
    n = 1;
  else if (sscanf(argv[2], "%lu", &n) != 1) {
    fprintf(stderr,
            "%s:%d: expected number of elements to "
            "allocate, got \"%s\"\n",
            filename, line, argv[2]);
    return;
  }

  if (*gsp) graph_dateline_destroy(*gsp);

  *gsp = graph_dateline_create(cm);

  if (!*gsp)
    fprintf(stderr, "%s:%d: graph_dateline_create(%lu) fails\n", filename, line,
            n);
}

static void command_dump(char const *progname, char const *const *argv,
                         char const *filename, int line) {
  graph_dateline **gsp;

  if (!argv[1]) {
    fprintf(stderr, "%s:%d: usage: dump X\n", filename, line);
    return;
  }

  if (!(gsp = var_set_loc(argv[1], filename, line))) return;
  if (!*gsp)
    printf("%s: null\n", argv[1]);
  else {
    char buf[1024 * 8];
    printf("%s: %s\n", argv[1],
           graph_dateline_to_string(*gsp, buf, sizeof buf));
  }
}

static void command_next(char const *progname, char const *const *argv,
                         char const *filename, int line) {
  graph_dateline **gsp;
  unsigned long n;

  if (!argv[1] || (argv[2] && argv[3])) {
    fprintf(stderr, "%s:%d: usage: next X [NELEMS]\n", filename, line);
    return;
  }

  if (!(gsp = var_set_loc(argv[1], filename, line))) return;
  if (!argv[2])
    n = 1;
  else if (sscanf(argv[2], "%lu", &n) != 1) {
    fprintf(stderr,
            "%s:%d: expected number of elements to "
            "iterate over, got \"%s\"\n",
            filename, line, argv[2]);
    return;
  }

  if (!*gsp)
    printf("%s: null\n", argv[1]);
  else {
    int err;
    unsigned long long dbid, n;
    void *state = NULL;

    while (n-- > 0 &&
           (err = graph_dateline_next(*gsp, &dbid, &n, &state)) !=
               GRAPH_ERR_NO) {
      if (err)
        printf("ERROR: %s\n", strerror(err));
      else
        printf("\t%llu.%llu\n", dbid, n);
    }
  }
}

static void command_scan(char const *progname, char const *const *argv,
                         char const *filename, int line) {
  graph_dateline **gsp;
  int err;

  if (!argv[1] || argv[2]) {
    fprintf(stderr, "%s:%d: usage: next* X [NELEMS]\n", filename, line);
    return;
  }

  if (!(gsp = var_set_loc(argv[1], filename, line))) return;

  if (*gsp == NULL) *gsp = graph_dateline_create(cm);

  err = graph_dateline_from_string(gsp, argv[1], argv[1] + strlen(argv[1]));
  if (err)
    printf("ERROR: %s\n", strerror(err));
  else
    printf("ok\n");
}

static void command_format(char const *progname, char const *const *argv,
                           char const *filename, int line) {
  graph_dateline **gsp, *gs;
  char b;
  int err;
  void *state;
  size_t offset;

  if (!argv[1] || argv[2]) {
    fprintf(stderr, "%s:%d: usage: format X\n", filename, line);
    return;
  }
  putchar('\'');

  if (!(gsp = var_set_loc(argv[1], filename, line)))
    gs = NULL;
  else
    gs = *gsp;

  offset = 0;
  state = NULL;

  for (;;) {
    char *buf = &b;

    err = graph_dateline_format(gs, &buf, &b + 1, &state, &offset);
    if (err != 0) {
      if (err != GRAPH_ERR_DONE) fprintf(stderr, "ERROR: %s\n", strerror(err));
      break;
    }
    if (buf != &b + 1) {
      fprintf(stderr, "command_format -- no output?\n");
      break;
    }
    putchar(b);
  }
  putchar('\'');
  putchar('\n');
}

static void command_destroy(char const *progname, char const *const *argv,
                            char const *filename, int line) {
  graph_dateline **gsp;

  if (argv[1] == NULL || argv[2] != NULL) {
    fprintf(stderr, "%s:%d: usage: destroy X\n", filename, line);
    return;
  }

  if (!(gsp = var_set_loc(argv[1], filename, line))) return;

  if (*gsp != NULL) graph_dateline_destroy(*gsp);

  *gsp = NULL;
}

static void command_help(char const *progname, char const *const *argv,
                         char const *filename, int line) {
  puts(

      "Graphdateline is a test client for the graph_dateline data type in "
      "libgraph.a\n"
      "\n"
      "Below, X and Y stand for single-character variable names;\n"
      "DBID stands for a 48-bit database ID, written as a number.\n"
      "N stands for a 34-bit element count, written as a number.\n"
      "\n"
      "Quickreference:\n"
      "    help                 create X        add  X DBID N IID\n"
      "    quit                 destroy X\n"
      "    dump X               format X\n"
      "    time [on / off]\n"
      "    time now\n");
}

static int command(char const *progname, char const *const *argv, FILE *fp,
                   char const *filename, int line) {
  if (strcasecmp(argv[0], "add") == 0)
    command_add(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "create") == 0)
    command_create(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "destroy") == 0)
    command_destroy(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "dump") == 0)
    command_dump(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "help") == 0)
    command_help(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "next") == 0)
    command_next(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "format") == 0)
    command_format(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "scan") == 0)
    command_scan(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "quit") == 0)
    return 1;
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
      fputs("graphdateline? ", stderr);
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
