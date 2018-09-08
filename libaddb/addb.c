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
#include "libaddb/addb.h"

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

#include "libcl/cl.h"
#include "libcm/cm.h"

#define ADDB_DEFAULT_ISTORE "addb-test-istore.d"
#define ADDB_DEFAULT_GMAP "addb-test-gmap.d"
#define ADDB_DEFAULT_FLAT "addb-test-flat"

/* Global variables implicit in commands. */

cl_handle *cl;
cm_handle *cm;
addb_handle *addb = NULL;
addb_istore *addb_is = NULL;
addb_gmap *addb_gm = NULL;
addb_flat *addb_fl = NULL;
char const *executable = NULL;

unsigned long long horizon = 0;
int opt_time = 0;

typedef struct time_monitor { struct timeval tmon_tv; } time_monitor;

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

static void dumpblock(char *s, unsigned long long n) {
  unsigned long long i;

  for (i = 0; i < n; i++, s++) {
    if (i % 32 == 31) putchar('\n');

    if (isascii(*s) && (isgraph(*s) || *s == ' ')) {
      putchar(*s);
      putchar(' ');
    } else
      printf("%2.2x", (unsigned char)*s);
  }
  if (i % 32 != 0) putchar('\n');
}

static void time_begin(time_monitor *tmon) {
  if (opt_time) gettimeofday(&tmon->tmon_tv, NULL);
}

static void time_end(time_monitor *tmon) {
  if (opt_time) {
    struct timeval tv;
    gettimeofday(&tv, NULL);

    if (tv.tv_usec < tmon->tmon_tv.tv_usec) {
      tv.tv_usec += 1000000;
      tv.tv_sec--;
    }

    printf("%lu.%.6lu seconds\n",
           (unsigned long)(tv.tv_sec - tmon->tmon_tv.tv_sec),
           (unsigned long)(tv.tv_usec - tmon->tmon_tv.tv_usec));
  }
}

static void command_time(char const *progname, char const *const *argv,
                         char const *filename, int line) {
  if (!argv[1])
    puts(opt_time ? "on" : "off");
  else if (strcasecmp(argv[1], "now") == 0) {
    char buf[200];
    struct timeval tv;
    struct tm tm;
    time_t tloc;

    gettimeofday(&tv, NULL);
    tloc = tv.tv_sec;
    localtime_r(&tloc, &tm);

    strftime(buf, sizeof buf, "%T", &tm);
    printf("%s.%.6lu\n", buf, (unsigned long)tv.tv_usec);
  } else if (strcasecmp(argv[1], "on") == 0 ||
             strncasecmp(argv[1], "y", 1) == 0 ||
             strncasecmp(argv[1], "t", 1) == 0)
    opt_time = 1;
  else
    opt_time = 0;
}

static void command_istore(char const *progname, char const *const *argv,
                           char const *filename, int line) {
  time_monitor tmon;
  int err;
  char const *dirpath;

  if (strcasecmp(argv[1], "open") == 0) {
    if (addb_is) {
      addb_istore_checkpoint(addb_is, true, true);
      err = addb_istore_close(addb_is);
    }

    dirpath = argv[2] ? argv[2] : ADDB_DEFAULT_ISTORE;

    time_begin(&tmon);
    addb_is =
        addb_istore_open(addb, dirpath, ADDB_MODE_WRITE | ADDB_MODE_READ, NULL);
    time_end(&tmon);

    if (addb_is == NULL) {
      fprintf(stderr,
              "%s:%d: "
              "addb_istore_open(%s) fails: %s\n",
              filename, line, dirpath, strerror(errno));
    }
  } else if (strcasecmp(argv[1], "close") == 0) {
    if (addb_is == NULL) {
      fprintf(stderr, "%s:%d: no istore to close.\n", filename, line);
      return;
    }

    time_begin(&tmon);

    addb_istore_checkpoint(addb_is, true, true);
    err = addb_istore_close(addb_is);

    time_end(&tmon);

    addb_is = NULL;
  } else if (strcasecmp(argv[1], "remove") == 0) {
    if (addb_is != NULL) {
      err = addb_istore_close(addb_is);

      addb_is = NULL;
    }

    time_begin(&tmon);

    dirpath = argv[2] ? argv[2] : ADDB_DEFAULT_ISTORE;
    addb_istore_remove(addb, dirpath);

    time_end(&tmon);

    addb_is = NULL;
  } else if (strcasecmp(argv[1], "read") == 0) {
    addb_data data;
    unsigned long long ull;
    addb_istore_id id;

    if (addb_is == NULL) {
      fprintf(stderr, "%s:%d: no istore to get from.\n", filename, line);
      return;
    }
    if (argv[2] == NULL) {
      fprintf(stderr, "%s:%d: usage: istore read <index>\n", filename, line);
      return;
    }

    if (sscanf(argv[2], "%llu", &ull) != 1) {
      fprintf(stderr,
              "%s:%d: istore read: expected id, "
              "got \"%s\"\n",
              filename, line, argv[2]);
      return;
    }
    id = ull;

    time_begin(&tmon);
    err = addb_istore_read(addb_is, id, &data);
    time_end(&tmon);

    if (err) {
      fprintf(stderr, "%s:%d: istore read %llu fails: %s\n", filename, line,
              (unsigned long long)id, addb_xstrerror(err));
    } else {
      printf("%llu\n", (unsigned long long)data.data_size);

      if (argv[3] != NULL) {
        if (sscanf(argv[3], "%llu", &ull) != 1) {
          fprintf(stderr,
                  "%s:%d: istore read: "
                  "expected bytecount, "
                  "got \"%s\"\n",
                  filename, line, argv[3]);
          return;
        }

        if (ull > data.data_size) ull = data.data_size;
        dumpblock(data.data_memory, ull);
      }
    }

    addb_istore_free(addb_is, &data);
  } else if (strcasecmp(argv[1], "write") == 0) {
    unsigned long long num, siz, num_i;
    char const *num_str, *siz_str;
    addb_istore_id id;

    if (addb_is == NULL &&
        (addb_is = addb_istore_open(addb, ADDB_DEFAULT_ISTORE,
                                    ADDB_MODE_READ_WRITE, NULL)) == NULL) {
      fprintf(stderr,
              "%s:%d: "
              "addb_istore_open(%s) fails: %s\n",
              filename, line, ADDB_DEFAULT_ISTORE, addb_xstrerror(errno));
      return;
    }

    if (argv[2] == NULL) {
      time_begin(&tmon);
      err = addb_istore_write(addb_is, "Hello, World!", 13, &id);
      time_end(&tmon);
      if (err != 0)
        fprintf(stderr,
                "%s:%d: istore write "
                "\"Hello, World!\" fails: %s\n",
                filename, line, addb_xstrerror(err));
      else
        printf("%llu\n", (unsigned long long)id);
      return;
    } else if (!isdigit(argv[2][0])) {
      size_t n;

      n = strlen(argv[2]);

      time_begin(&tmon);
      err = addb_istore_write(addb_is, argv[2], n, &id);

      time_end(&tmon);
      if (err == 0)
        fprintf(stderr,
                "%s:%d: istore write "
                "\"%s\" fails: %s\n",
                filename, line, argv[2], addb_xstrerror(err));
      else
        printf("%llu\n", (unsigned long long)id);
      return;
    }

    if (argv[3] != NULL) {
      num_str = argv[2];
      siz_str = argv[3];
    } else {
      num_str = "1";
      siz_str = argv[2];
    }

    if (sscanf(num_str, "%llu", &num) != 1) {
      fprintf(stderr,
              "%s:%d: istore write: "
              "expected number of objects, got "
              "\"%s\"\n",
              filename, line, num_str);
      return;
    }
    if (sscanf(siz_str, "%llu", &siz) != 1) {
      fprintf(stderr,
              "%s:%d: istore write: "
              "expected size of objects, got "
              "\"%s\"\n",
              filename, line, siz_str);
      return;
    }

    err = 0;
    time_begin(&tmon);
    for (num_i = 0; num_i < num; num_i++) {
      addb_data d;
      err = addb_istore_alloc(addb_is, (size_t)siz, &d, &id);
      if (err != 0) {
        fprintf(stderr,
                "%s:%d: istore "
                "write [%llu] fails: %s",
                filename, line, siz, addb_xstrerror(err));
        break;
      }

      /* spread christmas cheer */
      memset(d.data_memory, '*', (int)siz);
      addb_istore_free(addb_is, &d);
    }
    time_end(&tmon);
  } else if (strcasecmp(argv[1], "next-id") == 0) {
    addb_istore_id id;

    if (addb_is == NULL &&
        (addb_is = addb_istore_open(addb, ADDB_DEFAULT_ISTORE,
                                    ADDB_MODE_READ_WRITE, NULL)) == NULL) {
      fprintf(stderr,
              "%s:%d: "
              "addb_istore_open(%s) fails: %s\n",
              filename, line, ADDB_DEFAULT_ISTORE, addb_xstrerror(errno));
      return;
    }

    time_begin(&tmon);
    id = addb_istore_next_id(addb_is);
    time_end(&tmon);

    printf("%llu\n", (unsigned long long)id);
  } else
    fprintf(stderr, "%s:%d: unknown command: \"%s\"\n", filename, line,
            argv[1]);
}

/* Keep thing compiling.  Since this code was missing the sync
 * call, it hasn't been working for some time...
 */
static int addb_gmap_checkpoint(addb_gmap *gm, unsigned long long horizon,
                                addb_msclock_t deadline, int hard_sync) {
  return 0;
}

static void command_gmap(char const *progname, char const *const *argv,
                         char const *filename, int line) {
  time_monitor tmon;
  int err;
  char const *dirpath;

  if (strcasecmp(argv[1], "open") == 0) {
    if (addb_gm) {
      addb_gmap_checkpoint(addb_gm, horizon, 0, true);
      err = addb_gmap_close(addb_gm);
      if (err != 0)
        fprintf(stderr,
                "%s:%d: "
                "addb_gmap_close fails: %s\n",
                filename, line, addb_xstrerror(err));
    }

    dirpath = argv[2] ? argv[2] : ADDB_DEFAULT_GMAP;

    time_begin(&tmon);
    addb_gm = addb_gmap_open(addb, dirpath, ADDB_MODE_WRITE | ADDB_MODE_READ, 0,
                             NULL);
    time_end(&tmon);

    if (addb_gm == NULL) {
      fprintf(stderr,
              "%s:%d: "
              "addb_gmap_open(%s) fails: %s\n",
              filename, line, dirpath, strerror(errno));
    }
  } else if (strcasecmp(argv[1], "close") == 0) {
    if (addb_gm == NULL) {
      fprintf(stderr, "%s:%d: no gmap to close.\n", filename, line);
      return;
    }

    time_begin(&tmon);
    addb_gmap_checkpoint(addb_gm, horizon, 0, true);
    err = addb_gmap_close(addb_gm);
    if (err != 0)
      fprintf(stderr, "%s:%d: addb_gmap_close fails: %s\n", filename, line,
              addb_xstrerror(err));
    time_end(&tmon);

    addb_gm = NULL;
  } else if (strcasecmp(argv[1], "remove") == 0) {
    if (addb_gm != NULL) {
      addb_gmap_checkpoint(addb_gm, horizon, 0, true);
      err = addb_gmap_close(addb_gm);
      if (err != 0)
        fprintf(stderr, "%s:%d: addb_gmap_close fails: %s\n", filename, line,
                addb_xstrerror(err));
      addb_gm = NULL;
    }

    time_begin(&tmon);

    dirpath = argv[2] ? argv[2] : ADDB_DEFAULT_GMAP;
    addb_gmap_remove(addb, dirpath);

    time_end(&tmon);

    addb_gm = NULL;
  } else if (strcasecmp(argv[1], "read") == 0) {
    unsigned long long ull;
    addb_gmap_id source, id;
    addb_gmap_iterator iter;

    if (addb_gm == NULL) {
      fprintf(stderr, "%s:%d: no gmap to get from.\n", filename, line);
      return;
    }
    if (argv[2] == NULL) {
      fprintf(stderr, "%s:%d: usage: gmap read <index>\n", filename, line);
      return;
    }
    if (sscanf(argv[2], "%llu", &ull) != 1) {
      fprintf(stderr,
              "%s:%d: gmap read: expected id, "
              "got \"%s\"\n",
              filename, line, argv[2]);
      return;
    }

    time_begin(&tmon);
    source = ull;
    addb_gmap_iterator_initialize(&iter);
    while (!(err = addb_gmap_iterator_next(addb_gm, source, &iter, &id)))
      ;
    time_end(&tmon);

    addb_gmap_iterator_initialize(&iter);
    while (!(err = addb_gmap_iterator_next(addb_gm, source, &iter, &id)))
      printf("%llu\n", (unsigned long long)id);

    if (err == ADDB_ERR_NO)
      puts("ok");
    else
      fprintf(stderr,
              "%s:%d: addb_gmap_iterator_next "
              "%llu fails: %s\n",
              filename, line, (unsigned long long)source, addb_xstrerror(err));
  } else if (strcasecmp(argv[1], "add") == 0) {
    unsigned long long source_ull, id_ull;

    if (addb_gm == NULL &&
        (addb_gm = addb_gmap_open(addb, ADDB_DEFAULT_ISTORE,
                                  ADDB_MODE_READ_WRITE, 0, NULL)) == NULL) {
      fprintf(stderr,
              "%s:%d: "
              "addb_gmap_open(%s) fails: %s\n",
              filename, line, ADDB_DEFAULT_ISTORE, addb_xstrerror(errno));
      return;
    }

    if (argv[2] == NULL || argv[3] == NULL || argv[4] != NULL ||
        sscanf(argv[2], "%llu", &source_ull) != 1 ||
        sscanf(argv[3], "%llu", &id_ull) != 1) {
      fprintf(stderr, "%s:%d: usage: gmap add N M\n", filename, line);
      return;
    }

    time_begin(&tmon);
    horizon++;
    err = addb_gmap_add(addb_gm, source_ull, id_ull, 0);
    time_end(&tmon);

    if (err != 0)
      fprintf(stderr,
              "%s:%d: gmap add %llu %llu fails: "
              "%s\n",
              filename, line, source_ull, id_ull, addb_xstrerror(err));
    else
      puts("ok");
    return;
  } else
    fprintf(stderr, "%s:%d: unknown command: \"%s\"\n", filename, line,
            argv[1]);
}

static void command_flat(char const *progname, char const *const *argv,
                         char const *filename, int line) {
  time_monitor tmon;
  int err;
  char const *path;

  if (strcasecmp(argv[1], "open") == 0) {
    char const *data;

    if (addb_fl) {
      err = addb_flat_close(addb_fl);
      if (err != 0)
        fprintf(stderr,
                "%s:%d: "
                "addb_flat_close fails: %s\n",
                filename, line, addb_xstrerror(err));
    }

    path = argv[2] ? argv[2] : ADDB_DEFAULT_ISTORE;
    data = argv[2] && argv[3] ? argv[3] : "Hello, World!";

    time_begin(&tmon);
    addb_fl = addb_flat_open(addb, path, ADDB_MODE_WRITE | ADDB_MODE_READ, data,
                             strlen(data));
    time_end(&tmon);

    if (addb_fl == NULL) {
      fprintf(stderr,
              "%s:%d: "
              "addb_flat_open(%s) fails: %s\n",
              filename, line, path, addb_xstrerror(errno));
    }
  } else if (strcasecmp(argv[1], "close") == 0) {
    if (addb_fl == NULL) {
      fprintf(stderr, "%s:%d: no flat to close.\n", filename, line);
      return;
    }

    time_begin(&tmon);
    err = addb_flat_close(addb_fl);
    time_end(&tmon);
    if (err != 0)
      fprintf(stderr,
              "%s:%d: "
              "addb_flat_close fails: %s\n",
              filename, line, addb_xstrerror(err));

    addb_fl = NULL;
  } else if (strcasecmp(argv[1], "remove") == 0) {
    if (addb_fl != NULL) {
      err = addb_flat_close(addb_fl);
      addb_fl = NULL;

      if (err != 0)
        fprintf(stderr,
                "%s:%d: "
                "addb_flat_close fails: %s\n",
                filename, line, addb_xstrerror(err));
    }

    time_begin(&tmon);

    path = argv[2] ? argv[2] : ADDB_DEFAULT_ISTORE;
    addb_flat_remove(addb, path);

    time_end(&tmon);

    addb_fl = NULL;
  } else if (strcasecmp(argv[1], "read") == 0) {
    addb_data data;
    unsigned long long ull;

    if (addb_fl == NULL) {
      fprintf(stderr, "%s:%d: no flat to get from.\n", filename, line);
      return;
    }
    time_begin(&tmon);
    err = addb_flat_read(addb_fl, &data);
    time_end(&tmon);

    if (err) {
      fprintf(stderr, "%s:%d: flat read fails: %s\n", filename, line,
              addb_xstrerror(err));
    } else {
      printf("%llu\n", (unsigned long long)data.data_size);

      if (argv[2] != NULL) {
        if (sscanf(argv[2], "%llu", &ull) != 1) {
          fprintf(stderr,
                  "%s:%d: flat read: "
                  "expected bytecount, "
                  "got \"%s\"\n",
                  filename, line, argv[2]);
          return;
        }

        if (ull > data.data_size) ull = data.data_size;
        dumpblock(data.data_memory, ull);
      }
    }
  } else if (strcasecmp(argv[1], "write") == 0) {
    char const *data;

    data = (argv[2] == NULL) ? "Hello, World!" : argv[2];

    if (addb_fl == NULL &&
        (addb_fl = addb_flat_open(addb, ADDB_DEFAULT_FLAT, ADDB_MODE_READ_WRITE,
                                  data, strlen(data))) == NULL) {
      fprintf(stderr,
              "%s:%d: "
              "addb_flat_open(%s) fails: %s\n",
              filename, line, ADDB_DEFAULT_FLAT, addb_xstrerror(errno));
      return;
    }

    time_begin(&tmon);
    err = addb_flat_write(addb_fl, data, strlen(data));
    time_end(&tmon);

    if (err != 0)
      fprintf(stderr,
              "%s:%d: flat write "
              "\"%s\" fails: %s\n",
              filename, line, data, addb_xstrerror(err));
    return;
  } else
    fprintf(stderr, "%s:%d: unknown command: \"%s\"\n", filename, line,
            argv[1]);
}

static void command_help(char const *progname, char const *const *argv,
                         char const *filename, int line) {
  if (argv[1]) {
    if (!strcasecmp(argv[1], "flat"))
      puts(
          "FLAT is a very simple single piece of storage that can be read and\n"
          "written as a whole.  It never changes its size.\n"
          "Commands:\n"
          "    flat open [path] [data]    -- open or create the database in "
          "<path>;\n"
          "                                  if new, fill it with <data> "
          "(default:\n"
          "                                  \"Hello, World!\")\n"
          "    flat close                 -- close the currently open "
          "database\n"
          "    flat remove [path]         -- remove the database file <path>\n"
          "    flat write [data]	        -- write <data> into the flat "
          "database\n"
          "    flat read [N]              -- read data (and dump N bytes)\n");
    else if (!strcasecmp(argv[1], "help"))
      puts(
          "To get more detailed help on a subject, type \"help <subject>\".\n"
          "Subjects are:\n"
          "    help   -- you're reading it\n"
          "    flat   -- flat database commands\n"
          "    istore -- indexed database commands\n"
          "    gmap   -- map database commands\n"
          "    time   -- monitoring execution time\n");
    else if (!strcasecmp(argv[1], "istore"))
      puts(
          "ISTORE is an indexed table of fixed- but variable-sized "
          "structures.\n"
          "Commands:\n"
          "    istore open [path]         -- open or create the database in "
          "<path>\n"
          "    istore close               -- close the currently open "
          "database\n"
          "    istore remove [path]       -- rm -rf the database files\n"
          "    istore write [[N] size]    -- create 1 (or <N>) objects of size "
          "<size>\n"
          "    istore read N [size]       -- access object #n (and print "
          "<size> bytes)\n"
          "    istore next-id             -- print the next ID that would be "
          "allocated\n");
    else if (!strcasecmp(argv[1], "gmap"))
      puts(
          "GMAP maps indices to sets of indices.\n"
          "Commands:\n"
          "    gmap open [path]           -- open or create the database in "
          "<path>\n"
          "    gmap close                 -- close the currently open "
          "database\n"
          "    gmap remove [path]         -- rm -rf the database files\n"
          "    gmap add N M               -- add <M> to the index set for <N>\n"
          "    gmap read N                -- print the index set for <N>\n");
    else if (!strcasecmp(argv[1], "time"))
      puts(
          "A special \"time\" flag, if set, causes command execution times\n"
          "to be printed along with the results.  It can be set "
          "interactively,\n"
          "or from the addb command line with -t.\n"
          "Commands:\n"
          "    time                       -- print whether timing is on or "
          "off\n"
          "    time now                   -- print current time\n"
          "    time on                    -- start printing execution times\n"
          "    time off                   -- stop printing execution times\n");
    else if (!strcasecmp(argv[1], "quit"))
      puts("Quit.  It quits.\n");
    else {
      printf("Sorry, no help on \"%s\"\n", argv[1]);
      puts("Available: flat help istore time");
      return;
    }
    return;
  }
  puts(

      "Addb is a test client for libaddb.a, the attention deficit datab--OOH, "
      "SHINY!\n"
      "For help on a command, use \"help command\".  Quickreference:\n"
      "    help [command]       flat open [path [data]]    istore open [path]\n"
      "    quit                 flat close                 istore close\n"
      "    time                 flat remove [path]         istore remove "
      "[path]\n"
      "    time [on / off]      flat read [nbytes]         istore read N "
      "[nbytes]\n"
      "    time now             flat write data            istore write [[N] "
      "size]\n");
}

static int command(char const *progname, char const *const *argv, FILE *fp,
                   char const *filename, int line) {
  if (strcasecmp(argv[0], "istore") == 0)
    command_istore(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "flat") == 0)
    command_flat(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "gmap") == 0)
    command_gmap(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "help") == 0)
    command_help(progname, argv, filename, line);
  else if (strcasecmp(argv[0], "time") == 0)
    command_time(progname, argv, filename, line);
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
          "  -v		more verbose logging (v ... vvv)\n"
          "\n",
          progname);
  exit(EX_USAGE);
}

static void used_versions(char const *progname) {
  fprintf(stderr,
          "%s was linked with the following versions:\n"
          "  libcm:   %s\n"
          "  libcl:   %s\n"
          "  libaddb: %s\n",
          progname, cm_build_version, cl_build_version, addb_build_version);

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
      fputs("addb? ", stderr);
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
  char const *opt_coverage = 0;
  int opt_fast = 0;
  int opt_verbose = 0;

  if ((progname = strrchr(argv[0], '/')) != NULL)
    progname++;
  else
    progname = argv[0];

  signal(SIGSEGV, stacktrace);
  signal(SIGBUS, stacktrace);
  signal(SIGABRT, stacktrace);

  executable = argv[0];
  while ((opt = getopt(argc, argv, "c:fhtuvx:")) != EOF) switch (opt) {
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
      case 'u':
        used_versions(progname);
        exit(0);

      case 'h':
      default:
        usage(progname);
        /* NOTREACHED */
        break;
    }

  /* Create addb's execution environment.
   */
  cl = cl_create();
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

  if (opt_coverage) cl_set_coverage(cl, opt_coverage);

  if ((addb = addb_create(cm, cl, 1024 * 1024 * 1024, true)) == NULL) {
    fprintf(stderr, "%s: can't create addb environment: %s\n", progname,
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

  addb_destroy(addb);
  return 0;
}
