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
/* A simple test program for the GUID compression and
 * decompression algorithm.
 *
 * IT SLICES, IT DICES, ...
 * - invoked with GUIDs, it compresses them
 * - invoked with compressed GUIDs, it decompresses them
 * - invoked with garbage, it prints an error message.
 *
 *  A "database id" can be supplied using the -d or -n
 *  option.  With -d, it's numeric (in hex); with -n, the
 *  database name is specified as if with the "database { id }"
 *  option in the configuration file.
 */

#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sysexits.h>
#include <unistd.h>

#include "libgraph/graph.h"

static void usage(char const *progname) {
  fprintf(stderr, "Usage: %s [-d dbid] [-n dbname] guid...\n", progname);
  exit(EX_USAGE);
}

static char const pdb_database_name_alphabet[] =
    "abcdefghijklmnopqrstuvwxyz0123456789-";

/* Stolen from pdb-set-database-name.c */

static unsigned long long database_name_to_id(char const *name) {
  unsigned long long id;
  size_t i;

  if (name == NULL) name = "";

  id = 0;
  for (i = 0; *name != '\0' && i < 7; i++, name++) {
    int ch;

    if (!isascii(*name))
      ch = '-';
    else if (isalnum(*name))
      ch = tolower(*name);
    else
      ch = '-';

    id = id * 38 +
         (strchr(pdb_database_name_alphabet, ch) - pdb_database_name_alphabet);
  }

  /*  38^7 takes up 5 bytes at most.  IDs have 6 bytes.
   *
   *  Distinguish our IDs from the ones generated
   *  using an IP4 address by injecting a 0xFF in the
   *  fourth IPv4 address byte.   (Single-host IP addresses
   *  don't have FF bytes.)
   */
  return ((id & ~((1ull << 16) - 1)) << 8) | (0xFFull << 16) |
         (id & ((1ull << 16) - 1));
}

int main(int argc, char **argv) {
  char const *progname;
  int err, opt, i;
  unsigned long long dbid = 0;

  if ((progname = strrchr(argv[0], '/')) != NULL)
    progname++;
  else
    progname = argv[0];

  if (argc <= 1) usage(progname);

  /*  Parse parameters.
   */
  while ((opt = getopt(argc, argv, "d:n:")) != EOF) switch (opt) {
      case 'd':
        if (sscanf(optarg, "%llx", &dbid) != 1) {
          fprintf(stderr,
                  "%s: expected database id, "
                  "got \"%s\"\n",
                  progname, optarg);
          usage(progname);
        }
        break;
      case 'n':
        dbid = database_name_to_id(optarg);
        break;

      default:
        usage(progname);
    }

  if (optind >= argc) usage(progname);

  /*  Process trailing arguments as GUIDs.
   */
  for (i = optind; i < argc; i++) {
    graph_guid guid;
    char buf[GRAPH_GUID_SIZE];

    if (strlen(argv[i]) < GRAPH_GUID_SIZE - 1) {
      /* Decode a compresseed GUID. */

      err = graph_guid_uncompress(dbid, &guid, argv[i],
                                  argv[i] + strlen(argv[i]));
      if (err != 0) {
        fprintf(
            stderr, "%s: %s\n", argv[i],
            err == EILSEQ ? "syntax error in compressed GUID" : strerror(err));
      } else
        puts(graph_guid_to_string(&guid, buf, sizeof buf));
    } else {
      /*  Compress a regular GUID. */

      err = graph_guid_from_string(&guid, argv[i], argv[i] + strlen(argv[i]));
      if (err != 0)
        fprintf(stderr, "%s: %s\n", argv[i],
                err == EILSEQ ? "syntax error in GUID" : strerror(err));

      else {
        puts(graph_guid_compress(dbid, &guid, buf, sizeof buf));
      }
    }
  }
  return 0;
}
