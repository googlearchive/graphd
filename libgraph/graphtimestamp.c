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
#include <stdio.h>
#include <sysexits.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "libgraph/graph.h"

int main(int argc, char **argv) {
  char const *progname;
  char buf[200];
  graph_timestamp_t ts;
  int err;

  if ((progname = strrchr(argv[0], '/')) != NULL)
    progname++;
  else
    progname = argv[0];

  if (argc != 2) {
    fprintf(stderr, "Usage: %s <timestamp>\n", progname);
    exit(EX_USAGE);
  }

  err = graph_timestamp_from_string(&ts, argv[1], argv[1] + strlen(argv[1]));
  if (err) {
    fprintf(stderr, "error: %s\n",
            err == ERANGE ? "result out of range" : strerror(err));
    exit(1);
  }
  puts(graph_timestamp_to_string(ts, buf, sizeof buf));
  return 0;
}
