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
#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sysexits.h>

#include "libgraph/graph.h"
#include "libgraphdb/graphdb.h"

/*  This code demonstrates how to use a graphdb connection
 *  with an asynchronous event loop (poll(), select(), or gdu/libes).
 */

/* When a SIGTERM hits, this file descriptor gets closed.
 */
static int close_me = -1;
static void sig_term(int dummy) {
  if (close(close_me) != 0) fprintf(stderr, "close: %s\n", strerror(errno));
  close_me = -1;
}

typedef struct {
  graphdb_handle* my_graphdb;
  int my_pipe[2];

} my_handle;

/* Try these graphd servers.
 */
static char const* MY_SERVERS[] = {"tcp:localhost:8100", "tcp:taco:47274",
                                   "tcp:localhost:47274", NULL};

static void process(my_handle* my, char const* query) {
  int err;
  int n_outstanding = 0;

  if (query == NULL) query = "read (pagesize=100)";

  /*  Start up -- get a graphd connection.
   */
  my->my_graphdb = graphdb_create();
  err = graphdb_connect(my->my_graphdb, GRAPHDB_INFINITY, MY_SERVERS, 0);
  if (err != 0) {
    fprintf(stderr, "can't connect to servers: %s\n", strerror(err));
    return;
  }

  /*  Execute queries until someone stops me.
   */
  for (;;) {
    struct pollfd pfd[2];
    int graphdb_evs;
    int n;
    graphdb_request_id request_id;
    graphdb_iterator* my_it;
    void* my_data;

    /*  Send requests that need sending.  This is
     *  just demo code, so I'm sending requests one
     *  at a time - but in practice, they could
     *  overlap arbitrarily.
     */
    if (n_outstanding == 0) {
      err = graphdb_request_send(my->my_graphdb, &request_id, "Hello, World!",
                                 query, strlen(query));
      if (err != 0) break;

      n_outstanding++;
    }

    /*  Add graphdb's descriptor and expected events
     *  to the poll set.
     */
    pfd[0].fd = graphdb_descriptor(my->my_graphdb);
    pfd[0].events = 0;

    graphdb_evs = graphdb_descriptor_events(my->my_graphdb);
    if (graphdb_evs & GRAPHDB_INPUT) pfd->events |= POLLIN;
    if (graphdb_evs & GRAPHDB_OUTPUT) pfd->events |= POLLOUT;

    /*  Add the read-end of the controller socket to
     *  the event set.  We're waiting for it to become readable.
     */
    pfd[1].fd = my->my_pipe[0];
    pfd[1].events = POLLIN;

    /*  Wait for something to happen.
     */
    n = poll(pfd, sizeof(pfd) / sizeof(*pfd), -1);
    if (n < 0) {
      fprintf(stderr, "poll fails: %s", strerror(err));
      break;
    }

    /* Pass the events to the graphdb layer.
     */
    graphdb_evs = 0;
    if (pfd[0].revents & POLLIN) graphdb_evs |= GRAPHDB_INPUT;
    if (pfd[0].revents & POLLOUT) graphdb_evs |= GRAPHDB_OUTPUT;
    if (graphdb_evs != 0) {
      err = graphdb_descriptor_io(my->my_graphdb, graphdb_evs);
      if (err != 0) {
        fprintf(stderr, "graphdb_descriptor_io fails: %s", strerror(err));
        break;
      }
    }

    /* If our pipe is readable or closed, that's our signal to stop.
     */
    if (pfd[1].revents) {
      break;
    }

    /*  Get the results of all graphdb requests that have
     *  arrived as a result of the poll loop.  I'm using
     *  a 0 timeout - that will prevent graphdb_request_wait_*
     *  from blocking internally.
     */
    while (graphdb_request_wait_iterator(my->my_graphdb, &request_id,
                                         /* timeout */ 0, &my_data,
                                         &my_it) == 0) {
      char const* text;
      size_t text_n;

      while (graphdb_iterator_read(my->my_graphdb, my_it, &text, &text_n) ==
             0) {
        fwrite(text, text_n, 1, stdout);
      }
      graphdb_iterator_free(my->my_graphdb, my_it);
      if (n_outstanding > 0) n_outstanding--;
    }
  }

  /*  Shut down the back-end connection.
   */
  graphdb_destroy(my->my_graphdb);
}

int main(int argc, char** argv) {
  my_handle my;
  pid_t child;
  int status;

  memset(&my, 0, sizeof(my));
  signal(SIGTERM, sig_term);

  printf("Starting -- kill %lu to stop.\n", (unsigned long)getpid());
  sleep(2);

  if (pipe(my.my_pipe) != 0) {
    fprintf(stderr, "%s: failed to create a pipe: %s\n", argv[0],
            strerror(errno));
    exit(1);
  }

  /* The write end. */
  close_me = my.my_pipe[1];

  switch (child = fork()) {
    case 0:
      /* Child.  It starts an event loop around the
       * pipe's read end the back-end connection.
       */
      close(my.my_pipe[1]);
      signal(SIGTERM, SIG_IGN);
      process(&my, argv[1]);
      exit(0);

    case -1:
      fprintf(stderr, "%s: failed to fork: %s\n", argv[0], strerror(errno));
      exit(1);

    default:
      /*  Parent.  It is waiting for the child to exit.
       */
      close(my.my_pipe[0]);
      waitpid(child, &status, 0);
      exit(1);
  }
}
