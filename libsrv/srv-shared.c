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
#include <stdio.h>
#include <sys/mman.h>

#include "srvp.h"

int srv_shared_initialize(srv_handle *srv) {
  srv_shared *ssh;
  int err;
  int i;

  cl_assert(srv->srv_cl, srv->srv_shared == NULL);

  ssh = (srv_shared *)mmap(NULL, sizeof(srv_shared), PROT_READ | PROT_WRITE,
                           MAP_ANON | MAP_SHARED, -1, 0);

  if (ssh == (void *)-1) {
    err = errno;
    cl_log_errno(srv->srv_cl, CL_LEVEL_FATAL, "mmap", errno,
                 "failed to allocate %zu bytes of shared memory!",
                 sizeof(srv_shared));
    return err;
  }

  *ssh = (srv_shared){
      .ssh_safe = true,      // child in a safe state
      .ssh_restart = true,   // after a crash, restart child
      .ssh_crashed = false,  // never crashed before
  };
  for (i = 0; i < SRV_MAX_PROCESS_COUNT; i++) {
    ssh->ssh_connections[i] = -1;
  }

  srv->srv_shared = ssh;
  return 0;
}

void srv_shared_finish(srv_handle *srv) {
  cl_assert(srv->srv_cl, srv->srv_shared != NULL);

  munmap((void *)srv->srv_shared, sizeof(srv_shared));
  srv->srv_shared = NULL;
}

void srv_shared_set_restart(srv_handle *srv, bool can_restart) {
  srv->srv_shared->ssh_restart = can_restart;
}

bool srv_shared_can_restart(srv_handle const *srv) {
  return srv->srv_shared->ssh_restart;
}

void srv_shared_set_safe(srv_handle *srv, bool is_safe) {
  srv->srv_shared->ssh_safe = is_safe;
}

bool srv_shared_is_safe(srv_handle const *srv) {
  return srv->srv_shared->ssh_safe;
}

void srv_shared_set_crashed(srv_handle *srv) {
  srv->srv_shared->ssh_crashed = true;
}

bool srv_shared_has_crashed(srv_handle const *srv) {
  return srv->srv_shared->ssh_crashed;
}

void srv_shared_set_time(srv_handle const *srv, time_t start_time) {
  srv->srv_shared->ssh_time = start_time;
}

time_t srv_shared_get_time(srv_handle const *srv) {
  return srv->srv_shared->ssh_time;
}

static void srv_shared_set_connection_count(srv_handle *srv, int index,
                                            int connections) {
  if (index >= 0 && index < SRV_MAX_PROCESS_COUNT)
    srv->srv_shared->ssh_connections[index] = connections;
}

void srv_shared_connection_activate_index(srv_handle *srv, int index,
                                          bool valid) {
  srv_shared_set_connection_count(srv, index, valid ? 0 : -1);
}

void srv_shared_connection_increment(srv_handle *srv, int index) {
  int count = srv_shared_get_connection_count(srv, index);
  if (count != -1) srv_shared_set_connection_count(srv, index, count + 1);
}

void srv_shared_connection_decrement(srv_handle *srv, int index) {
  int count = srv_shared_get_connection_count(srv, index);
  if (count > 0) srv_shared_set_connection_count(srv, index, count - 1);
}

int srv_shared_get_connection_count(srv_handle *srv, int index) {
  return srv->srv_shared->ssh_connections[index];
}
