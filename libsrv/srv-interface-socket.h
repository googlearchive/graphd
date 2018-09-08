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
#include <string.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <netdb.h>
#include <sys/fcntl.h>
#if __sun__
#include <fcntl.h>
#endif

#include "srvp.h"
#include "srv-interface.h"

typedef struct socket_connection {
  es_descriptor conn_ed;
  srv_session* conn_protocol_session;
  srv_handle* conn_srv;
  es_handle* conn_es;
  int conn_sock;
  char const* conn_displayname;
  char* conn_peername;
  srv_interface_socket_type conn_socket_type;

  /*
          struct sockaddr_in		  conn_peer;
          unsigned int			  conn_poll_lost;
          char				  conn_sockname[
                                               sizeof("123.123.123.123:12345")
     ];
          char				  conn_peername[
                                               sizeof("123.123.123.123:12345")
     ];
  */

} socket_connection;

typedef struct unix_connection {
  socket_connection uconn_connection;

} unix_connection;

void srv_socket_close(cl_handle* cl, int fd, bool block);

int srv_socket_block(cl_handle* cl, int fd, bool block);

bool srv_socket_run(void* conn_data, srv_handle* srv, srv_session* ses,
                    srv_msclock_t deadline);

void srv_socket_listen(void* conn_data, srv_handle* srv, srv_session* ses);

void srv_socket_set_timeout(void* data, srv_timeout* timeout);

void srv_socket_es_connection_callback(es_descriptor* ed, int fd,
                                       unsigned int events);
