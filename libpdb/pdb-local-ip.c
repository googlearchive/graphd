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
#include "libpdb/pdbp.h"

#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>

unsigned long pdb_local_ip(void) {
  char hostname_buf[512];
  struct hostent *he;
  size_t i;

  if (gethostname(hostname_buf, sizeof hostname_buf) ||
      !(he = gethostbyname(hostname_buf)) || he->h_length != 4)
    return 0;

  for (i = 0; he->h_addr_list[i]; i++) {
    if (((struct in_addr *)he->h_addr_list[i])->s_addr == INADDR_LOOPBACK)
      continue;
    return ((struct in_addr *)he->h_addr_list[i])->s_addr;
  }
  return 0;
}
