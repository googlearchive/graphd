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
#include <ctype.h>
#include <errno.h>
#include <string.h>

static char const pdb_database_name_alphabet[] =
    "abcdefghijklmnopqrstuvwxyz0123456789-";

static unsigned long long pdb_database_name_to_id(char const* name) {
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

#ifndef UTILITY_PDBNAME

#include "libpdb/pdbp.h"

/**
 * @brief Set the database ID based on a string name.
 *
 *  The name should have between 0 and 7 characters (extra
 *  characters at the end are ignored); the characters should
 *  be from the alphabet 0-9a-z and "-".  Upper-case characters
 *  will be mapped to lower case; non-representable characters
 *  will be mapped to "-".
 *
 * @param pdb	Database whose ID we're setting
 * @param name	Name to set it to.
 *
 * @return 0 on success, EINVAL if parameters are NULL.
 */
int pdb_set_database_name(pdb_handle* pdb, char const* name) {
  if (pdb == NULL || name == NULL) return EINVAL;

  pdb->pdb_database_id = pdb_database_name_to_id(name);
  return 0;
}

#else /* UTILITY_PDBNAME */

#include <stdlib.h>
#include <sysexits.h>
#include <stdio.h>

int main(int argc, char** argv) {
  int i;

  if (argc == 1) {
    fprintf(stderr,
            "Usage: %s names... - convert a name to its "
            "corresponding numeric id\n",
            argv[0]);
    exit(EX_USAGE);
  }

  for (i = 1; i < argc; i++) printf("%llx\n", pdb_database_name_to_id(argv[i]));

  exit(0);
}

#endif /* UTILITY_PDBNAME */
