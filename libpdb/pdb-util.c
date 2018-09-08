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

#include <ctype.h>
#include <errno.h>
#include <stdlib.h>

#include "libgraph/graph.h"

int pdb_scan_ull(char const** s_ptr, char const* e,
                 unsigned long long* ull_out) {
  unsigned long long ull = 0;
  char const* s = *s_ptr;

  while (s < e && isascii(*s) && isdigit(*s)) {
    if (ull > ((unsigned long long)-1 - (*s - '0')) / 10) return PDB_ERR_SYNTAX;

    ull = ull * 10;
    ull += *s - '0';

    s++;
  }

  if (s == *s_ptr) return PDB_ERR_SYNTAX;

  *ull_out = ull;
  *s_ptr = s;

  return 0;
}

static int atox(int ch) {
  if (!isascii(ch)) return -1;
  if (isdigit(ch)) return ch - '0';
  if (islower(ch)) return 10 + (ch - 'a');
  if (isupper(ch)) return 10 + (ch - 'A');
  return -1;
}

int pdb_xx_encode(pdb_handle* pdb, const char* key, size_t key_n,
                  cm_buffer* buf) {
  char const* key_e;

  for (key_e = key + key_n; key < key_e; key++) {
    int err;

    err = (isascii(*key) && isalnum(*key))
              ? cm_buffer_add_bytes(buf, key, 1)
              : cm_buffer_sprintf(buf, "%%%2.2x", (unsigned char)*key);
    if (err != 0) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "cm_buf_sprintf", err,
                   "%lu bytes of key", (unsigned long)key_n);
      return err;
    }
  }
  return 0;
}

int pdb_xx_decode(pdb_handle* pdb, char const* s, char const* e,
                  cm_buffer* buf) {
  int err;

  for (; s < e; s++) {
    if (*s == '%' && s + 3 <= e) {
      int a = atox(s[1]), b = atox(s[2]);
      char ch;

      if (a < 0 || b < 0) {
        cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
               "pdb_xx_decode: expected two "
               "hex digits after %%, got \"%.3s\"",
               s);
        return PDB_ERR_SYNTAX;
      }
      ch = (a << 4) | b;
      err = cm_buffer_add_bytes(buf, &ch, 1);

      s += 2;
    } else {
      err = cm_buffer_add_bytes(buf, s, 1);
    }
    if (err != 0) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "cm_buffer_add_bytes", err,
                   "error decoding key");
      return err;
    }
  }
  return 0;
}
