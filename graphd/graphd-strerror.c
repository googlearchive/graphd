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
#include "graphd/graphd.h"

#include <errno.h>

#include "libgdp/gdp.h"

char const* graphd_strerror(int err) {
  char const* str;

  switch (err) {
    case GRAPHD_ERR_SEMANTICS:
      return "semantics error";

    case GRAPHD_ERR_SYNTAX:
      return "syntax error";

    case GRAPHD_ERR_LEXICAL:
      return "lexical error";

    case GRAPHD_ERR_TOO_LONG:
      return "request too long";

    case GRAPHD_ERR_MALFORMED:
      return "malformed request";

    case GRAPHD_ERR_NO:
      return "no";

    case GRAPHD_ERR_MORE:
      return "more...";

    case GRAPHD_ERR_PRIMITIVE_TOO_LARGE:
      return "primitive too large";

    case GRAPHD_ERR_ALREADY:
      return "operation already completed";

    case GRAPHD_ERR_UNIQUE_EXISTS:
      return "unique subgraph already exists";

    case GRAPHD_ERR_TILE_LEAK:
      return "tile reference leaked";

    case GRAPHD_ERR_TOO_MANY_MATCHES:
      return "maximum count exceeded";

    case GRAPHD_ERR_TOO_LARGE:
      return "value too large";

    case GRAPHD_ERR_TOO_SMALL:
      return "value too small";

    case GRAPHD_ERR_NOT_A_REPLICA:
      return "not a replica server";

    case GRAPHD_ERR_TOO_HARD:
      return "request too difficult";

    case GRAPHD_ERR_RESTORE_MISMATCH:
      return "attempt to replace an existing primitive "
             "with a different one during restore";

    case GRAPHD_ERR_SMP:
      return "could not affect the SMP followers";

    case GRAPHD_ERR_SMP_STARTUP:
      return "could not startup SMP";

    case GRAPHD_ERR_SUSPEND:
      return "unexpected request suspension";

    case GRAPHD_ERR_SMP_WRITE:
      return "SMP write error";

    case GRAPHD_ERR_BADCURSOR:
      return "invalid cursor";

    default:
      break;
  }
  if ((str = pdb_strerror(err)) != NULL) return str;

  if ((str = gdp_strerror(err)) != NULL) return str;

  if ((str = graph_strerror(err)) != NULL) return str;

  if ((str = srv_strerror(err)) != NULL) return str;

  return strerror(err);
}
