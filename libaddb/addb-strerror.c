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

char const* addb_strerror(int err) {
  switch (err) {
    case ADDB_ERR_NO:
      return "no";
    case ADDB_ERR_MORE:
      return "more...";

    case ADDB_ERR_PRIMITIVE_TOO_LARGE:
      return "primitive too large";

    case ADDB_ERR_BITMAP:
      return "cannot provide idarray for a bitmap";

    case ADDB_ERR_EXISTS:
      return "entry exists already";

    case ADDB_ERR_ALREADY:
      return "operation already complete";
    case ADDB_ERR_DATABASE:
      return "database corruption detected";

    default:
      break;
  }

  return NULL;
}

char const* addb_xstrerror(int err) {
  char const* str;

  if ((str = addb_strerror(err)) != NULL) return str;

  return strerror(err);
}
