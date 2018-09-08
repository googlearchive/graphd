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
#include "libgdp/gdp.h"

#include <errno.h>

int gdp_init(gdp* parser, cm_handle* cm, cl_handle* cl) {
  *parser = (gdp){
      .cm = cm, .cl = cl, .dbglex = false, .dbgf = stderr,
  };

  return 0;
}

char const* gdp_strerror(int err) {
  switch (err) {
    case GDP_ERR_LEXICAL:
      return "lexical error";

    case GDP_ERR_SYNTAX:
      return "syntax error";

    case GDP_ERR_SEMANTICS:
      return "semantics error";

    default:
      break;
  }
  return NULL;
}
