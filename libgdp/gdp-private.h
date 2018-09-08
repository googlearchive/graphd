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
#ifndef __GDP_PRIVATE_H__
#define __GDP_PRIVATE_H__

#include "libgdp/gdp-input.h"
#include "libgdp/gdp-misc.h"
#include "libgdp/gdp-output.h"
#include "libgdp/gdp-token.h"
#include "libgdp/gdp-lexer.h"

#include <stdbool.h>
#include <stdio.h>

/**
 * A @c graphd parser object.
 */
struct gdp {
  /** Memory allocator */
  cm_handle *cm;

  /** Log handle */
  cl_handle *cl;

  /** Debug flag for the lexer */
  bool dbglex;

  /** Debug file stream */
  FILE *dbgf;
};

#endif
