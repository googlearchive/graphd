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
#include <stdbool.h>
#include <stddef.h>

#include "libgraph/graph.h"

/*
 * Its preprocessor magic time!
 *
 * First we include pdb-bins-numbers with the NUMBER macro set one way.
 * The generates a bunch of statements that looks like:
 * const char pdb__numtruefalsefalse0_0[]="123":
 * (take the contents of the structure, forge a unique name and assign the
 * mantissa
 * as a string to that name).
 *
 * Then, we process the file again inside the number_table[] structure context
 * and assign the fields in the structure.
 *
 * We go through this mess so that we can have num_fnz and num_lnz (akin to
 * start/end)
 * set correctly without a runtime initialize step.
 */

/*
 * Infinity, Zero, Positive, Exponent, Mantissa
 */
#define NUMBER(I, Z, P, E, M) const char pdb__num##I##Z##P##E##_##M[] = #M;

#include "libpdb/pdb-bins-numtable.h"

#undef NUMBER
#define NUMBER(I, Z, P, E, M)             \
  {.num_infinity = I,                     \
   .num_zero = Z,                         \
   .num_positive = P,                     \
   .num_exponent = E,                     \
   .num_dot = NULL,                       \
   .num_fnz = pdb__num##I##Z##P##E##_##M, \
   .num_lnz =                             \
       pdb__num##I##Z##P##E##_##M + sizeof(pdb__num##I##Z##P##E##_##M) - 1},

graph_number pdb_bins_number_table[] = {
#include "libpdb/pdb-bins-numtable.h"
};
#undef NUMBER

const size_t pdb_bins_number_size =
    sizeof(pdb_bins_number_table) / sizeof(graph_number);
