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
#ifndef __GDP_MISC_H__
#define __GDP_MISC_H__

/**
 * Predicts a @e true if statement
 *
 * For instance:
 *
 * @code
 * if likely(x > 0)
 *   ...
 * @endcode
 */
#define likely(cond) (__builtin_expect(cond, 1))

/**
 * Predicts a @e false if statement
 *
 * For instance:
 *
 * @code
 * if unlikely((err = some_function()))
 *   ...
 * @endcode
 */
#define unlikely(cond) (__builtin_expect(cond, 0))

/**
 * Assertion.
 */
#define gdp_assert(cl, cond)                                                 \
  do {                                                                       \
    if                                                                       \
      unlikely((cond) == 0) {                                                \
        cl_notreached_loc(cl, __FILE__, __LINE__, "assertion error: \"%s\"", \
                          #cond);                                            \
        abort(); /* unreachable, but tells GCC we die here */                \
      }                                                                      \
  } while (0)

/**
 * Causes an assertion error due to a bug in the code.
 */
#define gdp_bug(cl)                                            \
  do {                                                         \
    cl_notreached_loc(cl, __FILE__, __LINE__, "oops, a bug!"); \
    abort(); /* unreachable, but tells GCC we die here */      \
  } while (0)

#endif
