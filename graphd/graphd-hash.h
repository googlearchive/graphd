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
#ifndef GRAPHD_HASH_H
#define GRAPHD_HASH_H

#include <limits.h>

#include "libgraph/graph.h"

#ifndef CHAR_BIT
#define CHAR_BIT 8
#endif

/*  In the macros below, "acc" is an unsinged integer type.
 * (It's being shifted right assuming no sign extension.)
 */

/**
 * @brief Rotate the accumulator left.
 * @param acc	the accumulator
 * @param b	bits to rotate by
 */
#define GRAPHD_HASH_ROTATE(acc, b) \
  ((acc) = ((acc) >> (sizeof(acc) * CHAR_BIT - (b))) | ((acc) << (b)))

/**
 * @brief Hash a value into the accumulator
 *
 *  The accumulator is rotated by some small value; the
 *  new value is simply exored into the accumulator.
 *
 * @param acc	the accumulator
 * @param val	the value, an integer
 */
#define GRAPHD_HASH_VALUE(acc, val) (GRAPHD_HASH_ROTATE(acc, 4), (acc) ^= (val))

/**
 * @brief Hash a single bit into the accumulator
 *
 *  Like GRAPHD_HASH_VALUE, but with smaller rotation.
 *
 * @param acc	the accumulator
 * @param val	the value, an integer with value 1 or 0.
 */
#define GRAPHD_HASH_BIT(acc, val) (GRAPHD_HASH_ROTATE(acc, 1), (acc) ^= !!(val))

/**
 * @brief Hash a stretch of bytes into the accumulator
 *
 *  The bytes are hashed using your usual friendly
 *  neighborhood hash function, acc * 33 + *s++;
 *  the result of that is then hashed into the accumulator.
 *
 * @param acc	the accumulator
 * @param s	beginning of the bytes
 * @param e	pointer just after the end of the bytes to hash.
 */
#define GRAPHD_HASH_BYTES(acc, s, e)       \
  do {                                     \
    unsigned char const *_r = (void *)(s); \
    unsigned char const *_e = (void *)(e); \
    unsigned long long _h = 0;             \
    while (_r < _e) _h = _h * 33 + *_r++;  \
    GRAPHD_HASH_ROTATE((acc), 8);          \
    (acc) ^= _h;                           \
  } while (0)

/**
 * @brief Hash a GRAPH_GUID into the accumulator
 *
 * @param acc	the accumulator
 * @param guid	guid
 */
#define GRAPHD_HASH_GUID(acc, guid) \
  GRAPHD_HASH_VALUE(acc, GRAPH_GUID_SERIAL(guid))

#endif /* GRAPHD_HASH_H */
