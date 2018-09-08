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
#ifndef ADDB_SCALAR_H
#define ADDB_SCALAR_H

typedef unsigned char addb_u1;
typedef unsigned char addb_u2[2];
typedef unsigned char addb_u3[3];
typedef unsigned char addb_u4[4];
typedef unsigned char addb_u5[5];
typedef unsigned char addb_u6[6];
typedef unsigned char addb_u8[8];

#define ADDB_GET_U2(S__)                                  \
  ((unsigned int)((unsigned char *)(S__))[0] << (1 * 8) | \
   ((unsigned char *)(S__))[1])

#define ADDB_PUT_U2(S__, V__)                      \
  (((unsigned char *)(S__))[0] = (V__) >> (1 * 8), \
   ((unsigned char *)(S__))[1] = (V__))

#define ADDB_GET_U3(S__)                                   \
  ((unsigned long)((unsigned char *)(S__))[0] << (2 * 8) | \
   (unsigned int)((unsigned char *)(S__))[1] << (1 * 8) |  \
   ((unsigned char *)(S__))[2])

#define ADDB_PUT_U3(S__, V__)                      \
  (((unsigned char *)(S__))[0] = (V__) >> (2 * 8), \
   ((unsigned char *)(S__))[1] = (V__) >> (1 * 8), \
   ((unsigned char *)(S__))[2] = (V__))

#define ADDB_GET_U4(S__)                                   \
  ((unsigned long)((unsigned char *)(S__))[0] << (3 * 8) | \
   (unsigned long)((unsigned char *)(S__))[1] << (2 * 8) | \
   (unsigned int)((unsigned char *)(S__))[2] << (1 * 8) |  \
   ((unsigned char *)(S__))[3])

#define ADDB_PUT_U4(S__, V__)                      \
  (((unsigned char *)(S__))[0] = (V__) >> (3 * 8), \
   ((unsigned char *)(S__))[1] = (V__) >> (2 * 8), \
   ((unsigned char *)(S__))[2] = (V__) >> (1 * 8), \
   ((unsigned char *)(S__))[3] = (V__))

#define ADDB_GET_U5(S__)                                        \
  ((unsigned long long)((unsigned char *)(S__))[0] << (4 * 8) | \
   (unsigned long)((unsigned char *)(S__))[1] << (3 * 8) |      \
   (unsigned long)((unsigned char *)(S__))[2] << (2 * 8) |      \
   (unsigned int)((unsigned char *)(S__))[3] << (1 * 8) |       \
   ((unsigned char *)(S__))[4])

#define ADDB_PUT_U5(S__, V__)                                       \
  (((unsigned char *)(S__))[0] = (unsigned char)((V__) >> (4 * 8)), \
   ((unsigned char *)(S__))[1] = (unsigned char)((V__) >> (3 * 8)), \
   ((unsigned char *)(S__))[2] = (unsigned char)((V__) >> (2 * 8)), \
   ((unsigned char *)(S__))[3] = (unsigned char)((V__) >> (1 * 8)), \
   ((unsigned char *)(S__))[4] = (unsigned char)(V__))

#define ADDB_U5_MAX 0xFFFFFFFFFFull

#define ADDB_GET_U6(S__)                                        \
  ((unsigned long long)((unsigned char *)(S__))[0] << (5 * 8) | \
   (unsigned long long)((unsigned char *)(S__))[1] << (4 * 8) | \
   (unsigned long)((unsigned char *)(S__))[2] << (3 * 8) |      \
   (unsigned long)((unsigned char *)(S__))[3] << (2 * 8) |      \
   (unsigned int)((unsigned char *)(S__))[4] << (1 * 8) |       \
   ((unsigned char *)(S__))[5])

#define ADDB_PUT_U6(S__, V__)                                       \
  (((unsigned char *)(S__))[0] = (unsigned char)((V__) >> (5 * 8)), \
   ((unsigned char *)(S__))[1] = (unsigned char)((V__) >> (4 * 8)), \
   ((unsigned char *)(S__))[2] = (unsigned char)((V__) >> (3 * 8)), \
   ((unsigned char *)(S__))[3] = (unsigned char)((V__) >> (2 * 8)), \
   ((unsigned char *)(S__))[4] = (unsigned char)((V__) >> (1 * 8)), \
   ((unsigned char *)(S__))[5] = (unsigned char)(V__))

#define ADDB_GET_U8(S__)                                        \
  ((unsigned long long)((unsigned char *)(S__))[0] << (7 * 8) | \
   (unsigned long long)((unsigned char *)(S__))[1] << (6 * 8) | \
   (unsigned long long)((unsigned char *)(S__))[2] << (5 * 8) | \
   (unsigned long long)((unsigned char *)(S__))[3] << (4 * 8) | \
   (unsigned long)((unsigned char *)(S__))[4] << (3 * 8) |      \
   (unsigned long)((unsigned char *)(S__))[5] << (2 * 8) |      \
   (unsigned int)((unsigned char *)(S__))[6] << (1 * 8) |       \
   ((unsigned char *)(S__))[7])

#define ADDB_PUT_U8(S__, V__)                                 \
  (((unsigned char *)(S__))[0] =                              \
       (unsigned char)((unsigned long long)(V__) >> (7 * 8)), \
   ((unsigned char *)(S__))[1] =                              \
       (unsigned char)((unsigned long long)(V__) >> (6 * 8)), \
   ((unsigned char *)(S__))[2] =                              \
       (unsigned char)((unsigned long long)(V__) >> (5 * 8)), \
   ((unsigned char *)(S__))[3] =                              \
       (unsigned char)((unsigned long long)(V__) >> (4 * 8)), \
   ((unsigned char *)(S__))[4] =                              \
       (unsigned char)((unsigned long)(V__) >> (3 * 8)),      \
   ((unsigned char *)(S__))[5] =                              \
       (unsigned char)((unsigned long)(V__) >> (2 * 8)),      \
   ((unsigned char *)(S__))[6] =                              \
       (unsigned char)((unsigned short)(V__) >> (1 * 8)),     \
   ((unsigned char *)(S__))[7] = (unsigned char)(V__))

#endif
