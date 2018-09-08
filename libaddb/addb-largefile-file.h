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
#ifndef ADDB_BIGFILE_FILE_H
#define ADDB_BIGFILE_FILE_H

#include "libaddb/addb-scalar.h"

#define ADDB_LARGE_HEADER 80
#define ADDB_LARGE_MAGIC "lfv3"

/*
 * This header lives at the begining of every large file.
 */
typedef struct {
  addb_u4 lhr_magic;
  addb_u8 lhr_size;

} addb_large_header;

#endif
