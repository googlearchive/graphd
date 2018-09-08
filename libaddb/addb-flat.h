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
#ifndef ADDB_FLAT_H
#define ADDB_FLAT_H

#include "libaddb/addb-flat-file.h"
#include "libaddb/addbp.h"

#include <stdlib.h> /* size_t */
#include <unistd.h> /* off_t */

/**
 * @brief Store a small fixed-size structure as a file.
 */
struct addb_flat {
  /**
   * @brief Opaque database handle.
   */
  addb_handle* fl_addb;

  /**
   * @brief Pathname of the underlying database file.
   */
  char* fl_path;

  /**
   * @brief Number of bytes mapped into memory.
   */
  size_t fl_memory_size;

  /**
   * @brief File contents.
   */
  char* fl_memory;

  /**
   * @brief Descriptor of the underlying file.
   */
  int fl_fd;
};

#endif /* ADDBP_FLAT_H */
