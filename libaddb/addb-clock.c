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
#include "libaddb/addbp.h"

#include <sys/time.h>

/**
 * @brief A modification has taken place.
 *
 *  Advance the global sequence number to record that the current
 *  overall system state is different from the previous state.
 *
 * @param addb handle allocated with addb_create()
 */
void addb_opcount_advance(addb_handle* addb) {
  cl_cover(addb->addb_cl);
  addb->addb_opcount++;
}

/**
 * @brief Get the current system state serial number.
 *
 * @param addb handle allocated with addb_create()
 * @return the current system sequence number.
 */
addb_opcount_t addb_opcount_now(addb_handle* addb) {
  cl_cover(addb->addb_cl);
  return addb->addb_opcount;
}

/**
 * @brief Get the current system clock in milliseconds.
 *
 *  This used to be all macros and calls to clock(), which
 *  gave me neither the resolution nor the time I wanted!
 *
 * @param addb handle allocated with addb_create()
 * @return the current wall clock time ni milliseconds.
 */
addb_msclock_t addb_msclock(addb_handle* addb) {
  struct timeval tv;

  gettimeofday(&tv, NULL);
  return (unsigned long long)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}
