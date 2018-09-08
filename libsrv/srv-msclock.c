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
#include "srvp.h"
#include <sys/time.h>

/**
 * @brief Get the current system clock in milliseconds.
 *
 *  This used to be all macros and calls to clock(), which
 *  gave me neither the resolution nor the time I wanted!
 *
 * @param srv  module handle
 * @return the current wall clock time in milliseconds.
 */
srv_msclock_t srv_msclock(srv_handle* srv) {
  struct timeval tv;

  gettimeofday(&tv, NULL);
  return (unsigned long long)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}
