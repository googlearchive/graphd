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
#include "libgraphdb/graphdbp.h"

#include <sys/time.h>

unsigned long long graphdb_time_millis(void) {
  struct timeval tv;

  gettimeofday(&tv, (struct timezone *)NULL);

  return (unsigned long long)tv.tv_sec * 1000 +
         (unsigned long long)tv.tv_usec / 1000;
}
