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
#include "graphd/graphd.h"

/**
 * @brief Track read suspension.
 *
 *  Update an internal counter that tracks how many times a "read"
 *  (or perhaps "iterate" or "dump", but those are rarer) is suspended
 *  for a "write" (or perhaps "restore") per minute.
 *
 * @param g		The graphd handle
 * @param msnow		Current timestamp in milliseconds
 * @param suspend	Are we suspending a read?
 *
 * @return the number of times any "read" has been suspended in
 *  favor of a "write" in the last minute.
 */
unsigned long graphd_suspend_a_read(graphd_handle* g, unsigned long long msnow,
                                    bool suspend) {
  if (g->g_read_suspends_per_minute_timer == 0) {
    g->g_read_suspends_per_minute_timer = msnow;
    g->g_read_suspends_per_minute = 0;
    g->g_read_suspends_per_minute_current = suspend ? 1 : 0;
  } else if (g->g_read_suspends_per_minute_timer + 60 * 1000 > msnow)
    g->g_read_suspends_per_minute_current += suspend ? 1 : 0;
  else {
    if (g->g_read_suspends_per_minute_timer + 120 * 1000 <= msnow) {
      /*  Start a fresh counting period now.
       */
      g->g_read_suspends_per_minute_timer = msnow;
      g->g_read_suspends_per_minute = 0;
    } else {
      /*  Finish the previous period, and move to
       *  the one immediately following it.
       */
      g->g_read_suspends_per_minute = g->g_read_suspends_per_minute_current;
      g->g_read_suspends_per_minute_timer += 60 * 1000;
    }
    g->g_read_suspends_per_minute_current = suspend ? 1 : 0;
  }
  return g->g_read_suspends_per_minute;
}
