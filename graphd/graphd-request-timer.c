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

#include <stdbool.h>
#include <stdint.h>
#include <sys/time.h>

#if defined(__GNUC__) && defined(__x86_64__)
#define USE_TSC
/*
 * drop a note in the object file that we are indeed using the TSC
 */
int graphd_was_compiled_with_tsc = 1;
#endif

/*
 * 100,000,000 ticks: about 20 milliseconds.
 */
#define TICKS_BETWEEN_GETTIMEOFDAY 100000000ull

#ifdef USE_TSC
unsigned long long graphd_request_timer_get_tsc(void) {
  uint32_t low, high;
  unsigned long long tsc;
  /*
   * Read the TSC.  Low is eax and high is edx.
   * Result is unpredictable but clobbers no state
   *
   * AMD document # 25112, page 293 says gives a maximum latency of
   * 12 cycles for rdtsc. Calling it often should not hurt.
   */

  asm volatile("rdtsc" : "=a"(low), "=d"(high));
  tsc = (unsigned long long)low | ((unsigned long long)high << 32);
  return tsc;
}
#else
unsigned long long graphd_request_timer_get_tsc(void) { return 0; }
#endif

static unsigned long long request_timer_get_us(void) {
  unsigned long long us;
  struct timeval tv;
  gettimeofday(&tv, NULL);

  us = (unsigned long long)tv.tv_usec + tv.tv_sec * 1000000ull;

  return us;
}

bool graphd_request_timer_check(graphd_request* greq) {
  unsigned long long now_ticks;
  unsigned long long now_us;

  if (greq->greq_timeout_deadline == 0) {
    return false;
  }
  now_ticks = graphd_request_timer_get_tsc();

  /*
   * Do not trust that ticks is always increasing.
   */
  if ((now_ticks > greq->greq_timeout_ticks) &&
      (greq->greq_timeout_ticks + TICKS_BETWEEN_GETTIMEOFDAY) < now_ticks) {
    greq->greq_timeout_ticks = now_ticks;
    return false;
  } else {
    now_us = request_timer_get_us();

    if (now_us > greq->greq_timeout_deadline) {
      cl_log(graphd_request_cl(greq), CL_LEVEL_FAIL,
             "graphd_request_timer_check: ran out of time"
             " now is %llu, deadline: %llu",
             now_us, greq->greq_timeout_deadline);

      greq->greq_timeout_deadline = 0;
      return true;
    } else
      return false;
  }
}

void graphd_request_timer_start(graphd_request* greq,
                                unsigned long long timeout) {
  greq->greq_timeout_ticks = graphd_request_timer_get_tsc();
  greq->greq_timeout_deadline = request_timer_get_us() + timeout;
}

void graphd_request_timer_stop(graphd_request* greq) {
  greq->greq_timeout_ticks = 0;
  greq->greq_timeout_deadline = 0;
}
