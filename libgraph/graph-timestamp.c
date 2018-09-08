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
#include <time.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>

#include "libgraph/graph.h"

#ifdef __sun__
static time_t timegm(struct tm *t) {
  time_t tl, tb;
  struct tm *tg;

  tl = mktime(t);
  if (tl == -1) {
    t->tm_hour--;
    tl = mktime(t);
    if (tl == -1) return -1; /* can't deal with output from strptime */
    tl += 3600;
  }
  tg = gmtime(&tl);
  tg->tm_isdst = 0;

  tb = mktime(tg);
  if (tb == -1) {
    tg->tm_hour--;
    tb = mktime(tg);
    if (tb == -1) return -1; /* can't deal with output from gmtime */
    tb += 3600;
  }

  return tl - (tb - tl);
}
#endif

#define IS_DIGIT(x) (isascii(x) && isdigit(x))

/**
 * @brief Convert a broken-down time to a timestamp.
 *
 *  @param buf	assign the new timestamp to this location
 *  @param year	 full year
 *  @param mon	month of the year, 1..12 (that is, 1 + tm_mon)
 *  @param mday	day of the month, 1..31
 *  @param hour	hour of the day, 0..23
 *  @param min	minute of the hour, 0..59
 *  @param sec	second of the minute, 0..60
 *  @param seq	sequence number of the second, 0..16*1024-1
 *  @return GRAPH_ERR_SEMANTICS if any of the parameters are out of range
 */
int graph_timestamp_from_members(graph_timestamp_t *buf, unsigned int year,
                                 unsigned int mon, unsigned int mday,
                                 unsigned int hour, unsigned int min,
                                 unsigned int sec, unsigned long seq) {
  struct tm tm;
  time_t t;

  if (mon <= 0 || mon > 12 || mday <= 0 || mday > 31 || hour >= 24 ||
      min >= 60 || sec >= 61 || seq >= (1ul << 16)) {
    return GRAPH_ERR_SEMANTICS;
  }

  memset(&tm, 0, sizeof(tm));
  tm.tm_year = year > 1900 ? year - 1900 : year;
  tm.tm_mon = mon - 1;
  tm.tm_mday = mday;
  tm.tm_hour = hour;
  tm.tm_min = min;
  tm.tm_sec = sec;
  t = timegm(&tm);
  if (t == (time_t)-1) {
    return GRAPH_ERR_SEMANTICS;
  }

  *buf = GRAPH_TIMESTAMP_MAKE(t, seq);
  return 0;
}

/**
 * @brief Convert a time to a timestamp.
 *
 *  Note that there are many different timestamps per second.
 *  The timestamp returned is the first of the available timestamps
 *  for the second argument.
 *
 *  @param buf	assign the new timestamp to this location
 *  @param second	the second whose first instant we want to timestamp.
 *  @return 0
 */
int graph_timestamp_from_time(graph_timestamp_t *buf, time_t second) {
  *buf = GRAPH_TIMESTAMP_MAKE(second, 0);
  return 0;
}

/**
 * @brief Convert a human-readable string to a timestamp.
 *
 *    Accomodate:
 *	...long string of digits...
 *	YY[YY[Y]]-MM-DD HH:MM:SS.NNNN
 *	YY[YY[Y]]-MM-DDTHH:MM:SS.NNNN[Z]
 *	12 34 5   67 89 01 23 45 6789		<- 19 digits
 *
 * @param buf store the converted timestamp here
 * @param s 	start of the timestamp string
 * @param e 	pointer just beyond the timestamp string
 * @return 0 on success
 * @return GRAPH_ERR_SEMANTICS on overflow
 * @return GRAPH_ERR_LEXICAL on sequence error
 */
int graph_timestamp_from_string(graph_timestamp_t *buf, char const *s,
                                char const *e) {
  uint_least64_t ull;
  unsigned int mon = 1, mday = 1, hour = 0, min = 0, sec = 0;
  unsigned long year = 0, num = 0;
  int i, err;

  year = 0;
  for (i = 0; i < 5; i++) {
    if (s >= e || *s == '-' || *s == 'Z') break;
    year = (year * 10) + (*s++ - '0');
  }
  if (s >= e || *s == '-' || *s == 'Z') {
    if (i <= 2 && year < 50)
      year += 2000;
    else if (i <= 3 && year < 150)
      year += 1900;

    if (year < 1970 || year >= 2106) return GRAPH_ERR_SEMANTICS;

    if (s >= e || *s == 'Z') goto have_members;
    s++;

/*  A structured date with dashes, colons, etc.  Oh my!
 */

#define TWO_DIGITS(v)                                     \
  do {                                                    \
    if (s >= e || !IS_DIGIT(*s)) {                        \
      return GRAPH_ERR_LEXICAL;                           \
    }                                                     \
    v = *s++ - '0';                                       \
    if (s < e && IS_DIGIT(*s)) v = v * 10 + (*s++ - '0'); \
    if (s >= e || *s == 'Z') goto have_members;           \
  } while (0)

    TWO_DIGITS(mon);
    if (*s++ != '-') {
      return GRAPH_ERR_LEXICAL;
    }
    TWO_DIGITS(mday);
    if (*s != 'T' && *s != ' ') {
      return GRAPH_ERR_LEXICAL;
    }
    s++;

    TWO_DIGITS(hour);
    if (*s++ != ':') {
      return GRAPH_ERR_LEXICAL;
    }
    TWO_DIGITS(min);
    if (*s++ != ':') {
      return GRAPH_ERR_LEXICAL;
    }
    TWO_DIGITS(sec);
    if (*s != '.') {
      return GRAPH_ERR_LEXICAL;
    }

  have_members:
    err = graph_timestamp_from_members(buf, year, mon, mday, hour, min, sec, 0);
    if (err) {
      return err;
    }
  } else {
    ull = year;

    /* A number.  Number of seconds or date-without-dashes. */

    while (s < e && IS_DIGIT(*s)) {
      unsigned long long tmp = ull;
      ull = (ull * 10) + (*s++ - '0');
      if (ull < tmp) {
        return GRAPH_ERR_SEMANTICS;
      }
    }

    /*  Are we looking at a time_t or at a date-without dashes?
     *  (E.g.: "20050221230055" for 2005-02-21T23:00:55)
     */
    if (ull >= 200ul * 365 * 24 * 60 * 60) {
      /* Looks like it's larger than a traditional time_t. */

      sec = ull % 100;
      if (sec > 60) goto try_tm;
      ull /= 100;
      min = ull % 100;
      if (min > 60) goto try_tm;
      ull /= 100;
      hour = ull % 100;
      if (hour > 23) goto try_tm;
      ull /= 100;
      mday = ull % 100;
      if (mday > 31) goto try_tm;
      ull /= 100;
      mon = ull % 100;
      if (mon > 12) goto try_tm;
      ull /= 100;

      year = ull;
      if (year < 50) year += 2000;
      if (year < 150) year += 1900;

      goto have_members;
    } else {
      struct tm *tm_p, tm_buf;
      time_t t;
    try_tm:
      t = (time_t)num;
      if (t != num) {
        return GRAPH_ERR_SEMANTICS;
      }

      errno = 0;
      tm_p = gmtime_r(&t, &tm_buf);
      if (!tm_p) {
        return errno ? errno : GRAPH_ERR_SEMANTICS;
      }
      err = graph_timestamp_from_members(
          buf, tm_p->tm_year + 1900, tm_p->tm_mon + 1, tm_p->tm_mday,
          tm_p->tm_hour, tm_p->tm_min, tm_p->tm_sec, 0);
      if (err) {
        return err;
      }
    }
  }

  if (s < e && *s == '.') {
    num = 0;
    s++;

    while (s < e && IS_DIGIT(*s)) {
      num = num * 10 + (*s - '0');
      s++;
    }
    *buf += num;
  }

  /* Timezone.  Z is the new UTC. */
  if (s < e && *s == 'Z') s++;

  if (s < e) {
    return GRAPH_ERR_LEXICAL;
  }
  return 0;
}

/**
 * @brief Catch up with the current second, if we're behind.
 *
 *  Reset the internal timestamp clock (which autonomously
 *  advances every time the state within the server changes)
 *  to match the current second as measured by the "now"
 *  parameter - but only if we've fallen behind.
 *
 *  This means that, if we've somehow managed to change state
 *  more than 2^16 times per second, we'll be stuck slightly
 *  in the future until the load drops off again.
 *
 * @param ts	the timestamp to advance
 * @param now	the current wall clock time
 */
void graph_timestamp_sync(graph_timestamp_t *ts, time_t now) {
  time_t then;

  if (graph_timestamp_to_time(*ts, &then) == 0 && difftime(now, then) > 0)

    graph_timestamp_from_time(ts, now);
}

/**
 * @brief Return the sequentially next graph_timestamp.
 *
 * Don't worry about the actual time;
 * periodic calls to graph_timestamp_sync() will take care of that.
 *
 *  @param ts	the current (soon to be previous) timestamp.
 *  @return the incremented value of ts
 */
graph_timestamp_t graph_timestamp_next(graph_timestamp_t *ts) {
  if (GRAPH_TIMESTAMP_SERIAL(*ts) < 9999) return ++*ts;
  return *ts = GRAPH_TIMESTAMP_MAKE(GRAPH_TIMESTAMP_TIME(*ts) + 1, 0);
}

/**
 * @brief Break down a timestamp into its time components.
 * @param ts	the timestamp
 * @param buf	a buffer for use in splitting the timestamp into
 *		components.
 * @return a struct tm that represents the year, month, day,
 *	hours, minutes, and seconds encoded in the timestamp.
 */
struct tm *graph_timestamp_to_tm(graph_timestamp_t ts, struct tm *buf) {
  time_t cl = GRAPH_TIMESTAMP_TIME(ts);
  return gmtime_r(&cl, buf);
}

/**
 * @brief Convert a graph_timestamp_t value to time_t.
 * @param ts	the timestamp
 * @param out	assign a time_t value to this (the number of
 *		seconds since Jan 1st, 1970, 00:00:00 GMT).
 * @return 0
 */
int graph_timestamp_to_time(graph_timestamp_t ts, time_t *out) {
  *out = GRAPH_TIMESTAMP_TIME(ts);
  return 0;
}

/**
 * @brief Convert a graph_timestamp_t value to string.
 *
 *  The string has the form YYYY-MM-DDTHH:MM:SS.NNNN, fit for
 *  use in a graphd protocol argument list.
 *
 * @param ts	the timestamp
 * @param buf	a buffer to use for formatting
 * @param bufsize	number of bytes pointed to by buf
 *
 * @return a string representation of the timestamp, formatted
 * 	as YYYY-MM-DDTHH:MM:SS.NNNN; or "????-??-??T??:??:??.????"
 * 	if the conversion failed.
 */
char const *graph_timestamp_to_string(graph_timestamp_t ts, char *buf,
                                      size_t bufsize) {
  struct tm tm_buf, *tm_ptr;

  tm_ptr = graph_timestamp_to_tm(ts, &tm_buf);
  if (!tm_ptr)
    return "????"
           "-"
           "??"
           "-"
           "??"
           "T"
           "??:??:??.????Z";

  snprintf(buf, bufsize, "%d-%2.2d-%2.2dT%2.2d:%2.2d:%2.2d.%.4dZ",
           1900 + tm_ptr->tm_year, tm_ptr->tm_mon + 1, tm_ptr->tm_mday,
           tm_ptr->tm_hour, tm_ptr->tm_min, tm_ptr->tm_sec,
           GRAPH_TIMESTAMP_SERIAL(ts));
  return buf;
}
