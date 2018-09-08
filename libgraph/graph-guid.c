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
#include <ctype.h>
#include <errno.h>
#include <time.h>

#include "libgraph/graphp.h"

graph_guid const graph_guid_null = {0, 0};

static char xtoa_tab[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                            '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

static signed char atox_tab[256] = {
    /* 00 */ -1, -1, -1, -1, -1, -1, -1, -1,
    -1,          -1, -1, -1, -1, -1, -1, -1,
    /* 10 */ -1, -1, -1, -1, -1, -1, -1, -1,
    -1,          -1, -1, -1, -1, -1, -1, -1,
    /* 20 */ -1, -1, -1, -1, -1, -1, -1, -1,
    -1,          -1, -1, -1, -1, -1, -1, -1,
    /* 30 */ 0,  1,  2,  3,  4,  5,  6,  7,
    8,           9,  -1, -1, -1, -1, -1, -1,
    /* 40 */ -1, 10, 11, 12, 13, 14, 15, -1,
    -1,          -1, -1, -1, -1, -1, -1, -1,
    /* 50 */ -1, -1, -1, -1, -1, -1, -1, -1,
    -1,          -1, -1, -1, -1, -1, -1, -1,
    /* 60 */ -1, 10, 11, 12, 13, 14, 15, -1,
    -1,          -1, -1, -1, -1, -1, -1, -1,
    /* 70 */ -1, -1, -1, -1, -1, -1, -1, -1,
    -1,          -1, -1, -1, -1, -1, -1, -1,
    /* 80 */ -1, -1, -1, -1, -1, -1, -1, -1,
    -1,          -1, -1, -1, -1, -1, -1, -1,
    /* 90 */ -1, -1, -1, -1, -1, -1, -1, -1,
    -1,          -1, -1, -1, -1, -1, -1, -1,
    /* A0 */ -1, -1, -1, -1, -1, -1, -1, -1,
    -1,          -1, -1, -1, -1, -1, -1, -1,
    /* B0 */ -1, -1, -1, -1, -1, -1, -1, -1,
    -1,          -1, -1, -1, -1, -1, -1, -1,
    /* C0 */ -1, -1, -1, -1, -1, -1, -1, -1,
    -1,          -1, -1, -1, -1, -1, -1, -1,
    /* D0 */ -1, -1, -1, -1, -1, -1, -1, -1,
    -1,          -1, -1, -1, -1, -1, -1, -1,
    /* E0 */ -1, -1, -1, -1, -1, -1, -1, -1,
    -1,          -1, -1, -1, -1, -1, -1, -1,
    /* F0 */ -1, -1, -1, -1, -1, -1, -1, -1,
    -1,          -1, -1, -1, -1, -1, -1, -1,
};

#define times16(x) x x x x x x x x x x x x x x x x

/**
 * @brief Build a GUID from its component parts.
 * @param buf	assign the result to this GUID
 * @param db 	world-wide unique database identifier
 * @param serial internal server-local serial number
 */
void graph_guid_from_db_serial(graph_guid *buf, unsigned long long db,
                               unsigned long long serial) {
  buf->guid_a = GRAPH_GUID_MAKE_A_HOST(db >> 16) |
                GRAPH_GUID_MAKE_A_RANDOM(db & ((1 << 16) - 1)) |
                GRAPH_GUID_MAKE_A_RFC4122;

  buf->guid_b = GRAPH_GUID_MAKE_B_SERIAL(serial) | GRAPH_GUID_MAKE_B_RFC4122;
}

/**
 * @brief Scan a string into an unsigned long long.  (Utility.)
 *
 * Normally, GUIDs are written as 32-byte hex strings.
 * Additionally, the words "0" and "null" are accepted
 * as synonyms for the NULL object identifier.
 *
 * @param out	assign the result to this location.
 * @param s	beginning of the string
 * @param e	pointer just behind the string's last byte
 * @result EINVAL if there was a syntax error
 * @result ERANGE on overflow
 * @result 0 on success
 */
int graph_ull_from_hexstring(unsigned long long *out, char const *s,
                             char const *e) {
  signed char c;
  unsigned long long ull;

  ull = 0;

  while (s < e) {
    if ((c = atox_tab[(unsigned char)*s++]) < 0) return EILSEQ;

    if (ull > (~0ull >> 4)) return ERANGE;

    ull = (ull << 4) | c;
  }

  *out = ull;
  return 0;
}

/**
 * @brief Scan a string into a GUID.
 *
 * Normally, GUIDs are written as 32-byte hex strings.
 * Additionally, the words "0" and "null" are accepted
 * as synonyms for the NULL object identifier.
 *
 * Additionally, the word "gXXXXXXX" is accepted as a
 * shortcut for a highly compressed GUID.
 *
 * @param buf	assign the result to this GUID
 * @param s	beginning of the string
 * @param e	pointer just behind the string's last byte
 * @result EINVAL if there was a syntax error
 * @result 0 on success
 */
int graph_guid_from_string(graph_guid *buf, char const *s, char const *e) {
  signed char c;
  unsigned long long ull;

  if (e - s != 32) {
    /* Accept both "0" and "null" as spellings for a null GUID.
     */
    if (e - s == 1 && *s == '0') {
      buf->guid_a = 0;
      buf->guid_b = 0;

      return 0;
    }
    if (e - s == 4 && strncasecmp(s, "null", 4) == 0) {
      buf->guid_a = 0;
      buf->guid_b = 0;

      return 0;
    } else if (e - s > 8 && *s == 'g') {
      /* XXX tinyguid
       */
    }
    return EINVAL;
  }

  ull = 0;

  times16(if ((c = atox_tab[(unsigned char)*s++]) < 0) return EINVAL;
          ull = (ull << 4) | c;)

      buf->guid_a = ull;

  ull = 0;
  times16(if ((c = atox_tab[(unsigned char)*s++]) < 0) return EINVAL;
          ull = (ull << 4) | c;)

      buf->guid_b = ull;

  /*  Transitional: Automatically convert from old-style GUIDs
   *  to new-style GUIDs.
   *
   *  Old: timestamp in
   *  New:
   */
  return 0;
}

/**
 * @brief Print a GUID as a string.
 *
 * Normally, GUIDs are written as 32-byte hex strings.
 * Additionally, the words "0" and "null" are accepted
 * as synonyms for the NULL object identifier.
 *
 * @param guid	assign the result to this GUID
 * @param buf	convert into this buffer
 * @param bufsize number of usable bytes pointed to by buf
 * @result "null" if the GUID pointer was a NULL pointer
 * @result "0" if the guid was null
 * @result otherwise, a pointer to a 32-byte hex string encoding a GUID.
 */
char const *graph_guid_to_string(graph_guid const *guid, char *buf,
                                 size_t bufsize) {
  char *p;
  uint_least64_t ull;

  if (guid == NULL) return "null";
  if (guid->guid_a == 0 && guid->guid_b == 0) return "0";

  if (bufsize < 33) return NULL;
  p = buf + 32;
  *p = '\0';

  /* typeset backwards.
   */

  ull = guid->guid_b;
  times16(*--p = xtoa_tab[(ull & 0xF)]; ull >>= 4;)

      ull = guid->guid_a;
  times16(*--p = xtoa_tab[(ull & 0xF)]; ull >>= 4;) return buf;
}

/**
 * @brief Represent a GUID in an endianness-independent, compact binary format.
 *
 * @param guid	assign the result to this GUID
 * @param buf	convert into this buffer
 * @param bufsize number of usable bytes pointed to by buf, must be >= 16.
 * @result a pointer to a 16-byte byte string encoding a GUID.
 * @result NULL if the GUID pointer was NULL, or the bufsize was too small.
 */
char const *graph_guid_to_network(graph_guid const *guid, char *buf,
                                  size_t bufsize) {
  if (guid == NULL || buf == NULL || bufsize < 16) return NULL;

  *(unsigned char *)buf++ = (unsigned char)(guid->guid_a >> (7 * 8));
  *(unsigned char *)buf++ = (unsigned char)(guid->guid_a >> (6 * 8));
  *(unsigned char *)buf++ = (unsigned char)(guid->guid_a >> (5 * 8));
  *(unsigned char *)buf++ = (unsigned char)(guid->guid_a >> (4 * 8));
  *(unsigned char *)buf++ = (unsigned char)(guid->guid_a >> (3 * 8));
  *(unsigned char *)buf++ = (unsigned char)(guid->guid_a >> (2 * 8));
  *(unsigned char *)buf++ = (unsigned char)(guid->guid_a >> (1 * 8));
  *(unsigned char *)buf++ = (unsigned char)(guid->guid_a >> (0 * 8));

  *(unsigned char *)buf++ = (unsigned char)(guid->guid_b >> (7 * 8));
  *(unsigned char *)buf++ = (unsigned char)(guid->guid_b >> (6 * 8));
  *(unsigned char *)buf++ = (unsigned char)(guid->guid_b >> (5 * 8));
  *(unsigned char *)buf++ = (unsigned char)(guid->guid_b >> (4 * 8));
  *(unsigned char *)buf++ = (unsigned char)(guid->guid_b >> (3 * 8));
  *(unsigned char *)buf++ = (unsigned char)(guid->guid_b >> (2 * 8));
  *(unsigned char *)buf++ = (unsigned char)(guid->guid_b >> (1 * 8));
  *(unsigned char *)buf++ = (unsigned char)(guid->guid_b >> (0 * 8));

  return buf - 16;
}

/**
 * @brief Represent a GUID in an endianness-independent, compact binary format.
 *
 * @param guid	assign the result to this GUID
 * @param buf	convert into this buffer
 * @param bufsize number of usable bytes pointed to by buf, must be >= 16.
 * @result a pointer to a 16-byte byte string encoding a GUID.
 * @result NULL if the GUID pointer was NULL, or the bufsize was too small.
 */
int graph_guid_from_network(graph_guid *guid, char const *buf, size_t bufsize) {
  if (bufsize != 16) return EINVAL;

  guid->guid_a = 0;
  guid->guid_a |= *(unsigned char const *)buf++;
  guid->guid_a <<= 8;
  guid->guid_a |= *(unsigned char const *)buf++;
  guid->guid_a <<= 8;
  guid->guid_a |= *(unsigned char const *)buf++;
  guid->guid_a <<= 8;
  guid->guid_a |= *(unsigned char const *)buf++;
  guid->guid_a <<= 8;
  guid->guid_a |= *(unsigned char const *)buf++;
  guid->guid_a <<= 8;
  guid->guid_a |= *(unsigned char const *)buf++;
  guid->guid_a <<= 8;
  guid->guid_a |= *(unsigned char const *)buf++;
  guid->guid_a <<= 8;
  guid->guid_a |= *(unsigned char const *)buf++;

  guid->guid_a = 0;
  guid->guid_b |= *(unsigned char const *)buf++;
  guid->guid_b <<= 8;
  guid->guid_b |= *(unsigned char const *)buf++;
  guid->guid_b <<= 8;
  guid->guid_b |= *(unsigned char const *)buf++;
  guid->guid_b <<= 8;
  guid->guid_b |= *(unsigned char const *)buf++;
  guid->guid_b <<= 8;
  guid->guid_a |= *(unsigned char const *)buf++;
  guid->guid_b <<= 8;
  guid->guid_b |= *(unsigned char const *)buf++;
  guid->guid_b <<= 8;
  guid->guid_b |= *(unsigned char const *)buf++;
  guid->guid_b <<= 8;
  guid->guid_b |= *(unsigned char const *)buf++;

  return 0;
}

/**
 * @brief Compare two guids for ordering.
 * @param A pointer to a guid
 * @param B pointer to another guid
 * @return a value smaller than, equal to, or greater than zero,
 *  depending on whether a is smaller than, equal to, or greater than b.
 */
int graph_guid_compare(void const *A, void const *B) {
  graph_guid const *const a = A;
  graph_guid const *const b = B;

  /* Compare _b before _a to sort versions of the
   * same object next to each other.
   */

  if (a->guid_b != b->guid_b) return a->guid_b < b->guid_b ? -1 : 1;

  if (a->guid_a != b->guid_a) return a->guid_a < b->guid_a ? -1 : 1;
  return 0;
}

/*  Render a hexadecimal number backwards.
 */
static char *graph_guid_compress_number(char *w, unsigned long long val) {
  while (val != 0) {
    *--w = "0123456789abcdef"[val & 0xF];
    val >>= 4;
  }
  return w;
}

/**
 * @brief Render a GUID as a short(er) string.
 *
 *  Design considerations:
 *
 *  - Not supposed to be the human-visible, but ...
 *
 *  - ... human programmers may see this and, in extreme cases,
 *        read it over the phone or send it in email.
 *
 *  - Long, meaningless GUIDs in URLs are uncool.
 *
 *  - We think lowercase hex looks pretty good.
 *
 *  - We don't want an encoding system that can be mistaken for
 *    words ("http://www.tinyurl.com/goose").  Well, we've still got
 *    0xdeadbeef, but at least that takes some straining.
 *
 *  Procedure for formatting:
 *
 *  - Get the DATABASE ID and the SEQUENCE NUMBER from the GUID.
 *  - EXOR the DATABASE ID with the DEFAULT DATABASE ID (passed in).
 *  - Encode the result as
 *	<length of the database ID in hex digits, as a single hex digit>
 *	<database id in hex digits, no leading zeroes>
 *	<serial number in hex digits, no leading zeroes>
 *
 *  At best, this yields a 1-digit number 1 (+1 NUL-byte terminator).
 *  At most, this yields a 22-digit number (+ 1 NUL-byte terminating).
 *
 *  Numbers from the default database start with a leading 1,
 *  followed by the serial number in hex.  GUIDs numbers in
 *  the 200,000,000 primitive range on the default database
 *  encode as 8-digit hex numbers.
 *
 *  Numbers from neighbours of the default database start with
 *  two-digit prefixes 20...2f.
 *
 *  Numbers from an independent, unnamed database (the worst case)
 *  start with a d and a 12-digit database prefix.
 *
 * @param default_database_id the default ID, configured
 * @param guid pointer to a guid to encode
 * @param buf bytes to use for rendering
 * @param size number of usable bytes, must be >= GRAPH_GUID_SIZE.
 *
 * @return NULL if the buffer is too small.
 * @return a pointer to a string rendering of the GUID.
 */
char const *graph_guid_compress(unsigned long long default_database_id,
                                graph_guid const *guid, char *buf,
                                size_t size) {
  unsigned long long ull;
  char *w = buf, *serial_start;

  if (size < 23) return NULL;

  /*  Render the serial number on the end.
   */
  buf[size - 1] = '\0';
  serial_start =
      graph_guid_compress_number(buf + size - 1, GRAPH_GUID_SERIAL(*guid));

  /*  Render the database number before that.
   */
  ull = GRAPH_GUID_DB(*guid);
  ull ^= default_database_id;
  w = graph_guid_compress_number(serial_start, ull);

  /*  Finally, write the number of bytes it took
   *  to write the database ID, plus one to avoid
   *  being mistaken for leading zeroes.
   */
  w[-1] = "123456789abcdef0"[w - serial_start];
  return w - 1;
}

/**
 * @brief Scan a compressed GUID.
 *
 * @param default_database_id the database ID of the "default" server
 * @param guid pointer to a guid, filled in by the call.
 * @param s pointer to beginning of string representation
 * @param e pointer just after end of string representation
 *
 * @return 0 on success, a nonzero error code on error.
 * @return EILSEQ if this can't possibly be a valid GUID.
 */
int graph_guid_uncompress(unsigned long long default_database_id,
                          graph_guid *guid, char const *s, char const *e) {
  unsigned long long db = 0, serial = 0, dblen;
  int err;

  if (s >= e) return EILSEQ;

  /*  Length of the database ID in digits, plus one.
   */
  err = graph_ull_from_hexstring(&dblen, s, s + 1);
  if (err) return err;
  if (dblen == 0 || dblen > 13 || --dblen > e - s) return EILSEQ;
  s++;

  /*  Database ID.
   */
  if (dblen) {
    err = graph_ull_from_hexstring(&db, s, s + dblen);
    if (err != 0) return err;
    s += dblen;
  }
  db ^= default_database_id;

  /*  Sequence number.
   */
  if (s < e) {
    err = graph_ull_from_hexstring(&serial, s, e);
    if (err != 0) return err;
  }

  /*  Put it all together.
   */
  graph_guid_from_db_serial(guid, db, serial);

  return 0;
}
