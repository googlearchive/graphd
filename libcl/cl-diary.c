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
#include "libcl/clp.h"

#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>

/**
 * @brief How many bytes we allocate for a diary by default
 */
#define CL_DIARY_DEFAULT_SIZE (1024 * 128)

/**
 * @file cl-diary.c
 * @brief A ring buffer of fixed, limited size that can be queried.
 *
 *  Beyond the usual ring buffer functionality:
 *
 * 	- The system keeps track of boundaries between entries.  When
 *	  a new entry is written, or when the size of the buffer is
 *	  changed, full multiples of entries are thrown out or added.
 *
 *	- The most recently opened entry can be expanded after it
 *	  is created.
 *
 * 	- Each entry has a unique serial number.
 *
 *  The ring buffer can be written to either directly via a
 *  cl_diary_handle,
 *
 *	cl_diary_handle * dia;
 *
 *	dia = cl_handle_create(...);
 *	cl_diary_entry_create(dia, buf, strlen(buf));
 *
 *  or via a cl_handle initialized with cl_diary().
 *
 *	dia = cl_handle_create(...);
 *	cl_diary(&cl, dia);
 *	cl_log(&cl, CL_LEVEL_VERBOSE, "Hello, %s!", "World");
 */

/**
 * @brief Ring buffer handle.
 */
struct cl_diary_handle {
  /**
   * @brief Log and assert through this.
   */
  cl_handle *dia_cl;

  /**
   * @brief The fixed-size memory buffer.
   */
  char *dia_data;

  /**
   * @brief Total number of bytes stored, starting at dia_i.
   *	The last valid byte is at (dia_i + dia_n - 1) % dia_m
   *	if dia_n is greater than 0.
   */
  size_t dia_n;

  /**
   * @brief Total number of bytes allocated, starting at 0.
   */
  size_t dia_m;

  /**
   * @brief Offset of the first valid byte, if dia_n is greater than 0.
   */
  size_t dia_i;

  /**
   * @brief Offset of the four-byte length of the most recently
   * 	written entry.
   */
  size_t dia_this;

  /**
   * @brief Unique serial number of the first entry in the ring buffer.
   */
  unsigned long long dia_first;

  /**
   * @brief Number of entries in the system.
   */
  size_t dia_entries;
};

static unsigned long get4(cl_diary_handle const *d, unsigned long off) {
  unsigned long ul = 0;

  if (off + 4 <= d->dia_m) {
    unsigned char const *ptr;

    ptr = (unsigned char const *)(d->dia_data + off);
    ul = ((unsigned long)ptr[0] << 24) | ((unsigned long)ptr[1] << 16) |
         ((unsigned long)ptr[2] << 8) | ((unsigned long)ptr[3]);
  } else {
    int four = 4;
    while (four--) {
      ul = (ul << 8) | (unsigned char)d->dia_data[off];
      off = (off + 1) % d->dia_m;
    }
  }
  return ul;
}

static void put4(cl_diary_handle *d, size_t off, unsigned long ul) {
  off %= d->dia_m;
  if (off + 4 <= d->dia_m) {
    unsigned char *ptr = (unsigned char *)(d->dia_data + off);

    *ptr++ = 0xFF & (ul >> 24);
    *ptr++ = 0xFF & (ul >> 16);
    *ptr++ = 0xFF & (ul >> 8);
    *ptr++ = 0xFF & ul;
  } else {
    int four = 4;
    while (four--) {
      d->dia_data[off] = (unsigned char)(ul >> (four * 8));
      off = (off + 1) % d->dia_m;
    }
  }
}

/**
 * @brief Create a new diary.
 * @param cl	cl handle to log through if something goes wrong
 */
cl_diary_handle *cl_diary_create(cl_handle *cl) {
  cl_diary_handle *d;

  if ((d = malloc(sizeof(*d))) == NULL) return NULL;
  memset(d, 0, sizeof(*d));

  if ((d->dia_data = malloc(CL_DIARY_DEFAULT_SIZE)) == NULL) {
    free(d);
    return NULL;
  }
  d->dia_cl = cl;
  d->dia_m = CL_DIARY_DEFAULT_SIZE;

  return d;
}

/**
 * @brief Free the least recently written diary entry.
 * @param d 	NULL or the diary
 */
static void cl_diary_entry_delete(cl_diary_handle *d) {
  unsigned long size;

  if (d == NULL || d->dia_n <= 0) return;

  cl_assert(d->dia_cl, d->dia_n >= 4);
  cl_assert(d->dia_cl, d->dia_entries > 0);

  size = get4(d, d->dia_i);
  cl_assert(d->dia_cl, d->dia_n >= 4 + size);

  d->dia_n -= 4 + size;
  d->dia_i = (d->dia_i + 4 + size) % d->dia_m;

  if (d->dia_n == 0) d->dia_i = 0;

  d->dia_entries--;
  d->dia_first++;
}

/**
 * @brief Set the size of a diary.
 *
 * @param d	The diary
 * @param size	Diary's new size.
 *
 * @return 0 on success
 * @return a nonzero error code on allocation error.
 */
int cl_diary_set_size(cl_diary_handle *d, size_t size) {
  int err = 0;
  char *tmp;
  size_t diff;

  if (d == NULL) return EINVAL;

  if (d->dia_m == size) return 0;

  while (d->dia_n > size) cl_diary_entry_delete(d);

  if (size <= 3) {
    free(d->dia_data);
    d->dia_data = NULL;
    d->dia_n = d->dia_m = d->dia_i = 0;

    return 0;
  }
  if (d->dia_n == 0) d->dia_i = 0;

  cl_assert(d->dia_cl, d->dia_n <= size);
  cl_assert(d->dia_cl, d->dia_n <= d->dia_m);
  cl_assert(d->dia_cl, d->dia_i <= d->dia_m);

  if (size > d->dia_m) {
    /* Growing. */
    diff = size - d->dia_m;
  } else {
    /*  Shrinking...
     */
    if (d->dia_i + d->dia_n <= d->dia_m) {
      /* All the data is in one block ...
       */
      if (d->dia_i + d->dia_n > size) {
        /*   .. but that block stretches past the
         *  cut-off point.
         *
         *  Move data to the front so it doesn't
         *  get cut off when we shrink.
         */
        cl_assert(d->dia_cl, d->dia_i > 0);
        cl_assert(d->dia_cl, d->dia_n > 0);

        memmove(d->dia_data, d->dia_data + d->dia_i, d->dia_n);
        d->dia_i = 0;
      }

      tmp = realloc(d->dia_data, size);
      if (tmp == NULL) return errno ? errno : ENOMEM;

      d->dia_data = tmp;
      d->dia_m = size;

      cl_assert(d->dia_cl, d->dia_n <= size);
      cl_assert(d->dia_cl, d->dia_n <= d->dia_m);
      cl_assert(d->dia_cl, d->dia_i <= d->dia_m);

      return 0;
    }

    /* The data is in two blocks.
     */

    diff = d->dia_m - size;

    cl_assert(d->dia_cl, diff < d->dia_m);
    cl_assert(d->dia_cl, diff < d->dia_i);

    /*  Move the top end down.
    *
     *           i    s  m
     *    789-------123456	(before)
     *    789----123456--- 	(after)
     */
    memmove(d->dia_data + d->dia_i - diff, d->dia_data + d->dia_i,
            d->dia_m - d->dia_i);
  }

  if ((tmp = realloc(d->dia_data, size)) == NULL) {
    err = errno ? errno : ENOMEM;

    if (size < d->dia_m)

      /*  We couldn't shrink.  (Huh?)
       *  Move the data back up.
       */
      memmove(d->dia_data + d->dia_i, d->dia_data + d->dia_i - diff,
              d->dia_m - d->dia_i);
    return err;
  }

  if (size < d->dia_m)
    d->dia_i -= diff;
  else {
    /*  Move the top end up.
    *
     *             i    m  size
     *	789----123456xxx
     *  ->  789-------123456
     */
    if (d->dia_i + d->dia_n > d->dia_m) {
      memmove(d->dia_data + d->dia_i + diff, d->dia_data + d->dia_i,
              d->dia_m - d->dia_i);
      d->dia_i += diff;
    }
  }
  d->dia_m = size;

  cl_assert(d->dia_cl, d->dia_n <= d->dia_m);
  cl_assert(d->dia_cl, d->dia_i <= d->dia_m);

  return 0;
}

/**
 * @brief Destroy the diary.
 * @param d 	NULL or the diary to destroy
 */
void cl_diary_destroy(cl_diary_handle *d) {
  if (d != NULL) {
    if (d->dia_m != 0) free(d->dia_data);
    free(d);
  }
}

/**
 * @brief Create a new diary entry.
 *
 * @param d 	NULL or the diary
 * @param s 	first byte of the diary entry
 * @param n	number of bytes pointed to by s.
 */
void cl_diary_entry_create(cl_diary_handle *d, char const *s, size_t n) {
  size_t off, size;

  if (d == NULL || d->dia_m == 0) return;

  /*  Make enough room for (n + 4) bytes.
   */
  while (d->dia_n > 0 && (d->dia_n + n + 4) > d->dia_m)
    cl_diary_entry_delete(d);

  d->dia_this = (d->dia_i + d->dia_n) % d->dia_m;

  /*  If even now we don't have space, return.
   */
  if (d->dia_n + n + 4 > d->dia_m) {
    cl_assert(d->dia_cl, d->dia_n == 0);
    cl_assert(d->dia_cl, d->dia_entries == 0);

    d->dia_first++;
    return;
  }
  d->dia_entries++;
  put4(d, (d->dia_i + d->dia_n) % d->dia_m, (unsigned long)n);
  d->dia_n += 4;

  while (n > 0) {
    off = (d->dia_i + d->dia_n) % d->dia_m;
    size = d->dia_m - off;

    if (size > n) size = n;

    memcpy(d->dia_data + off, s, size);

    n -= size;
    s += size;

    d->dia_n += size;
  }
}

/**
 * @brief Add to the most recently written diary entry.
 *
 * @param d 	NULL or the diary
 * @param s 	first byte of the diary entry
 * @param n	number of bytes pointed to by s.
 *
 * @return 0 on success.
 * @return nonzero error code on error.
 */
void cl_diary_entry_add(cl_diary_handle *d, char const *s, size_t n) {
  size_t size, chunk_size;

  /*  If we don't have a diary, or we don't have any
   *  diary entries, or we wouldn't be doing anything,
   *  just return.
   */
  if (d == NULL || d->dia_n == 0 || n == 0) return;

  /*  Measure the size of the most recently written diary entry.
   */
  size = get4(d, d->dia_this);

  /*  Remove old entries until we have space for n more bytes.
   */
  while (d->dia_n > 0 && d->dia_n + n > d->dia_m) cl_diary_entry_delete(d);

  /*  If that killed even the one last entry we were trying to extend,
   *  we're done.
   */
  if (d->dia_n == 0) {
    d->dia_this = 0;
    return;
  }

  /*  Update the size.
   */
  put4(d, d->dia_this, size + n);

  /*  Append the text.
   */
  while (n > 0) {
    size_t off = (d->dia_i + d->dia_n) % d->dia_m;
    chunk_size = d->dia_m - off;

    if (chunk_size > n) chunk_size = n;

    memcpy(d->dia_data + off, s, chunk_size);

    n -= chunk_size;
    s += chunk_size;

    d->dia_n += chunk_size;
  }
}

/**
 * @brief How many entries are there?
 * @param d	diary to iterate over
 * @return the number of diary entries currently stored.
 */
size_t cl_diary_entries(cl_diary_handle const *d) {
  return d == NULL ? 0 : d->dia_entries;
}

/**
 * @brief Visit the next diary entry.
 *
 * @param d	diary to iterate over
 * @param de	entry buffer, initially zero'd out.
 *
 * @return 0 on success, ENOENT once we run out of entry.
 */
int cl_diary_entry_next(cl_diary_handle const *d, cl_diary_entry *de) {
  if (d == NULL || d->dia_n == 0) return ENOENT;

  if (!de->de_initialized) {
    de->de_next = d->dia_i;
    de->de_last = (d->dia_i + d->dia_n) % d->dia_m;
    de->de_initialized = true;
    de->de_serial = d->dia_first;
  } else {
    if (de->de_next == de->de_last) return ENOENT;

    de->de_serial++;
  }

  de->de_offset = (de->de_next + 4) % d->dia_m;
  de->de_size = get4(d, de->de_next);
  de->de_next = (de->de_next + 4 + de->de_size) % d->dia_m;

  return 0;
}

/**
 * @brief Read bytes from the current diary entry.
 *
 *  Successive calls to cl_diary_entry_read() yield successive
 *  bytes from the current diary entry.
 *
 * @param d	diary to iterate over
 * @param de	entry descriptor used in a recent call to
 *		cl_diary_entry_next()
 * @param buf	Deposit content bytes here
 * @param buf_size	number of available bytes pointed to by buf
 *
 * @return the number of bytes written to buf
 * @return 0 once all available data for an entry has been written.
 */
size_t cl_diary_entry_read(cl_diary_handle const *d, cl_diary_entry *de,
                           char *buf, size_t buf_size) {
  size_t size, chunk_size;

  if (buf_size > de->de_size) buf_size = de->de_size;
  size = buf_size;

  while (size > 0) {
    chunk_size =
        (de->de_offset + size > d->dia_m) ? d->dia_m - de->de_offset : size;

    memcpy(buf, d->dia_data + de->de_offset, chunk_size);

    size -= chunk_size;
    buf += chunk_size;

    de->de_offset = (de->de_offset + chunk_size) % d->dia_m;
    de->de_size -= chunk_size;
  }

  return buf_size;
}

/**
 * @brief How many bytes are there in the current diary entry?
 *
 * @param d	diary to iterate over
 * @param de	entry descriptor used in a recent call to
 *		cl_diary_entry_next()
 *
 * @return the number of bytes in the diary entry.
 */
size_t cl_diary_entry_size(cl_diary_handle const *d, cl_diary_entry const *de) {
  return de->de_size;
}

/**
 * @brief What's the serial number of the current entry?
 *
 * @param d	diary to iterate over
 * @param de	entry descriptor used in a recent call to
 *		cl_diary_entry_next()
 *
 * @return the serial number of the diary entry.
 */
unsigned long long cl_diary_entry_serial(cl_diary_handle const *d,
                                         cl_diary_entry const *de) {
  return de->de_serial;
}

/**
 * @brief How many bytes are there in the current diary entry?
 * @param d	diary we're asking about
 * @return the number of bytes in entries in the diary.
 */
size_t cl_diary_total_size(cl_diary_handle const *d) {
  /*  d->dia_n is the total number of bytes stored in the
   *  ring buffer; subtract the number of bytes used for
   *  administration (4 per entry).
   */
  return d ? d->dia_n - (d->dia_entries * 4) : 0;
}

/**
 * @brief Write diary contents into a stream.
 *
 * @param d	diary handle created with cl_diary_create()
 * @param cl	cl handle to write to.
 * @param lev	loglevel for the resulting messages
 */
void cl_diary_log(cl_diary_handle *d, cl_handle *cl, cl_loglevel lev) {
  cl_diary_entry de;

  if (d == NULL) {
    return;
  }

  /*  Dump the contents of the diary into the specified cl stream.
   *
   *  While we're doing that, mask the diary of that cl stream.
   *  (Chances are, the diary is us or feeds in us, and we don't
   *  want the endless forwarding loop.)
   */
  memset(&de, 0, sizeof(de));
  while (cl_diary_entry_next(d, &de) == 0) {
    char buf[1024 + 1];
    size_t n;

    while ((n = cl_diary_entry_read(d, &de, buf, sizeof(buf) - 1)) > 0) {
      buf[n] = '\0';
      (*cl->cl_write)(cl->cl_write_data, lev, buf);
    }
  }
}

/**
 * @brief Write diary contents into a stream.
 *
 * @param d	diary handle created with cl_diary_create()
 * @param cl	cl handle to write to.
 */
void cl_diary_relog(cl_diary_handle *d, cl_handle *cl) {
  cl_diary_entry de;

  if (d == NULL) return;

  /*  Dump the contents of the diary into the specified cl stream.
   */

  memset(&de, 0, sizeof(de));
  while (cl_diary_entry_next(d, &de) == 0) {
    char b4[4];
    char buf[1024 * 16 + 1];
    size_t n;
    cl_loglevel lev;

    /*  Extract the loglevel from the ringbuffer.
     */
    n = cl_diary_entry_read(d, &de, b4, sizeof b4);
    if (n != 4) break;
    lev = ((cl_loglevel)b4[0] << 24) | ((cl_loglevel)b4[1] << 16) |
          ((cl_loglevel)b4[2] << 8) | (cl_loglevel)b4[3];

    /*  Extract the message, and log it at the loglevel.
     */
    while ((n = cl_diary_entry_read(d, &de, buf, sizeof(buf) - 1)) > 0) {
      buf[n] = '\0';
      (*cl->cl_write)(cl->cl_write_data, lev, buf);
    }
  }
}

/**
 * @brief Truncate (empty out) a diary.
 *
 * @param d	diary handle created with cl_diary_create()
 */
void cl_diary_truncate(cl_diary_handle *d) {
  if (d != NULL) d->dia_n = d->dia_i = 0;
}

static void cl_diary_write_callback(void *callback_data, cl_loglevel lev,
                                    char const *str) {
  cl_diary_handle *d = callback_data;

  if (d != NULL) cl_diary_entry_create(d, str, strlen(str));
}

/*  Default cl-handle abort callback.
 */
static void cl_diary_abort_callback(void *callback_data) {
  cl_diary_handle *d = callback_data;
  cl_diary_entry de;

  if (d == NULL) return;

  /*  Dump the contents of the diary into our underlying cl stream.
   */
  memset(&de, 0, sizeof(de));
  while (cl_diary_entry_next(d, &de) == 0) {
    char buf[1024 + 1];
    size_t n;

    while ((n = cl_diary_entry_read(d, &de, buf, sizeof(buf) - 1)) > 0) {
      buf[n] = '\0';

      (*d->dia_cl->cl_write)(d->dia_cl->cl_write_data, CL_LEVEL_FATAL, buf);
    }
  }
}

/**
 * @brief Create a cl_handle for an in-memory ring buffer.
 *
 * @param out	handle to initialize
 * @param d	diary to write into
 */
void cl_diary(cl_handle *out, cl_diary_handle *d) {
  out->cl_write = cl_diary_write_callback;
  out->cl_write_data = d;

  out->cl_abort = cl_diary_abort_callback;
  out->cl_abort_data = d;
}

/**
 * @brief Given a diary log stream, get the diary handle.
 *
 * @param cl	log stream initialized with cl_diary()
 * @return 	diary_handle
 */
cl_diary_handle *cl_diary_get_handle(cl_handle *cl) { return cl->cl_diary; }

/**
 * @brief Configure the short-term memory ("diary") of a log stream.
 *
 *   This works only with streams that begin their log function
 *   with
 *
 *	CL_DIARY_CHECK(cl, lev, str)
 *
 *   see clp.h for the definition.
 *
 *   When a message _below_ level {trigger} arrives, save it in
 *   the diary.  Do not log it.
 *
 *   When a message _at_or_above_ level {trigger} arrives,
 *	(1) dump the contents of the diary
 *	(2) truncate the diary
 *	(3) then log the message.
 *
 *  (So, our log stream has a "short term memory" that it examines
 *  if it looks for an explanation of something that is going wrong.)
 *
 * @param cl		The log handle to be configured
 * @param diary		a diary handle to which the short-term
 *			memory is saved.
 */
void cl_set_diary(cl_handle *cl, cl_diary_handle *diary) {
  if (cl->cl_diary) cl_diary_destroy(cl->cl_diary);
  cl->cl_diary = diary;
}
