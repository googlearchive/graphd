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
#include <stddef.h>
#include <string.h>
#include <errno.h>

#include "srvp.h"

srv_buffer *srv_buffer_alloc_loc(cm_handle *cm, cl_handle *cl, size_t size,
                                 char const *file, int line) {
  srv_buffer *buf;

  cl_assert(cl, size > 0);
  cl_assert(cl, cm != NULL);

  buf = (*cm->cm_realloc_loc)(cm, NULL, sizeof(*buf) + size, file, line);

  if (buf == NULL) {
    cl_log(cl, CL_LEVEL_ERROR, "failed to allocate %llu bytes for buffer: %s",
           (unsigned long long)size, strerror(errno));
  } else {
    memset(buf, 0, sizeof(*buf));

    buf->b_s = (char *)(buf + 1);
    buf->b_m = size;
    buf->b_next = NULL;
    buf->b_cm = cm;
    buf->b_cl = cl;
    buf->b_refcount = 0;
    buf->b_pre_callback = NULL;
    buf->b_pre_callback_data = NULL;

    cl_cover(cl);
  }
  return buf;
}

void srv_buffer_link_loc(srv_buffer *buf, char const *file, int line) {
  cl_log(buf->b_cl, CL_LEVEL_VERBOSE, "srv_buffer_link %p %d -> %d [%s:%d]",
         (void *)buf, (int)buf->b_refcount, (int)buf->b_refcount + 1, file,
         line);

  buf->b_refcount++;
}

/*   Returns whether the buffer link count dropped to zero.
 *   Idiom:
 *	if (srv_buffer_unlink(buf))
 *		srv_buffer_free(buf);
 */
bool srv_buffer_unlink_loc(srv_buffer *buf, char const *file, int line) {
  cl_log(buf->b_cl, CL_LEVEL_DEBUG,
         "%s:%d: -- buffer %p [i=%lu, n=%lu, m=%lu] (now %d) --", file, line,
         buf, (unsigned long)buf->b_i, (unsigned long)buf->b_n,
         (unsigned long)buf->b_m, (int)buf->b_refcount - 1);

  cl_assert(buf->b_cl, buf->b_refcount > 0);
  cl_cover(buf->b_cl);

  buf->b_refcount--;
  return buf->b_refcount == 0;
}

void srv_buffer_reinitialize(srv_buffer *buf) {
  buf->b_i = 0;
  buf->b_n = 0;
  buf->b_next = NULL;
  buf->b_refcount = 0;

  if (buf->b_pre_callback_data != NULL)
    cm_free(buf->b_cm, buf->b_pre_callback_data);

  buf->b_pre_callback = NULL;
  buf->b_pre_callback_data = NULL;
}

void srv_buffer_free(srv_buffer *buf) {
  cl_cover(buf->b_cl);

  if (buf->b_pre_callback_data != NULL)
    cm_free(buf->b_cm, buf->b_pre_callback_data);

  if (buf->b_s != (char *)(buf + 1)) cm_free(buf->b_cm, buf->b_s);

  cm_free(buf->b_cm, buf);
}

void srv_buffer_queue_initialize(srv_buffer_queue *raw) {
  raw->q_n = 0;
  raw->q_head = NULL;
  raw->q_tail = &raw->q_head;
}

void srv_buffer_queue_append(srv_buffer_queue *q, srv_buffer *buf) {
  cl_cover(buf->b_cl);

  *q->q_tail = buf;
  buf->b_next = NULL;
  q->q_tail = &buf->b_next;

  q->q_n++;
}

srv_buffer *srv_buffer_queue_remove(srv_buffer_queue *q) {
  srv_buffer *buf;

  if ((buf = q->q_head) != NULL) {
    if ((q->q_head = buf->b_next) == NULL) {
      cl_cover(buf->b_cl);
      q->q_tail = &q->q_head;
    }
    q->q_n--;
  }
  return buf;
}

srv_buffer *srv_buffer_queue_tail(srv_buffer_queue *q) {
  if (q->q_tail == &q->q_head) return NULL;
  return (srv_buffer *)((char *)q->q_tail - offsetof(srv_buffer, b_next));
}

void srv_buffer_check(cl_handle *cl, const srv_buffer *buf) {
  cl_assert(cl, buf != NULL);
  cl_assert(cl, buf->b_cm != NULL);
  cl_assert(cl, buf->b_s != NULL);
  cl_assert(cl, buf->b_m >= SRV_MIN_BUFFER_SIZE);
  cl_assert(cl, buf->b_n <= buf->b_m);
  cl_assert(cl, buf->b_i <= buf->b_n);
}

void srv_buffer_queue_check(cl_handle *cl, const srv_buffer_queue *q) {
  const srv_buffer *buf;

  for (buf = q->q_head; buf != NULL; buf = buf->b_next) {
    cl_assert(cl, buf->b_next != buf);
    cl_assert(cl, buf->b_next != q->q_head);
    cl_assert(cl, !buf->b_next == (q->q_tail == &buf->b_next));

    srv_buffer_check(cl, buf);
  }
}
