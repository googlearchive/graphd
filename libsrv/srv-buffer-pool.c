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

#include <string.h>
#include <errno.h>

#define GRAPHD_POOL_MIN_SIZE 512

/**
 * @brief How much of our memory pool is available?
 *
 *  Taking the max_level specified at srv_buffer_pool_create() as 100%,
 *  how much of our assigned memory is available?
 *
 *  Because the pool can grow beyond the originally specified maximum,
 *  the returned value can be larger than 100%.
 *
 * @param pool The buffer pool the caller is asking about.
 * @return X, where X : 100 = currently available : max_level.  Larger
 * 	numbers mean less memory is available.
 */
double srv_buffer_pool_available(srv_buffer_pool const *pool) {
  cl_cover(pool->pool_cl);
  return (100.00 * pool->pool_available) / pool->pool_max;
}

/**
 * @brief Release a buffer back into the pool.
 *
 * @param pool	Pool to release the buffer into
 * @param buf  Buffer to release
 */
void srv_buffer_pool_free(srv_handle *srv, srv_buffer_pool *pool,
                          srv_buffer *buf) {
  cl_assert(pool->pool_cl, buf != NULL);
  cl_assert(pool->pool_cl, buf->b_m >= SRV_MIN_BUFFER_SIZE);

  cl_log(pool->pool_cl, CL_LEVEL_DEBUG,
         "buffer pool: free buffer %p with %lu bytes at %p "
         "(i=%lu, n=%lu)",
         (void *)buf, (unsigned long)buf->b_m, (void *)buf->b_s,
         (unsigned long)buf->b_i, (unsigned long)buf->b_n);

  if (pool->pool_available > pool->pool_max) {
    cl_log(pool->pool_cl, CL_LEVEL_DEBUG,
           "buffer pool: high memory level, releasing memory "
           "to the runtime library.");
    srv_buffer_free(buf);
    cl_cover(pool->pool_cl);

    return;
  }

  srv_buffer_reinitialize(buf);
  srv_buffer_queue_append(&pool->pool_q, buf);

  pool->pool_available += buf->b_m;

  if (pool->pool_available > pool->pool_max) {
    if (pool->pool_report != SRV_POOL_REPORT_FULL) {
      cl_log(pool->pool_cl, CL_LEVEL_DETAIL,
             "buffer pool: full.  Level %llu "
             "now > maximum level %llu",
             pool->pool_available, pool->pool_max);
      pool->pool_report = SRV_POOL_REPORT_FULL;
      cl_cover(pool->pool_cl);
    }
  } else if (pool->pool_available >= pool->pool_min) {
    if (pool->pool_report == SRV_POOL_REPORT_LOW) {
      cl_log(pool->pool_cl, CL_LEVEL_DETAIL,
             "buffer pool: ok.  Level %llu "
             "between %llu..%llu, inclusive",
             pool->pool_available, pool->pool_min, pool->pool_max);
      pool->pool_report = SRV_POOL_REPORT_OK;
      cl_cover(pool->pool_cl);
    }
  }

  if (srv->srv_buffer_waiting_head != NULL) srv_request_buffer_wakeup_all(srv);
}

/*  Allocate a buffer from the pool.
 */
srv_buffer *srv_buffer_pool_alloc(srv_buffer_pool *pool) {
  srv_buffer *buf;

  if (!(buf = srv_buffer_queue_remove(&pool->pool_q))) {
    buf = srv_buffer_alloc(pool->pool_cm, pool->pool_cl, pool->pool_size);
    if (!buf) {
      cl_log(pool->pool_cl, CL_LEVEL_ERROR, "buffer pool: out of memory! (%s)",
             strerror(errno));
      cl_cover(pool->pool_cl);
      return NULL;
    }
    cl_cover(pool->pool_cl);
  } else {
    pool->pool_available -= buf->b_m;

    cl_log(pool->pool_cl, CL_LEVEL_SPEW,
           "buffer pool: recycle buffer %p (%lu bytes; "
           "pool: %llu of %llu..%llu)",
           (void *)buf, (unsigned long)pool->pool_size, pool->pool_available,
           pool->pool_min, pool->pool_max);
    cl_cover(pool->pool_cl);
  }

  /*  Report if the pool fill level has dropped below a new threshold.
   */
  if (pool->pool_available < pool->pool_min) {
    if (pool->pool_report != SRV_POOL_REPORT_LOW) {
      cl_log(pool->pool_cl, CL_LEVEL_DETAIL,
             "buffer pool: low.  Level %llu "
             "< minimum %llu",
             pool->pool_available, pool->pool_min);
      pool->pool_report = SRV_POOL_REPORT_LOW;
      cl_cover(pool->pool_cl);
    }
  } else if (pool->pool_available <= pool->pool_max) {
    if (pool->pool_report == SRV_POOL_REPORT_FULL) {
      cl_log(pool->pool_cl, CL_LEVEL_DETAIL,
             "buffer pool: ok.  Level %llu "
             "between %llu..%llu",
             pool->pool_available, pool->pool_min, pool->pool_max);
      pool->pool_report = SRV_POOL_REPORT_OK;
      cl_cover(pool->pool_cl);
    }
  }

  return buf;
}

void srv_buffer_pool_initialize(srv_buffer_pool *pool, cm_handle *cm,
                                cl_handle *cl, unsigned long long min_level,
                                unsigned long long max_level, size_t size) {
  srv_buffer *buf;

  cl_assert(cl, cm);
  cl_assert(cl, min_level <= max_level);
  cl_assert(cl, size > 0);
  cl_assert(cl, pool);

  pool->pool_cl = cl;
  pool->pool_cm = cm;

  pool->pool_min = min_level;
  pool->pool_max = max_level;
  pool->pool_available = 0;
  pool->pool_size = size;

  /*  Round up minimum and maximum to even multiples of size;
   *  adjust zero values to sane values.
   */
  if (pool->pool_size < GRAPHD_POOL_MIN_SIZE) {
    cl_log(cl, CL_LEVEL_DEBUG,
           "buffer pool: adjusting page size "
           "from specified %llu to internal minimum of %lu",
           (unsigned long long)size, (unsigned long)GRAPHD_POOL_MIN_SIZE);

    pool->pool_size = GRAPHD_POOL_MIN_SIZE;
    cl_cover(pool->pool_cl);
  }

  if (pool->pool_min % pool->pool_size) {
    pool->pool_min = ((pool->pool_min / pool->pool_size) + 1) * pool->pool_size;
    cl_log(cl, CL_LEVEL_DEBUG,
           "buffer pool: rounding up minimum "
           "pool size from specified %llu to a multiple of the "
           "page size %llu, %llu",
           min_level, (unsigned long long)pool->pool_size,
           (unsigned long long)pool->pool_min);
    cl_cover(pool->pool_cl);
  }
  if (pool->pool_max < pool->pool_min) {
    cl_log(cl, CL_LEVEL_DEBUG,
           "buffer pool: adjusting maximum "
           "pool size from specified %llu to at least "
           "minimum pool size %llu",
           max_level, (unsigned long long)pool->pool_min);
    pool->pool_max = pool->pool_min;
    cl_cover(pool->pool_cl);
  }
  if (pool->pool_max % pool->pool_size) {
    pool->pool_max = ((pool->pool_max / pool->pool_size) + 1) * pool->pool_size;
    cl_log(cl, CL_LEVEL_DEBUG,
           "buffer pool: rounding up maximum "
           "pool size from specified %llu to a multiple of the "
           "page size %llu, %llu",
           min_level, (unsigned long long)pool->pool_size,
           (unsigned long long)pool->pool_min);
    cl_cover(pool->pool_cl);
  }

  srv_buffer_queue_initialize(&pool->pool_q);

  /* Fill it. */
  while (pool->pool_available < pool->pool_max) {
    buf = srv_buffer_alloc(pool->pool_cm, pool->pool_cl, pool->pool_size);
    if (buf == NULL) {
      cl_log(cl, CL_LEVEL_ERROR,
             "buffer pool: allocation "
             "fails during initialization with %llu bytes "
             "allocated: %s (Boundaries: %llu..%llu)",
             pool->pool_available, strerror(errno), pool->pool_min,
             pool->pool_max);
      break;
    }
    pool->pool_available += pool->pool_size;
    srv_buffer_queue_append(&pool->pool_q, buf);
    cl_cover(pool->pool_cl);
  }

  cl_log(cl, CL_LEVEL_DEBUG,
         "buffer pool: %llu bytes in %lu buffer%s; "
         "low watermark %llu, high %llu; page size %llu",
         pool->pool_available, (unsigned long)pool->pool_q.q_n,
         pool->pool_q.q_n != 1 ? "s" : "", pool->pool_min, pool->pool_max,
         (unsigned long long)pool->pool_size);
}

void srv_buffer_pool_finish(srv_buffer_pool *pool) {
  srv_buffer *buf;

  while ((buf = srv_buffer_queue_remove(&pool->pool_q)) != NULL)
    srv_buffer_free(buf);
  pool->pool_min = pool->pool_max = pool->pool_available = 0;
}
