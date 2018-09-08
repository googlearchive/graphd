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

#include <errno.h>

#include "libsrv/srv.h"

/**
 * @file graphd-request-size-max.c
 * @brief Maintain "request-size-max" option.
 *
 *  This is a configuration file only option.
 *  It limits the size of a single request.
 *
 *  Sample usage:  request-size-max 128k
 */

/**
 * @brief Parse an option from the configuration file.  (Method.)
 *
 *  This is a method of the generic libsrv parameter mechanism,
 *  passed in via a srv_config_parameter[] structure declared in graphd.c.
 *
 * @param data		opaque application data handle (i.e., graphd)
 * @param srv 		generic libsrv handle
 * @param config_data	opaque application config data (i.e., graphd_config)
 * @param srv_cf	generic libsrv parameters
 * @param s		in/out: current position in the configuration file
 * @param e		in: end of the buffered configuration file
 *
 * @return 0 on success, a nonzero errno on error.
 */
int graphd_request_size_max_config_read(void* data, srv_handle* srv,
                                        void* config_data, srv_config* srv_cf,
                                        char** s, char const* e) {
  cl_handle* cl = srv_log(srv);
  graphd_config* gcf = config_data;
  unsigned long long n = 0;
  int err;

  err = srv_config_read_number(srv_cf, cl, "maximum request size, in bytes", s,
                               e, &n);
  if (err != 0) {
    cl_cover(cl);
    return err;
  }

  if (n > (size_t)-1) {
    cl_cover(cl);
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "configuration file %s, line %d: request-size-max %llu "
           "exceeds the largest internally representable size "
           "value, %llu",
           srv_config_file_name(srv_cf), srv_config_line_number(srv_cf, e), n,
           (unsigned long long)(size_t)-1);
    return ERANGE;
  }

  gcf->gcf_request_size_max = n;
  return 0;
}

/**
 * @brief Set an option as configured.  (Method.)
 *
 *  This is a method of the generic libsrv parameter mechanism,
 *  passed in via a srv_config_parameter[] structure declared in graphd.c.
 *
 * @param data		opaque application data handle (i.e., graphd)
 * @param srv 		generic libsrv handle
 * @param config_data	opaque application config data (i.e., graphd_config)
 * @param srv_cf	generic libsrv parameters
 *
 * @return 0 on success, a nonzero errno on error.
 */
int graphd_request_size_max_config_open(void* data, srv_handle* srv,
                                        void* config_data, srv_config* srv_cf) {
  graphd_handle* g = data;
  graphd_config* gcf = config_data;
  cl_handle* cl = srv_log(srv);

  cl_assert(cl, g != NULL);
  cl_assert(cl, config_data != NULL);

  cl_cover(cl);
  g->g_request_size_max = gcf->gcf_request_size_max;

  return 0;
}
