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
#ifndef SRV_INTERFACE_H
#define SRV_INTERFACE_H

#include "srv.h"

/**
 * @file srv-interface.h - headers for interface type plugins
 */

/**
 * @brief Interface match method
 *
 * @param s		beginning of interface "url"
 * @param e		end of "url"
 *
 * @return 0 on success, a nonzero error code on failure.
 */
typedef int srv_interface_type_match_callback(char const* s, char const* e);

/**
 * @brief Interface configuration read method
 *
 * @param config	libsrv-global configuration
 * @param cl		log through this
 * @param icf		specific interface configuration to fill
 * @param s		in/out: beginning of unparsed text
 * @param e		end of not-yet-parsed text
 *
 * @return 0 on success, a nonzero error code on failure.
 */
typedef int srv_interface_type_config_read_callback(srv_config* config,
                                                    cl_handle* cl,
                                                    srv_interface_config* icf,
                                                    char** s, char const* e);

/**
 * @brief Interface open method
 *
 * @param config	libsrv-global configuration
 * @param icf		specific interface configuration
 * @param data		out: per-interface runtime data
 *
 * @return 0 on success, a nonzero error code on failure.
 */
typedef int srv_interface_type_open_callback(srv_handle* srv,
                                             srv_interface_config* icf,
                                             void** data);

/**
 * @brief Interface close method
 *
 * @param srv		global libsrv handle
 * @param icf		specific interface configuration
 * @param data		per-interface runtime data, freed by this call.
 */
typedef void srv_interface_type_close_callback(srv_handle* srv,
                                               srv_interface_config* icf,
                                               void* data);

/**
 * @brief Interface connect method
 *
 * @param srv		global libsrv handle
 * @param address	address to connect to
 * @param session_out	assign new session to this.
 */
typedef int srv_interface_type_connect_callback(srv_handle* srv,
                                                char const* address,
                                                srv_session** session_out);

/**
 * @brief Interface type plugin harness.
 */
struct srv_interface_type {
  /**
   * @brief Name of the type, unused.
   */
  char const* sit_type;

  /**
   * @brief Method: claim a URL for this interface type
   */
  srv_interface_type_match_callback* sit_match;

  /**
   * @brief Method: read rest of configuration for an interface
   */
  srv_interface_type_config_read_callback* sit_config_read;

  /**
   * @brief Method: open an interface
   */
  srv_interface_type_open_callback* sit_open;

  /**
   * @brief Method: close an interface
   */
  srv_interface_type_close_callback* sit_close;

  /* @brief Method: open an outgoing socket.
   */
  srv_interface_type_connect_callback* sit_connect;
};

typedef enum {
  SRV_SOCKET_ERR,
  SRV_SOCKET_TCP,
  SRV_SOCKET_LOCAL
} srv_interface_socket_type;

extern const srv_interface_type srv_interface_type_tty[1];
extern const srv_interface_type srv_interface_type_tcp[1];
extern const srv_interface_type srv_interface_type_unix[1];

#endif /* SRV_INTERFACE_H */
