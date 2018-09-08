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

#include <stdio.h>

/**
 * @brief	Get a human-readable server name.
 *
 * The 'server name' is intended for use in error and log messages.
 * \code
 *	char buf[GRPAHDB_SERVER_NAME_SIZE];
 *	err = graphdb_connect(graphdb, 1000, NULL, 0);
 *	if (!err)
 *		printf(stderr, "%s: connected successfully\n",
 *			graphdb_server_name(graphdb, buf, sizeof buf));
 * \endcode
 *
 * @param graphdb	handle created with graphdb_create()
 * @param buf     	buffer that can be used for formatting
 * @param bufsize 	# of bytes pointed to by buffer
 *
 * @return 	a pointer to a string that can be printed to indicate
 *		the server the library was connected to, or was
 *  		trying to connect to at the most recent attempt.
 *		The returned value is always a valid string, even
 *		if the library is not actually connected to anywhere,
 *		or if the arguments are erroneous.
 */

char const* graphdb_server_name(graphdb_handle* graphdb, char* buf,
                                size_t bufsize) {
  if (!GRAPHDB_IS_HANDLE(graphdb)) return "[invalid graphdb handle!]";

  if (graphdb->graphdb_address_current == NULL)
    return (graphdb->graphdb_address_last == NULL)
               ? "[not connected]"
               : graphdb->graphdb_address_last->addr_display_name;

  if (graphdb->graphdb_connected)
    return graphdb->graphdb_address_current->addr_display_name;

  snprintf(buf, bufsize, "[%s]",
           graphdb->graphdb_address_current->addr_display_name);
  return buf;
}
