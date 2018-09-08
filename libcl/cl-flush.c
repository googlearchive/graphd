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
#include <ctype.h>


#define IS_LIT(lit, s, e) \
  (sizeof(lit) - 1 == ((e) - (s)) && strncasecmp(lit, s, sizeof(lit) - 1) == 0)

/**
 * @brief Convert a flush_policy to a string.
 *
 * @param pol 	Flush policy value, CL_FLUSH_...
 * @param buf	Buffer to use while formatting the result.
 * @param size	# of bytes pointed to by buf.
 *
 * @return 	A pointer to a '\\0'-terminated representation
 *		of the flush policy pol.
 */
char const* cl_flush_policy_to_string(cl_flush_policy pol, char* buf,
                                      size_t size) {
  switch (pol) {
    case CL_FLUSH_ALWAYS:
      return "always";

    case CL_FLUSH_NEVER:
      return "never";

    default:
      break;
  }
  snprintf(buf, size, "unexpected flush policy %d", (int)pol);
  return buf;
}

/**
 * @brief Scan a flush policy .
 *
 * @param s 	beginning of the string to scan
 * @param e	end of the string to scan
 * @param out	assign parsed flush policy to here.
 *
 * @return 	0 on success, otherwise EINVAL.
 */
int cl_flush_policy_from_string(char const* s, char const* e,
                                cl_flush_policy* out) {
  if (IS_LIT("never", s, e)) {
    *out = CL_FLUSH_NEVER;
    return 0;
  } else if (IS_LIT("always", s, e)) {
    *out = CL_FLUSH_ALWAYS;
    return 0;
  }
  return EINVAL;
}

/**
 * @brief Get the current flush_policy.
 *
 * @param cl 	log handle whose policy we're curious about.
 * @return 	the flush_policy currently set for the handle.
 */
cl_flush_policy cl_get_flush_policy(cl_handle* cl) { return cl->cl_flush; }

/**
 * @brief Set the current flush_policy.
 *
 * @param cl 	log handle whose level we're setting.
 * @param pol	flush_policy to set
 */
void cl_set_flush_policy(cl_handle* cl, cl_flush_policy pol) {
  cl->cl_flush = pol;
}
