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

#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#define IS_SPACE(ch) (isascii(ch) && isspace(ch))
#define IS_DIGIT(ch) (isascii(ch) && isdigit(ch))
#define IS_ALNUM(ch) (isascii(ch) && isalnum(ch))
#define TOLOWER(ch) (isascii(ch) ? tolower(ch) : (ch))

#define IS_LIT(s, e, lit)                     \
  ((s) != NULL && e - s == sizeof(lit) - 1 && \
   !strncasecmp(s, lit, sizeof(lit) - 1))

/**
 * @brief Turn the name of a cost component into an address.
 *
  * Given the name of a cost component (e.g., "tr"),
 *  return the address of the corresponding structure member.
 *
 *  For the time components, the millisecond member is returned
 *  (not the microsecond member).
 *
 * @param rts	 The address of a runtime statistics structure
 * @param name_s	Pointer to first byte of the name
 * @param name_e	Pointer just after last byte of the name
 *
 * @return The address of the named runtime statistics (i.e., cost) component.
 * @return NULL if the name wasn't recognized.
 */
unsigned long long* graphd_cost_to_address(graphd_runtime_statistics* rts,
                                           char const* name_s,
                                           char const* name_e) {
  if (name_e - name_s < 2) return NULL;

  switch (TOLOWER(name_s[0])) {
    case 't':
      switch (TOLOWER(name_s[1])) {
        case 'r':
          return &rts->grts_wall_millis;
        case 's':
          return &rts->grts_system_millis;
        case 'u':
          return &rts->grts_user_millis;
        case 'e':
          return &rts->grts_endtoend_millis;
      }
      return NULL;

    case 'p':
      switch (TOLOWER(name_s[1])) {
        case 'r':
          return &rts->grts_minflt;
        case 'f':
          return &rts->grts_majflt;
      }
      return NULL;

    case 'd':
      switch (TOLOWER(name_s[1])) {
        case 'r':
          return &rts->grts_pdb.rts_primitives_read;
        case 'w':
          return &rts->grts_pdb.rts_primitives_written;
      }
      return NULL;

    case 'v':
      if (TOLOWER(name_s[1]) == 'a') return &rts->grts_values_allocated;
      return NULL;

    case 'i':
      switch (TOLOWER(name_s[1])) {
        case 'w':
          return &rts->grts_pdb.rts_index_elements_written;
        case 'r':
          return &rts->grts_pdb.rts_index_elements_read;
        case 'n':
          return &rts->grts_pdb.rts_index_extents_read;
      }
      return NULL;
  }
  return NULL;
}

/**
 * @brief Parse an single cost option.
 *
 *  If the parse fails (and returns nonzero), a message is
 *  logged via cl at loglevel CL_LEVEL_OPERATOR_ERROR.
 *
 * @param cl		log through here
 * @param grt		fill in this structure
 * @param srv_cf	generic libsrv parameters
 * @param s		in/out: current position in the configuration file
 * @param e		in: end of the buffered configuration file
 * @param tok_s		the current token
 * @param tok_e		end of the current token
 *
 * @return 0 on success, a nonzero errno on error.
 */
static int graphd_cost_config_read_line(cl_handle* cl,
                                        graphd_runtime_statistics* grt,
                                        srv_config* srv_cf, char** s,
                                        char const* e, char const* tok_s,
                                        char const* tok_e) {
  unsigned long long* ptr;

  if (tok_s == NULL) return GRAPHD_ERR_NO;

  if ((ptr = graphd_cost_to_address(grt, tok_s, tok_e)) == NULL) {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "configuration file \"%s\", line %d: \"cost\": "
           "unknown cost abbreviation \"%.*s%s\", "
           "known: tr tu ts te pr pf dr dw ir iw in va fm mm ft mt",
           srv_config_file_name(srv_cf), srv_config_line_number(srv_cf, tok_s),
           tok_e - tok_s >= 80 ? 77 : (int)(tok_e - tok_s), tok_s,
           tok_e - tok_s >= 80 ? "..." : "");
    return GRAPHD_ERR_LEXICAL;
  }

  if (*s < e && **s == '=' &&
      (srv_config_get_token(s, e, &tok_s, &tok_e) != '=' ||
       tok_e != tok_s + 1)) {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "configuration file \"%s\", line %d: \"cost\": "
           "expected \"=\", got \"%.*s%s\"",
           srv_config_file_name(srv_cf), srv_config_line_number(srv_cf, *s),
           tok_e - tok_s >= 80 ? 77 : (int)(tok_e - tok_s), tok_s,
           tok_e - tok_s >= 80 ? "..." : "");
    return GRAPHD_ERR_LEXICAL;
  }

  return srv_config_read_number(srv_cf, cl, "cost", s, e, ptr);
}

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
int graphd_cost_config_read(void* data, srv_handle* srv, void* config_data,
                            srv_config* srv_cf, char** s, char const* e) {
  cl_handle* cl = srv_log(srv);
  graphd_config* gcf = config_data;
  graphd_runtime_statistics* grt = &gcf->gcf_runtime_statistics_allowance;
  int tok;
  char const* tok_s;
  char const* tok_e;
  int err;

  cl_enter(cl, CL_LEVEL_SPEW, "(%.*s)", (int)(s && *s ? e - *s : 4),
           s && *s ? *s : "null");

  cl_assert(cl, data);
  cl_assert(cl, config_data);
  cl_assert(cl, srv_cf);
  cl_assert(cl, gcf);

  if ((tok = srv_config_get_token(s, e, &tok_s, &tok_e)) == '{') {
    while ((tok = srv_config_get_token(s, e, &tok_s, &tok_e)) != '}') {
      err = graphd_cost_config_read_line(cl, grt, srv_cf, s, e, tok_s, tok_e);
      if (err != 0) return err;
    }
  } else {
    char errbuf[200];

    err = graphd_cost_from_string(grt, tok_s, tok_e, errbuf, sizeof errbuf);
    if (err != 0) {
      cl_cover(cl);
      if (*errbuf != '\0')
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "configuration file \"%s\", "
               "line %d: %s",
               srv_config_file_name(srv_cf),
               srv_config_line_number(srv_cf, tok_s), errbuf);
      else
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "configuration file \"%s\", "
               "line %d: syntax error in cost "
               "expression \"%.*s\"",
               srv_config_file_name(srv_cf),
               srv_config_line_number(srv_cf, tok_s), (int)(tok_e - tok_s),
               tok_s);
      return err;
    }
  }

  cl_leave(cl, CL_LEVEL_SPEW, "leave");
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
int graphd_cost_config_open(void* data, srv_handle* srv, void* config_data,
                            srv_config* srv_cf) {
  graphd_handle* graphd = data;
  graphd_config* gcf = config_data;
  cl_handle* cl = srv_log(srv);

  cl_assert(cl, data != NULL);
  cl_assert(cl, config_data != NULL);

  graphd->g_runtime_statistics_allowance =
      gcf->gcf_runtime_statistics_allowance;

  return 0;
}

/**
 * @brief Set the runtime parameter "cost".
 *
 * @param g	graphd handle
 * @param cost		parsed value
 */
void graphd_cost_set(graphd_handle* g, graphd_runtime_statistics const* grt) {
  g->g_runtime_statistics_allowance = *grt;
}

/**
 * @brief Given a string, scan a a single decimal number in a cost.
 *
 *  Leading white space is skipped.
 *  The number is scanned off the beginning of the string.
 *  It's not an error if there's trailing text.
 *  It's an error if the first token at the beginning is not a number.
 *  Overflow is checked for.
 *
 * @param s_ptr		pointer to pointer to beginning of number
 * @param e		pointer just after last byte of the input
 * @param out		assign the parsed number to this.
 *
 * @return 0 on success, a nonzero errno on error.
 * @return ERANGE on overflow
 * @return GRAPHD_ERR_LEXICAL if the number isn't there.
 */
static int graphd_cost_from_string_number(char const** s_ptr, char const* e,
                                          unsigned long long* out) {
  char const* s = *s_ptr;
  unsigned long ull;

  while (s < e && IS_SPACE(*s)) s++;

  if (s >= e || !IS_DIGIT(*s)) return GRAPHD_ERR_LEXICAL;

  ull = 0;
  for (; s < e && IS_DIGIT(*s); s++) {
    unsigned long long old_ull = ull;

    ull = ull * 10;
    ull = ull + (*s - '0');

    if (old_ull > ull) return ERANGE;
  }
  *s_ptr = s;
  *out = ull;

  return 0;
}

static int cost_token(char const** s_ptr, char const* e, char const** tok_s,
                      char const** tok_e) {
  char const* s = *s_ptr;

  while (s < e && IS_SPACE(*s)) s++;
  if (s >= e) return 0;

  *tok_s = s;
  s++;
  if (IS_ALNUM(s[-1]))
    while (s < e && IS_ALNUM(*s)) s++;

  *tok_e = *s_ptr = s;
  return **tok_s;
}

/**
 * @brief Given a string, scan a cost.
 *
 *  Leave unset cost members at maximum.
 *
 * @param rts 		assign outgoing cost to this
 * @param s		pointer to first byte of the string
 * @param e		pointer just after last byte of the string
 * @param errbuf	print error message here on nonzero result
 * @param errsize	number of bytes pointed to by errbuf
 *
 * @return 0 on success, a nonzero errno on error.
 */
int graphd_cost_from_string(graphd_runtime_statistics* rts, char const* s,
                            char const* e, char* errbuf, size_t errsize) {
  int err;
  int tok;
  char const *tok_s, *tok_e;

  graphd_runtime_statistics_max(rts);

  while ((tok = cost_token(&s, e, &tok_s, &tok_e)) != 0) {
    unsigned long long* ptr;

    if ((ptr = graphd_cost_to_address(rts, tok_s, tok_e)) == NULL) {
      snprintf(errbuf, errsize, "don't understand \"%.*s%s\"",
               tok_e - tok_s > 77 ? 77 : (int)(tok_e - tok_s), tok_s,
               tok_e - tok_s > 77 ? "..." : "");

      return GRAPHD_ERR_LEXICAL;
    }

    while (s < e && IS_SPACE(*s)) s++;
    if (s < e && *s == '=') {
      s++;
      while (s < e && IS_SPACE(*s)) s++;
    }

    err = graphd_cost_from_string_number((char const**)&s, e, ptr);
    if (err != 0) {
      snprintf(errbuf, errsize, "%s error in \"%.*s%s\"",
               err == ERANGE ? "overflow" : "syntax",
               tok_e - s > 77 ? 77 : (int)(tok_e - s), s,
               tok_e - s > 77 ? "..." : "");
      return err;
    }
  }
  return 0;
}

/**
 * @brief Turn a current cost limit into a string.
 *
 *  Only cost members that are actually limited are in the string.
 *
 * @param rts 		print this as a string
 * @param buf		bytes to use
 * @param size		number of bytes to use.
 *
 * @return 0 on success, a nonzero errno on error.
 */
char const* graphd_cost_limit_to_string(graphd_runtime_statistics const* rts,
                                        char* buf, size_t size) {
  char* w = buf;
  char const* buf_e = buf + size;

#define SET(ab, member)                                \
  do {                                                 \
    if (rts->member < (unsigned long long)-1 / 2) {    \
      if (buf_e - w < 42) {                            \
        if (buf_e - w > 4) {                           \
          *w++ = '.';                                  \
          *w++ = '.';                                  \
          *w++ = '.';                                  \
        }                                              \
        *w = '\0';                                     \
        return buf;                                    \
      }                                                \
      if (w > buf) *w++ = ' ';                         \
      snprintf(w, buf_e - w, ab "=%llu", rts->member); \
      w += strlen(w);                                  \
    }                                                  \
  } while (0)

  SET("tr", grts_wall_millis);
  SET("ts", grts_system_millis);
  SET("tu", grts_user_millis);
  SET("te", grts_endtoend_millis);

  SET("pr", grts_minflt);
  SET("pf", grts_majflt);

  SET("va", grts_values_allocated);

  SET("dr", grts_pdb.rts_primitives_read);
  SET("dw", grts_pdb.rts_primitives_written);

  SET("iw", grts_pdb.rts_index_elements_written);
  SET("ir", grts_pdb.rts_index_elements_read);
  SET("in", grts_pdb.rts_index_extents_read);

  *w = '\0';

  return buf;
}
