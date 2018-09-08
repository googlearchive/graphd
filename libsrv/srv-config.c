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
/*
 * srv-config.c -- srv configuration parser
 *
 *	Read and parse a configuration file for the server
 *	as a whole.
 */

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <syslog.h>
#include <limits.h>

#include <sys/types.h>
#include <sys/stat.h>

#include "srvp.h"

#define IS_TOKEN_PUNCT(x) ((x) == ',' || (x) == '{' || (x) == '}' || (x) == '#')
#define IS_SPACE(x) (isascii(x) && isspace(x))
#define IS_DIGIT(x) (isascii(x) && isdigit(x))
#define IS_LIT(lit, s, e) \
  ((e) - (s) == sizeof(lit) - 1 && !strncasecmp(lit, s, sizeof(lit) - 1))

/* Not counting dashes and underscores,
 * is s...e pretty much lit ?
 *
 * Note: lit is all-lowercase and doesn't contain
 * dashes or underscores.
 */
bool srv_config_is_name(char const *lit, char const *s, char const *e) {
  while (s < e && *lit != '\0')

    if (*s == '-' || *s == '_')
      s++;
    else if ((isascii(*s) ? tolower(*s) : *s) != *lit)
      return false;
    else
      s++, lit++;

  while (s < e && (*s == '-' || *s == '_')) s++;

  return s >= e && *lit == '\0';
}

static void srv_config_empty(srv_config *cf) {
  memset(cf, 0, sizeof *cf);

  cf->cf_shutdown_delay = SRV_SHUTDOWN_DELAY_SECONDS_DEFAULT;
  cf->cf_cpu = 1;
  cf->cf_cm = NULL;
  cf->cf_file = NULL;
  cf->cf_user_id = getuid();
  cf->cf_group_id = getgid();
  cf->cf_app_data = NULL;
  cf->cf_interface_head = NULL;
  cf->cf_interface_tail = &cf->cf_interface_head;

  cf->cf_netlog_level.clc_full = CL_LEVEL_DETAIL;
  cf->cf_netlog_level.clc_trigger = CL_LEVEL_DETAIL;
  cf->cf_netlog_flush = false;

  cf->cf_log_level.clc_full = CL_LEVEL_DETAIL;
  cf->cf_log_level.clc_trigger = CL_LEVEL_OPERATOR_ERROR;
  cf->cf_log_flush = true;

  cf->cf_want_core = true;

  cf->cf_short_timeslice_ms = 10;
  cf->cf_long_timeslice_ms = 100;
  cf->cf_processes = 1;
}

static srv_config *srv_config_alloc(srv_handle *srv, cm_handle *cm_env,
                                    cl_handle *cl) {
  cm_handle *cm;
  srv_config *cf;

  /*  Allocate a private heap for the configuration file,
   *  so we don't have to keep track of individual strings
   *  when free'ing.
   */
  if (!(cm = cm_heap(cm_env))) {
    cl_log(cl, CL_LEVEL_ERROR,
           "srv: failed to allocate heap allocator (how ironic!): %s",
           strerror(errno));
    return NULL;
  }

  cf = cm_malloc(cm, sizeof(srv_config) + srv->srv_app->app_config_size);
  if (!cf) {
    cl_log(cl, CL_LEVEL_ERROR,
           "srv: failed to allocate %lu bytes for "
           "configuration structure: %s",
           (unsigned long)sizeof(srv_config), strerror(errno));
    return NULL;
  }

  srv_config_empty(cf);

  cf->cf_cm = cm;
  cf->cf_app_data = srv->srv_app->app_config_size ? cf + 1 : NULL;
  if (cf->cf_app_data) {
    memset(cf->cf_app_data, 0, srv->srv_app->app_config_size);
    cl_cover(cl);
  }

  return cf;
}

srv_config *srv_config_default(srv_handle *srv, cm_handle *cm, cl_handle *cl) {
  srv_config *cf;

  cf = srv_config_alloc(srv, cm, cl);
  cf->cf_link++;

  cl_cover(srv->srv_cl);
  return cf;
}

/**
 * @brief Read a boolean from a configuration file.
 *
 *  If there's a problem with the number, print a syntax error
 *  message at loglevel CL_LEVEL_OPERATOR_ERROR.
 *
 * @param cf	overall configuration structure
 * @param cl	log through here.
 * @param s	in/out: beginning of yet unparsed text
 * @param e 	end of available text.
 * @param out	Assign the boolean value (1 or 0) to this output parameter.
 *
 * @return 0 on success
 * @return SRV_ERR_SYNTAX on syntax error
 * @return SRV_ERR_SEMANTICS on overflow
 */
int srv_config_read_boolean(srv_config *cf, cl_handle *cl, char **s,
                            char const *e, bool *out) {
  char const *tok_s;
  char const *tok_e;

  (void)srv_config_get_token(s, e, &tok_s, &tok_e);
  if (IS_LIT("true", tok_s, tok_e))
    *out = 1;
  else if (IS_LIT("false", tok_s, tok_e))
    *out = 0;
  else {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "configuration file \"%s\", line %d: expected "
           "one of true or false, got \"%.*s\"\n",
           cf->cf_file, srv_config_line_number(cf, *s), (int)(tok_e - tok_s),
           tok_s);
    return SRV_ERR_SYNTAX;
  }
  return 0;
}

static int srv_config_read_fd(char const *filename, int fd, cm_handle *cm,
                              cl_handle *cl, size_t header_size,
                              void **data_out, size_t *size_out) {
  struct stat st;
  ssize_t r;
  size_t off, n, m;
  char *data;

  *data_out = NULL;
  *size_out = 0;

  if (fstat(fd, &st)) {
    int err = errno;
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "srv: could not fstat configuration file \"%s\": %s", filename,
           strerror(err));

    return err;
  }

  off = header_size;
  n = header_size + st.st_size + 2;
  m = 0;

  data = NULL;

  for (;;) {
    void *tmp;

    if (off >= (n - 1)) {
      n = off + 4 * 1024;
      cl_cover(cl);
    }

    if (n > m) {
      if ((tmp = cm_realloc(cm, data, n)) == NULL) {
        int err = errno;

        if (data != NULL) cm_free(cm, data);

        cl_log(cl, CL_LEVEL_ERROR,
               "srv: failed to allocate "
               "%lu bytes for configuration file "
               "\"%s\": %s\n",
               (unsigned long)n, filename, strerror(err));
        return ENOMEM;
      }
      data = tmp;
      m = n;
    }

    r = read(fd, data + off, (n - 1) - off);
    if (r == 0) {
      cl_cover(cl);
      break;
    }

    if (r < 0) {
      int err = errno;
      cl_log(cl, CL_LEVEL_ERROR,
             "srv: error reading configuration file "
             "\"%s\": %s\n",
             filename, strerror(err));
      return err;
    }
    off += r;
    cl_cover(cl);
  }
  data[off] = '\0';

  *data_out = data;
  *size_out = off;

  return 0;
}

/**
 * @brief What's the name of the configuration file?
 * @param cf	Configuration we're asking about
 * @return the name of the configuration data's file source.
 */
char const *srv_config_file_name(srv_config const *cf) { return cf->cf_file; }

/**
 * @brief What line are we in?
 * @param cf	Configuration we're asking about
 * @param e	The current position of a parser or token.
 * @return The line number of the current line
 */
int srv_config_line_number(srv_config const *cf, char const *e) {
  char const *s;
  size_t n = 1;

  s = cf->cf_file + strlen(cf->cf_file) + 1;
  while (s < e) n += (*s++ == '\n');

  return (int)n;
}

/**
 * @brief Read a token from a string.
 *
 *  The string may be part of the configuration file data.
 *
  * @param s		In/out: beginning of yet-unparsed data
  * @param e		end of available data.
  * @param tok_s	out: beginning of the token.
  * @param tok_e	out: end of the token.  The token
  *			is not \\0-terminated by the call.
  *
  * @return EOF after when running out of tokens
  * @return '"' when returning a string
  * @return 'a' when returning an atom
  * @return x when returning punctuation x
 */
int srv_config_get_token(char **s, char const *e, char const **tok_s,
                         char const **tok_e) {
  char const *r;
  char *w;

  r = w = *s;

  for (;;) {
    while (r < e && IS_SPACE(*r)) r++;
    if (r >= e) goto eof;

    if (*r != '#') break;

    if ((r = memchr(r, '\n', e - r)) == NULL) goto eof;
    r++;
  }

  if (*r == '"') {
    *tok_s = w;
    for (r++; r < e && *r != '"'; *w++ = *r++)
      if (*r == '\\' && r + 1 < e) r++;

    if (w < e) *w = '\0';
    *tok_e = w;
    *s = (char *)r + (r < e);

    return '"';
  } else if (IS_TOKEN_PUNCT(*r)) {
    *tok_s = r;
    *tok_e = r + 1;
    *s = (char *)r + 1;

    return *r;
  }

  *tok_s = r;
  while (r < e && !IS_SPACE(*r) && !IS_TOKEN_PUNCT(*r)) r++;
  *tok_e = r;

  *s = (char *)r;

  return 'a';

eof:
  *tok_s = "EOF";
  *tok_e = *tok_s + 3;
  *s = (char *)e;

  return EOF;
}

static int srv_config_get_expression(char const **s, char const *e,
                                     char const **tok_s, char const **tok_e) {
  char const *p;
  bool in_string = false;
  size_t nparen = 0;

  p = *s;
  while (p < e && IS_SPACE(*p)) p++;
  if (p >= e) return SRV_ERR_NO;

  *tok_s = p;
  for (; p < e; p++) {
    if (*p == '"')
      in_string = !in_string;

    else if (!in_string) {
      if (*p == '(')
        nparen++;
      else if (*p == ')') {
        if (nparen > 0) nparen--;
        if (nparen == 0) {
          *s = *tok_e = p + 1;
          return 0;
        }
      } else if (p < e && nparen == 0 && isspace(*p)) {
        *s = *tok_e = p;
        return 0;
      }
    } else {
      if (*p == '\\' && p + 1 < e) p++;
    }
  }
  *tok_e = *s = p;
  return 0;
}

static cl_loglevel srv_config_read_loglevel_configuration(
    srv_config *cf, cl_facility const *facilities, cl_handle *cl,
    cl_loglevel_configuration *clc, char **s, char const *e) {
  char const *tok_s, *tok_e, *s0 = *s;
  int err;

  cl_cover(cl);

  err = srv_config_get_expression((char const **)s, e, &tok_s, &tok_e);
  if (err) return err;

  err = cl_loglevel_configuration_from_string(tok_s, tok_e, facilities, clc);
  if (err != 0)
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "configuration file \"%s\", line %d: expected "
           "loglevel, got \"%.*s\"\n",
           cf->cf_file, srv_config_line_number(cf, s0), (int)(tok_e - tok_s),
           tok_s);
  return err;
}

static int srv_config_read_logfacility(srv_config *cf, cl_handle *cl, char **s,
                                       char const *e) {
  int tok;
  char const *tok_s, *tok_e;
  int fac = 0;

  cl_cover(cl);
  tok = srv_config_get_token(s, e, &tok_s, &tok_e);

  if (IS_LIT("auth", tok_s, tok_e)) fac = LOG_AUTH;
#ifndef __sun__
  else if (IS_LIT("authpriv", tok_s, tok_e))
    fac = LOG_AUTHPRIV;
#endif
  else if (IS_LIT("cron", tok_s, tok_e))
    fac = LOG_CRON;
  else if (IS_LIT("daemon", tok_s, tok_e))
    fac = LOG_DAEMON;
#ifndef __sun__
  else if (IS_LIT("ftp", tok_s, tok_e))
    fac = LOG_FTP;
#endif
  else if (IS_LIT("kern", tok_s, tok_e))
    fac = LOG_KERN;
  else if (IS_LIT("lpr", tok_s, tok_e))
    fac = LOG_LPR;
  else if (IS_LIT("mail", tok_s, tok_e))
    fac = LOG_MAIL;
  else if (IS_LIT("news", tok_s, tok_e))
    fac = LOG_NEWS;
  else if (IS_LIT("syslog", tok_s, tok_e))
    fac = LOG_SYSLOG;
  else if (IS_LIT("user", tok_s, tok_e))
    fac = LOG_USER;
  else if (IS_LIT("uucp", tok_s, tok_e))
    fac = LOG_UUCP;
  else if (IS_LIT("local0", tok_s, tok_e))
    fac = LOG_LOCAL0;
  else if (IS_LIT("local1", tok_s, tok_e))
    fac = LOG_LOCAL1;
  else if (IS_LIT("local2", tok_s, tok_e))
    fac = LOG_LOCAL2;
  else if (IS_LIT("local3", tok_s, tok_e))
    fac = LOG_LOCAL3;
  else if (IS_LIT("local4", tok_s, tok_e))
    fac = LOG_LOCAL4;
  else if (IS_LIT("local5", tok_s, tok_e))
    fac = LOG_LOCAL5;
  else if (IS_LIT("local6", tok_s, tok_e))
    fac = LOG_LOCAL6;
  else if (IS_LIT("local7", tok_s, tok_e))
    fac = LOG_LOCAL7;
  else
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "configuration file \"%s\", line %d: expected "
           "syslog facility, got \"%.*s\"\n",
           cf->cf_file, srv_config_line_number(cf, *s), (int)(tok_e - tok_s),
           tok_s);
  return fac;
}

/**
 * @brief Read a number from a configuration file.
 *
 *  If there's a problem with the number, print a syntax error
 *  message at loglevel CL_LEVEL_OPERATOR_ERROR.
 *
 * @param cf	overall configuration structure
 * @param cl	log through here.
 * @param what	word for the kind of value we're trying to read.
 * @param s	in/out: beginning of yet unparsed text
 * @param e 	end of available text.
 * @param out	Assign the number to here.
 *
 * @return 0 on success
 * @return SRV_ERR_SYNTAX on syntax error
 * @return SRV_ERR_SEMANTICS on overflow
 */
int srv_config_read_number(srv_config *cf, cl_handle *cl, char const *what,
                           char **s, char const *e, unsigned long long *out) {
  int tok;
  char const *tok_s, *tok_e, *p;
  unsigned long ull;

  tok = srv_config_get_token(s, e, &tok_s, &tok_e);
  if (tok != 'a' || tok_s >= tok_e || !IS_DIGIT(*tok_s)) {
  syntax:
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "configuration file \"%s\", line %d: expected "
           "%s, got \"%.*s\"\n",
           cf->cf_file, srv_config_line_number(cf, *s), what,
           (int)(tok_e - tok_s), tok_s);
    cl_cover(cl);
    return SRV_ERR_SYNTAX;
  }

  ull = 0;
  for (p = tok_s; p < tok_e && IS_DIGIT(*p); p++) {
    unsigned long long old_ull = ull;

    ull = ull * 10;
    ull = ull + (*p - '0');

    if (old_ull > ull) {
      cl_cover(cl);
    overflow:
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "configuration file \"%s\", line %d: "
             "overflow in %s \"%.*s\"\n",
             cf->cf_file, srv_config_line_number(cf, *s), what,
             (int)(tok_e - tok_s), tok_s);
      return SRV_ERR_SEMANTICS;
    }
    cl_cover(cl);
  }
  if (p < tok_e) {
    unsigned long long mul;
    unsigned long long old_ull = ull;

    switch (tolower(*p)) {
      case 'k':
        mul = 1024ull;
        break;
      case 'm':
        mul = 1024ull * 1024;
        break;
      case 'g':
        mul = 1024ull * 1024 * 1024;
        break;
      case 't':
        mul = 1024ull * 1024 * 1024 * 1024;
        break;
      default:
        cl_cover(cl);
        goto syntax;
    }
    cl_cover(cl);
    if (p + 1 != tok_e) {
      cl_cover(cl);
      goto syntax;
    }

    ull *= mul;
    if (old_ull >= ull) {
      cl_cover(cl);
      goto overflow;
    }
    cl_cover(cl);
  }
  *out = ull;
  return 0;
}

/**
 * @brief Read a string from a configuration file.
 *
 *  If there's a problem with the string, print a syntax error
 *  message at loglevel CL_LEVEL_OPERATOR_ERROR.
 *
 * @param cf	overall configuration structure
 * @param cl	log through here.
 * @param what	word for the kind of value we're trying to read.
 * @param s	in/out: beginning of yet unparsed text
 * @param e 	end of available text.
 *
 * @return a pointer to a '\\0'-terminated string on success.
 *	The string is allocated in the configuration file heap
 *	and will be automatically free'd when the configuration
 *	is freed.
 *
 * @return NULL on error
 */
char *srv_config_read_string(srv_config *cf, cl_handle *cl, char const *what,
                             char **s, char const *e) {
  int tok;
  char const *tok_s, *tok_e;
  char *result = NULL;

  tok = srv_config_get_token(s, e, &tok_s, &tok_e);
  if (tok != 'a' && tok != '"') {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "configuration file \"%s\", line %d: expected "
           "%s, got \"%.*s\"\n",
           cf->cf_file, srv_config_line_number(cf, *s), what,
           (int)(tok_e - tok_s), tok_s);
    cl_cover(cl);
  } else if ((result = cm_substr(cf->cf_cm, tok_s, tok_e)) == NULL) {
    cl_log(cl, CL_LEVEL_ERROR,
           "srv: failed to allocate %lu bytes for "
           "%s \"%.*s\" in configuration file \"%s\", line %d",
           (unsigned long)(tok_e - tok_s), what, (int)(tok_e - tok_s), tok_s,
           cf->cf_file, srv_config_line_number(cf, *s));
    cl_cover(cl);
  }
  return result;
}

static int srv_config_read_interface(srv_config *cf, cl_handle *cl, char **s,
                                     char const *e) {
  srv_interface_type const *git;
  srv_interface_config *icf;
  int tok;
  char const *tok_s, *tok_e;
  int err;

  tok = srv_config_get_token(s, e, &tok_s, &tok_e);

  if (tok != 'a' && tok != '"') {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "configuration file \"%s\", line %d: expected "
           "interface address, got \"%.*s\"\n",
           cf->cf_file, srv_config_line_number(cf, *s), (int)(tok_e - tok_s),
           tok_s);
    cl_cover(cl);
    return -1;
  }

  if (!(git = srv_interface_type_match(tok_s, tok_e))) {
    cl_log(cl, CL_LEVEL_FAIL, "unknown interface type \"%.*s\"",
           (int)(tok_e - tok_s), tok_s);
    cl_cover(cl);
    return SRV_ERR_SYNTAX;
  }

  if (!(icf = srv_interface_config_alloc(cf, cl, tok_s, tok_e))) return ENOMEM;
  icf->icf_type = git;

  err = srv_interface_config_read(cf, cl, icf, s, e);
  if (err) {
    cl_cover(cl);
    return err;
  }
  cl_cover(cl);
  srv_interface_config_chain_in(cf, icf);
  return 0;
}

static srv_config_parameter const *srv_config_app_parameter(srv_handle *srv,
                                                            char const *s,
                                                            char const *e) {
  srv_config_parameter const *p;

  if (s < e && (p = srv->srv_app->app_config_parameters))
    for (; p->config_name != NULL; p++)
      if (*p->config_name == *s && strncasecmp(p->config_name, s, e - s) == 0 &&
          p->config_name[e - s] == '\0')
        return p;
  return NULL;
}

/**
 * @brief Read a a configuration file.
 *
 * @param srv	module handle
 * @param filename	pathname of the file to read
 * @param cm_env	allocate a heap here
 * @param cl		log through this.
 * @param config_out	assign configuration data to this.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int srv_config_read(srv_handle *srv, char const *filename, cm_handle *cm_env,
                    cl_handle *cl, srv_config **config_out) {
  cm_handle *cm;
  srv_config *cf;
  int fd;
  size_t filename_n;
  void *data;
  size_t size;
  char *s;
  char const *e, *base;
  int err = 0;
  int tok;
  char const *tok_s, *tok_e;

  *config_out = NULL;

  /*  Allocate a private heap for the configuration file,
   *  so we don't have to keep track of individual strings
   *  when free'ing.
   */
  if (!(cm = cm_heap(cm_env))) {
    cl_log(cl, CL_LEVEL_ERROR,
           "srv: failed to allocate heap allocator "
           "(how ironic!): %s",
           strerror(errno));

    return ENOMEM;
  }

  /*  Read the file into a big buffer.  When allocating it,
   *  leave space for the filename and the config structure
   *  itself.
   */
  if ((fd = open(filename, O_RDONLY)) < 0) {
    err = errno;
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "%s: "
           "failed to open \"%s\" as a configuration "
           "file: %s",
           srv->srv_app->app_name, filename, strerror(err));
    cm_heap_destroy(cm);
    cl_cover(cl);

    return err;
  }

  filename_n = strlen(filename) + 1;

  err = srv_config_read_fd(
      filename, fd, cm, cl,
      sizeof(*cf) + srv->srv_app->app_config_size + filename_n, &data, &size);
  if (close(fd) != 0) {
    cl_log(cl, CL_LEVEL_ERROR,
           "srv: error closing configuration file "
           "\"%s\": %s\n",
           filename, strerror(errno));

    if (!err) err = errno;
    cl_cover(cl);
    goto err;
  }
  if (err) {
    cl_cover(cl);
    goto err;
  }

  /*  From the partially filled heap we just allocated, take
   *  memory for the configuration structure and its filename.
   *  Place <s> and <e> around the configuration file data.
   */
  cf = data;
  srv_config_empty(cf);
  if (srv->srv_app->app_config_size) {
    memset(cf->cf_app_data = (char *)(cf + 1), 0,
           srv->srv_app->app_config_size);
    cl_cover(cl);
  }
  s = (char *)(cf + 1) + srv->srv_app->app_config_size;
  memcpy(s, filename, filename_n);
  cf->cf_file = s;
  cf->cf_cm = cm;

  s += filename_n;
  e = (char const *)data + size;
  base = s;

  /*  Tokenize the file data, and create and parameterize
   *  substructures accordingly.
   */
  err = 0;
  while ((tok = srv_config_get_token(&s, e, &tok_s, &tok_e)) != EOF) {
    srv_config_parameter const *p;

    /*  Does this match any of the application-defined
     *  configuration parameters?  If yes, let the application
     *  handle it, reading more tokens from our stream as
     *  it needs to.
     */
    p = srv_config_app_parameter(srv, tok_s, tok_e);
    if (p != NULL) {
      err =
          (*p->config_read)(srv->srv_app_data, srv, cf->cf_app_data, cf, &s, e);
      if (err) {
        cl_cover(cl);
        goto err;
      }

      cl_cover(cl);
      continue;
    }

    if (tok != 'a') {
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "\"%s\", line %d: expected configuration "
             "parameter name, got \"%.*s\"\n",
             cf->cf_file, srv_config_line_number(cf, s), (int)(tok_e - tok_s),
             tok_s);
      cl_cover(cl);
      err = SRV_ERR_SYNTAX;
      goto err;
    } else if (srv_config_is_name("core", tok_s, tok_e)) {
      err = srv_config_read_boolean(cf, cl, &s, e, &cf->cf_want_core);
      if (err != 0) return err;
    } else if (srv_config_is_name("shorttimeslicems", tok_s, tok_e)) {
      err = srv_config_read_number(cf, cl, "short timeslice milliseconds", &s,
                                   e, &cf->cf_short_timeslice_ms);
      if (err != 0) return err;
    } else if (srv_config_is_name("longtimeslicems", tok_s, tok_e)) {
      err = srv_config_read_number(cf, cl, "long timeslice milliseconds", &s, e,
                                   &cf->cf_long_timeslice_ms);
      if (err != 0) return err;
    } else if (srv_config_is_name("cpu", tok_s, tok_e)) {
      err =
          srv_config_read_number(cf, cl, "cpu identifier", &s, e, &cf->cf_cpu);
      if (cf->cf_cpu > 32) {
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "\"%s\", line %d: cpu id of %llu "
               "invalid, defaulting to 0",
               cf->cf_file, srv_config_line_number(cf, s), cf->cf_cpu);
        cf->cf_cpu = 0;
      }
      if (err) goto err;
    } else if (srv_config_is_name("group", tok_s, tok_e)) {
      char *name = srv_config_read_string(cf, cl, "group name", &s, e);
      err = srv_unixid_name_to_gid(name, &cf->cf_group_id);
      if (err)
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "\"%s\", line %d: can't get Unix "
               "group ID for \"%s\": %s",
               cf->cf_file, srv_config_line_number(cf, s), name, strerror(err));

      cl_assert(cl, cm == cf->cf_cm);
      cm_free(cm, name);
      cl_cover(cl);
      if (err) goto err;
    } else if (srv_config_is_name("listen", tok_s, tok_e)) {
      err = srv_config_read_interface(cf, cl, &s, e);
      cl_cover(cl);
      if (err != 0) goto err;
    } else if (srv_config_is_name("logfacility", tok_s, tok_e)) {
      if (!(cf->cf_log_facility = srv_config_read_logfacility(cf, cl, &s, e))) {
        cl_cover(cl);
        err = SRV_ERR_SYNTAX;
        goto err;
      }
      cl_cover(cl);
    } else if (srv_config_is_name("logfile", tok_s, tok_e)) {
      if (!(cf->cf_log_file =
                srv_config_read_string(cf, cl, "logfile name", &s, e))) {
        cl_cover(cl);
        err = errno ? errno : ENOMEM;
        goto err;
      }
      cl_cover(cl);
    } else if (srv_config_is_name("logident", tok_s, tok_e)) {
      if (!(cf->cf_log_ident =
                srv_config_read_string(cf, cl, "syslog identity", &s, e))) {
        cl_cover(cl);
        err = errno ? errno : ENOMEM;
        goto err;
      }
      cl_cover(cl);
    } else if (srv_config_is_name("logflush", tok_s, tok_e)) {
      bool pol;
      if (srv_config_read_boolean(cf, cl, &s, e, &pol)) {
        cl_cover(cl);
        err = SRV_ERR_SYNTAX;
        goto err;
      }
      cf->cf_log_flush = pol ? CL_FLUSH_ALWAYS : CL_FLUSH_NEVER;
      cl_cover(cl);
    } else if (srv_config_is_name("loglevel", tok_s, tok_e)) {
      if (srv_config_read_loglevel_configuration(
              cf, srv->srv_app->app_facilities, cl, &cf->cf_log_level, &s, e)) {
        cl_cover(cl);
        err = SRV_ERR_SYNTAX;
        goto err;
      }
      cl_cover(cl);
    } else if (srv_config_is_name("netlogfile", tok_s, tok_e)) {
      if (!(cf->cf_netlog_file =
                srv_config_read_string(cf, cl, "logfile name", &s, e))) {
        cl_cover(cl);
        err = errno ? errno : ENOMEM;
        goto err;
      }
      cl_cover(cl);
    } else if (srv_config_is_name("netlogflush", tok_s, tok_e)) {
      bool pol;
      if (srv_config_read_boolean(cf, cl, &s, e, &pol)) {
        cl_cover(cl);
        err = SRV_ERR_SYNTAX;
        goto err;
      }
      cf->cf_netlog_flush = pol ? CL_FLUSH_ALWAYS : CL_FLUSH_NEVER;
      cl_cover(cl);
    } else if (srv_config_is_name("netloglevel", tok_s, tok_e)) {
      if (srv_config_read_loglevel_configuration(
              cf, srv->srv_app->app_facilities, cl, &cf->cf_netlog_level, &s,
              e)) {
        cl_cover(cl);
        err = SRV_ERR_SYNTAX;
        goto err;
      }
      cl_cover(cl);
    } else if (srv_config_is_name("pidfile", tok_s, tok_e)) {
      if (!(cf->cf_pid_file =
                srv_config_read_string(cf, cl, "pid-file name", &s, e))) {
        cl_cover(cl);
        err = errno ? errno : ENOMEM;
        goto err;
      }
      cl_cover(cl);
    } else if (srv_config_is_name("poolmax", tok_s, tok_e)) {
      err = srv_config_read_number(cf, cl, "maximum pool level", &s, e,
                                   &cf->cf_pool_max);
      cl_cover(cl);
      if (err) goto err;
    } else if (srv_config_is_name("poolmin", tok_s, tok_e)) {
      err = srv_config_read_number(cf, cl, "minimum pool level", &s, e,
                                   &cf->cf_pool_min);
      cl_cover(cl);
      if (err) goto err;
    } else if (srv_config_is_name("poolpagesize", tok_s, tok_e)) {
      unsigned long long ull;
      err =
          srv_config_read_number(cf, cl, "pool buffer page size", &s, e, &ull);
      cl_cover(cl);
      if (err) goto err;

      if (ull < SRV_MIN_BUFFER_SIZE) {
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "\"%s\", line %d: pool-page-size %llu "
               "must be at least %d",
               cf->cf_file, srv_config_line_number(cf, s), ull,
               SRV_MIN_BUFFER_SIZE);
        cl_cover(cl);
        err = SRV_ERR_SEMANTICS;
        goto err;
      }

      if (ull >= (size_t)-1 - sizeof(srv_buffer)) {
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "\"%s\", line %d: overflow -- %llu "
               "is too large (stay below %llu)",
               cf->cf_file, srv_config_line_number(cf, s), ull,
               (unsigned long long)((size_t)-1 - sizeof(srv_buffer)));
        cl_cover(cl);
        err = SRV_ERR_SEMANTICS;
        goto err;
      }
      cf->cf_pool_page_size = (size_t)ull;
      cl_cover(cl);
    } else if (srv_config_is_name("processes", tok_s, tok_e)) {
      unsigned long long ull;
      err = srv_config_read_number(cf, cl, "number of processes to spawn", &s,
                                   e, &ull);
      cl_cover(cl);
      if (err) goto err;
      if (ull > SRV_MAX_PROCESS_COUNT) {
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "\"%s\", line %d: number of processes "
               "requested: %llu "
               "must be no more than %d",
               cf->cf_file, srv_config_line_number(cf, s), ull,
               SRV_MAX_PROCESS_COUNT);
        cl_cover(cl);
        err = SRV_ERR_SEMANTICS;
        goto err;
      }
      cf->cf_processes = ull;
      cl_cover(cl);
    } else if (srv_config_is_name("shutdowndelay", tok_s, tok_e)) {
      unsigned long long ull;
      err = srv_config_read_number(cf, cl, "shutdown-delay in seconds", &s, e,
                                   &ull);
      cl_cover(cl);
      if (err != 0) goto err;
      cf->cf_shutdown_delay = (ull > LONG_MAX) ? LONG_MAX : ull;
      cl_cover(cl);
    } else if (srv_config_is_name("smp", tok_s, tok_e)) {
      bool want_smp;
      if (srv_config_read_boolean(cf, cl, &s, e, &want_smp)) {
        cl_cover(cl);
        err = SRV_ERR_SYNTAX;
        goto err;
      }
      if (want_smp) {
        cf->cf_processes = (unsigned long long)sysconf(_SC_NPROCESSORS_ONLN);
      } else {
        cf->cf_processes = 1;
      }
      cl_cover(cl);
    } else if (srv_config_is_name("user", tok_s, tok_e)) {
      char *name = srv_config_read_string(cf, cl, "user name", &s, e);
      err = srv_unixid_name_to_uid(name, &cf->cf_user_id);
      if (err)
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "\"%s\", line %d: can't get Unix "
               "user ID for \"%.*s\": %s",
               cf->cf_file, srv_config_line_number(cf, s), (int)(tok_e - tok_s),
               tok_s, strerror(err));
      cl_assert(cl, cm == cf->cf_cm);
      cm_free(cm, name);
      cl_cover(cl);
      if (err) goto err;
    } else {
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "\"%s\", line %d: expected configuration "
             "parameter name, got \"%.*s\"\n",
             filename, srv_config_line_number(cf, s), (int)(tok_e - tok_s),
             tok_s);
      cl_cover(cl);
      err = SRV_ERR_SYNTAX;
      goto err;
    }
  }

  if (cf->cf_pool_max != 0 && cf->cf_pool_min != 0) {
    if (cf->cf_pool_max < cf->cf_pool_min) {
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "\"%s\": range error: pool minimum (%llu) must "
             "not exceed pool maximum (%llu)",
             filename, (unsigned long long)cf->cf_pool_min,
             (unsigned long long)cf->cf_pool_max);
      cl_cover(cl);
      err = SRV_ERR_SYNTAX;
      goto err;
    }
    cl_cover(cl);
  }

  cl_cover(cl);
  *config_out = cf;
  return 0;

err:
  cm_heap_destroy(cm);
  return err;
}

/**
 * @brief Return the memory manager for a configuration.
 * @param cf	Server configuration whose memory manager we want.
 * @return the memory manager for a configuration.
 */
cm_handle *srv_config_mem(srv_config *cf) { return cf->cf_cm; }

/**
 * @brief Link to a configuration fragment.
 *
 *  Linkcounted configurations are part of an overall system where
 *  configurations can be reloaded, and where parts of configurations
 *  are pointed to by pieces of the system elsewhere.
 *
 *  One day we need to make this work with dynamic reloads.
 *
 * @param cf	Server configuration to link to.
 */
void srv_config_link(srv_config *cf) { cf->cf_link++; }

/**
 * @brief Unlink a configuration fragment.
 *  When the reference count drops to 0, the configuration is destroyed.
 * @param cf	Serverr configuration to link to.
 */
void srv_config_unlink(srv_config *cf) {
  if (cf->cf_link-- <= 1) cm_heap_destroy(cf->cf_cm);
}
