/*
Copyright 2018 Google Inc. All rights reserved.
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

#include <curses.h>
#include <errno.h>
#include <getopt.h>
#include <stdio.h>
#include <string.h>
#include <sysexits.h>

#include "gld.h"

/* gld -- graphdb loader, loads data into the graphdb
 */

#define TOK_IS_LIT(lit, s, e)        \
  (((e) - (s) == sizeof(lit) - 1) && \
   strncasecmp((s), (lit), sizeof(lit) - 1) == 0)

#define IS_SPACE(x) ((x) == ' ' || (x) == '\t')

#define GLD_REQUEST_WINDOW_HIGH 1024
#define GLD_REQUEST_WINDOW_LOW 512

static char const gld_char_class[256] = {
    /*  Language note: the mechanism this code is using is called
     *  "designated initializers".  They were added to C in 1999.
     *  The obvious meaning is the correct one; that is, each expression
     *
     * 	[ x ] = y
     *
     *  initializes the array element with index x to the value y.
     */

    ['\n'] = 1, ['\r'] = 1, [0] = 1,   [' '] = 1, ['\t'] = 1,
    ['('] = 1,  [')'] = 1,  [','] = 1, ['"'] = 1,

    ['<'] = 3,  ['>'] = 3,  ['='] = 3, ['*'] = 3, ['~'] = 3,

    ['-'] = 2};
#define CLASS(ch) (gld_char_class[(unsigned char)(ch)] ^ 1)

typedef struct build_version_reference {
  char const *vr_module;
  char const *vr_version;
} build_version_reference;

static const build_version_reference gld_versions[] = {
    {"graphdb", graphdb_build_version},
    {"graph", graph_build_version},
    {"cm", cm_build_version},
    {"cl", cl_build_version},
    {NULL, NULL},
};

static char const *newest_version(void) {
  build_version_reference const *vr;
  char const *best;

  vr = gld_versions;
  best = vr->vr_version;

  for (vr++; vr->vr_module != NULL; vr++)
    if (strcmp(vr->vr_version, best) > 0) best = vr->vr_version;

  return best;
}

static void list_modules(void) {
  build_version_reference const *vr;
  vr = gld_versions;

  for (; vr->vr_module != NULL; vr++)
    printf("%s:%*s%s\n", vr->vr_module,
           (strlen(vr->vr_module) > 10 ? 0 : (int)(10 - strlen(vr->vr_module))),
           "", vr->vr_version);
  exit(0);
}

static void usage(char const *progname) {
  fprintf(
      stderr,
      "usage: %s options.... [files...] (version 0.1 %.17s)\n"
      "Options:\n"
      "   -a                          print server replies to the query\n"
      "   -h                          print this brief message\n"
      "   -m                          print module versions\n"
      "   -p                          passthrough - just send it as is\n"
      "   -v                          increase verbosity of debug output\n"
      "   -t timeout-in-milliseconds	wait this long to connect or query\n"
      "   -s server-url		connect to <server-url>\n"
      "\n"
      "Language:\n"
      "	(expr)       -- write <expr>\n"
      "	var = (expr) -- write <expr>, assign resulting GUIDs to <var>\n"
      "	$var         -- anywhere in an expression: insert scalar\n"
      "			value of variable <var>\n"
      "	$var.1.2     -- anywhere in an expression: insert value of\n"
      "			second child of first child of <var>\n",
      progname, newest_version());
  exit(EX_USAGE);
}

static int get_token(gld_handle *gld, int *depth, char const **s, char const *e,
                     char const **tok_s, char const **tok_e) {
  char const *p = *s;
  int cl;

  *tok_s = NULL;
  *tok_e = NULL;

  /* Skip leading white space. */
  while (p < e &&
         (IS_SPACE(*p) || (*depth > 0 && ((*p) == '\n' || (*p) == '\r'))))
    p++;
  if (p >= e) {
    *s = e;
    return EOF;
  }

  /* We're looking at a newline? */
  if (*depth == 0 && (*p == '\r' || *p == '\n')) {
    *tok_s = p;

    p++;
    if (p < e && p[-1] == '\r' && *p == '\n') p++;

    *tok_e = *s = p;
    return '\n';
  }

  /* Quote? */
  if (*p == '"') {
    *tok_s = ++p;

    while (p < e && *p != '"')
      if (*p++ == '\\' && p < e) p++;

    *tok_e = p;
    if (p < e) p++;
    *s = p;

    return '"';
  }

  /* Atom. */

  *tok_s = p;
  if ((cl = CLASS(*p)) == 0) {
    *tok_e = *s = p + 1;

    if (*p == '(')
      (*depth)++;
    else if (*p == ')' && (*depth) > 0)
      (*depth)--;
    return (unsigned char)*p;
  }

  while (p < e && (cl &= CLASS(*p)) != 0) p++;
  *tok_e = *s = p;
  return (*tok_s)[0];
}

static int read_request(gld_handle *gld, FILE *fp, char **s, size_t *n,
                        size_t *m) {
  int c;
  size_t depth = 0;
  int in_string = 0;
  int escaped = 0;

  /*  Read lines up to a "\n" outside of a parenthesized
   *  list or a quoted string.
   *
   *  Strings are surrounded by ""; inside strings, \ escape
   *  \ and ".
   */

  *n = 0;
  for (;;) {
    if (*n + 2 >= *m) {
      char *tmp;
      tmp = cm_realloc(gld->gld_cm, *s, *m + 1024);
      if (tmp == NULL) return ENOMEM;

      *s = tmp;
      *m += 1024;
    }

    if ((c = getc(fp)) == EOF) {
      if (*n == 0) return ENOENT;
      if (in_string) {
        fprintf(stderr,
                "EOF in string (in request "
                "\"%.*s\")\n",
                (int)*n, *s);
        return EILSEQ;
      }
      break;
    }
    if (in_string) {
      if (escaped)
        escaped = 0;
      else if (c == '\\')
        escaped = 1;
      else if (c == '"')
        in_string = 0;

      (*s)[(*n)++] = c;
    } else {
      (*s)[(*n)++] = c;
      if (c == '\n' || c == '\r') {
        if (*n == 0) continue;
        if (depth == 0) break;
      }
      if (c == '(') depth++;
      if (c == ')' && depth > 0) depth--;
      if (c == '"') in_string = 1;
    }
  }

  (*s)[(*n)] = '\0';
  return 0;
}

static int subst(gld_handle *gld, char **s, size_t *n, size_t *m,
                 size_t src_off, size_t src_n, char const *dst, size_t dst_n) {
  if (*n + dst_n - src_n >= *m) {
    char *tmp;

    tmp = cm_realloc(gld->gld_cm, *s, *m + dst_n + 1024 - src_n);
    if (tmp == NULL) return ENOMEM;

    *s = tmp;
    *m += dst_n + 1024 - src_n;
  }

  if (src_off + src_n < *n)
    memmove(*s + src_off + dst_n, *s + src_off + src_n, *n - (src_off + src_n));
  if (dst_n > 0) memcpy(*s + src_off, dst, dst_n);

  *n += dst_n;
  *n -= src_n;

  return 0;
}

static int expand(gld_handle *gld, char **s, size_t *n, size_t *m) {
  int err;
  int depth = 0;

  char const *p = *s;
  char const *e = *s + *n;

  int tok;
  char const *tok_s, *tok_e;

  char guid_buf[GRAPH_GUID_SIZE];
  char const *guid_str;
  size_t guid_n;
  graph_guid const *guid;

  size_t p_off;

  while ((tok = get_token(gld, &depth, &p, e, &tok_s, &tok_e)) != EOF) {
    if (tok_s >= tok_e || *tok_s != '$') continue;

    guid = gld_var_lookup(gld, tok_s + 1, tok_e);
    if (guid == NULL) {
      cl_log(gld->gld_cl, CL_LEVEL_DEBUG,
             "gld: can't expand \"%.*s\" (left alone)", (int)(tok_e - tok_s),
             tok_s);
      continue;
    }

    guid_str = graph_guid_to_string(guid, guid_buf, sizeof guid_buf);
    guid_n = strlen(guid_str);

    /*  Make space to replace tok_s...tok_e with <guid>.
     *  This may involve growing.
     */

    p_off = (p - *s) + strlen(guid_str) - (tok_e - tok_s);
    err = subst(gld, s, n, m, tok_s - *s, tok_e - tok_s, guid_str,
                strlen(guid_str));
    if (err != 0) return err;
    p = *s + p_off;
    e = *s + *n;
  }
  return 0;
}

#if 0
static char const result_expr_nested[] = " result=((guid content))";
static char const result_expr_flat[]   = " result=((guid))";

static int write_to_read(gld_handle * gld, char **s, size_t * n, size_t * m)
{
	size_t	i, parens;
	int 	err = 0;
	bool 	in_string = false, escaped = false;
	char 	prev_paren = 0;

	/*  Replace unescaped ) with "result=((guid))"
	 *  if the () container had no subcontainers, and
	 *  with "result=((guid contents))" if it did.
	 */
	for (i = 0; i < *n; i++)
	{
		if (escaped)
		{
			escaped = false;
			continue;
		}

		if (in_string)
		{
			if ((*s)[i] == '"')
				in_string = false;
			else if ((*s)[i] == '\\')
				escaped = true;
			continue;
		}
		if ((*s)[i] == '(')
		{
			prev_paren = '(';
			parens++;
		}
		else if ((*s)[i] == ')')
		{
			err = (prev_paren == ')')
				? subst(gld, s, n, m, i, i,
					result_expr_nested,
					sizeof(result_expr_nested) - 1)
				:  subst(gld, s, n, m, i, i,
					result_expr_flat,
					sizeof(result_expr_flat) - 1);
			prev_paren = ')';
			parens--;
		}
		else if ((*s)[i] == '"')
		{
			in_string = true;
		}
	}
	return 0;
}
#endif

static int process(gld_handle *gld, FILE *fp, char const *name) {
  static char *s = NULL;
  static size_t m = 0;

  size_t n = 0;
  char const *tok_s, *tok_e, *op_s, *op_e;
  int op, tok;
  char const *p;
  char *e;
  int err = 0;
  char server_buf[GRAPHDB_SERVER_NAME_SIZE];
  cm_buffer varbuf;

  cm_buffer_initialize(&varbuf, gld->gld_cm);

  for (;;) {
    int depth = 0;
    gld_request_data *d = NULL;
    char const *var_s = NULL, *var_e = NULL;

    /* read a full request. */
    err = read_request(gld, fp, &s, &n, &m);
    if (err == ENOENT) {
      /* EOF -- nothign left to read. */
      err = 0;
      break;
    }
    if (err != 0) {
      if (err == EILSEQ)
        fprintf(stderr, "%s: syntax error\n", name);
      else
        fprintf(stderr, "%s: request read fails: %s\n", name, strerror(err));
      err = EX_DATAERR;
      break;
    }

    p = s;
    e = s + n;

    tok = get_token(gld, &depth, &p, e, &tok_s, &tok_e);

    /* Comment or empty request */
    if (tok == '#' || tok == '\n') continue;

    if (tok != '(' &&
        (op = get_token(gld, &depth, &p, e, &op_s, &op_e)) == '=') {
      if (op_e - op_s == 1) {
      assign: /* We'll be assigning to variable
               * $tok_s..tok_e
               */

        /*  Make a duplicate of the variable name for later.
         */
        cm_buffer_truncate(&varbuf);
        err = cm_buffer_add_bytes(&varbuf, tok_s, (size_t)(tok_e - tok_s));
        if (err != 0) {
          fprintf(stderr,
                  "%s: variable allocation fails: "
                  "%s\n",
                  name, strerror(errno));
          err = 1;
          break;
        }
        var_s = cm_buffer_memory(&varbuf);
        var_e = cm_buffer_memory_end(&varbuf);

        /* Shift the rest of the line up. */
        memmove(s, p, e - p);
        n -= p - s;
        e -= p - s;
      } else if (op_e - op_s == 2 && op_s[1] == '>') {
        /*  Catch up with the variable value.
         */
        if (gld_var_lookup(gld, tok_s, tok_e) != NULL) {
          /* Skip this request. */
          continue;
        }
        goto assign;
      }
    }

    /* Expand variable references in the request.
     */
    if (!gld->gld_passthrough) {
      if ((err = expand(gld, &s, &n, &m)) != 0) {
        fprintf(stderr, "%s: expand fails: %s\n", name, strerror(errno));
        err = EX_DATAERR;
        break;
      }

      /* If there's no leading "write", "read", "set",
       * "status", etc., add a "write".
       */
      p = s;
      e = s + n;
      tok = get_token(gld, &depth, &p, e, &tok_s, &tok_e);

      if (TOK_IS_LIT("read", tok_s, tok_e)) {
        if (d != NULL) d->d_can_be_empty = true;
      } else if (!TOK_IS_LIT("write", tok_s, tok_e) &&
                 !TOK_IS_LIT("set", tok_s, tok_e) &&
                 !TOK_IS_LIT("status", tok_s, tok_e)) {
        err = subst(gld, &s, &n, &m, 0, 0, "write ", 6);
        if (err) {
          fprintf(stderr, "%s: substitution fails: %s\n", name,
                  strerror(errno));
          err = EX_DATAERR;
          break;
        }
      }
    }

    if (var_s != NULL) {
      d = gld_request_alloc(gld, var_s, var_e);
      if (d == NULL) {
        fprintf(stderr,
                "%s: request allocation fails: "
                "%s\n",
                name, strerror(errno));
        err = 1;
        break;
      }
    }

    /*  Send the total request.
     */
    err = gld_request_send(gld, d, s, s + n);
    if (err != 0) {
      fprintf(
          stderr, "%s: request_send fails: %s\n",
          graphdb_server_name(gld->gld_graphdb, server_buf, sizeof server_buf),
          strerror(err));
      err = EX_DATAERR;
      break;
    }

    if (gld_request_outstanding(gld) > GLD_REQUEST_WINDOW_HIGH) {
      while (gld_request_outstanding(gld) > GLD_REQUEST_WINDOW_LOW) {
        gld_request_wait_any(gld);
      }
    }
  }

  while (gld_request_outstanding(gld) > 0) {
    gld_request_wait_any(gld);
  }

  return err;
}

int main(int argc, char **argv) {
  int opt, err, status = 0;
  char const *progname;
  char const *logfile = NULL;
  char **s_arg = NULL;
  char sbuf[GRAPHDB_SERVER_NAME_SIZE];
  char ebuf[GRAPHDB_ERROR_SIZE];
  int verbose = 0;
  gld_handle gld;

  memset(&gld, 0, sizeof(gld));

  gld.gld_cl = cl_create();
  gld.gld_cm = cm_trace(cm_c());
  gld.gld_timeout = -1;
  gld.gld_print_answers = false;

  if ((progname = strrchr(argv[0], '/')) != NULL)
    progname++;
  else
    progname = argv[0];

  while ((opt = getopt(argc, argv, "ahps:t:v")) != EOF) {
    switch (opt) {
      case 's':
        s_arg = cm_argvadd(gld.gld_cm, s_arg, optarg);
        if (s_arg == NULL) {
          fprintf(stderr,
                  "%s: out of memory while "
                  "parsing command line arguments: %s\n",
                  progname, strerror(errno));
          exit(1);
        }
        break;

      case 't':
        if (sscanf(optarg, "%ld", &gld.gld_timeout) != 1) {
          fprintf(stderr,
                  "%s: expected timeout "
                  "(in milliseconds), got \"%s\"\n",
                  progname, optarg);
          exit(EX_USAGE);
        }
        break;

      case 'm':
        list_modules();
        /* NOTREACHED */

      case 'v':
        verbose++;
        break;

      case 'a':
        gld.gld_print_answers = true;
        break;

      case 'p':
        gld.gld_passthrough = true;
        break;

      case 'h':
      case '?':
        usage(progname);
        break;

      default:
        break;
    }
  }

  /*  Create and parametrize a graphdb handle.
   */
  gld.gld_graphdb = graphdb_create();
  graphdb_set_memory(gld.gld_graphdb, gld.gld_cm);
  graphdb_set_logging(gld.gld_graphdb, gld.gld_cl);

  if (verbose) {
    switch (verbose) {
      case 1:
        verbose = CL_LEVEL_FAIL;
        break;
      case 2:
        verbose = CL_LEVEL_DETAIL;
        break;
      case 3:
        verbose = CL_LEVEL_DEBUG;
        break;
      default:
        verbose = CL_LEVEL_SPEW;
        break;
    }

    cl_set_loglevel_full(gld.gld_cl, verbose);
    graphdb_set_loglevel(gld.gld_graphdb, verbose);
  }
  if (logfile != NULL) cl_file(gld.gld_cl, logfile);

  /* Connect to a server.
   */
  if ((err = graphdb_connect(gld.gld_graphdb, gld.gld_timeout,
                             (char const *const *)s_arg, 0)) != 0) {
    fprintf(stderr, "%s: failed to connect to %s: %s\n", progname,
            graphdb_server_name(gld.gld_graphdb, sbuf, sizeof sbuf),
            graphdb_strerror(err, ebuf, sizeof ebuf));
    graphdb_destroy(gld.gld_graphdb);
    return EX_UNAVAILABLE;
  }

  if (optind >= argc)
    status = process(&gld, stdin, "*stdin*");
  else
    for (; optind < argc; optind++) {
      FILE *fp;
      fp = fopen(argv[optind], "r");
      if (fp == NULL) {
        fprintf(stderr,
                "%s: failed to open \"%s\" "
                "for reading: %s\n",
                progname, argv[optind], strerror(errno));
        status = EX_NOINPUT;
      } else {
        status = process(&gld, fp, argv[optind]);
        fclose(fp);
      }

      if (status) break;
    }

  /* cm_trace_list(gld.gld_cm);
   */
  graphdb_destroy(gld.gld_graphdb);
  return status;
}
