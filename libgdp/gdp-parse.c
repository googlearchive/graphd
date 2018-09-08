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
#include "libgdp/gdp.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>

#include "graphd/graphd.h"

/** The maximum number of lookaheads */
#define GDP_MAX_LOOKAHEAD 4

/**
 * Contains the lookahead tokens.
 */
struct gdp_lookahead {
  gdp_token lah_buf[GDP_MAX_LOOKAHEAD];  ///< Circular buffer
  unsigned int lah_n;                    ///< Number of lookaheads
  unsigned int lah_head;                 ///< Head position in the buffer
  unsigned int lah_tail;                 ///< Tail position in the buffer
};

typedef struct gdp_lookahead gdp_lookahead;

/**
 * A structure representing the internal state of the parser.
 */
struct gdp_context {
  gdp *ctx_parser;             ///< Parser configuration
  gdp_input *ctx_in;           ///< Input specs
  gdp_output *ctx_out;         ///< Output specs
  gdp_lookahead ctx_lah;       ///< Lookahead tokens
  graphd_command ctx_cmd;      ///< The request command
  gdp_conlist_t *ctx_conlist;  ///< Current constraint list
  gdp_modlist_t *ctx_modlist;  ///< Request modifiers

  /** A bitmask keeping track of inward/outward links in the current
   * constraint list. Each bit correspond to a linkage kind (typeguid,
   * right, left, scope). There are @c PDB_LINKAGE_N linkage kinds. */
  unsigned int ctx_linkmap;
};

typedef struct gdp_context gdp_context;

static int parse_constraints(gdp_context *, gdp_conlist_t **);  // forward

/**
 * If `err' is a parse error, then print the given `fmt' string. The error is
 * returned by the function.
 */
static int notify_error(gdp_context *ctx, int err, gdp_token const *tok,
                        char const *fmt, ...) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  bool syserr = false;
  char const *type;
  va_list ap;
  char img[64];
  char buf[256];
  int n;

  if (ctx->ctx_parser->dbglex) fputc('\n', ctx->ctx_parser->dbgf);

  /* a syntax error? */
  switch (err) {
    case 0:
      gdp_bug(ctx->ctx_parser->cl);
    case GDP_ERR_LEXICAL:
      type = "LEXICAL";
      break;
    case GDP_ERR_SEMANTICS:
    case EINVAL:
      type = "SEMANTICS";
      break;
    case GDP_ERR_SYNTAX:
      type = "SYNTAX";
      break;
    case GDP_ERR_TOO_LONG:
      type = "LEXICAL";
      break;
    default:
      type = "SYSTEM";
      syserr = true;
  }

  /* the token's image */
  gdp_token_image(tok, img, sizeof(img));

  /* format error message in buf[] */
  if (tok->tkn_kind == TOK_STR)
    n = snprintf(buf, sizeof(buf), "%s on line %d, column %d: \"%s\": ", type,
                 tok->tkn_row, tok->tkn_col, img);
  else
    n = snprintf(buf, sizeof(buf), "%s on line %d, column %d: '%s': ", type,
                 tok->tkn_row, tok->tkn_col, img);
  if ((n > 0) && ((size_t)n < sizeof(buf))) {
    if (syserr) snprintf(buf + n, sizeof(buf) - n, "%s", strerror(err));
    if (err == GDP_ERR_TOO_LONG) {
      snprintf(buf + n, sizeof(buf) - n, "comment too long");
    } else {
      va_start(ap, fmt);
      vsnprintf(buf + n, sizeof(buf) - n, fmt, ap);
      va_end(ap);
    }
  }

  /* create error request */
  ast->request_new_error(ctx->ctx_out, ctx->ctx_modlist, err, buf);

  //-	/* log error */
  //-	cl_log(ctx->ctx_parser->cl, CL_LEVEL_ERROR, buf);

  return err;
}

/**
 * Return `true' if given request is a `read' or `iterate' request.
 */
static inline bool is_read_request(graphd_command cmd) {
  return (cmd == GRAPHD_REQUEST_READ) || (cmd == GRAPHD_REQUEST_ITERATE);
}

static int lookahead(gdp_context *ctx, unsigned int count, gdp_token *tok) {
  gdp_lookahead *lah = &ctx->ctx_lah;
  int err;

  /* Initialize token as empty.
   */
  tok->tkn_kind = 0;

  if
    unlikely((count == 0) || (count > GDP_MAX_LOOKAHEAD))
        gdp_bug(ctx->ctx_parser->cl);

  /* read tokens from the input stream if necessary */
  while (lah->lah_n < count) {
    /* where to insert the lookahead token */
    gdp_token *cur = &lah->lah_buf[lah->lah_tail];
    /* consume token from input */
    if ((err = gdp_lexer_consume(ctx->ctx_parser, ctx->ctx_in, cur)))
      return notify_error(ctx, err, cur, "invalid token");
    /* advance tail cursor in the circular buffer */
    lah->lah_tail = (lah->lah_tail + 1) % GDP_MAX_LOOKAHEAD;
    lah->lah_n++;
  }

  /* get token */
  *tok = lah->lah_buf[(lah->lah_head + count - 1) % GDP_MAX_LOOKAHEAD];

  return 0;
}

static int next(gdp_context *ctx, gdp_token *tok) {
  gdp_token dummy;
  int err;

  /* anything in the lookahead buffer? */
  if (ctx->ctx_lah.lah_n > 0) {
    gdp_lookahead *const lah = &ctx->ctx_lah;
    /* retrieve token from lookahead buffer */
    if (tok != NULL) *tok = lah->lah_buf[lah->lah_head];
    /* pop element from buffer */
    lah->lah_head = (lah->lah_head + 1) % GDP_MAX_LOOKAHEAD;
    lah->lah_n--;
  }
  /* ...no, then consume token from the input stream */
  else {
    if
      unlikely(tok == NULL) tok = &dummy;
    if ((err = gdp_lexer_consume(ctx->ctx_parser, ctx->ctx_in, tok)))
      return notify_error(ctx, err, tok, "invalid token");
  }

  return 0;
}

/**
 * Match next token to given kind.
 *
 * @param ctx
 *	The parse context.
 * @param kind
 *	What kind of token we are expecting next.
 * @param [out] tok
 *	The next token, on success.
 * @return
 *	Zero if the next() token matches @a kind, otherwise an error code.
 */
static inline int match(gdp_context *ctx, gdp_token_kind kind, gdp_token *tok) {
  gdp_token dummy;
  int err;

  if (tok == NULL) tok = &dummy;
  if ((err = next(ctx, tok))) return err;
  if (tok->tkn_kind != kind) return GDP_ERR_SYNTAX;

  return 0;
}

static inline bool linkmap_test(unsigned int linkmap, unsigned int linkage) {
  return (linkmap & (1 << linkage));
}

static inline void linkmap_set(unsigned int *linkmap, unsigned int linkage) {
  *linkmap |= (1 << linkage);
}

static graphd_command lookup_request(gdp_context const *ctx,
                                     gdp_token const *tok) {
  switch (tolower(*tok->tkn_start)) {
    case 'c':
      if (gdp_token_matches(tok, "crash")) return GRAPHD_REQUEST_CRASH;
      break;
    case 'd':
      if (gdp_token_matches(tok, "dump")) return GRAPHD_REQUEST_DUMP;
      break;
    case 'i':
      if (gdp_token_matches(tok, "iterate")) return GRAPHD_REQUEST_ITERATE;
      if (gdp_token_matches(tok, "islink")) return GRAPHD_REQUEST_ISLINK;
      break;
    case 'r':
      if (gdp_token_matches(tok, "read")) return GRAPHD_REQUEST_READ;
      if (gdp_token_matches(tok, "replica")) return GRAPHD_REQUEST_REPLICA;
      if (gdp_token_matches(tok, "replica-write"))
        return GRAPHD_REQUEST_REPLICA_WRITE;
      if (gdp_token_matches(tok, "restore")) return GRAPHD_REQUEST_RESTORE;
      break;
    case 's':
      if (gdp_token_matches(tok, "set")) return GRAPHD_REQUEST_SET;
      if (gdp_token_matches(tok, "smp")) return GRAPHD_REQUEST_SMP;
      if (gdp_token_matches(tok, "status")) return GRAPHD_REQUEST_STATUS;
      if (gdp_token_matches(tok, "sync")) return GRAPHD_REQUEST_SYNC;
      break;
    case 'v':
      if (gdp_token_matches(tok, "verify")) return GRAPHD_REQUEST_VERIFY;
      break;
    case 'w':
      if (gdp_token_matches(tok, "write")) return GRAPHD_REQUEST_WRITE;
      break;
  }

  return GRAPHD_REQUEST_UNSPECIFIED;
}

static graphd_pattern_type lookup_pattern(gdp_token const *tok) {
  switch (tolower(*tok->tkn_start)) {
    case 'a':
      if (gdp_token_matches(tok, "archival")) return GRAPHD_PATTERN_ARCHIVAL;
      break;
    case 'c':
      if (gdp_token_matches(tok, "contents")) return GRAPHD_PATTERN_CONTENTS;
      if (gdp_token_matches(tok, "count")) return GRAPHD_PATTERN_COUNT;
      if (gdp_token_matches(tok, "cursor")) return GRAPHD_PATTERN_CURSOR;
      break;
    case 'd':
      if (gdp_token_matches(tok, "datatype")) return GRAPHD_PATTERN_DATATYPE;
      break;
    case 'e':
      if (gdp_token_matches(tok, "estimate-count"))
        return GRAPHD_PATTERN_ESTIMATE_COUNT;
      if (gdp_token_matches(tok, "estimate")) return GRAPHD_PATTERN_ESTIMATE;
      break;
    case 'g':
      if (gdp_token_matches(tok, "guid")) return GRAPHD_PATTERN_GUID;
      if (gdp_token_matches(tok, "generation"))
        return GRAPHD_PATTERN_GENERATION;
      break;
    case 'i':
      if (gdp_token_matches(tok, "iterator")) return GRAPHD_PATTERN_ITERATOR;
      break;
    case 'l':
      if (gdp_token_matches(tok, "left")) return GRAPHD_PATTERN_LEFT;
      if (gdp_token_matches(tok, "literal")) return GRAPHD_PATTERN_LITERAL;
      if (gdp_token_matches(tok, "live")) return GRAPHD_PATTERN_LIVE;
      break;
    case 'm':
      if (gdp_token_matches(tok, "meta")) return GRAPHD_PATTERN_META;
      break;
    case 'n':
      if (gdp_token_matches(tok, "name")) return GRAPHD_PATTERN_NAME;
      if (gdp_token_matches(tok, "next")) return GRAPHD_PATTERN_NEXT;
      if (gdp_token_matches(tok, "none")) return GRAPHD_PATTERN_NONE;
      break;
    case 'p':
      if (gdp_token_matches(tok, "prev")) return GRAPHD_PATTERN_PREVIOUS;
      if (gdp_token_matches(tok, "previous")) return GRAPHD_PATTERN_PREVIOUS;
      break;
    case 'r':
      if (gdp_token_matches(tok, "right")) return GRAPHD_PATTERN_RIGHT;
      break;
    case 's':
      if (gdp_token_matches(tok, "scope")) return GRAPHD_PATTERN_SCOPE;
      break;
    case 't':
      if (gdp_token_matches(tok, "timestamp")) return GRAPHD_PATTERN_TIMESTAMP;
      if (gdp_token_matches(tok, "timeout")) return GRAPHD_PATTERN_TIMEOUT;
      if (gdp_token_matches(tok, "type")) return GRAPHD_PATTERN_TYPE;
      if (gdp_token_matches(tok, "typeguid")) return GRAPHD_PATTERN_TYPEGUID;
      break;
    case 'v':
      if (gdp_token_matches(tok, "value"))
        return GRAPHD_PATTERN_VALUE;
      else if (gdp_token_matches(tok, "valuetype"))
        return GRAPHD_PATTERN_VALUETYPE;
      break;
    default:
      break;
  }

  return GRAPHD_PATTERN_UNSPECIFIED;
}

static int lookup_operator(gdp_token const *tok, graphd_operator *op) {
  switch (tok->tkn_kind) {
    case TOK_EQ:
      *op = GRAPHD_OP_EQ;
      break;
    case TOK_NE:
      *op = GRAPHD_OP_NE;
      break;
    case TOK_FE:
      *op = GRAPHD_OP_MATCH;
      break;
    case TOK_LT:
      *op = GRAPHD_OP_LT;
      break;
    case TOK_LE:
      *op = GRAPHD_OP_LE;
      break;
    case TOK_GT:
      *op = GRAPHD_OP_GT;
      break;
    case TOK_GE:
      *op = GRAPHD_OP_GE;
      break;
    default:
      *op = GRAPHD_OP_UNSPECIFIED;
      return EINVAL;
  }

  return 0;
}

/**
 * Add a meta constraint to the current constraint list (in ctx->ctx_conlist).
 */
static int set_meta(gdp_context *ctx,
                    gdp_token const *tok,  // (used to report errors)
                    gdp_meta meta) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;

  /* meta-constraint already present? */
  if (ast->conlist_has_meta(ctx->ctx_out, ctx->ctx_conlist)) goto fail_meta;
  //+	/* following test prevents `<-' and `->' from being intermixed with
  //+	 * explicit linkages in the same constraint (e.g. "(<- .. right=X)") */
  //+	if (linkmap_test(ctx->ctx_linkmap, PDB_LINKAGE_LEFT) ||
  //+	    linkmap_test(ctx->ctx_linkmap, PDB_LINKAGE_RIGHT))
  //+		goto fail_linkage;
  //+	/* outward link already defined? */
  //+	if (ast->conlist_has_linkage(ctx->ctx_conlist))
  //+		goto fail_outward;
  /* ok, set metaconstraint */
  if (ast->conlist_set_meta(ctx->ctx_out, ctx->ctx_conlist, meta))
    gdp_bug(ctx->ctx_parser->cl);

  return 0;

fail_meta:
  return notify_error(ctx, GDP_ERR_SEMANTICS, tok,
                      "a meta constraint has already been defined");
  //+fail_outward:
  //+	return notify_error(ctx, GDP_ERR_SEMANTICS, tok,
  //+		"meta constraint conflicts with an already defined "
  //+		"outward linkage");
  //+fail_linkage:
  //+	return notify_error(ctx, GDP_ERR_SEMANTICS, tok,
  //+		"meta constraint conflicts with an already defined linkage");
}

/**
 * Add a sub-constraint to the current constraint list.
 *
 * The linkage of the sub-constraint is chosen according to the cases below:
 *
 * CASE-1
 *
 * An explicit linkage in the sub-constraint:
 *
 *    ... ( <-left ... )
 *    ... ( <-right ... )
 *
 * CASE-2
 *
 * An implicit linkage through a meta attribute in the sub-constraint:
 *
 *    ... ( -> ... )     // FROM meta
 *    ... ( <- ... )     // TO meta
 *
 * In this case, the linkage of the sub-constraint is set as follows:
 *
 *    ... ( <-left ... )
 *    ... ( <-right ... )
 *
 * CASE-3
 *
 * An implicit linkage through a meta attribute in the current constraint list:
 *
 *    -> ... ( ... )     // FROM meta
 *    <- ... ( ... )     // TO meta
 *
 * In this case, the linkage of the sub-constraint is set as follows:
 *
 *    ... right->( ... )
 *    ... left->( ... )
 */
static int set_subcon(
    gdp_context *ctx,      // current parse context
    gdp_token const *tok,  // token to be used for error messages
    gdp_conlist_t *subcon  // sub-constraint
    ) {
  gdp_output *out = ctx->ctx_out;
  gdp_ast_ops *ast = &out->out_ops;
  gdp_conlist_t *conlist = ctx->ctx_conlist;
  unsigned int linkage;
  unsigned int iam;
  unsigned int my;

  /* (1): the sub-constraint specifies how it is linked to us */
  if (ast->conlist_has_linkage(out, subcon)) {
    /* (nothing to do) */
  }
  /* (2): implicit linkage by a meta attribute in the sub-constraint */
  else if (ast->conlist_has_meta(out, subcon)) {
    switch (ast->conlist_get_meta(out, subcon)) {
      case GDP_META_FROM:
        my = graphd_linkage_make_my(PDB_LINKAGE_LEFT);
        break;
      case GDP_META_TO:
        my = graphd_linkage_make_my(PDB_LINKAGE_RIGHT);
        break;
      default:
        gdp_bug(ctx->ctx_parser->cl);
    }
    if (ast->conlist_set_linkage(ctx->ctx_out, subcon, my))
      gdp_bug(ctx->ctx_parser->cl);
  }
  /* (3): implicit linkage by a meta attribute in the current list */
  else if (ast->conlist_has_meta(out, conlist)) {
    switch (ast->conlist_get_meta(out, conlist)) {
      case GDP_META_FROM:
        linkage = PDB_LINKAGE_RIGHT;
        break;
      case GDP_META_TO:
        linkage = PDB_LINKAGE_LEFT;
        break;
      default:
        gdp_bug(ctx->ctx_parser->cl);
    }
    /* make sure the meta linkage hasn't been already used */
    if (linkmap_test(ctx->ctx_linkmap, linkage)) goto fail_unknown;
    linkmap_set(&ctx->ctx_linkmap, linkage);
    /* set linkage */
    iam = graphd_linkage_make_i_am(linkage);
    if (ast->conlist_set_linkage(ctx->ctx_out, subcon, iam))
      gdp_bug(ctx->ctx_parser->cl);
  } else
    goto fail_unknown;

  /* add the sub-constraint to our list */
  return ast->conlist_add_sublist(ctx->ctx_out, conlist, subcon);

fail_unknown:
  return notify_error(ctx, GDP_ERR_SEMANTICS, tok,
                      "unknown subconstraint linkage");
}

static int set_outward_link(gdp_context *ctx,
                            gdp_token const *larr,  // (for error reporting)
                            gdp_token const *id,    // (for error reporting)
                            unsigned int linkage) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  unsigned int my;

  cl_assert(ctx->ctx_parser->cl, (linkage < PDB_LINKAGE_N));

  /* linkage already defined ? */
  //+	if (ast->conlist_has_meta(ctx->ctx_conlist))
  //+		goto fail_conflict_meta;
  if (ast->conlist_has_linkage(ctx->ctx_out, ctx->ctx_conlist))
    goto fail_conflict_outward;
  if (!is_read_request(ctx->ctx_cmd))
    if (linkmap_test(ctx->ctx_linkmap, linkage)) goto fail_conflict_linkage;
  linkmap_set(&ctx->ctx_linkmap, linkage);

  /* ok, set linkage */
  my = graphd_linkage_make_my(linkage);
  if (ast->conlist_set_linkage(ctx->ctx_out, ctx->ctx_conlist, my))
    gdp_bug(ctx->ctx_parser->cl);

  return 0;

fail_conflict_linkage:
  return notify_error(ctx, GDP_ERR_SEMANTICS, id,
                      "a linkage of the same kind has already been defined");
fail_conflict_outward:
  return notify_error(ctx, GDP_ERR_SEMANTICS, id,
                      "an outward linkage has already been defined");
  //+ fail_conflict_meta:
  //+ 	return notify_error(ctx, GDP_ERR_SEMANTICS, id,
  //+ 		"linkage conflicts with an already defined meta constraint");
}

static int set_inward_link(
    gdp_context *ctx,
    gdp_token const *id,    // for error reporting, if necessary
    gdp_token const *opar,  // for error reporting, if necessary
    gdp_conlist_t *subcon, unsigned int linkage) {
  gdp_output *out = ctx->ctx_out;
  gdp_ast_ops const *ast = &out->out_ops;
  unsigned int iam;

  cl_assert(ctx->ctx_parser->cl, (linkage < PDB_LINKAGE_N));

  /* does the subconstraint define a linkage ? */
  if (ast->conlist_has_linkage(out, subcon) ||
      ast->conlist_has_meta(out, subcon))
    goto fail_redefined;

  /* ok, set linkage */
  iam = graphd_linkage_make_i_am(linkage);
  if (ast->conlist_set_linkage(ctx->ctx_out, subcon, iam))
    gdp_bug(ctx->ctx_parser->cl);

  /* add sub-constraint */
  return set_subcon(ctx, opar, subcon);

fail_redefined:
  return notify_error(ctx, GDP_ERR_SEMANTICS, opar,
                      "ambiguous subconstraint linkage");
}

/**
 * Add value to a GUID set.
 */
static int parse_guidset_add(gdp_context *ctx,
                             gdp_guidset_t *guidset,  // GUID set
                             gdp_token const *tok     // GUID value
                             ) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  graph_guid guid;
  bool null;
  int err;

  null = (tok->tkn_kind == TOK_NULL);

  if (!null && (err = gdp_token_toguid(tok, &guid)))
    return notify_error(ctx, GDP_ERR_SEMANTICS, tok, "invalid GUID value");

  return ast->guidset_add(ctx->ctx_out, guidset, null ? NULL : &guid);
}

/**
 * Perform basic semantic checks on a new GUID set:
 *
 * - An empty set can only be used where null values are allowed.
 *
 * - A set with multiple values can only be used in read-like requests.
 */
static int parse_guidset_check(gdp_context *ctx,
                               gdp_token const *tok,  // for error reporting
                               unsigned int n,  // number of elements in the set
                               bool allow_null  // null values allowed?
                               ) {
  if ((n == 0) && !allow_null)
    return notify_error(ctx, GDP_ERR_SEMANTICS, tok,
                        "empty set not allowed here");
  else if ((n > 1) && !is_read_request(ctx->ctx_cmd))
    return notify_error(
        ctx, GDP_ERR_SEMANTICS, tok,
        "multiple GUID values are only allowed in read requests");

  return 0;
}

/*
 * Guids <-- ( GUID | `(' GUID* `)' )
 */
static int parse_guidset(gdp_context *ctx, gdp_guidset_t **new_set,
                         bool allow_null) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok;
  gdp_token opar;
  gdp_guidset_t *guidset;
  unsigned int count;

  int err;

  /* create the set */
  if ((err = ast->guidset_new(ctx->ctx_out, &guidset))) return err;

  /* parse a guid, or a list of guids */
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_NULL:
      if (!allow_null) goto fail_null;
    case TOK_STR:
    case TOK_ATOM:
      if ((err = parse_guidset_add(ctx, guidset, &tok))) return err;
      break;
    case TOK_OPAR:
      count = 0;  // count the element in the set
      opar = tok;
    again:
      if ((err = next(ctx, &tok))) return err;
      switch (tok.tkn_kind) {
        case TOK_NULL:
          if (!allow_null) goto fail_null;
        case TOK_STR:
        case TOK_ATOM:
          count++;
          /* add guid to the set */
          if ((err = parse_guidset_add(ctx, guidset, &tok))) return err;
          goto again;
        case TOK_CPAR:
          /* apply restrictions to the set based on the number of
           * elements and whether null values are allowed */
          if ((err = parse_guidset_check(ctx, &opar, count, allow_null)))
            return err;
          break;
        default:
          return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                              "expected a GUID value or ')'");
      }
      break;
    default:
      return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                          "expected a GUID value or '('");
  }

  *new_set = guidset;
  return 0;

fail_null:
  return notify_error(ctx, GDP_ERR_SEMANTICS, &tok,
                      "null GUIDs cannot be used in this context");
}

/*
 * Strings <-- ( STR | NULL | `(' (STR | NULL)* `)' )
 */
static int parse_stringset(gdp_context *ctx,
                           bool allow_multi,  // allow multiple values
                           gdp_strset_t **new_strset) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_strset_t *strset;
  gdp_token tok;
  unsigned int count;
  int err;

  /* create empty set of strings */
  if ((err = ast->strset_new(ctx->ctx_out, &strset))) return err;

  /* keep track of the number of strings in the set */
  count = 0;

  // STR | NULL | `(' ...
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_NULL:
      /* NOTE: I know, we should add a `null' element to the set, but
       * the old parser doesn't do so. Notice that `null' is added to
       * the set when the input string is "(null)" (see below) */
      break;
    case TOK_STR:
      /* add string to set */
      if ((err = ast->strset_add(ctx->ctx_out, strset, &tok))) return err;
      break;
    case TOK_OPAR:
    again:
      // ... (STR | NULL)* `)'
      if ((err = next(ctx, &tok))) return err;
      switch (tok.tkn_kind) {
        case TOK_STR:
        case TOK_NULL:
          count++;
          /* multiple values allowed? */
          if ((count > 1) && !allow_multi) goto fail_multi;
          /* add string to set */
          if ((err = ast->strset_add(ctx->ctx_out, strset, &tok))) return err;
          goto again;
        case TOK_CPAR:
          break;
        default:
          goto fail_string_or_CPAR;
      }
      break;
    default:
      goto fail_string_or_OPAR;
  }

  *new_strset = strset;
  return 0;

fail_string_or_CPAR:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected a string, 'null', or ')'");
fail_string_or_OPAR:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok, "expected a string or '('");
fail_multi:
  return notify_error(ctx, GDP_ERR_SEMANTICS, &tok,
                      "multiple values are only allowed in read requests");
}

/**
 * Pattern <-- [ `+' | `-' ] ( ATOM | VAR | `(' Pattern* `)' )
 */
static int parse_pattern(
    gdp_context *ctx,
    gdp_pattern_t *ppat,     // parent pattern (NULL if none)
    gdp_pattern_t **new_pat  // new pattern (NULL if don't care)
    ) {
  gdp_conlist_t *conlist = ctx->ctx_conlist;
  gdp_output *out = ctx->ctx_out;
  gdp_ast_ops const *ast = &out->out_ops;
  gdp_token tok;
  gdp_pattern_t *pat;
  bool fwd;
  graphd_pattern_type kind;
  int err;

  // [ `+' | `-' ]
  if ((err = next(ctx, &tok))) return err;
  fwd = true;
  switch (tok.tkn_kind) {
    case TOK_MINUS:
      fwd = false;
    case TOK_PLUS:
      if ((err = next(ctx, &tok))) return err;
    default:
      break;
  }

  // VAR | ATOM | `(' Pattern* `)'
  switch (tok.tkn_kind) {
    case TOK_VAR:
      kind = GRAPHD_PATTERN_VARIABLE;
      if ((err = ast->pattern_new(out, conlist, &tok, kind, fwd, ppat, &pat)))
        return err;
      break;
    case TOK_ATOM:
      kind = lookup_pattern(&tok);
      if (kind == GRAPHD_PATTERN_UNSPECIFIED)
        goto fail_invalid;
      else if (kind == GRAPHD_PATTERN_LITERAL) {
        if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
        if ((err = next(ctx, &tok))) return err;
        if (tok.tkn_kind != TOK_ATOM && tok.tkn_kind != TOK_STR)
          goto fail_STR_or_ATOM;
      }
      if ((err = ast->pattern_new(out, conlist, &tok, kind, fwd, ppat, &pat)))
        return err;
      break;
    case TOK_OPAR:
      kind = GRAPHD_PATTERN_LIST;
      if ((err = ast->pattern_new(out, conlist, NULL, kind, fwd, ppat, &pat)))
        return err;
      while (1) {
        if ((err = lookahead(ctx, 1, &tok))) return err;
        if (tok.tkn_kind == TOK_CPAR)  // `)'
          break;
        if ((err = parse_pattern(ctx, pat, NULL))) return err;
      }
      next(ctx, NULL);  // the final `)' token
      break;
    default:
      goto fail_pattern;
  }

  if (new_pat != NULL) *new_pat = pat;
  return 0;

fail_pattern:
  return notify_error(
      ctx, GDP_ERR_SYNTAX, &tok,
      ppat ? "expected a pattern or ')'" : "expected a pattern");
fail_STR_or_ATOM:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected a value following 'literal ='");
fail_EQ:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok, "missing '=' after 'literal'");
fail_invalid:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok, "invalid pattern");
}

/**
 * AnyConstraint <-- `any'
 */
static int parse_con_any(gdp_context *ctx) {
  // nothing to do
  return next(ctx, NULL);
}

/**
 * Parse the remaining of the `<-' ... production.
 *
 * See parse_con_arrow().
 */
static int parse_con_arrow_left(
    gdp_context *ctx,
    gdp_token const *larr  // `<-', used for error reporting
    ) {
  const struct {
    char const *img;
    unsigned int value;
  } * l, LINKAGEs[] = {{"left", PDB_LINKAGE_LEFT},
                       {"right", PDB_LINKAGE_RIGHT},
                       {"scope", PDB_LINKAGE_SCOPE},
                       {"typeguid", PDB_LINKAGE_TYPEGUID},
                       {NULL, 0}};

  gdp_token id, op;
  int err;

  // [ `left' | `right' | `scope' | `typeguid' ]
  if ((err = lookahead(ctx, 1, &id))) return err;
  if (id.tkn_kind != TOK_ATOM) goto do_set_meta;
  for (l = LINKAGEs; l->img; l++)
    if (gdp_token_matches(&id, l->img)) break;
  if (l->img == NULL) goto do_set_meta;

  // the action below is pointless, but without it we get a spurious
  // compiler warning (as of GCC 4.1.2)
  memset(&op, 0, sizeof(op));

  /* ok, so we parsed "<- LINKAGE", but maybe we are looking at
   * "<- LINKAGE=VALUE" ? */
  if ((err = lookahead(ctx, 2, &op))) return err;
  switch (op.tkn_kind) {
    case TOK_EQ:
    case TOK_NE:
    case TOK_FE:
    case TOK_LT:
    case TOK_LE:
    case TOK_GT:
    case TOK_GE:
    case TOK_RARR:
      /* ignore LINKAGE, it's part of "LINKAGE=.." */
      goto do_set_meta;
    default:
      break;
  }

  /* we parsed `<- LINKAGE' indeed! */
  next(ctx, NULL);  // LINKAGE
  return set_outward_link(ctx, larr, &id, l->value);

do_set_meta:
  /* set "TO" meta attribute */
  return set_meta(ctx, larr, GDP_META_TO);
}

/**
 * ArrowConstraint <-- `<-' [ `left' | `right' | `scope' | `typeguid' ]
 *                 <-- `->'
 */
static int parse_con_arrow(gdp_context *ctx) {
  gdp_token tok;
  int err;

  // `<-' ... | `->'
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_LARR:
      // [ `left' | `right' | `scope' | `typeguid' ]
      return parse_con_arrow_left(ctx, &tok);
    case TOK_RARR:
      /* set "FROM" meta attribute */
      return set_meta(ctx, &tok, GDP_META_FROM);
    default:
      gdp_bug(ctx->ctx_parser->cl);
  }
}

/**
 * SortComparatorConstraint <-- `sort-comparator' `=' Strings
 */
static int parse_con_sortcomparator(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok;
  int err;

  if ((err = next(ctx, &tok))) return err;

  if ((err = match(ctx, TOK_EQ, &tok))) return err;

  if ((err = ast->conlist_new_sortcomparator(ctx->ctx_out, ctx->ctx_conlist)))
    return err;

  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_NULL:
      return 0;
      break;

    case TOK_STR:

      err =
          ast->conlist_add_sortcomparator(ctx->ctx_out, ctx->ctx_conlist, &tok);
      return err;

    case TOK_OPAR:
      do {
        err = next(ctx, &tok);
        if (err) return err;
        if (tok.tkn_kind == TOK_STR) {
          err = ast->conlist_add_sortcomparator(ctx->ctx_out, ctx->ctx_conlist,
                                                &tok);

          if (err) return err;
        } else if (tok.tkn_kind == TOK_CPAR)
          return 0;
        else {
          return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                              "Malformed comparator list");
        }
      } while (true);
    default:
      return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                          "Malformed comparator list");
  }

  gdp_bug(ctx->ctx_parser->cl);

  return 0;
}

/**
 * ComparatorConstraint <-- `value-comparator' `=' STR
 */
static int parse_con_valuecomparator(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok0;
  gdp_token tok;
  int err;

  // `value-comparator' `=' STR
  if ((err = next(ctx, &tok))) return err;
  tok0 = tok;
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
  if ((err = match(ctx, TOK_STR, &tok))) goto fail_STR;

  if ((err = ast->conlist_add_valuecomparator(ctx->ctx_out, ctx->ctx_conlist,
                                              &tok)))
    goto fail_add;

  return 0;

fail_add:
  return err == EBUSY
             ? notify_error(ctx, GDP_ERR_SEMANTICS, &tok0,
                            "only one value comparator per constraint")
             : notify_error(ctx, err, &tok, "invalid value comparator");
fail_STR:
  return notify_error(ctx, err, &tok, "expected a comparator");
fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
}

/**
 * ComparatorConstraint <-- `comparator' `=' STR
 */
static int parse_con_comparator(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok0;
  gdp_token tok;
  int err;

  // `comparator' `=' STR
  if ((err = next(ctx, &tok))) return err;
  tok0 = tok;
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
  if ((err = match(ctx, TOK_STR, &tok))) goto fail_STR;

  if ((err = ast->conlist_add_comparator(ctx->ctx_out, ctx->ctx_conlist, &tok)))
    goto fail_add;

  return 0;

fail_add:
  return err == EBUSY ? notify_error(ctx, GDP_ERR_SEMANTICS, &tok0,
                                     "only one comparator per constraint")
                      : notify_error(ctx, err, &tok, "invalid comparator");
fail_STR:
  return notify_error(ctx, err, &tok, "expected a comparator");
fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
}

/**
 * CountConstraint <-- `count' ( `<' | `<=' | `=' | `>=' | `>' ) NUM
 *                 <-- `atleast' `=' NUM
 *                 <-- `optional'
 */
static int parse_con_count(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok;
  graphd_operator op;
  unsigned long long ull;
  int err;

  // `atleast' | `count' | `optional'
  if ((err = next(ctx, &tok))) return err;
  if (!is_read_request(ctx->ctx_cmd)) goto fail_reqid;

  switch (tolower(tok.tkn_start[0])) {
    case 'a':
      // `='
      if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
      op = GRAPHD_OP_GE;
      break;
    case 'c':
      // `<' | `<=' | `=' | `>=' | `>'
      if ((err = next(ctx, &tok))) return err;
      if ((err = lookup_operator(&tok, &op))) goto fail_op;
      if ((op == GRAPHD_OP_MATCH) || (op == GRAPHD_OP_NE)) goto fail_op;
      break;
    case 'o':
      ull = 0;
      op = GRAPHD_OP_GE;
      goto optional;
    default:
      gdp_bug(ctx->ctx_parser->cl);
  }

  // NUM
  if ((err = next(ctx, &tok))) return err;
  if ((err = gdp_token_toull(&tok, &ull))) goto fail_num;
optional:
  return ast->conlist_add_count(ctx->ctx_out, ctx->ctx_conlist, op, ull);

fail_num:
  return notify_error(ctx, err, &tok, "expected a numerical value");
fail_op:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected '<', '<=', '=', '>=', or '>'");
fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
fail_reqid:
  return notify_error(ctx, GDP_ERR_SEMANTICS, &tok,
                      "constraint can only be used in read requests");
}

/**
 * CursorConstraint <-- `cursor' `=' ( STR | NULL )
 */
static int parse_con_cursor(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token id;
  gdp_token tok;
  int err;

  // `cursor' `=' ( STR | NULL )
  if ((err = next(ctx, &id))) return err;
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
  if ((err = next(ctx, &tok))) return err;
  if ((tok.tkn_kind != TOK_STR) && (tok.tkn_kind != TOK_NULL))
    goto fail_STR_or_NULL;

  if ((err = ast->conlist_add_cursor(ctx->ctx_out, ctx->ctx_conlist, &tok)))
    goto fail_add;

  return 0;

fail_add:
  return notify_error(ctx, err, &id, "duplicate cursor declaration");
fail_STR_or_NULL:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected 'null' or a cursor value");
fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
}

/**
 * DatelineConstraint <-- `dateline' ( `<' | `>' ) DATELINE
 */
static int parse_con_dateline(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  graphd_operator op;
  gdp_token tok;
  int err;

  // `dateline'
  if ((err = next(ctx, NULL))) return err;

  // `<' | `>'
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_GT:
      op = GRAPHD_OP_GT;
      break;
    case TOK_LT:
      op = GRAPHD_OP_LT;
      break;
    default:
      goto fail_op;
  }

  // DATELINE
  if ((err = next(ctx, &tok))) return err;
  if ((tok.tkn_kind != TOK_STR) && (tok.tkn_kind != TOK_ATOM))
    goto fail_dateline;

  /* create dateline */
  if ((err =
           ast->conlist_add_dateline(ctx->ctx_out, ctx->ctx_conlist, op, &tok)))
    goto fail_dateline;

  return 0;

fail_dateline:
  return notify_error(ctx, err, &tok, "expected a dateline value");
fail_op:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok, "expected '<' or '>'");
}

/**
 * FalseConstraint <-- `false'
 */
static int parse_con_false(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok;
  int err;

  // `false'
  if ((err = next(ctx, &tok))) return err;
  if (!is_read_request(ctx->ctx_cmd))
    return notify_error(ctx, GDP_ERR_SEMANTICS, &tok,
                        "constraint can only be used in read requests");

  return ast->conlist_add_false(ctx->ctx_out, ctx->ctx_conlist);
}

/**
 * Anchor <-- `anchor' [ `=' ( `true' | `false' | `local' )]
 */
static int parse_con_anchor(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token name;
  gdp_token tok;
  graphd_flag_constraint flag;
  int err;

  // `anchor'
  if ((err = next(ctx, &name))) return err;

  if (is_read_request(ctx->ctx_cmd)) {
    return notify_error(ctx, GDP_ERR_SYNTAX, &name,
                        "\"anchor\" flag cannot be used "
                        "in a \"read\" command");
  }

  /*  Optional = assignment (default: true.)
   */
  /*  It's okay for there to
   *  not be a lookahead.
   */
  if ((err = lookahead(ctx, 1, &tok))) return 0;

  if (tok.tkn_kind != TOK_EQ) {
    flag = GRAPHD_FLAG_TRUE;
  } else {
    /*  `='.
     */
    if ((err = next(ctx, &tok)) != 0) return err;

    /*  `true', `false', or `local'
     */
    if ((err = next(ctx, &tok))) return err;

    if (gdp_token_matches(&tok, "true"))
      flag = GRAPHD_FLAG_TRUE;
    else if (gdp_token_matches(&tok, "false"))
      flag = GRAPHD_FLAG_FALSE;
    else if (gdp_token_matches(&tok, "local"))
      flag = GRAPHD_FLAG_TRUE_LOCAL;
    else {
      return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                          "expected 'true' or 'false' with `anchor='");
    }
  }

  /*  Add as a flag constraint.
   */
  if ((err = ast->conlist_add_flag(ctx->ctx_out, ctx->ctx_conlist, &name,
                                   flag))) {
    return notify_error(ctx, err, &name, "anchor flag already specified");
  }
  return 0;
}

/**
 * FlagConstraint <-- ( `live' | `archival' ) `=' ( BOOL | `dontcare' )
 */
static int parse_con_flag(gdp_token const *ctok,  // for error reporting
                          gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token name;
  gdp_token tok;
  graphd_flag_constraint flag;
  int err;

  // `live' | `archival'
  if ((err = next(ctx, &name))) return err;
  // `='
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
  // BOOL | `dontcare'
  if ((err = next(ctx, &tok))) return err;
  if (gdp_token_matches(&tok, "true"))
    flag = GRAPHD_FLAG_TRUE;
  else if (gdp_token_matches(&tok, "false"))
    flag = GRAPHD_FLAG_FALSE;
  else if (gdp_token_matches(&tok, "dontcare"))
    flag = GRAPHD_FLAG_DONTCARE;
  else
    goto fail_ATOM;

  /* add constraint */
  if ((err =
           ast->conlist_add_flag(ctx->ctx_out, ctx->ctx_conlist, &name, flag)))
    goto fail_add;

  return 0;

fail_add:
  return notify_error(ctx, err, ctok, "flag already specified");
fail_ATOM:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected 'true', 'false', or 'dontcare'");
fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
}

/**
 * GenerationConstraint <-- ( `newest' | `oldest' ) GenerationOp NUM
 *
 * GenerationOp <-- `<' | `<=' | `=' | `>=' | `>'
 */
static int parse_con_gen(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok;
  gdp_gencon_kind gen;
  graphd_operator op;
  unsigned long long ull;
  int err;

  // `newest' | `oldest'
  if ((err = next(ctx, &tok))) return err;
  switch (tolower(tok.tkn_start[0])) {
    case 'n':
      gen = GDP_GENCON_NEWEST;
      break;
    case 'o':
      gen = GDP_GENCON_OLDEST;
      break;
    default:
      gdp_bug(ctx->ctx_parser->cl);
  }

  // `<' | `<=' | `=' | `>=' | `>'
  if ((err = next(ctx, &tok))) return err;
  if ((err = lookup_operator(&tok, &op))) goto fail_OP;
  if ((op == GRAPHD_OP_MATCH) || (op == GRAPHD_OP_NE)) goto fail_OP;

  // NUM
  if ((err = next(ctx, &tok))) return err;
  if ((err = gdp_token_toull(&tok, &ull))) goto fail_ATOM;

  return ast->conlist_add_gen(ctx->ctx_out, ctx->ctx_conlist, gen, op, ull);

fail_ATOM:
  return notify_error(ctx, err, &tok, "expected a numerical value");
fail_OP:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected '<' , '<=' , '=' , '>=' , or '>'");
}

/**
 * GuidConstraint <-- GuidKind ( `=' | `~=' | `!=' ) Guids
 *
 * GuidKind <-- `guid' | `next' | `prev' | `previous'
 */
static int parse_con_guid(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_guidset_t *guidset;
  gdp_token tok;
  gdp_guidcon_kind kind;
  graphd_operator op;
  bool allow_null;
  int err;

  // `guid' | `next' | `prev' | `previous'
  if ((err = match(ctx, TOK_ATOM, &tok))) gdp_bug(ctx->ctx_parser->cl);
  switch (tolower(*tok.tkn_start)) {
    case 'g':
      kind = GDP_GUIDCON_THIS;
      break;
    case 'n':
      kind = GDP_GUIDCON_NEXT;
      break;
    case 'p':
      kind = GDP_GUIDCON_PREV;
      break;
    default:
      gdp_bug(ctx->ctx_parser->cl);
  }

  allow_null = (kind != GDP_GUIDCON_THIS);

  // `=' | `~=' | `!='
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_EQ:
      op = GRAPHD_OP_EQ;
      break;
    case TOK_FE:
      op = GRAPHD_OP_MATCH;
      break;
    case TOK_NE:
      op = GRAPHD_OP_NE;
      break;
    default:
      return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                          "expected '=', '!=', or '~='");
  }
  // Guids
  if ((err = parse_guidset(ctx, &guidset, allow_null))) return err;

  /* create GUID constraint */
  return ast->conlist_add_guid(ctx->ctx_out, ctx->ctx_conlist, kind, op,
                               guidset);
}

/**
 * KeyConstraint <-- `key' `=' Pattern
 */
static int parse_con_key(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_pattern_t *pat;
  gdp_token tok;
  int err;

  // `key' `=' Pattern
  if ((err = next(ctx, NULL))) return err;
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
  if ((err = parse_pattern(ctx, NULL, &pat))) return err;

  /* add constraint */
  if ((err = ast->conlist_add_key(ctx->ctx_out, ctx->ctx_conlist, pat)))
    goto fail_key;

  return 0;

fail_key:
  return notify_error(ctx, err, &tok, "invalid key value");
fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
}

/**
 * LinkageConstraint <-- Linkage `->' `(' Constraints `)'
 *                   <-- Linkage ( `=' | `~=' | `!=' ) Guids
 *
 * Linkage <-- `left' | `right' | `scope' | `typeguid'
 */
static int parse_con_linkage(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_guidset_t *guidset = NULL;
  gdp_conlist_t *subcon;
  gdp_token tok;
  gdp_token id;
  gdp_token opar;
  graphd_operator op;
  unsigned int linkage;
  int err;

  /*
   * NOTE: We assume that the first token of the production has already
   * been determined to be `left', `right', `scope', or `typeguid'.
   */

  // `left' | `right' | `scope' | `typeguid'
  if ((err = match(ctx, TOK_ATOM, &tok))) gdp_bug(ctx->ctx_parser->cl);
  switch (tolower(*tok.tkn_start)) {
    case 'l':
      linkage = PDB_LINKAGE_LEFT;
      break;
    case 'r':
      linkage = PDB_LINKAGE_RIGHT;
      break;
    case 's':
      linkage = PDB_LINKAGE_SCOPE;
      break;
    case 't':
      linkage = PDB_LINKAGE_TYPEGUID;
      break;
    default:
      gdp_bug(ctx->ctx_parser->cl);
  }
  id = tok;

  //+	/* `left' and `right' linkages cannot be used if a meta constraint is
  //+	 * present */
  //+	if ((linkage == PDB_LINKAGE_LEFT) || (linkage == PDB_LINKAGE_RIGHT))
  //+		if (ast->conlist_has_meta(ctx->ctx_conlist))
  //+			goto fail_meta;

  /* in a write request, there can only be one linkage per kind */
  if (!is_read_request(ctx->ctx_cmd))
    if (linkmap_test(ctx->ctx_linkmap, linkage)) goto fail_linkage;
  linkmap_set(&ctx->ctx_linkmap, linkage);

  // `->' `(' Constraints ')'
  // ( `=' | `~=' | `!=' ) Guids
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_RARR:
      if ((err = match(ctx, TOK_OPAR, &tok))) goto fail_OPAR;
      opar = tok;
      if ((err = parse_constraints(ctx, &subcon))) return err;
      if ((err = match(ctx, TOK_CPAR, &tok))) goto fail_CPAR;
      /* create sub-constraint and set linkage */
      return set_inward_link(ctx, &id, &opar, subcon, linkage);
    case TOK_EQ:
      op = GRAPHD_OP_EQ;
      break;
    case TOK_FE:
      op = GRAPHD_OP_MATCH;
      break;
    case TOK_NE:
      op = GRAPHD_OP_NE;
      break;
    default:
      goto fail_OP;
  }

  // Guids
  if ((err = parse_guidset(ctx, &guidset, true /* null GUIDs ok */)))
    return err;

  /* create linkage constraint */
  return ast->conlist_add_linkage(ctx->ctx_out, ctx->ctx_conlist,
                                  linkage,  // (left, right, ..)
                                  op,       // (=, <, >, ..)
                                  guidset);
fail_CPAR:
  return notify_error(ctx, err, &tok, "expected ')' at end of linkage");
fail_OPAR:
  return notify_error(ctx, err, &tok,
                      "expected '(', followed by a sub-constraint list");
fail_OP:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected '->', or '=', '~=', '!='");
fail_linkage:
  return notify_error(ctx, GDP_ERR_SEMANTICS, &id,
                      "duplicate linkage declaration");
  //+fail_meta:
  //+	return notify_error(ctx, GDP_ERR_SEMANTICS, &id,
  //+		"linkage conflicts with an already defined meta constraint");
}

/**
 * NodeConstraint <-- `node'
 */
static int parse_con_node(gdp_context *ctx) {
  // nothing to do
  return next(ctx, NULL);
}

/**
 * PageConstraint <-- `pagesize' `=' NUM
 * 		  <-- `countlimit' `=' NUM
 * 		  <-- `resultpagesize' `=' NUM
 *                <-- `start' `=' NUM
 */
static int parse_con_page(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token name;
  gdp_token tok;
  unsigned long long ull;
  size_t size;
  int err;

  // `pagesize' | `resultpagesize' | `countlimit' | `start'
  if ((err = next(ctx, &name))) return err;
  // `='
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
  // NUM
  if ((err = next(ctx, &tok))) return err;
  if ((err = gdp_token_toull(&tok, &ull))) goto fail_ATOM;

  /* convert value to size_t type */
  size = (size_t)ull;
  if (((unsigned long long)size) != ull) goto fail_number;

  switch (tolower(name.tkn_start[0])) {
    case 'c':
      return ast->conlist_add_countlimit(ctx->ctx_out, ctx->ctx_conlist, size);
    case 'p':
      return ast->conlist_add_pagesize(ctx->ctx_out, ctx->ctx_conlist, size);
    case 'r':
      return ast->conlist_add_resultpagesize(ctx->ctx_out, ctx->ctx_conlist,
                                             size);
    case 's':
      return ast->conlist_add_start(ctx->ctx_out, ctx->ctx_conlist, size);
    default:
      gdp_bug(ctx->ctx_parser->cl);
  }

fail_number:
  return notify_error(ctx, GDP_ERR_SEMANTICS, &tok, "invalid number");
fail_ATOM:
  return notify_error(ctx, err, &tok, "expected a numerical value");
fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
}

/**
 * ResultConstraint <-- `result' `=' Pattern
 */
static int parse_con_result(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_pattern_t *pat;
  gdp_token tok;
  gdp_token tok0;
  int err;

  // `result' `=' Pattern
  if ((err = next(ctx, &tok0))) return err;
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
  if ((err = parse_pattern(ctx, NULL, &pat))) return err;

  /* add constraint */
  if ((err = ast->conlist_add_result(ctx->ctx_out, ctx->ctx_conlist, pat)))
    goto fail_add;
  return 0;

fail_add:
  return notify_error(ctx, err, &tok0, "duplicate result definition");
fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
}

/**
 * SortConstraint <-- `sort' `=' [ '+' | '-' ] Pattern
 */
static int parse_con_sort(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_pattern_t *pat;
  gdp_token tok;
  int err;

  // `sort'
  if ((err = next(ctx, &tok))) return err;
  /* check request type */
  switch (ctx->ctx_cmd) {
    case GRAPHD_REQUEST_READ:
    case GRAPHD_REQUEST_ITERATE:
    case GRAPHD_REQUEST_DUMP:
      break;
    default:
      goto fail_invalid_request;
  }

  // `='
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;

  // Pattern
  if ((err = parse_pattern(ctx, NULL, &pat))) return err;

  /* add constraint */
  return ast->conlist_add_sort(ctx->ctx_out, ctx->ctx_conlist, pat);

fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
fail_invalid_request:
  return notify_error(ctx, GDP_ERR_SEMANTICS, &tok,
                      "only allowed in read requests");
}

/**
 * Perform some semantic checks on the `StringConstraint' production.
 */
static int parse_con_string_check(
    gdp_context *ctx,
    gdp_token const *name,  // the constraint name (`value', `type' ..)
    gdp_token const *op     // the operator
    ) {
  /* the `=' operator is always welcome */
  if (op->tkn_kind == TOK_EQ) return 0;
  /* a `write' request only accepts `=' */
  if (ctx->ctx_cmd == GRAPHD_REQUEST_WRITE)
    return notify_error(ctx, GDP_ERR_SEMANTICS, op,
                        "operator cannot be used in write requests");
  /* a `value' constraint accepts any operator, other constraints only
   * accept `!=' */
  if (!gdp_token_matches(name, "value") && (op->tkn_kind != TOK_NE))
    return notify_error(ctx, GDP_ERR_SEMANTICS, op,
                        "operator can only be used with 'value' constraints");

  return 0;
}

/**
 * StringConstraint <-- ( `value' | `type' | `name' ) StringOp Strings
 *
 * StringOp <-- `=' | `!=' | `~=' | `>' | `>=' | `<' | `<='
 */
static int parse_con_string(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_strset_t *values;
  gdp_token name;
  gdp_token op_tok;
  graphd_operator op;
  bool allow_multi;
  int err;

  /* allow multiple constraint values only if we are in a read request */
  allow_multi = is_read_request(ctx->ctx_cmd);

  // ID
  if ((err = match(ctx, TOK_ATOM, &name))) gdp_bug(ctx->ctx_parser->cl);

  // `=' | `!=' | `~=' | `>' | `>=' | `<' | `<='
  if ((err = next(ctx, &op_tok))) return err;
  switch (op_tok.tkn_kind) {
    case TOK_EQ:
      op = GRAPHD_OP_EQ;
      break;
    case TOK_NE:
      op = GRAPHD_OP_NE;
      break;
    case TOK_FE:
      op = GRAPHD_OP_MATCH;
      break;
    case TOK_GT:
      op = GRAPHD_OP_GT;
      break;
    case TOK_GE:
      op = GRAPHD_OP_GE;
      break;
    case TOK_LT:
      op = GRAPHD_OP_LT;
      break;
    case TOK_LE:
      op = GRAPHD_OP_LE;
      break;
    default:
      goto fail_op;
  }

  // Strings
  if ((err = parse_stringset(ctx, allow_multi, &values))) return err;

  /* perform a few basic semantic checks */
  if ((err = parse_con_string_check(ctx, &name, &op_tok))) return err;

  /* add string constraint to current list */
  if ((err = ast->conlist_add_string(ctx->ctx_out,      // output specs
                                     ctx->ctx_conlist,  // current con list
                                     &name,             // constraint name
                                     op,                // constraint operator
                                     values,            // constraint values
                                     allow_multi)))     // allow multiple values
    goto fail_multi;

  return 0;

fail_multi:
  return notify_error(ctx, err, &name, "duplicate constraint declaration");
fail_op:
  return notify_error(ctx, GDP_ERR_SYNTAX, &op_tok,
                      "expected '=', '!=', '~=', '<', '>', '<=', or '>='");
}

/**
 * TimestampConstraint <-- `timestamp' TimestampOp TIMESTAMP
 *
 * TimestampOp <-- `<' | `<=' | `=' | `!=' | `>=' | `>'
 */
static int parse_con_timestamp(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok;
  graphd_operator op;
  graph_timestamp_t ts;
  int err;

  // `timestamp'
  if ((err = next(ctx, NULL))) return err;
  // `<' | `<=' | `=' | `!=' | | `>=' | `>'
  if ((err = next(ctx, &tok))) return err;
  if ((err = lookup_operator(&tok, &op))) goto fail_op;
  if (op == GRAPHD_OP_MATCH) goto fail_op;
  // TIMESTAMP
  if ((err = next(ctx, &tok))) return err;
  if ((err = gdp_token_totime(&tok, &ts))) goto fail_timestamp;

  return ast->conlist_add_timestamp(ctx->ctx_out, ctx->ctx_conlist, op, ts);

fail_timestamp:
  return notify_error(ctx, err, &tok, "expected a timestamp value");
fail_op:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected '<', '<=', '=', '!=', '>=', or '>'");
}

/**
 * TypeConstraint <-- STR
 *
 * Parse the compact form of a type constraint expression, in which the `type'
 * identifier and the equal operator (`=') are omitted.
 *
 * For instance, the expression:
 *
 *	( "Person" ... )
 *
 * is equivalent to:
 *
 *	( type="Person" ... )
 */
static int parse_con_type(gdp_context *ctx,
                          gdp_token const *tok1  // (lookahead(1))
                          ) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_strset_t *value;
  gdp_token dummy;
  bool allow_multi;
  int err;

  cl_assert(ctx->ctx_parser->cl, (tok1->tkn_kind == TOK_STR));

  /* allow multiple constraint values only if we are in a read request */
  allow_multi = is_read_request(ctx->ctx_cmd);

  /* parse string value */
  if ((err = parse_stringset(ctx, 0 /* don't care */, &value))) return err;

  /* make fake `type' token, needed for the `conlist_add_string' function */
  char const *image = "type";
  dummy = (gdp_token){
      .tkn_kind = TOK_ATOM,
      .tkn_start = &image[0],
      .tkn_end = &image[4],
      .tkn_row = tok1->tkn_row,
      .tkn_col = tok1->tkn_col,
  };

  /* add constraint to current list */
  if ((err = ast->conlist_add_string(ctx->ctx_out,      // output specs
                                     ctx->ctx_conlist,  // current con list
                                     &dummy,            // constraint name token
                                     GRAPHD_OP_EQ,      // constraint operator
                                     value,             // constraint value
                                     allow_multi)))     // allow multiple values
    goto fail_multi;

  return 0;

fail_multi:
  return notify_error(ctx, err, tok1,
                      "duplicate 'type' constraint declaration");
}

/**
 * UniqueConstraint <-- `unique' `=' Pattern
 */
static int parse_con_unique(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_pattern_t *pat;
  gdp_token tok;
  int err;

  // `unique' `=' Pattern
  if ((err = next(ctx, &tok))) return err;
  switch (ctx->ctx_cmd) {
    case GRAPHD_REQUEST_WRITE:
      break;
    default:
      goto fail_request;
  }
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
  if ((err = parse_pattern(ctx, NULL, &pat))) return err;

  /* add constraint */
  if ((err = ast->conlist_add_unique(ctx->ctx_out, ctx->ctx_conlist, pat)))
    goto fail_bad_value;

  return 0;

fail_bad_value:
  return notify_error(ctx, err, &tok, "invalid value");
fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
fail_request:
  return notify_error(ctx, GDP_ERR_SEMANTICS, &tok,
                      "constraint only allowed in write requests");
}

/**
 * ValueTypeConstraint <-- (`valuetype' | `datatype') `=' (ATOM | NULL | STR)
 */
static int parse_con_valuetype(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  graph_datatype vt;
  gdp_token id;
  gdp_token tok;
  int err;

  // `valuetype' `=' ATOM
  if ((err = match(ctx, TOK_ATOM, &id))) gdp_bug(ctx->ctx_parser->cl);
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_STR:
    case TOK_ATOM:
    case TOK_NULL:
      if ((err = gdp_token_totype(&tok, &vt))) goto fail_inval;
      break;
    default:
      goto fail_kind;
  }

  /* add constraint */
  if ((err = ast->conlist_add_valuetype(ctx->ctx_out, ctx->ctx_conlist, vt)))
    goto fail_add;

  return 0;

fail_add:
  return notify_error(ctx, err, &id, "duplicate constraint declaration");
fail_inval:
  return notify_error(ctx, err, &tok, "invalid datatype");
fail_kind:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected a datatype name or number");
fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
}

/**
 * VariableConstraint <-- VAR `=' Pattern
 */
static int parse_con_variable(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_pattern_t *pat;
  gdp_token var;
  gdp_token tok;
  int err;

  // `variable' `=' Pattern
  if ((err = match(ctx, TOK_VAR, &var))) gdp_bug(ctx->ctx_parser->cl);

  if ((err = match(ctx, TOK_EQ, &tok)))
    return notify_error(ctx, err, &tok, "expected '='");

  if ((err = parse_pattern(ctx, NULL, &pat))) return err;

  /* add constraint */
  return ast->conlist_add_variable(ctx->ctx_out, ctx->ctx_conlist, &var, pat);
}

/**
 * ConstraintSequence <-- `{' Constraints `}'
 */
static int parse_con_sequence(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_conlist_t *sub_list;  // child constraint list
  gdp_token cbrc;
  int err;

  /*  Consume the opening brace.
   */
  if ((err = next(ctx, NULL)) != 0) return err;

  /*  Parse a list of constraints up to a
   *  syntactic element it doesn't understand,
   *  e.g. a closing brace.
   */
  if ((err = parse_constraints(ctx, &sub_list)) != 0) return err;

  /*  Match the closing brace.
   */
  if ((err = match(ctx, TOK_CBRC, &cbrc)) != 0)
    return notify_error(ctx, err, &cbrc, /*{*/ "expected '}'");

  /* Turn the parsed constraint list into a constraint
   * sequence
   */
  err = ast->conlist_add_sequence(ctx->ctx_out,      // output specs
                                  ctx->ctx_conlist,  // current con list
                                  sub_list);
  if (err != 0) return err;

  return 0;
}

/**
 * OrConstraint <-- AnyConstraint
 *            <-- ArrowConstraint
 *            <-- ComparatorConstraint
 *            <-- ConstraintSequence
 *            <-- CountConstraint
 *            <-- CursorConstraint
 *            <-- DatelineConstraint
 *            <-- FalseConstraint
 *            <-- FlagConstraint
 *            <-- GenerationConstraint
 *            <-- GuidConstraint
 *            <-- KeyConstraint
 *            <-- LinkageConstraint
 *            <-- NodeConstraint
 *            <-- PageConstraint
 *            <-- ResultConstraint
 *            <-- StringConstraint
 *            <-- TimestampConstraint
 *            <-- TypeConstraint
 *            <-- ValueTypeConstraint
 *            <-- VariableConstraint
 */
static int parse_or_con(gdp_context *ctx,
                        gdp_token const *tok1  // (lookahead(1))
                        ) {
  gdp_conlist_t *sub_conlist;  // child constraint list
  gdp_token cpar;
  int err;

  switch (tok1->tkn_kind) {
    case TOK_ATOM:
      break;  // (fall through -- see below)
    case TOK_LARR:
    case TOK_RARR:
      return parse_con_arrow(ctx);
    case TOK_STR:
      return parse_con_type(ctx, tok1);
    case TOK_VAR:
      return parse_con_variable(ctx);
    case TOK_OBRC:
      return parse_con_sequence(ctx);
    case TOK_OPAR:
      // ... Constraints `)'
      err = next(ctx, NULL);
      if (err != 0) return err;

      err = parse_constraints(ctx, &sub_conlist);
      if (err != 0) return err;

      err = match(ctx, TOK_CPAR, &cpar);
      if (err != 0)
        return notify_error(ctx, err, &cpar,
                            /*(*/ "expected ')' at end of linkage");

      return set_subcon(ctx, &cpar, sub_conlist);

    default:
      return notify_error(ctx, GDP_ERR_SYNTAX, tok1, "unexpected token");
  }

  /* dispatch by constraint name */
  switch (tolower(*tok1->tkn_start)) {
    case 'a':
      if (gdp_token_matches(tok1, "any")) return parse_con_any(ctx);
      if (gdp_token_matches(tok1, "anchor")) return parse_con_anchor(ctx);
      if (gdp_token_matches(tok1, "archival")) return parse_con_flag(tok1, ctx);
      if (gdp_token_matches(tok1, "atleast")) return parse_con_count(ctx);
      break;
    case 'c':
      if (gdp_token_matches(tok1, "comparator"))
        return parse_con_comparator(ctx);
      if (gdp_token_matches(tok1, "countlimit")) return parse_con_page(ctx);
      if (gdp_token_matches(tok1, "count")) return parse_con_count(ctx);
      if (gdp_token_matches(tok1, "cursor")) return parse_con_cursor(ctx);
      break;
    case 'd':
      if (gdp_token_matches(tok1, "datatype")) return parse_con_valuetype(ctx);
      if (gdp_token_matches(tok1, "dateline")) return parse_con_dateline(ctx);
      break;
    case 'f':
      if (gdp_token_matches(tok1, "false")) return parse_con_false(ctx);
      break;
    case 'g':
      if (gdp_token_matches(tok1, "guid")) return parse_con_guid(ctx);
      break;
    case 'k':
      if (gdp_token_matches(tok1, "key")) return parse_con_key(ctx);
      break;
    case 'l':
      if (gdp_token_matches(tok1, "left")) return parse_con_linkage(ctx);
      if (gdp_token_matches(tok1, "live")) return parse_con_flag(tok1, ctx);
      break;
    case 'n':
      if (gdp_token_matches(tok1, "name")) return parse_con_string(ctx);
      if (gdp_token_matches(tok1, "next")) return parse_con_guid(ctx);
      if (gdp_token_matches(tok1, "newest")) return parse_con_gen(ctx);
      if (gdp_token_matches(tok1, "node")) return parse_con_node(ctx);
      break;
    case 'o':
      if (gdp_token_matches(tok1, "oldest")) return parse_con_gen(ctx);
      if (gdp_token_matches(tok1, "optional")) return parse_con_count(ctx);
      break;
    case 'p':
      if (gdp_token_matches(tok1, "pagesize")) return parse_con_page(ctx);
      if (gdp_token_matches(tok1, "prev")) return parse_con_guid(ctx);
      if (gdp_token_matches(tok1, "previous")) return parse_con_guid(ctx);
      break;
    case 'r':
      if (gdp_token_matches(tok1, "resultpagesize")) return parse_con_page(ctx);
      if (gdp_token_matches(tok1, "result")) return parse_con_result(ctx);
      if (gdp_token_matches(tok1, "right")) return parse_con_linkage(ctx);
      break;
    case 's':
      if (gdp_token_matches(tok1, "scope")) return parse_con_linkage(ctx);
      if (gdp_token_matches(tok1, "sort")) return parse_con_sort(ctx);
      if (gdp_token_matches(tok1, "start")) return parse_con_page(ctx);
      if (gdp_token_matches(tok1, "sort-comparator"))
        return parse_con_sortcomparator(ctx);
      break;
    case 't':
      if (gdp_token_matches(tok1, "timestamp")) return parse_con_timestamp(ctx);
      if (gdp_token_matches(tok1, "type")) return parse_con_string(ctx);
      if (gdp_token_matches(tok1, "typeguid")) return parse_con_linkage(ctx);
      break;
    case 'u':
      if (gdp_token_matches(tok1, "unique")) return parse_con_unique(ctx);
      break;
    case 'v':
      if (gdp_token_matches(tok1, "value")) return parse_con_string(ctx);
      if (gdp_token_matches(tok1, "valuetype")) return parse_con_valuetype(ctx);
      if (gdp_token_matches(tok1, "value-comparator"))
        return parse_con_valuecomparator(ctx);
      break;
  }

  return notify_error(ctx, GDP_ERR_SEMANTICS, tok1, "invalid constraint");
}

/**
 * Constraint <-- OrConstraint
 *            <-- OrConstraint | Constraint
 *            <-- OrConstraint || Constraint
 */
static int parse_con(gdp_context *ctx,
                     gdp_token const *tok1  // (lookahead(1))
                     ) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_conlist_t *container;
  int container_linkmap;
  gdp_token tok, or_tok;
  int err;

  if (tok1->tkn_kind == TOK_BOR || tok1->tkn_kind == TOK_LOR) {
    /*  It's okay for there to not be a
     *  right-hand-side to an "OR" - we
     *  interpret that as an optional empty
     *  sequence.
     */
    err = ast->conlist_add_sequence(ctx->ctx_out,      // output specs
                                    ctx->ctx_conlist,  // current con list
                                    NULL);             // empty sublist
    if (err != 0) return err;

    or_tok = *tok1;
  } else {
    err = parse_or_con(ctx, tok1);
    if (err != 0) return err;

    /*  It's okay for there to
     *  not be a lookahead.
     */
    if ((err = lookahead(ctx, 1, &or_tok))) return 0;

    if (or_tok.tkn_kind != TOK_BOR && or_tok.tkn_kind != TOK_LOR) return 0;
  }

  /*  Consume the "or".
   */
  if ((err = next(ctx, &tok)) != 0) return err;

  /* save parent context
   */
  container = ctx->ctx_conlist;
  container_linkmap = ctx->ctx_linkmap;

  /* create a fresh constraint list
   */
  if ((err = ast->conlist_new(ctx->ctx_out, &ctx->ctx_conlist))) return err;
  ctx->ctx_linkmap = 0;

  if ((err = lookahead(ctx, 1, &tok)) != 0 || tok.tkn_kind == TOK_CBRC ||
      tok.tkn_kind == TOK_CPAR) {
    /*  It's okay for there to not be a
     *  right-hand-side to an "OR" - we
     *  interpret that as an optional empty
     *  sequence.
     */
    err = ast->conlist_add_sequence(ctx->ctx_out,      // output specs
                                    ctx->ctx_conlist,  // current con list
                                    NULL);             // empty sublist
    if (err != 0) return err;
  } else {
    /*  We can't just parse the next subclause and then
     *  somehow join the two - the parser would complain
     *  about duplicates.
     *
     *  Instead, the subclause must go into a separate
     *  list, then be joined with the previous token
     *  in an "or" constructor.
     */

    if ((err = parse_con(ctx, &tok))) return err;
  }

  /* Join what we just parsed and the waiting "or" query.
   */
  err = ast->conlist_add_or(ctx->ctx_out,      // output specs
                            container,         // parent with waiting subclause
                            ctx->ctx_conlist,  // current con list
                            or_tok.tkn_kind == TOK_LOR  // short-circuiting?
                            );
  if (err != 0) return err;

  /* Restore the parent context.
   */
  ctx->ctx_conlist = container;
  ctx->ctx_linkmap = container_linkmap;

  return 0;
}

/*
 * Constraints <-- ( Constraint | `(' Constraints `)' )*
 */
static int parse_constraints(gdp_context *ctx, gdp_conlist_t **new_conlist) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_conlist_t *conlist0;  // parent constraint list
  unsigned int linkmap0;    // parent linkage map
  gdp_token tok;
  int err;

  /* save parent context */
  conlist0 = ctx->ctx_conlist;
  linkmap0 = ctx->ctx_linkmap;
  /* create constraint list */
  if ((err = ast->conlist_new(ctx->ctx_out, &ctx->ctx_conlist))) return err;
  ctx->ctx_linkmap = 0;

  while (true) {
    // Constraint | `(' ...
    if ((err = lookahead(ctx, 1, &tok))) return err;
    switch (tok.tkn_kind) {
      case TOK_ATOM:
      case TOK_LARR:
      case TOK_RARR:
      case TOK_STR:
      case TOK_VAR:
      case TOK_OBRC:
      case TOK_OPAR:
        if ((err = parse_con(ctx, &tok))) return err;
        break;

      default:
        *new_conlist = ctx->ctx_conlist;
        /* restore parent context */
        ctx->ctx_conlist = conlist0;
        ctx->ctx_linkmap = linkmap0;
        return 0;
    }
  }
}

/**
 * AsofModifier <-- `asof' `=' ( ATOM | STRING )
 */
static int parse_mod_asof(gdp_context *ctx, gdp_modlist_t *modlist) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok;
  int err;

  // `asof' `='
  if ((err = next(ctx, &tok))) return err;
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_op;
  // ATOM | STRING
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_STR:
    case TOK_ATOM:
      if ((err = ast->modlist_add_asof(ctx->ctx_out, modlist, &tok)))
        goto fail_value_1;
      break;
    default:
      goto fail_value;
  }

  return 0;

fail_value_1:
  return notify_error(ctx, err, &tok, "invalid 'asof' value");
fail_value:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected a GUID value or a timestamp");
fail_op:
  return notify_error(ctx, err, &tok, "expected '='");
}

/**
 * CostModifier <-- `cost' ( `=' | `~=' ) STRING
 */
static int parse_mod_cost(gdp_context *ctx, gdp_modlist_t *modlist) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  graphd_operator op;
  gdp_token tok;
  int err;

  // `cost'
  if ((err = next(ctx, &tok))) return err;
  // `=' | `~='
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_EQ:
    case TOK_FE:
      if ((err = lookup_operator(&tok, &op))) gdp_bug(ctx->ctx_parser->cl);
      break;
    default:
      goto fail_op;
  }
  // STRING
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_STR:
      if ((err = ast->modlist_add_cost(ctx->ctx_out, modlist, op, &tok)))
        goto fail_value_1;
      break;
    default:
      goto fail_value;
  }

  return 0;

fail_value_1:
  return notify_error(ctx, err, &tok, "invalid 'cost' value");
fail_value:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok, "expected a cost string");
fail_op:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok, "expected '=' or '~='");
}

/**
 * DatelineModifier <-- `dateline' `=' STR
 */
static int parse_mod_dateline(gdp_context *ctx, gdp_modlist_t *modlist) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok;
  int err;

  // `dateline' `=' STR
  if ((err = next(ctx, &tok))) return err;
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_op;
  if ((err = match(ctx, TOK_STR, &tok))) goto fail_STR;

  if ((err = ast->modlist_add_dateline(ctx->ctx_out, modlist, &tok)))
    goto fail_add;

  return 0;

fail_add:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok, "invalid dateline value");
fail_STR:
  return notify_error(ctx, err, &tok,
                      "expected an empty string or a dateline value");
fail_op:
  return notify_error(ctx, err, &tok, "expected '='");
}

/**
 * IdModifier <-- `id' `=' ( ATOM | STRING )
 */
static int parse_mod_id(gdp_context *ctx, gdp_modlist_t *modlist) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok;
  int err;

  // `id'
  if ((err = next(ctx, &tok))) return err;
  // `='
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_op;
  // ATOM | STRING
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_STR:
    case TOK_ATOM:
      if ((err = ast->modlist_add_id(ctx->ctx_out, modlist, &tok)))
        goto fail_value_1;
      break;
    default:
      goto fail_value;
  }

  return 0;

fail_value_1:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok, "invalid 'id' value");
fail_value:
  return notify_error(ctx, err, &tok, "expected a value");
fail_op:
  return notify_error(ctx, err, &tok, "expected '='");
}

/**
 * HeatmapModifier <-- `heatmap' `=' ( ATOM | STRING )
 */
static int parse_mod_heatmap(gdp_context *ctx, gdp_modlist_t *modlist) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok;
  int err;

  // `id'
  if ((err = next(ctx, &tok))) return err;
  // `='
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_op;
  // ATOM | STRING
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_STR:
    case TOK_ATOM:
      if ((err = ast->modlist_add_heatmap(ctx->ctx_out, modlist, &tok)))
        goto fail_value_1;
      break;
    default:
      goto fail_value;
  }

  return 0;

fail_value_1:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok, "invalid 'heatmap' value");
fail_value:
  return notify_error(ctx, err, &tok, "expected a value");
fail_op:
  return notify_error(ctx, err, &tok, "expected '='");
}

/**
 * LoglevelModifier <-- `loglevel' `=' ( ATOM | `(' ATOM* `)' )
 */
static int parse_mod_loglevel(gdp_context *ctx, gdp_modlist_t *modlist) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok;
  int err;

  // `loglevel' `='
  if ((err = next(ctx, &tok))) return err;
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
  // ATOM | `(' ATOM* `)'
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_ATOM:
      if ((err = ast->modlist_add_loglevel(ctx->ctx_out, modlist, &tok)))
        goto fail_value;
      break;
    case TOK_OPAR:
    again:
      if ((err = next(ctx, &tok))) return err;
      switch (tok.tkn_kind) {
        case TOK_ATOM:
          if ((err = ast->modlist_add_loglevel(ctx->ctx_out, modlist, &tok)))
            goto fail_value;
          goto again;
        case TOK_CPAR:
          break;
        default:
          goto fail_ATOM_or_CPAR;
      }
      break;
    default:
      goto fail_ATOM_or_OPAR;
  }

  return 0;

fail_value:
  return notify_error(ctx, err, &tok, "invalid loglevel value");
fail_ATOM_or_CPAR:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected a loglevel value or ')'");
fail_ATOM_or_OPAR:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected a loglevel value or '('");
fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
}

/**
 * TimeoutModifier <-- `timeout' `=' NUM
 */
static int parse_mod_timeout(gdp_context *ctx, gdp_modlist_t *modlist) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok;
  unsigned long long timeout;
  int err;

  // `timeout' `='
  if ((err = next(ctx, NULL))) return err;
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
  // NUM
  if ((err = match(ctx, TOK_ATOM, &tok))) goto fail_ATOM;
  if ((err = gdp_token_toull(&tok, &timeout))) goto fail_inval;

  return ast->modlist_add_timeout(ctx->ctx_out, modlist, timeout);

fail_inval:
  return notify_error(ctx, err, &tok, "invalid 'timeout' value");
fail_ATOM:
  return notify_error(ctx, err, &tok, "expected a numerical value");
fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
}

/**
 * RequestModifier <-- AsofModifier
 *                 <-- CostModifier
 *                 <-- DatelineModifier
 *                 <-- IdModifier
 *                 <-- LoglevelModifier
 *                 <-- TimeoutModifier
 */
static int parse_mod(gdp_context *ctx, gdp_modlist_t *modlist,
                     gdp_token const *tok1  // (lookahead)
                     ) {
  if (gdp_token_matches(tok1, "asof")) return parse_mod_asof(ctx, modlist);
  if (gdp_token_matches(tok1, "cost")) return parse_mod_cost(ctx, modlist);
  if (gdp_token_matches(tok1, "dateline"))
    return parse_mod_dateline(ctx, modlist);
  if (gdp_token_matches(tok1, "id")) return parse_mod_id(ctx, modlist);
  if (gdp_token_matches(tok1, "loglevel"))
    return parse_mod_loglevel(ctx, modlist);
  if (gdp_token_matches(tok1, "heatmap"))
    return parse_mod_heatmap(ctx, modlist);
  if (gdp_token_matches(tok1, "timeout"))
    return parse_mod_timeout(ctx, modlist);

  (void)next(ctx, NULL);

  return notify_error(ctx, GDP_ERR_SYNTAX, tok1, "unknown request modifier");
}

/*
 * RequestModifiers <-- RequestModifier*
 */
static int parse_modifiers(gdp_context *ctx, gdp_modlist_t **new_modlist) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_modlist_t *modlist;
  gdp_token tok1;
  int err;

  if ((err = ast->modlist_new(ctx->ctx_out, &modlist))) return err;

  while (true) {
    if ((err = lookahead(ctx, 1, &tok1))) return err;
    switch (tok1.tkn_kind) {
      case TOK_ATOM:
        if ((err = parse_mod(ctx, modlist, &tok1))) return err;
        break;
      default:
        *new_modlist = modlist;
        return 0;
    }
  }
}

/**
 * SetProperty <-- property_name = property_value
 */
static int parse_set(gdp_context *ctx, gdp_proplist_t *props,
                     gdp_token const *tok1  // ( lookahead(1) )
                     ) {
  int err;
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok2;

  if ((err = next(ctx, NULL)))
    return notify_error(ctx, err, tok1, "insufficient arguments to set");

  if ((err = match(ctx, TOK_EQ, &tok2)))
    return notify_error(ctx, err, tok1, "expected '=' after set variable");

  if ((err = next(ctx, &tok2)))
    return notify_error(ctx, err, tok1, "insufficient arguments to set");

  if ((tok2.tkn_kind == TOK_ATOM) || (tok2.tkn_kind == TOK_STR)) {
    err = ast->proplist_add(ctx->ctx_out, props, tok1->tkn_start, tok1->tkn_end,
                            tok2.tkn_start, tok2.tkn_end);
    return err;
  }
  return notify_error(ctx, err, &tok2, "no");
}

static int parse_record_guid(gdp_context *ctx, unsigned char version,
                             graph_guid *guid) {
  gdp_token tok;
  int err;

  if ((err = next(ctx, &tok))) return err;
  if ((err = gdp_token_toguid(&tok, guid))) goto fail_guid;
  /* convert GUIDs versions 1 and 2 to version 5, if not zero */
  if ((version <= 2) && !GRAPH_GUID_IS_NULL(*guid)) {
    unsigned long long serial = GRAPH_GUID_SERIAL(*guid);
    unsigned long long db = GRAPH_V2GUID_DB(*guid);
    graph_guid_from_db_serial(guid, db, serial);
  }

  return 0;

fail_guid:
  return notify_error(ctx, err, &tok, "expected a GUID value");
}

static int parse_record_string(gdp_context *ctx, gdp_token *tok) {
  int err;

  if ((err = next(ctx, tok))) return err;
  switch (tok->tkn_kind) {
    case TOK_NULL:
    case TOK_STR:
      break;
    default:
      return notify_error(ctx, GDP_ERR_SYNTAX, tok,
                          "expected a string or 'null'");
  }

  return 0;
}

static int parse_record_datatype(gdp_context *ctx, graph_datatype *type) {
  gdp_token tok;
  int err;

  if ((err = next(ctx, &tok))) return err;
  if ((err = gdp_token_totype(&tok, type)))
    return notify_error(ctx, err, &tok, "expected a data type value");

  return 0;
}

static int parse_record_bool(gdp_context *ctx, bool *val) {
  gdp_token tok;
  int err;

  if ((err = next(ctx, &tok))) return err;
  if ((err = gdp_token_tobool(&tok, val)))
    return notify_error(ctx, err, &tok, "expected 'true' or 'false'");

  return 0;
}

static int parse_record_timestamp(gdp_context *ctx, graph_timestamp_t *ts) {
  gdp_token tok;
  int err;

  if ((err = next(ctx, &tok))) return err;
  if ((err = gdp_token_totime(&tok, ts)))
    return notify_error(ctx, err, &tok, "expected a timestamp value");

  return 0;
}

/**
 * RestoreRecord <-- `(' <..see below..> `)'
 */
static int parse_record(gdp_context *ctx, unsigned char version,
                        gdp_recordlist_t *records, unsigned int index) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok;
  gdp_record rec;
  int err;

  // `('
  if ((err = match(ctx, TOK_OPAR, &tok))) goto fail_OPAR;

  /* guid */
  if ((err = parse_record_guid(ctx, version, &rec.r_v5_guid))) return err;

  /* type */
  if (version == 1) {
    if ((err = parse_record_string(ctx, &rec.r_v1_type))) return err;
  } else {
    if ((err = parse_record_guid(ctx, version, &rec.r_v5_typeguid))) return err;
  }

  /* name */
  if ((err = parse_record_string(ctx, &rec.r_v5_name))) return err;

  /* datatype */
  if ((err = parse_record_datatype(ctx, &rec.r_v5_datatype))) return err;

  /* value */
  if ((err = parse_record_string(ctx, &rec.r_v5_value))) return err;

  /* scope */
  if ((err = parse_record_guid(ctx, version, &rec.r_v5_scope))) return err;

  /* live */
  if ((err = parse_record_bool(ctx, &rec.r_v5_live))) return err;

  /* archival */
  if ((err = parse_record_bool(ctx, &rec.r_v5_archival))) return err;

  /* txstart (version 6 only) */
  if (version == 6)
    if ((err = parse_record_bool(ctx, &rec.r_v6_txstart))) return err;

  /* timestamp */
  if ((err = parse_record_timestamp(ctx, &rec.r_v5_timestamp))) return err;

  /* left GUID */
  if ((err = parse_record_guid(ctx, version, &rec.r_v5_left))) return err;

  /* right GUID */
  if ((err = parse_record_guid(ctx, version, &rec.r_v5_right))) return err;

  /* prev GUID */
  if ((err = parse_record_guid(ctx, version, &rec.r_v5_prev))) return err;

  // `)'
  if ((err = match(ctx, TOK_CPAR, &tok))) goto fail_CPAR;

  /* set record */
  return ast->recordlist_set(ctx->ctx_out,  // output specs
                             version,       // version number
                             records,       // record set
                             index,         // which record
                             &rec);         // the record image

fail_CPAR:
  return notify_error(ctx, err, &tok, "expected ')' at end of record");
fail_OPAR:
  return notify_error(ctx, err, &tok, "expected '(' at beginning of record");
}

/**
 * CrashRequest
 *
 * Parse a `crash' request.
 */
static int parse_request_crash(gdp_context *ctx, gdp_token const *tok) {
  (void)notify_error(ctx, GDP_ERR_SEMANTICS, tok, "crashing!");
  exit(4);
}

/**
 * DefaultRequest <-- ID RequestModifiers `(' Constraints `)' END
 */
static int parse_request_default(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_conlist_t *conlist = NULL;
  gdp_token tok;
  int err;

  // `(' Constraints `)'
  if ((err = match(ctx, TOK_OPAR, &tok))) goto fail_OPAR;
  if ((err = parse_constraints(ctx, &conlist))) return err;
  if ((err = match(ctx, TOK_CPAR, &tok))) goto fail_CPAR;

  // END
  if ((err = match(ctx, TOK_END, &tok))) goto fail_END;

  /* finally, create the request */
  return ast->request_new(ctx->ctx_out,      // output specs
                          ctx->ctx_cmd,      // request kind
                          ctx->ctx_modlist,  // request modifiers
                          conlist);          // constraint list
fail_END:
  return notify_error(ctx, err, &tok, "expected end of the request");
fail_CPAR:
  return notify_error(ctx, err, &tok, "expected ')' at end of request");
fail_OPAR:
  return notify_error(ctx, err, &tok,
                      "expected '(' or a list of request modifiers");
}

/**
 * Parse a constraint for `dump' requests.
 */
static int parse_request_dump_con(gdp_context *ctx, unsigned long long *ull) {
  gdp_token tok;
  int err;

  // `='
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
  // NUM
  if ((err = match(ctx, TOK_ATOM, &tok))) goto fail_number;
  if ((err = gdp_token_toull(&tok, ull))) goto fail_inval;

  return 0;

fail_inval:
  return notify_error(ctx, err, &tok, "invalid value");
fail_number:
  return notify_error(ctx, err, &tok, "expected a numerical value");
fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
}

/**
 * DumpRequest <-- `dump' `(' DumpConstraint* `)' END
 *
 * DumpConstraint <-- ( `start' | `end' | `pagesize' ) `=' NUM
 */
static int parse_request_dump(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  unsigned long long start = ~0ULL;
  unsigned long long end = ~0ULL;
  unsigned long long pgsize = 0ULL;
  gdp_token tok;
  int err;

  // `('
  if ((err = match(ctx, TOK_OPAR, &tok))) goto fail_OPAR;
again:
  // ( `start' | `end' | `pagesize' )* `)'
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_CPAR:
      // END
      if ((err = match(ctx, TOK_END, &tok))) goto fail_END;
      return ast->request_new_dump(ctx->ctx_out, ctx->ctx_modlist, start, end,
                                   pgsize);
    case TOK_ATOM:
      if (gdp_token_matches(&tok, "start"))
        err = parse_request_dump_con(ctx, &start);
      else if (gdp_token_matches(&tok, "end"))
        err = parse_request_dump_con(ctx, &end);
      else if (gdp_token_matches(&tok, "pagesize"))
        err = parse_request_dump_con(ctx, &pgsize);
      else
        goto fail_ATOM;
      if (err) return err;
      goto again;
    default:
      goto fail_CPAR_or_ATOM;
  }

fail_END:
  return notify_error(ctx, err, &tok, "expected end of the request");
fail_ATOM:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected 'start', 'end', or 'pagesize'");
fail_CPAR_or_ATOM:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected a dump constraint, or ')'");
fail_OPAR:
  return notify_error(ctx, err, &tok, "expected '('");
}

/**
 * ReplicaRequest <-- `replica' `(' ReplicaConstraint `)' END
 *
 * ReplicaConstraint <-- `check-master'
 *                   <-- `start-id' `=' NUM
 *                   <-- `version' `=' NUM
 */
static int parse_request_replica(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  unsigned long long start = 0;
  unsigned long long v = 0;
  bool has_v = false;
  bool master = false;
  gdp_token tok;
  int err;

  // `('
  if ((err = match(ctx, TOK_OPAR, &tok))) goto fail_OPAR;
again:
  // `)' | ReplicaConstraint
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_CPAR:
      if (!has_v) goto fail_no_version;
      if (v != 1) goto fail_version;
      if ((err = match(ctx, TOK_END, &tok))) goto fail_END;
      return ast->request_new_replica(ctx->ctx_out, ctx->ctx_modlist, start, v,
                                      master);
    case TOK_ATOM:
      break;
    default:
      goto fail_id;
  }

  // `check-master'
  if (gdp_token_matches(&tok, "check-master")) master = true;
  // `start-id' `=' NUM
  else if (gdp_token_matches(&tok, "start-id")) {
    if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
    if ((err = next(ctx, &tok))) return err;
    if ((err = gdp_token_toull(&tok, &start))) goto fail_num;
  }
  // `version' `=' NUM
  else if (gdp_token_matches(&tok, "version")) {
    if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
    if ((err = next(ctx, &tok))) return err;
    if ((err = gdp_token_toull(&tok, &v))) goto fail_num;
    has_v = true;
  } else
    goto fail_id;

  goto again;

fail_END:
  return notify_error(ctx, err, &tok, "expected end of the request");
fail_version:
  return notify_error(ctx, GDP_ERR_SEMANTICS, &tok, "version not supported");
fail_no_version:
  return notify_error(ctx, GDP_ERR_SEMANTICS, &tok, "missing version number");
fail_num:
  return notify_error(ctx, err, &tok, "expected a numerical value");
fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
fail_id:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected 'start-id', 'version', or 'check-master'");
fail_OPAR:
  return notify_error(ctx, err, &tok, "expected '('");
}

/*
 * ReplicaOkRequest <-- `rok' `(' VERSION ( STR | `archive' ) `)' END
 */
static int parse_request_replica_ok(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  unsigned long long version;
  gdp_token tok;
  gdp_token address;
  char buf[8];
  int err;

  // `rok' (already matched)

  // `('
  if ((err = match(ctx, TOK_OPAR, &tok))) goto fail_OPAR;

  // "1"
  if ((err = next(ctx, &tok))) return err;
  if ((err = gdp_token_toull(&tok, &version))) goto fail_version;
  if (version != 1) goto fail_version_1;

  // STR | `archive'
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_ATOM:
      if (!gdp_token_matches(&tok, "archive")) goto fail_STR_or_archive;
      break;
    case TOK_STR:
      if (gdp_token_len(&tok) > 0) {
        gdp_token_image(&tok, buf, sizeof(buf));
        if (strncmp(buf, "tcp://", 6)) goto fail_bad_tcp;
      }
      break;
    default:
      goto fail_STR_or_archive;
  }
  address = tok;

  // `)'
  if ((err = match(ctx, TOK_CPAR, &tok))) goto fail_CPAR;

  // END
  if ((err = match(ctx, TOK_END, &tok))) goto fail_END;

  return ast->request_new_rok(ctx->ctx_out, ctx->ctx_modlist, version,
                              &address);

fail_END:
  return notify_error(ctx, err, &tok, "expected end of the request");
fail_CPAR:
  return notify_error(ctx, err, &tok, "expected ')'");
fail_STR_or_archive:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected 'archive' or \"tcp://host:port\"");
fail_bad_tcp:
  return notify_error(ctx, err, &tok,
                      "address must be in the form \"tcp://host:port\"");
fail_version_1:
  return notify_error(ctx, GDP_ERR_SEMANTICS, &tok, "version not supported");
fail_version:
  return notify_error(ctx, err, &tok, "expected a version number");
fail_OPAR:
  return notify_error(ctx, err, &tok, "expected '('");
}

/**
 * ReplicaWriteRequest <-- `replica-write' `(' NUM NUM RestoreRecord* `)' END
 */
static int parse_request_replica_write(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_recordlist_t *records;
  gdp_token tok;
  size_t num;
  size_t ix;
  unsigned long long start;
  unsigned long long end;
  int err;

  // `('
  if ((err = match(ctx, TOK_OPAR, &tok))) goto fail_OPAR;

  /* start */
  if ((err = next(ctx, &tok))) return err;
  if ((err = gdp_token_toull(&tok, &start))) goto fail_start;

  /* end */
  if ((err = next(ctx, &tok))) return err;
  if ((err = gdp_token_toull(&tok, &end))) goto fail_end;
  if (start > end) goto fail_end_1;

  /* allocate record set */
  num = end - start;
  if (num > GRAPHD_RESTORE_MAX) goto fail_end_2;
  if (num == 0)
    records = NULL;
  else {
    if ((err = ast->recordlist_new(ctx->ctx_out, num, &records))) return err;
    /* parse individual records */
    for (ix = 0; ix < num; ix++)
      if ((err = parse_record(ctx, 6, records, ix))) return err;
  }

  // `)'
  if ((err = match(ctx, TOK_CPAR, &tok))) goto fail_CPAR;

  // END
  if ((err = match(ctx, TOK_END, &tok))) goto fail_END;

  /* create replica-write request */
  return ast->request_new_replica_write(ctx->ctx_out,      // output specs
                                        ctx->ctx_modlist,  // req modifiers
                                        records,           // records
                                        num,               // number of records
                                        start,             // start record
                                        end);              // end record
fail_END:
  return notify_error(ctx, err, &tok, "expected end of the request");
fail_CPAR:
  return notify_error(ctx, err, &tok, "expected ')'");
fail_end_2:
  return notify_error(ctx, GDP_ERR_SEMANTICS, &tok, "too many records");
fail_end_1:
  return notify_error(ctx, GDP_ERR_SEMANTICS, &tok,
                      "start value cannot exceed end value");
fail_end:
  return notify_error(ctx, err, &tok, "expected end record number");
fail_start:
  return notify_error(ctx, err, &tok, "expected start record number");
fail_OPAR:
  return notify_error(ctx, err, &tok, "expected '(' after 'replica-write'");
}

/**
 * RestoreRequest <-- `restore' `(' STR NUM NUM RestoreRecord* `)' END
 */
static int parse_request_restore(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_recordlist_t *records;
  gdp_token tok;
  size_t num;
  size_t ix;
  unsigned long long version;
  unsigned long long start;
  unsigned long long end;
  int err;

  // `('
  if ((err = match(ctx, TOK_OPAR, &tok))) goto fail_OPAR;

  /* version: "1" | "2" | "5" | "6" */
  if ((err = match(ctx, TOK_STR, &tok))) goto fail_version;
  if (gdp_token_toull(&tok, &version)) goto fail_version;
  switch (version) {
    case 1:
    case 2:
    case 5:
    case 6:
      break;
    default:
      goto fail_version;
  }

  /* start */
  if ((err = match(ctx, TOK_ATOM, &tok))) goto fail_start;
  if ((err = gdp_token_toull(&tok, &start))) goto fail_start;

  /* end */
  if ((err = match(ctx, TOK_ATOM, &tok))) goto fail_end;
  if ((err = gdp_token_toull(&tok, &end))) goto fail_end;
  if (end < start) goto fail_end_1;

  /* allocate record set */
  num = end - start;
  if (num > GRAPHD_RESTORE_MAX) goto fail_end_2;
  if (num == 0)
    records = NULL;
  else {
    if ((err = ast->recordlist_new(ctx->ctx_out, num, &records))) return err;
    /* parse individual records */
    for (ix = 0; ix < num; ix++)
      if ((err = parse_record(ctx, version, records, ix))) return err;
  }

  // `)'
  if ((err = match(ctx, TOK_CPAR, &tok))) goto fail_CPAR;
  // END
  if ((err = match(ctx, TOK_END, &tok))) goto fail_END;

  /* create the restore request */
  return ast->request_new_restore(ctx->ctx_out,      // output specs
                                  ctx->ctx_modlist,  // req modifiers
                                  records,           // records (or NULL)
                                  num,               // number of records
                                  version,           // record version
                                  start,             // start record boundary
                                  end);              // end record boundary
fail_END:
  return notify_error(ctx, err, &tok, "expected end of the request");
fail_CPAR:
  return notify_error(ctx, err, &tok, "expected ')'");
fail_end_2:
  return notify_error(ctx, GDP_ERR_SEMANTICS, &tok, "too many records");
fail_end_1:
  return notify_error(ctx, GDP_ERR_SEMANTICS, &tok,
                      "start value cannot exceed end value");
fail_end:
  return notify_error(ctx, err, &tok,
                      "expected a decimal number as the end value");
fail_start:
  return notify_error(ctx, err, &tok,
                      "expected a decimal number as the start value");
fail_version:
  return notify_error(
      ctx, err ?: GDP_ERR_SEMANTICS, &tok,
      "expected a version number: \"1\", \"2\", \"5\", or \"6\"");
fail_OPAR:
  return notify_error(ctx, err, &tok, "expected '('");
}

/**
 * SetRequest <-- `set' `(' SetProperty* `)' END
 */
static int parse_request_set(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_property_t *proplist;
  gdp_token tok;
  int err;

  /* create a property list */
  if ((err = ast->proplist_new(ctx->ctx_out, &proplist))) return err;

  // `('
  if ((err = match(ctx, TOK_OPAR, &tok))) goto fail_OPAR;
again:
  // SetProperty* | `)'
  if ((err = lookahead(ctx, 1, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_ATOM:
    case TOK_STR:
      if ((err = parse_set(ctx, proplist, &tok))) return err;
      goto again;
    case TOK_CPAR:
      next(ctx, NULL);
      break;
    default:
      goto fail_CPAR_or_property;
  }

  // END
  if ((err = match(ctx, TOK_END, &tok))) goto fail_END;

  /* create `set' request */
  return ast->request_new_set(ctx->ctx_out, ctx->ctx_modlist, proplist);

fail_END:
  return notify_error(ctx, err, &tok, "expected end of the request");
fail_CPAR_or_property:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected a property name or ')'");
fail_OPAR:
  return notify_error(ctx, err, &tok, "expected '(' after 'set'");
}

/**
 * SmpRequest <-- `smp' `(' SmpCommand `)' END
 * SmpCommand <-- `pre-write'
 *            <-- `post-write'
              <-- `connect'
 */
static int parse_request_smp(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_smpcmd_t *smpcmd;
  unsigned long long *smppid;
  gdp_token tok;
  int err;

  /* create a property list */
  if ((err = ast->smpcmd_new(ctx->ctx_out, &smpcmd, &smppid))) return err;

  // `('
  if ((err = match(ctx, TOK_OPAR, &tok))) goto fail_OPAR;
again:
  // `)' | SmpCommand
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_CPAR:
      break;
    case TOK_ATOM:
      if ((gdp_token_matches(&tok, "connect"))) {
        err = ast->smpcmd_set(ctx->ctx_out, smpcmd, &tok);
        if (err) return err;
        if ((err = lookahead(ctx, 1, &tok))) return err;
        switch (tok.tkn_kind) {
          case TOK_STR:
          case TOK_ATOM:
            if ((err = next(ctx, NULL))) return err;
            if ((err = gdp_token_toull(&tok, smppid))) goto fail_number;
          default:
            break;
        }
      } else if ((err = ast->smpcmd_set(ctx->ctx_out, smpcmd, &tok)))
        return err;
      goto again;
    default:
      goto fail_CPAR_or_ATOM;
  }

  // END
  if ((err = match(ctx, TOK_END, &tok))) goto fail_END;

  /* create `smp' request */
  return ast->request_new_smp(ctx->ctx_out, ctx->ctx_modlist, smpcmd);

fail_number:
  return notify_error(ctx, err, &tok, "expected number");
fail_END:
  return notify_error(ctx, err, &tok, "expected end of the request");
fail_CPAR_or_ATOM:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok, "expected ')' or command");
fail_OPAR:
  return notify_error(ctx, err, &tok, "expected '(' after 'smp'");
}

/*
 * StatusRequest <-- `status' `(' StatusProperty* `)' END
 *
 * StatusProperty <-- NAME
 *                <-- `(' StatusCompound `)'
 *
 * StatusCompound <-- NAME
 *                <-- `diary' [ NUM ]
 */
static int parse_request_status(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok;
  gdp_token name;
  gdp_statlist_t *statlist;
  unsigned long long num;
  int err;

  /* create status list */
  if ((err = ast->statlist_new(ctx->ctx_out, &statlist))) return err;

  // `('
  if ((err = match(ctx, TOK_OPAR, &tok))) goto fail_OPAR;
again:
  // NAME | `(' [ NAME | `diary' [NUM] ] `)'
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_STR:
    case TOK_ATOM:
      // NAME
      if ((err = ast->statlist_add(ctx->ctx_out, statlist, &tok, 0)))
        goto fail_property;
      goto again;
    case TOK_OPAR:
      // `(' [ NAME | `diary' [NUM] ] `)'
      if ((err = next(ctx, &tok))) return err;
      switch (tok.tkn_kind) {
        case TOK_STR:
        case TOK_ATOM:
          num = 0;
          name = tok;
          // `diary' [ NUM ]
          if (gdp_token_matches(&tok, "diary")) {
            if ((err = lookahead(ctx, 1, &tok))) return err;
            switch (tok.tkn_kind) {
              case TOK_STR:
              case TOK_ATOM:
                if ((err = next(ctx, NULL))) return err;
                if ((err = gdp_token_toull(&tok, &num))) goto fail_number;
              default:
                break;
            }
          }
          // `)'
          if ((err = match(ctx, TOK_CPAR, &tok))) goto fail_CPAR;
          if ((err = ast->statlist_add(ctx->ctx_out, statlist, &name, num)))
            goto fail_property;
          break;
        case TOK_CPAR:
          break;
        default:
          goto fail_CPAR_or_property;
      }
      goto again;
    case TOK_CPAR:
      break;
    default:
      goto fail_CPAR_or_property;
  }

  /* the end */
  if ((err = match(ctx, TOK_END, &tok))) goto fail_END;

  return ast->request_new_status(ctx->ctx_out, ctx->ctx_modlist, statlist);

fail_END:
  return notify_error(ctx, err, &tok, "expected end of the request");
fail_CPAR_or_property:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected a property name or ')'");
fail_CPAR:
  return notify_error(ctx, err, &tok, "expected ')'");
fail_number:
  return notify_error(ctx, err, &tok, "invalid numerical argument");
fail_property:
  return notify_error(ctx, err, &tok, "invalid property name");
fail_OPAR:
  return notify_error(ctx, err, &tok, "expected '('");
}

/**
 * SyncRequest <-- `sync' `(' `)' END
 */
static int parse_request_sync(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok;
  int err;

  // `(' `)' END
  if ((err = match(ctx, TOK_OPAR, &tok))) goto fail_OPAR;
  if ((err = match(ctx, TOK_CPAR, &tok))) goto fail_CPAR;
  if ((err = match(ctx, TOK_END, &tok))) goto fail_END;

  /* create request */
  return ast->request_new(ctx->ctx_out,      // output specs
                          ctx->ctx_cmd,      // request kind
                          ctx->ctx_modlist,  // request modifiers
                          NULL);             // no constraint list

fail_END:
  return notify_error(ctx, err, &tok, "expected end of the request");
fail_CPAR:
  return notify_error(ctx, err, &tok, "expected ')'");
fail_OPAR:
  return notify_error(ctx, err, &tok, "expected '('");
}

static int parse_request_verify_guid(gdp_context *ctx, graph_guid *guid) {
  gdp_token tok;
  int err;

  // `='
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
  // GUID
  if ((err = next(ctx, &tok))) return err;
  if ((err = gdp_token_toguid(&tok, guid))) goto fail_GUID;

  return 0;

fail_GUID:
  return notify_error(ctx, err, &tok, "expected a GUID value");
fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
}

static int parse_request_verify_ull(gdp_context *ctx, unsigned long long *ull) {
  gdp_token tok;
  int err;

  // `='
  if ((err = match(ctx, TOK_EQ, &tok))) goto fail_EQ;
  // NUM
  if ((err = next(ctx, &tok))) return err;
  if ((err = gdp_token_toull(&tok, ull))) goto fail_ULL;

  return 0;

fail_ULL:
  return notify_error(ctx, err, &tok, "expected a numerical value");
fail_EQ:
  return notify_error(ctx, err, &tok, "expected '='");
}

/*
 * VerifyRequest <-- `verify' `(' VerifyConstraint `)' END
 *
 * VerifyConstraint <-- `low' `=' GUID
 *                  <-- `high' `=' GUID
 *                  <-- `pagesize' `=' NUM
 */
static int parse_request_verify(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
  gdp_token tok;
  graph_guid low;
  graph_guid high;
  unsigned long long pagesize;
  int err;

  /* default values */
  memset(&low, 0, sizeof(low));
  memset(&high, 0, sizeof(high));
  pagesize = 1000;

  // `('
  if ((err = match(ctx, TOK_OPAR, &tok))) goto fail_OPAR;
again:
  // `)' | VerifyConstraint
  if ((err = next(ctx, &tok))) return err;
  switch (tok.tkn_kind) {
    case TOK_CPAR:
      break;
    case TOK_ATOM:
      // `low' `=' GUID
      if (gdp_token_matches(&tok, "low")) {
        if ((err = parse_request_verify_guid(ctx, &low))) return err;
      }
      // `high' `=' GUID
      else if (gdp_token_matches(&tok, "high")) {
        if ((err = parse_request_verify_guid(ctx, &high))) return err;
      }
      // `pagesize' `=' NUM
      else if (gdp_token_matches(&tok, "pagesize")) {
        if ((err = parse_request_verify_ull(ctx, &pagesize))) return err;
      } else
        goto fail_CPAR_or_ATOM;
      goto again;
    default:
      goto fail_CPAR_or_ATOM;
  }

  if ((err = match(ctx, TOK_END, &tok))) goto fail_END;

  return ast->request_new_verify(ctx->ctx_out, ctx->ctx_modlist, &low, &high,
                                 pagesize);

fail_END:
  return notify_error(ctx, err, &tok, "expected end of the request");
fail_CPAR_or_ATOM:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok,
                      "expected 'high', 'low', 'pagesize', or ')'");
fail_OPAR:
  return notify_error(ctx, err, &tok, "expected '('");
}

static int request_initialize(gdp_context *ctx) {
  gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;

  if (ast->request_initialize != NULL)
    return (*ast->request_initialize)(ctx->ctx_out, ctx->ctx_cmd);
  return 0;
}

/*
 * Request <-- CancelRequest
 *         <-- CrashRequest
 *         <-- DefaultRequest
 *         <-- DumpRequest
 *         <-- ReplicaRequest
 *         <-- ReplicaOkRequest
 *         <-- ReplicaWriteRequest
 *         <-- ResponseRequest
 *         <-- RestoreRequest
 *         <-- SetRequest
 *         <-- SmpRequest
 *         <-- StatusRequest
 *         <-- SyncRequest
 *         <-- VerifyRequest
 */
static int parse_request(gdp_context *ctx) {
  gdp_token tok;
  int err;

  /* Parse command ("read", "write", etc.)
   */

  // ID
  if ((err = match(ctx, TOK_ATOM, &tok))) goto fail_ID;

  /* look-up request kind */
  ctx->ctx_cmd = lookup_request(ctx, &tok);
  if (ctx->ctx_cmd == GRAPHD_REQUEST_UNSPECIFIED) goto fail_ID_1;

  if ((err = request_initialize(ctx)) != 0) return err;

  /* Requests that have an unusual structure
   */

  switch (ctx->ctx_cmd) {
    case GRAPHD_REQUEST_CRASH:
      return parse_request_crash(ctx, &tok);  // CrashRequest
    default:
      break;
  }

  /* Requests that have the general structure:
   * COMMAND MODIFIERS ( ... )
   */

  if ((err = parse_modifiers(ctx, &ctx->ctx_modlist))) return err;

  switch (ctx->ctx_cmd) {
    case GRAPHD_REQUEST_DUMP:
      return parse_request_dump(ctx);  // DumpRequest
    case GRAPHD_REQUEST_REPLICA:
      return parse_request_replica(ctx);  // ReplicaRequest
    case GRAPHD_REQUEST_REPLICA_WRITE:
      return parse_request_replica_write(ctx);  // ReplicaWriteRequest
    case GRAPHD_REQUEST_RESTORE:
      return parse_request_restore(ctx);  // RestoreRequest
    case GRAPHD_REQUEST_SET:
      return parse_request_set(ctx);  // SetRequest
    case GRAPHD_REQUEST_STATUS:
      return parse_request_status(ctx);  // StatusRequest
    case GRAPHD_REQUEST_SMP:
      return parse_request_smp(ctx);  // SmpRequest
    case GRAPHD_REQUEST_SYNC:
      return parse_request_sync(ctx);  // SyncRequest
    case GRAPHD_REQUEST_VERIFY:
      return parse_request_verify(ctx);  // VerifyRequest

    case GRAPHD_REQUEST_ITERATE:
    case GRAPHD_REQUEST_ISLINK:
    case GRAPHD_REQUEST_READ:
    case GRAPHD_REQUEST_WRITE:
      return parse_request_default(ctx);  // DefaultRequest

    default:
      gdp_bug(ctx->ctx_out->out_cl);
  }

fail_ID_1:
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok, "no such request");
fail_ID:
  return notify_error(ctx, err, &tok, "invalid start of a request");
}

/*
 * ReplicaReply   <-- OK (...) / ERROR ...
 */
static int parse_replica_reply(gdp_context *ctx) {
  gdp_token tok;
  int err;

  if ((err = match(ctx, TOK_ATOM, &tok))) goto fail_ID;

  /* "rok" is deprecated - accept it simply as an alias for "ok".
   */
  if (gdp_token_matches(&tok, "ok") || gdp_token_matches(&tok, "rok"))

    return parse_request_replica_ok(ctx);

  else if (gdp_token_matches(&tok, "error")) {
    gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
    return ast->request_new_rok(ctx->ctx_out, ctx->ctx_modlist, 0, NULL);
  }
  return notify_error(ctx, GDP_ERR_SYNTAX, &tok, "no such replica reply");
fail_ID:
  return notify_error(ctx, err, &tok, "invalid start of a replica reply");
}

/*
 * Reply   <-- OK / ERROR stuff...
 */
static int parse_reply(gdp_context *ctx) {
  gdp_token tok;
  int err;
  bool ok;

  if ((err = match(ctx, TOK_ATOM, &tok))) goto fail_ID;

  if ((ok = gdp_token_matches(&tok, "ok")) ||
      (ok = gdp_token_matches(&tok, "rok")) ||
      gdp_token_matches(&tok, "error")) {
    gdp_ast_ops const *ast = &ctx->ctx_out->out_ops;
    return ast->request_new_response(ctx->ctx_out, ctx->ctx_modlist, ok);
  }

  return notify_error(ctx, GDP_ERR_SYNTAX, &tok, "no such reply");
fail_ID:
  return notify_error(ctx, err, &tok, "invalid start of a reply");
}

int gdp_parse(gdp *parser, gdp_input *input, gdp_output *output) {
  int err;

  /* the parser's context */
  gdp_context ctx = {
      .ctx_parser = parser, .ctx_in = input, .ctx_out = output,
  };

  /* parse request */
  if ((err = parse_request(&ctx))) {
    return err;
  }

  /* done */
  if (parser->dbglex) fputc('\n', parser->dbgf);

  return err;
}

/*  Parse a reply to command <cmd>.
 *
 * Reply <-- ( `error' | `ok' | `rok' ) .* END
 */
int gdp_parse_reply(gdp *parser, graphd_command cmd, gdp_input *input,
                    gdp_output *output) {
  int err;

  /* the parser's context */
  gdp_context ctx = {
      .ctx_parser = parser, .ctx_in = input, .ctx_out = output, .ctx_cmd = cmd};

  if (cmd == GRAPHD_REQUEST_CLIENT_REPLICA)
    err = parse_replica_reply(&ctx);
  else
    err = parse_reply(&ctx);

  if (parser->dbglex) fputc('\n', parser->dbgf);
  return err;
}
