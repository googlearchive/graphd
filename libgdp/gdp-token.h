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
#ifndef __GDP_TOKEN_H__
#define __GDP_TOKEN_H__

#include <stdio.h>

#include "libgraph/graph.h"

/**
 * A token kind.
 */
typedef enum gdp_token_kind {
  TOK_END = 1,  ///< The end of the input stream
  TOK_ATOM,     ///< An identifier or literal (number, GUID, timestamp)
  TOK_VAR,      ///< A variable
  TOK_STR,      ///< A string
  TOK_NULL,     ///< The @c null token
  TOK_OPAR,     ///< Open parenthesis
  TOK_CPAR,     ///< Closed parenthesis
  TOK_LARR,     ///< Left arrow
  TOK_RARR,     ///< Right arrow
  TOK_EQ,       ///< Equal
  TOK_NE,       ///< Not equal
  TOK_FE,       ///< Fuzzy equal
  TOK_LT,       ///< Less than
  TOK_LE,       ///< Less than or equal
  TOK_GT,       ///< Greater than
  TOK_GE,       ///< Greater than or equal
  TOK_MINUS,    ///< Minus sign
  TOK_PLUS,     ///< Plus sign
  TOK_OBRC,     ///< Open curly brace
  TOK_CBRC,     ///< Closing curly brace
  TOK_BOR,      ///< Binary (single) or
  TOK_LOR,      ///< Logical (double) or
  TOK_CBEGIN,   ///< Begining of comment
  TOK_CEND      ///< End of comment
} gdp_token_kind;

/**
 * A token scanned by the #gdp_lexer_consume() function.
 */
struct gdp_token {
  enum gdp_token_kind tkn_kind;  ///< The token kind
  char const *tkn_start;         ///< First char in the token image
  char const *tkn_end;           ///< Last char plus one in the image
  unsigned int tkn_row;          ///< Row in the input stream
  unsigned int tkn_col;          ///< Column in the input stream
};

typedef struct gdp_token gdp_token;

/**
 * Match the token's image to a string.
 *
 * @param tok
 *	The token in question.
 * @param img
 *	The image to be matched.
 * @return
 *	True on a match.
 */
extern bool gdp_token_matches(const gdp_token *tok, const char *img);

/**
 * Return the length of the token's image.
 *
 * This function only applies to #TOK_ATOM and #TOK_STR tokens. If @a tok is a
 * string literal, the return value does not include the initial and terminal
 * `"' characters.
 *
 * @param tok
 *	The token in question.
 */
extern size_t gdp_token_len(const gdp_token *tok);

/**
 * Convert token to an unsigned 64-bit number.
 *
 * @param tok
 *	A #TOK_ATOM token.
 * @param [out] val
 *	The corresponding value.
 * @return
 *	Zero on success, otherwise @c EINVAL if @a tok could not be converted
 */
extern int gdp_token_toull(const gdp_token *tok, unsigned long long *val);

/**
 * Convert token to a GUID value.
 *
 * @param tok
 *	A #TOK_ATOM or #TOK_NULL token.
 * @param [out] guid
 *	The corresponding GUID value.
 * @return
 *	Zero on success, otherwise @c EINVAL if @a tok could not be converted.
 */
extern int gdp_token_toguid(const gdp_token *tok, graph_guid *guid);

/**
 * Convert token to a boolean value.
 *
 * @param tok
 *	A #TOK_ATOM token.
 * @param [out] val
 *	The corresponding boolean value.
 * @return
 *	Zero on success, otherwise @c EINVAL if @a tok could not be converted.
 */
extern int gdp_token_tobool(const gdp_token *tok, bool *val);

/**
 * Convert token to a timestamp value.
 *
 * @param tok
 *	A #TOK_ATOM token.
 * @param [out] tm
 *	The corresponding timestamp value.
 * @return
 *	Zero on success, otherwise @c EINVAL if @a tok could not be converted.
 */
extern int gdp_token_totime(const gdp_token *tok, graph_timestamp_t *tm);

/**
 * Convert a token to a data-type value.
 *
 * @param tok
 *	A #TOK_ATOM, or #TOK_NULL token.
 * @param [out] type
 *	The corresponding data-type value.
 * @return
 *	Zero on success, otherwise @c EINVAL if @a tok could not be converted.
 */
extern int gdp_token_totype(const gdp_token *tok, enum graph_datatype *type);

/**
 * Store the token's image into the given buffer.
 *
 * The buffer is always nil-terminated. Non-pritable characters are displayed
 * in the form @c \\ooo, where @c ooo represents the character's ASCII code, as
 * an octal number.
 *
 * @param buf
 *	Where to copy the token's image.
 * @param size
 *	Size of @a buf.
 * @param tok
 *	The token in question.
 */
extern void gdp_token_image(gdp_token const *tok, char *buf, size_t size);

/**
 * Print token information to a file stream.
 *
 * @param file
 *	Standard file stream.
 * @param fmt
 *	A string representing how the token should be printed. Should contain
 *	the special sequences @c $n and @c $i, which are expanded to the name
 *	of the token (e.g. @c ID, @c STR, etc. - see #gdp_token_kind) and the
 *	token's image respectively. For instance, if @a fmt is <tt>[$n $i]</tt>
 *	and the token in question is the <tt>read</tt> identifier, then the
 *	function will write the string <tt>[ID read]</tt> to @a file.
 * @param tok
 *	The token in question.
 */
extern void gdp_token_printf(FILE *file, char const *fmt, gdp_token const *tok);

#endif
