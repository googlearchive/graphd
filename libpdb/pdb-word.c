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
#include "libpdb/pdbp.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>

/* Treat any Unicode characters as word characters. */
#define ISWORD(x) ((unsigned char)(x) >= 0x80 || isalnum(x))
#define ISDIGIT(x) ((unsigned char)(x) < 0x80 && isdigit(x))

#define ISPUNCT(a) (isascii(a) && ispunct(a))
#define ISSPACE(a) (isascii(a) && isspace(a))
#define ISBREAK(a) (ISSPACE(a) || (ISPUNCT(a) && (a) != '-' && (a) != '+'))

#define ISSIGN(a) ((a) == '-' || (a) == '+')
#define ISSIGNPTR(s, s0) \
  ((*(s) == '-' || *(s) == '+') && (s == s0 || ISBREAK(s[-1])))
#define ISPOINT(a) ((a) == '.')

static const unsigned char ascii_to_hash[128] = {
        ['\0'] = 0,

        ['a'] = 3, ['b'] = 4, ['c'] = 5, ['d'] = 6, ['e'] = 7, ['f'] = 8,
        ['g'] = 9, ['h'] = 10, ['i'] = 11, ['j'] = 12, ['k'] = 13, ['l'] = 14,
        ['m'] = 15, ['n'] = 16, ['o'] = 17, ['p'] = 18, ['q'] = 19, ['r'] = 20,
        ['s'] = 21, ['t'] = 22, ['u'] = 23, ['v'] = 24, ['w'] = 25, ['x'] = 26,
        ['y'] = 27, ['z'] = 28,

        ['A'] = 3, ['B'] = 4, ['C'] = 5, ['D'] = 6, ['E'] = 7, ['F'] = 8,
        ['G'] = 9, ['H'] = 10, ['I'] = 11, ['J'] = 12, ['K'] = 13, ['L'] = 14,
        ['M'] = 15, ['N'] = 16, ['O'] = 17, ['P'] = 18, ['Q'] = 19, ['R'] = 20,
        ['S'] = 21, ['T'] = 22, ['U'] = 23, ['V'] = 24, ['W'] = 25, ['X'] = 26,
        ['Y'] = 27, ['Z'] = 28,

        /*  Projecting the numbers into little-used letter slots to
         *  get more distribution, smaller sets.  0 and 1 get their
         *  own full slots - they're expected to be the most popular
         *  numbers.
         */
        ['0'] = 1, ['1'] = 2, ['2'] = 17, ['3'] = 19, ['4'] = 24, ['5'] = 26,
        ['6'] = 27, ['7'] = 28, ['8'] = 29, ['9'] = 30,

        /*  Alphabetical; keeping braces, single quotes,
         *  and spaces together.
         */
        [' '] = 1, /* various spaces	*/
        ['\t'] = 1, ['\n'] = 1, ['\r'] = 1,
        ['&'] = 2,   /* ampersand		*/
        ['*'] = 3,   /* asterisk 		*/
        ['@'] = 4,   /* at-sign 		*/
        ['^'] = 5,   /* circumflex		*/
        ['}'] = 6,   /* close brace		*/
        [')'] = 6,   /* close paren		*/
        [']'] = 6,   /* close square bracket	*/
        [':'] = 7,   /* colon		*/
        [','] = 8,   /* comma		*/
        ['-'] = 9,   /* dash 		*/
        ['$'] = 10,  /* dollar 		*/
        ['"'] = 11,  /* double quote		*/
        ['='] = 12,  /* equal sign		*/
        ['!'] = 13,  /* exclamation mark  	*/
        ['>'] = 14,  /* greater than 	*/
        ['<'] = 15,  /* less than 		*/
        ['#'] = 16,  /* octothorpe		*/
        ['{'] = 17,  /* open brace		*/
        ['('] = 17,  /* open paren		*/
        ['['] = 17,  /* open square bracket	*/
        ['%'] = 18,  /* percent sign		*/
        ['+'] = 19,  /* plus			*/
        ['.'] = 20,  /* point		*/
        ['?'] = 22,  /* question mark 	*/
        ['\''] = 23, /* quote		*/
        ['`'] = 23,  /* quote (back ~)	*/
        [';'] = 24,  /* semicolon		*/
        ['/'] = 25,  /* slash		*/
        ['\\'] = 25, /* slash (back ~)	*/
        ['~'] = 27,  /* tilde		*/
        ['_'] = 28,  /* underscore		*/
        ['|'] = 29   /* vertical bar		*/
};

/*  Render a hash code as a 4-byte key.
 */
void pdb_word_key(unsigned long code, char *buf) {
  ((unsigned char *)buf)[3] = code & 0xFF;
  code >>= 8;
  ((unsigned char *)buf)[2] = code & 0xFF;
  code >>= 8;
  ((unsigned char *)buf)[1] = code & 0xFF;
  code >>= 8;
  ((unsigned char *)buf)[0] = code & 0xFF;
}

/**
 * @brief Return the 5-bit hash value for a Unicode character
 *
 *  Hash values within one hash family are in lexical order.
 *  Note that 0x1F is mapped to 0x1E to avoid having the stop value,
 *  0x1F (31), naturally occur in the input.
 *
 * @param uc	the Unicode character
 * @return 	a 5-bit hash value
 */
#define hash_value(uc)                \
  (((uc) <= 0x7F) ? ascii_to_hash[uc] \
                  : ((0x1F & (uc)) == 0x1F ? 0x1E : (0x1F & (uc))))

static char const *render_chars(char const *s, char const *e, char *buf,
                                size_t bufsize) {
  char *w = buf;
  char *buf_end = buf + bufsize;

  while (s < e && buf_end - w >= 5) {
    if (isascii(*s) && isprint(*s))
      *w++ = *s++;
    else {
      if (*s == '\0') {
        *w++ = '\\';
        *w++ = '0';
      } else if (*s == '\t') {
        *w++ = '\\';
        *w++ = 't';
      } else if (*s == '\n') {
        *w++ = '\\';
        *w++ = 'n';
      } else if (*s == '\r') {
        *w++ = '\\';
        *w++ = 'r';
      } else {
        snprintf(w, (size_t)(buf_end - w), "\\%3.3o", (unsigned char)*s);
        w += strlen(w);
      }
      s++;
    }
  }
  if (w < buf_end) *w = '\0';
  return buf;
}

/**
 * @brief Return an ordered hash value for a word.
 *
 *  We hash up to 5 first Unicode characters.    If there are
 *  fewer than 5, the hash mask is left-justified.
 *
 * @param pdb	request context
 * @param s	beginning of the word's spelling
 * @param e	end of the word's spelling
 */

unsigned long pdb_word_hash(pdb_handle *pdb, char const *s, char const *e) {
  char const *s0 = s;
  unsigned long h = 0;
  unsigned long uc = 0;
  size_t n_chars = 5;

  if (s >= e) return 0;

  for (; s < e && n_chars > 0; s++, n_chars--) {
    /*  At the end of this if..else if .. else block,
     *  <uc> is either the next UCS-4 unicode character, or a
     *  (0x80000000 | garbage) encoding of a coding error
     *  (in the latter case, a message is logged as well.)
     */

    if ((*(unsigned char const *)s & 0x80) == 0)
      uc = *s;
    else if ((*(unsigned char const *)s & 0x40) == 0) {
      char rbuf[200];

      cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
             "pdb_word_hash: coding error: "
             "continuation character %#.2x without prefix: "
             "uc=%lx (bytes: %s)",
             (unsigned int)*(unsigned char const *)s, uc,
             render_chars(s0, e, rbuf, sizeof rbuf));
      uc = 0x80000000 | *(unsigned char const *)s;
      cl_cover(pdb->pdb_cl);
    } else {
      unsigned char const *r;
      int nc;
      unsigned char m;

      /* Start of a new character. */
      for (m = 0x20, nc = 1; m != 0; m >>= 1, nc++)
        if ((*(unsigned char const *)s & m) == 0) break;
      uc = *(unsigned char const *)s & ((1 << (6 - nc)) - 1);

      /* Continuation bytes. */
      for (r = (unsigned char const *)s + 1; nc > 0; nc--, r++) {
        if (r >= (unsigned char const *)e) {
          char rbuf[200];
          cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
                 "pdb_word_hash: coding "
                 "error: end-of-word in "
                 "multibyte character: "
                 "uc=%lx, *s=%x (nc=%d) "
                 "(bytes: %s)",
                 uc, (unsigned int)*r, nc,
                 render_chars(s0, e, rbuf, sizeof rbuf));

          /* Go back to *s and encode it as
           * garbage; don't interpret it as
           * a Unicode character, since it
           * obviously isn't.
           */
          r = (unsigned char const *)s;
          uc = 0x80000000 | *r;
          break;
        } else if ((*r & 0xC0) != 0x80) {
          char rbuf[200];
          cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
                 "pdb_word_hash: coding "
                 "error: expected continuation "
                 "character, got %#2.2x "
                 "(uc=%lx, nc=%d) (bytes: %s)",
                 (unsigned int)*r, uc, nc,
                 render_chars(s0, e, rbuf, sizeof rbuf));
          r = (unsigned char const *)s;
          uc = 0x80000000 | *r;
          break;
        } else
          uc = (uc << 6) | (0x3F & *r);
      }
      /*  S is incremented between iterations.  R is the
       *  first character we're having trouble with.
       */
      s = (char const *)r - 1;

      cl_cover(pdb->pdb_cl);
    }

    /*  <uc> holds the next Unicode character in our input word.
     *  Translate it into a hash 5-bit piece, and add it to the
     *  hash code.
     *
     *  Trust that hash_value will always return something
     *  < 31 -- no need to explicitly mask it.
     */
    cl_assert(pdb->pdb_cl, hash_value(uc) < 31);
    h = (h << 5) | hash_value(uc);
  }

  /*
  cl_log(pdb->pdb_cl, CL_LEVEL_DEBUG,
          "pdb_word_hash \"%.*s\" is %lx",
          (int)(e - s0), s0, (unsigned long)(h << (n_chars * 5)));
  */

  return h << (n_chars * 5);
}

/* How many UTF-8 characters are in s...e?  (Stop counting at 5.) */
int pdb_word_utf8len(pdb_handle *pdb, char const *s, char const *e) {
  size_t n_chars = 0;

  if (s >= e) return 0;

  for (; s < e && n_chars < 5; s++, n_chars++) {
    if ((*(unsigned char const *)s & 0xC0) == 0xC0) {
      unsigned char const *r;
      int nc;
      unsigned char m;

      /* Start of a new character. */
      for (m = 0x20, nc = 1; m != 0; m >>= 1, nc++)
        if ((*(unsigned char const *)s & m) == 0) break;

      /* Continuation bytes. */
      for (r = (unsigned char const *)s + 1; nc > 0; nc--, r++) {
        if (r >= (unsigned char const *)e) {
          /* Go back to *s and encode it as
           * garbage; don't interpret it as
           * a Unicode character, since it
           * obviously isn't.
           */
          r = (unsigned char const *)s;
          break;
        } else if ((*r & 0xC0) != 0x80) {
          r = (unsigned char const *)s;
          break;
        }
      }
      /* Incremented between iterations. */
      s = (char const *)r - 1;

      cl_cover(pdb->pdb_cl);
    }
  }
  return n_chars;
}

int pdb_word_chop(void *data, pdb_handle *pdb, pdb_id id, char const *s,
                  char const *e, pdb_word_chop_callback *callback) {
  cl_handle *cl = pdb->pdb_cl;
  cm_handle *cm = pdb->pdb_cm;
  char const *s0 = s; /* save original start */
  char const *word_s, *word_e;
  int word_type;
  int err;

  cl_log(cl, CL_LEVEL_SPEW, "pdb_word_chop(%llx, \"%.*s\")",
         (unsigned long long)id, (int)(e - s), s);

  while (pdb_word_fragment_next(s0, &s, e, &word_s, &word_e, &word_type)) {
    char const *norm_s, *norm_e;
    char *norm_buf = NULL;
    char const *orig_int, *orig_point, *orig_frac;

    if (word_type == PDB_WORD_PUNCTUATION || word_type == PDB_WORD_SPACE)
      continue;

    cl_cover(cl);

    if (word_type != PDB_WORD_NUMBER) {
      /* Insert the word into the list as is. */
      err = (*callback)(data, pdb, id, word_s, word_e);
      if (err != 0) return err;
      continue;
    }

    err = pdb_word_number_split(word_s, word_e, &orig_int, &orig_point,
                                &orig_frac);
    cl_assert(cl, err == 0);
    if (err != 0) return err;

    /*  Insert the integral part and the fraction,
     *  if they're non-empty.
     */
    if (orig_int != orig_point) {
      /* Insert the word into the list as is. */
      err = (*callback)(data, pdb, id, orig_int, orig_point);
      if (err != 0) return err;
    }
    if (orig_frac != word_e) {
      /* Insert the fraction digits */
      err = (*callback)(data, pdb, id, orig_frac, word_e);
      if (err != 0) return err;
    }

    /*  Normalize the number.
     */
    err = pdb_word_number_normalize(cm, word_s, word_e, &norm_buf, &norm_s,
                                    &norm_e);
    if (err != 0) return err;

    /*  If the normalized version isn't identical
     *  to the original's integral part...
     */
    if (norm_s != orig_int || norm_e != orig_point) {
      char const *norm_int, *norm_point, *norm_frac;

      /* The whole normalized version. */
      err = (*callback)(data, pdb, id, norm_s, norm_e);
      if (err != 0) {
        if (norm_buf != NULL) cm_free(cm, norm_buf);
        return err;
      }

      /*  Split the normalization into
       *  integral part and fraction.
       */
      err = pdb_word_number_split(norm_s, norm_e, &norm_int, &norm_point,
                                  &norm_frac);
      cl_assert(cl, err == 0);
      if (err != 0) {
        if (norm_buf != NULL) cm_free(cm, norm_buf);
        return err;
      }
      cl_assert(cl, norm_int < norm_point);

      /* Just the integer part, with sign - if
       * that's different from the full value.
       */
      if (norm_point != norm_e) {
        /*  Hash in the pre-. word alone, since
         *  it's different from the normalized word.
         */
        err = (*callback)(data, pdb, id, norm_s, norm_point);
        if (err != 0) {
          if (norm_buf != NULL) cm_free(cm, norm_buf);
          return err;
        }
      }
    }
    if (norm_buf != NULL) cm_free(cm, norm_buf);
  }
  return 0;
}

static int pdb_word_has_prefix_callback(void *data, pdb_handle *pdb, pdb_id id,
                                        char const *s, char const *e) {
  char const *prefix = data;

  for (; *prefix != '\0'; s++, prefix++) {
    if (s >= e) return 0;

    if (isascii(*prefix) ? tolower(*prefix) != tolower(*s) : *prefix != *s)

      return 0;
  }
  return PDB_ERR_ALREADY;
}

bool pdb_word_has_prefix(pdb_handle *pdb, char const *prefix, char const *s,
                         char const *e) {
  return pdb_word_chop((void *)prefix, pdb, 0, s, e,
                       pdb_word_has_prefix_callback) == PDB_ERR_ALREADY;
}

static int pdb_word_has_prefix_hash_callback(void *data, pdb_handle *pdb,
                                             pdb_id id, char const *s,
                                             char const *e) {
  unsigned long long const *prefix = data;
  unsigned long long word_hash = pdb_word_hash(pdb, s, e);

  return (word_hash & prefix[0]) == prefix[1] ? PDB_ERR_ALREADY : 0;
}

bool pdb_word_has_prefix_hash(pdb_handle *pdb, unsigned long long const *prefix,
                              char const *s, char const *e) {
  return pdb_word_chop((void *)prefix, pdb, 0, s, e,
                       pdb_word_has_prefix_hash_callback) == PDB_ERR_ALREADY;
}

/* Compile a prefix string s..e into a two-element prefix
 * code.  prefix[0] is the mask, prefix[1] the hash code
 * of the bytes set in the mask.
 */
void pdb_word_has_prefix_hash_compile(pdb_handle *pdb,
                                      unsigned long long *prefix, char const *s,
                                      char const *e) {
  int n = pdb_word_utf8len(pdb, s, e);

  cl_assert(pdb->pdb_cl, n <= 5);

  prefix[0] = ~0 << (5 * (5 - n));
  prefix[1] = pdb_word_hash(pdb, s, e);
}

/* Use two bytes of the hash as "key" thereby dumping
* all hashes having the same upper 3 bytes into the
* same bucket.
*/

static int pdb_word_hmap_add(pdb_handle *pdb, addb_hmap *hm, unsigned long h,
                             pdb_id id, int map) {
  char key[4];

  pdb->pdb_runtime_statistics.rts_index_elements_written++;

  pdb_word_key(h, key);
  return addb_hmap_add(hm, h, key, sizeof key, map, id);
}

static int pdb_word_add_callback(void *data, pdb_handle *pdb, pdb_id id,
                                 char const *s, char const *e) {
  unsigned long m;
  unsigned long word_hash = pdb_word_hash(pdb, s, e);
  int err;
  bool bit;

  /*  Tell the prefix cache that its statistics may be
   *  changing, and may need recalculating.
   */
  pdb_prefix_statistics_drift(pdb, s, e);

  /*  Add the word entry to the index.
   */
  err = pdb_word_hmap_add(pdb, pdb->pdb_hmap, word_hash, id, addb_hmt_word);

  if (err == PDB_ERR_EXISTS) return 0;
  if (err) {
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_word_add_callback(%.*s): error %s",
           (int)(e - s), s, strerror(err));
    return err;
  }

  /*  Update the prefix index.
   */
  err = addb_bmap_check_and_set(pdb->pdb_prefix, word_hash, &bit);
  if (err) {
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_word_add_callback(%.*s): error %s",
           (int)(e - s), s, strerror(err));
    return err;
  }

  if (bit) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_word_add_callback(%.*s, id=%llx): "
           "hash %llx already in the database",
           (int)(e - s), s, (unsigned long long)id,
           (unsigned long long)word_hash);
    return 0;
  }
  cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
         "pdb_word_add_callback(%.*s, id=%llx): "
         "add hash %llx",
         (int)(e - s), s, (unsigned long long)id,
         (unsigned long long)word_hash);

  /*  That covered the full word.
   *
   * 	1 2 3 4 5
   *
   *  Now mark the four prefixes:
   *
   * 	1 2 3 4 #
   * 	1 2 3 # #
   * 	1 2 # # #
   * 	1 # # # #
   *
   *  Stop as soon as you hit a prefix that is already marked.
   *  (It doesn't matter how many markers there are, or what
   *  their value is.)
   */

  for (m = 0x1F; m < (1 << (5 * 4)); m |= (m << 5)) {
    unsigned long wh = word_hash | m;
    cl_assert(pdb->pdb_cl, (word_hash & m) != wh);

    err = addb_bmap_check_and_set(pdb->pdb_prefix, wh, &bit);

    if (err < 0) return err;
    if (bit > 0) {
      cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
             "pdb_word_add_callback(%.*s): "
             "prefix %lx already in the database",
             (int)(e - s), s, (unsigned long)wh);
      return 0;
    }
  }
  return 0;
}

/**
 * @brief Add a value to the word index database.
 */
int pdb_word_add(pdb_handle *pdb, pdb_id id, char const *s, char const *e) {
  cl_cover(pdb->pdb_cl);
  return pdb_word_chop(NULL, pdb, id, s, e, pdb_word_add_callback);
}

/**
 * @brief Add a primitive to the word index.
 *
 * @param pdb	module handle
 * @param id	local ID of the primitive
 * @param pr	primitive data
 *
 * @return 0 on success, a nonzero error code on unexpected error.
 */
int pdb_word_synchronize(pdb_handle *pdb, pdb_id id, pdb_primitive const *pr) {
  size_t sz;

  if ((sz = pdb_primitive_value_get_size(pr)) > 0) {
    char const *mem;
    mem = pdb_primitive_value_get_memory(pr);

    cl_cover(pdb->pdb_cl);
    return pdb_word_chop(NULL, pdb, id, mem, mem + sz - 1,
                         pdb_word_add_callback);
  }
  cl_cover(pdb->pdb_cl);
  return 0;
}

/**
 * @brief initialize an iterator over everything that might contain
 * 	a given word.
 *
 *  The indices on the map are guaranteed to be a superset of the
 *  word's actual occurrences - the caller still needs to rescan the
 *  value contents.
 *
 * @param pdb		module handle
 * @param s		first byte of the word in question
 * @param e		last byte of the word in question
 * @param low		lower boundary, or PDB_ITERATOR_LOW_ANY
 * @param high		upper boundary, or PDB_ITERATOR_HIGH_ANY
 * @param forward	if we should turn into a single
 *			iteration, which direction should it have?
 * @param it_out	the iterator to initialize
 *
 * @return 0 on success, a nonzero error code on error.
 */

int pdb_iterator_word_create(pdb_handle *pdb, char const *s, char const *e,
                             pdb_id low, pdb_id high, bool forward,
                             bool error_if_null, pdb_iterator **it_out) {
  unsigned long word_hash = pdb_word_hash(pdb, s, e);
  char key[4];

  pdb_word_key(word_hash, key);
  return pdb_iterator_hmap_create(pdb, pdb->pdb_hmap, word_hash, key,
                                  sizeof key, addb_hmt_word, low, high, forward,
                                  error_if_null, it_out);
}

/**
 * @brief Return the next fragment from a text value.
 *
 * @param s0 	the begin of the entire text
 * @param s	in/out: the beginning of the text to parse
 * @param e	end of the text to parse
 * @param word_s_out	out: beginning of a fragment
 * @param word_e_out	out: end of a fragment
 * @param word_type_out	out: type of word
 *
 * @return true if a fragment has been extracted
 * @return false if we're out of words.
 */
bool pdb_word_fragment_next(char const *s0, char const **s, char const *e,
                            char const **word_s_out, char const **word_e_out,
                            int *word_type_out) {
  char const *r;
  char const *pre_s, *pre_e;
  char const *post_s, *post_e;

  if ((r = *s) == NULL) r = *s = s0;

  if (r >= e) return false;

  *word_s_out = r;

  /* What's the longest number that we can pull out of this?
   */
  if (ISSIGNPTR(r, s0)) r++;
  pre_s = r;
  while (r < e && ISDIGIT(*r)) r++;
  pre_e = r;
  if ((pre_s == s0 || !ISPOINT(pre_s[-1])) &&
      (pre_s < pre_e || r == s0 || !ISDIGIT(r[-1])) && r < e && ISPOINT(*r)) {
    r++;
    post_s = r;
    while (r < e && ISDIGIT(*r)) r++;
    post_e = r;

    if ((r >= e || !ISWORD(*r)) && ((post_e > post_s) || (pre_e > pre_s))) {
      /*   5.
       *  +1.
       *  -.01
       */

      /* There isn't another dot after this
       * number, right?
       */
      if (r >= e || !ISPOINT(*r)) {
        /* Regular floating point number.
         */
        *word_e_out = *s = r;
        *word_type_out = PDB_WORD_NUMBER;

        return true;
      }

      /* IP addresses and dot-separated hierarchial
       * names are not floating point numbers -
       * take them one segment at a time.
       */
      if (pre_s < pre_e) {
        *word_e_out = *s = pre_e;
        *word_type_out = PDB_WORD_NUMBER;

        return true;
      }

      /*  Weirdness of the form [+-].34. -- skip
       *  punctuation, let the next iteration
       *  take care of the number.
       */
      *word_e_out = *s = post_s;
      *word_type_out = PDB_WORD_PUNCTUATION;

      return true;
    }
  }

  if (pre_s < pre_e && (pre_e == e || !ISWORD(*pre_e))) {
    *word_e_out = *s = pre_e;
    *word_type_out = PDB_WORD_NUMBER;

    return true;
  }

  /*  OK, that didn't work.  Whatever this is, we're
   *  not standing on a number.  Just pull out a normal
   *  word or nonword.
   */
  r = *s;
  if (ISWORD(*r)) {
    do
      r++;
    while (r < e && ISWORD(*r));

    *word_type_out = PDB_WORD_ATOM;
  } else if (ISSPACE(*r)) {
    do
      r++;
    while (r < e && ISSPACE(*r));

    *word_type_out = PDB_WORD_SPACE;
  } else {
    do
      r++;
    while (r < e && ISPUNCT(*r) && !ISSIGNPTR(r, s0));

    *word_type_out = PDB_WORD_PUNCTUATION;
  }

  *word_e_out = *s = r;
  return true;
}

/**
 * @brief Split a number into its pieces
 *
 * @param s		the beginning of the number
 * @param e		end of the number
 * @param pre_s		end of sign, pre-point start
 * @param point_s	end of pre-point, dot start
 * @param post_s	end of dot, post-point start
 *
 ** @return PDB_ERR_NO if the input is not a number.
 */
int pdb_word_number_split(char const *s, char const *e, char const **pre_s,
                          char const **point_s, char const **post_s) {
  if (s >= e) return PDB_ERR_NO;

  s += ISSIGN(*s);
  *pre_s = s;

  if (s >= e) return PDB_ERR_NO;

  for (; s < e; s++)
    if (ISPOINT(*s)) {
      *point_s = s;
      *post_s = s + 1;

      return 0;
    }
  *point_s = *post_s = e;
  return 0;
}

/**
 * @brief Normalize a number
 *
 *  - Remove "+",
 *  - Remove "-" from 0
 *  - Remove leading 0 (except a lone 0) from pre-. or , values.
 *  - Turn , into .
 *  - Remove trailing 0 from post-. or , values.
 *  - Remove trailing . if post-. or , values were 0 only.
 *  - Turn .323 into 0.323
 *
 * Numbers grow at most by one digit on normalization.  If you
 * want to avoid overflow errors, allocate (e - s) + 2 positions
 * for the buffer.
 *
 * @param cm		allocate normalization buffer here
 * @param s		the beginning of the number
 * @param e		end of the number
 * @param *buf_out	buffer if allocated
 * @param norm_s_out	normalized string, start
 * @param norm_e_out	normalized string, end
 *
 * @return 0 on success, PDB_ERR_NO on syntax error.
 */
int pdb_word_number_normalize(cm_handle *cm, char const *s, char const *e,
                              char **buf_out, char const **norm_s_out,
                              char const **norm_e_out) {
  char const *int_s, *int_e, *frac_s, *frac_e;
  char const *sig_s, *sig_e;
  char *buf;
  size_t need;
  bool need_reconstruction = false;

  *buf_out = NULL;
  if (s >= e) return PDB_ERR_NO;

  sig_s = sig_e = s;
  if (*sig_s == '+')
    sig_e = ++sig_s;
  else if (*sig_s == '-')
    sig_e = sig_s + 1;

  frac_s = frac_e = e;
  int_s = sig_e;

  for (int_e = int_s; int_e < e; int_e++)

    /* Fraction starts here?
     */
    if (ISPOINT(*int_e)) {
      frac_s = int_e + 1;

      /* Strip trailing zeros. */
      while (frac_e > frac_s && frac_e[-1] == '0') frac_e--;
      break;
    }

  /*  Add/Remove leading zeros.
   */
  if (int_e == int_s) {
    /* .150 -> 0.150 */

    int_s = "0";
    int_e = int_s + 1;

    if (frac_s < frac_e || sig_s < sig_e) need_reconstruction = true;
  } else {
    /* 01 -> 1, 0000 -> 0 */

    while (int_s + 1 < int_e && *int_s == '0') {
      int_s++;
      if (sig_s < sig_e) need_reconstruction = true;
    }
  }

  /*  If the integral part is "0", and there is
   *  no fraction, remove the leading sign.
   */
  if (int_e == int_s + 1 && *int_s == '0' && frac_s == frac_e)
    sig_s = sig_e = int_s;

  /* Regroup empty sign and fraction value
   * around the int value.
   */
  if (sig_s == sig_e) sig_s = sig_e = int_s;
  if (frac_s == frac_e) frac_s = frac_e = int_e;

  if (!need_reconstruction) {
    /* The normalized value is fully contained
     * in the pre-normalized value.
     */
    *norm_s_out = sig_s;
    *norm_e_out = frac_e;

    return 0;
  }

  need = (sig_e - sig_s)     /* sign 			*/
         + (int_e - int_s)   /* integer part 		*/
         + (frac_s < frac_e) /* 1 for . if there's a fraction*/
         + (frac_e - frac_s) /* fraction			*/
         + 1;                /* terminating \0 		*/
  buf = cm_malloc(cm, need);
  if (buf == NULL) return errno ? errno : ENOMEM;

  if (frac_s < frac_e)
    snprintf(buf, need, "%.*s%.*s.%.*s", (int)(sig_e - sig_s), sig_s,
             (int)(int_e - int_s), int_s, (int)(frac_e - frac_s), frac_s);
  else
    snprintf(buf, need, "%.*s%.*s", (int)(sig_e - sig_s), sig_s,
             (int)(int_e - int_s), int_s);
  *norm_s_out = *buf_out = buf;
  *norm_e_out = buf + strlen(buf);

  return 0;
}

static char *shrink_spaces(const char *in_s, const char *in_e, char *out) {
  char c;
  // char * last_nonspace = out;

  while (in_s < in_e) {
    c = *out = *in_s;

    if (ISSPACE(c)) {
      *out = ' ';
      out++;
      while (ISSPACE(*in_s) && (in_s < in_e)) in_s++;

    } else {
      in_s++;
      out++;
      // last_nonspace = out;
    }
  }

  return out;
}

/*
 * Render a number to a string in graphd normalized form.
 * A normalized number looks like:
 * (-)[0-9]+e(-)[0-9]+
 * with the resitrctions that zero is always "0" and
 * no other instances of leading zeroes are allowed in both
 * the mantissa and exponent. Except in the case of "0", the
 * exponent must always be present. There is an implicit decimal point
 * after the first digit.  This is exactly strict scientific notation
 * with an implicit dot.
 */
char *pdb_number_to_string(cm_handle *cm, const graph_number *n) {
  char *out;

  if (n->num_zero)
    out = cm_strmalcpy(cm, "0");
  else if (n->num_infinity && n->num_positive)
    out = cm_strmalcpy(cm, "+Inf");
  else if (n->num_infinity && !n->num_positive)
    out = cm_strmalcpy(cm, "-Inf");
  else if (n->num_dot) {
    out = cm_sprintf(cm, "%s%.*s%.*se%i", n->num_positive ? "" : "-",
                     (int)(n->num_dot - n->num_fnz), n->num_fnz,
                     (int)(n->num_lnz - n->num_dot) - 1, n->num_dot + 1,
                     n->num_exponent);
  } else {
    out =
        cm_sprintf(cm, "%s%.*se%i", n->num_positive ? "" : "-",
                   (int)(n->num_lnz - n->num_fnz), n->num_fnz, n->num_exponent);
  }
  return out;
}

static int pdb_number_normalize(pdb_handle *pdb, cm_handle *cm, const char *s,
                                const char *e, const char **out_s,
                                const char **out_e, char **buf) {
  graph_number n;
  int err;

  err = graph_decode_number(s, e, &n, true);

  if (err) {
    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
           "pdb_normalize_number: '%.*s' is not a number", (int)(e - s), s);
    return PDB_ERR_SYNTAX;
  }

  *buf = NULL;

  if (n.num_zero)
    *out_s = "0";
  else if (n.num_infinity && n.num_positive)
    *out_s = "+Inf";
  else if (n.num_infinity && !n.num_positive)
    *out_s = "-Inf";
  else if (n.num_dot) {
    *out_s = *buf = cm_sprintf(cm, "%s%.*s%.*se%i", n.num_positive ? "" : "-",
                               (int)(n.num_dot - n.num_fnz), n.num_fnz,
                               (int)(n.num_lnz - n.num_dot) - 1, n.num_dot + 1,
                               n.num_exponent);
  } else {
    *out_s = *buf =
        cm_sprintf(cm, "%s%.*se%i", n.num_positive ? "" : "-",
                   (int)(n.num_lnz - n.num_fnz), n.num_fnz, n.num_exponent);
  }
  if (!(*out_s)) {
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL, "normalize_number: out of memory");

    return ENOMEM;
  }
  *out_e = *out_s + strlen(*out_s);

  return 0;
}

static int pdb_word_normalize(pdb_handle *pdb, char const *s, char const *e,
                              char const **norm_s_out, char const **norm_e_out,
                              char **buf_out) {
  char const *r, *word_s, *word_e;
  char *buf_w, *buf_e = NULL;
  char const *word_r;
  size_t point = 0, digit = 0;
  int word_type;
  int space = 0;
  cl_handle *cl = pdb->pdb_cl;
  cm_handle *cm = pdb->pdb_cm;

  *buf_out = NULL;
  for (r = s; r < e; r++) {
    if (ISPOINT(*r))
      point++;
    else if (ISDIGIT(*r))
      digit++;
    else if (ISSPACE(*r))
      space++;
  }
  if (!point && !digit && !space) {
    *norm_s_out = s;
    *norm_e_out = e;
    *buf_out = NULL;
    return 0;
  }
  while (isspace(*s) && s < e) s++;

  r = word_r = s;
  buf_w = NULL;

  while (pdb_word_fragment_next(s, &r, e, &word_s, &word_e, &word_type)) {
    int err;

    char *norm_buf = NULL;
    char const *norm_s;
    char const *norm_e;

    if (word_type != PDB_WORD_NUMBER) continue;

    err = pdb_word_number_normalize(cm, word_s, word_e, &norm_buf, &norm_s,
                                    &norm_e);
    if (err != 0) return err;

    if (norm_s == word_s && norm_e == word_e) {
      cl_assert(cl, norm_buf == NULL);
      continue;
    }

    /*  OK, we need to normalize.
     */
    if (*buf_out == NULL) {
      *buf_out = cm_malloc(cm, point + 1 + (e - s));
      if (*buf_out == NULL) {
        if (norm_buf != NULL) cm_free(cm, norm_buf);
        return ENOMEM;
      }
      buf_w = *buf_out;
      buf_e = buf_w + point + 1 + (e - s);
      cl_assert(cl, buf_w < buf_e);
    }

    /*  Catch up to here.
     *
     *  word_r is how far we're caught up.
     *
     *  word_s is the beginning of the area
     *  that may change with normalization.
     */
    if (word_s > word_r) {
      /* copy norm here */
      buf_w = shrink_spaces(word_r, word_s, buf_w);
      cl_assert(cl, buf_w < buf_e);
    }

    /* Append the normalized word */
    memcpy(buf_w, norm_s, norm_e - norm_s);
    buf_w += norm_e - norm_s;
    cl_assert(cl, buf_w < buf_e);

    /*  Advance the base of the next non-normalized
     *  text to behind the number.
     */
    word_r = r;

    if (norm_buf != NULL) cm_free(cm, norm_buf);
  }

  /*  Catch up to the end.
   */
  if (*buf_out == NULL) {
    /*  We never normalized anything.
     */
    if (e == s) {
      /* nothing to do... */
      *buf_out = NULL;
      *norm_s_out = *norm_e_out = s;
      return 0;
    }

    *norm_s_out = *buf_out = cm_malloc(cm, e - s);
    if (!*norm_s_out) return ENOMEM;

    *norm_e_out = shrink_spaces(s, e, *buf_out);
    while ((*norm_e_out > *norm_s_out) && isspace((*norm_e_out)[-1]))
      (*norm_e_out)--;

    return 0;
  }

  /* Catch up to here. */
  if (word_r < e) {
    buf_w = shrink_spaces(word_r, e, buf_w);
  }
  while ((buf_w > *buf_out) && isspace(buf_w[-1])) buf_w--;

  cl_assert(cl, buf_w < buf_e);
  *buf_w = '\0';

  *norm_s_out = *buf_out;
  *norm_e_out = buf_w;

  return 0;
}

int pdb_hmap_value_normalize(pdb_handle *pdb, const char *s, const char *e,
                             const char **out_s, const char **out_e,
                             char **buf) {
  int err;
  err = pdb_number_normalize(pdb, pdb->pdb_cm, s, e, out_s, out_e, buf);

  if (!err) {
    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
           "pdb_hmap_value_normalize: number '%.*s' to '%.*s'", (int)(e - s), s,
           (int)(*out_e - *out_s), *out_s);
    return 0;
  }

  if (err != PDB_ERR_SYNTAX) return err;

  err = pdb_word_normalize(pdb, s, e, out_s, out_e, buf);

  cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
         "pdb_hmap_value_normalize: string '%.*s' to '%.*s'", (int)(e - s), s,
         (int)(*out_e - *out_s), *out_s);

  return err;
}
