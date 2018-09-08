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

#include <ctype.h>
#include <errno.h>

static const cl_facility cl_builtin_levels[] = {
    {"ultra", CL_LEVEL_ULTRA},

    {"verbose", CL_LEVEL_VERBOSE},
    {"spew", CL_LEVEL_VERBOSE},

    {"debug", CL_LEVEL_DEBUG},

    {"detail", CL_LEVEL_DETAIL},

    {"info", CL_LEVEL_INFO},

    {"fail", CL_LEVEL_FAIL},

    {"overview", CL_LEVEL_OVERVIEW},

    {"operator-error", CL_LEVEL_OPERATOR_ERROR},
    {"operator", CL_LEVEL_OPERATOR_ERROR},

    {"error", CL_LEVEL_ERROR},

    {"fatal", CL_LEVEL_FATAL},

    {0, 0, 0} /* Sentinel */
};

/**
 * @brief Local utility: Look up a facility name, given its bit.
 *
 *  This also works for non-bit facilities (i.e. our builtin
 *  list of levels).
 *
 * @param facs 	List of facilities to match against.
 * @param lev	A single element of a loglevel (i.e., a single
 *		bit for the facilities, or the general linear level
 *		for the builtin facilities.)
 *
 * @return 	NULL if the level wasn't found anywhere,
 *		otherwise the name of the facility or loglevel,
 *		as a '\\0'-terminated string.
 */
static char const* cl_facility_to_string(cl_facility const* facs,
                                         cl_loglevel lev) {
  for (; facs != NULL; facs = facs->fac_reference) {
    for (; facs->fac_name != NULL; facs++)
      if (facs->fac_loglevel == lev) return facs->fac_name;

    if (facs->fac_reference == NULL) break;

    for (; facs[1].fac_reference != NULL; facs++) {
      char const* name;
      name = cl_facility_to_string(facs->fac_reference, lev);
      if (name != NULL) return name;
    }
  }
  return NULL;
}

/**
 * @brief Convert a loglevel to a string.
 *
 *  This isn't very fast, so rewrite this if you want to do
 *  it for e.g. every single log message.
 *
 * @param lev 	Loglevel value, composed of a linear builtin level
 *		(fatal, error, ..., debug, spew) and of zero or
 *		more facility bits.
 * @param facs 	List of facilities that describe what the
 *		facility bits are.
 * @param buf	Buffer to use while formatting the result.
 * @param size	# of bytes pointed to by buf.
 *
 * @return 	A pointer to a '\\0'-terminated representation
 *		of the loglevel setting.
 */
char const* cl_loglevel_to_string(cl_loglevel lev, cl_facility const* facs,
                                  char* buf, size_t size) {
  cl_loglevel bit, unaccounted = 0;
  char* e;
  char* w;
  char const* name;
  size_t n;

  w = buf;
  e = buf + size;

  if (lev == ~(cl_loglevel)0) return "everything";

  name = cl_facility_to_string(cl_builtin_levels, lev & 0xF);
  if (name != NULL) {
    n = strlen(name);
    if (size < n + 6) return name;

    memcpy(buf, name, n);
    w = buf + n;
  }

  for (bit = (1ul << 31); bit > 0xF; bit >>= 1) {
    if (!(lev & bit)) continue;

    /* find the facility corresponding to this bit */
    if ((name = cl_facility_to_string(facs, bit)) == NULL) {
      unaccounted |= bit;
      continue;
    }

    n = strlen(name);
    if (e - w < 2 + n + 6) /* "x, ..." */
    {
      if (e - w < 6) /* , ... */
      {
        if (e - w < 3) return "<loglevel:...>";

        *w++ = '.';
        *w++ = '.';
        *w = '\0';

        return buf;
      }
      if (w > buf)
        memcpy(w, ", ...", 6);
      else
        memcpy(w, "...", 4);
      return buf;
    }
    if (w != buf) {
      *w++ = ' ';
    }
    memcpy(w, name, n);
    w += n;
  }
  *w = '\0';

  if (unaccounted && e - w >= 4) {
    if (w != buf) {
      *w++ = ' ';
      *w++ = '+';
    }
    snprintf(w, e - w, "%lx", (unsigned long)unaccounted);
  }
  return buf;
}

/**
 * @brief Convert a loglevel configuration to a string.
 *
 * @param lev 	Loglevel configuration value
 * @param facs 	List of facilities that describe what the
 *		facility bits are.
 * @param buf	Buffer to use while formatting the result.
 * @param size	# of bytes pointed to by buf.
 *
 * @return 	A pointer to a '\\0'-terminated representation
 *		of the loglevel setting.
 */
char const* cl_loglevel_configuration_to_string(
    cl_loglevel_configuration const* clc, cl_facility const* facs, char* buf,
    size_t size) {
  char abuf[1024], bbuf[1024];
  char const *a, *b;

  if (clc->clc_full == clc->clc_trigger)
    return cl_loglevel_to_string(clc->clc_full, facs, buf, size);

  a = cl_loglevel_to_string(clc->clc_trigger, facs, abuf, sizeof abuf);
  b = cl_loglevel_to_string(clc->clc_full, facs, bbuf, sizeof bbuf);

  if (a == NULL || b == NULL) return NULL;

  snprintf(buf, size, "%s[%s]", a, b);
  return buf;
}

/**
 * @brief Look up a facility, given its name.
 *
 * @param s 	Beginning of the facility name
 * @param e	End of the facility name
 * @param facs  sentinel-terminated list of facilities
 */
static int cl_facility_from_string(char const* s, char const* e,
                                   cl_facility const* facs, cl_loglevel* out) {
  if (s == NULL || s >= e) return EINVAL;

  for (; facs != NULL; facs = facs->fac_reference) {
    for (; facs->fac_name != NULL; facs++) {
      char const* n = facs->fac_name;
      char const* p = s;

      for (p = s, n = facs->fac_name; p < e; p++, n++) {
        if (!isascii(*p)) return EINVAL;
        if (tolower(*p) != tolower(*n)) break;
      }
      if (p == e && *n == '\0') {
        *out = facs->fac_loglevel;
        return 0;
      }
    }

    if (facs->fac_reference == NULL) break;

    for (; facs[1].fac_reference != NULL; facs++) {
      int err = cl_facility_from_string(s, e, facs->fac_reference, out);
      if (err != ENOENT) return err;
    }
  }
  return ENOENT;
}

/**
 * @brief Scan facilities out of a multi-facility loglevel
 *
 *  Parentheses, commas, and white space in the loglevel are ignored.
 *  Everything else must be either a buitlin level or a facility
 *  listed in the facs parameter.
 *
 * @param s 	beginning of the string to scan
 * @param e	end of the string to scan
 * @param facs  sentinel-terminated list of facilities
 * @param out	assign parsed loglevel to here.
 *
 * @return 	0 on success, otherwise ENOENT if one of the
 *		strings used in s...e didn't name a loglevel.
 */
int cl_loglevel_from_string(char const* s, char const* e,
                            cl_facility const* facs, cl_loglevel* out) {
  cl_loglevel lev;

  *out = 0;

  if (s == NULL || s >= e) return 0;
  while (s < e) {
    int err;
    char const *level_s, *level_e;

    /* Skip past (), and space. */
    while (s < e && (isascii(*s) &&
                     (isspace(*s) || *s == '(' || *s == ')' || *s == ',')))
      s++;
    if (s >= e) return 0;

    level_s = s;

    /* Skip past non-punctuation */
    while (s < e && (!isascii(*s) ||
                     (!isspace(*s) && *s != '(' && *s != ')' && *s != ',')))
      s++;
    if (s == level_s) return ENOENT;
    level_e = s;

    /* Look up a matching facility in the caller-supplied list.
     */
    err = cl_facility_from_string(level_s, level_e, facs, &lev);

    /* If that fails, look it up in our builtin level list.
     */
    if (err == 0 ||
        cl_facility_from_string(level_s, level_e, cl_builtin_levels, &lev) == 0)
      *out |= lev;
    else
      return err;
  }
  return 0;
}

/**
 * @brief Scan a loglevel with optional diary.
 *
 *  Parentheses, commas, and white space in the loglevel are ignored.
 *  Everything else must be either a buitlin level or a facility
 *  listed in the facs parameter.
 *
 * @param s 	beginning of the string to scan
 * @param e	end of the string to scan
 * @param facs  sentinel-terminated list of facilities
 * @param out	assign parsed loglevel to here.
 *
 * @return 	0 on success, otherwise ENOENT if one of the
 *		strings used in s...e didn't name a loglevel.
 */
int cl_loglevel_configuration_from_string(char const* s, char const* e,
                                          cl_facility const* facs,
                                          cl_loglevel_configuration* clc) {
  int err;
  char const* obr;

  /*  The diary syntax is  triggerlevel "[" diarylevel "]"
   *
   *  The two loglevels must be inclusive of one another; the
   *  higher one triggers a flush of the ringbuffer containing
   *  stream of the lower one.
   */
  if (e == s || e[-1] != ']' || (obr = memchr(s, '[', e - s)) == NULL) {
    /*  Just the normal syntax.  Loglevel and diarylevel
     *  are the same, and no diary is actually kept.  Everything
     *  is logged straight away.
     */
    err = cl_loglevel_from_string(s, e, facs, &clc->clc_full);
    if (err != 0) return err;
    clc->clc_trigger = clc->clc_full;
    return 0;
  }

  err = cl_loglevel_from_string(s, obr, facs, &clc->clc_trigger);
  if (err != 0) return err;

  err = cl_loglevel_from_string(obr + 1, e - 1, facs, &clc->clc_full);
  if (err != 0) return err;

  /* The stricter loglevel sits on the diary,
   * the weaker is on the log as a whole.
   *
   *  CL_IS_LOGGED(A, B) is true if A is weaker than
   *  or equal to B.
   */
  if (CL_IS_LOGGED(clc->clc_trigger, clc->clc_full)) {
    cl_loglevel lv;

    /*  Switch them.  The log level is the weaker of the two.
     */
    lv = clc->clc_full;
    clc->clc_full = clc->clc_trigger;
    clc->clc_trigger = lv;
  }
  return 0;
}

/**
 * @brief Get the current loglevel.
 *
 * @param cl 	log handle whose level we're curious about.
 * @return 	the loglevel currently set for the handle.
 */
void(cl_get_loglevel_configuration)(cl_handle* cl,
                                    cl_loglevel_configuration* clc) {
  clc->clc_full = cl_get_loglevel_full(cl);
  clc->clc_trigger = cl_get_loglevel_trigger(cl);
}

/**
 * @brief Get the current loglevel.
 *
 * @param cl 	log handle whose level we're curious about.
 * @return 	the loglevel currently set for the handle.
 */
cl_loglevel(cl_get_loglevel_full)(cl_handle* cl) {
  return cl_get_loglevel_full(cl);
}

/**
 * @brief Get the current loglevel.
 *
 * @param cl 	log handle whose level we're curious about.
 * @return 	the loglevel currently set for the handle.
 */
cl_loglevel(cl_get_loglevel_trigger)(cl_handle* cl) {
  return cl_get_loglevel_trigger(cl);
}

/**
 * @brief Set the current loglevel.
 *
 * @param cl 	log handle whose level we're setting.
 * @param lev	loglevel to set
 */
void(cl_set_loglevel_full)(cl_handle* cl, cl_loglevel lev) {
  cl_set_loglevel_full(cl, lev);
}

/**
 * @brief Set the current trigger level.
 *
 * @param cl 	log handle whose level we're setting.
 * @param lev	loglevel to set
 */
void(cl_set_loglevel_trigger)(cl_handle* cl, cl_loglevel lev) {
  cl_set_loglevel_trigger(cl, lev);
}

/**
 * @brief Set the current loglevel from a configuration.
 *
 * @param cl 	log handle whose level we're setting.
 * @param lev	loglevel to set
 */
void cl_set_loglevel_configuration(cl_handle* cl,
                                   cl_loglevel_configuration const* clc) {
  /*  If the loglevels change, truncate the diary.
   */
  if (clc->clc_full == cl_get_loglevel_full(cl) &&
      clc->clc_trigger == cl_get_loglevel_trigger(cl))
    return;

  if (cl->cl_diary != NULL) cl_diary_truncate(cl->cl_diary);

  cl_set_loglevel_trigger(cl, clc->clc_trigger);
  cl_set_loglevel_full(cl, clc->clc_full);
}

void cl_loglevel_configuration_max(cl_loglevel_configuration const* a,
                                   cl_loglevel_configuration const* b,
                                   cl_loglevel_configuration* out) {
  out->clc_trigger = cl_loglevel_max(a->clc_trigger, b->clc_trigger);
  out->clc_full = cl_loglevel_max(a->clc_full, b->clc_full);
}
