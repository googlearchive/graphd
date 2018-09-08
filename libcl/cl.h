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
/**
 * @file cl.h
 * @brief Logging support.
 *
 * The libcl library is a homogenous interface to different
 * kinds of logging - to standard error, to syslog, to a diary,
 * to a netlog-style file, or to a plain file.  Its upper
 * half is a printf-style formatter; its backends then dispatch
 * the formatted strings to various kinds of log mechanisms.
 *
 * The application first creates a generic log handle, then
 * "installs" a specific logging format  into the handle
 *  by calling one of the installer callbacks.
 *
 * <pre>
 *  cl_handle * my_cl;
 *  int err;
 *
 *  my_cl = cl_create();
 *  err = cl_netlog(my_cl, "/var/log/my-app.%Y-%m-%d");
 *  if (err != 0) error...
 * </pre>
 *
 * or
 *
 * <pre>
 *  cl_handle * my_cl;
 *  int err;
 *
 *  my_cl = cl_create();
 *  cl_file(my_cl, "/tmp/my-app.log");
 * </pre>
 *
 * Each handle has its own loglevel, set with cl_set_loglevel_configuration().
 * A loglevel is composed of a linear increasing severity level,
 * from CL_LEVEL_VERBOSE to CL_LEVEL_FATAL, and a set of
 * application-defined facility bits that can be individually
 * toggled on or off.  Only messages that either match one
 * of the enabled facility bits or have a loglevel greater than
 * or equal to that of the handle are let through by the handle's
 * formatting code.
 *
 * Each log call includes the handle, the loglevel of the
 * specific message, and the format string and optional arguments.
 * There are a few wrappers around simple cl_log() calls that
 * formalize specific applications of logging, sometimes combined
 * with an application abort.
 * <pre>
 *  char const planet[] = "World";
 *  cl_log(my_cl, CL_LEVEL_INFO, "Hello, %s", plaent);
 *  if (strcmp(planet, "World") != 0)
 *  	cl_notreached(my_cl, "Unexpected planet: %s", planet);
 *  cl_assert(my_cl, strcmp(planet, "World") == 0);
 * </pre>
 *
 *
 * At the end of a handle's lifetime, it can be destroyed:
 * <pre>
 *   cl_destroy(my_cl);
 * </pre>
 *
 *
 */

#ifndef CL_H
#ifndef DOCUMENTATION_GENERATOR_ONLY
#define CL_H 1 /* Guard against multiple includes */
#endif

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>
#include <string.h>
#include <stdbool.h>

#ifndef DOCUMENTATION_GENERATOR_ONLY

/* this compiler support C9X-style preprocessor __VA_ARGS__? */
#ifndef CL_HAVE_C9X_VA_ARGS
#if __STDC__ >= 199901L || __GNUC__
#define CL_HAVE_C9X_VA_ARGS 1 /* Yes. */
#else
#define CL_HAVE_C9X_VA_ARGS 0 /* No.  */
#endif
#endif

#endif

/**
 * @brief Opaque logging interface.
 * Allocated with cl_create(), free'ed with cl_destroy().
 */
typedef struct cl_handle cl_handle;

/**
 * @brief Builtin linear loglevel.
 *
 *  Traditionally, the higher the number, the chattier the application.
 *
 *  The top 4 bits (0..15) of a loglevel value are reserved for a
 *  linear overall "severity" level; the lower the number, the more
 *  important the message.
 *
 *  The other bits are reserved for messages grouped by facilitiy.
 *  They don't carry a level.
 *  If a bit is set, the message applies to that module.
 *
 * 31 30 29 28 27 26 25 24 23 22 21 20 19 18 17 16 15 14 13 12 11 10 9 8 7 6 5
 *  |               |<-- addb ->|<-cm>|<---pdb--->|<-----graphd ----->|<-srv->|
 *  |
 *  DIARY (flushing diary entries)
 */
#define CL_FACILITY_DIARY (1ul << 31)

typedef enum cl_loglevel_level {
  /**
   * @brief Most verbose level possible
   */
  CL_LEVEL_ULTRA = 15,

  /**
   * @brief Developer debug output, extremely chatty.
   */
  CL_LEVEL_VERBOSE = 8,
  CL_LEVEL_SPEW = CL_LEVEL_VERBOSE,

  /**
   * @brief Developer and curious layperson debug output.
   */
  CL_LEVEL_DEBUG = 7,

  /**
   * @brief What's going on in the system?
   *
   * A non-implementor might enable loggign at this level to
   * explore the more detailed operation of a software.
   * Messages should tell a coherent story about how the
   * objects in the system are created, interact, and are destroyed.
   *
   * This is also the level at which protocol exchanges
   * should be logged.
   */
  CL_LEVEL_DETAIL = 6,

  /**
   * @brief What's going on in the system, roughly?
   *
   * This level might be enabled long-term in a running system
   * whose operators have a high tolerance for chatter.
   */
  CL_LEVEL_INFO = 5,

  /**
   * @brief Non-fatal failures that can happen in response
   * to non-privileged user input.
   */
  CL_LEVEL_FAIL = 4,

  /**
   * @brief High-level, rare messages, e.g. startup- and shutdown.
   *
   * This level might be enabled long-term in a running system
   * whose operators have a low tolerance for chatter.
   */
  CL_LEVEL_OVERVIEW = 3,

  /**
   * @brief Messages that should always be seen.
   *
   * (Likely) operator error.  When starting up, this is the
   * default log level above which messages are printed to
   * standard error.
   */
  CL_LEVEL_OPERATOR_ERROR = 2,

  /**
   * @brief Messages that should always be seen.
   *
   * This shouldn't happen, but it might not be a programmer
   * error - could be a resource shortage or extraordinary
   * circumstances.  On systems that support this, the error
   * message is printed along with a stack trace.
   */
  CL_LEVEL_ERROR = 1,

  /**
   * @brief Assertion failures.  Should always be logged.
   */
  CL_LEVEL_FATAL = 0

} cl_loglevel_level;

/**
 * @brief Flush policy
 *
 *  When writing to a file, how often should the file be
 *  flushed to disk?
 */
typedef enum cl_flush_policy {
  /**
   * @brief Flush after every write
   */
  CL_FLUSH_ALWAYS = 1,

  /**
   * @brief Don't flush; let the system buffering take care of it.
   */
  CL_FLUSH_NEVER = 0,

} cl_flush_policy;

/**
 * @brief Loglevel, as passed to cl_log() etc.
 *
 *  The value consists of exactly one CL_LEVEL_*, "or"ed
 *  with zero or more application-defined module bits between
 *  0x10 and 0x80000000.
 */
typedef unsigned long cl_loglevel;

typedef struct {
  /*  Anything with this level or better is logged *somewhere* -
   *  either to the output stream or to the diary ring buffer.
   */
  cl_loglevel clc_full;

  /*  Anything with this level or better is triggers
   *
   * 	(1) flushing the diary
   *	(2) writing the message itself to the writer.
   */
  cl_loglevel clc_trigger;

} cl_loglevel_configuration;

#ifndef DOCUMENTATION_GENERATOR_ONLY

/*  When a statement's loglvel (stmt) and a configured loglevel (conf) meet,
 *  the message is logged if either or both of the following is true:
 *
 * 	- the message's overall loglevel (last four bits) is <=
 * 	  (more important than, or as important as) the configured loglevel.
 *
 *      - any of the message's module bits are set in the configured bits.
 */
#define CL_IS_LOGGED(conf, stmt) \
  (((stmt)&0xF) <= ((conf)&0xF) || ((stmt) & (conf) & ~0xFul))
#endif

/**
 * @brief Pre-crash callback.
 *
 * This function, if installed with cl_set_abort(), is called just before
 * the library terminates in response to a failed cl_assert() or
 * cl_notreached() call.
 *
 * @param data opaque application data pointer;
 *	passed as third argument to cl_set_abort().
 */
typedef void cl_abort_callback(void *data);

/**
 * @brief Strerror callback.
 *
 * This function, if installed with cl_set_strerror(), is called
 * to resolve errors for cl_log_errno().
 *
 * @param data opaque application data pointer;
 *	passed as third argument to cl_set_strerror().
 */
typedef char const *cl_strerror_callback(void *data, int err);

/**
 * @brief Hard error callback.
 *
 * This function, if installed with cl_set_hard_error(), is called
 * to react to a "hard error" - something indiciative of serious
 * data corruption, but not necessarily due to programmer error.
 *
 * @param data opaque application data pointer;
 *	passed as third argument to cl_set_abort().
 */
typedef void cl_hard_error_callback(void *data);

/**
 * @brief Write callback for cl_handle implementations.
 * @param data opaque application data pointer;
 *	passed as third argument to cl_set_write().
 * @param level loglevel of the message, e.g. CL_LEVEL_ERROR
 * @param text NUL-terminated string that should be logged.
 */
typedef void cl_write_callback(void *data, cl_loglevel level, char const *text);

cl_handle *cl_create(void);
void cl_destroy(cl_handle *);

int cl_netlog3(cl_handle *, char const *);
int cl_netlog(cl_handle *, char const *);
int cl_netlog_set_ciid(cl_handle *cl, char const *ciid);
char const *cl_netlog_get_filename(cl_handle *cl);
int cl_netlog_set_filename(cl_handle *cl, char const *pattern);

int cl_netlog3(cl_handle *, char const *);

void cl_syslog(cl_handle *, char const *, int);

int cl_file(cl_handle *, char const *);
const char *cl_file_get_name(cl_handle *cl);
int cl_file_set_name(cl_handle *cl, const char *pattern);

cl_loglevel cl_get_loglevel_trigger(cl_handle *);
cl_loglevel cl_get_loglevel_full(cl_handle *);
void cl_get_loglevel_configuration(cl_handle *, cl_loglevel_configuration *);

void cl_set_loglevel_trigger(cl_handle *, cl_loglevel);
void cl_set_loglevel_full(cl_handle *, cl_loglevel);
void cl_set_loglevel_configuration(cl_handle *,
                                   cl_loglevel_configuration const *);

cl_flush_policy cl_get_flush_policy(cl_handle *);
void cl_set_flush_policy(cl_handle *, cl_flush_policy);

void cl_notreached(cl_handle *, char const *fmt, ...);

#ifndef DOCUMENTATION_GENERATOR_ONLY

#if CL_HAVE_C9X_VA_ARGS
#define cl_notreached(cl, ...) \
  cl_notreached_loc(cl, __FILE__, __LINE__, __VA_ARGS__)
#endif
#endif

void cl_notreached_loc(cl_handle *, char const *, int, char const *, ...)
#if __GNUC__
    __attribute__((format(printf, 4, 5)))
#endif
    ;

void cl_wnotreached(cl_handle *, char const *, ...);

#if CL_HAVE_C9X_VA_ARGS
#define cl_wnotreached(cl, ...) \
  cl_wnotreached_loc(cl, __FILE__, __LINE__, __VA_ARGS__)
#else
#endif

extern void cl_wnotreached_loc(cl_handle *, char const *, int, char const *,
                               ...)
#if __GNUC__
    __attribute__((format(printf, 4, 5)))
#endif
    ;

#ifdef DOCUMENTATION_GENERATOR_ONLY
/**
 * @brief Abort the program if an expression evaluates to false.
 *
 * If cl_assert's second argument evaluates to zero, a programmer error
 * has occurred, the call prints an error message, and the program
 * terminates with a call to abort() (which usually sends a SIGABRT,
 * signal 6.)
 *
 * If a pre-abort callback has been installed with cl_set_abort(),
 * that callback is called first.
 *
 * @param cl the module handle
 * @param expression an expression that the programmer guarantees
 * 	will evaluate to a non-zero value.
 */
void cl_assert(cl_handle cl, int expression) {}
void cl_assert_loc(cl_handle cl, int expression, char const *file, int line) {}
#else
#define cl_assert(cl, expr)                                                 \
  do                                                                        \
    if (!(expr))                                                            \
      cl_notreached_loc(cl, __FILE__, __LINE__, "assertion failed: \"%s\"", \
                        #expr);                                             \
  while (0)
#define cl_assert_loc(cl, expr, file, line)                                 \
  do                                                                        \
    if (!(expr))                                                            \
      cl_notreached_loc(cl, file, line, "assertion failed: \"%s\"", #expr); \
  while (0)
#endif

void cl_log(cl_handle *, cl_loglevel, char const *, ...)
#if __GNUC__
    __attribute__((format(printf, 3, 4)))
#endif
    ;

#ifdef DOCUMENTATION_GENERATOR_ONLY

/**
 * @brief Log a message about a failed call.
 *
 *  Normally, uses of cl_log_errno are redirected to cl_log_errno_loc()
 *  by a C9X varargs macro that inserts __FILE__, __LINE__, and
 *  __func__.  This function is for installations that don't have
 *  varargs macros.
 *
 * @param cl a log-handle created with cl_create().
 * @param level the loglevel of the message, e.g. #CL_LEVEL_DEBUG.
 * @param called the function causing the error, typically a unix system call
 * @param err the value of errno
 * @param fmt a printf-style format string, followed by its arguments.
 */

void cl_log_errno(cl_handle *cl, cl_loglevel level, char const *called, int err,
                  char const *fmt, ...) {}
#else

#if CL_HAVE_C9X_VA_ARGS
#define cl_log_errno(cl, level, called, err, ...)                        \
  cl_log_errno_loc(cl, level, __FILE__, __LINE__, __func__, called, err, \
                   __VA_ARGS__)
#else
void cl_log_errno(cl_handle *cl, cl_loglevel level, char const *called, int err,
                  char const *fmt, ...)
#if __GNUC__
    __attribute__((format(printf, 7, 8)))
#endif
    ;

#endif /* CL_HAVE_C9X_VARARGS */
#endif /* DOCUMENTATION_GENERATOR_ONLY */

void cl_log_errno_loc(cl_handle *cl, cl_loglevel level, char const *file,
                      int line, char const *caller, char const *called, int err,
                      char const *fmt, ...)
#if __GNUC__
    __attribute__((format(printf, 8, 9)))
#endif
    ;

/**
 * Something to write when you notice an error is being lost but
 * don't have time to go on a search/replace jihad.
 */

#define CL_LOST_ERROR(cl, err, fn) \
  cl_log_errno(cl, CL_LEVEL_ERROR, #fn, err, "Lost error")

#ifndef DOCUMENTATION_GENERATOR_ONLY

/*  Shortcut: we know the loglevel is the first element in the
 *  otherwise opaque cl_handle structure, and can thus skip a
 *  bunch of function calls.
 */
#define cl_is_logged(cl, level) \
  ((cl) != NULL && CL_IS_LOGGED(*(cl_loglevel const *)(cl), (level)))

#define cl_get_loglevel_full(cl) ((cl) != NULL ? *(cl_loglevel const *)(cl) : 0)

#define cl_get_loglevel_trigger(cl) \
  ((cl) != NULL ? ((cl_loglevel const *)(cl))[1] : 0)

#define cl_set_loglevel_full(cl, lev) \
  ((void)((cl) == NULL ? 0 : (*(cl_loglevel *)(cl) = (lev))))

#define cl_set_loglevel_trigger(cl, lev) \
  ((void)((cl) == NULL ? 0 : (((cl_loglevel *)(cl))[1] = (lev))))

#endif

#ifdef DOCUMENTATION_GENERATOR_ONLY

/**
 * @brief Log entry into a function.
 *
 *  A C9X varargs macro inserts __func__, the name of the calling
 *  function, at the beginning of the varargs string; it is not
 *  necessary to spell it out.
 *
 *  In the macro case, the varargs arguments are only evaluated
 *  if the loglevel is high enough for the results to be logged.
 *  (Don't rely on that for the sake of side effects; but do rely
 *  on it for performance.)
 *
 * @param cl a log-handle created with cl_create().
 * @param level the loglevel of the message, e.g. #CL_LEVEL_VERBOSE.
 * @param fmt a printf-style format string, followed by its arguments.
 */

void cl_enter(cl_handle *cl, cl_loglevel level, char const *fmt, ...) {}

/**
 * @brief Log normal return from a function.
 *
 *  A C9X varargs macro inserts __func__, the name of the calling
 *  function, at the beginning of the varargs string; it is not
 *  necessary to explicitly spell it out.
 *
 *  In the macro case, the varargs arguments are only evaluated
 *  if the loglevel is high enough for the results to be logged.
 *  (Don't rely on that for the sake of side effects; but do rely
 *  on it for performance.)
 *
 * @param cl a log-handle created with cl_create().
 * @param level the loglevel of the message, e.g. #CL_LEVEL_VERBOSE.
 * @param fmt a printf-style format string, followed by its arguments.
 */

void cl_leave(cl_handle *cl, cl_loglevel level, char const *fmt, ...) {}

/**
 * @brief Log exit from a function with an error result.
 *
 *  A C9X varargs macro inserts __func__, the name of the calling
 *  function, at the beginning of the varargs string; it is not
 *  necessary to explicitly spell it out.
 *
 *  In the macro case, the varargs arguments are only evaluated
 *  if the loglevel is high enough for the results to be logged.
 *  (Don't rely on that for the sake of side effects; but do rely
 *  on it for performance.)
 *
 *  Matching entry and exit statements are connected by  matching
 *  {} in the logfile (which, at least in vi, speeds traversal
 *  from starting point to end point.)  For that reason, it
 *  is useful to log leave and enter statements at the same
 *  loglevel.
 *
 * @param cl a log-handle created with cl_create().
 * @param level the loglevel of the message, e.g. #CL_LEVEL_VERBOSE.
 * @param err the errno-style error result of the function
 * @param fmt a printf-style format string, followed by its arguments.
 */

int cl_leave_err(cl_handle *cl, cl_loglevel level, int err, char const *fmt,
                 ...) {}

#else

/* Fast versions of cl_enter and cl_leave
 */
#if CL_HAVE_C9X_VA_ARGS

void cl_enter_func(cl_handle *cl, cl_loglevel lev, char const *func,
                   char const *fmt, ...)
#if __GNUC__
    __attribute__((format(printf, 4, 5)))
#endif
    ;

int cl_leave_err_func(cl_handle *cl, cl_loglevel lev, int err, char const *func,
                      char const *fmt, ...)
#if __GNUC__
    __attribute__((format(printf, 5, 6)))
#endif
    ;

#define cl_enter(cl, level, ...)                                             \
  (cl_is_logged(cl, level) ? cl_enter_func(cl, level, __func__, __VA_ARGS__) \
                           : (void)0)

#define cl_leave_err(cl, level, err, ...)                         \
  (cl_is_logged(cl, level)                                        \
       ? cl_leave_err_func(cl, level, err, __func__, __VA_ARGS__) \
       : (err))

#define cl_leave(cl, level, ...) (void) cl_leave_err(cl, level, 0, __VA_ARGS__)

#else

void cl_enter(cl_handle *cl, cl_loglevel level, char const *fmt, ...)
#if __GNUC__
    __attribute__((format(printf, 3, 4)))
#endif
    ;

void cl_leave(cl_handle *cl, cl_loglevel level, char const *fmt, ...)
#if __GNUC__
    __attribute__((format(printf, 3, 4)))
#endif
    ;

int cl_leave_err(cl_handle *cl, cl_loglevel level, int err, char const *fmt,
                 ...)
#if __GNUC__
    __attribute__((format(printf, 4, 5)))
#endif
    ;

#endif /* not CL_HAVE_C9X_VARARGS */
#endif /* not DOCUMENTATION_GENERATOR_ONLY */

/*  Don't execute a call to cl_log unless it actually will get logged
 *  (I.e., the loglevel is high enough.)
 *
 *  I'm thinking not just of the function call but of the calls
 *  used to evaluate the data that is printed.
 */
#if CL_HAVE_C9X_VA_ARGS
#define cl_log(cl, level, ...) \
  (cl_is_logged(cl, level) ? (cl_log)(cl, level, __VA_ARGS__) : (void)0)
#endif

#ifdef DOCUMENTATION_GENERATOR_ONLY
/**
 * @brief Record this line as a code coverage check point.
 *
 *  If code coverage logging is enabled (by calling
 *  cl_set_coverage() with a non-null pathname), the call logs
 *  a code coverage record that can be processed by code coverage
 *  analysis tools, e.g. cocoa.
 *
 * @param cl the module handle
 */
void cl_cover(cl_handle cl);
#else
extern void cl_cover_loc(cl_handle *, char const *, unsigned long);
#define cl_cover(cl)                        \
  do {                                      \
    static char cl_covered = 0;             \
    if (!cl_covered) {                      \
      cl_covered = 1;                       \
      cl_cover_loc(cl, __FILE__, __LINE__); \
    }                                       \
  } while (0)
#endif /* DOCUMENTATION_GENERATOR_ONLY */

void cl_vlog(cl_handle *, cl_loglevel, char const *, va_list);

void cl_set_abort(cl_handle *, cl_abort_callback *, void *);
void cl_set_strerror(cl_handle *, cl_strerror_callback *, void *);
void cl_set_hard_error(cl_handle *_cl, cl_hard_error_callback *_callback,
                       void *_data);
void cl_set_write(cl_handle *, cl_write_callback *, void *);
int cl_set_coverage(cl_handle *, char const *);
void cl_indent(cl_handle *, cl_loglevel lev, int i);

#define cl_indent(cl, level, i) \
  (cl_is_logged(cl, level) ? (cl_indent)(cl, level, i) : (void)0)
/**
 * @brief The chattiest hull of loglevels a and b.
 */
#define cl_loglevel_max(a, b) \
  (((((a)&0xF) > ((b)&0xF) ? (a) : (b)) & 0xF) | (((a) | (b)) & ~0xFul))

/**
 * @brief Structure used to manage application-defined loglevel module bits
 *
 *  A library that wants to declare its own orthogonal log-bits
 *  can do so by #defining its own loglevels
 *  <pre>
 *	#define MYLIB_LEVEL_FOO	0x0100
 *	#define MYLIB_LEVEL_BAR 0x0200
 *  </pre>
 *  and declaring a sentinel-terminated array of cl_facility structs
 *  that translate the command-line name of the application bit
 *  to the numbers.
 *  <pre>
 *	static const cl_facility mods[] = {
 *	  { "foo", MYLIB_LEVEL_FOO },
 *	  { "bar", MYLIB_LEVEL_FOO },
 *	  { 0 }
 *	};
 *  </pre>
 *  A null record terminates the array.
 *  Given such an array, the functions cl_loglevel_to_string()
 *  and cl_loglevel_from_string() can be used to translate
 *  back and forth between comma- or space-separated lists of
 *  loglevels and the single cl_loglevel bitmap.
 *
 */
typedef struct cl_facility {
  /**
   * @brief string option name, usable with cl_loglevel_from_string()
   */
  char const *fac_name;

  /**
   * @brief option bit, turns option on if set.
   */
  cl_loglevel fac_loglevel;

  /**
   * @brief reference to a separately defined list of options.
   *
   *  At the end of each cl_facility array, there
   *  can be zero or more elements with null fac_name
   *  but non-null fac_reference.  These references pull the
   *  bits listed in the pointed-to facilities into the set
   *  of names.
   */
  struct cl_facility const *fac_reference;

} cl_facility;

int cl_loglevel_from_string(char const *_s, char const *_e,
                            cl_facility const *_mods, cl_loglevel *_out);

int cl_loglevel_configuration_from_string(char const *_s, char const *_e,
                                          cl_facility const *_facs,
                                          cl_loglevel_configuration *_clc);

char const *cl_loglevel_to_string(cl_loglevel _lev, cl_facility const *_mods,
                                  char *_buf, size_t _size);

char const *cl_loglevel_configuration_to_string(
    cl_loglevel_configuration const *_clc, cl_facility const *_facs, char *_buf,
    size_t _size);

void cl_loglevel_configuration_max(cl_loglevel_configuration const *_a,
                                   cl_loglevel_configuration const *_b,
                                   cl_loglevel_configuration *_out);
/* cl-diary.c */

/**
 * @brief Iterator through a diary.
 */
typedef struct cl_diary_entry {
  /**
   * @brief 0 if not initialized, 1 if valid.
   */
  unsigned int de_initialized : 1;

  /**
   * @brief Pointer to current content byte in the current entry.
   */
  size_t de_offset;

  /**
   * @brief Number of content bytes left in the current entry.
   */
  size_t de_size;

  /**
   * @brief Serial of the current entry.
   */
  unsigned long long de_serial;

  /**
   * @brief Index of the first byte of the next entry in the ring buffer.
   *   	If this is equal to de_last (other than at initialization),
   * 	we're doing going through the entries.
   */
  size_t de_next;

  /**
   * @brief Once we point to this byte, we're done
   * 	reading the ring buffer.
   */
  size_t de_last;

} cl_diary_entry;

/**
 * @brief Opaque handle for a diary, created with cl_diary_create().
 */
typedef struct cl_diary_handle cl_diary_handle;
struct cm_handle;

cl_diary_handle *cl_diary_create(cl_handle *);

int cl_diary_set_size(cl_diary_handle *, size_t);

void cl_diary_destroy(cl_diary_handle *);

void cl_diary_entry_create(cl_diary_handle *_d, char const *_s, size_t _n);

void cl_diary_entry_add(cl_diary_handle *_d, char const *_s, size_t _n);

size_t cl_diary_entries(cl_diary_handle const *);

int cl_diary_entry_next(cl_diary_handle const *_d, cl_diary_entry *_de);

size_t cl_diary_entry_read(cl_diary_handle const *_d, cl_diary_entry *_de,
                           char *_buf, size_t _buf_size);

size_t cl_diary_entry_size(cl_diary_handle const *_d,
                           cl_diary_entry const *_de);

unsigned long long cl_diary_entry_serial(cl_diary_handle const *_d,
                                         cl_diary_entry const *_de);

size_t cl_diary_total_size(cl_diary_handle const *);
void cl_diary_log(cl_diary_handle *, cl_handle *, cl_loglevel lev);
void cl_diary_relog(cl_diary_handle *, cl_handle *);

void cl_diary(cl_handle *, cl_diary_handle *);
cl_diary_handle *cl_diary_get_handle(cl_handle *);
void cl_diary_truncate(cl_diary_handle *);

void cl_set_diary(cl_handle *_cl, cl_diary_handle *_diary);

/* cl-dup2.c */

int cl_dup2(cl_handle *, int);

/* cl-flush.c */

char const *cl_flush_policy_to_string(cl_flush_policy _pol, char *_buf,
                                      size_t _size);

int cl_flush_policy_from_string(char const *_s, char const *_e,
                                cl_flush_policy *_out);

cl_flush_policy cl_get_flush_policy(cl_handle *cl);
void cl_set_flush_policy(cl_handle *cl, cl_flush_policy fp);

/* cl-hard-error.c */

void cl_hard_error(cl_handle *cl);

/* cl-siphon.c */

void cl_set_siphon(cl_handle *cl, cl_write_callback *callback, void *data,
                   cl_loglevel level);

/* cl-set-stacktrace.c */

int cl_set_stacktrace(cl_handle *cl, bool wantstack);

/* cl-strerror.c */

char const *cl_strerror(cl_handle *cl, int err);

/**
 * @brief Version string for this library.
 */
extern char const cl_build_version[];

#ifdef __cplusplus
} /* end extern "C" */
#endif

#endif /* CL_H */
