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
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sysexits.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define PROCNAME "addbspec"

#define log(fmt, args...) plog(__FILE__, __LINE__, __FUNCTION__, fmt, ##args)

#define info(...)                          \
  {                                        \
    if (opt_verbose > 0) log(__VA_ARGS__); \
  }

#define debug(...)                         \
  {                                        \
    if (opt_verbose > 1) log(__VA_ARGS__); \
  }

#define warning(...) log("warning: " __VA_ARGS__)

#define error(...) log("error: " __VA_ARGS__)

/** Page shift length (in bits) */
#define PAGESHIFT 12  // 4 KB

/** Page size */
#define PAGESIZE ((size_t)(1 << PAGESHIFT))

/** Default number of data files */
#define DEFAULT_NFILES 10

/** Default number of pages per data file */
#define DEFAULT_NPAGES 2560  // 10 MB

/** Default total number of read and write operations */
#define DEFAULT_NOPS (DEFAULT_NPAGES * DEFAULT_NFILES)  // 100 MB

/** Default R/W ratio */
#define DEFAULT_RWRATIO 10  // 10 reads for every write

/** Keep and reuse data files */
static bool opt_keep = false;

/** Number of mmap'ed data files */
static unsigned int opt_nfiles = DEFAULT_NFILES;

/** Number of read or write transactions */
static unsigned int opt_nops = DEFAULT_NOPS;

/** Number of pages in each data file */
static unsigned int opt_npages = DEFAULT_NPAGES;

/** Seed to the random number generator (0=automatic) */
static unsigned int opt_seed = 0;

/** Read vs write approximate ratio (for instance, a ratio of 3 means 3R:1W, or
 * 3 read operations for every write operation). */
static unsigned int opt_rwratio = DEFAULT_RWRATIO;

/** Verbose level (0=errors and warnings, 1=info, 2=debug) */
static int opt_verbose = 0;

/**
 * A simple timer.
 */
struct timer {
  struct timeval start;
  struct timeval end;
  struct timeval diff;
};

/**
 * mincore() stats
 */
struct incore {
  double min;  ///< Minimum number of pages in memory
  double max;  ///< Maximum number of pages in memory
  double avg;  ///< Average number of pages in memory
};

/**
 * Statistics.
 */
static struct {
  unsigned int nreads;    ///< Number of reads
  unsigned int nwrites;   ///< Number of writes
  unsigned int nfsync;    ///< Calls to fsync
  double realtime;        ///< Total time
  double synctime;        ///< Time in fsync() function
  struct incore incore0;  ///< Pages in memory, beginning of test
  struct incore incore1;  ///< Pages in memory, end of test
} stats;

/**
 * File memory map descriptor.
 */
struct filemm {
  char name[64];  ///< File name
  void *ptr;      ///< Pointer to mmap'ed area
  int fd;
};

/** File memory maps */
static struct filemm *fmmaps;

/** Output page buffer */
static char pageout[PAGESIZE];

/** Input page buffer */
static char pagein[PAGESIZE];

/** Interrupted */
static volatile bool interrupted = false;

/**
 * Log to standard error.
 */
static int plog(const char *filename, int lineno, const char *func,
                const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  if (opt_verbose)
    fprintf(stderr, "%s:%d: %s(): ", filename, lineno, func);
  else
    fprintf(stderr, "%s: ", PROCNAME);
  vfprintf(stderr, fmt, ap);
  fputs("\n", stderr);
  va_end(ap);
  return 0;
}

/**
 * Start timer.
 */
static inline void timer_on(struct timer *t) { gettimeofday(&t->start, NULL); }

/**
 * Stop timer.
 */
static void timer_off(struct timer *t) {
  gettimeofday(&t->end, NULL);
  /* (NOTE: Assumes `t->end' is greater than `t->start') */
  t->diff.tv_sec = t->end.tv_sec - t->start.tv_sec;
  if (t->end.tv_usec >= t->start.tv_usec)
    t->diff.tv_usec = t->end.tv_usec - t->start.tv_usec;
  else {
    t->diff.tv_usec = 1000000 + t->end.tv_usec - t->start.tv_usec;
    t->diff.tv_sec--;
  }
}

static inline double timer_calc(const struct timer *t) {
  return ((double)t->diff.tv_sec) + (t->diff.tv_usec / 1.0e6);
}

/**
 * Return a random unsigned integer between 0 and `limit-1'.
 */
static inline unsigned int getuint(unsigned int limit) {
  return (((unsigned long)random()) % limit);
}

/**
 * Parse unsigned integer from string.
 */
static int parseuint(const char *str, unsigned int *val) {
  long long ret;
  char *bad;

  if (*str == '\0') return EINVAL;
  ret = strtoll(str, &bad, 0);
  if (*bad != '\0') return EINVAL;
  if ((ret < 0) || (ret > 0xffffffffLL)) return EINVAL;

  *val = ret;
  return 0;
}

/**
 * Format a size value.
 */
static const char *prettysize(unsigned long long size) {
  static char buf[256];
  if (size < (1 << 20))
    snprintf(buf, sizeof(buf), "%llu KB", size >> 10);
  else if (size < (1 << 30))
    snprintf(buf, sizeof(buf), "%llu MB", size >> 20);
  else
    snprintf(buf, sizeof(buf), "%.2f GB", ((double)size) / (1 << 30));
  return buf;
}

/**
 * Open or create data file.
 */
static int setup_file(int fl) {
  const mode_t o_mode = S_IRUSR | S_IWUSR;
  const int mm_prot = PROT_READ | PROT_WRITE;
  const int mm_flags = MAP_FILE | MAP_SHARED;
  const size_t filesz = opt_npages << PAGESHIFT;  // file size
  struct stat stbuf;
  int o_flags;
  bool exists;

  struct filemm *mm = &fmmaps[fl];

  /* data file name */
  sprintf(mm->name, "%s.%d.tmp", PROCNAME, fl);

  /* check whether the data file already exists */
  memset(&stbuf, 0, sizeof(stbuf));
  exists = (stat(mm->name, &stbuf) == 0);
  /* if the `-keep' option is present, do not truncate existing files */
  o_flags = O_CREAT | O_RDWR | (opt_keep ? 0 : O_TRUNC);

  /* create file */
  info("%s %s", opt_keep && exists ? "opening" : "creating", mm->name);
  if ((mm->fd = open(mm->name, o_flags, o_mode)) < 0) {
    error("open(): %s", strerror(errno));
    goto fail;
  }
  if (ftruncate(mm->fd, filesz) < 0) {
    error("ftruncate(): %s", strerror(errno));
    goto fail_1;
  }
#ifdef __linux__
  /* bring it all in, random access */
  const int f_advise = POSIX_FADV_WILLNEED | POSIX_FADV_RANDOM;
  if (posix_fadvise(mm->fd, 0, filesz, f_advise)) {
    error("fadvise(): %s", strerror(errno));
    goto fail_1;
  }
#endif
  /* map file to memory */
  mm->ptr = mmap(NULL, filesz, mm_prot, mm_flags, mm->fd, 0);
  if (mm->ptr == ((void *)-1)) {
    error("mmap(): %s", strerror(errno));
    goto fail_1;
  }

  /* initialize data file */
  if (!opt_keep || !exists || (stbuf.st_size < (off_t)filesz)) {
    unsigned int pg;
    /* write some data into the file */
    info("initializing %s", mm->name);
    for (pg = 0; pg < opt_npages; pg++) {
      const char val = (pg & 0xf) ?: 0xab;
      memset(mm->ptr + (pg << PAGESHIFT), val, PAGESIZE);
    }
    /* synchronize to disk */
    info("synchronizing %s", mm->name);
    if (fsync(mm->fd)) {
      error("fsync() error");
      goto fail_2;
    }
  }

  return 0;

fail_2:
  munmap(mm->ptr, filesz);
fail_1:
  close(mm->fd);
  unlink(mm->name);
fail:
  return EIO;
}

/**
 * Create data files and initialize resources.
 */
static int setup(void) {
  unsigned int fl;

  /* size in bytes of a data file */
  const size_t filesz = opt_npages << PAGESHIFT;

  int err = 0;

  /* initialize stats */
  memset(&stats, 0, sizeof(stats));

  /* initialize page in/out buffers */
  memset(pagein, 0x00, PAGESIZE);
  memset(pageout, 0xff, PAGESIZE);

  /* initialize random number generator */
  if (opt_seed == 0) {
    struct timeval tv;
    memset(&tv, 0, sizeof(tv));
    gettimeofday(&tv, NULL);
    opt_seed = (tv.tv_sec << 16) | (tv.tv_usec >> 4);
    info("seed is 0x%08x", opt_seed);
  }
  srandom(opt_seed);

  /* allocate file map descriptors */
  if ((fmmaps = calloc(opt_nfiles, sizeof(struct filemm))) == NULL) {
    error("insufficient memory");
    goto fail_malloc;
  }

  /* create or oper data files */
  for (fl = 0; fl < opt_nfiles; fl++) {
    /* create or open data file */
    if ((err = setup_file(fl))) goto fail_file;
    /* interrupted? */
    if (interrupted) {
      error("interrupted");
      goto fail_eintr;
    }
  }

  return 0;

fail_eintr:
  err = EINTR;
  info("cleaning up");
  fl++;
fail_file:
  for (; fl > 0; fl--) {
    struct filemm *mm = &fmmaps[fl - 1];
    munmap(mm->ptr, filesz);
    close(mm->fd);
    unlink(mm->name);
  }
fail_malloc:
  err = err ?: ENOMEM;
  return err;
}

/**
 * Remove data files, deallocate resources.
 */
static void cleanup(void) {
  unsigned int i;

  const size_t filesz = opt_npages << PAGESHIFT;

  info("cleaning up");

  /* unmap, close, and remove data files */
  for (i = 0; i < opt_nfiles; i++) {
    struct filemm *mm = &fmmaps[i];
    if (munmap(mm->ptr, filesz) < 0) warning("ignoring munmap() error");
    if (close(mm->fd)) warning("ignoring close() error");
    if (!opt_keep && unlink(mm->name)) warning("ignoring unlink() error");
  }

  free(fmmaps);
}

static int sync_page(int fd, unsigned int page) {
#if defined(__NR_sync_file_range) && defined(USE_SYNC_FILE_RANGE)
  struct timer t;
  /* file offset where the page begins */
  const loff_t offs = ((loff_t)page) << PAGESHIFT;
  /* sync_file_range() flags */
  const int flags = SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE;

  timer_on(&t);
  /* sync_file_range(fd, from, length, flags) */
  if (syscall(__NR_sync_file_range, fd, offs, PAGESIZE, flags)) {
    error("sync_file_range(): %s", strerror(errno));
    return EIO;
  }
  timer_off(&t);

  stats.synctime += timer_calc(&t);
#endif
  return 0;
}

/**
 * Invoke fsync() on all files.
 */
static int sync_all(void) {
  unsigned int fl;
  struct timer t;

  timer_on(&t);
  for (fl = 0; (fl < opt_nfiles) && !interrupted; fl++) {
    struct filemm *mm = &fmmaps[fl];
    if (fsync(mm->fd)) {
      error("fsync(): %s", strerror(errno));
      return EIO;
    }
  }
  timer_off(&t);

  stats.synctime += timer_calc(&t);
  stats.nfsync++;

  return 0;
}

/**
 * Calculate mincore() stats.
 */
static int calc_incore(struct incore *incore) {
  unsigned int fl;
  unsigned char *bitmap;

  /* length of a data fl, in bytes */
  const size_t length = opt_npages << PAGESHIFT;

  /* allocate page vector (one element per page) */
  if ((bitmap = malloc(opt_npages)) == NULL) {
    error("insufficient memory");
    return ENOMEM;
  }

  memset(incore, 0, sizeof *incore);
  for (fl = 0; fl < opt_nfiles; fl++) {
    const struct filemm *mm = &fmmaps[fl];
    unsigned int pg, count;
    double perc;

    /* get page vector of the current file */
    if (mincore(mm->ptr, length, (void *)bitmap)) {
      error("mincore(%s): %s", mm->name, strerror(errno));
      free(bitmap);
      return EIO;
    }

    /* calculate % of pages in memory */
    for (pg = 0, count = 0; pg < opt_npages; pg++)
      if (bitmap[pg] & 1) count++;
    perc = ((double)count) / ((double)opt_npages);

    /* update stats */
    if (fl == 0)
      *incore = (struct incore){perc, perc, perc};
    else {
      if (perc < incore->min) incore->min = perc;
      if (perc > incore->max) incore->max = perc;
      incore->avg += perc;
    }
  }
  incore->avg /= opt_nfiles;

  incore->min *= 100.0;
  incore->max *= 100.0;
  incore->avg *= 100.0;

  free(bitmap);
  return 0;
}

/**
 * Perform read operation.
 */
static int run_rd(unsigned int file, unsigned int page) {
  const struct filemm *mm = &fmmaps[file];

  debug("%s, page %u", mm->name, page);
  memcpy(pagein, mm->ptr + (page << PAGESHIFT), PAGESIZE);

  stats.nreads++;

  return 0;
}

/**
 * Perform write operation.
 */
static int run_wr(unsigned int file, unsigned int page) {
  struct filemm *mm = &fmmaps[file];
  off_t offs = ((off_t)page) << PAGESHIFT;
  int err;

  debug("%s, page %u *", mm->name, page);
  memcpy(mm->ptr + offs, pageout, PAGESIZE);

  stats.nwrites++;

  /* sync page to disk */
  if ((err = sync_page(mm->fd, page))) return err;

  return 0;
}

/**
 * Run test.
 */
static int run(void) {
  time_t timestamp;
  struct timer t;
  unsigned int op;
  int err;

  info("performing r/w transactions...");

  /* calculate incore stats */
  if ((err = calc_incore(&stats.incore0))) return err;

  /* start timer */
  timer_on(&t);

  time(&timestamp);
  for (op = 0; op < opt_nops; op++) {
    /* file to operate on */
    unsigned int file = getuint(opt_nfiles);
    /* page to operate on */
    unsigned int page = getuint(opt_npages);
    /* whether to read (>0) or write (0) */
    unsigned int rw = getuint(opt_rwratio + 1);

    /* run test */
    if ((err = rw ? run_rd(file, page) : run_wr(file, page))) return err;

    /* synchronize every 256 write operations */
    if ((stats.nwrites > 0) && ((stats.nwrites % 256) == 0))
      if ((err = sync_all())) return err;

    /* show how much is left, every 10 s */
    if ((time(NULL) - timestamp) >= 10) {
      time(&timestamp);
      info("[%u ops, %.2f %%]", op + 1, ((double)op + 1) / opt_nops * 100.0);
    }

    /* interrupted? */
    if (interrupted) {
      error("interrupted");
      return EINTR;
    }
  }

  /* execute one last fsync() on all files */
  info("final fsync()");
  if ((err = sync_all())) return err;

  /* end timer */
  timer_off(&t);
  stats.realtime = timer_calc(&t);

  /* calculate incore stats */
  if ((err = calc_incore(&stats.incore1))) return err;

  return 0;
}

/**
 * Show results.
 */
static void output(void) {
  char path[512];
  char host[128];

  double rwtime = stats.realtime - stats.synctime;
  double rwperc = rwtime / stats.realtime * 100;
  double syncperc = stats.synctime / stats.realtime * 100.0;

  /* directory */
  if (getcwd(path, sizeof(path)) == NULL) strcpy(path, "unknown");
  /* host name */
  if (gethostname(host, sizeof(host))) strcpy(host, "unknown");

  printf("Location       : %s:%s\n", host, path);
  printf("Random seed    : 0x%08x\n", opt_seed);
  printf("Data files     : %u\n", opt_nfiles);
  printf("Pages/file     : %u\n", opt_npages);
  printf("Operations     : %u\n", opt_nops);
  printf("  Read ops     : %u\n", stats.nreads);
  printf("  Write ops    : %u\n", stats.nwrites);
  printf("Total time     : %.6f s\n", stats.realtime);
  printf("  R/W time     : %.6f s (%.2f %%)\n", rwtime, rwperc);
  printf("  fsync() time : %.6f s (%.2f %%)\n", stats.synctime, syncperc);
  printf("fsync() calls  : %u\n", stats.nfsync);
  printf("mincore() start: min=%6.2f%%, max=%6.2f%%, avg=%6.2f%%\n",
         stats.incore0.min, stats.incore0.max, stats.incore0.avg);
  printf("mincore() end  : min=%6.2f%%, max=%6.2f%%, avg=%6.2f%%\n",
         stats.incore1.min, stats.incore1.max, stats.incore1.avg);
}

/**
 * Print usage information
 */
static void help(void) {
  fprintf(
      stderr,
      "Usage:\n"
      "   %s [options]\n"
      "\nOptions:\n"
      "   -nfiles=NUM    Number of data files to be created [%d]\n"
      "   -npages=NUM    Number of pages in a data file (1 page = 4KB) [%d]\n"
      "   -nops=NUM      Number of read and write operations "
      "[nfiles*npages:%d]\n"
      "   -seed=VAL      Seed to the random number generator [auto]\n"
      "   -rwratio=NUM   Read vs write approximate ratio [%d]\n"
      "   -keep          Keep and reuse data files [no]\n"
      "   -v             Show what is being done\n"
      "   -vv            Dump every single R/W transaction to stderr\n"
      "   -help          Print this help page\n",
      PROCNAME, DEFAULT_NFILES, DEFAULT_NPAGES, DEFAULT_NOPS, DEFAULT_RWRATIO);
}

/**
 * Parse command line arguments.
 */
static int init(int argc, char *argv[]) {
  enum {
    OPT_HELP,
    OPT_KEEP,
    OPT_NFILES,
    OPT_NOPS,
    OPT_NPAGES,
    OPT_RANDSEED,
    OPT_RWRATIO,
    OPT_V,
    OPT_VV
  };

  struct option opts[] = {{"help", no_argument, NULL, OPT_HELP},
                          {"keep", no_argument, NULL, OPT_KEEP},
                          {"nfiles", required_argument, NULL, OPT_NFILES},
                          {"nops", required_argument, NULL, OPT_NOPS},
                          {"npages", required_argument, NULL, OPT_NPAGES},
                          {"seed", required_argument, NULL, OPT_RANDSEED},
                          {"rwratio", required_argument, NULL, OPT_RWRATIO},
                          {"v", no_argument, NULL, OPT_V},
                          {"vv", no_argument, NULL, OPT_VV},
                          {NULL, 0, NULL, 0}};

  int opt;
  int err;

  /* parse command line arguments */
  while ((opt = getopt_long_only(argc, argv, "", opts, NULL)) != -1) {
    switch (opt) {
      case OPT_HELP:
        help();
        exit(0);
      case OPT_KEEP:
        opt_keep = true;
        break;
      case OPT_NFILES:
        if ((err = parseuint(optarg, &opt_nfiles))) goto fail_value;
        if (opt_nfiles == 0) goto fail_value;
        opt_nops = opt_npages * opt_nfiles;
        break;
      case OPT_NPAGES:
        if ((err = parseuint(optarg, &opt_npages))) goto fail_value;
        if (opt_npages == 0) goto fail_value;
        opt_nops = opt_npages * opt_nfiles;
        break;
      case OPT_NOPS:
        if ((err = parseuint(optarg, &opt_nops))) goto fail_value;
        if (opt_nops == 0) goto fail_value;
        break;
      case OPT_RANDSEED:
        if ((err = parseuint(optarg, &opt_seed))) goto fail_value;
        break;
      case OPT_RWRATIO:
        if ((err = parseuint(optarg, &opt_rwratio))) goto fail_value;
        break;
      case OPT_V:
        opt_verbose = 1;
        break;
      case OPT_VV:
        opt_verbose = 2;
        break;
      default:
        return EINVAL;
    }
  }

  const unsigned long long filesz = opt_npages << PAGESHIFT;
  info("data file size: %u pages (%s)", opt_npages, prettysize(filesz));
  info("number of data files: %u files", opt_nfiles);
  info("total memory: %s", prettysize(filesz * opt_nfiles));
  info("r/w operations: %u ops", opt_nops);

  return 0;

fail_value:
  error("invalid value `%s'", optarg);
  return err ?: EINVAL;
}

/**
 * CTRL^C
 */
static void onsigint(int signo) { interrupted = true; }

int main(int argc, char *argv[]) {
  int err;

  signal(SIGINT, onsigint);

  /* parse command-line args */
  if (init(argc, argv)) exit(EX_USAGE);

  /* create data files */
  if ((err = setup())) goto fail_0;
  /* run tests */
  if ((err = run())) goto fail_1;
  /* clean up */
  cleanup();
  /* print results */
  output();

  exit(0);

fail_1:
  cleanup();
fail_0:
  exit(err == EINTR ? EX_SOFTWARE : EX_OSERR);
}
