// clang-format off

#define BUCKET_SIZE 64
#define FAST_BUCKET_FRACTION 50
#define FAST_BUCKET_MEMORY 8000
#define AVAILABLE_NODES 32
#define AVAILABLE_MEMORY 234000
#define WATT_PER_CORE 8
#define CENTS_PER_KWH 27

static const char * usage =
"usage: zort [ <option> ] [ <benchmarks> ] <directory>\n"
"\n"
"where '<option>' is one of the following:\n"
"\n"
"  -h | --help         print this command line summary\n"
"  -q | --quiet        no messages at all (default disabled)\n"
"  -v | --verbose      print verbose messages (default disabled)\n"
"  -k | --keep         keep benchmark order (but compute and print costs)\n"
"  -g | --generate     generate and print new benchmarks order\n"
"  -b <cores>          cores per bucket aka bucket-size (default %d)\n"
"  -f <percent>        fraction of fast buckets in percent (default %d%%)\n"
"  -l <memory>         fast bucket memory limit in MB (default %d MB)\n"
"  -n <nodes>          assumed number of available nodes (default %d)\n"
"  -m <memory>         assumed memory in MB per node (default %d MB)\n"
"  -w <watt>           assumed Watt per core (default %d Watt)\n"
"  -c <cents>          assumed cents per kWh (default %d cents)\n"
"  --euro              assume '€' as currency sign (default)\n"
"  --dollar            assume '$' as currency sign\n"
"\n"

"This tool is supposed to be given two arguments, a 'benchmarks' file and a\n"
"'directory', where 'benchmarks' is a file which has three fields per line\n"
"separated by spaces. The first gives the benchmark order the second gives\n"
"the path to the benchmark and the third a unique name of the benchmark.\n"
"If only two entries are give per line in 'benchmarks' we assume the path\n"
"was ommitted.  The 'directory' is supposed to contain a 'zummary' file\n"
"produced by the 'zummarize' tool (which is meant to parse 'runlim' output).\n"
"\n"
"If 'benchmarks' is missing it is searched as 'benchmarks' next to 'zummary'\n"
"in the given directory.  If both are giving, i.e., a directory and a\n"
"file they can occur in arbitrary order. The tool then reads both files\n"
"and tries to match names.  If this is successful it sorts the benchmarks\n"
"according to the memory usage of that recorded run and time needed to\n"
"solve them and puts them into buckets of the given size (default 64).\n"
"\n"
"It then produces a new list of benchmarks ordered by the bucket assignment.\n"
"If requested through '-g' this list is also printed to 'stdout' (in the\n"
"same format as the original benchmark file, i.e., with two or three entries\n"
"per line).  On 'stderr' it reports expected maximum running time per bucket\n"
"(if all jobs in that bucket / task are run in parallel) and the sum of the\n"
"memory usage of those jobs.  If no benchmark list is generated and printed\n"
"this information of the computed statistics and costs go to 'stdout'.\n"
"The '-v' and '-q' options determine the amount of information printed.\n"
"\n"
"The primary goal is to maximize memory usage per job / benchmark, while\n"
"trying to stay below a total limit of available cores per task (SLURM\n"
"parlance).  The secondary goal is to minimize the maximum running time\n"
"per bucket for a fast terminating fraction (default half) of the buckets.\n"
"Ultimately our objective is to minimize the running cost in terms of\n"
"power needed for the number of allocated cores.\n"

;

// clang-format on

#include "config.h"

#include <assert.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

struct zummary;

struct benchmark {
  size_t number;
  char *path;
  char *name;
  struct zummary *zummary;
};

struct zummary {
  char *name;
  int status;
  double time;
  double real;
  double memory;
  struct {
    double time;
    double real;
    double memory;
  } limit;
  struct benchmark *benchmark;
  bool scheduled;
  bool memory_limit_hit;
};

struct bucket {
  double real;
  double memory;
  size_t size;
  bool finished;
  double start, end;
  size_t memory_limit_hit;
  struct zummary **zummaries;
};

static char *line;
static size_t size_line, capacity_line;

static FILE *file;
static const char *file_name;
static size_t lineno;

static struct zummary *zummaries;
static size_t size_zummaries, capacity_zummaries;

static struct benchmark *benchmarks;
static size_t size_benchmarks, capacity_benchmarks;
static int entries_per_benchmark_line;

static const char *benchmarks_path;
static char *missing_benchmarks_path;
static const char *directory_path;
static char *zummary_path;
static double max_memory;

static bool keep;
static int verbosity;
static bool generate;
static unsigned fast_bucket_fraction;
static unsigned fast_bucket_memory;
static size_t bucket_size;
static size_t last_bucket_size;
static size_t tasks;

static size_t max_memory_limit_hit;
static struct bucket *buckets;
static size_t scheduled;

static size_t size_nodes;
static struct bucket **nodes;

static size_t size_memory;

static bool use_euro_sign = true;
static int watt_per_core = -1;
static int cents_per_kwh = -1;

static struct zummary *find_zummary(const char *name) {
  for (size_t i = 0; i != size_zummaries; i++)
    if (!strcmp(name, zummaries[i].name))
      return zummaries + i;
  return 0;
}

static struct benchmark *find_benchmark(const char *name) {
  for (size_t i = 0; i != size_benchmarks; i++)
    if (!strcmp(name, benchmarks[i].name))
      return benchmarks + i;
  return 0;
}

static void die(const char *, ...) __attribute__((format(printf, 1, 2)));
static void msg(const char *, ...) __attribute__((format(printf, 1, 2)));
static void vrb(int, const char *, ...) __attribute__((format(printf, 2, 3)));

static void die(const char *fmt, ...) {
  fputs("zort: error: ", stderr);
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fputc('\n', stderr);
  exit(1);
}

static void msg(const char *fmt, ...) {
  if (verbosity < 0)
    return;
  FILE *message_file;
  if (generate) {
    fflush(stdout);
    message_file = stderr;
  } else
    message_file = stdout;
  va_list ap;
  va_start(ap, fmt);
  vfprintf(message_file, fmt, ap);
  va_end(ap);
  fputc('\n', message_file);
  fflush(message_file);
}

static void vrb(int level, const char *fmt, ...) {
  if (verbosity < level)
    return;
  FILE *message_file;
  if (generate) {
    fflush(stdout);
    message_file = stderr;
  } else
    message_file = stdout;
  va_list ap;
  va_start(ap, fmt);
  vfprintf(message_file, fmt, ap);
  va_end(ap);
  fputc('\n', message_file);
  fflush(message_file);
}

static void out_of_memory(const char *what) { die("out-of-memory %s", what); }

static void push_char(int ch) {
  assert(ch != EOF);
  assert(ch != '\n');
  if (size_line == capacity_line) {
    capacity_line = capacity_line ? 2 * capacity_line : 1;
    line = realloc(line, capacity_line);
    if (!line)
      out_of_memory("reallocating line");
  }
  line[size_line++] = ch;
}

static bool file_exists(const char *path) {
  struct stat buf;
  return !stat(path, &buf) && (buf.st_mode & S_IFMT) == S_IFREG;
}

static bool directory_exists(const char *path) {
  struct stat buf;
  return !stat(path, &buf) && (buf.st_mode & S_IFMT) == S_IFDIR;
}

static void init_line_reading(FILE *f, const char *name) {
  file = f;
  file_name = name;
  lineno = 0;
}

static bool read_line(void) {
  int ch = fgetc(file);
  if (ch == EOF)
    return false;
  lineno++;
  if (ch == '\n')
    die("empty line %zu in '%s'", lineno, file_name);
  size_line = 0;
  push_char(ch);
  while ((ch = fgetc(file)) != '\n')
    if (ch == EOF)
      die("unexpected end-of-file before new-line in line %zu in '%s'", lineno,
          file_name);
    else if (!ch)
      die("unexpected zero character in line %zu in '%s'", lineno, file_name);
    else
      push_char(ch);
  push_char(0);
  return true;
}

static void determine_entries_per_benchmark_line(void) {
  assert(!entries_per_benchmark_line);
  const char *p = line;
  int spaces = 0;
  char ch;
  while ((ch = *p++))
    if (ch == ' ')
      spaces++;
  if (!spaces)
    die("expected at least one space in line %zu in '%s'", lineno, file_name);
  else if (spaces > 2)
    die("%d spaces in line %zu in '%s' (expected 2 or 3)", spaces, lineno,
        file_name);
  entries_per_benchmark_line = spaces + 1;
  if (entries_per_benchmark_line == 2)
    vrb(1, "found two entries per benchmark line");
  else {
    assert(entries_per_benchmark_line == 3);
    vrb(1, "found three entries per benchmark line");
  }
}

static void parse_benchmark2(struct benchmark *benchmark) {
  char *p = line;
  size_t number = 0;
  if (!isdigit(*p))
  EXPECTED_DIGIT:
    die("expected digit in line %zu in '%s'", lineno, file_name);
  char ch;
  while ((ch = *p++) != ' ')
    if (!isdigit(ch))
      goto EXPECTED_DIGIT;
    else
      number = 10 * number + (ch - '0');
  benchmark->number = number;
  char *q = p;
  while ((ch = *p))
    if (ch == ' ')
      die("unexpected second space in line %zu in '%s'", lineno, file_name);
    else
      p++;
  benchmark->path = 0;
  if (!(benchmark->name = strdup(q)))
    out_of_memory("copying benchmark name");
}

static void parse_benchmark3(struct benchmark *benchmark) {
  char *p = line;
  size_t number = 0;
  if (!isdigit(*p))
  EXPECTED_DIGIT:
    die("expected digit in line %zu '%s'", lineno, file_name);
  char ch;
  while ((ch = *p++) != ' ')
    if (!isdigit(ch))
      goto EXPECTED_DIGIT;
    else
      number = 10 * number + (ch - '0');
  benchmark->number = number;
  char *q = p;
  while ((ch = *p) != ' ')
    if (!ch)
      die("line %zu truncated in '%s'", lineno, file_name);
    else
      p++;
  *p++ = 0;
  if (!(benchmark->path = strdup(q)))
    out_of_memory("copying benchmark path in");
  if (!(benchmark->name = strdup(p)))
    out_of_memory("copying benchmark name");
}

static void parse_benchmark(struct benchmark *benchmark) {
  if (!entries_per_benchmark_line)
    determine_entries_per_benchmark_line();
  if (entries_per_benchmark_line == 2)
    parse_benchmark2(benchmark);
  else
    parse_benchmark3(benchmark);
}

static void push_benchmark(struct benchmark *benchmark) {
  if (size_benchmarks == capacity_benchmarks) {
    capacity_benchmarks = capacity_benchmarks ? 2 * capacity_benchmarks : 1;
    benchmarks = realloc(benchmarks, capacity_benchmarks * sizeof *benchmarks);
    if (!benchmarks)
      out_of_memory("reallocating benchmarks");
  }
  benchmarks[size_benchmarks++] = *benchmark;
}

static void parse_zummary(struct zummary *zummary) {
  char *p = line, ch;
  while ((ch = *p) != ' ')
    if (!ch)
      die("line %zu truncated in '%s'", lineno, file_name);
    else
      p++;
  *p++ = 0;
  if (!(zummary->name = strdup(line)))
    out_of_memory("allocating zummary name");
  if (sscanf(p, "%d %lf %lf %lf %lf %lf %lf", &zummary->status, &zummary->time,
             &zummary->real, &zummary->memory, &zummary->limit.time,
             &zummary->limit.real, &zummary->limit.memory) != 7)
    die("invalid zummary line %zu in '%s'", lineno, file_name);
  if (max_memory < zummary->memory)
    max_memory = zummary->memory;
}

static void push_zummary(struct zummary *zummary) {
  if (size_zummaries == capacity_zummaries) {
    capacity_zummaries = capacity_zummaries ? 2 * capacity_zummaries : 1;
    zummaries = realloc(zummaries, capacity_zummaries * sizeof *zummaries);
    if (!zummaries)
      out_of_memory("reallocating zummaries");
  }
  zummaries[size_zummaries++] = *zummary;
}

static void sort_zummaries_by_memory(void) {
  assert(size_zummaries);
  for (size_t i = 0; i != size_zummaries - 1; i++) {
    if (zummaries[i].scheduled)
      continue;
    for (size_t j = i + 1; j != size_zummaries; j++) {
      if (zummaries[j].scheduled)
        continue;
      if (zummaries[i].memory < zummaries[j].memory)
        continue;
      if (zummaries[i].memory == zummaries[j].memory &&
          zummaries[i].real <= zummaries[j].real)
        continue;
      struct zummary tmp = zummaries[i];
      zummaries[i] = zummaries[j];
      zummaries[j] = tmp;
    }
  }
}

static void sort_zummaries_by_time(void) {
  assert(size_zummaries);
  for (size_t i = 0; i != size_zummaries - 1; i++) {
    if (zummaries[i].scheduled)
      continue;
    for (size_t j = i + 1; j != size_zummaries; j++) {
      if (zummaries[j].scheduled)
        continue;
      if (zummaries[i].real < zummaries[j].real)
        continue;
      if (zummaries[i].real == zummaries[j].real &&
          zummaries[i].memory <= zummaries[j].memory)
        continue;
      struct zummary tmp = zummaries[i];
      zummaries[i] = zummaries[j];
      zummaries[j] = tmp;
    }
  }
}

static void sort_buckets_by_real(void) {
  assert(tasks);
  for (size_t i = 0; i != tasks; i++)
    for (size_t j = i + 1; j != tasks; j++) {
      if (buckets[i].real <= buckets[j].real)
        continue;
      struct bucket tmp = buckets[i];
      buckets[i] = buckets[j];
      buckets[j] = tmp;
    }
}

static void schedule_zummary(struct bucket *bucket, struct zummary *zummary) {
  assert(!zummary->scheduled);
  assert(bucket->size < bucket_size);
  bucket->zummaries[bucket->size++] = zummary;
  if (bucket->real < zummary->real)
    bucket->real = zummary->real;
  bucket->memory += zummary->memory;
  if (zummary->status == 2 || zummary->memory >= zummary->limit.memory) {
    zummary->memory_limit_hit = true;
    bucket->memory_limit_hit++;
    if (max_memory_limit_hit < bucket->memory_limit_hit)
      max_memory_limit_hit = bucket->memory_limit_hit;
  } else
    zummary->memory_limit_hit = false;
  zummary->scheduled = true;
  scheduled++;
}

static size_t next_bucket(size_t j) {
  assert(j < tasks);
  size_t res = j;
  for (;;) {
    if (++res == tasks)
      res = 0;
    size_t max_size = (res + 1 == tasks) ? last_bucket_size : bucket_size;
    if (buckets[res].size < max_size)
      return res;
  }
}

static double average(double a, double b) { return b ? a / b : a; }

static double percent(double a, double b) { return average(100 * a, b); }

int main(int argc, char **argv) {
  const char *quiet_options = 0;
  const char *verbose_option = 0;
  for (int i = 1; i != argc; i++) {
    const char *arg = argv[i];
    if (!strcmp(arg, "-h") || !strcmp(arg, "--help")) {
      printf(usage, BUCKET_SIZE, FAST_BUCKET_FRACTION, FAST_BUCKET_MEMORY,
             WATT_PER_CORE);
      fflush(stdout);
      return 0;
    } else if (!strcmp(arg, "-q") || !strcmp(arg, "--quiet")) {
      if (verbose_option)
        die("unexpected '%s' option after '%s'", arg, verbose_option);
      quiet_options = arg;
      verbosity = -1;
    } else if (!strcmp(arg, "-v") || !strcmp(arg, "--verbose")) {
      if (quiet_options)
        die("unexpected '%s' option after '%s'", arg, quiet_options);
      if (verbosity == 2)
        die("can not increase verbosity more than two times");
      verbose_option = arg;
      verbosity++;
    } else if (!strcmp(arg, "-k") || !strcmp(arg, "--keep"))
      keep = true;
    else if (!strcmp(arg, "-g") || !strcmp(arg, "--generate"))
      generate = true;
    else if (!strcmp(arg, "-b")) {
      if (++i == argc)
      ARGUMENT_MISSING:
        die("argument to '%s' missing", arg);
      int tmp = atoi(argv[i]);
      if (tmp <= 0)
      INVALID_ARGUMENT:
        die("invalid argument in '%s %s'", arg, argv[i]);
      bucket_size = tmp;
    } else if (!strcmp(arg, "-f")) {
      if (++i == argc)
        goto ARGUMENT_MISSING;
      int tmp = atoi(argv[i]);
      if (tmp < 0)
        goto INVALID_ARGUMENT;
      fast_bucket_fraction = tmp;
    } else if (!strcmp(arg, "-l")) {
      if (++i == argc)
        goto ARGUMENT_MISSING;
      int tmp = atoi(argv[i]);
      if (tmp < 0)
        goto INVALID_ARGUMENT;
      fast_bucket_memory = tmp;
    } else if (!strcmp(arg, "-n")) {
      if (++i == argc)
        goto ARGUMENT_MISSING;
      int tmp = atoi(argv[i]);
      if (tmp < 0)
        goto INVALID_ARGUMENT;
      size_nodes = tmp;
    } else if (!strcmp(arg, "-m")) {
      if (++i == argc)
        goto ARGUMENT_MISSING;
      int tmp = atoi(argv[i]);
      if (tmp < 0)
        goto INVALID_ARGUMENT;
      size_memory = tmp;
    } else if (!strcmp(arg, "-w")) {
      if (++i == argc)
        goto ARGUMENT_MISSING;
      int tmp = atoi(argv[i]);
      if (tmp < 0)
        goto INVALID_ARGUMENT;
      watt_per_core = tmp;
    } else if (!strcmp(arg, "-c")) {
      if (++i == argc)
        goto ARGUMENT_MISSING;
      int tmp = atoi(argv[i]);
      if (tmp < 0)
        goto INVALID_ARGUMENT;
      cents_per_kwh = tmp;
    } else if (!strcmp(arg, "--euro"))
      use_euro_sign = true;
    else if (!strcmp(arg, "--dollar"))
      use_euro_sign = false;
    else if (arg[0] == '-')
      die("invalid option '%s' (try '-h')", arg);
    else if (!benchmarks_path)
      benchmarks_path = arg;
    else if (!directory_path)
      directory_path = arg;
    else
      die("too many arguments '%s', '%s' and '%s' (try '-h')", benchmarks_path,
          directory_path, arg);
  }
  if (!benchmarks_path) {
    assert(!directory_path);
    die("benchmark and directory path missing (try '-h')");
  }
  if (!directory_path) {
    directory_path = benchmarks_path;
    if (!directory_exists(directory_path))
    DIRECTORY_DOES_NOT_EXISTS:
      die("directory '%s' does not exist", directory_path);
    size_t missing_benchmarks_path_len =
        strlen(directory_path) + strlen("benchmarks") + 2;
    missing_benchmarks_path = malloc(missing_benchmarks_path_len);
    if (!missing_benchmarks_path)
      out_of_memory("allocating missing benchmarks paths");
    snprintf(missing_benchmarks_path, missing_benchmarks_path_len, "%s/%s",
             directory_path, "benchmarks");
    benchmarks_path = missing_benchmarks_path;
  }
  if (benchmarks_path && directory_path &&
      directory_exists (benchmarks_path) && file_exists (directory_path)) {
    const char * tmp = benchmarks_path;
    benchmarks_path = directory_path;
    directory_path = tmp;
  }
  if (!file_exists(benchmarks_path))
    die("benchmarks file '%s' does not exist", benchmarks_path);
  FILE *benchmarks_file = fopen(benchmarks_path, "r");
  if (!benchmarks_file)
    die("could not open and read '%s'", benchmarks_path);
  if (!missing_benchmarks_path && !directory_exists(directory_path))
    goto DIRECTORY_DOES_NOT_EXISTS;
  size_t zummary_path_len = strlen(directory_path) + strlen("zummary") + 2;
  zummary_path = malloc(zummary_path_len);
  if (!zummary_path_len)
    out_of_memory("allocating zummary path");
  snprintf(zummary_path, zummary_path_len, "%s/%s", directory_path, "zummary");
  if (!file_exists(zummary_path))
    die("zummary file '%s' does not exist", zummary_path);
  FILE *zummary_file = fopen(zummary_path, "r");
  if (!zummary_file)
    die("could not open and read '%s'", zummary_path);
  if (verbosity >= 0) {
    FILE * message_file = generate ? stderr : stdout;
    fprintf (message_file, "Zort Benchmark Sorting\n");
    fprintf (message_file, "Copyright (c) 2025 Armin Biere, University of Freiburg\n");
    fprintf (message_file, "Version %s", VERSION);
    if (IDENTIFIER && *IDENTIFIER)
      fprintf (message_file, " %s", IDENTIFIER);
    fputc ('\n', message_file);
    fprintf (message_file, "Compiled %s\n", COMPILE);
    fflush (message_file);
  }
  init_line_reading(benchmarks_file, benchmarks_path);
  while (read_line()) {
    struct benchmark benchmark;
    parse_benchmark(&benchmark);
    push_benchmark(&benchmark);
  }
  fclose(benchmarks_file);
  if (!size_benchmarks)
    die("could not find any benchmark in '%s'", benchmarks_path);
  vrb(1, "parsed %zu benchmarks in '%s'", size_benchmarks, benchmarks_path);
  init_line_reading(zummary_file, zummary_path);
  if (!read_line())
    die("failed to read header line in '%s'", zummary_path);
  while (read_line()) {
    struct zummary zummary;
    parse_zummary(&zummary);
    push_zummary(&zummary);
  }
  fclose(zummary_file);
  vrb(1, "parsed %zu zummaries in '%s'", size_zummaries, zummary_path);
  for (size_t i = 0; i != size_zummaries; i++) {
    struct zummary *zummary = zummaries + i;
    struct benchmark *benchmark = find_benchmark(zummary->name);
    if (!benchmark)
      die("could not find zummary entry '%s' in benchmarks", zummary->name);
    zummary->benchmark = benchmark;
    zummary->scheduled = false;
  }
  for (size_t i = 0; i != size_benchmarks; i++) {
    struct benchmark *benchmark = benchmarks + i;
    struct zummary *zummary = find_zummary(benchmark->name);
    if (!zummary)
      die("could not find benchmark entry '%s' in zummary", benchmark->name);
    benchmark->zummary = zummary;
  }
  if (size_benchmarks == size_zummaries)
    vrb(1, "zummaries and benchmarks match (found %zu of both)",
        size_zummaries);
  else
    die("%zu benchmarks different from %zu zummaries", size_benchmarks,
        size_zummaries);
  if (bucket_size)
    vrb(1, "using specified bucket size %zu", bucket_size);
  else {
    bucket_size = 64;
    vrb(1, "using default bucket size %zu", bucket_size);
  }
  if (fast_bucket_fraction)
    vrb(1, "using specified fast bucket fraction %u%%", fast_bucket_fraction);
  else {
    fast_bucket_fraction = FAST_BUCKET_FRACTION;
    vrb(1, "using default fast bucket fraction %u%%", fast_bucket_fraction);
  }
  if (fast_bucket_memory)
    vrb(1, "using specified fast bucket memory limit of %u MB",
        fast_bucket_memory);
  else {
    fast_bucket_memory = FAST_BUCKET_MEMORY;
    vrb(1, "using default fast bucket memory limit of %u MB",
        fast_bucket_memory);
  }
  if (size_nodes)
    vrb(1, "assuming specified number of nodes %zu", size_nodes);
  else {
    size_nodes = AVAILABLE_NODES;
    vrb(1, "assuming default number of nodes %zu", size_nodes);
  }
  if (size_memory)
    vrb(1, "assuming specified available memory of %zu MB", size_memory);
  else {
    size_memory = AVAILABLE_MEMORY;
    vrb(1, "assuming default available meoory of %zu MB", size_memory);
  }
  if (watt_per_core >= 0)
    vrb(1, "using specified %d Watt per core", watt_per_core);
  else {
    watt_per_core = WATT_PER_CORE;
    vrb(1, "using default %d Watt per core", watt_per_core);
  }
  if (cents_per_kwh >= 0)
    vrb(1, "using specified %d cents per kWh", cents_per_kwh);
  else {
    cents_per_kwh = CENTS_PER_KWH;
    vrb(1, "using default %d cents per kWh", cents_per_kwh);
  }
  tasks = size_benchmarks / bucket_size;
  if (tasks * bucket_size == size_benchmarks) {
    msg("need exactly %zu tasks "
        "(number of benchmarks multiple of bucket size)",
        tasks);
    last_bucket_size = bucket_size;
  } else {
    tasks++;
    last_bucket_size = size_benchmarks % bucket_size;
    msg("need %zu buckets "
        "(%zu full with %zu and one with %zu benchmarks)",
        tasks, tasks - 1, bucket_size, last_bucket_size);
  }
  buckets = calloc(tasks, sizeof *buckets);
  if (!buckets)
    out_of_memory("allocating buckets");
  for (size_t i = 0; i != tasks; i++)
    if (!(buckets[i].zummaries =
              malloc(bucket_size * sizeof *buckets[i].zummaries)))
      out_of_memory("allocating bucket");
  if (keep) {
    for (size_t i = 0, j = 0; i != size_benchmarks; i++) {
      struct benchmark * benchmark = benchmarks + i;
      struct zummary *zummary = benchmark->zummary;
      assert (zummary);
      assert(!zummary->scheduled);
      assert (zummary->benchmark == benchmark);
      struct bucket *bucket = buckets + j;
      schedule_zummary(bucket, zummary);
      if (buckets[j].size >= bucket_size)
        j++;
    }
  } else {
    sort_zummaries_by_time();
    size_t j = 0, limit = (fast_bucket_fraction * tasks) / 100u;
    for (size_t i = 0; i != size_zummaries; i++) {
      struct zummary *zummary = zummaries + i;
      if (zummary->status != 10 && zummary->status != 20)
        continue;
      if (zummary->memory > fast_bucket_memory)
        continue;
      assert(!zummary->scheduled);
      struct bucket *bucket = buckets + j;
      schedule_zummary(bucket, zummary);
      zummary->scheduled = true;
      if (buckets[j].size >= bucket_size && ++j == limit)
        break;
    }
    sort_zummaries_by_memory();
    size_t last = size_zummaries;
    j = tasks - 1;
    for (;;) {
      struct zummary *zummary = zummaries + --last;
      if (zummary->scheduled)
        continue;
      struct bucket *bucket = buckets + j;
      schedule_zummary(bucket, zummary);
      zummary->scheduled = true;
      if (scheduled != size_zummaries)
        j = next_bucket(j);
      else
        break;
    }
  }
  size_t printed = 0;
  double sum_real = 0;
  double max_total_memory = 0;
  for (size_t i = 0; i != tasks; i++) {
    struct bucket *bucket = buckets + i;
    vrb(1, "bucket[%zu] maximum-time %.2f seconds, total-memory %.0f MB", i + 1,
        bucket->real, bucket->memory);
    if (bucket->memory > max_total_memory)
      max_total_memory = bucket->memory;
    sum_real += bucket->real;
    for (size_t j = 0; j != bucket->size; j++) {
      struct zummary *zummary = bucket->zummaries[j];
      struct benchmark *benchmark = zummary->benchmark;
      assert(zummary->scheduled);
      assert(benchmark);
      vrb(2, "  %.2f %.2f %s%s", zummary->real, zummary->memory, zummary->name,
          zummary->memory_limit_hit ? " *" : "");
      if (!generate)
        continue;
      printf("%zu", ++printed);
      if (benchmark->path)
        fputc(' ', stdout), fputs(benchmark->path, stdout);
      fputc(' ', stdout), fputs(zummary->name, stdout);
      fputc('\n', stdout);
    }
  }
  fflush(stdout);
  msg("maximum bucket-memory %.0f MB (%.0f%% of %zu MB available)", 
      max_total_memory, percent (max_total_memory, size_memory), size_memory);
  msg("maximum benchmark-memory %.0f MB (%.0f%% maximum bucket-memory)",
      max_memory, percent(max_memory, max_total_memory));
  if (verbosity > 0 || max_memory_limit_hit)
    msg("maximum of %zu times memory-limit exceeded in one bucket", max_memory_limit_hit);
  vrb(1, "sum of maximum running times per bucket %.0f seconds", sum_real);
  double core_seconds = bucket_size * sum_real;
  double core_hours = core_seconds / 3600;
  msg("allocated core-time of %.2f core-hours (%.0f = %zu * %.0f sec)",
      core_hours, core_seconds, bucket_size, sum_real);
  double power_usage = core_hours * watt_per_core / 1000.0;
  msg("power usage of %.3f kWh (%u W * %.2f h / 1000)", power_usage,
      watt_per_core, core_hours);
  sort_buckets_by_real();
  nodes = calloc(size_nodes, sizeof *nodes);
  if (!nodes)
    out_of_memory("allocating nodes");
  double latency = 0;
  for (size_t i = 0; i != tasks; i++) {
    struct bucket *next = buckets + i;
    struct bucket *replace = 0;
    const size_t invalid_position = ~(size_t)0;
    size_t pos = invalid_position;
    for (size_t j = 0; j != size_nodes; j++) {
      struct bucket *prev = nodes[j];
      if (!prev) {
        replace = 0;
        pos = j;
        break;
      }
      if (!replace || prev->end < replace->end) {
        replace = prev;
        pos = j;
      }
    }
    double start = replace ? replace->end : 0;
    double end = start + next->real;
    next->start = start;
    next->end = end;
    assert(pos != invalid_position);
    vrb(1, "running bucket[%zu] at node %zu after %.0f seconds (%.0f..%.0f)", i + 1, 
            pos, next->start, next->start, next->end);
    nodes[pos] = next;
    if (end > latency)
      latency = end;
  }
  msg("latency of %.0f seconds (%.2f h running %zu nodes in parallel)",
      latency, latency / 2600, size_nodes);
  double costs = cents_per_kwh * power_usage / 100.0;
  msg("costs %s %.2f (¢ %d * %.3f kWh / 100)", use_euro_sign ? "€" : "$", costs,
      cents_per_kwh, power_usage);
  free(nodes);
  for (size_t i = 0; i != tasks; i++)
    free(buckets[i].zummaries);
  free(buckets);
  for (size_t i = 0; i != size_zummaries; i++)
    free(zummaries[i].name);
  for (size_t i = 0; i != size_benchmarks; i++)
    free(benchmarks[i].path), free(benchmarks[i].name);
  free(zummaries);
  free(benchmarks);
  free(missing_benchmarks_path);
  free(zummary_path);
  free(line);
  return 0;
}
