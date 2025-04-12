// clang-format off

#define FAST_BUCKET_FRACTION 50
#define FAST_BUCKET_MEMORY 8000

static const char * usage =
"usage: zort [ <option> ] [ <benchmarks> ] <directory>\n"
"\n"
"where '<option>' is one of the following:\n"
"\n"
"  -h | --help     print this command line summary\n"
"  -q | --quiet    no messages at all (default disabled)\n"
"  -v | --verbose  print verbose messages (default disabled)\n"
"  -f <percent>    fraction of fast buckets in percent (default %u%%)\n"
"  -l <memory>     fast bucket memory limit in MB (default %u MB)\n"
"  -<n>            bucket size (default 64)\n"
"\n"

"This tool is supposed to be given two arguments, a 'benchmarks' file and a\n"
"'directory', where 'benchmarks' is a file which has three fields per line\n"
"separated by spaces. The first gives the benchmark order the second gives\n"
"the path to the benchmark and the third a unique name of the benchmark.\n"
"The 'directory' is supposed to contain a 'zummary' file produced by the\n"
"'zummarize' tool (which is meant to parse 'runlim' output).\n"
"\n"
"If 'benchmark' is missing it is searched as 'benchmarks' next 'zummary'\n"
"in the given directory.  The tool then reads both files and tries to\n"
"match names.  If this is successful it sorts the benchmarks according to\n"
"the memory usage of that recorded run and time needed to solve them and\n"
"puts them into buckets of the given size (default 64).  It then produces\n"
"a new list of benchmarks ordered by the bucket assignment on 'stdout' (in\n"
"the same format as the original benchmark file) and on 'stderr' reports\n"
"expected maximum running time per bucket (if all jobs in that bucket /\n"
"task are run in parallel) and the sum of the memory usage of those jobs.\n"
"\n"
"The primary goal is to maximize memory usage per job / benchmark, while\n"
"trying to stay a total limit of available per task (SLURM parlance) and\n"
"the secondary goal is to minimize the maximum running time per bucket\n"
"for a fast fraction (default half) of the buckets.\n"

;

// clang-format on

#include <assert.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

struct benchmark {
  size_t number;
  char *path;
  char *name;
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
};

struct bucket {
  double real;
  double memory;
  size_t size;
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

static const char *benchmarks_path;
static char *missing_benchmarks_path;
static const char *directory_path;
static char *zummary_path;

static int verbosity;
static size_t bucket_size;
static size_t last_bucket_size;
static size_t tasks;

static struct bucket *buckets;
static size_t scheduled;

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
static void vrb(const char *, ...) __attribute__((format(printf, 1, 2)));

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
  fflush(stdout);
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fputc('\n', stderr);
  fflush(stderr);
}

static void vrb(const char *fmt, ...) {
  if (verbosity < 1)
    return;
  fflush(stdout);
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fputc('\n', stderr);
  fflush(stderr);
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

static void parse_benchmark(struct benchmark *benchmark) {
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

static void sort_zummaries_by_memory() {
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

static void sort_zummaries_by_time() {
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

static void schedule_zummary(struct bucket *bucket, struct zummary *zummary) {
  assert(!zummary->scheduled);
  assert(bucket->size < bucket_size);
  bucket->zummaries[bucket->size++] = zummary;
  if (bucket->real < zummary->real)
    bucket->real = zummary->real;
  bucket->memory += zummary->memory;
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

int main(int argc, char **argv) {
  for (int i = 1; i != argc; i++) {
    const char *arg = argv[i];
    if (!strcmp(arg, "-h") || !strcmp(arg, "--help")) {
      printf(usage, FAST_BUCKET_FRACTION, FAST_BUCKET_MEMORY);
      fflush(stdout);
      return 0;
    } else if (!strcmp(arg, "-q") || !strcmp(arg, "--quiet"))
      verbosity = -1;
    else if (!strcmp(arg, "-v") || !strcmp(arg, "--verbose"))
      verbosity = 1;
    else if (arg[0] == '-' && isdigit(arg[1])) {
      bucket_size = arg[1] - '0';
      const size_t max_size_t = ~(size_t)0;
      char ch;
      for (const char *p = arg + 2; (ch = *p); p++) {
        if (max_size_t / 10 < bucket_size)
        INVALID_BUCKET_SIZE:
          die("invalid bucket size '%s'", arg + 1);
        bucket_size *= 10;
        const unsigned digit = ch - '0';
        if (max_size_t - digit < bucket_size)
          goto INVALID_BUCKET_SIZE;
        bucket_size += digit;
      }
      if (!bucket_size)
        goto INVALID_BUCKET_SIZE;
    } else if (arg[0] == '-')
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
  init_line_reading(benchmarks_file, benchmarks_path);
  while (read_line()) {
    struct benchmark benchmark;
    parse_benchmark(&benchmark);
    push_benchmark(&benchmark);
  }
  fclose(benchmarks_file);
  if (!size_benchmarks)
    die("could not find any benchmark in '%s'", benchmarks_path);
  msg("parsed %zu benchmarks in '%s'", size_benchmarks, benchmarks_path);
  init_line_reading(zummary_file, zummary_path);
  if (!read_line())
    die("failed to read header line in '%s'", zummary_path);
  while (read_line()) {
    struct zummary zummary;
    parse_zummary(&zummary);
    push_zummary(&zummary);
  }
  fclose(zummary_file);
  msg("parsed %zu zummaries in '%s'", size_zummaries, zummary_path);
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
  }
  if (size_benchmarks == size_zummaries)
    msg("zummaries and benchmarks match (found %zu of both)", size_zummaries);
  else
    die("%zu benchmarks different from %zu zummaries", size_benchmarks,
        size_zummaries);
  if (bucket_size)
    msg("using specified bucket size %zu", bucket_size);
  else {
    bucket_size = 64;
    msg("using default bucket size %zu", bucket_size);
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
    msg("need %zu tasks "
        "(%zu full buckets and one with %zu benchmarks)",
        tasks, tasks - 1, last_bucket_size);
  }
  buckets = calloc(tasks, sizeof *buckets);
  if (!buckets)
    out_of_memory("allocating buckets");
  for (size_t i = 0; i != tasks; i++)
    if (!(buckets[i].zummaries =
              malloc(bucket_size * sizeof *buckets[i].zummaries)))
      out_of_memory("allocating bucket");
  sort_zummaries_by_time();
  size_t j = 0;
  for (size_t i = 0; i != size_zummaries; i++) {
    struct zummary *zummary = zummaries + i;
    if (zummary->status != 10 && zummary->status != 20)
      continue;
    if (zummary->memory > 8000)
      continue;
    assert(!zummary->scheduled);
    struct bucket *bucket = buckets + j;
    schedule_zummary(bucket, zummary);
    zummary->scheduled = true;
    if (buckets[j].size >= bucket_size && ++j == (tasks + 0) / 2)
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
  size_t printed = 0;
  for (size_t i = 0; i != tasks; i++) {
    struct bucket *bucket = buckets + i;
    msg("task[%zu] maximum-time %.2f, total-memory %.2f", i + 1, bucket->real,
        bucket->memory);
    for (size_t j = 0; j != bucket->size; j++) {
      struct zummary *zummary = bucket->zummaries[j];
      struct benchmark *benchmark = zummary->benchmark;
      assert(zummary->scheduled);
      assert(benchmark);
      vrb("  %.2f %.2f %s", zummary->real, zummary->memory, zummary->name);
      printf("%zu %s %s\n", ++printed, benchmark->path, zummary->name);
    }
  }
  fflush(stdout);
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
