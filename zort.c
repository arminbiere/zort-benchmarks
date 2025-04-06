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
static const char *directory_path;
static char *zummary_path;

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

static void die(const char *fmt, ...) {
  fputs("zort: error: ", stderr);
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fputc('\n', stderr);
  exit(1);
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
  if (sscanf(p, "%lf %lf %lf %lf %lf %lf", &zummary->time, &zummary->real,
             &zummary->memory, &zummary->limit.time, &zummary->limit.real,
             &zummary->limit.memory) != 6)
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

int main(int argc, char **argv) {
  if (argc != 3) {
    fputs("usage: zort <benchmarks> <dir>\n", stderr);
    exit(1);
  }
  benchmarks_path = argv[1];
  directory_path = argv[2];
  if (!file_exists(benchmarks_path))
    die("benchmarks file '%s' does not exist", benchmarks_path);
  FILE *benchmarks_file = fopen(benchmarks_path, "r");
  if (!benchmarks_file)
    die("could not open and read '%s'", benchmarks_path);
  if (!directory_exists(directory_path))
    die("directory '%s' does not exist", directory_path);
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
  init_line_reading(zummary_file, zummary_path);
  if (!read_line())
    die("failed to read header line in '%s'", zummary_path);
  while (read_line()) {
    struct zummary zummary;
    parse_zummary(&zummary);
    push_zummary(&zummary);
  }
  fclose(benchmarks_file);
  fclose(zummary_file);
  for (size_t i = 0; i != size_zummaries; i++) {
    struct zummary *zummary = zummaries + i;
    struct benchmark *benchmark = find_benchmark(zummary->name);
    if (!benchmark)
      die("could not find zummary entry '%s' in benchmarks", zummary->name);
    zummary->benchmark = benchmark;
  }
  for (size_t i = 0; i != size_benchmarks; i++) {
    struct benchmark *benchmark = benchmarks + i;
    struct zummary *zummary = find_zummary(benchmark->name);
    if (!zummary)
      die("could not find benchmark entry '%s' in zummary", benchmark->name);
  }
  for (size_t i = 0; i != size_zummaries; i++)
    free(zummaries[i].name);
  for (size_t i = 0; i != size_benchmarks; i++)
    free(benchmarks[i].path), free(benchmarks[i].name);
  free(zummaries);
  free(benchmarks);
  free(zummary_path);
  free(line);
  return 0;
}
