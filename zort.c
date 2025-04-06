#include <assert.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

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
};

struct benchmark {
  size_t number;
  char *path;
  char *name;
};

static char *line;
static size_t size_line, capacity_line;

static struct zummary *zummaries;
static size_t size_zummaries, capacity_zummaries;

static struct benchmark *benchmarks;
static size_t size_benchmarks, capacity_benchmarks;

static const char *benchmarks_path;
static const char *directory_path;
static char *zummary_path;

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

static bool read_line(FILE *file, const char *path) {
  int ch = fgetc(file);
  if (ch == EOF)
    return false;
  if (ch == '\n')
    die("empty line in '%s'", path);
  size_line = 0;
  push_char(ch);
  while ((ch = fgetc(file)) != '\n')
    if (ch == EOF)
      die("unexpected end-of-file before new-line in '%s'", path);
    else if (!ch)
      die("unexpected zero character in '%s'", path);
    else
      push_char(ch);
  push_char(0);
  return true;
}

static void parse_benchmark(struct benchmark *benchmark, const char *path) {
  char *p = line;
  size_t number = 0;
  if (!isdigit(*p))
  EXPECTED_DIGIT:
    die("expected digit in '%s'", path);
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
      die("unexpected end-of-line in '%s'", path);
    else
      p++;
  *p++ = 0;
  if (!(benchmark->path = strdup(q)))
    out_of_memory("copying benchmark path in");
  if (!(benchmark->name = strdup(p + 1)))
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

static void parse_zummary(struct zummary *zummary, const char *path) {}

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
  directory_path = argv[1];
  if (!file_exists(benchmarks_path))
    die("benchmarks file '%s' does not exist", benchmarks_path);
  FILE *benchmarks_file = fopen(benchmarks_path, "r");
  if (!benchmarks_file)
    die("could not open and read '%s'", benchmarks_path);
  if (!directory_exists(directory_path))
    die("directory '%s' does not exist", directory_path);
  size_t zummary_path_len = strlen(directory_path) + strlen("/zummary") + 1;
  zummary_path = malloc(zummary_path_len);
  if (!zummary_path_len)
    out_of_memory("allocating zummary path");
  if (!file_exists(zummary_path))
    die("zummary file '%s' does not exist", zummary_path);
  snprintf(zummary_path, zummary_path_len, directory_path, "/zummary");
  FILE *zummary_file = fopen(zummary_path, "r");
  if (!zummary_file)
    die("could not open and read '%s'", zummary_path);
  while (!read_line(benchmarks_file, benchmarks_path)) {
    struct benchmark benchmark;
    parse_benchmark(&benchmark, benchmarks_path);
    push_benchmark(&benchmark);
  }
  if (!read_line(zummary_file, zummary_path))
    die("failed to read header line in '%s'", zummary_path);
  while (read_line(zummary_file, zummary_path)) {
    struct zummary zummary;
    parse_zummary(&zummary, zummary_path);
    push_zummary(&zummary);
  }
  fclose(benchmarks_file);
  fclose(zummary_file);
  for (size_t i = 0; i != size_zummaries; i++)
    free(benchmarks[i].name);
  for (size_t i = 0; i != size_benchmarks; i++)
    free(benchmarks[i].path), free(benchmarks[i].name);
  free(benchmarks);
  free(zummary_path);
  free(line);
  return 0;
}
