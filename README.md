Zort-Benchmarks
---------------

To compile use `make`.

This tool is supposed to be given two arguments, a 'benchmarks' file and a
'directory', where 'benchmarks' is file which has three fields per line
separated by spaces, where the first gives the benchmark order the second
gives the path to the benchmark and the third a unique name of the
benchmark.  The 'directory' is supposed to contain a 'zummary' file
produced by the 'zummarize' tool (which is meant to parse 'runlim' output).
If 'benchmark' is missing it is searched as 'benchmarks' next 'zummary' in
the same directory.  The tool then reads both files and tries to match
names.  If this is successful it sorts the benchmarks according to the
memory usage and time needed to solve them and puts them into buckets of the
given size (default 64). It then produces a new list of benchmarks ordered
by the bucket assignment on 'stdout'.  The primary goal is to maximize
memory availability per bucket, and the secondary goal is to minimize the
maximum running time per bucket.
