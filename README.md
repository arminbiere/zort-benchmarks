Zort-Benchmarks
===============

Building
--------

To compile use `make`.

Usage
-----

This tool is supposed to be given two arguments, a 'benchmarks' file and a
'directory', where 'benchmarks' is a file which has three fields per line
separated by spaces. The first gives the benchmark order the second gives
the path to the benchmark and the third a unique name of the benchmark.
The 'directory' is supposed to contain a 'zummary' file produced by the
'zummarize' tool (which is meant to parse 'runlim' output).

If 'benchmark' is missing it is searched as 'benchmarks' next 'zummary'
in the given directory.  The tool then reads both files and tries to
match names.  If this is successful it sorts the benchmarks according to
the memory usage of that recorded run and time needed to solve them and
puts them into buckets of the given size (default 64).

It then produces a new list of benchmarks ordered by the bucket assignment
on 'stdout' (in the same format as the original benchmark file) and on
'stderr' reports expected maximum running time per bucket (if all jobs in
that bucket / task are run in parallel) and the sum of the memory usage
of those jobs.

The primary goal is to maximize memory usage per job / benchmark, while
trying to stay a total limit of available per task (SLURM parlance) and
the secondary goal is to minimize the maximum running time per bucket
for a fast fraction (default half) of the buckets.
