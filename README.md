Zort-Benchmarks
===============

Building
--------

To compile use `./configure && make` (see `./configure -h` for options).

Usage
-----

usage: zort [ <option> ] [ <benchmarks> ] <directory>

where '<option>' is one of the following:

  -h | --help         print this command line summary
  -q | --quiet        no messages at all (default disabled)
  -v | --verbose      print verbose messages (default disabled)
  -k | --keep         keep benchmark order (but compute and print costs)
  -g | --generate     generate and print new benchmarks order
  -o <output>         set output (otherwise 'stdout', implies '-g')
  -b <cores>          cores per bucket aka bucket-size (default 64)
  -f <percent>        fraction of fast buckets in percent (default 50%)
  -l <memory>         fast bucket memory limit in MB (default 8000 MB)
  -n <nodes>          assumed number of available nodes (default 32)
  -m <memory>         assumed memory in MB per node (default 234000 MB)
  -w <watt>           assumed Watt per core (default 8 Watt)
  -c <cents>          assumed cents per kWh (default 27 cents)
  --euro              assume '€' as currency sign (default)
  --dollar            assume '$' as currency sign

This tool is supposed to be given two arguments, a 'benchmarks' file and a
'directory', where 'benchmarks' is a file which has three fields per line
separated by spaces. The first gives the benchmark order the second gives
the path to the benchmark and the third a unique name of the benchmark.
If only two entries are given per line in 'benchmarks' we assume the path
was ommitted.  The 'directory' is supposed to contain a 'zummary' file
produced by the 'zummarize' tool (which is meant to parse 'runlim' output).

If 'benchmarks' is missing it is searched as 'benchmarks' next to 'zummary'
in the given directory.  If both are giving, i.e., a directory and a
file they can occur in arbitrary order. The tool then reads both files
and tries to match names.  If this is successful it sorts the benchmarks
according to the memory usage of that recorded run and time needed to
solve them and puts them into buckets of the given size (default 64).

It then produces a new list of benchmarks ordered by the bucket assignment.
If requested through '-g' this list is also printed to 'stdout' (in the
same format as the original benchmark file, i.e., with two or three entries
per line).  On 'stderr' it reports expected maximum running time per bucket
(if all jobs in that bucket / task are run in parallel) and the sum of the
memory usage of those jobs.  If no benchmark list is generated and printed
this information of the computed statistics and costs go to 'stdout'.
The '-v' and '-q' options determine the amount of information printed.

The primary goal is to maximize memory usage per job / benchmark, while
trying to stay below a total limit of available cores per task (SLURM
parlance).  The secondary goal is to minimize the maximum running time
per bucket for a fast terminating fraction (default half) of the buckets.
Ultimately our objective is to minimize the running cost in terms of
power needed for the number of allocated cores.
