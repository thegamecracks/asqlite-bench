# asqlite-bench

This repository contains my own preliminary benchmarks for [asqlite].

## Requirements

Benchmarks should be ran using Python 3.11 or greater.

## Usage

1. Clone this repository
2. Install asqlite with `pip install git+https://github.com/Rapptz/asqlite`
3. Run `python -m asqlite_bench queries/1-inserts.toml`

> [!WARNING]
>
> The following benchmarks depend on [#13](https://github.com/Rapptz/asqlite/pull/13)
> being merged into asqlite for database cleanup to run correctly.
> Until this is complete, the above command should be amended with `--no-cleanup`,
> requiring you to delete the database file artifacts after each run.
> Alternatively, you can install the PR yourself with
> `pip install git+https://github.com/thegamecracks/asqlite@bf435508d507aec3929b6d3bc3cd94dfe82efa81`.

## Profiling

This benchmark package has native support for the [yappi] asynchronous profiler.
To profile the benchmarks:

1. Install yappi with `pip install yappi`
2. Use the `-p/--profile` option to write a [pstats] file after completion, e.g.
   `python -m asqlite_bench queries/1-inserts.toml --profile`

You may also want to use [snakeviz], a browser-based visualizer for profiler
stat outputs.

[asqlite]: https://github.com/Rapptz/asqlite
[yappi]: https://github.com/sumerc/yappi
[pstats]: https://docs.python.org/3/library/profile.html#pstats.Stats
[snakeviz]: https://pypi.org/project/snakeviz/
