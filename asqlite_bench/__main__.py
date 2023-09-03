import argparse
import asyncio
import logging
import sys

from .runners import runner
from .queries import load_query_spec


def start_yappi():
    try:
        import yappi
    except ModuleNotFoundError:
        sys.exit("yappi must be installed to use -p/--profile option")

    yappi.set_clock_type("WALL")
    yappi.start()


def stop_yappi_and_dump(filename: str) -> None:
    import yappi

    yappi.stop()
    stats = yappi.get_func_stats()
    stats.save(filename, "pstat")

    # # Merge stats from all threads
    # # NOTE: this doesn't seem to help with tracking sqlite3 calls
    # with tempfile.TemporaryDirectory() as dirname:
    #     stats_dir = Path(dirname)
    #
    #     threads = list(yappi.get_thread_stats())
    #     first_stats = yappi.get_func_stats(ctx_id=threads[0].id)
    #
    #     for thread in threads[1:]:
    #
    #         stats = yappi.get_func_stats(ctx_id=thread.id)
    #         if first_stats is None:
    #             first_stats = stats
    #         else:
    #             stats_file = stats_dir / str(thread.id)
    #             stats_filename = str(stats_file)
    #             stats.save(stats_filename)
    #             first_stats.add(stats_filename)
    #
    #     first_stats.save(filename, "pstat")


async def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-c",
        "--connections",
        default=10,
        help="number of concurrent connections",
        type=int,
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_false",
        dest="cleanup",
        help="skip deletion of database file",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        help="increase logging verbosity",
    )
    parser.add_argument(
        "-p",
        "--profile",
        help="a filename to dump pstats results to (requires yappi)",
    )
    parser.add_argument(
        "queries",
        help="query specification file to run",
        type=argparse.FileType(encoding="utf-8"),
    )

    args = parser.parse_args()

    if args.verbose:
        level = logging.INFO
        if args.verbose > 1:
            level = logging.DEBUG

        logging.basicConfig(
            level=level,
            stream=sys.stdout,
        )

    if args.profile:
        start_yappi()

    queries = load_query_spec(args.queries)
    await runner(
        queries,
        cleanup=args.cleanup,
        n_connections=args.connections,
    )

    if args.profile:
        stop_yappi_and_dump(args.profile)


if __name__ == "__main__":
    asyncio.run(main())
