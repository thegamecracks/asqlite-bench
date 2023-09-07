import argparse
import asyncio
import logging
import sqlite3
import sys
from pathlib import Path

from .runners import runner
from .queries import load_query_spec

log = logging.getLogger(__name__)


def start_yappi():
    try:
        import yappi
    except ModuleNotFoundError:
        sys.exit("yappi must be installed to use -p/--profile option")

    yappi.set_clock_type("WALL")
    yappi.start(builtins=True)


def stop_yappi_and_dump(filename: str) -> None:
    import yappi

    yappi.stop()
    stats = yappi.get_func_stats()
    stats.save(filename, "pstat")


def resolve_profile_path(args: argparse.Namespace) -> str:
    if args.profile.lower() != "auto":
        return args.profile

    path = Path(args.queries.name)
    path = path.with_stem(
        "{stem}-c{c}".format(
            stem=path.stem,
            c=args.connections,
        )
    )
    path = path.with_suffix(".stats")
    return path.name


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
        const="auto",
        help="a filename to dump pstats results to (requires yappi)",
        nargs="?",
    )
    parser.add_argument(
        "queries",
        help="query specification file to run",
        type=argparse.FileType("rb"),
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

    log.info("SQLite version: %s", sqlite3.sqlite_version)

    if args.profile:
        start_yappi()
        args.profile = resolve_profile_path(args)

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
