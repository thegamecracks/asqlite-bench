import argparse
import asyncio
import logging
import sys

from .runners import runner
from .queries import load_query_spec


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

    queries = load_query_spec(args.queries)
    await runner(
        queries,
        cleanup=args.cleanup,
        n_connections=args.connections,
    )


if __name__ == "__main__":
    asyncio.run(main())
