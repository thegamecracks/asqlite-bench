# yappi -c wall -o 1-inserts.stats yappi_entrypoint.py queries\1-inserts.json
import asyncio

from asqlite_bench import __main__

asyncio.run(__main__.main())
