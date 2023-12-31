import io
from typing import Any, Iterator, Sequence


class QuerySpec:
    """Defines a set of queries to execute.

    Parameters
    ----------
    setup: str
        One or more queries to run before the benchmark.
    query: str
        The query to be benchmarked.
    repeat: int
        The number of times to repeat the query.
    args: list[Any]
        The arguments to pass to the query.

        This can be a list of values::

            [1, 2, 3, 4]

        Or a list of rows, for multiple parameters::

            [[1, "a"], [2, "b"]]

        Or a list of dictionaries following the given structure::

            {
                "row": [1, "a"],
                "repeat?": 10000
            }

    """

    def __init__(
        self,
        *,
        setup: str = "",
        query: str,
        repeat: int = 1,
        args: list[Any] | None = None,
    ):
        self.setup = setup
        self.query = query
        self.repeat = repeat
        self.args = args or []

    def iter_args(self) -> Iterator[Sequence[Any]]:
        """
        Parses and iterates through the arguments to be used in executing the query.
        """
        for _ in range(self.repeat):
            if not self.args:
                yield ()
                continue

            for value in self.args:
                yield from self._expand_args(value)

    def _expand_args(self, value: Any) -> Iterator[Sequence[Any]]:
        if isinstance(value, list):
            yield value
        elif isinstance(value, dict):
            repeat = value.get("repeat", 1)
            for _ in range(repeat):
                yield value["row"]
        else:
            yield (value,)


def load_query_spec(file: io.BufferedReader) -> QuerySpec:
    """Loads a query specification from a JSON or TOML file."""
    if file.name.endswith(".toml"):
        import tomllib

        return QuerySpec(**tomllib.load(file))

    import json

    return QuerySpec(**json.load(file))
