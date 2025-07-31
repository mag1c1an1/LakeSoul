from lakesoul_dataset._core import hello_from_bin
from lakesoul_dataset._core import add
from lakesoul_dataset._core import arrow_main


def main() -> None:
    print(hello_from_bin())
    print(add(1, 2))
    arrow_main()
