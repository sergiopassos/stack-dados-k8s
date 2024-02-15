"""
CLI that uses Typer to build the command line interface.

python cli.py --help

python cli.py all
python cli.py mssql
python cli.py postgres
python cli.py mongodb
python cli.py redis
"""

import typer

from rich import print
from main import MinioStorage
from dotenv import load_dotenv

load_dotenv()


def main(dstype: str):
    """
    Perform actions based on the specified data source type.

    Allowed types are: mssql, postgres, mongodb, redis, all

    Args:
        dstype: The type of the data source.
    """

    if dstype == "mssql":
        print(MinioStorage().write_file(ds_type="mssql"))
    elif dstype == "postgres":
        print(MinioStorage().write_file(ds_type="postgres"))
    elif dstype == "mongodb":
        print(MinioStorage().write_file(ds_type="mongodb"))
    elif dstype == "redis":
        print(MinioStorage().write_file(ds_type="redis"))
    elif dstype == "all":
        print(MinioStorage().write_file(ds_type="mssql"))
        print(MinioStorage().write_file(ds_type="postgres"))
        print(MinioStorage().write_file(ds_type="mongodb"))
        print(MinioStorage().write_file(ds_type="redis"))


if __name__ == "__main__":
    typer.run(main)
