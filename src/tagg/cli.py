"""CLI frontend to aggregation functions."""
from pathlib import Path

import click
import dask.dataframe
from dask.dataframe import DataFrame

from tagg.aggregate import by_week_and_token


def write_transfers(data_frame: DataFrame):
    """Summarize trasnfer counts and sums to csv files."""
    click.echo("Summarizing the sums")
    data_frame.to_csv("sum.csv", columns=["sum"])
    click.echo("Summarizing the counts")
    data_frame.to_csv("count.csv", columns=["count"])


@click.command()
@click.argument("path", type=click.Path(exists=True))
def main(path: str):
    """A tool to summarize Etherium token transfer logs."""
    path_obj = Path(path)
    columns = ["BLOCK_DATE", "ADDRESS", "TOPIC0", "PARAMS"]
    if path_obj.is_dir():
        path_obj = path_obj.joinpath("*.parquet")
    data = dask.dataframe.read_parquet(path_obj, columns=columns)
    click.echo("Completed reading files")
    summary = by_week_and_token(data)
    click.echo("Writting summary")
    write_transfers(summary)


if __name__ == "__main__":
    main()
