# tagg
Etherium token summarization and aggregation tool. This code is an exploration of
decoded Etherium transaction logs and is not indented for use elsewhere.


# Installing
Install via pip directly from git

```bash
pip install git+https://github.com/ww-daniel-mora/rwa@main
```

# Running
This code can be used as a library or CLI. To use the code as a CLI pass either a path
to an individual parquet file or a path to a directory containing the parquet files to
be processed, e.g

```bash
tagg myfile.parquet
```

```bash
tagg myparquetfiles/
```

When run, two directories will be created in the current directory `sum.csv` and
`count.csv`. Each directory will contains a csv file containing the summary transaction
data for transfers aggregated by week number and token address number.

The files are assumed to be in a consistent format and contain the following columns:
`BLOCK_DATE`, `ADDRESS`, `TOPIC0`, `PARAMS`, where `PARAMS` contains the decoded
transaction parameters in JSON string format.


# Supporting Multiple Time Frames
To build a system that supported summarization across multiple time frames, 1D, 1W, 1M
etc, I would pre-compute the day, week, and month number of each transaction. This would
enable enable per-day level selection of an aggregation window and then enable trival
aggregation by using a groupby on the appropriate, pre-computed, column. This would
consume some additional disk space, but it would make groupby operations fast and flexible for
each aggregation level.

# Further Improvements
* Switch from `dask` to `spark` for processing large datasets more quickly
* Add type information and validation to the dataframes
* Better handling of data validation errors - right now the code will substitute a 0
  when the parameter data does not conform to the expectations
* Incorporate into a data pipeline with separate clean, process, write stages
* Enable processing files in a remote S3 bucket
* Parameterize the output destination
* Expand testing to handle invalid data formats


# Unknown Issues
* There are two different transfer types Transfer and BulkTransfer. This solution only
  handles the first type but the second type may also be required for accurate reporting
* There were some inconsistencies in the data and some transfers did not have an
  associated value right now these are handled as value 0 so thew will show up in the
  count summaries but not in the sum summary
* There was an inconsistency in the instructions where the `Problem Statement` requested
  the most transferred token per week but the result datasets specified to use the month
  as a column. This solution uses weeks (specifically week number) rather than month.
