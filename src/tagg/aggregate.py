"""Transaction aggregation functions."""
import json

import dask.dataframe
import pandas
from dask.dataframe import DataFrame

TRANSFER_VALUE_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)


def _transfer_value(data: str) -> int:
    if data:
        obj = json.loads(data)
        if "value" in obj:
            return int(obj["value"])
        return int(obj["_value"])
    return 0


def by_week_and_token(data_frame: DataFrame) -> DataFrame:
    """Compute the sum and count of transfers aggregated by week and token address."""
    transfers = data_frame.query(f'TOPIC0 == "{TRANSFER_VALUE_TOPIC}"').query(
        f'PARAMS.str.contains("value")'
    )
    transfers["YEAR_WEEK"] = dask.dataframe.to_datetime(
        transfers.BLOCK_DATE
    ).dt.strftime("%Y-%U")
    transfers["TRANSFER_VALUE"] = transfers.PARAMS.apply(
        _transfer_value, meta=pandas.Series(dtype="UInt64")
    )
    groups = transfers.groupby(["YEAR_WEEK", "ADDRESS"])
    aggregation = groups["TRANSFER_VALUE"].agg(["sum", "count"])
    return aggregation
