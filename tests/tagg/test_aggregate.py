"""Test aggregation function."""
import datetime
import json

import pandas
from dask.dataframe import from_pandas

from tagg.aggregate import TRANSFER_VALUE_TOPIC, by_week_and_token


def test_aggregate_one_address():
    param1 = {"value": "1"}
    param2 = {"value": 2}
    address = "a contract address"
    data = {
        "BLOCK_DATE": ["2023-01-01", "2023-01-01"],
        "PARAMS": [json.dumps(param1), json.dumps(param2)],
        "ADDRESS": [address, address],
        "TOPIC0": [TRANSFER_VALUE_TOPIC, TRANSFER_VALUE_TOPIC],
    }
    df = from_pandas(pandas.DataFrame(data), npartitions=1)
    result = by_week_and_token(df).compute()
    assert len(result) == 1
    row = result.iloc[0]
    assert row.name[0] == "2023-01"
    assert row.name[1] == address
    assert row["sum"] == 3
    assert row["count"] == 2


def test_aggregate_two_addresses():
    param1 = {"value": "1"}
    param2 = {"value": 2}
    address1 = "address1"
    address2 = "address2"
    data = {
        "BLOCK_DATE": [
            "2023-01-01",
            "2023-01-01",
            "2023-01-01",
            "2023-01-01",
        ],
        "PARAMS": [
            json.dumps(param1),
            json.dumps(param2),
            json.dumps(param1),
            json.dumps(param2),
        ],
        "ADDRESS": [address1, address1, address2, address2],
        "TOPIC0": [
            TRANSFER_VALUE_TOPIC,
            TRANSFER_VALUE_TOPIC,
            TRANSFER_VALUE_TOPIC,
            TRANSFER_VALUE_TOPIC,
        ],
    }
    df = from_pandas(pandas.DataFrame(data), npartitions=1)
    result = by_week_and_token(df).compute()
    assert len(result) == 2
    assert result.iloc[0]["sum"] == 3
    assert result.iloc[0]["count"] == 2
    assert result.iloc[1]["sum"] == 3
    assert result.iloc[1]["count"] == 2


def test_aggregate_two_dates():
    param1 = {"value": "1"}
    param2 = {"value": 2}
    address1 = "address1"
    address2 = "address2"
    date1 = datetime.datetime(year=2023, month=1, day=1).strftime("%Y-%U")
    date2 = datetime.datetime(year=2023, month=2, day=1).strftime("%Y-%U")
    data = {
        "BLOCK_DATE": [
            "2023-01-01",
            "2023-02-01",
            "2023-01-01",
            "2023-02-01",
        ],
        "PARAMS": [
            json.dumps(param1),
            json.dumps(param2),
            json.dumps(param1),
            json.dumps(param2),
        ],
        "ADDRESS": [address1, address1, address2, address2],
        "TOPIC0": [
            TRANSFER_VALUE_TOPIC,
            TRANSFER_VALUE_TOPIC,
            TRANSFER_VALUE_TOPIC,
            TRANSFER_VALUE_TOPIC,
        ],
    }
    df = from_pandas(pandas.DataFrame(data), npartitions=1)
    result = by_week_and_token(df).compute()
    assert len(result) == 4
    assert result.loc[(date1, address1)]["count"] == 1
    assert result.loc[(date2, address1)]["count"] == 1
    assert result.loc[(date1, address2)]["count"] == 1
    assert result.loc[(date2, address2)]["count"] == 1

    assert result.loc[(date1, address1)]["sum"] == 1
    assert result.loc[(date2, address2)]["sum"] == 2
    assert result.loc[(date2, address1)]["sum"] == 2
    assert result.loc[(date1, address2)]["sum"] == 1
