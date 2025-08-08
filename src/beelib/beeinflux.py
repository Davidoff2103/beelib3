import time
from datetime import datetime, timezone
import influxdb_client
import isodate
import pandas as pd

def connect_influx(influx_connection):
    """
    Establish a connection to InfluxDB.

    Parameters:
    - influx_connection (dict): Configuration dictionary containing 'url', 'org', and 'token'.

    Returns:
    - influxdb_client.InfluxDBClient: An InfluxDB client instance.
    """
    client = influxdb_client.InfluxDBClient(
        url=influx_connection['connection']['url'],
        org=influx_connection['connection']['org'],
        token=influx_connection['connection']['token'],
        timeout=60000
    )
    return client

def run_query(influx_connection, query):
    """
    Execute an InfluxDB query.

    Parameters:
    - influx_connection (dict): InfluxDB connection configuration.
    - query (str): The InfluxDB Flux query to execute.

    Returns:
    - pandas.DataFrame: Results of the query as a DataFrame.
    """
    client = connect_influx(influx_connection)
    query_api = client.query_api()
    return query_api.query_data_frame(query)

def get_timeseries_by_hash(d_hash, freq, influx_connection, ts_ini, ts_end):
    """
    Retrieve a time series from InfluxDB filtered by hash and time range.

    Parameters:
    - d_hash (str): Hash identifier for the time series.
    - freq (str): ISO 8601 duration for the time interval.
    - influx_connection (dict): InfluxDB connection configuration.
    - ts_ini (datetime): Start timestamp for the query.
    - ts_end (datetime): End timestamp for the query.

    Returns:
    - pandas.DataFrame: A DataFrame with time-indexed data.
    """
    aggregation_window = int(isodate.parse_duration(freq).total_seconds() * 10**9)
    start = int(ts_ini.timestamp()) * 10**9
    end = int(ts_end.timestamp()) * 10**9
    query = f"""
        from(bucket: "{influx_connection['bucket']}")
        |> range(start: time(v:{start}), stop: time(v:{int(end)}))
        |> filter(fn: (r) => r["_measurement"] == "{influx_connection['measurement']}")
        |> filter(fn: (r) => r["hash"] == "{d_hash}")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> filter(fn: (r) => r["is_null"]==0.0)
        |> keep(columns: ["_time", "value", "end", "isReal"])
    """
    client = connect_influx(influx_connection)
    query_api = client.query_api()
    df = query_api.query_data_frame(query)
    if df.empty:
        return pd.DataFrame()
    df['end'] = pd.to_datetime(df['end'], unit="s").dt.tz_localize("UTC")
    df.rename(columns={"_time": "start"}, inplace=True)
    df = df[["start", "end", "isReal", "value"]]
    df.set_index("start", inplace=True)
    return df