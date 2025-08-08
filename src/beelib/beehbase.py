import uuid
import happybase
import re

def get_tables(str_filter, hbase_conf):
    """
    List HBase tables matching a filter.

    Parameters:
    - str_filter (str): Regex pattern to filter table names.
    - hbase_conf (dict): HBase connection configuration.

    Returns:
    - list: List of matching table names as strings.
    """
    hbase = happybase.Connection(**hbase_conf)
    return [x.decode() for x in hbase.tables() if re.match(str_filter, x.decode())]

def __get_h_table__(hbase, table_name, cf=None):
    """
    Ensure an HBase table exists and return its handle.

    Parameters:
    - hbase (happybase.Connection): HBase connection.
    - table_name (str): Name of the table.
    - cf (dict, optional): Column family mapping.

    Returns:
    - happybase.Table: The HBase table object.
    """
    try:
        if not cf:
            cf = {"cf": {}}
        hbase.create_table(table_name, cf)
    except Exception as e:
        if str(e.__class__) == "<class 'Hbase_thrift.AlreadyExists'>":
            pass
        else:
            print(e)
    return hbase.table(table_name)

def save_to_hbase(documents, h_table_name, hbase_conf, cf_mapping, row_fields=None, batch_size=1000):
    """
    Save a list of documents to HBase.

    Parameters:
    - documents (list): List of dictionaries to save.
    - h_table_name (str): Name of the HBase table.
    - hbase_conf (dict): HBase connection configuration.
    - cf_mapping (list): List of tuples specifying column family and fields.
    - row_fields (list, optional): Fields to use for generating row keys.
    - batch_size (int, optional): Number of rows to batch per write.

    Raises:
    - Exception: If column mapping is invalid.
    """
    hbase = happybase.Connection(**hbase_conf)
    table = __get_h_table__(hbase, h_table_name, {cf: {} for cf, _ in cf_mapping})
    h_batch = table.batch(batch_size=batch_size)
    row_auto = 0
    uid = uuid.uuid4()
    for d in documents:
        d_ = d.copy()
        if not row_fields:
            row = f"{uid}~{row_auto}"
            row_auto += 1
        else:
            row = "~".join([str(d_.pop(f)) if f in d_ else "" for f in row_fields])
        values = {}
        for cf, fields in cf_mapping:
            if fields == "all":
                for c, v in d_.items():
                    values[f"{cf}:{c}"] = str(v)
            elif isinstance(fields, list):
                for c in fields:
                    if c in d_:
                        values[f"{cf}:{c}"] = str(d_[c])
            else:
                raise Exception("Column mapping must be a list of fields or 'all'")
        h_batch.put(str(row), values)
    h_batch.send()

def get_hbase_data_batch(hbase_conf, hbase_table, row_start=None, row_stop=None, row_prefix=None, columns=None,
                         _filter=None, timestamp=None, include_timestamp=False, batch_size=100000,
                         scan_batching=None, limit=None, sorted_columns=False, reverse=False):
    """
    Retrieve data from HBase in batches.

    Parameters:
    - hbase_conf (dict): HBase connection configuration.
    - hbase_table (str): Name of the HBase table.
    - row_start (str, optional): Start row key for the scan.
    - row_stop (str, optional): Stop row key for the scan.
    - row_prefix (str, optional): Row key prefix for filtering.
    - columns (list, optional): Columns to retrieve.
    - _filter (str, optional): HBase filter string.
    - timestamp (int, optional): Timestamp for versioning.
    - include_timestamp (bool, optional): Whether to include timestamps in results.
    - batch_size (int, optional): Number of rows per batch.
    - scan_batching (int, optional): Maximum number of rows per RPC.
    - limit (int, optional): Maximum number of rows to return.
    - sorted_columns (bool, optional): Whether columns are sorted.
    - reverse (bool, optional): Whether to scan in reverse.

    Yields:
    - list: Batches of rows as (row_key, columns) tuples.
    """
    if row_prefix:
        row_start = row_prefix
        row_stop = row_prefix[:-1] + chr(ord(row_prefix[-1]) + 1)

    if limit:
        if limit > batch_size:
            current_limit = batch_size
        else:
            current_limit = limit
    else:
        current_limit = batch_size
    current_register = 0
    while True:
        hbase = happybase.Connection(**hbase_conf)
        table = hbase.table(hbase_table)
        data = list(table.scan(row_start=row_start, row_stop=row_stop, columns=columns, filter=_filter,
                               timestamp=timestamp, include_timestamp=include_timestamp, batch_size=batch_size,
                               scan_batching=scan_batching, limit=current_limit, sorted_columns=sorted_columns,
                               reverse=reverse))
        if not data:
            break
        yield data
        if len(data) <= 1:
            break

        last_record = data[-1][0].decode()
        current_register += len(data)

        if limit:
            if current_register >= limit:
                break
            else:
                current_limit = min(batch_size, limit - current_register)
        row_start = last_record[:-1] + chr(ord(last_record[-1]) + 1)