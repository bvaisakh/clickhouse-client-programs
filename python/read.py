"""
Clickhouse client sample
"""

import clickhouse_connect
import pandas as pd


def select():
    """select fn"""
    print("--------------------------------------------------------------------------------")
    print("SELECT: SELECT id, name, mail FROM demo.contacts LIMIT 10")
    print("--------------------------------------------------------------------------------")
    client = clickhouse_connect.get_client(
        host='localhost', port="9200", username='default', connect_timeout=300)
    result_df = client.query_df('SELECT id, name, mail FROM demo.contacts LIMIT 10')
    print(result_df)
    print("--------------------------------------------------------------------------------")


if __name__ == "__main__":
    select()
