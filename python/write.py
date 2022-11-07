"""
Clickhouse client sample
"""

import clickhouse_connect
import pandas as pd


def select():
    """select fn"""
    print("--------------------------------------------------------------------------------")
    print("INSERT")
    print("--------------------------------------------------------------------------------")

    client = clickhouse_connect.get_client(
        host='localhost', port="9200", username='default', connect_timeout=300)
    result = client.query('SELECT COUNT(*) FROM test.numbers')
    print("ROW COUNT BEFORE INSERT: ", result.result_set[0][0])

    print("INSERTION IN PROGRESS...")
    df = pd.DataFrame({'id': [1, 2],
                      'name': ["vaisakh", "praseed"]})
    client.insert_df('test.numbers', df)

    result = client.query('SELECT COUNT(*) FROM test.numbers')
    print("ROW COUNT AFTER INSERT: ", result.result_set[0][0])
    print("--------------------------------------------------------------------------------")


if __name__ == "__main__":
    select()
