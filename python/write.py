"""
Clickhouse client sample
"""

import clickhouse_connect
import pandas as pd


def insert():
    """select fn"""
    print("--------------------------------------------------------------------------------")
    print("INSERT")
    print("--------------------------------------------------------------------------------")

    client = clickhouse_connect.get_client(
        host='localhost', port="9200", username='default', connect_timeout=300)
    result = client.query('SELECT COUNT(*) FROM demo.contacts')
    print("ROW COUNT BEFORE INSERT: ", result.result_set[0][0])

    print("INSERTION IN PROGRESS...")
    df = pd.DataFrame({'id': [5],
                      'name': ["Suman Iyer"],
                      'mail': ["suman.iyer@chistadata.com"]})
    client.insert_df('demo.contacts', df)

    result = client.query('SELECT COUNT(*) FROM demo.contacts')
    print("ROW COUNT AFTER INSERT: ", result.result_set[0][0])
    print("--------------------------------------------------------------------------------\n")


if __name__ == "__main__":
    insert()
