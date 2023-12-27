import clickhouse_connect
import sqlite3



def get_data_from_clickhouse(host, user, password, query,port):
    

    try:
        client = clickhouse_connect.get_client(host=host, secure=True, port=port, user=user, password=password)
        results = client.query_df(query)

        return results
    except Exception as e:
        print(f"An error occurred: {e}")
        return None




def write_data_to_sqlite(data):
    try:
        data.to_sql('my_table', '/workspace/moniepoint_DE/taxi.db', driver='sqlite3')

    except Exception as e:
        print("Error writing data to SQLite:", e)






