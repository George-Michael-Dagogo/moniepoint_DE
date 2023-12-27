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
        conn = sqlite3.connect('/workspace/moniepoint_DE/taxi.db')

        cursor = conn.cursor()

        cursor.execute('''CREATE TABLE IF NOT EXISTS taxi_data (
            month TEXT,
            avg_trips_saturday FLOAT,
            avg_fare_saturday FLOAT,
            avg_duration_saturday_mins FLOAT,
            avg_trips_sunday FLOAT,
            avg_fare_sunday FLOAT,
            avg_duration_sunday_mins FLOAT

        )''')

        data_tuples = list(data.itertuples(index=False, name=None))

        cursor.executemany("INSERT INTO taxi_data VALUES (?, ?, ?, ?, ?, ?, ?)", data_tuples)


    except Exception as e:
        print("Error writing data to SQLite:", e)

    finally:
        conn.commit()
        conn.close()




