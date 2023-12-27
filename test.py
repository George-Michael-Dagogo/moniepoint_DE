import clickhouse_connect
import pandas as pd


import sqlite3 
sqlite_file =  '/workspace/moniepoint_DE/taxi.db'

client = clickhouse_connect.get_client(host='github.demo.trial.altinity.cloud', secure=True, port=8443, user='demo', password='demo')


result = client.query_df('''SELECT
    DATE_FORMAT(pickup_date, '%Y-%m') AS month,
    ROUND(AVG(CASE WHEN DAYOFWEEK(pickup_date) = 7 THEN 1 ELSE 0 END),2) AS avg_trips_saturday,
    ROUND(AVG(CASE WHEN DAYOFWEEK(pickup_date) = 7 THEN fare_amount ELSE NULL END), 2) AS avg_fare_saturday,
    ROUND(AVG(CASE WHEN DAYOFWEEK(pickup_date) = 7 THEN TIMESTAMPDIFF(SECOND, pickup_datetime, dropoff_datetime) ELSE NULL END) / 60, 2) AS avg_duration_saturday_mins,  -- Converted to minutes
    ROUND(AVG(CASE WHEN DAYOFWEEK(pickup_date) = 1 THEN 1 ELSE 0 END),2) AS avg_trips_sunday,
    ROUND(AVG(CASE WHEN DAYOFWEEK(pickup_date) = 1 THEN fare_amount ELSE NULL END), 2) AS avg_fare_sunday,
    ROUND(AVG(CASE WHEN DAYOFWEEK(pickup_date) = 1 THEN TIMESTAMPDIFF(SECOND, pickup_datetime, dropoff_datetime) ELSE NULL END) / 60, 2) AS avg_duration_sunday_mins  -- Converted to minutes
FROM tripdata
WHERE pickup_date BETWEEN '2014-01-01' AND '2016-12-31'
GROUP BY DATE_FORMAT(pickup_date, '%Y-%m')
ORDER BY month
''')
print(result)
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

data_tuples = list(result.itertuples(index=False, name=None))

cursor.executemany("INSERT INTO taxi_data VALUES (?, ?, ?, ?, ?, ?, ?)", data_tuples)

conn.commit()
conn.close()

