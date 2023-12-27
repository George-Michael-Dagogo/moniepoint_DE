from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sqlite3
from main import get_data_from_clickhouse, write_data_to_sqlite
import clickhouse_connect


default_args = {
    'owner': 'Michael',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('taxi_metrics_pipeline', default_args=default_args, schedule='@daily')

fetch_data_task = PythonOperator(
    task_id='fetch_data_from_clickhouse',
    python_callable=get_data_from_clickhouse,
    op_kwargs={
        'host': 'github.demo.trial.altinity.cloud',  
        'user': 'demo',
        'port': '8443', 
        'password': 'demo',
        'query': '''SELECT
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
'''  
    },
    dag=dag
)

write_data_task = PythonOperator(
    task_id='write_data_to_sqlite',
    python_callable=write_data_to_sqlite,
    op_args=['{{ ti.xcom_pull(task_ids="fetch_data_from_clickhouse") }}'],
    provide_context=True,
    dag=dag
)

fetch_data_task >> write_data_task
