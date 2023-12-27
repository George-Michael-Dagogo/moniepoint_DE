SELECT
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
ORDER BY month;
