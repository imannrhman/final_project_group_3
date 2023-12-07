INSERT INTO dimensional.dim_date (date_key, date, day_of_week, week, month, quarter, year)
SELECT TO_CHAR(order_date, 'YYYYMMDD')::integer AS date_key,
       order_date                               AS date,
       EXTRACT(DOW FROM order_date)             AS day_of_week,
       EXTRACT(WEEK FROM order_date)            AS week,
       EXTRACT(MONTH FROM order_date)           AS month,
       EXTRACT(QUARTER FROM order_date)         AS quarter,
       EXTRACT(YEAR FROM order_date)            AS year
FROM (SELECT DISTINCT created_at as order_date
      FROM orders) AS subquery
ON CONFLICT (date_key)
    DO UPDATE SET
                  date = excluded.date,
                  day_of_week = excluded.day_of_week,
                  week = excluded.week,
                  month = excluded.month,
                  quarter = excluded.quarter,
                  year = excluded.year;