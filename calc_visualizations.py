import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sqlite3

# Database connection (replace 'your_database.db' with your actual database file)
conn = sqlite3.connect("weather_data.db")
cursor = conn.cursor()

# Execute SQL queries and fetch data into Pandas DataFrames
query1 = """
SELECT
    strftime('%Y-%m', t.date) AS Month,
    AVG(t.temperature) AS AvgTemperature,
    AVG(p.rain) AS AvgRainfall,
    AVG(s.open_price + s.close_price + s.high_price + s.low_price) / 4 AS AvgStockPrice

FROM
    temperature t
JOIN
    precipitation p ON strftime('%Y-%m-%d', tdate) = strftime('%Y-%m-%d', p.date)
JOIN
    stock_data s ON strftime('%Y-%m-%d', t.date) = strftime('%Y-%m-%d', s.date)
WHERE
    t.date BETWEEN '2023-01-01' AND '2023-12-01'
GROUP BY
    Month
"""
df_viz1 = pd.read_sql_query(query1, conn)

query2 = """
SELECT 
    strftime('%Y-%m', aq.date) AS Month,
    AVG(aq.pm10) AS AvgPM10,
    AVG(aq.pm2_5) AS AvgPM25,
    AVG(t.temperature) AS AvgTemperature,
    AVG(s.open_price + s.close_price + s.high_price + s.low_price) / 4 AS AvgStockPrice
FROM 
    air_quality aq
JOIN 
    temperature t ON aq.temp_id = t.id
JOIN 
    stock_data s ON aq.date = s.date
WHERE 
    aq.date BETWEEN '2023-01-01' AND '2023-12-01'
GROUP BY 
    Month
"""
df_viz2 = pd.read_sql_query(query2, conn)

# Close the database connection
conn.close()