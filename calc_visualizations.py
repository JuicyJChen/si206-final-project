import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sqlite3

def main():
    # Database connection (replace 'your_database.db' with your actual database file)
    conn = sqlite3.connect("weather_data_final.db")
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
        precipitation p ON strftime('%Y-%m-%d', t.date) = strftime('%Y-%m-%d', p.date)
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
    # conn.close()
 # Plotting Visualization 1
    fig, ax1 = plt.subplots(figsize=(10, 6))

    ax1.set_xlabel("Month (2023)")
    ax1.set_ylabel("Avg Temperature (°C)", color="tab:blue")
    ax1.plot(
        df_viz1["Month"].values,
        df_viz1["AvgTemperature"].values,
        color="tab:blue",
        label="Avg Temperature",
    )
    ax1.tick_params(axis="y", labelcolor="tab:blue")

    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    ax2.set_ylabel(
        "Avg Stock Price ($)", color="tab:red"
    )  # we already handled the x-label with ax1
    ax2.plot(
        df_viz1["Month"].values,
        df_viz1["AvgStockPrice"].values,
        color="tab:red",
        label="Avg Stock Price",
    )
    ax2.tick_params(axis="y", labelcolor="tab:red")

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.title("Average Temperature in Ann Arbor vs Stock Price (2023)")
    plt.show()

    # Plotting Visualization 2
    fig, ax = plt.subplots(figsize=(12, 6))

    ax.bar(
        df_viz2["Month"], df_viz2["AvgPM10"], color="skyblue", width=0.4, label="Avg PM10"
    )
    ax.bar(
        df_viz2["Month"],
        df_viz2["AvgPM25"],
        color="orange",
        width=0.4,
        label="Avg PM2.5",
        bottom=df_viz2["AvgPM10"],
    )
    ax.set_xlabel("Month (2023)")
    ax.set_ylabel("Average PM10 and PM2.5 (μg/m³)")
    ax.tick_params(axis="y")

    ax2 = ax.twinx()
    ax2.plot(
        df_viz2["Month"].values,
        df_viz2["AvgStockPrice"].values,
        color="green",
        label="Avg Stock Price",
    )
    ax2.set_ylabel("Avg Stock Price ($)")
    ax2.tick_params(axis="y", labelcolor="green")

    fig.tight_layout()
    plt.title("Air Quality (PM10 & PM2.5) in Ann Arbor and Stock Price (2023)")
    plt.legend(loc="upper left")
    plt.show()
 # Visualization 3: Average Rainfall vs. Air Quality Index (PM10 as a proxy)
    query3 = """
    SELECT
        strftime('%Y-%m', p.date) AS Month,
        AVG(p.rain) AS AvgRainfall,
        AVG(aq.pm10) AS AvgPM10
    FROM
        precipitation p
    JOIN
        air_quality aq ON strftime('%Y-%m-%d', p.date) = strftime('%Y-%m-%d', aq.date)
    GROUP BY
        Month
    """
    df_viz3 = pd.read_sql_query(query3, conn)

    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.set_xlabel("Month (2023)")
    ax1.set_ylabel("Avg Rainfall (mm)", color="tab:blue")
    ax1.bar(
        df_viz3["Month"].values,
        df_viz3["AvgRainfall"].values,
        color="tab:blue",
        label="Avg Rainfall",
    )
    ax1.tick_params(axis="y", labelcolor="tab:blue")

    ax2 = ax1.twinx()
    ax2.set_ylabel("Avg PM10 (μg/m³)", color="tab:green")
    ax2.plot(
        df_viz3["Month"].values,
        df_viz3["AvgPM10"].values,
        color="tab:green",
        label="Avg PM10",
    )
    ax2.tick_params(axis="y", labelcolor="tab:green")

    plt.title("Average Rainfall in Ann Arbor vs. Air Quality Index (2023)")
    plt.show()

    # Visualization 4: Temperature Variance vs. Stock Market Volatility
    query4 = """
    SELECT
        strftime('%Y-%m', t.date) AS Month,
        AVG((t.temperature - (SELECT AVG(temperature) FROM temperature)) * (t.temperature - (SELECT AVG(temperature) FROM temperature))) AS TempVariance,
        AVG((s.high_price - s.low_price) / s.open_price) AS StockVolatility
    FROM
        temperature t
    JOIN
        stock_data s ON strftime('%Y-%m-%d', t.date) = strftime('%Y-%m-%d', s.date)
    GROUP BY
        Month
    """
    df_viz4 = pd.read_sql_query(query4, conn)

    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.set_xlabel("Month (2023)")
    ax1.set_ylabel("Temperature Variance (°C²)", color="tab:red")
    ax1.plot(
        df_viz4["Month"].values,
        df_viz4["TempVariance"].values,
        color="tab:red",
        label="Temperature Variance",
    )
    ax1.tick_params(axis="y", labelcolor="tab:red")

    ax2 = ax1.twinx()
    ax2.set_ylabel("Stock Market Volatility", color="tab:purple")
    ax2.plot(
        df_viz4["Month"].values,
        df_viz4["StockVolatility"].values,
        color="tab:purple",
        label="Stock Market Volatility",
    )
    ax2.tick_params(axis="y", labelcolor="tab:purple")

    plt.title("Temperature Variance in Ann Arbor vs. Stock Market Volatility (2023)")
    plt.show()

    # Write query results to a text file
    with open('calculated_data.txt', 'w') as file:
        file.write("Average Temperature in Ann Arbor vs Stock Price (2023)\n")
        df_viz1.to_string(file)
        file.write("\n\nAir Quality (PM10 & PM2.5) in Ann Arbor and Stock Price (2023)\n")
        df_viz2.to_string(file)
        file.write("\n\nAverage Rainfall in Ann Arbor vs. Air Quality Index (2023)\n")
        df_viz3.to_string(file)
        file.write("\n\nTemperature Variance in Ann Arbor vs. Stock Market Volatility (2023)\n")
        df_viz4.to_string(file)
        
if __name__ == "__main__":
    main()
