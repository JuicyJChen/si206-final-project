import requests
import sqlite3
import time
from datetime import datetime, timedelta

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('weather_data.db')
cursor = conn.cursor()

# Create tables if they don't exist
cursor.execute('''CREATE TABLE IF NOT EXISTS temperature (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT UNIQUE,
                    temperature REAL
                )''')
cursor.execute('''CREATE TABLE IF NOT EXISTS precipitation (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT,
                    rain REAL,
                    snowfall REAL,
                    temp_id INTEGER,
                    FOREIGN KEY(temp_id) REFERENCES temperature(id)
                )''')

# Create a table for air quality data if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS air_quality (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        temp_id INTEGER,
        date TEXT UNIQUE,
        pm10 REAL,
        pm2_5 REAL,
        carbon_monoxide REAL,
        nitrogen_dioxide REAL,
        sulphur_dioxide REAL,
        ozone REAL,
        FOREIGN KEY(temp_id) REFERENCES temperature(id)
    )
''')

# Create a table for stock data if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT UNIQUE,
        open_price REAL,
        close_price REAL,
        high_price REAL,
        low_price REAL,
        volume INTEGER
    )
''')
