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


def fetch_stock_data_for_date(date, symbol='DTE'):
    api_key = '21030c7bd2b86ce748be56519bc6744d'  # Your MarketStack API key
    endpoint = f"http://api.marketstack.com/v1/eod/{date.strftime('%Y-%m-%d')}"

    params = {
        'access_key': api_key,
        'symbols': symbol
       # 'date-from': date.strftime('%Y-%m-%d'),
        #'date-to': date.strftime('%Y-%m-%d')# Format the date as required by the API
        
    }

    response = requests.get(endpoint, params=params)
    if response.status_code == 200:
        data = response.json().get('data', [])
        if data:
            # Extract the first (and presumably only) entry
            stock_data = data[0]
            return {
                'open': stock_data['open'],
                'close': stock_data['close'],
                'high': stock_data['high'],
                'low': stock_data['low'],
                'volume': stock_data['volume']
            }
    return None

def fetch_air_quality_data_for_date(date):
    # Your API request for air quality data
    params = {
        "latitude": 42.2776,
        "longitude": -83.7409,
        "hourly": ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone"],
        "start_date": date.strftime('%Y-%m-%d'),
        "end_date": date.strftime('%Y-%m-%d')
    }
    response = requests.get("https://air-quality-api.open-meteo.com/v1/air-quality", params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return None
    
# Function to get the dates that already have data
def get_existing_dates(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SELECT date FROM temperature")
    return {datetime.strptime(row[0].split()[0], "%Y-%m-%d").date() for row in cursor.fetchall()}

existing_dates = get_existing_dates(conn)


# Function to calculate sample dates throughout the year, excluding existing dates
def calculate_sample_dates(start_date, end_date, total_samples, existing_dates):
    # Ensure all_dates is a set of datetime.date objects from start_date to end_date
    all_dates = {start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)}

    # Subtract existing_dates from all_dates to get available_dates
    available_dates = list(all_dates - existing_dates)
    available_dates.sort()
    
    # Determine the number of dates to sample
    remaining_dates_to_sample = total_samples - len(existing_dates)
    dates_to_sample = min(remaining_dates_to_sample, 25)
    
    # Convert datetime.datetime objects to datetime.date for consistency
    next_batch = [date for date in available_dates[:dates_to_sample]]
    return next_batch

# Calculate 100 dates throughout the year 2022, excluding dates that already have data
total_required_dates = 100
sample_dates = calculate_sample_dates(datetime(2023, 1, 1).date(), datetime(2023, 12, 1).date(), 200, existing_dates)

# Fetch data for each sample date and insert into the database
for sample_date in sample_dates:
    # Format the date for the API request
    formatted_date = sample_date.strftime('%Y-%m-%d')

    # API parameters for a single day
    params = {
        "latitude": 42.2776,
        "longitude": -83.7409,
        "start_date": formatted_date,
        "end_date": formatted_date,
        "hourly": ["temperature_2m", "rain", "snowfall"]
    }

    # Make the API request
    response = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params)
    data = response.json()

    if 'hourly' in data:
        hourly_data = data['hourly']
        times = hourly_data['time']
        temperatures = hourly_data['temperature_2m']
        rain_values = hourly_data.get('rain', [0] * len(times))
        snowfall_values = hourly_data.get('snowfall', [0] * len(times))

        # Just take the first entry of the day
        date_str = times[0]
        date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M")
        date = date_obj.strftime("%Y-%m-%d %H:%M:%S")

        temperature = temperatures[0]
        rain = rain_values[0]
        snowfall = snowfall_values[0]

        try:
            # Insert temperature data
            cursor.execute("INSERT INTO temperature (date, temperature) VALUES (?, ?)", (date, temperature))
            temp_id = cursor.lastrowid

            # Insert precipitation data
            cursor.execute("INSERT INTO precipitation (date, rain, snowfall, temp_id) VALUES (?, ?, ?, ?)",
                           (date, rain, snowfall, temp_id))
        except sqlite3.IntegrityError:
            # Skip if this date already exists
            continue
        
        
        
    # Fetch stock data
    stock_data = fetch_stock_data_for_date(sample_date)
    if stock_data:
        # Extract stock data values
        open_price = stock_data['open']
        close_price = stock_data['close']
        high_price = stock_data['high']
        low_price = stock_data['low']
        volume = stock_data['volume']

        # Insert stock data
        try:
            cursor.execute("INSERT INTO stock_data (date, open_price, close_price, high_price, low_price, volume) VALUES (?, ?, ?, ?, ?, ?)",
                        (sample_date.strftime("%Y-%m-%d"), open_price, close_price, high_price, low_price, volume))
        except sqlite3.IntegrityError:
            # Skip if this date already exists in stock_data table
            continue
        
    # Fetch air quality data
    air_quality_data = fetch_air_quality_data_for_date(sample_date)
    if air_quality_data and 'hourly' in air_quality_data:
        # Assuming you take the first entry of the day for simplicity
        hourly_data = air_quality_data['hourly']
        pm10 = hourly_data['pm10'][0]
        pm2_5 = hourly_data['pm2_5'][0]
        carbon_monoxide = hourly_data['carbon_monoxide'][0]
        nitrogen_dioxide = hourly_data['nitrogen_dioxide'][0]
        sulphur_dioxide = hourly_data['sulphur_dioxide'][0]
        ozone = hourly_data['ozone'][0]

        # Insert air quality data
        try:
            cursor.execute("INSERT INTO air_quality (temp_id, date, pm10, pm2_5, carbon_monoxide, nitrogen_dioxide, sulphur_dioxide, ozone) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                           (temp_id, sample_date.strftime("%Y-%m-%d"), pm10, pm2_5, carbon_monoxide, nitrogen_dioxide, sulphur_dioxide, ozone))
        except sqlite3.IntegrityError:
            # Skip if this date already exists in air_quality table
            continue

# Commit the changes and close the database connection
conn.commit()
conn.close()
