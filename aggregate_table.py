import sqlite3

def create_and_combine_tables():
    # Connect to the SQLite Database
    conn = sqlite3.connect("weather_data_final.db")
    cursor = conn.cursor()

    # Create Tables
    cursor.execute("""CREATE TABLE IF NOT EXISTS temperature (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT UNIQUE,
                        temperature REAL,
                        location_id INTEGER,
                        latitude REAL,
                        longitude REAL,
                        FOREIGN KEY (location_id) REFERENCES locations(location_id)
                    )""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS precipitation (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT,
                        rain REAL,
                        snowfall REAL,
                        temp_id INTEGER,
                        location_id INTEGER,
                        latitude REAL,
                        longitude REAL,
                        FOREIGN KEY (location_id) REFERENCES locations(location_id),
                        FOREIGN KEY(temp_id) REFERENCES temperature(id)
                    )""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS air_quality (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        temp_id INTEGER,
                        date TEXT UNIQUE,
                        pm10 REAL,
                        pm2_5 REAL,
                        carbon_monoxide REAL,
                        nitrogen_dioxide REAL,
                        sulphur_dioxide REAL,
                        ozone REAL,
                        location_id INTEGER,
                        latitude REAL,
                        longitude REAL,
                        FOREIGN KEY (location_id) REFERENCES locations(location_id),
                        FOREIGN KEY(temp_id) REFERENCES temperature(id)
                    )""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS stock_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT UNIQUE,
                        open_price REAL,
                        close_price REAL,
                        high_price REAL,
                        low_price REAL,
                        volume INTEGER
                    )""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS locations (
                        location_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        city_name TEXT UNIQUE,
                        latitude REAL,
                        longitude REAL
                    )""")

    # Create an Aggregated Table
    cursor.execute("""CREATE TABLE IF NOT EXISTS aggregated_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT,
                        city_name TEXT,
                        temperature REAL,
                        rain REAL,
                        snowfall REAL,
                        pm10 REAL,
                        pm2_5 REAL,
                        carbon_monoxide REAL,
                        nitrogen_dioxide REAL,
                        sulphur_dioxide REAL,
                        ozone REAL,
                        open_price REAL,
                        close_price REAL,
                        high_price REAL,
                        low_price REAL,
                        volume INTEGER
                    )""")

    # Insert Data into Aggregated Table
    cursor.execute("""
        INSERT INTO aggregated_data (date, city_name, temperature, rain, snowfall, pm10, pm2_5, carbon_monoxide, nitrogen_dioxide, sulphur_dioxide, ozone, open_price, close_price, high_price, low_price, volume)
        SELECT t.date, l.city_name, t.temperature, p.rain, p.snowfall, aq.pm10, aq.pm2_5, aq.carbon_monoxide, aq.nitrogen_dioxide, aq.sulphur_dioxide, aq.ozone, sd.open_price, sd.close_price, sd.high_price, sd.low_price, sd.volume
        FROM temperature t
        LEFT JOIN precipitation p ON t.id = p.temp_id
        LEFT JOIN air_quality aq ON t.id = aq.temp_id
        LEFT JOIN stock_data sd ON t.date = sd.date
        LEFT JOIN locations l ON t.location_id = l.location_id
    """)

    # Commit changes and close connection
    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_and_combine_tables()
