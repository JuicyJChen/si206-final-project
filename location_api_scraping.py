import requests
import sqlite3


# Function to get latitude and longitude for a city name using the geocoding API
def get_lat_lon(city_name):
    response = requests.get(
        f"https://geocoding-api.open-meteo.com/v1/search?name={city_name}&count=1&format=json"
    )
    if response.status_code == 200:
        data = response.json()
        results = data.get("results")
        if results:
            return results[0]["latitude"], results[0]["longitude"]
    return None, None


# Function to insert city data into the locations table
def insert_city_data(city_name, cursor):
    latitude, longitude = get_lat_lon(city_name)
    if latitude is not None and longitude is not None:
        cursor.execute(
            "INSERT OR IGNORE INTO locations (city_name, latitude, longitude) VALUES (?, ?, ?)",
            (city_name, latitude, longitude),
        )
        return True
    return False


def main():
    # Connect to SQLite database
    conn = sqlite3.connect("weather_data_final.db")
    cursor = conn.cursor()

    # Create the locations table if it doesn't exist
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS locations (
                        location_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        city_name TEXT UNIQUE,
                        latitude REAL,
                        longitude REAL
                    )"""
    )

    # Get the list of cities already in the database to avoid duplicates
    cursor.execute("SELECT city_name FROM locations")
    existing_cities = {row[0] for row in cursor.fetchall()}

    # Your list of city names
    cities = [
        "Ann Arbor",
        "New York City",
        "Tokyo",
        "Delhi",
        "Shanghai",
        "São Paulo",
        "Mexico City",
        "Cairo",
        "Mumbai",
        "Beijing",
        "Dhaka",
        "Osaka",
        "New York City",
        "Karachi",
        "Buenos Aires",
        "Chongqing",
        "Istanbul",
        "Kolkata",
        "Manila",
        "Lagos",
        "Rio de Janeiro",
        "Tianjin",
        "Kinshasa",
        "Guangzhou",
        "Los Angeles",
        "Moscow",
        "Shenzhen",
        "Lahore",
        "Bangalore",
        "Paris",
        "Bogotá",
        "Jakarta",
        "Chennai",
        "Lima",
        "Bangkok",
        "Seoul",
        "Nagoya",
        "Hyderabad",
        "London",
        "Tehran",
        "Chicago",
        "Chengdu",
        "Nanjing",
        "Wuhan",
        "Ho Chi Minh City",
        "Luanda",
        "Ahmedabad",
        "Kuala Lumpur",
        "Xi'an",
        "Hong Kong",
        "Dongguan",
        "Hangzhou",
        "Foshan",
        "Shenyang",
        "Riyadh",
        "Baghdad",
        "Santiago",
        "Surat",
        "Madrid",
        "Suzhou",
        "Pune",
        "Harbin",
        "Houston",
        "Dallas",
        "Toronto",
        "Dar es Salaam",
        "Miami",
        "Belo Horizonte",
        "Singapore",
        "Philadelphia",
        "Atlanta",
        "Fukuoka",
        "Khartoum",
        "Barcelona",
        "Johannesburg",
        "Saint Petersburg",
        "Qingdao",
        "Dalian",
        "Washington, D.C.",
        "Yangon",
        "Alexandria",
        "Jinan",
        "Guadalajara",
        "Boston",
        "Zhengzhou",
        "Hanoi",
        "Ankara",
        "Monterrey",
        "Yokohama",
        "Nairobi",
        "Zhoukou",
        "Cape Town",
        "Shijiazhuang",
        "Changsha",
        "Kunming",
        "Taiyuan",
        "Xiamen",
        "Hefei",
        "Changchun",
        "Sydney",
        "Nanchang",
        "Zhumadian",
        "Abington",
        "Ypsilanti",
        "Chicago",
    ]  # ... include all 100 city names in the list

    # Counter for the number of cities processed
    cities_processed = 0

    # Loop through each city name and insert data into the locations table
    for city_name in cities:
        if city_name not in existing_cities:
            success = insert_city_data(city_name, cursor)
            if success:
                cities_processed += 1
                if cities_processed >= 25:
                    break

    # Commit the changes and close the database connection
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
