import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import requests


# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
	"latitude": 42.2776,
	"longitude": -83.7409,
	"start_date": "2022-01-01",
	"end_date": "2022-12-31",
	"hourly": ["temperature_2m", "rain", "snowfall"]
}
responses = openmeteo.weather_api(url, params=params)