r"""°°°
# Open Weather Map
°°°"""
# |%%--%%| <VT6zFxmFar|PML09xKaNs>

import requests
import datetime
import time # For potential rate limiting
import pandas as pd

# |%%--%%| <PML09xKaNs|KE5jtD3HsV>

# Store your API keys securely!
OWM_API_KEY = "0f43e304d419803820851b1775d44dde" # Replace with your key
DANANG_LAT = 16.047079
DANANG_LON = 108.220825

# |%%--%%| <KE5jtD3HsV|uXfskOIgq3>

def get_owm_weather(lat, lon, api_key):
    base_url = "https://api.openweathermap.org/data/3.0/onecall"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "exclude": "minutely,alerts", # Exclude parts we don't need
        "units": "metric" # Celsius, m/s, ...
    }
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching OpenWeatherMap data: {e}")
        return None

owm_data = get_owm_weather(DANANG_LAT, DANANG_LON, OWM_API_KEY)

# |%%--%%| <uXfskOIgq3|0TvPGepjmc>

if owm_data:
    print("\nSuccessfully fetched OpenWeatherMap data.")
    current_weather = owm_data.get('current', {})
    hourly_forecast = owm_data.get('hourly', [])
    daily_forecast = owm_data.get('daily', [])

    # Process the Current Weather
    current_weather_processed = {
        "timestamp_utc": datetime.datetime.fromtimestamp(current_weather.get('dt', 0), tz=datetime.timezone.utc),
        "temp_c": current_weather.get('temp'),
        "feels_like_c": current_weather.get('feels_like'),
        "pressure_hpa": current_weather.get('pressure'),
        "humidity_percent": current_weather.get('humidity'),
        "dew_point_c": current_weather.get('dew_point'),
        "uvi": current_weather.get('uvi'),
        "clouds_percent": current_weather.get('clouds'),
        "visibility_m": current_weather.get('visibility'),
        "wind_speed_mps": current_weather.get('wind_speed'),
        "wind_deg": current_weather.get('wind_deg'),
        "wind_gust_mps": current_weather.get('wind_gust'),
        "weather_main": current_weather.get('weather', [{}])[0].get('main'),
        "weather_desc": current_weather.get('weather', [{}])[0].get('description'),
        "weather_icon": current_weather.get('weather', [{}])[0].get('icon'),
        "rain_1h_mm": current_weather.get('rain', {}).get('1h'), # Check if 'rain' key exists
        "snow_1h_mm": current_weather.get('snow', {}).get('1h')  # Check if 'snow' key exists
    }
    current_df = pd.DataFrame([current_weather_processed])

    # --- Process Hourly Forecast ---
    hourly_list = []
    for hour_data in hourly_forecast:
        hourly_list.append({
            "timestamp_utc": datetime.datetime.fromtimestamp(hour_data.get('dt', 0), tz=datetime.timezone.utc),
            "temp_c": hour_data.get('temp'),
            "feels_like_c": hour_data.get('feels_like'),
            "pressure_hpa": hour_data.get('pressure'),
            "humidity_percent": hour_data.get('humidity'),
            "clouds_percent": hour_data.get('clouds'),
            "visibility_m": hour_data.get('visibility'),
            "wind_speed_mps": hour_data.get('wind_speed'),
            "wind_deg": hour_data.get('wind_deg'),
            "pop_percent": hour_data.get('pop', 0) * 100, # Probability of precipitation
            "weather_main": hour_data.get('weather', [{}])[0].get('main'),
            "rain_1h_mm": hour_data.get('rain', {}).get('1h'),
            "snow_1h_mm": hour_data.get('snow', {}).get('1h')
        })
    hourly_df = pd.DataFrame(hourly_list)

    # Save data
    current_df.to_csv("danang_current_weather_owm.csv", index=False)
    hourly_df.to_csv("danang_hourly_forecast_owm.csv", index=False)
    print("OpenWeatherMap current and hourly data saved.")
else:
    print("Failed to fetch OpenWeatherMap data.")

# |%%--%%| <0TvPGepjmc|kQIbZPQT5C>

print("\nCurrent Weather Processed:")
current_df

# |%%--%%| <kQIbZPQT5C|fNJTtJRbcg>

print("\nHourly Forecast Sample (first 5 hours):")
hourly_df.head()

# |%%--%%| <fNJTtJRbcg|Y3uzvv5tzU>
r"""°°°
# Historical Weather (Okilab API)
°°°"""
# |%%--%%| <Y3uzvv5tzU|NCoy1MT9FW>

import numpy as np
from io import StringIO # To read the string data as a file

historical_weather_df = pd.read_csv("04_05_25.csv")

# Rename columns for consistency and ease of use 
rename_map = {
    "datetime (UTC)": "timestamp_utc",
    "coordinates (lat,lon)": "coordinates_lat_lon",
    "model (name)": "model_name",
    "model elevation (surface)": "model_elevation_m",
    "utc_offset (hrs)": "utc_offset_hrs",
    "temperature (degC)": "temp_c",
    "relative_humidity (0-1)": "relative_humidity",
    "wind_speed (m/s)": "wind_speed_mps",
    "wind_direction (deg)": "wind_deg",
    "10m_wind_gust (m/s)": "wind_gust_mps",
    "total_cloud_cover (0-1)": "cloud_cover", 
    "total_precipitation (mm of water equivalent)": "precip_mm"
}
historical_weather_df.rename(columns=rename_map, inplace=True)

# Convert timestamp_utc to datetime objects and set timezone
historical_weather_df['timestamp_utc'] = pd.to_datetime(historical_weather_df['timestamp_utc'], utc=True)

# Handle missing values (example: forward fill, then backward fill for any remaining at the start)
numeric_cols = historical_weather_df.select_dtypes(include=np.number).columns
for col in numeric_cols:
    if historical_weather_df[col].isnull().any():
        historical_weather_df[col] = historical_weather_df[col].ffill().bfill()

# Cyclical Time Features
if 'timestamp_utc' in historical_weather_df.columns:
    dt_col = historical_weather_df['timestamp_utc']
    historical_weather_df['hour_of_day'] = dt_col.dt.hour
    historical_weather_df['day_of_week'] = dt_col.dt.dayofweek  # Monday=0, Sunday=6
    historical_weather_df['day_of_year'] = dt_col.dt.dayofyear
    historical_weather_df['month_of_year'] = dt_col.dt.month
    historical_weather_df['year'] = dt_col.dt.year # Useful for partitioning or long-term trends

    # Sin/Cos transformations
    historical_weather_df['hour_sin'] = np.sin(2 * np.pi * historical_weather_df['hour_of_day'] / 24)
    historical_weather_df['hour_cos'] = np.cos(2 * np.pi * historical_weather_df['hour_of_day'] / 24)
    
    historical_weather_df['day_of_week_sin'] = np.sin(2 * np.pi * historical_weather_df['day_of_week'] / 7)
    historical_weather_df['day_of_week_cos'] = np.cos(2 * np.pi * historical_weather_df['day_of_week'] / 7)
    
    # For month, it's 1-12
    historical_weather_df['month_sin'] = np.sin(2 * np.pi * (historical_weather_df['month_of_year'] -1) / 12) # Adjust to 0-11 range for proper cycle
    historical_weather_df['month_cos'] = np.cos(2 * np.pi * (historical_weather_df['month_of_year'] -1) / 12)

    # Day of year 
    historical_weather_df['day_of_year_sin'] = np.sin(2 * np.pi * (historical_weather_df['day_of_year']-1) / 365.25)
    historical_weather_df['day_of_year_cos'] = np.cos(2 * np.pi * (historical_weather_df['day_of_year']-1) / 365.25)
else:
    print("Warning: 'timestamp_utc' column not found. Cannot create cyclical features.")



# |%%--%%| <NCoy1MT9FW|CGuydnrH4P>

# Display processed data
print("\n--- Info for Processed Oikolab Data ---")
historical_weather_df.info()

print("\n--- Processed Oikolab Historical Weather Data (Head) ---")
historical_weather_df.head()

# |%%--%%| <CGuydnrH4P|M3kmA9A7TM>

# Save the processed DataFrame
# Following the previous pattern of saving Oikolab data to CSV
output_filename = "danang_historical_weather_oikolab_processed.csv"
try:
    historical_weather_df.to_csv(output_filename, index=False)
    print(f"\nProcessed Oikolab historical data saved to '{output_filename}'.")
except Exception as e:
    print(f"\nError saving processed data to CSV: {e}")
