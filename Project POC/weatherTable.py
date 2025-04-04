import requests
import pandas as pd
import snowflake.connector
from datetime import datetime
import time
import os
from dotenv import load_dotenv

load_dotenv()

cities = [
    {"name": "New York", "lat": 40.7128, "lon": 74.0060},
    {"name": "San Francisco", "lat": 37.7749, "lon": 122.4194},
    {"name": "Chicago", "lat": 41.8781, "lon": 87.6298},
    {"name": "Seattle", "lat": 47.6062, "lon": 122.3321},
    {"name": "Las Vegas", "lat": 36.1699, "lon": 115.1398},
    {"name": "Los Angeles", "lat": 34.0522, "lon": 118.2437}
]

def get_weather_data():
    api_key = os.getenv('OPENWEATHERMAP_API_KEY')
    base_url = "https://api.openweathermap.org/data/2.5/onecall"
    
    weather_data = []
    
    for city in cities:
        params = {
            "lat": city["lat"],
            "lon": city["lon"],
            "units": "imperial",
            "exclude": "minutely,alerts",
            "appid": api_key
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            current = data.get("current", {})
            
            weather_entry = {
                "CITY_NAME": city["name"],
                "LATITUDE": city["lat"],
                "LONGITUDE": city["lon"],
                "TIMESTAMP": datetime.fromtimestamp(current.get("dt", 0)),
                "TEMPERATURE": current.get("temp"),
                "FEELS_LIKE": current.get("feels_like"),
                "HUMIDITY": current.get("humidity"),
                "WIND_SPEED": current.get("wind_speed"),
                "WIND_DIRECTION": current.get("wind_deg"),
                "WEATHER_CONDITION": current.get("weather", [{}])[0].get("main"),
                "WEATHER_DESCRIPTION": current.get("weather", [{}])[0].get("description"),
                "PRESSURE": current.get("pressure"),
                "VISIBILITY": current.get("visibility"),
                "CLOUD_COVER": current.get("clouds"),
                "UV_INDEX": current.get("uvi"),
                "FORECAST_DATE": datetime.now().date()
            }
            
            for idx, daily_data in enumerate(data.get("daily", [])):
                forecast_entry = weather_entry.copy()
                forecast_entry["TIMESTAMP"] = datetime.fromtimestamp(daily_data.get("dt", 0))
                forecast_entry["TEMPERATURE"] = daily_data.get("temp", {}).get("day")
                forecast_entry["MIN_TEMPERATURE"] = daily_data.get("temp", {}).get("min")
                forecast_entry["MAX_TEMPERATURE"] = daily_data.get("temp", {}).get("max")
                forecast_entry["FEELS_LIKE"] = daily_data.get("feels_like", {}).get("day")
                forecast_entry["HUMIDITY"] = daily_data.get("humidity")
                forecast_entry["WIND_SPEED"] = daily_data.get("wind_speed")
                forecast_entry["WIND_DIRECTION"] = daily_data.get("wind_deg")
                forecast_entry["WEATHER_CONDITION"] = daily_data.get("weather", [{}])[0].get("main")
                forecast_entry["WEATHER_DESCRIPTION"] = daily_data.get("weather", [{}])[0].get("description")
                forecast_entry["PRESSURE"] = daily_data.get("pressure")
                forecast_entry["CLOUD_COVER"] = daily_data.get("clouds")
                forecast_entry["UV_INDEX"] = daily_data.get("uvi")
                forecast_entry["PRECIPITATION_PROBABILITY"] = daily_data.get("pop")
                forecast_entry["FORECAST_DAY"] = idx
                
                weather_data.append(forecast_entry)
            
            time.sleep(1)
            
        except Exception as e:
            print(f"Error fetching weather data for {city['name']}: {e}")
    
    return pd.DataFrame(weather_data)

def load_to_snowflake(df):
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS WEATHER_DATA (
            CITY_NAME VARCHAR(50),
            LATITUDE FLOAT,
            LONGITUDE FLOAT,
            TIMESTAMP TIMESTAMP_NTZ,
            TEMPERATURE FLOAT,
            MIN_TEMPERATURE FLOAT,
            MAX_TEMPERATURE FLOAT,
            FEELS_LIKE FLOAT,
            HUMIDITY FLOAT,
            WIND_SPEED FLOAT,
            WIND_DIRECTION FLOAT,
            WEATHER_CONDITION VARCHAR(50),
            WEATHER_DESCRIPTION VARCHAR(100),
            PRESSURE FLOAT,
            VISIBILITY FLOAT,
            CLOUD_COVER FLOAT,
            UV_INDEX FLOAT,
            PRECIPITATION_PROBABILITY FLOAT,
            FORECAST_DAY INT,
            FORECAST_DATE DATE
        )
        """)
        
        temp_file = '/tmp/weather_data.csv'
        df.to_csv(temp_file, index=False, header=False)
        
        cursor.execute("CREATE OR REPLACE STAGE temp_stage")
        cursor.execute(f"PUT file://{temp_file} @temp_stage")
        
        cursor.execute("""
        COPY INTO WEATHER_DATA
        FROM @temp_stage/weather_data.csv.gz
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 0)
        """)
        
        print("Weather data loaded to Snowflake successfully")
        
    except Exception as e:
        print(f"Error loading data to Snowflake: {e}")
        
    finally:
        cursor.close()
        conn.close()

def main():
    weather_df = get_weather_data()
    
    if not weather_df.empty:
        load_to_snowflake(weather_df)
    else:
        print("No weather data to load")

if __name__ == "__main__":
    main()
