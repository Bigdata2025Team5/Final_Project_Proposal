import requests
import json
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
BASE_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"

SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

CITIES = ["New York", "San Francisco", "Chicago", "Seattle", "Las Vegas", "Los Angeles"]

def get_city_details(city_name):
    """
    Get detailed information about a city using Google Places API
    """
    params = {
        'query': f"{city_name} city",
        'type': 'locality',
        'key': API_KEY
    }
    
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    
    if 'results' in data and len(data['results']) > 0:
        city_data = data['results'][0]
        
        place_id = city_data.get('place_id')
        details_url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=name,formatted_address,geometry,place_id,vicinity,url,website,rating,user_ratings_total,formatted_phone_number,international_phone_number,opening_hours,price_level,types&key={API_KEY}"
        details_response = requests.get(details_url)
        details_data = details_response.json()
        
        if 'result' in details_data:
            details = details_data['result']
            
            return {
                'city_id': place_id,
                'city_name': city_name,
                'formatted_address': details.get('formatted_address', ''),
                'latitude': details.get('geometry', {}).get('location', {}).get('lat'),
                'longitude': details.get('geometry', {}).get('location', {}).get('lng'),
                'google_map_url': details.get('url', ''),
                'website': details.get('website', ''),
                'rating': details.get('rating', 0),
                'user_ratings_total': details.get('user_ratings_total', 0),
                'types': ','.join(details.get('types', [])),
                'photo_reference': city_data.get('photos', [{}])[0].get('photo_reference', '') if 'photos' in city_data else '',
                'vicinity': details.get('vicinity', ''),
                'timezone': '',  # We would need another API call to get timezone
                'country': 'USA',
                'state': city_name.split(',')[-1].strip() if ',' in city_name else '',
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
    
    return None

def main():
    cities_data = []
    
    for city in CITIES:
        print(f"Processing {city}...")
        city_details = get_city_details(city)
        
        if city_details:
            cities_data.append(city_details)
        else:
            print(f"Could not retrieve data for {city}")
    
    df = pd.DataFrame(cities_data)
    
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )

        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS destinations (
            city_id VARCHAR PRIMARY KEY,
            city_name VARCHAR,
            formatted_address VARCHAR,
            latitude FLOAT,
            longitude FLOAT,
            google_map_url VARCHAR,
            website VARCHAR,
            rating FLOAT,
            user_ratings_total INTEGER,
            types VARCHAR,
            photo_reference VARCHAR,
            vicinity VARCHAR,
            timezone VARCHAR,
            country VARCHAR,
            state VARCHAR,
            updated_at TIMESTAMP_NTZ
        )
        """)
        
        success, num_chunks, num_rows, output = write_pandas(conn, df, 'DESTINATIONS')
        
        print(f"Data loaded successfully: {success}")
        print(f"Number of chunks: {num_chunks}")
        print(f"Number of rows: {num_rows}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")

if __name__ == "__main__":
    main()