import requests
import json
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

YELP_API_KEY = os.getenv('YELP_API_KEY')
BASE_URL = "https://api.yelp.com/v3/businesses/search"
DETAILS_URL = "https://api.yelp.com/v3/businesses/"

SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

CITIES = ["New York", "San Francisco", "Chicago", "Seattle", "Las Vegas", "Los Angeles"]

def get_restaurants(city_name, limit=50, offset=0):
    headers = {
        "Authorization": f"Bearer {YELP_API_KEY}",
        "accept": "application/json"
    }
    
    params = {
        "location": city_name,
        "term": "restaurants",
        "limit": limit,
        "offset": offset,
        "sort_by": "best_match"
    }
    
    try:
        response = requests.get(BASE_URL, headers=headers, params=params)
        data = response.json()
        
        if 'businesses' in data:
            restaurants = []
            
            for business in data['businesses']:
                business_id = business.get('id')
                detail_response = requests.get(f"{DETAILS_URL}{business_id}", headers=headers)
                detail_data = detail_response.json()
                
                categories = [category.get('title') for category in business.get('categories', [])]
                
                hours = []
                if 'hours' in detail_data:
                    for hour in detail_data['hours'][0].get('open', []):
                        day = hour.get('day')
                        start = hour.get('start')
                        end = hour.get('end')
                        hours.append(f"{day}:{start}-{end}")
                
                restaurant_info = {
                    'restaurant_id': business_id,
                    'city': city_name,
                    'name': business.get('name', ''),
                    'address': ', '.join(business.get('location', {}).get('display_address', [])),
                    'latitude': business.get('coordinates', {}).get('latitude', 0),
                    'longitude': business.get('coordinates', {}).get('longitude', 0),
                    'rating': business.get('rating', 0),
                    'review_count': business.get('review_count', 0),
                    'price_level': business.get('price', ''),
                    'phone': business.get('display_phone', ''),
                    'url': business.get('url', ''),
                    'image_url': business.get('image_url', ''),
                    'cuisine_types': ','.join(categories),
                    'is_closed': 1 if business.get('is_closed', True) else 0,
                    'transactions': ','.join(business.get('transactions', [])),
                    'operating_hours': ';'.join(hours),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                restaurants.append(restaurant_info)
            
            return restaurants
    
    except Exception as e:
        print(f"Error fetching restaurant data for {city_name}: {e}")
    
    return []

def main():
    all_restaurants = []
    
    for city in CITIES:
        print(f"Processing restaurants in {city}...")
        
        for offset in [0, 50]:
            restaurants = get_restaurants(city, limit=50, offset=offset)
            
            if restaurants:
                all_restaurants.extend(restaurants)
                print(f"Found {len(restaurants)} restaurants in {city} (offset: {offset})")
            else:
                print(f"Could not retrieve restaurant data for {city} (offset: {offset})")
    
    df = pd.DataFrame(all_restaurants)
    
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
        CREATE TABLE IF NOT EXISTS restaurants (
            restaurant_id VARCHAR PRIMARY KEY,
            city VARCHAR,
            name VARCHAR,
            address VARCHAR,
            latitude FLOAT,
            longitude FLOAT,
            rating FLOAT,
            review_count INTEGER,
            price_level VARCHAR,
            phone VARCHAR,
            url VARCHAR,
            image_url VARCHAR,
            cuisine_types VARCHAR,
            is_closed BOOLEAN,
            transactions VARCHAR,
            operating_hours VARCHAR,
            updated_at TIMESTAMP_NTZ
        )
        """)
        
        success, num_chunks, num_rows, output = write_pandas(conn, df, 'RESTAURANTS')
        
        print(f"Data loaded successfully: {success}")
        print(f"Number of chunks: {num_chunks}")
        print(f"Number of rows: {num_rows}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")

if __name__ == "__main__":
    main()
