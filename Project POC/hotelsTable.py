import requests
import json
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

RAPID_API_KEY = os.getenv('RAPID_API_KEY')
RAPID_API_HOST = "booking-com.p.rapidapi.com"
BASE_URL = "https://booking-com.p.rapidapi.com/v1/hotels/search"

SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

CITIES = ["New York", "San Francisco", "Chicago", "Seattle", "Las Vegas", "Los Angeles"]

CITY_COORDS = {
    "New York": {"lat": 40.7128, "lng": -74.0060},
    "San Francisco": {"lat": 37.7749, "lng": -122.4194},
    "Chicago": {"lat": 41.8781, "lng": -87.6298},
    "Seattle": {"lat": 47.6062, "lng": -122.3321},
    "Las Vegas": {"lat": 36.1699, "lng": -115.1398},
    "Los Angeles": {"lat": 34.0522, "lng": -118.2437}
}

def get_hotel_details(city_name, checkin_date, checkout_date, adults=2):
    """
    Get hotel information for a specific city using Booking.com API
    """
    headers = {
        "X-RapidAPI-Key": RAPID_API_KEY,
        "X-RapidAPI-Host": RAPID_API_HOST
    }
    
    params = {
        "units": "metric",
        "room_number": "1",
        "checkout_date": checkout_date,
        "filter_by_currency": "USD",
        "locale": "en-us",
        "adults_number": str(adults),
        "order_by": "popularity",
        "checkin_date": checkin_date,
        "dest_type": "city",
        "dest_id": city_name,
        "page_number": "0",
        "include_adjacency": "true"
    }
    
    if city_name in CITY_COORDS:
        params["latitude"] = CITY_COORDS[city_name]["lat"]
        params["longitude"] = CITY_COORDS[city_name]["lng"]
    
    try:
        response = requests.get(BASE_URL, headers=headers, params=params)
        data = response.json()
        
        if 'result' in data:
            hotels = []
            
            for hotel in data['result']:
                hotel_id = hotel.get('hotel_id')
                hotel_detail_url = f"https://booking-com.p.rapidapi.com/v1/hotels/details"
                detail_params = {
                    "hotel_id": hotel_id,
                    "locale": "en-us"
                }
                
                detail_response = requests.get(hotel_detail_url, headers=headers, params=detail_params)
                detail_data = detail_response.json()
                
                hotel_info = {
                    'hotel_id': hotel_id,
                    'city': city_name,
                    'name': hotel.get('hotel_name', ''),
                    'address': hotel.get('address', ''),
                    'latitude': hotel.get('latitude', 0),
                    'longitude': hotel.get('longitude', 0),
                    'star_rating': hotel.get('class', 0),
                    'review_score': hotel.get('review_score', 0),
                    'review_count': hotel.get('review_nr', 0),
                    'price_level': hotel.get('price_level', ''),
                    'min_price': hotel.get('min_total_price', 0),
                    'currency': hotel.get('currency_code', 'USD'),
                    'url': hotel.get('url', ''),
                    'image_url': hotel.get('main_photo_url', ''),
                    'checkout_time': detail_data.get('checkout', {}).get('to', ''),
                    'checkin_time': detail_data.get('checkin', {}).get('from', ''),
                    'is_free_cancellable': 1 if hotel.get('is_free_cancellable', False) else 0,
                    'amenities': ','.join(detail_data.get('facilities', [])),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                hotels.append(hotel_info)
            
            return hotels
    
    except Exception as e:
        print(f"Error fetching hotel data for {city_name}: {e}")
    
    return []

def main():
    all_hotels = []
    
    today = datetime.now()
    checkin_date = (today.replace(day=1) + pd.DateOffset(months=1)).strftime('%Y-%m-%d')
    checkout_date = (today.replace(day=1) + pd.DateOffset(months=1, days=2)).strftime('%Y-%m-%d')
    
    for city in CITIES:
        print(f"Processing hotels in {city}...")
        hotels = get_hotel_details(city, checkin_date, checkout_date)
        
        if hotels:
            all_hotels.extend(hotels)
            print(f"Found {len(hotels)} hotels in {city}")
        else:
            print(f"Could not retrieve hotel data for {city}")
    
    df = pd.DataFrame(all_hotels)
    
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
        CREATE TABLE IF NOT EXISTS hotels (
            hotel_id VARCHAR PRIMARY KEY,
            city VARCHAR,
            name VARCHAR,
            address VARCHAR,
            latitude FLOAT,
            longitude FLOAT,
            star_rating FLOAT,
            review_score FLOAT,
            review_count INTEGER,
            price_level VARCHAR,
            min_price FLOAT,
            currency VARCHAR,
            url VARCHAR,
            image_url VARCHAR,
            checkout_time VARCHAR,
            checkin_time VARCHAR,
            is_free_cancellable BOOLEAN,
            amenities VARCHAR,
            updated_at TIMESTAMP_NTZ
        )
        """)
        
        success, num_chunks, num_rows, output = write_pandas(conn, df, 'HOTELS')
        
        print(f"Data loaded successfully: {success}")
        print(f"Number of chunks: {num_chunks}")
        print(f"Number of rows: {num_rows}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")

if __name__ == "__main__":
    main()