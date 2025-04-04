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
RAPID_API_HOST = "tripadvisor16.p.rapidapi.com"
BASE_URL = "https://tripadvisor16.p.rapidapi.com/api/v1/attractions/searchLocation"
DETAILS_URL = "https://tripadvisor16.p.rapidapi.com/api/v1/attractions/getAttractionDetails"

SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

CITIES = ["New York", "San Francisco", "Chicago", "Seattle", "Las Vegas", "Los Angeles"]

def get_location_id(city_name):
    headers = {
        "X-RapidAPI-Key": RAPID_API_KEY,
        "X-RapidAPI-Host": RAPID_API_HOST
    }
    
    params = {
        "query": city_name,
        "language": "en"
    }
    
    try:
        response = requests.get(BASE_URL, headers=headers, params=params)
        data = response.json()
        
        if 'data' in data and len(data['data']) > 0:
            for result in data['data']:
                if 'locationId' in result:
                    return result['locationId']
    
    except Exception as e:
        print(f"Error fetching location ID for {city_name}: {e}")
    
    return None

def get_attractions(location_id, city_name):
    headers = {
        "X-RapidAPI-Key": RAPID_API_KEY,
        "X-RapidAPI-Host": RAPID_API_HOST
    }
    
    attractions_url = "https://tripadvisor16.p.rapidapi.com/api/v1/attractions/searchAttractionsInLocation"
    params = {
        "locationId": location_id,
        "language": "en",
        "currency": "USD",
        "offset": "0"
    }
    
    try:
        response = requests.get(attractions_url, headers=headers, params=params)
        data = response.json()
        
        if 'data' in data and 'attractions' in data['data']:
            attractions = []
            
            for attraction in data['data']['attractions']:
                attraction_id = attraction.get('locationId')
                
                detail_params = {
                    "locationId": attraction_id,
                    "language": "en",
                    "currency": "USD"
                }
                
                detail_response = requests.get(DETAILS_URL, headers=headers, params=detail_params)
                detail_data = detail_response.json()
                
                address = ''
                if 'data' in detail_data and 'location' in detail_data['data']:
                    address_obj = detail_data['data']['location']
                    address_parts = []
                    
                    if 'street1' in address_obj and address_obj['street1']:
                        address_parts.append(address_obj['street1'])
                    if 'street2' in address_obj and address_obj['street2']:
                        address_parts.append(address_obj['street2'])
                    if 'city' in address_obj and address_obj['city']:
                        address_parts.append(address_obj['city'])
                    if 'state' in address_obj and address_obj['state']:
                        address_parts.append(address_obj['state'])
                    if 'postalCode' in address_obj and address_obj['postalCode']:
                        address_parts.append(address_obj['postalCode'])
                    
                    address = ', '.join(address_parts)
                
                attraction_info = {
                    'attraction_id': attraction_id,
                    'city': city_name,
                    'name': attraction.get('title', ''),
                    'description': attraction.get('description', ''),
                    'address': address,
                    'latitude': attraction.get('latitude', 0),
                    'longitude': attraction.get('longitude', 0),
                    'rating': attraction.get('averageRating', 0),
                    'review_count': attraction.get('reviewCount', 0),
                    'category': attraction.get('primaryCategory', {}).get('name', ''),
                    'subcategory': ','.join([subcat.get('name', '') for subcat in attraction.get('secondaryCategories', [])]),
                    'price_level': attraction.get('priceLevel', ''),
                    'price_range': attraction.get('priceRange', ''),
                    'website': detail_data.get('data', {}).get('website', ''),
                    'image_url': attraction.get('thumbnail', {}).get('url', ''),
                    'suggested_duration': detail_data.get('data', {}).get('suggestedDuration', ''),
                    'opening_hours': ', '.join(detail_data.get('data', {}).get('openingHours', [])),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                attractions.append(attraction_info)
            
            return attractions
    
    except Exception as e:
        print(f"Error fetching attraction data for {city_name}: {e}")
    
    return []

def main():
    all_attractions = []
    
    for city in CITIES:
        print(f"Processing attractions in {city}...")
        
        location_id = get_location_id(city)
        
        if location_id:
            attractions = get_attractions(location_id, city)
            
            if attractions:
                all_attractions.extend(attractions)
                print(f"Found {len(attractions)} attractions in {city}")
            else:
                print(f"Could not retrieve attraction data for {city}")
        else:
            print(f"Could not find location ID for {city}")
    
    df = pd.DataFrame(all_attractions)
    
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
        CREATE TABLE IF NOT EXISTS attractions (
            attraction_id VARCHAR PRIMARY KEY,
            city VARCHAR,
            name VARCHAR,
            description VARCHAR,
            address VARCHAR,
            latitude FLOAT,
            longitude FLOAT,
            rating FLOAT,
            review_count INTEGER,
            category VARCHAR,
            subcategory VARCHAR,
            price_level VARCHAR,
            price_range VARCHAR,
            website VARCHAR,
            image_url VARCHAR,
            suggested_duration VARCHAR,
            opening_hours VARCHAR,
            updated_at TIMESTAMP_NTZ
        )
        """)
        
        success, num_chunks, num_rows, output = write_pandas(conn, df, 'ATTRACTIONS')
        
        print(f"Data loaded successfully: {success}")
        print(f"Number of chunks: {num_chunks}")
        print(f"Number of rows: {num_rows}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")

if __name__ == "__main__":
    main()
