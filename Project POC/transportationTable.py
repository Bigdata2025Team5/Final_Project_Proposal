import requests
import pandas as pd
import snowflake.connector
from datetime import datetime
import time
import itertools
import os
from dotenv import load_dotenv

load_dotenv()

cities = ["New York", "San Francisco", "Chicago", "Seattle", "Las Vegas", "Los Angeles"]

def get_transportation_data():
    api_key = os.getenv('ROME2RIO_API_KEY')
    base_url = "https://api.rome2rio.com/api/1.5/json/Search"
    
    transportation_data = []
    
    city_pairs = list(itertools.permutations(cities, 2))
    
    for origin, destination in city_pairs:
        params = {
            "key": api_key,
            "oName": origin,
            "dName": destination,
            "currencyCode": "USD"
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            routes = data.get("routes", [])
            
            for route in routes:
                segments = []
                
                for segment in route.get("segments", []):
                    segment_data = {
                        "SEGMENT_TYPE": segment.get("kind"),
                        "SEGMENT_DISTANCE": segment.get("distance"),
                        "SEGMENT_DURATION": segment.get("duration")
                    }
                    segments.append(segment_data)
                
                route_entry = {
                    "ORIGIN_CITY": origin,
                    "DESTINATION_CITY": destination,
                    "ROUTE_NAME": route.get("name"),
                    "ROUTE_TYPE": route.get("kind"),
                    "TOTAL_DISTANCE": route.get("distance"),
                    "TOTAL_DURATION": route.get("duration"),
                    "PRICE_LOW": route.get("indicativePrices", [{}])[0].get("priceLow"),
                    "PRICE_HIGH": route.get("indicativePrices", [{}])[0].get("priceHigh"),
                    "CURRENCY": route.get("indicativePrices", [{}])[0].get("currency"),
                    "SEGMENTS": str(segments),
                    "DATA_TIMESTAMP": datetime.now()
                }
                
                transportation_data.append(route_entry)
            
            time.sleep(1)
            
        except Exception as e:
            print(f"Error fetching transportation data for {origin} to {destination}: {e}")
    
    return pd.DataFrame(transportation_data)

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
        CREATE TABLE IF NOT EXISTS TRANSPORTATION_DATA (
            ORIGIN_CITY VARCHAR(50),
            DESTINATION_CITY VARCHAR(50),
            ROUTE_NAME VARCHAR(100),
            ROUTE_TYPE VARCHAR(50),
            TOTAL_DISTANCE FLOAT,
            TOTAL_DURATION FLOAT,
            PRICE_LOW FLOAT,
            PRICE_HIGH FLOAT,
            CURRENCY VARCHAR(10),
            SEGMENTS VARCHAR(10000),
            DATA_TIMESTAMP TIMESTAMP_NTZ
        )
        """)
        
        temp_file = '/tmp/transportation_data.csv'
        df.to_csv(temp_file, index=False, header=False)
        
        cursor.execute("CREATE OR REPLACE STAGE temp_stage")
        cursor.execute(f"PUT file://{temp_file} @temp_stage")
        
        cursor.execute("""
        COPY INTO TRANSPORTATION_DATA
        FROM @temp_stage/transportation_data.csv.gz
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 0)
        """)
        
        print("Transportation data loaded to Snowflake successfully")
        
    except Exception as e:
        print(f"Error loading data to Snowflake: {e}")
        
    finally:
        cursor.close()
        conn.close()

def main():
    transportation_df = get_transportation_data()
    
    if not transportation_df.empty:
        load_to_snowflake(transportation_df)
    else:
        print("No transportation data to load")

if __name__ == "__main__":
    main()
