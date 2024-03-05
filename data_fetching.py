import requests
import backoff
import concurrent.futures
from datetime import datetime
from constants import USERS_API_URL, WEATHER_API_URL
from structured_logger import StructuredLogger
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType

# Initialize the structured logger 
logger = StructuredLogger(__name__)

class DataFetcher:
    def __init__(self, api_key, spark: SparkSession):
        self.api_key = api_key
        self.spark = spark

    # Add exponential backoff as the retry strategy
    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.RequestException, requests.exceptions.ConnectionError, requests.exceptions.HTTPError, requests.exceptions.SSLError),
                          max_tries=5,
                          max_time=300,
                          giveup=lambda e: e.response is not None and e.response.status_code < 500)
    def fetch_user_data(self):
        try:
            # Fetch user data from the API and convert it to a Spark DataFrame
            response = requests.get(USERS_API_URL)
            response.raise_for_status()  # Raises HTTPError for bad responses
            users = response.json()
            processed_users = [{
                'id': user['id'],
                'name': user['name'],
                'username': user['username'],
                'email': user['email'],
                'latitude': float(user['address']['geo']['lat']),
                'longitude': float(user['address']['geo']['lng'])
            } for user in users]

            # Define the schema for the DataFrame
            schema = StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("username", StringType(), True),
                StructField("email", StringType(), True),
                StructField("latitude", FloatType(), True),
                StructField("longitude", FloatType(), True)
            ])

            # Create a Spark DataFrame using the processed user data and the defined schema
            user_data_df = self.spark.createDataFrame(processed_users, schema)            
            logger.info("User data fetched from the API successfully", endpoint=USERS_API_URL, status_code=response.status_code)
            return user_data_df
        except requests.exceptions.HTTPError as e:
            logger.error("HTTPError while fetching user data", error=str(e), endpoint=USERS_API_URL, status_code=e.response.status_code)
            raise 
        except requests.exceptions.RequestException as e:
            logger.error("RequestException while fetching user data", error=str(e), endpoint=USERS_API_URL)
            raise  
        except Exception as e:
            logger.error("Unexpected error while fetching user data", error=str(e))

    # Add exponential backoff as the retry strategy
    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.RequestException, requests.exceptions.ConnectionError, requests.exceptions.SSLError, requests.exceptions.HTTPError),
                          max_tries=5,
                          max_time=300,
                          giveup=lambda e: e.response is not None and e.response.status_code < 500)
    def fetch_weather_data_for_location(self, location):
        lat, lng, date = location
        datetime_obj = datetime.combine(date, datetime.min.time())
        # Convert the datetime object to a Unix timestamp to use in the API request
        timestamp = int(datetime_obj.timestamp())

        url = WEATHER_API_URL.format(lat=lat, lng=lng, timestamp=timestamp, api_key=self.api_key)
        try:
            # Fetch weather data from the API 
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            temp = float(data['data'][0]['temp'])
            weather_condition = data['data'][0]['weather'][0]['main']
            weather_data = (lat, lng, date, temp, weather_condition)  
            return weather_data
        except requests.exceptions.HTTPError as e:
            logger.error("HTTPError while fetching weather data", error=str(e), endpoint=WEATHER_API_URL, status_code=e.response.status_code)
            raise  
        except requests.exceptions.RequestException as e:
            logger.error("RequestException while fetching weather data", error=str(e), endpoint=WEATHER_API_URL)
            raise  
        except Exception as e:
            logger.error("Unexpected error while fetching weather data", error=str(e))

    def fetch_bulk_weather_info(self, locations):
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all weather info fetching tasks to the executor
            futures = [executor.submit(self.fetch_weather_data_for_location, loc) for loc in locations]
            # Collect results as they complete
            results = []
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)
        # Define the schema for the DataFrame
        schema = StructType([
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
            StructField("date", DateType(), True),
            StructField("temperature", FloatType(), True),
            StructField("weather_condition", StringType(), True)
        ])
        # Create a Spark DataFrame using the processed weather data and the defined schema
        weather_df = self.spark.createDataFrame(results, schema)
        return weather_df