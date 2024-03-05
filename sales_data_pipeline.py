import configparser
from data_fetching import DataFetcher
from data_transformation import DataTransformer
from data_validation import DataValidator
from database import DatabaseManager
from structured_logger import StructuredLogger
from constants import *
from pyspark.sql import SparkSession

# Initialize the structured logger
logger = StructuredLogger(__name__)

class SalesDataPipeline:
    def __init__(self, config_path):
        # Load configuration
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.api_key = self.config['API']['api_key']
        self.db_config = {
            'dbname': self.config['Database']['dbname'],
            'user': self.config['Database']['user'],
            'password': self.config['Database']['password'],
            'host': self.config['Database']['host']
        }
        self.csv_path = self.config['DATA']['data_path']
        self.spark_jars = self.config['Spark']['jars']
        # Initialize Spark session
        self.spark = SparkSession.builder.appName("SalesDataPipeline").config("spark.jars", self.spark_jars).getOrCreate()

    def run(self):
        try:
            # Fetch and transform data
            logger.info("Fetching user information from the API")
            fetcher = DataFetcher(self.api_key, self.spark)
            user_data = fetcher.fetch_user_data()
            transformer = DataTransformer(user_data, fetcher, self.spark)
            final_data = transformer.transform_data(self.csv_path)

            # Calculate different metrics required for analysis
            logger.info("Calculating different metrics for analysis")
            customer_metrics = transformer.calculate_customer_metrics(final_data)
            product_metrics = transformer.calculate_product_metrics(final_data)
            seasonal_sales_metrics = transformer.calculate_seasonal_sales_metrics(final_data)
            monthly_sales_metrics = transformer.calculate_monthly_sales_metrics(final_data)

            # Validate the data
            DataValidator.validate_data(final_data)

            # Setup databases and write data to primary tables
            db_manager = DatabaseManager(self.db_config, self.spark)            
            db_manager.setup_database()           
            db_manager.handle_primary_data(final_data)

            # Write metrics to respective tables
            logger.info("Writing calculated metrics to respective tables")
            db_manager.write_to_db(customer_metrics, 'customer_metrics')
            db_manager.write_to_db(product_metrics, 'product_metrics')
            db_manager.write_to_db(seasonal_sales_metrics, 'seasonal_sales_metrics')
            db_manager.write_to_db(monthly_sales_metrics, 'monthly_sales_metrics')            
            logger.info("Sales data pipeline executed successfully.")
        except Exception as e:
            logger.error("Sales data pipeline execution failed", error=str(e))
            raise



if __name__ == "__main__":
    pipeline = SalesDataPipeline('config.ini')
    pipeline.run()
