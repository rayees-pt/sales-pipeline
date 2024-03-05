import psycopg2
from psycopg2.extras import execute_batch
from pyspark.sql import SparkSession
from structured_logger import StructuredLogger
from constants import *
from pyspark.sql import functions as F

# Initialize the structured logger
logger = StructuredLogger(__name__)

class DatabaseManager:
    def __init__(self, db_config, spark_session: SparkSession):
        self.db_config = db_config
        self.spark = spark_session
        self.jdbc_url = f"jdbc:postgresql://{db_config['host']}/{db_config['dbname']}"
        self.connection_properties = {
            "user": db_config['user'],
            "password": db_config['password'],
            "driver": "org.postgresql.Driver"
        }

    def setup_staging_tables(self):
        try:
            # Create staging tables
            logger.info("Setting up staging tables")
            with psycopg2.connect(**self.db_config) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    self._create_staging_tables(cur)
                    logger.info("Staging tables setup completed successfully.")
        except Exception as e:
            logger.error("Failed to setup staging tables", error=str(e))
            raise

    def _create_staging_tables(self, cursor):
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {STAGING_CUSTOMERS_TABLE} (
                {CUSTOMER_ID} SERIAL PRIMARY KEY,
                {NAME} VARCHAR(255) NOT NULL,
                {USERNAME} VARCHAR(255) UNIQUE NOT NULL,
                {EMAIL} VARCHAR(255) UNIQUE NOT NULL,
                {LATITUDE} DECIMAL(9,6),
                {LONGITUDE} DECIMAL(9,6)
            );


            CREATE TABLE IF NOT EXISTS {STAGING_SALES_TABLE} (            
                {ORDER_ID} INT NOT NULL,
                {CUSTOMER_ID} INT NOT NULL,
                {PRODUCT_ID} INT NOT NULL,
                {QUANTITY} INT NOT NULL,
                {PRICE} DECIMAL(10,2) NOT NULL,
                {ORDER_DATE} DATE NOT NULL,
                {TEMPERATURE} DECIMAL(5,2),
                {WEATHER_CONDITION} VARCHAR(255),
                PRIMARY KEY ({ORDER_ID}, {PRODUCT_ID}),
                FOREIGN KEY ({CUSTOMER_ID}) REFERENCES {STAGING_CUSTOMERS_TABLE}({CUSTOMER_ID}),
                CONSTRAINT chk_quantity CHECK ({QUANTITY} > 0),
                CONSTRAINT chk_temperature CHECK ({TEMPERATURE} BETWEEN -100 AND 100)
        );
        """)

    def setup_database(self):
        try:
            # Create schema and tables for main and metrics tables
            logger.info("Setting up database and tables")
            with psycopg2.connect(**self.db_config) as conn:
                conn.autocommit = True  
                with conn.cursor() as cur:
                    self._create_schema(cur)
                    self._create_metrics_tables(cur)  
                    logger.info("Database and tables setup completed successfully.")
        except Exception as e:
            logger.error("Failed to setup database and tables", error=str(e))
            raise

    def merge_staging_to_production(self):
        try:
            # Merge staging tables to production tables
            logger.info("Merging staging tables to production tables")
            with psycopg2.connect(**self.db_config) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    self._merge_staging_to_production(cur)
                    logger.info("Merge staging to production tables completed successfully.")
        except Exception as e:
            logger.error("Failed to merge staging to production", error=str(e))
            raise

    def _merge_staging_to_production(self, cursor):
        cursor.execute(f"""
            INSERT INTO {CUSTOMERS_TABLE} ({CUSTOMER_ID}, {NAME}, {USERNAME}, {EMAIL}, {LATITUDE}, {LONGITUDE})
            SELECT {CUSTOMER_ID}, {NAME}, {USERNAME}, {EMAIL}, {LATITUDE}, {LONGITUDE}
            FROM {STAGING_CUSTOMERS_TABLE}
            ON CONFLICT ({USERNAME}) DO NOTHING;

            INSERT INTO {SALES_TABLE} ({ORDER_ID}, {CUSTOMER_ID}, {PRODUCT_ID}, {QUANTITY}, {PRICE}, {ORDER_DATE}, {TEMPERATURE}, {WEATHER_CONDITION})
            SELECT {ORDER_ID}, {CUSTOMER_ID}, {PRODUCT_ID}, {QUANTITY}, {PRICE}, {ORDER_DATE}, {TEMPERATURE}, {WEATHER_CONDITION}
            FROM {STAGING_SALES_TABLE}
            ON CONFLICT ({ORDER_ID}, {PRODUCT_ID}) DO NOTHING;
        """)

    def drop_staging_tables(self):
        try:
            # Drop staging tables 
            logger.info("Dropping staging tables")
            with psycopg2.connect(**self.db_config) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    self._drop_staging_table(cur, STAGING_CUSTOMERS_TABLE)
                    self._drop_staging_table(cur, STAGING_SALES_TABLE)
                    logger.info("Staging tables dropped successfully.")
        except Exception as e:
            logger.error("Failed to drop staging tables", error=str(e))
            raise

    def _drop_staging_table(self, cursor, table_name):
        # Drop staging tables after removing constraints
        cursor.execute(f"ALTER TABLE {STAGING_SALES_TABLE} DROP CONSTRAINT IF EXISTS sales_staging_customer_id_fkey")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    def _create_schema(self, cursor):
        # Schema creation for main tables
        self._create_table_customers(cursor)
        self._create_table_sales(cursor)

    def _create_table_customers(self, cursor):
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {CUSTOMERS_TABLE} (
                {CUSTOMER_ID} SERIAL PRIMARY KEY,
                {NAME} VARCHAR(255) NOT NULL,
                {USERNAME} VARCHAR(255) UNIQUE NOT NULL,
                {EMAIL} VARCHAR(255) UNIQUE NOT NULL,
                {LATITUDE} DECIMAL(9,6),
                {LONGITUDE} DECIMAL(9,6)
            );
            CREATE INDEX IF NOT EXISTS idx_customer_id ON {CUSTOMERS_TABLE} ({CUSTOMER_ID});
        """)

    def _create_table_sales(self, cursor):
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SALES_TABLE} (
                {ORDER_ID} INT NOT NULL,
                {CUSTOMER_ID} INT NOT NULL,
                {PRODUCT_ID} INT NOT NULL,
                {QUANTITY} INT NOT NULL,
                {PRICE} DECIMAL(10,2) NOT NULL,
                {ORDER_DATE} DATE NOT NULL,
                {TEMPERATURE} DECIMAL(5,2),
                {WEATHER_CONDITION} VARCHAR(255),
                PRIMARY KEY ({ORDER_ID}, {PRODUCT_ID}),
                FOREIGN KEY ({CUSTOMER_ID}) REFERENCES {CUSTOMERS_TABLE}({CUSTOMER_ID}),
                CONSTRAINT chk_quantity CHECK ({QUANTITY} > 0),
                CONSTRAINT chk_temperature CHECK ({TEMPERATURE} BETWEEN -100 AND 100)
            );
            CREATE INDEX IF NOT EXISTS idx_customer_id ON {SALES_TABLE} ({CUSTOMER_ID});
            CREATE INDEX IF NOT EXISTS idx_order_date ON {SALES_TABLE} ({ORDER_DATE});
        """)

    def _create_metrics_tables(self, cursor):
        # Monthly Sales Metrics Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS monthly_sales_metrics (
                year_month VARCHAR(7) PRIMARY KEY,
                total_sales_volume INT,
                total_sales_revenue DECIMAL(10,2),
                average_sales_volume DECIMAL(10,2),
                order_count INT
            );
        """)
    
        # Seasonal Sales Metrics Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS seasonal_sales_metrics (
                weather_condition VARCHAR(255) PRIMARY KEY,
                total_sales_volume INT,
                total_sales_revenue DECIMAL(10,2),
                average_sales_volume DECIMAL(10,2),
                order_count INT
            );
        """)
    
        # Product Metrics Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS product_metrics (
                product_id INT PRIMARY KEY,
                sales_volume INT,
                total_revenue DECIMAL(10,2),
                average_order_quantity DECIMAL(10,2)
            );
        """)
    
        # Customer Metrics Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS customer_metrics (
                customer_id INT PRIMARY KEY,
                ltv DECIMAL(10,4),
                ltv_segment VARCHAR(255),
                recency INT,
                frequency INT,
                total_revenue DECIMAL(10,2)
            );
        """)       

    def write_to_db(self, dataframe, table_name):
        try:
            # Write data to the database 
            dataframe.write.jdbc(url=self.jdbc_url, table=table_name, mode='append', properties=self.connection_properties)
            logger.info(f"Data successfully written to {table_name}")
        except Exception as e:
            if "duplicate key value violates unique constraint" in str(e):
                logger.info(f"Skipping insertion of duplicate records in {table_name}.")
            else:
                logger.error(f"Failed to write data to {table_name}", error=str(e))
                raise

    def handle_primary_data(self, final_data):
        try:
            self.setup_staging_tables()
            # Prepare and write customer data to staging table
            logger.info("Writing customer data to staging table")
            customer_columns = [CUSTOMER_ID, NAME, USERNAME, EMAIL, LATITUDE, LONGITUDE]
            customers_df = final_data.select(*customer_columns).distinct()
            self.write_to_db(customers_df, STAGING_CUSTOMERS_TABLE)

            # Prepare and write sales data to staging table
            logger.info("Writing sales data to staging table")
            sales_columns = [ORDER_ID, CUSTOMER_ID, PRODUCT_ID, QUANTITY, PRICE, ORDER_DATE, TEMPERATURE, WEATHER_CONDITION]
            sales_df = final_data.select(*sales_columns)
            self.write_to_db(sales_df, STAGING_SALES_TABLE)

            # Merge staging tables to production tables and drop staging tables
            self.merge_staging_to_production()  
            self.drop_staging_tables()  
            logger.info("Data pipeline executed successfully.")
            
        except Exception as e:
            logger.error("Data pipeline execution failed", error=str(e))
            raise
