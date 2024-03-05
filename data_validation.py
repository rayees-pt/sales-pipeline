from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.types import StringType, FloatType, DoubleType, IntegerType, DateType
from structured_logger import StructuredLogger

# Initialize the structured logger
logger = StructuredLogger(__name__)

class DataValidator:
    @staticmethod
    def validate_data(final_data):
        try:
            # Get total number of records for logging
            total_records = final_data.count()
            logger.info("Starting data validation", total_records=total_records)

            # Define expected data types in PySpark terms
            expected_dtypes = {
                'customer_id': IntegerType,
                'product_id': IntegerType,
                'quantity': IntegerType,
                'name': StringType,
                'username': StringType,
                'email': StringType,
                'latitude': FloatType,
                'longitude': FloatType,
                'price': DoubleType,
                'order_date': DateType,
                'temperature': FloatType,
                'weather_condition': StringType
            }

            # Validate column presence and data types
            for column, expected_type in expected_dtypes.items():
                if column not in final_data.columns:
                    logger.error("Missing mandatory column", missing_column=column)
                    raise ValueError(f"Missing mandatory column: {column}")
                
                actual_type = [f.dataType for f in final_data.schema.fields if f.name == column][0]
                if not isinstance(actual_type, expected_type):
                    logger.error("Column data type mismatch", column=column, expected_type=str(expected_type), actual_type=str(actual_type))
                    raise ValueError(f"Column {column} data type mismatch. Expected: {expected_type}, Found: {actual_type}")

            # Validate email format
            email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            invalid_emails_count = final_data.filter(~regexp_extract(col("email"), email_regex, 0).alias("valid_email").cast("boolean")).count()
            if invalid_emails_count > 0:
                logger.error("Invalid email format detected", invalid_emails_count=invalid_emails_count)
                raise ValueError("Invalid email format detected in 'email' column.")

            # Validate latitude and longitude ranges
            if final_data.filter(~col('latitude').between(-90, 90)).count() > 0:
                logger.error("Latitude values out of range")
                raise ValueError("'latitude' values must be between -90 and 90.")

            if final_data.filter(~col('longitude').between(-180, 180)).count() > 0:
                logger.error("Longitude values out of range")
                raise ValueError("'longitude' values must be between -180 and 180.")

            # Validate non-negative quantities
            if final_data.filter(col('quantity') < 0).count() > 0:
                logger.error("Negative values found in 'quantity' column")
                raise ValueError("'quantity' column contains negative values.")

            logger.info("Data validation passed successfully")
            
        except Exception as e:
            logger.error("Error during data validation", error=str(e))
            raise
