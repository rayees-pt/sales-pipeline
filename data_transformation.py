from structured_logger import StructuredLogger
from pyspark.sql.functions import (
    sum as _sum, min as _min, max as _max, countDistinct, col, to_date, lit, 
    avg, when, date_format, datediff
)    
from pyspark.sql import SparkSession


# Initialize the structured logger
logger = StructuredLogger(__name__)

class DataTransformer:
    def __init__(self, user_data, weather_fetcher, spark: SparkSession):
        self.user_data = user_data
        self.weather_fetcher = weather_fetcher
        self.spark = spark

    def calculate_customer_metrics(self, final_data):
        try:
            logger.info("Starting customer metrics calculation")
            # Calculate analysis date for LTV calculation
            analysis_date = final_data.select(_max('order_date')).collect()[0][0]
            
            ltv_analysis = final_data.groupBy('customer_id').agg(
                _sum('price').alias('total_revenue'),
                _min('order_date').alias('first_order_date'),
                _max('order_date').alias('last_order_date'),
                countDistinct('order_id').alias('total_orders')
            )
            # Calculate LTV and other metrics
            ltv_analysis = ltv_analysis.withColumn('analysis_date', lit(analysis_date)) \
                                       .withColumn('customer_tenure', datediff(col('analysis_date'), col('first_order_date'))) \
                                       .withColumn('ltv', col('total_revenue') / col('customer_tenure')) \
                                       .withColumn('recency', datediff(col('analysis_date'), col('last_order_date'))) \
                                       .withColumn('frequency', col('total_orders'))

            # Approximate quantiles with Spark SQL
            quantiles = ltv_analysis.approxQuantile("ltv", [0.25, 0.5, 0.75], 0.05)
            # Segment customers based on LTV
            ltv_analysis = ltv_analysis.withColumn(
                'ltv_segment',
                when(col('ltv') <= quantiles[0], 'Low')
                .when((col('ltv') > quantiles[0]) & (col('ltv') <= quantiles[1]), 'Medium')
                .when((col('ltv') > quantiles[1]) & (col('ltv') <= quantiles[2]), 'High')
                .otherwise('Very High')
            )
            logger.info("Customer metrics calculation completed")
            return ltv_analysis.select('customer_id', 'ltv', 'ltv_segment', 'recency', 'frequency', 'total_revenue')
        
        except Exception as e:
            logger.error("Error during customer metrics calculation", error=str(e))
            raise  


    def calculate_product_metrics(self, sales_data):
        try:
            logger.info("Starting product metrics calculation")
            # Aggregate values for each product
            product_agg = sales_data.groupBy('product_id').agg(
                _sum('quantity').alias('sales_volume'),
                _sum('price').alias('total_revenue'),
                avg('quantity').alias('average_order_quantity')
            )
            logger.info("Product metrics calculation completed")
            return product_agg

        except Exception as e:
            logger.error("Error during product metrics calculation", error=str(e))
            raise 


    def calculate_seasonal_sales_metrics(self, final_data):
        try:
            logger.info("Starting seasonal sales metrics calculation")
            # Aggregate sales metrics based on weather condition
            seasonal_metrics = final_data.groupBy('weather_condition').agg(
                _sum('quantity').alias('total_sales_volume'),
                _sum('price').alias('total_sales_revenue'),
                avg('quantity').alias('average_sales_volume'),
                countDistinct('order_id').alias('order_count')
            )
            logger.info("Seasonal sales metrics calculation completed")
            return seasonal_metrics
        
        except Exception as e:
            logger.error("Error during seasonal sales metrics calculation", error=str(e))
            raise    
    
    def calculate_monthly_sales_metrics(self, final_data):
        try:
            logger.info("Starting monthly sales metrics calculation")
            # Extract year and month from the order date
            final_data = final_data.withColumn('year_month', date_format(col('order_date'), 'yyyy-MM'))
            # Aggregate sales metrics for each month
            monthly_sales_metrics = final_data.groupBy('year_month').agg(
                _sum('quantity').alias('total_sales_volume'),
                _sum('price').alias('total_sales_revenue'),
                avg('quantity').alias('average_sales_volume'),
                countDistinct('order_id').alias('order_count')
            )
            logger.info("Monthly sales metrics calculation completed")
            return monthly_sales_metrics
        
        except Exception as e:
            logger.error("Error during monthly sales metrics calculation", error=str(e))
            raise
  
    def transform_data(self, sales_data):
        try:
            logger.info("Starting data transformation")
            # Read sales data and user data
            sales_data = self.spark.read.csv(sales_data, header=True, inferSchema=True)
            sales_data = sales_data.withColumn('order_date', to_date(col('order_date')))            
            # Join sales data with user data
            final_data = sales_data.join(self.user_data, sales_data['customer_id'] == self.user_data['id'], 'left').drop(self.user_data['id'])

            # Collect unique combinations of latitude, longitude, and date for weather data fetching
            unique_locations = final_data.select('latitude', 'longitude', 'order_date').distinct().collect()
            locations = [(row['latitude'], row['longitude'], row['order_date']) for row in unique_locations]

            # Fetch bulk weather data and join with final data
            logger.info("Fetching weather info. Please wait...")
            weather_df = self.weather_fetcher.fetch_bulk_weather_info(locations)

            weather_df = weather_df.withColumnRenamed('latitude', 'weather_latitude') \
                       .withColumnRenamed('longitude', 'weather_longitude') \
                       .withColumnRenamed('date', 'weather_date')
            
            logger.info("Weather info fetched successfully")
            # Perform the join             
            final_data = final_data.join(weather_df,
                             (final_data.latitude == weather_df.weather_latitude) &
                             (final_data.longitude == weather_df.weather_longitude) &
                             (final_data.order_date == weather_df.weather_date),
                             'left')
            
            # Drop redundant columns
            final_data = final_data.drop('weather_latitude', 'weather_longitude', 'weather_date')
            logger.info("Data transformation completed")
            return final_data
        except Exception as e:
            logger.error("Error during data transformation", error=str(e))
            raise
