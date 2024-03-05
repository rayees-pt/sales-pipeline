
# Sales Data Pipeline

## Introduction
This project implements a comprehensive sales data pipeline for a retail company, designed to process, transform, and analyze sales data for valuable insights into customer behavior and sales performance. The pipeline integrates sales data with external user and weather information, performs data transformations and aggregations, and stores the results in a PostgreSQL database for analysis. This enables in-depth analysis of customer behavior and sales performance.

## Prerequisites
Before setting up the project, ensure you have the following installed:

* Python 3.x
* Java 8
* Docker and Docker Compose (for Docker setup)
* PostgreSQL (for local setup)

## Project Structure
Ensure the following files are present in the project directory before proceeding with the setup:

* `data/sales_data.csv`: The CSV file containing generated sales data.
* `config.ini`: Configuration file for database connections and other settings.
* `constants.py`: Defines API endpoints for fetching user and weather data, database and staging table names. 
* `data_fetching.py`, `data_transformation.py`, `data_validation.py`, `database.py`, `structured_logger.py`: Python modules for various pipeline components.
* `Dockerfile` and `docker-compose.yml`: Docker configuration files for container setup.
* `requirements.txt`: List of Python packages required for the project.
* `jar/postgresql-42.7.2.jar`: Driver to establish JDBC connections for interacting with the PostgreSQL database.

## Setup Instructions
### Local Setup
   1. **Clone the Repository:** Start by cloning this repository to your local machine.
   2. **Install Dependencies:** Navigate to the project directory and install the required Python packages:
```
pip install -r requirements.txt
```
   3. **Database Configuration:** Ensure PostgreSQL is installed and running. Use the `config.ini` file to configure your database connection settings and OpenWeatherMap API key.
   4. **Run the Pipeline:** Execute the main script to start the data processing:
```   
python sales_data_pipeline.py
```
### Docker Setup 
  1. **Docker and Docker Compose:** Ensure Docker and Docker Compose are installed on your machine.
  2. **Build the Docker Image:** From the project directory, build the Docker image using:
```  
docker-compose build
```
  3. **Run the Container:** Start the container with Docker Compose:
```
docker-compose up
```
This command will set up the database and application containers, and execute the pipeline within the application container.

**Debugging**: For debugging purposes, you can enable console output by setting `debug = 1` in the `config.ini` file. This option is helpful for troubleshooting and monitoring the pipeline's execution flow in detail.

## Usage Instructions
After setting up the project, the sales data pipeline will:

* Fetch user data from the JSONPlaceholder API and merge it with the provided sales data.
* Fetch weather information for each sale from the OpenWeatherMap API and include it in the dataset.
* Perform data transformations, validations, and aggregations to prepare the data for analysis.
* Store the transformed and aggregated data in the configured PostgreSQL database.
* Query the database to analyze the data and derive insights into customer behavior and sales performance.

  *1. Calculate total sales amount per customer:*
  ```
  SELECT customer_id, total_revenue AS total_sales_amount
  FROM customer_metrics;
  ```
  *2. Determine the average order quantity per product:*
  ```
  SELECT product_id, average_order_quantity
  FROM product_metrics;
  ```
  *3. Identify the top-selling products based on quantity:*
  ```
  SELECT product_id, sales_volume
  FROM product_metrics
  ORDER BY sales_volume DESC
  LIMIT 5; -- or any number you prefer to list the top N products
  ```
  *4. Analyze sales trends over time (monthly):*
  ```
  SELECT year_month, average_sales_volume, total_sales_volume, total_sales_revenue
  FROM monthly_sales_metrics
  ORDER BY year_month;
  ```
  *5. Seasonal sales analysis:*
  ```
  SELECT weather_condition, total_sales_volume, total_sales_revenue, average_sales_volume
  FROM seasonal_sales_metrics
  ORDER BY total_sales_volume DESC;
  ```

## Troubleshooting
* Ensure all environment variables are correctly set, especially the API keys for external data sources.
* Verify the database connection settings in `config.ini` are correct and the PostgreSQL server is running.
* Check Docker logs for any errors if using Docker setup, especially related to network issues.


