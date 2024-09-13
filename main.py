import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, input_file_name, when


spark = SparkSession.builder.appName("AB ETL").getOrCreate()
logging.basicConfig(filename="logs/logs.log", level=logging.INFO)
logger = logging.getLogger(__name__)
processed_files = "logs/processed_files.log"

schema = "id INT, name STRING, host_id INT, host_name STRING, neighbourhood_group STRING, " \
         "neighbourhood STRING, latitude DECIMAL(7, 5), longitude DECIMAL(7, 5), " \
         "room_type STRING, price INT, minimum_nights INT, number_of_reviews INT, " \
         "last_review STRING, reviews_per_month DECIMAL(3, 2), calculated_host_listings_count INT, " \
         "availability_365 INT"

# 3 Transform the Data using PySpark
def data_transformation(df):
    df = df.filter("price > 0")
    df = df.withColumn("last_review", to_date(df.last_review))

    min_date = df.selectExpr("min(last_review)").first()[0]
    df = df.fillna({'last_review': str(min_date)})
    # again because fillna doesnt work with date type
    df = df.withColumn("last_review", to_date(df.last_review))

    df = df.fillna({'reviews_per_month': 0})
    df = df.na.drop(subset=['latitude', 'longitude'])

    df = df.withColumn("price_category", when(df.price < 100, "Budget").when(df.price.between(100, 200), "Mid-range").otherwise("Luxury"))
    df = df.withColumn("price_per_view", df.price / df.number_of_reviews)

    logger.info("Data transformed")
    return df

# 4 Perform SQL Queries using PySpark SQL
def sql_queries(df):
    df.createOrReplaceTempView("airbnb_data")
    neighbourhood_group_listings = spark.sql("SELECT neighbourhood_group, COUNT(*) AS listings FROM airbnb_data "
                                             "GROUP BY neighbourhood_group ORDER BY listings DESC")
    neighbourhood_group_listings.show()

    most_expensive = spark.sql("SELECT * FROM airbnb_data ORDER BY price DESC LIMIT 10")
    most_expensive.show()

    avg_price_by_room_type = spark.sql("SELECT neighbourhood_group, room_type, AVG(price) FROM airbnb_data GROUP BY neighbourhood_group, room_type")
    avg_price_by_room_type.show()

# 5 Save the Data using PySpark:
def save_data(df):
    df.write.mode("append").partitionBy("neighbourhood_group").parquet("processed/")
    logger.info("File saved successfully")


def log_processed_file(file):
    with open(processed_files, 'a') as log_file:
        log_file.write(f"{file}\n")

def check_if_processed_file(file):
    with open(processed_files, 'r') as log_file:
        result = log_file.read().splitlines()
    return file in result

def read_stream_data():
    try:
        stream_data = spark.readStream.option("header", "True").schema(schema).csv("raw/").withColumn("file_name", input_file_name())
        logger.info("Files loaded successfully")
        return stream_data

    except Exception as e:
        logger.error(f"Error during file reading: {str(e)}")
        raise


def process_stream_data(df):
    transformed_df = data_transformation(df)
    save_data(transformed_df)
    # sql_queries(transformed_df)
    file_name = df.select(input_file_name()).first()[0]
    log_processed_file(file_name)


raw_stream = read_stream_data()

query = raw_stream.writeStream \
    .foreachBatch(lambda df, epoch_id: process_stream_data(df)) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination(60)
