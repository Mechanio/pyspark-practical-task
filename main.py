# NOT FULLY DONE TASK
# STEP 2
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.functions import when


spark = SparkSession.builder.appName("AB ETL").getOrCreate()

df_pyspark = spark.read.csv("raw/AB_NYC_2019.csv", header=True, inferSchema=True)
# 3 Transform the Data using PySpark
df_pyspark = df_pyspark.filter("price > 0")
df_pyspark = df_pyspark.withColumn("last_review", to_date(df_pyspark.last_review))

min_date = df_pyspark.selectExpr("min(last_review)").first()[0]
df_pyspark = df_pyspark.fillna({'last_review': str(min_date)})
# again because fillna doesnt work with date type
df_pyspark = df_pyspark.withColumn("last_review", to_date(df_pyspark.last_review))

df_pyspark = df_pyspark.fillna({'reviews_per_month': 0})
df_pyspark = df_pyspark.na.drop(subset=['latitude', 'longitude'])

df_pyspark = df_pyspark.withColumn("price_category", when(df_pyspark.price < 100, "Budget").when(df_pyspark.price.between(100, 200), "Mid-range").otherwise("Luxury"))
df_pyspark = df_pyspark.withColumn("price_per_view", df_pyspark.price / df_pyspark.number_of_reviews)

# 4 Perform SQL Queries using PySpark SQL
df_pyspark.createOrReplaceTempView("airbnb_data")
neighbourhood_group_listings = spark.sql("SELECT neighbourhood_group, COUNT(*) AS listings FROM airbnb_data "
                                         "GROUP BY neighbourhood_group ORDER BY listings DESC")
# neighbourhood_group_listings.show()

most_expensive = spark.sql("SELECT * FROM airbnb_data ORDER BY price DESC LIMIT 10")
# most_expensive.show()

avg_price_by_room_type = spark.sql("SELECT neighbourhood_group, room_type, AVG(price) FROM airbnb_data GROUP BY neighbourhood_group, room_type")
# avg_price_by_room_type.show()

# 5 Save the Data using PySpark:
df_pyspark = df_pyspark.repartition("neighbourhood_group")
df_pyspark.write.partitionBy("neighbourhood_group").parquet("processed/result.parquet")

# 6 Data Quality Checks using PySpark
print(f"Number of records - {df_pyspark.count()}")