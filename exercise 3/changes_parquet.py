# This code is not commented as this code uses the same structure and functions as the code "trains_parquet.py" which is commented

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, input_file_name, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


spark = SparkSession.builder \
	.appName("TrainTimetableETL") \
	.config("spark.master", "local[3]") \
	.config("spark.driver.memory", "2g") \
	.config("spark.executor.memory", "2g") \
	.config("spark.sql.shuffle.partitions", "100") \
	.getOrCreate()


changes_schema = StructType([
	StructField("_station", StringType()),
	StructField("s", ArrayType(
		StructType([
			StructField("_id", StringType()),
			
			StructField("m", ArrayType(
				StructType([
					StructField("_id", StringType()),
					StructField("_t", StringType()),
					StructField("_c", StringType()),
					StructField("_cat", StringType()),
					StructField("_ts", StringType())
				])
			]),

			StructField("ar", StructType([
				StructField("_ct", StringType()),
				StructField("m", ArrayType(
					StructType([
						StructField("_id", StringType()),
						StructField("_t", StringType()),
						StructField("_c", StringType()),
						StructField("_ts", StringType())
					])
				))
			])),

			StructField("dp", StructType([
				StructField("_ct", StringType()),
				StructField("m", ArrayType(
					StructType([
						StructField("_id", StringType()),
						StructField("_t", StringType()),
						StructField("_c", StringType()),
						StructField("_ts", StringType())
					])
				))
			]))

		])
	))
])


changes_raw = spark.read \
	.format("com.databricks.spark.xml") \
	.option("rowTag", "timetable") \
	.schema(changes_schema)\
	.load("timetable_changes_compressed/*/*/*_change.xml")\
	.withColumn("file_path", input_file_name())


changes_raw = changes_raw \
	.withColumn("snapshot_range", regexp_extract(col("file_path"), r"timetable_changes_compressed/([^/]+)/", 1)) \
	.withColumn("snapshot_time", regexp_extract(col("file_path"), r"/([^/]+)/[^/]+_change\.xml$", 1))                    


changes_s = changes_raw.withColumn("stop", explode(col("s")))
changes_m = changes_s.withColumn("msg", explode(col("stop.m")))


changes_final = changes_m.select(
	col("_station").alias("station"),
	col("stop._id").alias("stop_id"),
	col("msg._id").alias("message_id"),
	col("msg._c").alias("message_code"),
	col("msg._t").alias("message_type"),
	col("msg._cat").alias("message_category"),
	col("msg._ts").alias("message_timestamp"),
	col("snapshot_range"),
	col("snapshot_time"))

changes_final.write \
	.mode("overwrite") \
	.partitionBy("snapshot_range", "snapshot_time") \
	.parquet("parquet/changes")

print("Timetable change ETL pipeline completed successfully.")