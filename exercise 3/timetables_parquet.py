from pyspark.sql import SparkSession   # To import SparkSession to create and configure a Spark application
from pyspark.sql.functions import col, explode, input_file_name, regexp_extract   # To import Sprak SQL functions which will be used in the code
from pyspark.sql.types import StructType, StructField, StringType, ArrayType   # To import data types needed for parsing xml files


# Creation and configuration of a SparkSession

spark = SparkSession.builder \
	.appName("TrainTimetableETL") \                    # Name of the spark application
	.config("spark.master", "local[3]") \              # Spark is run locally, and 3 cores are used
	.config("spark.driver.memory", "2g") \             # 2gb memory is allocated for the drivers
	.config("spark.executor.memory", "2g") \           # 2gb memory is allocated for the executors
	.config("spark.sql.shuffle.partitions", "100") \   # Numbers of shuffle partitions is defined 
	.getOrCreate()                                     # This function is used to create ot reuse a SparkSession


# Here, we are defining the schema for the timetable XML file
# This schema should be carefully defined, otherwise the code would extract wrong values into wrong files and the whole data would be corrupted

timetable_schema = StructType([
	StructField("_station", StringType()),
	StructField("s", ArrayType(   # Here, ArrayType is used because there are multiple 's' fields found in one xml file
		StructType([
			StructField("_id", StringType()),
			StructField("_tl", StructType([
				StructField("_c", StringType()),
				StructField("_n", StringType())
			])),

			StructField("_ar", StructType([
				StructField("_pt", StringType()),
				StructField("_pp", StringType())
			])),

			StructField("_dp", StructType([
				StructField("_pt", StringType()),
				StructField("_pp", StringType())
			]))
		])
	])
])


# This is to read timetable xml files into a DataFrame 

timetable_raw = spark.read \
	.format("com.databricks.spark.xml") \                # Use Spark xml reader
	.option("rowTag", "timetable") \                     # Each timetable becomes a row
	.schema(timetable_schema)\                           # The predefined Schema is applied here
	.load("timetables_compressed/*/*/*_timetable.xml")\  # Load all the time xml files ending with ..._timetable.xml
	.withColumn("file_path", input_file_name())          # The path of the input files is extracted (.withColumn adds a column to a table)


# This extracts "snapshot_range" and "snapshot_time" values

timetable_raw = timetable_raw \
	.withColumn("snapshot_range", regexp_extract(col("file_path"), r"timetables_compressed/([^/]+)/", 1)) \
	.withColumn("snapshot_time", regexp_extract(col("file_path"), r"/([^/]+)/[^/]+_timetable\.xml$", 1))                    


# There are multiple 's' fields found in an xml file. Therefore, ArrayType is used to save those multiple values
# Here the array is exploded so that each becomes it"s own row

timetable_df = timetable_raw.withColumn("stop", explode(col("s")))


#The required fields are chosen and flattened into a clean tabular structure

timetable_final = timetable_df.select(
	col("_station").alias("station"),   # "_station" value is stored under "station" in the new table - timetable_final
	col("stop._id").alias("stop_id"),
	col("stop._tl._c").alias("train_type"),
	col("stop._tl._n").alias("train_number"),
	col("stop._ar._pt").alias("arrival_time"),
	col("stop._ar._pp").alias("arrival_platform"),
	col("stop._dp._pt").alias("departure_time"),
	col("stop._dp._pp").alias("departure_platform"),
	col("snapshot_range"),
	col("snapshot_time"))


#Writes the final DataFrame to Parquet format

timetable_final.write \
	.mode("overwrite") \                                # Overwrites the existing output
	.partitionBy("snapshot_range", "snapshot_time") \   # partitions the output data by snapshot_range and snapshot_time
	.parquet("parquet/timetables")                      # The resulting parquet file is saved in a directory

print("Timetable ETL pipeline completed successfully.")     # Log message to see whether the execution is successful











