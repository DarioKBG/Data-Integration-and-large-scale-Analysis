import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Task32{

	def main(args: Array[String]): Unit = {

		val spark = SparkSession.builder
			.appName("Task3.2 - Average Daily Delay")
			.master("local[*]")    // runs locally with all the cores. All the cores are employed because the process doesn't last long
			.getOrCreate()

		val enriched = spark.read.parquet("parquet/enriched")   // Reads the enriched parquet file
		println("Enriched schema:")
		enriched.printSchema() 

		val station_name = "Berlin Hbf"   // This can be changed into any other valid station name

		val with_delay = enriched
			.filter(col("station") === station_name)     // Filters any rows that does not have the station_name = "Berlin Hbf"
			.filter(col("arrival_time").isNotNull && col("message_timestamp").isNotNull)     // Removes all the rows if the arrival_time or the message_timestamp is missing
			.withColumn("day", substring(col("arrival_time"), 1, 6))                         // Creates a new coloumn and extracts the day from the arrival_time
			.withColumn("delay_minutes", (col("message_timestamp").cast("long") - col("arrival_time").cast("long")) / 60.0)     // Creates a new column and calculates the delay time

		val daily_average = with_delay
			.groupBy("station", "day")    // groups the data frame using station name and day
			.agg(avg("delay_minutes").as("avg_daily_delay"))   // calculates the daily average delay for every single day
 
		val final_result = daily_average
			.groupBy("station")
			.agg(avg("avg_daily_delay").as("average_daily_delay"))   // calculates the overall daily delay 

		final_result.write
			.mode("overwrite")
			.parquet("parquet/task_3_2")

		val task_3_2 = spark.read.parquet("parquet/task_3_2")   // Reads the average_daily_delay parquet file
		println("average_daily_delay schema:")
		task_3_2.printSchema() 


		println("Task 3.2 successfully completed.")	

		spark.stop()  // stops the SparkSession and releases the resources

	}
}