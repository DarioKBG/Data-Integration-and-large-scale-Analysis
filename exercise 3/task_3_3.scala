import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Task33 {

	def main(args: Array[String]): Unit = {

		val spark = SparkSession.builder
			.appName("Task3.3 - Average Peak Hour Departures")
			.master("local[*]")
			.getOrCreate()

		import spark.implicits._

		val enriched = spark.read.parquet("parquet/enriched")

		val peak_departures = enriched
			.filter(col("departure_time").isNotNull)     // Filters those rows which "departure_time" value is missing
			.withColumn("day", substring(col("departure_time"), 1, 6))     // creates a column, extracts the day from the departure_time and saves it
			.withColumn("hour", substring(col("departure_time"), 7, 2).cast("int"))	// extracts hour from the departure_time and converted the data type to int
			.filter(    // This removes the rows that are not from 07:00 till 08:59 and 17:00 till 18:59 (peak hours) 
 				(col("hour") >= 7 && col("hour") <= 8) || (col("hour") >= 17 && col("hour") <= 18))   // The peak hours (7:00 to 09:00 and 17:00 to 19:00) for which we have to calculate

		val daily_counts = peak_departures
			.groupBy("station", "day")     // the data frame is grouped according to 'station' and 'day'
			.agg(count("*").as("daily_peak_departures"))     // This counts the total number of departures in peak hours 

		val result = daily_counts
			.groupBy("station")
			.agg(avg("daily_peak_departures").as("avg_departures_peak_time"))   // This computes the average number of train departures at peak times

		result.write
			.mode("overwrite")
			.parquet("parquet/task_3_3")

		val task_3_3 = spark.read.parquet("parquet/task_3_3")   // Reads the avg_departures_peak_time parquet file
		println("avg_departures_peak_time schema:")
		task_3_3.printSchema() 


		println("Task 3.3 successfully completed.")	

		spark.stop()  // stops the SparkSession and releases the resources
	}
}