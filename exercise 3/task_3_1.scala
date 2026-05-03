import org.apache.spark.sql.SparkSession   // Imports SparkSession class - entry point to use Spark SQL
import org.apache.spark.sql.functions._    // Imports Spark SQL built-in functions

object Task31{   // we are defining  scala object here.

	def main(args: Array[String]): Unit = {   // This is where the main method starts. Spark application execution starts from here

		val spark = SparkSession.builder                   
			.appName("Task3.2_Timetable_Enrichment")   // Name of the Spark application
			.getOrCreate()                             // Function to create a new or use an existing Spark Session

		import spark.implicits._   // here, we import implicit conversions

		val timetables = spark.read.parquet("parquet/timetables")   // Reads the timetables parquet file
		println("Timetables schema:")
		timetables.printSchema()                                    // prints the schema of timetables parquet file

		val changes = spark.read.parquet("parquet/changes")
		println("Changes schema:")
		changes.printSchema()

                // Performs a left join of timetables dataset with changes dataset. The 4 variables in the "Seq" are used as join keys
		val enriched = timetables.join(changes, Seq("station", "stop_id", "snapshot_range", "snapshot_time"), "left")

                // writes the enriched DataFrame into a parquest file
		enriched.write
			.mode("overwrite") 
			.partitionBy("snapshot_range", "snapshot_time") 
			.parquet("parquet/enriched")

		println("Enriched parquet file successfully generated.")	

		spark.stop()  // stops the SparkSession and releases the resources
	}
}