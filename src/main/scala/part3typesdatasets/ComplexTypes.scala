package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {
  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  // Dates
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  val moviesWithReleaseDates = moviesDF.select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))

  moviesWithReleaseDates
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release"))/365)

  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull).show()

  // parse DF multiple times, then union the small DFs
  val stocksDF = spark.read.format("csv")
    .option("inferSchema","true")
    .option("header","true")
    .load("src/main/resources/data/stocks.csv")

  val stocksDFWithDates = stocksDF
    .withColumn("actual_date",to_date(col("date"), "MMM dd yyyy"))

  stocksDFWithDates.show()

  // with col operators
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
    .show()

  // with expression strings
  moviesDF
    .selectExpr("Title","(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title","Profit.US_Gross")
    .show()

  // arrays
  val moviesWithWords = moviesDF.select(col("Title"),split(col("Title"), " |,").as("Title_Words")) // array of strings
  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  ).show()

}
