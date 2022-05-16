package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregrations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  // counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre")))
  genresCountDF.show()
  moviesDF.selectExpr("count(Major_Genre)")
  moviesDF.select(count("*")).show()
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  // min max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum("US_Gross"))
  moviesDF.selectExpr("sum(US_Gross)")

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating")),
  ).show()

  // grouping
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .count() // select count(*) from moviesDF group by Major_Genre

  countByGenreDF.show()

  val avgRatingByGenre = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")
  avgRatingByGenre.show()

  val aggregrationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  aggregrationsByGenreDF.show()
  // 1
  moviesDF.select(sum(col("US_Gross") + col("Worldwide_Gross")).as("Total Profit")).show()
  // 2
  moviesDF.select(countDistinct(col("Director"))).show()
  // 3
  moviesDF.select(
    avg(col("US_Gross")).as("Mean US Gross"),
    stddev(col("US_Gross")).as("Standard deviation US Gross")
  ).show()
  // 4
  moviesDF.groupBy(col("Director"))
    .agg(
      avg("IMDB_Rating").as("Average_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    )
    .orderBy(col("Total_US_Gross").desc_nulls_last)
    .show()
}
