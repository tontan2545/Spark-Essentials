package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Joins extends App {
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master","local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/bands.json")

  // joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristBandsDF = guitaristsDF.join(
    bandsDF,
    joinCondition,
    "inner"
  )
  guitaristBandsDF.show()

  // Outer joins
  // left outer = everything in the inner join + all the rows in the LEFT table with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer").show()

  // right outer = everything in the inner join + all the rows in the RIGHT table with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer").show()

  // outer = everything in the inner join + all the rows in the BOTH table with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer").show()

  // semi-joins = everything in the left DF for which there's a row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_semi").show()

  // anti-semi-joins = everything in the left DF for which there's no row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show()

  // Dealing with duplicate column name
  // option 1 - rename column
  guitaristsDF.join(bandsDF.withColumnRenamed("id","band"), "band").show()

  // option 2 - drop dupe column
  guitaristBandsDF.drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id","bandId")
  guitarsDF.join(bandsModDF, guitarsDF.col("id") === bandsModDF.col("bandId"))

  // using complex types
  guitarsDF.join(guitarsDF.withColumnRenamed("id","guitarId"), expr("array_contains(guitars,guitarId)"))


}
