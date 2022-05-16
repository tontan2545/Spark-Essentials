package part3typesdatasets

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object Datasets extends App {
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master","local")
    .getOrCreate()

  val numbersDF = spark.read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()
  import spark.implicits._
  val numbersDS = numbersDF.as[Int]

  // dataset of complex type
  case class Car(
                Name: String,
                Miles_Per_Gallon: Option[Double],
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: String,
                Origin: String
                )
  def readDF(fileName: String) = spark.read
    .option("inferSchema","true")
    .json(s"src/main/resources/data/$fileName")
  val carsDF = readDF("cars.json")
  val carsDS = carsDF.as[Car]

  // DS Collections functions
  numbersDS.filter(_ < 100).show

  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())
  carNamesDS.show

  // 1
  val carCount = carsDS.count()
  // 2
  val countPowerfulCars = carsDS.filter(car => car.Horsepower.getOrElse(0L) < 140).count()
  // 3
  val countAverageHP = carsDS.map(car => car.Horsepower.getOrElse(0L)).reduce(_ + _) / carsDS.count()

  // also use DF functions since DF is a generic of DS
  carsDS.select(avg(col("Horsepower")))

  // Joins
  case class Guitar(id: Long, model: String, make: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer,Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"))
  guitarPlayerBandsDS.show

  guitarPlayersDS
    .joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
    .show()

  // grouping datasets
  val carsGroupedByOrigin = carsDS.groupByKey(_.Origin).count()
  carsGroupedByOrigin.show()

  // joins and groups are WIDE transformations (Expensive!), will involve SHUFFLE operations
}
