package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master","local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
    Reading a DF:
    - format
    - schema or inferSchema = true
     - zero or more options
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    .option("mode","failFast") //dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    )).load()

  /*
    Writing DFs
    - format
    - save mode = overwrite, append, ignore, errorIfExists
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

  // JSON flags
  spark.read
    .format("json")
    .schema(carsSchema)
    .option("dateFormat", "yyyy-MM-dd") // couple with schema; if Spark fails parsing will return null
    .option("allowSingleQuotes","true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .load("src/main/resources/data/cars.json")

  // CSV Flags
  val stocksSchema = StructType(Array(
    StructField("symbol",StringType),
    StructField("date",DateType),
    StructField("price",DoubleType)
  ))

  spark.read
    .format("csv")
    .schema(stocksSchema)
    .options(Map(
      "dateFormat" -> "MMM dd yyyy",
      "header" -> "true",
      "sep" -> ",",
      "nullValue" -> ""
    ))
    .csv("src/main/resources/data/stocks.csv")

  // Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet")

  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val pass = "docker"

  // Reading from a remote DB
  val employeesDF = spark.read
    .format("jdbc")
    .options(Map(
      "driver" -> driver,
      "url" -> url,
      "user" -> user,
      "password" -> pass,
      "dbtable" -> "public.employees"
    ))
    .load()
//
//  employeesDF.show()

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("path", "src/main/resources/data/movies.json")
    .load()

  moviesDF.show()

  // tsv
  moviesDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .options(Map(
      "header"->"true",
      "sep"->"\t"
    ))
    .save("src/main/resources/data/movies.csv")

  // Parquet
  moviesDF.write.mode(SaveMode.Overwrite).save("src/main/resources/data/movies.parquet")

  // save to DB
  moviesDF.write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .options(Map(
    "driver" -> driver,
    "url" -> url,
    "user" -> user,
    "password" -> pass,
    "dbtable" -> "public.movies"
  ))
    .save()
}
