package dc.paris.integration

import org.apache.spark.sql.SparkSession

// import java.io.File
import java.util.Properties



object Main extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Data Integration")
    .master("local[*]")
    .config("fs.s3a.access.key", "RAPFJEDEXlliIdJuG2df") // A renseigner
    .config("fs.s3a.secret.key", "sVIfgM5Sa2d23pNCEKfx2AyQVsUUuwBUyN24za57") // A renseigner
    .config("fs.s3a.endpoint", "http://localhost:9000/")
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "1000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()

  // list files
  val files = List(
    "s3a://spark/yellow_tripdata_2024-10.parquet",
    "s3a://spark/yellow_tripdata_2024-11.parquet",
    "s3a://spark/yellow_tripdata_2024-12.parquet"
  )

  // connexion to postgrsql
  val urlDb = "jdbc:postgresql://localhost:15432/yellow_taxi"
  private val dbProperties = new Properties()
  dbProperties.setProperty("Driver", "org.postgresql.Driver")
  dbProperties.setProperty("user", "postgres")
  dbProperties.setProperty("password", "admin")

  // insert files into DB
  files.foreach { path =>
    println(s"Lecture du fichier : $path")
    val df = spark.read.parquet(path)
    df.show(10);
    df.write
      .mode("overwrite")
      .jdbc(urlDb, "yellow_tripdata", dbProperties)
    println(s"Données insérées du fichier : $path")
  }

  println("Données dans la DB !!!")
}