package dc.paris.integration

import org.apache.spark.sql.SparkSession

//import java.io.File



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


  // import spark.implicits._

  def showAndUpload(inputPath: String, outputPath: String): Unit = {
    // read file
    val file = spark.read.parquet(inputPath)
    file.show()
    // upload file in minio
    file.write
      .mode("overwrite")
      .parquet(outputPath)
  }

  showAndUpload("../data/raw/yellow_tripdata_2024-10.parquet", "s3a://spark/yellow_tripdata_2024-10.parquet")
  showAndUpload("../data/raw/yellow_tripdata_2024-11.parquet", "s3a://spark/yellow_tripdata_2024-11.parquet")
  showAndUpload("../data/raw/yellow_tripdata_2024-12.parquet", "s3a://spark/yellow_tripdata_2024-12.parquet")
}