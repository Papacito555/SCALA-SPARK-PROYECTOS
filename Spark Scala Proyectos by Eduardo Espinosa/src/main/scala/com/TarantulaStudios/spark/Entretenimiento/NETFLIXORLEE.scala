package com.TarantulaStudios.spark.Entretenimiento

import org.apache.log4j._
import org.apache.spark.sql._

class NETFLIXORLEE {
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("NETFLIXHISTORY")
      .master("local[*]")
      .getOrCreate()

    val netflix_df = spark.read
      .option("header", "True")
      .option("InferSchema", "True")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/Netflix/NetflixViewingHistory.csv")

    netflix_df.printSchema()
    netflix_df.show(10)

    spark.stop()
  }
}
