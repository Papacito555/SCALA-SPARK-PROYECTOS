package com.TarantulaStudios.spark.Dataset

object MostPopularMovie {

  case class Movie(userId: Int, movieId: Int, raiting: Int, timestamp: Long)

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Spark")
      .master("local[*]")
      .getOrCreate()

    val schema = new StructType()
      .add("userId", IntegerType, false)
      .add("movieId", IntegerType, false)
      .add("raiting", IntegerType, false)
      .add("timestamp", LongType, false)

    val movies_ds = spark
      .read
      .option("sep", "\t")
      .option("header", "false")
      .schema(schema)
      .csv("data/ml-100k/u.data")
      .as[Movie]

    movies_ds.printSchema()
    movies_ds.show(10)

    val most_popular_movie = movies_ds
      .filter($"raiting" === 5)
      .groupBy("movieId")
      .agg(count("*").alias("score"))
      .sort(desc("score"))
      .show()
    spark.stop()
  }


}
