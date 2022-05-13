package com.TarantulaStudios.spark.SparkSQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkDatasetsg {
  case class Person(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Dataset")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data_set = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    data_set.printSchema()
    data_set.show(10)

    data_set.select("name").show(10)
    data_set.filter(data_set("age") < 21).show(10)
    data_set.groupBy(data_set("age")).count().show(10)
    data_set.select(data_set("name"), (data_set("age") + 10).as("age")).where(data_set("age") < 22).show(10)

    spark.stop()
  }

}
