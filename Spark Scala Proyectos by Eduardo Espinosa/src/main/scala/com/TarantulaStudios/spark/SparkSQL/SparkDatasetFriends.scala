package com.TarantulaStudios.spark.SparkSQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, round}

object SparkDatasetFriends {
  case class Person(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Dataset")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val ds = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv("data/fakefriends.csv")
      .as[Person]

    ds.printSchema()
    ds.show(10)
    ds.createOrReplaceTempView("people")

    spark.sql("SELECT age,avg(friends) FROM people GROUP BY age ").show(10)
    ds.groupBy("age").min("friends").show(10)
    spark.sql("SELECT age, min(friends) FROM people GROUP BY age ORDER BY age ASC").show(10)
    ds.groupBy("age").agg(round(avg("friends"), 2)).sort("age").show(10)
    spark.stop()

  }

}
