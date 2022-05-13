package com.TarantulaStudios.spark.SparkSQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSQLG {
  case class Person(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("SparkSession")
      .master("local[*]")
      .getOrCreate()
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
    //.as[Person]

    ds.printSchema()

    ds.createOrReplaceTempView("people")

    val young_friends = spark.sql("SELECT name, age FROM people WHERE age <= 18")

    val results = young_friends.collect()
    results.foreach(println)
    spark.stop()
  }
}
