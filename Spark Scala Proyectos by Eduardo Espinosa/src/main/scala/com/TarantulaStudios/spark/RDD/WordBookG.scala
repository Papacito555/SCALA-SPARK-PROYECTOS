package com.TarantulaStudios.spark.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}

object WordBookG {

  case class Book(value: String)

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("Spark").master("local[*]").getOrCreate()

    import spark.implicits._
    val book_ds = spark.read.text("data/book.txt").as[Book]

    book_ds.printSchema()
    book_ds.show(10)

    val book_words = book_ds.select(explode(split(book_ds("value"), "\\W+")).as("value"))
      .filter($"value" =!= "")
    book_words.show(10)

    val book_word_sorted = book_words.groupBy("value").count().sort("count")
    book_word_sorted.show(book_word_sorted.count().toInt)

    spark.stop()

  }

}
