package com.TarantulaStudios.spark.Dataset
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._
import org.apache.spark.SparkContext

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Calendar
object RatingsCounterEx {

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "RatingCounterEx")
    val lines = sc.textFile("data/ml-100k/u.data")
    println(lines.count())
    val ratings = lines.map(x => x.split("\t")(2))
    val countByratings = ratings.countByValue()
    val result = countByratings.toSeq.sorted
    result.foreach(println)


  }
}
