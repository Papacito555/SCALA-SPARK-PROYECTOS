package com.TarantulaStudios.spark.Dataset
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._
import org.apache.spark.SparkContext

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Calendar
/** Rating. */
object RatingsCounter {


  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("data/ml-100k/u.data")
    println(lines.count())
    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => x.split("\t")(2))

    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()
    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sorted

    // Print each result on its own line.
    sortedResults.foreach(println)
  }
}
