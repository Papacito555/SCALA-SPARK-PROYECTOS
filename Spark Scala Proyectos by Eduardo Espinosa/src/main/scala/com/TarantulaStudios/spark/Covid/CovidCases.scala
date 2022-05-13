package com.TarantulaStudios.spark.Covid

import org.apache.log4j._
import org.apache.spark.sql._


class CovidCases {

  def main(args:Array[String])={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark= SparkSession
      .builder()
      .appName("Covid_Cases")
      .master("local[*]")
      .getOrCreate()

    val covid_df=spark.read
      .option("header","True")
      .option("inferSchema","True")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/covid19/211125COVID19MEXICO.csv")

    covid_df.printSchema()
    covid_df.show(10)


    spark.stop()

  }
}
