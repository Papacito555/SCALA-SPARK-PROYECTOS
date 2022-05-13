package com.TarantulaStudios.spark.SEDENAEJEMPLOSWORK

import org.apache.log4j._
import org.apache.spark.sql._


class SEDENACases {
  def main(args:Array[String])={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark= SparkSession
      .builder()
      .appName("SEDENA")
      .master("local[*]")
      .getOrCreate()

    val SEDENA_df=spark.read
      .option("header","True")
      .option("inferSchema","True")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/SEDENA.csv")

    SEDENA_df.printSchema()
    SEDENA_df.show(10)


    spark.stop()


  }

}
