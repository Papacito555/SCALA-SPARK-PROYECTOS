package com.TarantulaStudios.spark.Dataset

  import org.apache.spark.sql.{SaveMode, SparkSession}

  object CreateDataFrame extends App {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //read parquet file
    val df = spark.read.format("parquet")
      .load("C:/Users/SDS-Usuario/Downloads/spark/data/data/resources/zipcodes.parquet")
    df.show()
    df.printSchema()

    //convert to avro
    df.write.format("csv")
      .mode(SaveMode.Overwrite)
      .save("C:/Users/SDS-Usuario/Downloads/spark/data/data/zipcodes.csv")

}
