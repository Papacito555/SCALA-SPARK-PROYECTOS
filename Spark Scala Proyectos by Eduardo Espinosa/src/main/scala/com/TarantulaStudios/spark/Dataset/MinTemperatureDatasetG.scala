package com.TarantulaStudios.spark.Dataset

object MinTemperatureDatasetG {

  case class Weather(stationId: String, date: Int, mesure_type: String, temperature: Float)

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("MinTemp")
      .master("local[*]")
      .getOrCreate()


    val schema = new StructType()
      .add("stationId", StringType, false)
      .add("date", IntegerType, false)
      .add("mesure_type", StringType, false)
      .add("temperature", FloatType, false)
    val temperature = spark
      .read
      .schema(schema)
      .csv("data/1800.csv")
      .as[Weather]

    temperature.printSchema()
    temperature.show(10)

    val temperature_dt = temperature.filter($"mesure_type" === "TMIN")
      .select($"stationId", round($"temperature" * 0.1f * (9.0f / 5.0f) + 32.0f, 2).as("temperature"))
      .groupBy($"stationId")
      .min("temperature")
      .withColumnRenamed("min(temperature)", "temperature")
      .sort("temperature")
    temperature_dt.show(10)

    spark.stop()


  }

}
