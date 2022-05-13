package com.TarantulaStudios.spark.Entretenimiento

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.text.SimpleDateFormat
import java.util.Calendar

object FechasNetflix {
  def sex(sexo: Int): String = {
    if (sexo == null) {
      return "No Binario"
    } else if (sexo == 1) {
      return "Hombre"
    } else if (sexo == 2) {
      return "Mujer"
    } else
      return "No Binario"
  }

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Covid_Cases")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val spotify_df = spark.read
      .option("Header", "True")
      .option("InferSchema", "True")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/Spotify/regional-global-daily-latest.csv")

    val dateFormatter = new SimpleDateFormat("dd/MM/yyyy")
    spotify_df.printSchema()

    val sex = spark.udf.register("sex", (sexo: Int) => {
      if (sexo == null) {
        "No Binario"
      } else if (sexo == 1) {
        "Hombre"
      } else if (sexo == 2) {
        "Mujer"
      } else {
        "No Binario"
      }
    })

    val theweeknd = spotify_df.na.drop("any", Seq("Fecha"))
      .filter($"Artist" === "The Weeknd")
      //.filter ($"Track_Name" === "The Weeknd")
      .withColumn("Sexo", sex($"Sexo"))
      .select($"Sexo",
        $"Track_Name".as("Canción"),
        $"FECHA".as("FECHA TRENDING"),
        lit(dateFormatter.format(Calendar.getInstance().getTime())).as("FECHA_ACTUAL"))

    val top10 = spotify_df.na.drop("any", Seq("Streams"))
      .filter($"Streams" >= 2000000)
      .withColumn("Sexo", sex($"Sexo"))
      .select($"Sexo",
        $"Artist".as("Artista"),
        $"Streams".as("Reproducciones"),
        lit(dateFormatter.format(Calendar.getInstance().getTime())).as("FECHA_ACTUAL"))

    val total_songs = spotify_df.count()
    val total_theweeknd_songs = theweeknd.count()
    val total_Trending = top10.count()

    println("Total de Canciones Analizadas en el Trending Topic: " + total_songs)
    println("Total de canciones de The Weeknd: " + total_theweeknd_songs)
    println("Total de Artistas que rebasan los 2 millones de Reproducciones: " + total_Trending)

    theweeknd.show(10)
    top10.show(10)

    //Almacena la información Requerida en un archivo CSV
    top10.write.format("csv")
      .mode(SaveMode.Overwrite)
      .save("C:/Users/SDS-Usuario/Downloads/spark/data/data/top10.csv")

    theweeknd.write.format("csv")
      .mode(SaveMode.Overwrite)
      .save("C:/Users/SDS-Usuario/Downloads/spark/data/data/theweekndlist")

    spark.stop()
  }

}
