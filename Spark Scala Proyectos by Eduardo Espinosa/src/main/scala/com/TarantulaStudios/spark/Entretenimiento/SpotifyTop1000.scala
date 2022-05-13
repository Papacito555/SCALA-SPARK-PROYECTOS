package com.TarantulaStudios.spark.Entretenimiento

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

import java.text.SimpleDateFormat
import java.util.Calendar

object SpotifyTop1000 {
  /*def top(song:Int):String= {
    if ("Position" == null){
      return "Sin Posicion"
    }else if (song = 0 to 100){
      return "Hot 100"
    }else if (song = 101 to 400){
      return "Hot 400"
    }else if (song = 401 to 1000){
      return "Hot 1000"
    }else{
      return "Sin Posición"
    }
  } */

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Spotify_Top1000")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val hot1000_df = spark.read
      .option("Header", "True")
      .option("InferSchema", "True")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/Spotify/regional-mx-daily-latest.csv")

    val dateFormatter = new SimpleDateFormat("dd/MM/yyyy")
    hot1000_df.printSchema()

    val top100 = hot1000_df.na.drop("any", Seq("Artist"))
      .filter(($"Artist" === "The Weeknd") ||
        ($"Track_Name" == "The Weeknd"))
      .select($"Track_Name",
        $"Artist",
        $"Streams",
        lit(dateFormatter.format(Calendar.getInstance().getTime())).as("FECHA_ACTUAL"))

    val boys = hot1000_df.na.drop("any", Seq("Position"))
      .filter($"Artist" === "Backstreet Boys")
      .select($"Track_Name",
        $"Artist",
        $"Streams",
        lit(dateFormatter.format(Calendar.getInstance().getTime())).as("FECHA_ACTUAL"))


    hot1000_df.createOrReplaceTempView("LISTA")
    val weeknd = spark.sql("SELECT Track_Name, Artist, Streams from LISTA where Artist LIKE 'The Weeknd%' OR Track_Name LIKE '%The Weeknd%' order by Track_name")

    /* weeknd.sort($"Track_Name".asc).repartition(1).write
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/The Weeknd Songs")
*/

    val total_songs = hot1000_df.count()
    val total_theweeknd_songs = top100.count()
    val total_masescuchadas = boys.count()
    val total_weeknd = weeknd.count()

    println("Total de Canciones Analizadas en el Trending Topic: " + total_songs)
    println("Total de canciones de The Weeknd: " + total_theweeknd_songs)

    println("Total de canciones de más escuchadas: " + total_masescuchadas)

    println("Total de Canciones y colaboraciones de The Weeknd: " + total_weeknd)

    top100.show(10)
    boys.show(10)
    weeknd.show()

    spark.stop()
  }


}
