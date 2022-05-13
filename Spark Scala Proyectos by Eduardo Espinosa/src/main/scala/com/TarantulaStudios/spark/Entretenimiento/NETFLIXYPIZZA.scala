package com.TarantulaStudios.spark.Entretenimiento

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

import java.text.SimpleDateFormat
import java.util.Calendar

object NETFLIXYPIZZA {

  def rated(rating: Int): String = {
    if (rating == null) {
      return "sin rating"
    } else if (rating == 1) {
      return "G"
    } else if (rating == 2) {
      return "PG"
    } else if (rating == 3) {
      return "TV-G"
    } else if (rating == 4) {
      return "TV-PG"
    } else if (rating == 5) {
      return "PG-13"
    } else if (rating == 6) {
      return "TV-MA"
    } else if (rating == 7) {
      return "R"
    } else {
      return "Sin Rating"
    }

  }

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("NETFLIXORLEE")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val netflix_df = spark.read
      .option("header", "True")
      .option("inferSchema", "True")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/Netflix/NetflixViewingHistory.csv")
    val dateFormatter = new SimpleDateFormat("dd/MM/yyyy")
    netflix_df.printSchema()


    val rating = spark.udf.register("rated", (rating: Int) => {
      if (rating == null) {
        "sin rating"
      } else if (rating == 1) {
        "G"
      } else if (rating == 2) {
        "PG"
      } else if (rating == 3) {
        "TV-G"
      } else if (rating == 4) {
        "TV-PG"
      } else if (rating == 5) {
        "PG-13"
      } else if (rating == 6) {
        "TV-MA"
      } else if (rating == 7) {
        "R"
      } else {
        "Sin Rating"
      }

    })
    val clasificacionM = netflix_df.na.drop("any", Seq("Rating"))
      .filter((($"Rating" === 6) ||
        ($"Rating" === 7)))

      .select("Title",
        "Date",
        "Rating",
      )
    val dateJanuary2021 = netflix_df.na.drop("any", Seq("Date"))
      .filter($"Date" between("05/09/2021", "11/01/2022"))
      //.where($"Date" > "05/09/2021" && $"Date" < "11/01/2022")
      //.orderBy($"Date".desc).show()
      .withColumn("Rating", rating($"Rating"))
      .select($"Title",
        $"Rating",
        $"Date",
        lit(dateFormatter.format(Calendar.getInstance().getTime())).as("FECHA_ACTUAL"))

    netflix_df.createOrReplaceTempView("CASOS")
    val fechaenero = spark.sql("SELECT Title, Rating, Date FROM CASOS  where Date LIKE '%2022' order by Date desc")
      .withColumn("Rating", rating($"Rating"))

    val Agosto = spark.sql("SELECT Title, Rating, Date FROM CASOS where Date LIKE '%02%08%' order by Title").withColumn("Rating", rating($"Rating"))


    val total_records_analized = netflix_df.count()
    val total_records_Mature_Watched = clasificacionM.count()
    val total_movies_watched_January2022 = fechaenero.count()
    val total_movies_watched_in_my_birthday = Agosto.count()

    println("Total de Peliculas Visualizadas = " + total_records_analized)
    println("Total de Peliculas 18+ Vistas = " + total_records_Mature_Watched)
    println("Total de Peliculas Vistas en Enero de 2022= " + total_movies_watched_January2022)
    println("Total de Peliculas vistas el 2 de Agosto= " + total_movies_watched_in_my_birthday)
    dateJanuary2021.show(10)
    fechaenero.show(10)
    Agosto.show()
    /*
      fechaenero.repartition(1).write.format("csv")
        .mode(SaveMode.Overwrite)
        .option("header","True")
        .save("C:/Users/SDS-Usuario/Downloads/spark/data/data/RangosNetflix") */

    spark.stop()

  }

}
