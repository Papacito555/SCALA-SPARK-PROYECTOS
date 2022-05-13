package com.TarantulaStudios.spark.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object SuperHeroesG {
  case class SuperHeroNames(marvel_id: Int, name: String)

  case class SuperHero(value: String)


  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("SparkSession")
      .master("local[*]")
      .getOrCreate()

    val SuperHeroSchema = new StructType()
      .add("value", StringType, true)

    val SuperHeroNameSchema = new StructType()
      .add("marvel_id", IntegerType, false)
      .add("name", StringType, false)

    val values = spark
      .read
      .schema(SuperHeroSchema)
      .csv("data/Marvel-graph.txt")
      .as[SuperHero]

    println("Imprimiendo values")
    values.printSchema()
    values.show(10)

    val connections = values.withColumn("id", split($"value", " ")(0))
      .withColumn("connections", size(split($"value", " ")) - 1)
      .groupBy("id")
      .agg(sum("connections").alias("connections"))

    connections.createOrReplaceTempView("connections")

    //val mostPopularHeroe= connections.sort(desc("connections")).first()

    val superHeroNames = spark
      .read
      .option("sep", " ")
      .schema(SuperHeroNameSchema)
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]
    println("Imprimiendo heroes")

    superHeroNames.printSchema()
    superHeroNames.show(10)
    superHeroNames.createOrReplaceTempView("superHeros")

    val mostPopular = spark.sql("SELECT name, c.connections FROM connections c " +
      "INNER JOIN superHeros s ON c.id == s.marvel_id " +
      "ORDER BY connections desc " +
      "LIMIT 1")
    mostPopular.show()
    /*val mostPopularHeroeData= superHeroNames
      .filter($"marvel_id" === mostPopularHeroe(0))
      .select("name")
      .first()
    println(s"${mostPopularHeroeData(0)} is the most popular superhero with ${mostPopularHeroe(1)} co-appearances.")*/
    spark.stop()

  }
}
