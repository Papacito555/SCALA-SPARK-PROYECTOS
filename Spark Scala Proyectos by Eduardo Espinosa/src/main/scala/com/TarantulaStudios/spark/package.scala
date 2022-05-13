package com.sundogsoftware

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.log4j._

package object spark {

  case class SuperHeroNames(marvel_id:Int,name:String)
  case class SuperHero(value:String)

  def main(args:Array[String])= {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("SparkSession")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

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
    val superHeroNames = spark
      .read
      .option("sep", " ")
      .schema(SuperHeroNameSchema)
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]
    println("Imprimiendo heroes")

    superHeroNames.printSchema()
    superHeroNames.show(10)
    spark.stop()

      def suppressLogs(params: List[String]): Unit = {
        import org.apache.log4j.{Level, Logger}
        params.foreach(Logger.getLogger(_).setLevel(Level.OFF))
      }
    }

}
