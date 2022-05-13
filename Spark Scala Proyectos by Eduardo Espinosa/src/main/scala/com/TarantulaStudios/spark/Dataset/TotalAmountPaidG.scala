package com.TarantulaStudios.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType,IntegerType,FloatType,StructType}
import org.apache.log4j._

case class Costumer(userId:Int,itemId:Int,total:Float)

object TotalAmountPaidG {

  def main(args:Array[String])={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession
      .builder()
      .appName("Spark")
      .master("local[*]")
      .getOrCreate()

    val schema= new StructType()
      .add("userId",IntegerType,false)
      .add("itemId",IntegerType,false)
      .add("total",FloatType,false)

    import spark.implicits._

    val costumer_ds=spark
      .read
      .option("header","false")
      .schema(schema)
      .csv("data/customer-orders.csv")
      .as[Costumer]

    costumer_ds.printSchema()
    costumer_ds.show(10)

    val amount_by_costumer=
      costumer_ds
      .groupBy("userId")
      .agg(round(sum($"total").as("total"),2).alias("total"))
      .sort("total")

    amount_by_costumer.show(amount_by_costumer.count().toInt)
    spark.stop()



  }

}
