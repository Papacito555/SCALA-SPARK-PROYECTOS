package com.TarantulaStudios.spark.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TotalAmountG {

  def main(args: Array[String]) = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Amount")

    val costumer_orders = sc.textFile("data/customer-orders.csv")
    val costumer_orders_data = costumer_orders.map(costumerOrdersMap)
    val amount_costumer_data = costumer_orders_data.reduceByKey((x, y) => x + y)
    val results = amount_costumer_data.sortByKey()
    results.foreach(println)


  }

  def costumerOrdersMap(row: String): (Int, Float) = {
    val data = row.split(",")
    val costumer_id = data(0).toInt
    val costumer_amount = data(2).toFloat
    (costumer_id, costumer_amount)
  }

}
