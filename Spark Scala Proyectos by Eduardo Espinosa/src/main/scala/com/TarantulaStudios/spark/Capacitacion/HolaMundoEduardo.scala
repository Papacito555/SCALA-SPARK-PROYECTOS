package com.TarantulaStudios.spark.Capacitacion

import org.apache.spark.sql.SparkSession
object HolaMundoEduardo {

def main(args:Array[String]) ={

  val spark = SparkSession.builder()
    .appName("Hola Mundo")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  println("█▀▀▄░░░░░░░░░░░▄▀▀█ ")
  println("░█░░░▀▄░▄▄▄▄▄░▄▀░░░█")
  println("░░▀▄░░░▀░░░░░▀░░░▄▀")
  println("░░░░▌░▄▄░░░▄▄░▐▀▀")
  println("░░░▐░░█▄░░░▄█░░▌▄▄▀▀▀▀█")
  println("░░░▌▄▄▀▀░▄░▀▀▄▄▐░░░░░░█")
  println("▄▀▀▐▀▀░▄▄▄▄▄░▀▀▌▄▄▄░░░█")
  println("█░░░▀▄░█░░░█░▄▀░░░░█▀▀▀")
  println("░▀▄░░▀░░▀▀▀░░▀░░░▄█▀")
  println("░░░█░░░░░░░░░░░▄▀▄░▀▄")
  println("░░░█░░░░░░░░░▄▀█░░█░░█")
  println("░░░█░░░░░░░░░░░█▄█░░▄▀")
  println("░░░█░░░░░░░░░░░████▀")
  println("░░░▀▄▄▀▀▄▄▀▀▄▄▄█▀")
}
}