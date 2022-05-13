package com.TarantulaStudios.spark.Capacitacion

object Capacitacion7 {
  //Functions
  def squareIt(x:Double):Double={
    x*x
  }
  def cubeIt(x:Double):Double={
    x*x*x
  }
  println(squareIt(4.3))
  println(cubeIt(32.4))

  def transformInt(x:Double,f:Double => Double):Double={
    f(x)
  }
  println(transformInt(4.5,cubeIt))

  //lambda functions
  print(transformInt(25.5,x=>x*x))

  println(transformInt(8.0,x=>{
    val y= x*2;
    y*y
  }))

  def stringToUpperCase(str:String):String={
    str.toUpperCase()
  }
  println(stringToUpperCase(str="Hello World"))
  val strUpperC=(str:String)=>str.toUpperCase
  println(strUpperC("Hello World"))
}
