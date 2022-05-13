package com.TarantulaStudios.spark.Capacitacion

object Capacitacion5 {
  //no se pueden cambiar val
  val hello:String="Hola";
  println(hello)
  //son variables
  var helloThere:String= hello

  //DATA TYPES
  val numberOne:Int=1
  val truth:Boolean=true
  val charA:Char='a'
  val pi:Double=3.1416
  val singlePrecision:Float=3.14f
  val bigNomber:Long=1423515
  val smallNumber:Byte=127
  //concat
  println("number one: "+numberOne+pi)
  println(f"Pi is $pi")
  println(s"It's used to many variables $pi $charA $truth")
  println(s"Hom much is 2 +1 ${pi+2}")

  val age:String="My age is 24"
  val patter  = """.* ([\d+]).*""".r
  val patter(answer)=age
  val anw=answer.toInt
  println(anw)

  //boolean
  val isGreater= 1>2
  val isLesser = 1<2
  val nombre= "Edgar"
  val nombre1="Armando"
  val isEqualTo= nombre==nombre1

}
