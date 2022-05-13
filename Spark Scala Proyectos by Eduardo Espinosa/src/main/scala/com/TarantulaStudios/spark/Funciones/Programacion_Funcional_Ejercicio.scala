package com.TarantulaStudios.spark.Funciones

object Programacion_Funcional_Ejercicio extends App {


  def sayHello(name: String) {
    println("Hello, " + name)
  }
  /*val sayHelloFunc = sayHello _
  sayHelloFunc("leo")
val m1 = Map("a"->1,"b"->2,"c"->3)
  m1("a")
   */

  val sayHelloFunc = (name: String) => println("Hello, " + name)

  def greeting(func: (String) => Unit, name: String) {
    func(name)
  }

  greeting(sayHelloFunc, "leo")

  Array(1, 2, 3, 4, 5).map((num: Int) => num * num)


  def greeting12(func: (String) => Unit, name: String) {
    func(name)
  }

  greeting((name: String) => println("Hello, " + name), "leo")
  greeting((name) => println("Hello, " + name), "leo")
  greeting(name => println("Hello, " + name), "leo")

  def triple(func: (Int) => Int) = {
    func(3)
  }

  triple(3 * _)
  println("Mi novia es bien toxica pero de buenos sentimientos")
  println("Mala Fama, Buena Vida")


}
