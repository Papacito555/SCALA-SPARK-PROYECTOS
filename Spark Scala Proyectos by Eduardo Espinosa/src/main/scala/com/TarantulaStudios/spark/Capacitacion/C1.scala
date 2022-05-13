package com.TarantulaStudios.spark.Capacitacion

object C1 {
  def main(args: Array[String]): Unit = {
    // 1, variables básicas
    val name = "zhh"
    // name = "zzz" // error val no puede ser modificado
    var name3 = "zhh" // var se puede modificar
    name3 = "zzz"
    val name2: String = "jack"
    // 2. Salida con formato de cadena de declaración variable
    val age = 18
    val address = "desfsff"
    println("name1=" + name, "age1=" + age, "address=" + address)
    println(f"name2=$name%s,age2=$age%s,address=$address%s")
    println(f"name3=$name ,age3=$age,address=$address%s")
    println(s"name4=$name ,age4=$age,address=$address")
    var m = 100
    var n = 3
    println(s"m/n=${m / n}")
    println(s"m/n=${m * 1.0 / n}")

    var i = 3
    var res = if (i > 5) i
    // 3, recorrer
    var a1 = Array(1, 2, 3, 4, 5, 6);
    for (v <- a1) // <- recorre a1 y asigna el valor de cada recorrido a v
    {
      print(v + " ")
    }
    println()
    a1.foreach(v => print(v + "")) // v es equivalente a un parámetro de función, print (v + "") es equivalente a un cuerpo de función (equivalente a una función anónima, y ​​el todo es similar a la función de función (v) {} )
    println()

    for (i <- 1 to 3) {
      print(i + " ")
    }
    println()

    // Salida del contenido de la matriz a1
    for (i <- 0 to a1.length - 1) {
      print(a1(i) + "") // Usa a1 (i) para representar elementos de la matriz
    }
    println()

    for (i <- a1 if i % 2 == 0) {
      print(i + "") // si i% 2 == 0 condición de salida límite
    }
    println()

    // Doble capa para bucle
    for (i <- 1 to 3; j <- 1 to 3 if i != j) {
      print(i * 10 + j + " ")
    } /*output:12,13,21,23,31,32*/
    println()
    // Equivalente al bucle anterior
    for (i <- 1 to 3) {
      for (j <- 1 to 3 if i != j) {
        print(i * 10 + j + " ")
      }
    }
    println()
    // 4, método
    var c2 = new C2(); // Objeto de instancia
    c2.f1();
    // 5. Función
    // Este es el contenido en la función principal
    println("Soy el contenido de la función principal")
    f2();
    m3(3, 4) // La función con parámetros debe llamarse (generalmente no se usa de esta manera)

    // ... (1) Conversión explícita
    val f3_hanshu = f3 _; // método de conversión explícito f3 para funcionar
    cal(f3_hanshu, 20);
    // ..... fin de la conversión explícita

    // .... (2) Conversión implícita
    cal(f3, 20); // 20 se pasa al método f3 como parámetro de f3
    // Llame al método cal. Tenga en cuenta que f1 aquí es un método, que se puede utilizar directamente como un parámetro de función,
    // El sistema predeterminado f1 para funcionar.
    // ..... termina la conversión implícita


    // (3) Parámetros variables
    m2(canshu = 1, 3, 4, 5, 2, 5);


  }

  // Conversión explícita, conversión implícita. . . . . . .
  def f3(m: Int): Int = { // f3 se pasa como un parámetro al método cal, es decir, f3 es el parámetro fun1,
    return m + 10;
  }

  def cal(fun1: Int => Int, m: Int): Unit = { // func: nombre y tipo de parámetro Int, Int, x: tipo de valor de retorno Int y parámetro de valor de retorno
    var t = fun1(m);
    println(t)
  }
  // La conversión explícita termina, la conversión implícita termina. . . . .


  // parámetro variable ..............
  def m2(canshu: Int*): Unit = {
    for (x <- canshu) {
      print(x + " ")
    }
    println()
  }
  // Para decirlo sin rodeos, el parámetro variable es usar una matriz para tomar el valor recibido, func ahora es equivalente a una matriz
  //// Fin de los parámetros variables ..............

  // 5. Función
  // ¡El programa primero ejecutará hashu y hanshu2 (la función val a es equivalente a crear una instancia similar a una variable) y luego ejecutará la función principal! ! ! ! !

  def f2(): Unit = {
    println("Soy el método f2")
  }

  def f1(x: Int, y: Int): Int = {
    return x + y;
  }

  val hanshu = println("Yo soy la función hanshu")
  val hanshu2 = {
    println("Yo soy la función hanshu2222")
  }
  val m3: (Int, Int) => Int = (x, y) => { // Con función de parámetro
    println("dsfdfsfsff")
    x + y
  } // m3 es el nombre de la función, tipo de parámetro de la función (Int, Int), Int es el parámetro del valor de retorno del tipo de valor de retorno (x, y), {x + y} es el valor de retorno

}
