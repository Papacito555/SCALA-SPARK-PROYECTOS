package com.TarantulaStudios.spark.Funciones

object Metodo_Do {

  // Metodo principal
  def main(args: Array[String]) {
    // Declaring an array
    var a = Array("Honey (ah)", "I'd walk through fire for you", "Just let me adore you", "Like it's the only thing I'll ever do", "Like it's the only thing I'll ever do")
    var str = "bye"
    var i = 0

    // ejecucion en bucle
    do {
      println("El programa esta Cantando: " + a(i));
      i = i + 1;
    }
    while (a(i) != str);
  }


  //Segundo metodo Do While
  var a = 10;

  do {
    println("Valor de 'A': " + a)
    a = a + 5;

  }
  while (a <= 100)

  //Tercer Metodo
  var a1 = 10;

  // do
    println("Valor de 'A1': " + a1);
    a1 = a1 + 1;
  } while (a1 < 20)

  //Cuarto Metodo
  var b = 0;
  // for
  for (b <- 1 until 10) {
    println("Valor de 'B': " + b);
  }

  //Quinto Método
  var c = 0;
  var d = 0;
  // Ciclo for
  for (c <- 1 to 3; d <- 1 to 3) {
    println("Value of 'C': " + c);
    println("Value of 'D': " + d);
  }

  //Sexto Metodo
  var a2 = 0;
  val numList = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

  // for 循环
  var retVal = for {a2 <- numList
                    if a2 != 3; if a2 < 8
                    } yield a2

  // 输出返回值
  for (a2 <- retVal) {
    println("Value of 'A2': " + a2);
  }
  /*

//Septimo metodo
  var X = 0;
  var Y = 0;
  val numList1 = List(1,2,3,4,5);
  val numList2 = List(11,12,13);

  val outer = new Breaks;
  val inner = new Breaks;

  outer.breakable {
    for( X <- numList1){
      println( "Value of a: " + X );
      inner.breakable {
        for( Y <- numList2){
          println( "Value of b: " + Y );
          if( Y == 12 ){
            inner.break;
          }
        }
      } // 内嵌循环中断
    }
  } // 外部循环中断


   */
}
