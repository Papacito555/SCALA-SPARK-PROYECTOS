package com.TarantulaStudios.spark.Capacitacion

class C2 {
  // 4. Declaración del método
  def f1(): Unit ={
    println ("Soy C2.f1")
    var t = f2 (n1 = 3, n2 = 4) // Método de llamada f2
    println(t)

  }

  def f2(n1:Int,n2:Int): Int ={
    // No se puede cambiar el valor de n1, puedes usar otra variable para asignar el valor de n1
    var m=n1;
    m=222;
    return n1+n2
  }


}