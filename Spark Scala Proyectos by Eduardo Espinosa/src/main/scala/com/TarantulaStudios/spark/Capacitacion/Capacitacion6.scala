package com.TarantulaStudios.spark.Capacitacion

object Capacitacion6 {
  //if else
  if(3>4)
    print("Noup")
  else if (0>1)
    print("yeah")
  else
    print("Nop")

  val number:Int = 3
  number match {
    case 1 => print(number)
    case 3 => print(number)
    case default => print("Default")
  }

  //for
  for(w<-1 to 10)println(w)
  for(a<-1 until 10)println(a)

  var x=0
  while(x<=10){
    println(x)
    x+=1
  }
  x=10

  do{
    print(x)
    x-=1
  }while(x>=0)

  print({val p=23;p*2})
}
