package com.TarantulaStudios.spark.Capacitacion

object Capacitacion8 {
  //Data structures
  //tuples
  //Immutable Lists
  val programmingStuff=("PC","HeadPhones","Smarthphone")
  println(programmingStuff)
  programmingStuff.productIterator.foreach(x=>println(x))
  programmingStuff._2

  val stuff= "Pc" -> "Headphones"
  print(stuff)

  val buchOfStuff=("Hello",true,5.6,1)
  println(buchOfStuff)

  //non-zero based

  //List
  // like tuple but more functionality
  //must be of same type
  //zero-based
  val videoGames=List("Zelda","Cod","Forza Horizon")
  println(videoGames)
  println(videoGames(0))
  for(x<-videoGames){
    println(x)
  }
  val backWardVideoGames=videoGames.map((videoGame:String)=>{videoGame.reverse})
  for(videogame <- backWardVideoGames)println(videogame)

  val scores= List(1,2,3,4,5)
  println(scores)
  //reduce
  val totalScore=scores.reduce((x,y)=>x+y)
  println(totalScore)
  //filter
  val withOutThree=scores.filter(p=>p != 3)
  println(withOutThree)
  val withOutFives=scores.filter(_ != 5)
  println(withOutFives)

  //concat lists
  val totalScores= List(10,85,96)
  val resume= scores ++ totalScores ++ scores
  println(resume)

  println(resume.reverse)
  println(resume.sorted)
  println((resume.distinct))
  println(resume.max)
  println(resume.sum)

  //Map
  val boletas= Map("Edgar"->2013041412,"Ewin"->2013012343)
  println(boletas("Edgar"))
  try {
    println(boletas("Ewin"))
  } catch {
    case e=>println(e)
  }
}
