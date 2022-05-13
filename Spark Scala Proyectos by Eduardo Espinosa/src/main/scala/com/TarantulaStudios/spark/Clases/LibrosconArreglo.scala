package com.TarantulaStudios.spark.Clases

case class LibrosconArreglo(_id:String, author:String, description:String, price:Double, publish_date:String, title:String,otherInfo:OtraInfo,stores:Tiendas)
case class OtraInfo(pagesCount:String,language:String,country:String,address:Direccion)
case class Direccion(addressline1:String,city:String,state:String)
case class Tiendas(store:Array[Tienda])
case class Tienda(name:String)


