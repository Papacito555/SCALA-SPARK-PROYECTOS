package com.TarantulaStudios.spark.RDD

import java.awt.event.{ActionEvent, ActionListener}
import javax.swing.JButton

object Conversion_SAM extends App {


  val button = new JButton("Click")
  button.addActionListener(new ActionListener {
    override def actionPerformed(event: ActionEvent) {
      println("Click Me!!!")
    }
  })

  implicit def getActionListener(actionProcessFunc: (ActionEvent) => Unit) = new ActionListener {
    override def actionPerformed(event: ActionEvent) {
      actionProcessFunc(event)
    }
  }

  button.addActionListener((event: ActionEvent) => println("Click Me!!!"))
  /*
  militares <- data.frame("militar_id"= c(1,2,3,4,5,6,7,8,9,10),
    "primer_nombre"= c("Antonio","Jacinta","Manuel","Armando","Jason","Porfirio","Dolores","Luis","Raul","Abel"),
    "Segundo_Nombre"= c("Carlos","Mclovin", "Alberto", "Grenas ", "Joel", "El Pollo","XD ", " Mario", "Rulas "," Weeknd"),
    "AP"=c("Garcia","Espinosa","Mijares","Bazan","Desrouleaux","Espinosa","Montalvo","Jimenez","Hernandez","Makkonen"),
    "AM"= c("Ruiz","Montalvo","Lopez","Martinez"," ", "Bazan","Correa","Bautista","Martinez","Tesfaye"),
    "mil_grado_id"= c(2,4,1,8,6,3,9,4,3,8),
    "calle"= c("Lope de vega","Sierra Ventanas","Avenida Molliere","Gallo Colorado","Bahía de Ballenas","Av. Paseo de la Reforma","Av. Paseo de la Reforma","Hermelegildo Galeana","Calle 10","Rexford Dr"),
    "Numerodomicilio"= c("209","650","222","46","S/N","510","509","181","370","258"),
    "colonia"= c("Polanco V Secc","Lomas de Chapultepec","Polanco II Secc","Veronica Anzures","Juarez","Juarez","San Jeronimo 4 Vientos","Esperanza","Beverly Hills"),
    "CP"=  c("11560","11000","11550","57000","11300","06600","06600","56531","57800","90212"),
    "estado"= c("Ciudad de Mexico","Ciudad de Mexico","Ciudad de Mexico","Estado de Mexico","Ciudad de Mexico","Ciudad de Mexico","Ciudad de Mexico","Ciudad de Mexico","Estado de Mexico","California"),
  )
  militares

 */

}
