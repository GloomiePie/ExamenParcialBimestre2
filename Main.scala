import com.github.tototoshi.csv._
import play.api.libs.json.Json

import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._

import java.io.File

object Main extends App {
  val reader = CSVReader.open(new File("C:\\Users\\USUARIO\\Downloads\\data_parcial_2_OO.csv"))
  val data = reader.allWithHeaders()
  reader.close()
  //Pregunta 1.1
  val idTournament = data
    .flatMap(elem => elem.get("tourney_id"))

  println("Se jugaron en total: " + idTournament.distinct.size +" Torneos")

  //Pregunta 1.2
  val torneos = data.flatMap(x => x.get("tourney_name")).distinct
  val id_torneos = data.flatMap(x => x.get("tourney_id")).distinct

  println("Nombres de los torneos")
  torneos.foreach(x => printf("%s\n", x))
  id_torneos.foreach(x => printf("%s\n", x))

  //Pregunta 2
  //Hand
  val playersInfo = data.flatMap(x => x.get("players_info")).map(Json.parse)
  val hand = playersInfo.flatMap(_ \\ "hand")

  val handNoNulls = hand.map(_.as[String]).filter(x => x != "null")
  val maxHand = handNoNulls.groupBy(x => x).toList.maxBy(_._2.size)._2.size
  val minHand = handNoNulls.groupBy(x => x).toList.minBy(_._2.size)._2.size
  //Falta gráficos y promedio, cambiar eso
  println("Máximo de hands" + maxHand)
  println("Mínimo de hands" +minHand)

  //Height
  val datosHeight = data
    .flatMap(row => row.get("players_info"))
    .map(row => Json.parse(row))
    .flatMap(jsonData => jsonData \\ "height")
    .map(jsValue => jsValue)
    .map(jsValue => jsValue.asOpt[Int] match {
      case Some(i) => i
      case None => 0
    })

  println("Máximo de altura" + datosHeight.max)
  println("Mínimo de altura" + datosHeight.filter(x => x != 0).min)
  println("El promedio de la altura: " + datosHeight.map(x => x).sum / datosHeight.size)
  println("El promedio de la altura sin nulls: " + datosHeight.map(x => x).sum / datosHeight
    .filter(x => x != 0).size)

  val height: Seq[Double] = Seq(datosHeight.max, datosHeight.filter(x => x != 0).min,
    datosHeight.map(x => x).sum / datosHeight.size, datosHeight.map(x => x).sum / datosHeight
      .filter(x => x != 0).size, 0)

  val height2 = List("Máximo", "Mínimo", "Promedio con 0", "Promedio sin 0")

  BarChart(height)
    .title("Datos descriptivos")
    .xAxis(height2)
    .yAxis()
    .frame()
    .yLabel("Hands")
    .bottomLegend()
    .render()
    .write(new File("C:\\Users\\USUARIO\\Downloads\\Height.png"))
  //sets - best_of
  val sets = data
    .flatMap(row => row.get("match_info"))
    .map(row => Json.parse(row))
    .flatMap(jsonData => jsonData \\ "best_of")
    .map(jsValue => jsValue)
    .map(jsValue => jsValue.asOpt[Int] match {
      case Some(i) => i
      case None => 0
    })

  println("El partido con más sets: " + sets.max)
  println("El partido con menos sets: " + sets.filter(x => x != 0).min)
  println("El promedio de los sets: " + sets.map(x => x).sum / sets.size)

  val set1: Seq[Double] = Seq(sets.max, sets.filter(x => x != 0).min, sets.map(x => x).sum / sets.size, 0)
  val set2 = List("Máximo", "Mínimo", "Promedio")
  BarChart(set1)
    .title("Datos descriptivos")
    .xAxis(set2)
    .yAxis()
    .frame()
    .yLabel("Sets")
    .bottomLegend()
    .render()
    .write(new File("C:\\Users\\USUARIO\\Downloads\\Sets.png"))

  //minutes
  val minutes = data
    .flatMap(row => row.get("match_info"))
    .map(row => Json.parse(row))
    .flatMap(jsonData => jsonData \\ "minutes")
    .map(jsValue => jsValue).map(jsValue => jsValue.asOpt[Int] match {
    case Some(i) => i
    case None => 0
  })

  println("El máximo de minutos es: " + minutes.max)
  println("El mínimo de minutos es: " + minutes.filter(x => x != 0).min)
  println("El promedio de minutos es: " + minutes.map(x => x).sum/ minutes.size)
  println("El promedio de minutos, sin contra 0, es: " + minutes.map(x => x).sum
    / minutes.filter(x => x != 0).size)

  val minutes1: Seq[Double] = Seq(minutes.max, minutes.filter(x => x != 0).min,
    minutes.map(x => x).sum/ minutes.size, minutes.map(x => x).sum
      / minutes.filter(x => x != 0).size)

  val minutes2 = List("Máximo", "Mínimo", "Promedio con 0", "Promedio sin 0")
  BarChart(minutes1)
    .title("Datos descriptivos")
    .xAxis(minutes2)
    .yAxis()
    .frame()
    .yLabel("Minutes")
    .bottomLegend()
    .render()
    .write(new File("C:\\Users\\USUARIO\\Downloads\\Minutes.png"))
  
  //Pregunta 3
  println("-------------------------------")
  println("- ¿Cuál fue el puntaje y numeros de sets que se jugaron en el partido que más minutos duro?")
  val puntaje = data
    .flatMap(row => row.get("match_info"))
    .map(row => Json.parse(row))
    .flatMap(jsonData => jsonData \\ "score")
    .map(jsValue => jsValue)

  val minutosJugados = data
    .flatMap(row => row.get("match_info"))
    .map(row => Json.parse(row))
    .flatMap(jsonData => jsonData \\ "minutes")
    .map(jsValue => jsValue)
    .map(jsValue => jsValue.asOpt[Int] match {
      case Some(i) => i
      case None => 0
    })

  val setScoreMins = sets.zip(puntaje).zip(minutosJugados)
  println(setScoreMins.map(x => x).sortBy(_._2).reverse.take(1).map(x => x._1))
}
