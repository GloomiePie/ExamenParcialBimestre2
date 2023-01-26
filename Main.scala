import com.github.tototoshi.csv._
import play.api.libs.json.Json

import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._

import java.io.File

object Main extends App {
  val reader = CSVReader.open(new File("C:\\Users\\SALA A\\Desktop\\data_parcial_2_OO.csv"))
  val data = reader.allWithHeaders()
  reader.close()
  //Pregunta 1.1
  val idTournament = data
    .flatMap(elem => elem.get("tourney_id"))

  print("Se jugaron en total: " + idTournament.distinct.size)

  //Pregunta 1.2
  val torneos = data.flatMap(x => x.get("tourney_name")).distinct
  val id_torneos = data.flatMap(x => x.get("tourney_id")).distinct
  val numTorneos = torneos.size

  println(numTorneos + "\n")
  torneos.foreach(x => printf("%s\n", x))
  id_torneos.foreach(x => printf("%s\n", x))

  //Pregunta 2
  //Hand
  val playersInfo = data.flatMap(x => x.get("players_info")).map(Json.parse)
  val hand = playersInfo.flatMap(_ \\ "hand")

  val handNoNulls = hand.map(_.as[String]).filter(x => x != "null")
  val maxHand = handNoNulls.groupBy(x => x).toList.maxBy(_._2.size)._2.size
  val minHand = handNoNulls.groupBy(x => x).toList.minBy(_._2.size)._2.size

  println(maxHand)
  println(minHand)

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

  println(datosHeight.max)
  println(datosHeight.filter(x => x != 0).min)
  println("El promedio de la altura: " + datosHeight.map(x => x).sum / datosHeight.size)
  println("El promedio de la altura sin nulls: " + datosHeight.map(x => x).sum / datosHeight
    .filter(x => x != 0).size)

  val height: Seq[Double] = Seq(datosHeight.max, datosHeight.filter(x => x != 0).min,
    datosHeight.map(x => x).sum / datosHeight.size, datosHeight.map(x => x).sum / datosHeight
      .filter(x => x != 0).size)

  val height2 = List("Máximo", "Mínimo", "Promedio sin 0", "Promedio con 0")

  BarChart(height)
    .title("Datos descriptivos")
    .xAxis(height2)
    .yAxis()
    .frame()
    .yLabel("Hands")
    .bottomLegend()
    .render()
    .write(new File("C:\\Users\\SALA A\\Downloads\\Representación1.png"))
//sets - best_of
  println("-------------------------------")
  println("El partido con mas sets: " + sets.max)
  println("El partido con menos sets: " + sets.filter(x => x != 0).min)
  println("El promedio de los sets: " + sets.map(x => x).sum / sets.size)

  val set1: Seq[Double] = Seq(sets.max, sets.filter(x => x != 0).min, sets.map(x => x).sum / sets.size)
  val set2 = List("Máximo", "Mínimo", "Promedio")
  BarChart(set1)
    .title("Datos descriptivos")
    .xAxis(set2)
    .yAxis()
    .frame()
    .yLabel("Sets")
    .bottomLegend()
    .render()
    .write(new File("C:\\Users\\SALA A\\Downloads\\Representación2.png"))

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

  val sets = data
    .flatMap(row => row.get("match_info"))
    .map(row => Json.parse(row))
    .flatMap(jsonData => jsonData \\ "best_of")
    .map(jsValue => jsValue)
    .map(jsValue => jsValue.as[Int])

  val setScoreMins = sets.zip(puntaje).zip(minutosJugados)
  println(setScoreMins.map(x => x).sortBy(_._2).reverse.take(1).map(x => x._1))
}
