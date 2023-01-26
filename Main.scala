package examenProg

import com.github.tototoshi.csv._
import java.io.File

object Main extends App {
  val reader = CSVReader.open(new File("C:\\Users\\SALA A\\Desktop\\data_parcial_2_OO.csv\\data_parcial_2_OO.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  val idTournament = data
    .flatMap(elem => elem.get("tourney_id"))

  print("Se jugaron en total: " + idTournament.distinct.size)
}
