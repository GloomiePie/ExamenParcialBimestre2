ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "Semana15",
    libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.10",
    libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.3",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3",
    libraryDependencies += "org.scalikejdbc" %% "scalikejdbc" % "4.0.0",
    libraryDependencies += "com.mysql" % "mysql-connector-j" % "8.0.31",
    libraryDependencies += "com.lihaoyi" %% "requests" % "0.8.0"
  )
