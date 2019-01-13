import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      name         := "mot-cassandra",
      organization := "com.okmich",
      scalaVersion := "2.11.8",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "mot-cassandra",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided",
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.0",
    libraryDependencies += scalaTest % Test
  )

