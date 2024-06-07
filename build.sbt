name := "pace-finder"

version := "0.1"

scalaVersion := "2.13.14"

libraryDependencies := Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1" % Provided,
  "org.apache.spark" %% "spark-sql" % "3.5.1" % Provided,
  "org.apache.hadoop" % "hadoop-common" % "3.3.4" % Provided,
  "org.scalatest" %% "scalatest" % "3.2.18" % Test,
  "org.scalamock" %% "scalamock" % "6.0.0" % Test,
  "org.mockito" %% "mockito-scala-scalatest" % "1.17.31" % Test,
  "org.mockito" % "mockito-inline" % "5.2.0" % Test
)
