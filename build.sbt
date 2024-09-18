ThisBuild / version := "glue-read"

ThisBuild / scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.6.0",
  "org.apache.iceberg" % "iceberg-aws" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "software.amazon.awssdk" % "bundle" % "2.20.0",
  "org.apache.hadoop" % "hadoop-aws" % "3.2.2"
)

lazy val root = (project in file("."))
  .settings(
    name := "spark-iceberg"
  )
