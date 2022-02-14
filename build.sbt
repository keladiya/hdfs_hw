ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "hdfs_hw",
    mainClass := Some("hdfs_hw.Main")
  )

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "3.2.1"
)