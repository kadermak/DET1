name := "ScalaSparkProject"

version := "0.1"

scalaVersion := "2.12.15"  // or the latest compatible Scala version

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",   // replace with the latest version
  "org.apache.spark" %% "spark-sql" % "3.1.2"     // replace with the latest version
)

// Add any other dependencies you need here
