name := "DE_Top_Item_RDD"

version := "0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test",
  "org.scalastyle" %% "scalastyle" % "1.0.0" 
)
 
dependencyOverrides ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % "2.1.0",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.1"
)

 
resolvers ++= Seq(
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/"
)  
