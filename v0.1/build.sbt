name := "sparkql"

version := "1.0"

scalaVersion := "2.10.6"

resolvers += "sparkrepository" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.0.0"

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.3"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.6" % "test"

libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.10.6"

libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.10.6"

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
