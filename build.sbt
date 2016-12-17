name := "sparkql"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "sparkrepository" at "https://dl.bintray.com/spark-packages/maven/"

//unmanagedJars in Compile += file("src/resource/spark-testing-base_2.10-0.4.7.jar")

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.1"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.0.1"

libraryDependencies += "com.holdenkarau" % "spark-testing-base_2.11" % "2.0.1_0.4.7"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.6" % "test"

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
