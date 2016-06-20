name := "spark-adaptive-exec"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += Resolver.sonatypeRepo("public")

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies += "org.scalanlp" %% "breeze" % "0.12" % "provided"

libraryDependencies += "org.scalanlp" %% "breeze-viz" % "0.12" % "provided"

libraryDependencies += "com.esotericsoftware" % "kryo" % "3.0.2" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.3.0"
