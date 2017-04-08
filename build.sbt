name := "epic_realtime"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVersion = "1.6.1"
  Seq(
   "com.github.nscala-time" %% "nscala-time" % "1.8.0",
   "org.scalaz.stream" %% "scalaz-stream" % "0.8.5"
  )
}
