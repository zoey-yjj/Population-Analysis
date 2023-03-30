val scala3Version = "3.2.2"
val globalVersion = "0.1.0-SNAPSHOT"

val deps = Seq(
  "org.scalactic" %% "scalactic" % "3.2.15",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test",
  "org.scalacheck" %% "scalacheck" % "1.17.0" % "test",
  "io.getquill" %% "quill-jdbc" % "4.6.0",
  "io.getquill" %% "quill-jasync-postgres" % "4.6.0",
  "org.postgresql" % "postgresql" % "42.5.4",
  "org.slf4j" % "slf4j-simple" % "2.0.5"
)

lazy val root = project
  .in(file("."))
  .settings(
    name := "assignment04",
    version := globalVersion,
    scalaVersion := scala3Version,
    libraryDependencies ++= deps
  )
