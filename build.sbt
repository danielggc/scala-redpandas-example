val scala3Version = "3.3.0"
crossScalaVersions := Seq("2.13.5", "3.3.0")

lazy val root = project
  .in(file("."))
  .settings(
    name := "service",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "0.7.29" % Test
  )

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core" % "0.14.5",
  "io.circe" %% "circe-generic" % "0.14.5",
  "io.circe" %% "circe-parser" % "0.14.5",
  "org.apache.kafka" % "kafka-clients" % "3.5.1",
  "org.apache.kafka" % "kafka-streams" % "3.5.1",
  ("org.apache.kafka" %% "kafka-streams-scala" % "3.5.1").cross(CrossVersion.for3Use2_13),

)


