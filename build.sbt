val prometheusExport = project
  .in(file("./prometheus-export"))
  .settings(
    organization := "contiamo",
    name := "prom-servlet",
    version := "0.0.1",
    scalaVersion := "2.12.17",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.2" % "provided",
      "org.scalatest" %% "scalatest" % "3.2.9" % Test
    )
  )