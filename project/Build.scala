import sbt._
import sbt.Keys._
import scala.util.Properties

// sbt-assembly
import sbtassembly.Plugin._
import AssemblyKeys._

object Version {
  def either(environmentVariable: String, default: String): String =
    Properties.envOrElse(environmentVariable, default)

  val geotrellis  = "0.10.0-SNAPSHOT"
  val scala       = "2.10.4"
  val spark       = "1.4.0-SNAPSHOT"
}

object BenchmarkBuild extends Build {
  //val vectorBenchmarkKey = AttributeKey[Boolean]("vectorJavaOptionsPatched")
  val geotiffKey = AttributeKey[Boolean]("gdalJavaOptionsPatched")
  //val benchmarkKey = AttributeKey[Boolean]("javaOptionsPatched")

  val resolutionRepos = Seq(
    "Local Maven Repository"  at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    "NL4J Repository"         at "http://nativelibs4java.sourceforge.net/maven/",
    "maven2 dev repository"   at "http://download.java.net/maven/2",
    "Typesafe Repo"           at "http://repo.typesafe.com/typesafe/releases/",
    "spray repo"              at "http://repo.spray.io/",
    "sonatypeSnapshots"       at "http://oss.sonatype.org/content/repositories/snapshots",
    "sparksnapshots" at "https://repository.apache.org/content/repositories/snapshots/org/apache/spark/"
  )

  // Default settings
  override lazy val settings =
    super.settings ++
  Seq(
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " },
    version := "0.1.0",
    scalaVersion := Version.scala,
    organization := "com.azavea.geotrellis",

    // disable annoying warnings about 2.10.x
    conflictWarning in ThisBuild := ConflictWarning.disable,
    scalacOptions ++=
      Seq("-deprecation",
        "-unchecked",
        "-Yinline-warnings",
        "-language:implicitConversions",
        "-language:reflectiveCalls",
        "-language:higherKinds",
        "-language:postfixOps",
        "-language:existentials",
        "-feature"),

    publishMavenStyle := true,

    publishArtifact in Test := false,

    pomIncludeRepository := { _ => false },
    licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
  )

  val defaultAssemblySettings =
    assemblySettings ++
  Seq(
    test in assembly := {},
    mergeStrategy in assembly <<= (mergeStrategy in assembly) {
      (old) => {
        case "reference.conf" => MergeStrategy.concat
        case "application.conf" => MergeStrategy.concat
        case "META-INF/MANIFEST.MF" => MergeStrategy.discard
        case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
        case _ => MergeStrategy.first
      }
    },
    resolvers ++= resolutionRepos
  )

  // Project: root
  lazy val root =
    Project("benchmarks", file("."))
      .aggregate(geotiff)
      .aggregate(spark)
      .settings(
      initialCommands in console:=
        """
          import geotrellis.raster._
          import geotrellis.vector._
          import geotrellis.proj4._
          """
    )


  lazy val spark: Project =
    Project("spark", file("spark"))
      .settings(sparkSettings:_*)

  lazy val sparkSettings =
    Seq(
      organization := "com.azavea.geotrellis",
      name := "benchmark-spark",

      scalaVersion := Version.scala,

      libraryDependencies ++= Seq(
        "com.azavea.geotrellis" %% "geotrellis-spark" % Version.geotrellis,
        "com.google.code.caliper" % "caliper" % "1.0-SNAPSHOT" from "http://plastic-idolatry.com/jars/caliper-1.0-SNAPSHOT.jar",
        "org.apache.spark" %% "spark-core" % Version.spark % "provided",
        "org.apache.spark" %% "spark-sql" % Version.spark % "provided",
        "com.google.guava" % "guava" % "r09",
        "com.google.code.java-allocation-instrumenter" % "java-allocation-instrumenter" % "2.0",
        "com.google.code.gson" % "gson" % "1.7.1"
      ),

      // enable forking in both run and test
      fork := true

    ) ++ defaultAssemblySettings

  // Project: geotiff-benchmark

  lazy val geotiff: Project =
    Project("geotiff", file("geotiff"))
      .settings(geotiffSettings:_*)

  lazy val geotiffSettings =
    Seq(
      organization := "com.azavea.geotrellis",
      name := "benchmark-geotiff",

      scalaVersion := Version.scala,
      // raise memory limits here if necessary
      javaOptions += "-Xmx2G",
      javaOptions += "-Djava.library.path=/usr/local/lib",

      libraryDependencies ++= Seq(
        "com.google.code.caliper" % "caliper" % "1.0-SNAPSHOT" from "http://plastic-idolatry.com/jars/caliper-1.0-SNAPSHOT.jar",
        "com.google.guava" % "guava" % "r09",
        "com.google.code.java-allocation-instrumenter" % "java-allocation-instrumenter" % "2.0",
        "com.google.code.gson" % "gson" % "1.7.1",
        "com.azavea.geotrellis" %% "geotrellis-gdal" % Version.geotrellis,
        "com.azavea.geotrellis" %% "geotrellis-geotools" % Version.geotrellis
      ),


      // enable forking in both run and test
      fork := true,
      // custom kludge to get caliper to see the right classpath

      // we need to add the runtime classpath as a "-cp" argument to the
      // `javaOptions in run`, otherwise caliper will not see the right classpath
      // and die with a ConfigurationException unfortunately `javaOptions` is a
      // SettingsKey and `fullClasspath in Runtime` is a TaskKey, so we need to
      // jump through these hoops here in order to feed the result of the latter
      // into the former
      onLoad in Global ~= { previous => state =>
        previous {
          state.get(geotiffKey) match {
            case None =>
              // get the runtime classpath, turn into a colon-delimited string
              Project
                .runTask(fullClasspath in Runtime in geotiff, state)
                .get
                ._2
                .toEither match {
                case Right(x) =>
                  val classPath =
                    x.files
                      .mkString(":")
                  // return a state with javaOptionsPatched = true and javaOptions set correctly
                  Project
                    .extract(state)
                    .append(
                    Seq(javaOptions in (geotiff, run) ++= Seq("-Xmx8G", "-cp", classPath)),
                      state.put(geotiffKey, true)
                  )
                case _ => state
              }
            case Some(_) =>
              state // the javaOptions are already patched
          }
        }
      }
    ) ++
  defaultAssemblySettings

}
