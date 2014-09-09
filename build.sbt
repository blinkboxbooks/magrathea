import AssemblyKeys._

name := "magrathea"

organization := "com.blinkbox.books.marvin"

version := scala.util.Try(scala.io.Source.fromFile("VERSION").mkString.trim).getOrElse("0.0.0")

scalaVersion := "2.11.2"

scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8", "-target:jvm-1.7")

libraryDependencies ++= {
  val akkaV = "2.3.6"
  val sprayV = "1.3.1"
  val json4sV = "3.2.10"
  Seq(
    "io.spray"                  %% "spray-testkit"     % sprayV    % Test,
    "org.json4s"                %% "json4s-jackson"    % json4sV,  // for swagger :-/
    "com.typesafe.akka"         %% "akka-slf4j"        % akkaV,
    "com.typesafe.akka"         %% "akka-testkit"      % akkaV     % Test,
    "com.blinkbox.books"        %% "common-scala-test" % "0.3.0"   % Test,
    "com.blinkbox.books"        %% "common-spray"      % "0.16.2",
    "com.blinkbox.books"        %% "common-spray-auth" % "0.6.0",
    "com.blinkbox.books.hermes" %% "rabbitmq-ha"       % "6.0.6"
  )
}

rpmPrepSettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { old =>
  {
    case "asm-license.txt" | "overview.html" => MergeStrategy.discard
    case x => old(x)
  }
}
