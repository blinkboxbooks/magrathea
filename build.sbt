name := "magrathea"

organization := "com.blinkbox.books.marvin"

version := scala.util.Try(scala.io.Source.fromFile("VERSION").mkString.trim).getOrElse("0.0.0")

scalaVersion := "2.11.4"

scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8", "-target:jvm-1.7")

libraryDependencies ++= {
  val akkaV = "2.3.6"
  val sprayV = "1.3.2"
  Seq(
    "io.spray"                  %% "spray-testkit"     % sprayV    % Test,
    "com.typesafe.akka"         %% "akka-slf4j"        % akkaV,
    "com.typesafe.akka"         %% "akka-testkit"      % akkaV     % Test,
    "com.blinkbox.books"        %% "common-scala-test" % "0.3.0"   % Test,
    "com.blinkbox.books"        %% "common-spray"      % "0.17.3",
    "com.blinkbox.books"        %% "common-spray-auth" % "0.7.2",
    "com.blinkbox.books.hermes" %% "rabbitmq-ha"       % "7.1.1",
    "com.sksamuel.elastic4s"    %% "elastic4s"         % "1.3.2"
  )
}

rpmPrepSettings
