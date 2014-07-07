import AssemblyKeys._

name := "magrathea"

organization := "com.blinkbox.books.marvin"

version := scala.util.Try(scala.io.Source.fromFile("VERSION").mkString.trim).getOrElse("0.0.0")

scalaVersion := "2.10.4"

scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8", "-target:jvm-1.7")

javacOptions := Seq("-source", "1.7", "-target", "1.7", "-Xlint:unchecked")

libraryDependencies ++= {
  val akkaV = "2.3.3"
  val sprayV = "1.3.1"
  val json4sV = "3.2.10"
  Seq(
    "io.spray"                  %  "spray-can"         % sprayV,
    "io.spray"                  %  "spray-routing"     % sprayV,
    "io.spray"                  %% "spray-json"        % "1.2.6",
    "io.spray"                  %  "spray-client"      % sprayV,
    "io.spray"                  %  "spray-testkit"     % sprayV    % "test",
    "org.json4s"                %% "json4s-ext"        % json4sV,
    "org.json4s"                %% "json4s-native"     % json4sV, // for swagger :-/
    "com.typesafe.akka"         %% "akka-actor"        % akkaV,
    "com.typesafe.akka"         %% "akka-slf4j"        % akkaV,
    "com.typesafe.akka"         %% "akka-testkit"      % akkaV     % "test",
    "org.scalatest"             %% "scalatest"         % "2.2.0"   % "test",
    "junit"                     %  "junit"             % "4.11"    % "test",
    "com.novocode"              %  "junit-interface"   % "0.10"    % "test",
    "org.mockito"               %  "mockito-all"       % "1.9.5"   % "test",
    "com.gettyimages"           %% "spray-swagger"     % "0.4.3",
    "com.blinkbox.books"        %% "common-spray"      % "0.12.0",
    "com.blinkbox.books"        %% "common-spray-auth" % "0.3.4",
    "com.blinkbox.books"        %% "common-config"     % "0.7.0",
    "com.blinkbox.books"        %% "common-messaging"  % "0.2.1",
    "com.blinkbox.books.hermes" %% "rabbitmq-ha"       % "3.0.2"
  )
}

Revolver.settings.settings

assemblySettings

packageOptions in (Compile, packageBin) += Package.ManifestAttributes(java.util.jar.Attributes.Name.CLASS_PATH -> ".")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { old =>
  {
    case "asm-license.txt" | "overview.html" => MergeStrategy.discard
    case "application.conf" => MergeStrategy.discard
    case x => old(x)
  }
}

artifact in (Compile, assembly) ~= { art => art.copy(`classifier` = Some("assembly")) }

addArtifact(artifact in (Compile, assembly), assembly).settings
