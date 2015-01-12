lazy val root = (project in file(".")).
  settings(
    name := "ingestion-metadata-service",
    organization := "com.blinkbox.books.marvin",
    version := scala.util.Try(scala.io.Source.fromFile("VERSION").mkString.trim).getOrElse("0.0.0"),
    scalaVersion := "2.11.4",
    scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8", "-target:jvm-1.7", "-Xfatal-warnings", "-Xfuture"),
    libraryDependencies ++= {
      val akkaV = "2.3.8"
      val sprayV = "1.3.2"
      Seq(
        "io.spray"                  %% "spray-testkit"     % sprayV    % Test,
        "com.typesafe.akka"         %% "akka-testkit"      % akkaV     % Test,
        "com.blinkbox.books"        %% "common-scala-test" % "0.3.0"   % Test,
        "com.blinkbox.books"        %% "common-spray"      % "0.24.0",
        "com.blinkbox.books"        %% "common-spray-auth" % "0.7.6",
        "com.blinkbox.books"        %% "elastic-http"      % "0.0.11",
        "com.blinkbox.books.hermes" %% "rabbitmq-ha"       % "8.1.1",
        "org.postgresql"            %  "postgresql"        % "9.3-1102-jdbc41",
        "com.github.tminglei"       %% "slick-pg"          % "0.7.0"
      )
    }
  ).
  settings(rpmPrepSettings: _*)
