organization  := "net.vangas"

name := "vangas-cassandra"

version       := "0.7"

scalaVersion  := "2.11.2"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

val akkaV  = "2.3.6"
val scalaTestV = "2.2.1"
val mockitoV = "1.9.5"
val scalaV = "2.11.2"

libraryDependencies ++= {
  Seq(
    "org.scala-lang"            %   "scala-reflect"        % scalaV,
    "com.typesafe.akka"         %%  "akka-actor"           % akkaV,
    "com.typesafe.akka"         %%  "akka-slf4j"           % akkaV,
    "ch.qos.logback"            %   "logback-classic"      % "1.0.13",
    "com.github.nscala-time"    %%  "nscala-time"          % "1.4.0",
    "com.typesafe.akka"         %%  "akka-testkit"         % akkaV % "test",
    "org.scalatest"             %%  "scalatest"            % scalaTestV % "test",
    "org.mockito"               %   "mockito-core"         % mockitoV % "test",
    "org.apache.commons"        %   "commons-exec"         % "1.2"    % "test" exclude ("junit", "junit")
  )
}

net.virtualvoid.sbt.graph.Plugin.graphSettings
