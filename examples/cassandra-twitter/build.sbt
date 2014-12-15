organization  := "net.vangas"

name := "cassandra-twitter-example"

version       := "1.0"

scalaVersion  := "2.11.2"

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/"
)

val vangasV = "0.7"
val akkaV = "2.3.6"
val sprayV = "1.3.2"
val json4sV = "3.2.11"

libraryDependencies ++= {
  Seq(
    "net.vangas"                %%  "vangas-cassandra"      % vangasV,
    "io.spray"                  %%  "spray-can"             % sprayV,
    "io.spray"                  %%  "spray-httpx"           % sprayV,
    "io.spray"                  %%  "spray-routing"         % sprayV,
    "com.typesafe.akka"         %%  "akka-actor"            % akkaV,
    "com.typesafe.akka"         %%  "akka-slf4j"            % akkaV,
    "org.json4s"                %%  "json4s-jackson"        % json4sV,
    "org.json4s"                %%  "json4s-ext"            % json4sV
  )
}

seq(Revolver.settings: _*)
