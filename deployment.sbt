// ============================================================================
// Sonatype Deployment
// ============================================================================

import SonatypeKeys._

sonatypeSettings

publishMavenStyle := true

publishArtifact in Test := false

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomIncludeRepository := { _ => true }

pomExtra := (
  <url>https://github.com/ekalyoncu/vangas-cassandra</url>
  <licenses>
    <license>
      <name>Apache License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:ekalyoncu/vangas-cassandra.git</url>
    <connection>scm:git@github.com:ekalyoncu/vangas-cassandra.git</connection>
  </scm>
    <developers>
      <developer>
        <id>ekalyoncu</id>
        <name>Egemen Kalyoncu</name>
        <url>http://github.com/ekalyoncu</url>
      </developer>
    </developers>
  )