name := "sclera-plugin-oracle"

description := "Add-on that enables Sclera to work with your data stored in Oracle"

version := "4.0-SNAPSHOT"

homepage := Some(url("https://github.com/scleradb/sclera-plugin-oracle"))

organization := "com.scleradb"

organizationName := "Sclera, Inc."

organizationHomepage := Some(url("https://www.scleradb.com"))

startYear := Some(2012)

scalaVersion := "2.13.1"

licenses := Seq("Apache License version 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

libraryDependencies ++= Seq(
    "com.scleradb" %% "sclera-core" % "4.0-SNAPSHOT" % "provided"
)

scalacOptions ++= Seq(
    "-Werror", "-feature", "-deprecation", "-unchecked"
)

exportJars := true
