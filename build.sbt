name := "sclera-plugin-oracle"

description := "Add-on that enables Sclera to work with your data stored in Oracle"

homepage := Some(url(s"https://github.com/scleradb/${name.value}"))

scmInfo := Some(
    ScmInfo(
        url(s"https://github.com/scleradb/${name.value}"),
        s"scm:git@github.com:scleradb/${name.value}.git"
    )
)

version := "4.0"

startYear := Some(2012)

scalaVersion := "2.13.1"

licenses := Seq("Apache License version 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
    "com.scleradb" %% "sclera-core" % "4.0" % "provided"
)

scalacOptions ++= Seq(
    "-Werror", "-feature", "-deprecation", "-unchecked"
)

exportJars := true
