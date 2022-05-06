name := "OTLStatsCommandsPlugin"

version := "1.0.3"

scalaVersion := "2.11.12"

ThisBuild / useCoursier := false

retrieveManaged := true

resolvers += Resolver.jcenterRepo

resolvers += "Sonatype OSS Snapshots" at (sys.env.getOrElse("NEXUS_OTP_URL_HTTPS", "https://repo1.maven.org/maven2/"))

libraryDependencies += "ot.dispatcher" % "dispatcher-sdk_2.11" % "1.2.1"% Compile