name := "OTLStatsCommandsPlugin"

version := "1.0.3"

scalaVersion := "2.11.12"

ThisBuild / useCoursier := false

retrieveManaged := true

resolvers += Resolver.jcenterRepo

resolvers += ("Sonatype OSS Snapshots" at (sys.env.getOrElse("NEXUS_OTP_URL_HTTPS","http://storage.dev.isgneuro.com")
  + "/repository/ot.platform-sbt-releases/")).withAllowInsecureProtocol(true)

libraryDependencies += "ot.dispatcher" % "dispatcher-sdk_2.11" % "1.2.1"% Compile