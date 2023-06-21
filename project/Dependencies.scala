import sbt._

object Dependencies {
  private object Version {
    val Flink = "1.16.0"
    val Circe = "0.14.3"
  }

  private object Circe {
    val Core    = "io.circe" %% "circe-core"    % Version.Circe
    val Generic = "io.circe" %% "circe-generic" % Version.Circe
    val Parser  = "io.circe" %% "circe-parser"  % Version.Circe

    lazy val All: Seq[ModuleID] = Seq(Core, Generic, Parser)
  }

  private object Flink {
    val Clients        = "org.apache.flink" % "flink-clients"                 % Version.Flink % Provided
    val StateProcessor = "org.apache.flink" % "flink-state-processor-api"     % Version.Flink % Provided
    val ConnectorKafka = "org.apache.flink" % "flink-connector-kafka"         % Version.Flink
    val Scala          = "org.apache.flink" %% "flink-scala"                  % Version.Flink % Provided
    val StreamingScala = "org.apache.flink" %% "flink-streaming-scala"        % Version.Flink % Provided
    val TableCommon    = "org.apache.flink" % "flink-table-common"            % Version.Flink % Provided
    val TableApi       = "org.apache.flink" %% "flink-table-api-scala"        % Version.Flink
    val TableApiBridge = "org.apache.flink" %% "flink-table-api-scala-bridge" % Version.Flink
    val Planner        = "org.apache.flink" %% "flink-table-planner"          % Version.Flink

    lazy val All: Seq[ModuleID] =
      Seq(
        Clients,
        Scala,
        StreamingScala,
        StateProcessor,
        ConnectorKafka,
        TableCommon,
        TableApi,
        TableApiBridge,
        Planner
      )
  }

  private object Logging {
    val Slf4jApi       = "org.slf4j"                % "slf4j-api"        % "1.7.30"
    val Log4jSlf4jImpl = "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.13.0"

    lazy val All: Seq[ModuleID] = Seq(Slf4jApi, Log4jSlf4jImpl)
  }

  private object Utils {
    val Faker = "com.github.javafaker" % "javafaker" % "1.0.2"

    lazy val All: Seq[ModuleID] = Seq(Faker)
  }

  val Dependencies: Seq[ModuleID] = Flink.All ++ Logging.All ++ Circe.All ++ Utils.All
}
