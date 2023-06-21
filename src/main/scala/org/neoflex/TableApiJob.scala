package org.neoflex

import io.circe.generic.auto._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{FieldExpression, Schema, Table, _}
import org.apache.flink.table.connector.ChangelogMode
import org.neoflex.RowTransformer._
import org.neoflex.model._
import org.neoflex.source.KafkaConsumerSource

object TableApiJob {
  def main(args: Array[String]): Unit = {
    implicit val env: StreamExecutionEnvironment  = StreamExecutionEnvironment.getExecutionEnvironment
    implicit val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val clients = KafkaConsumerSource.configureKafkaDataSource[Client]("clients")
      .toStreamTable("clientId")
      .renameColumns($"clientId".as("client.clientId"))

    clients.print("Client")

    val companies = KafkaConsumerSource
      .configureKafkaDataSource[ClientCompany]("companies")
      .toStreamTable("clientId")
      .renameColumns($"clientId".as("company.clientId"))

    companies.print("Companies")

    val payments = KafkaConsumerSource
      .configureKafkaDataSource[Payment]("payments")
      .toStreamTable("clientId")
      .renameColumns($"clientId".as("payment.clientId"))

    payments.print("Payments")

    clients
      .join(companies, $"client.clientId" === $"company.clientId")
      .join(payments, $"client.clientId" === $"payment.clientId")
      .select("client.clientId", "name", "surname", "companyId", "companyName", "amount", "timestamp")
      .toChangelogStream(Schema.derived(), ChangelogMode.upsert())
      .print("Portfolio")

    env.execute("Running job...")
  }
  implicit class DataStreamOps[A: RowTransformer](val stream: DataStream[A]) {

    def toStreamTable(primaryKey: String*)(implicit env: StreamTableEnvironment): Table = {
      val transformer = RowTransformer[A]
      stream
        .map(transformer.toRow(_))
        .flatMap(_.toSeq)(transformer.typeInformation)
        .toChangelogTable(
          env,
          Schema
            .newBuilder()
            .primaryKey(primaryKey: _*)
            .build(),
          ChangelogMode.upsert()
        )
    }
  }
}
