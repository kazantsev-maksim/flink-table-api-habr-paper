package org.neoflex

import io.circe.generic.auto._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector
import org.neoflex.model.{Client, ClientCompany, Payment, Portfolio}
import org.neoflex.source.KafkaConsumerSource

object StateApiJob {

  def main(args: Array[String]): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val clients   = KafkaConsumerSource.configureKafkaDataSource[Client]("clients").map(PortfolioState(_))
    val companies = KafkaConsumerSource.configureKafkaDataSource[ClientCompany]("companies").map(PortfolioState(_))
    val payments  = KafkaConsumerSource.configureKafkaDataSource[Payment]("payments").map(PortfolioState(_))

    clients
      .union(companies)
      .union(payments)
      .keyBy(_.id) // шардиурем данные по полю id (clientId)
      .process(new PortfolioStateProcessor())
      .print("Portfolio")

    env.execute("Running job")
  }
}

case class PortfolioState(
  id: Int,
  client: Option[Client.Value],
  company: Option[ClientCompany.Value],
  payment: Option[Payment.Value]) {

  def complete: Option[Portfolio] = {
    for {
      client  <- client
      company <- company
      payment <- payment
    } yield {
      Portfolio(
        client.clientId,
        client.name,
        client.surname,
        company.companyId,
        company.companyName,
        payment.amount,
        payment.tmMs
      )
    }
  }
}

object PortfolioState {

  def apply(client: Client): PortfolioState = {
    val id = client.before.map(_.clientId).getOrElse(client.after.map(_.clientId).get)
    PortfolioState(id, client.before.orElse(client.after), None, None)
  }

  def apply(company: ClientCompany): PortfolioState = {
    val id = company.before.map(_.clientId).getOrElse(company.after.map(_.clientId).get)
    PortfolioState(id, None, company.before.orElse(company.after), None)
  }

  def apply(payment: Payment): PortfolioState = {
    val id = payment.before.map(_.clientId).getOrElse(payment.after.map(_.clientId).get)
    PortfolioState(id, None, None, payment.before.orElse(payment.after))
  }

}

class PortfolioStateProcessor extends KeyedProcessFunction[Int, PortfolioState, Portfolio] {

  private var state: ValueState[PortfolioState] = _

  override def open(parameters: Configuration): Unit = {
    state = getRuntimeContext.getState(
      new ValueStateDescriptor[PortfolioState]("state", createTypeInformation[PortfolioState])
    )
  }

  override def processElement(
    value: PortfolioState,
    ctx: KeyedProcessFunction[Int, PortfolioState, Portfolio]#Context,
    out: Collector[Portfolio]
  ): Unit = {
    Option(state.value()).fold {
      state.update(value) // если state == null, инициализируем его входным значением
    } { currentState =>
      val updatedState = currentState.copy(
        client = value.client.orElse(currentState.client),
        company = value.company.orElse(currentState.company),
        payment = value.payment.orElse(currentState.payment)
      )
      updatedState.complete.foreach(out.collect) // если данных хватает, то выдаем результат
      state.update(updatedState)
    }
  }
}
