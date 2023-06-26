package org.neoflex.testdata

import io.circe.generic.auto._
object TestdataApp {
  def main(args: Array[String]): Unit = {
    val clients   = TestdataGenerator.genNewClients(10)
    val companies = TestdataGenerator.genNewCompanies(10)
    val payments  = TestdataGenerator.genNewPayments(7)

    clients.foreach(TestKafkaProducer.produce("clients", _))
    companies.foreach(TestKafkaProducer.produce("companies", _))
    payments.foreach(TestKafkaProducer.produce("payments", _))
  }
}
