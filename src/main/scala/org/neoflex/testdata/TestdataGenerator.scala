package org.neoflex.testdata

import com.github.javafaker.Faker
import org.neoflex.Const.Command
import org.neoflex.model.{ Client, ClientCompany, Payment }
import org.neoflex.model.Client.Value

import java.time.Instant
import scala.util.Random
import scala.collection.immutable

sealed trait TestdataGenerator {

  private val faker = new Faker()

  def genNewClients(count: Int): immutable.Seq[Client] = {
    (1 to count).map { id =>
      val value = Client.Value(id, faker.name().firstName(), faker.name().lastName(), None, "Male")
      Client(None, Some(value), Command.Insert)
    }
  }

  def genNewCompanies(count: Int): immutable.Seq[ClientCompany] = {
    (1 to count).map { id =>
      val value = ClientCompany.Value(id, id + id, s"company${id + id}")
      ClientCompany(None, Some(value), Command.Insert)
    }
  }

  def genNewPayments(count: Int): immutable.Seq[Payment] = {
    (1 to count).map { id =>
      val value = Payment.Value(id, Random.nextInt(1000000), Instant.now())
      Payment(Some(value), None, Command.Insert)
    }
  }

  def genUpdatePayments(count: Int): immutable.Seq[Payment] = {
    (1 to count).map { id =>
      val value = Payment.Value(id, Random.nextInt(1000000), Instant.now())
      Payment(None, Some(value), Command.Update)
    }
  }

  def genUpdateCompanies(count: Int): immutable.Seq[ClientCompany] = {
    (1 to count).map { id =>
      val value = ClientCompany.Value(id, id + id + id, s"company${id + id + id}")
      ClientCompany(None, Some(value), Command.Update)
    }
  }

}

object TestdataGenerator extends TestdataGenerator
