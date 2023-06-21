package org.neoflex.model

case class Client (before: Option[Client.Value], after: Option[Client.Value], operation: String)

object Client {
  case class Value(clientId: Int, name: String, surname: String, patronymic: Option[String], sex: String)
}
