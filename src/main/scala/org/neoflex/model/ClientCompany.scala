package org.neoflex.model

case class ClientCompany (before: Option[ClientCompany.Value], after: Option[ClientCompany.Value], op: String)

object ClientCompany {

  case class Value(clientId: Int, companyId: Int, companyName: String)
}
