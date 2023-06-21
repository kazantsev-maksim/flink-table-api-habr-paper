package org.neoflex.model

import java.time.Instant

case class Payment (before: Option[Payment.Value], after: Option[Payment.Value], operation: String)

object Payment {

  case class Value(clientId: Int, amount: Int, timestamp: Instant)
}
