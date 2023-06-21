package org.neoflex.model

import java.time.Instant

case class Portfolio(
  clientId: Int,
  name: String,
  surname: String,
  companyId: Int,
  companyName: String,
  amount: Int,
  timestamp: Instant)
