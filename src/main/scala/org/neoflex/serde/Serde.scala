package org.neoflex.serde

import io.circe
import io.circe.jawn.decode
import io.circe.syntax._
import io.circe.{ Decoder, Encoder }

object Serde {

  def toObj[A: Decoder](json: String): Either[circe.Error, A] = decode[A](json)

  def toJson[A: Encoder](obj: A): String = obj.asJson.toString()
}
