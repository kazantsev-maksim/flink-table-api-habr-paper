package org.neoflex

import org.apache.flink.api.common.typeinfo.{ TypeInformation, Types }
import org.apache.flink.types.{ Row, RowKind }
import org.neoflex.Const.Command
import org.neoflex.model.{ Client, ClientCompany, Payment }

sealed trait RowTransformer[A] {
  def toRow(entity: A): Option[Row]
  protected val fieldsName: Array[String]
  implicit def typeInformation: TypeInformation[Row]
}

object RowTransformer {

  def apply[A: RowTransformer]: RowTransformer[A] = implicitly[RowTransformer[A]]

  private def kindOf(operation: String): RowKind = operation match {
    case Command.Insert => RowKind.INSERT
    case Command.Delete => RowKind.DELETE
    case Command.Update => RowKind.UPDATE_AFTER
    case other          => throw new IllegalArgumentException(s"Unsupported operation: $other")
  }

  implicit case object ClientRow extends RowTransformer[Client] {
    override def toRow(entity: Client): Option[Row] = {
      val kind   = kindOf(entity.operation)
      val record = entity.before.orElse(entity.after)
      record.map(r => Row.ofKind(kind, Int.box(r.clientId), r.name, r.surname, r.patronymic.orNull, r.sex))
    }

    override implicit def typeInformation: TypeInformation[Row] = {
      Types.ROW_NAMED(fieldsName, Types.INT, Types.STRING, Types.STRING, Types.STRING, Types.STRING)
    }

    override protected val fieldsName: Array[String] = Array("clientId", "name", "surname", "patronymic", "sex")
  }

  implicit case object PaymentRow extends RowTransformer[Payment] {
    override def toRow(entity: Payment): Option[Row] = {
      val kind   = kindOf(entity.operation)
      val record = entity.before.orElse(entity.after)
      record.map(r => Row.ofKind(kind, Int.box(r.clientId), Int.box(r.amount), r.timestamp))
    }

    override implicit def typeInformation: TypeInformation[Row] = {
      Types.ROW_NAMED(fieldsName, Types.INT, Types.INT, Types.INSTANT)
    }

    override protected val fieldsName: Array[String] = Array("clientId", "amount", "timestamp")
  }

  implicit case object ClientCompanyRow extends RowTransformer[ClientCompany] {
    override def toRow(entity: ClientCompany): Option[Row] = {
      val kind   = kindOf(entity.operation)
      val record = entity.before.orElse(entity.after)
      record.map { r =>
        Row.ofKind(
          kind,
          Int.box(r.clientId),
          Int.box(r.companyId),
          r.companyName
        )
      }
    }

    override implicit def typeInformation: TypeInformation[Row] = {
      Types.ROW_NAMED(fieldsName, Types.INT, Types.INT, Types.STRING)
    }

    override protected val fieldsName: Array[String] = Array("clientId", "companyId", "companyName")
  }
}
