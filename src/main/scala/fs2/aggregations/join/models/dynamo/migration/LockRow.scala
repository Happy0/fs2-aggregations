package fs2.aggregations.join.models.dynamo.migration

import meteor.codec.{Decoder, Encoder}
import meteor.syntax.{RichReadAttributeValue, RichWriteAttributeValue}

case class LockRow(PK: String, SK: String, startedAt: Long, finishedAt: Option[Long], TTL: Long)
object LockRow {

  implicit val encoder: Encoder[LockRow] =
    Encoder.instance { record =>
      Map(
        "PK" -> record.PK.asAttributeValue,
        "SK" -> record.SK.asAttributeValue,
        "startedAt" -> record.startedAt.asAttributeValue,
        "finishedAt" -> record.finishedAt.asAttributeValue,
        "TTL" -> record.TTL.asAttributeValue
      ).asAttributeValue
    }

  implicit val decoder: Decoder[LockRow] =
    Decoder.instance { attributeValue =>
      for {
        pk <- attributeValue.getAs[String]("PK")
        sk <- attributeValue.getAs[String]("SK")
        startedAt <- attributeValue.getAs[Long]("startedAt")
        finishedAt <- attributeValue.getOpt[Long]("finishedAt")
        ttl <- attributeValue.getAs[Long]("TTL")
      } yield LockRow(pk, sk, startedAt, finishedAt, ttl)
    }
}
