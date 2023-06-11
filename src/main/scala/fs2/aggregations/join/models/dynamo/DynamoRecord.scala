package fs2.aggregations.join.models.dynamo

import meteor._
import meteor.codec.Codec.dynamoCodecFromEncoderAndDecoder
import meteor.codec._
import meteor.syntax._
case class DynamoRecord[Z](PK: String, SK: String, content: Z)
object DynamoRecord {
  def dynamoRecordEncoder[Z](implicit contentEncoder: Codec[Z]): Encoder[DynamoRecord[Z]] = {
    Encoder.instance { record =>
      Map(
        "PK" -> record.PK.asAttributeValue,
        "SK" -> record.SK.asAttributeValue,
        "content" -> record.content.asAttributeValue
      ).asAttributeValue
    }
  }

  def dynamoRecordDecoder[Z](implicit contentDecoder: Codec[Z]): Decoder[DynamoRecord[Z]] = {
    Decoder.instance { attributeValue =>
      for {
        pk <- attributeValue.getAs[String]("PK")
        sk <- attributeValue.getAs[String]("SK")
        content <- attributeValue.getAs[Z]("content")
      } yield (DynamoRecord(pk, sk, content))
    }
  }

}
