package fs2.aggregations.join.models.dynamo

import meteor._
import meteor.codec.Codec.dynamoCodecFromEncoderAndDecoder
import meteor.codec._
import meteor.syntax._
case class DynamoRecord[Z](PK: String, SK: String, content: Z)
object DynamoRecord {
  def dynamoRecordEncoder[Z](implicit
      contentEncoder: Codec[Z]
  ): Encoder[DynamoRecord[Z]] = {
    Encoder.instance { record =>
      Map(
        "PK" -> record.PK.asAttributeValue,
        "SK" -> record.SK.asAttributeValue,
        "content" -> record.content.asAttributeValue
      ).asAttributeValue
    }
  }

  def dynamoRecordDecoder[Z](implicit
      contentDecoder: Codec[Z]
  ): Decoder[DynamoRecord[Z]] = {
    Decoder.instance { attributeValue =>
      for {
        pk <- attributeValue.getAs[String]("PK")
        sk <- attributeValue.getAs[String]("SK")
        content <- attributeValue.getAs[Z]("content")
      } yield (DynamoRecord(pk, sk, content))
    }
  }

  def eitherDecoder[X, Y](implicit
      leftDecoder: Codec[X],
      rightDecoder: Codec[Y]
  ): Decoder[Either[DynamoRecord[X], DynamoRecord[Y]]] = {
    Decoder.instance { attributeValue =>
      {
        for {
          sk <- attributeValue.getAs[String]("SK")
          isLeft = sk.startsWith("LEFT")
          result <-
            if (isLeft)
              dynamoRecordDecoder(leftDecoder).read(attributeValue).map(Left(_))
            else
              dynamoRecordDecoder(rightDecoder)
                .read(attributeValue)
                .map(Right(_))
        } yield {
          result
        }
      }
    }
  }

}
