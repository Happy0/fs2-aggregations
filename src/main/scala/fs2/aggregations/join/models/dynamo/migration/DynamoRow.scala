package fs2.aggregations.join.models.dynamo.migration

import fs2.aggregations.join.models.dynamo.DynamoRecord
import meteor.codec.{Codec, Decoder}
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import meteor.syntax.{RichReadAttributeValue, RichWriteAttributeValue}
sealed trait DynamoMigrationRow[X,Y]
case class UnrecognisedRow[X,Y]() extends DynamoMigrationRow[X,Y]
case class DynamoJoinRow[X,Y](row: Either[DynamoRecord[X], DynamoRecord[Y]]) extends DynamoMigrationRow[X,Y]
object DynamoMigrationRow {
  def getDecoder[X, Y](leftCodec: Codec[X], rightCodec: Codec[Y]): Decoder[DynamoMigrationRow[X,Y]] = {

    val eitherDecoder = DynamoRecord.eitherDecoder[X, Y](leftCodec, rightCodec)

    Decoder.instance[DynamoMigrationRow[X,Y]] {
      attributeValue: AttributeValue =>
        eitherDecoder.read(attributeValue)
          .map(x => DynamoJoinRow[X,Y](x))
          .orElse(Right(UnrecognisedRow[X,Y]()))
    }


  }

}