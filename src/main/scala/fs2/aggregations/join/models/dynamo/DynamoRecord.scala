package fs2.aggregations.join.models.dynamo

import cats.implicits.catsSyntaxTuple3Semigroupal
import dynosaur.Schema
import meteor.codec.Codec
import dynosaur._
case class DynamoRecord[Z](PK: String, SK: String, content: Z)
object DynamoRecord {
  def dynamoRecordSchema[Z](implicit contentCodec: Schema[Z]): Schema[DynamoRecord[Z]] = {
    Schema.record[DynamoRecord[Z]] {
      field => (
        field("PK", _.PK ),
        field("SK", _.SK),
        field("Content", _.content))
          .mapN(DynamoRecord[Z].apply)
    }

  }

}
