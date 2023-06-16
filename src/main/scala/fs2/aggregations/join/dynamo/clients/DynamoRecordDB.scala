package fs2.aggregations.join.dynamo.clients

import cats.effect.IO
import fs2.Stream
import fs2.aggregations.join.models.dynamo.DynamoRecord
import meteor.api.hi.CompositeTable
import meteor.codec.{Codec, Decoder}
final class DynamoRecordDB(table: CompositeTable[IO, String, String]) {
  def writeToTable[Z](
      table: CompositeTable[IO, String, String],
      PK: String,
      SK: String,
      item: Z
  )(implicit
      itemCodec: Codec[DynamoRecord[Z]]
  ): IO[Unit] = {
    val record = DynamoRecord[Z](PK, SK, item)

    table.put(record)(itemCodec)
  }

  def streamDynamoPartition[X, Y](
      table: CompositeTable[IO, String, String],
      pk: String
  )(implicit
      decoder: Decoder[Either[DynamoRecord[X], DynamoRecord[Y]]]
  ): Stream[IO, Either[DynamoRecord[X], DynamoRecord[Y]]] = {
    table.retrieve[Either[DynamoRecord[X], DynamoRecord[Y]]](pk, true)
  }

}
