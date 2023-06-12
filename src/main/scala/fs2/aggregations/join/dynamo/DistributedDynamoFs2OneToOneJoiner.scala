package fs2.aggregations.join.dynamo

import cats.effect.{Async, IO}
import dynosaur.Schema
import fs2.Stream
import fs2.aggregations.join.models.dynamo.{DynamoRecord, DynamoStoreConfig}
import fs2.aggregations.join.{Fs2OneToOneJoiner, JoinedResult}
import fs2.aggregations.join.models.StreamSource
import fs2.kafka.CommittableOffset
import meteor.{DynamoDbType, KeyDef}
import meteor.api.hi.CompositeTable
import meteor.codec.{Codec, Encoder}
import meteor.codec.Encoder.dynamoEncoderForString

import meteor.dynosaur.formats.conversions._
import meteor.codec.Codec

import scala.concurrent.duration.DurationInt

final case class DistributedDynamoFs2OneToOneJoiner[X, Y, CommitMetadata](
    config: DynamoStoreConfig[X, Y]
) extends Fs2OneToOneJoiner[X, Y, CommitMetadata, CommittableOffset[IO]] {

  private val table: CompositeTable[IO, String, String] =
    CompositeTable[IO, String, String](
      config.tableName,
      KeyDef[String]("PK", DynamoDbType.S),
      KeyDef[String]("SK", DynamoDbType.S),
      config.client
    )

  private def writeToTable[Z](PK: String, SK: String, item: Z)(implicit
      itemCodec: Codec[DynamoRecord[Z]]
  ): IO[Unit] = {
    val record = DynamoRecord[Z](PK, SK, item)

    table.put(record)(itemCodec)
  }
  private def publishToKafka(PK: String, SK: String): IO[Unit] = {
    IO.println("I would be publishing to kafka here")
  }

  private def sink[Z, CommitMetadata](
      stream: StreamSource[Z, CommitMetadata],
      isLeft: Boolean
  )(implicit codec: Codec[Z]): Stream[IO, Unit] = {

    val SK = if (isLeft) "LEFT" else "RIGHT"

    val decoder = DynamoRecord.dynamoRecordDecoder[Z]
    val encoder = DynamoRecord.dynamoRecordEncoder[Z]

    implicit val autoCodec: Codec[DynamoRecord[Z]] = Codec.dynamoCodecFromEncoderAndDecoder(encoder, decoder)

    stream.source
      .evalMap((x) => writeToTable(stream.key(x.record), SK, x.record)(autoCodec) as x)
      .evalMap(x =>
        publishToKafka(stream.key(x.record), SK) as x.commitMetadata
      )
      .through(stream.commitProcessed)
  }

  override def sinkToStore(
      left: StreamSource[X, CommitMetadata],
      right: StreamSource[Y, CommitMetadata]
  ): Stream[IO, Unit] = {
    val leftSink = sink(left, true)(config.leftCodec)
    val rightSink = sink(right, false)(config.rightCodec)

    leftSink concurrently rightSink
  }

  implicit val F = Async[IO]
  override def streamFromStore()
      : fs2.Stream[IO, JoinedResult[X, Y, CommittableOffset[IO]]] = {

    // Placeholder
    Stream.sleep(1.minute).flatMap(_ => Stream.empty)
  }
}
