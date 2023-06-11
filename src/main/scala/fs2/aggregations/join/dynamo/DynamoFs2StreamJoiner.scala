package fs2.aggregations.join.dynamo

import cats.effect.{Async, IO}
import fs2.Stream
import dynosaur._
import fs2.aggregations.join.models.dynamo.DynamoRecord
import fs2.aggregations.join.{Fs2OneToOneJoiner, JoinedResult}
import fs2.aggregations.join.models.{JoinRecord, StreamSource}
import fs2.kafka.{CommittableOffset, KafkaProducer}
import meteor.{DynamoDbType, KeyDef}
import meteor.api.hi.CompositeTable
import meteor.codec.{Codec, Encoder}

import meteor.codec.Encoder.dynamoEncoderForString
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.duration.DurationInt


case class DynamoStoreConfig[X, Y](
    client: DynamoDbAsyncClient,
    tableName: String,
    kafkaNotificationTopic: String,
    leftSchema: Schema[X],
    rightSchema: Schema[Y]
)

final class DistributedDynamoFs2OneToOneJoiner[X, Y, CommitMetadata](
    config: DynamoStoreConfig[X, Y]
) extends Fs2OneToOneJoiner[X, Y, CommitMetadata, CommittableOffset[IO]] {

  import DynamoRecord._

  private val table: CompositeTable[IO, String, String] =
    CompositeTable[IO, String, String](
      config.tableName,
      KeyDef[String]("PK", DynamoDbType.S),
      KeyDef[String]("SK", DynamoDbType.S),
      config.client
    )

  private def writeToTable[Z](PK: String, SK: String, item: Z)(implicit
      itemCodec: Encoder[DynamoRecord[Z]]
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
  ): Stream[IO, Unit] = {

    implicit val encoder: Encoder[DynamoRecord[Z]] = ???

    val SK = if (isLeft) "LEFT" else "RIGHT"

    stream.source
      .evalMap((x) => writeToTable(stream.key(x.record), SK, x.record) as x)
      .evalMap(x =>
        publishToKafka(stream.key(x.record), SK) as x.commitMetadata
      )
      .through(stream.commitProcessed)
  }

  override def sinkToStore(
      left: StreamSource[X, CommitMetadata],
      right: StreamSource[Y, CommitMetadata]
  ): Stream[IO, Unit] = {
    val leftSink = sink(left, true)
    val rightSink = sink(right, false)

    leftSink concurrently rightSink
  }

  implicit val F = Async[IO]
  override def streamFromStore()
      : fs2.Stream[IO, JoinedResult[X, Y, CommittableOffset[IO]]] = {

    // Placeholder
    Stream.sleep(1.minute).flatMap(_ => Stream.empty)
  }
}
