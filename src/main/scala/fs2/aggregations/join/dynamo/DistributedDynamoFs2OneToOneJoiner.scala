package fs2.aggregations.join.dynamo

import cats.effect.{Async, IO}
import dynosaur.Schema
import fs2.Stream
import fs2.aggregations.join.models.dynamo.{DynamoRecord, DynamoStoreConfig}
import fs2.aggregations.join.{Fs2OneToOneJoiner, JoinedResult}
import fs2.aggregations.join.models.StreamSource
import fs2.kafka.{CommittableOffset, KafkaProducer}
import meteor.{DynamoDbType, KeyDef}
import meteor.api.hi.CompositeTable
import meteor.codec.{Codec, Decoder, Encoder}
import meteor.codec.Encoder.dynamoEncoderForString

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

  override def sinkToStore(
      left: StreamSource[X, CommitMetadata],
      right: StreamSource[Y, CommitMetadata]
  ): Stream[IO, Unit] = {
    val leftSink = sink(left, true)(config.leftCodec)
    val rightSink = sink(right, false)(config.rightCodec)

    leftSink concurrently rightSink
  }

  override def streamFromStore()
      : fs2.Stream[IO, JoinedResult[X, Y, CommittableOffset[IO]]] = {

    implicit val eitherDecoder =
      DynamoRecord.eitherDecoder[X, Y](config.leftCodec, config.rightCodec)

    config.kafkaNotificationTopicConsumer.stream
      .evalMap(item => {
        val pk = item.record.key

        for {
          items <- getDynamoPartition(pk).compile.toList
          joined = joinItems(items)
          result = Stream
            .fromOption[IO](joined.map(x => JoinedResult(x, item.offset)))

          // Commit ourselves if there was no join before we continue
          _ <- if (joined.isEmpty) item.offset.commit else IO.unit
        } yield (result)
      })
      .flatten
  }

  private def getDynamoPartition(pk: String)(implicit
      decoder: Decoder[Either[DynamoRecord[X], DynamoRecord[Y]]]
  ): Stream[IO, Either[DynamoRecord[X], DynamoRecord[Y]]] = {
    table.retrieve[Either[DynamoRecord[X], DynamoRecord[Y]]](pk, true)
  }
  private def joinItems(
      dynamoRecords: List[Either[DynamoRecord[X], DynamoRecord[Y]]]
  ): Option[(X, Y)] = {
    for {
      left <- dynamoRecords.map(x => x.left.toOption).headOption.flatten
      right <- dynamoRecords.map(x => x.right.toOption).headOption.flatten
    } yield (left.content, right.content)
  }

  private def writeToTable[Z](PK: String, SK: String, item: Z)(implicit
      itemCodec: Codec[DynamoRecord[Z]]
  ): IO[Unit] = {
    val record = DynamoRecord[Z](PK, SK, item)

    table.put(record)(itemCodec)
  }

  private def publishNotificationToKafka(PK: String, SK: String): IO[Unit] = {
    val producer = config.kafkaNotificationTopicProducer
    producer.produceOne(config.kafkaNotificationTopic, PK, SK).flatten.void
  }

  private def sink[Z, CommitMetadata](
      stream: StreamSource[Z, CommitMetadata],
      isLeft: Boolean
  )(implicit codec: Codec[Z]): Stream[IO, Unit] = {

    val SK = if (isLeft) "LEFT" else "RIGHT"

    val decoder = DynamoRecord.dynamoRecordDecoder[Z]
    val encoder = DynamoRecord.dynamoRecordEncoder[Z]

    implicit val autoCodec: Codec[DynamoRecord[Z]] =
      Codec.dynamoCodecFromEncoderAndDecoder(encoder, decoder)

    stream.source
      .evalMap((x) =>
        writeToTable(stream.key(x.record), SK, x.record)(autoCodec) as x
      )
      .evalMap(x =>
        publishNotificationToKafka(stream.key(x.record), SK) as x.commitMetadata
      )
      .through(stream.commitProcessed)
  }
}
