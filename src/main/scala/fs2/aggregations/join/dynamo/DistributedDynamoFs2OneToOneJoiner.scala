package fs2.aggregations.join.dynamo

import cats.effect.kernel.Deferred
import cats.effect.{Async, IO}
import dynosaur.Schema
import fs2.Stream
import fs2.aggregations.join.models.dynamo.{DynamoRecord, DynamoStoreConfig}
import fs2.aggregations.join.{Fs2OneToOneJoiner, JoinedResult}
import fs2.aggregations.join.models.StreamSource
import fs2.kafka.{CommittableOffset, KafkaConsumer}
import meteor.{DynamoDbType, KeyDef}
import meteor.api.hi.CompositeTable
import meteor.codec.{Codec, Decoder}
import meteor.codec.Encoder.dynamoEncoderForString

import fs2.aggregations.join.utils.StreamJoinUtils._

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

    concurrentlyUntilBothComplete(leftSink, rightSink)
  }

  override def streamFromStore()
      : fs2.Stream[IO, JoinedResult[X, Y, CommittableOffset[IO]]] = {

    for {
      _ <- Stream.eval(
        config.kafkaNotificationTopicConsumer.subscribeTo(
          config.kafkaNotificationTopic
        )
      )
      stream <- streamUpdates(config.kafkaNotificationTopicConsumer)
    } yield (stream)
  }

  private def streamUpdates(
      kafkaConsumer: KafkaConsumer[IO, String, String]
  ): Stream[IO, JoinedResult[X, Y, CommittableOffset[IO]]] = {
    implicit val eitherDecoder =
      DynamoRecord.eitherDecoder[X, Y](config.leftCodec, config.rightCodec)

    kafkaConsumer.records
      .evalMap(item => {
        val pk = item.record.key

        for {
          items <- streamDynamoPartition(table, pk).compile.toList
          joined = joinItems(items)
          result = Stream
            .fromOption[IO](joined.map(x => JoinedResult(x, item.offset)))

          // Commit ourselves if there was no join before we continue
          _ <- if (joined.isEmpty) item.offset.commit else IO.unit
        } yield (result)
      })
      .flatten
  }

  private def joinItems(
      dynamoRecords: List[Either[DynamoRecord[X], DynamoRecord[Y]]]
  ): Option[(X, Y)] = {
    for {
      left <- dynamoRecords.find(x => x.isLeft).flatMap(x => x.left.toOption)
      right <- dynamoRecords.find(x => x.isRight).flatMap(x => x.right.toOption)
    } yield (left.content, right.content)
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
        writeToTable(table, stream.key(x.record), SK, x.record)(autoCodec) as x
      )
      .evalMap(x =>
        publishNotificationToKafka(
          config.kafkaNotificationTopicProducer,
          config.kafkaNotificationTopic,
          stream.key(x.record),
          SK
        ) as x.commitMetadata
      )
      .through(stream.commitProcessed)

  }
}
