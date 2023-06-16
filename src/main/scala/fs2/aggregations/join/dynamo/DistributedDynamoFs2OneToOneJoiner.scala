package fs2.aggregations.join.dynamo

import cats.effect.kernel.Deferred
import cats.effect.{Async, IO}
import dynosaur.Schema
import fs2.Stream
import fs2.aggregations.join.dynamo.base.DistributedDynamoJoiner
import fs2.aggregations.join.dynamo.clients.{
  Clients,
  DynamoRecordDB,
  KafkaNotifier
}
import fs2.aggregations.join.models.dynamo.{DynamoRecord, DynamoStoreConfig}
import fs2.aggregations.join.{Fs2OneToOneJoiner, JoinedResult}
import fs2.aggregations.join.models.StreamSource
import fs2.kafka.{CommittableConsumerRecord, CommittableOffset, KafkaConsumer}
import meteor.{DynamoDbType, KeyDef}
import meteor.api.hi.CompositeTable
import meteor.codec.{Codec, Decoder}
import meteor.codec.Encoder.dynamoEncoderForString
import fs2.aggregations.join.utils.StreamJoinUtils._

final case class DistributedDynamoFs2OneToOneJoiner[X, Y, CommitMetadata](
    config: DynamoStoreConfig[X, Y]
) extends Fs2OneToOneJoiner[X, Y, CommitMetadata, CommittableOffset[IO]] {

  private val clients = Clients(config)

  private val distributedDynamoJoiner =
    new DistributedDynamoJoiner[X, Y, CommitMetadata](
      clients.dynamoRecordDB,
      clients.kafkaNotifier
    )
  override def sinkToStore(
      left: StreamSource[X, CommitMetadata],
      right: StreamSource[Y, CommitMetadata]
  ): Stream[IO, Unit] = {

    distributedDynamoJoiner.sink(left, right, left => "LEFT", right => "RIGHT")(
      config.leftCodec,
      config.rightCodec
    )
  }

  override def streamFromStore(
  ): Stream[IO, JoinedResult[X, Y, CommittableOffset[IO]]] = {
    implicit val eitherDecoder =
      DynamoRecord.eitherDecoder[X, Y](config.leftCodec, config.rightCodec)

    distributedDynamoJoiner.streamFromStore(joinResults)
  }

  private def joinResults(
      notification: CommittableConsumerRecord[IO, String, String]
  )(implicit
      decoder: Decoder[Either[DynamoRecord[X], DynamoRecord[Y]]]
  ): Stream[IO, JoinedResult[X, Y, CommittableOffset[IO]]] = {

    val pk = notification.record.key

    val result = for {
      items <- clients.dynamoRecordDB
        .streamDynamoPartition(pk)
        .compile
        .toList
      joined = joinItems(items)
      result = Stream
        .fromOption[IO](joined.map(x => JoinedResult(x, notification.offset)))

      // Commit ourselves if there was no join before we continue
      _ <- if (joined.isEmpty) notification.offset.commit else IO.unit
    } yield result

    Stream.eval(result).flatten
  }

  private def joinItems(
      dynamoRecords: List[Either[DynamoRecord[X], DynamoRecord[Y]]]
  ): Option[(X, Y)] = {
    for {
      left <- dynamoRecords.find(x => x.isLeft).flatMap(x => x.left.toOption)
      right <- dynamoRecords.find(x => x.isRight).flatMap(x => x.right.toOption)
    } yield (left.content, right.content)
  }
}
