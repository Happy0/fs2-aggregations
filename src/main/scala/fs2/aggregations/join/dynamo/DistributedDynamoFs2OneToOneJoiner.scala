package fs2.aggregations.join.dynamo

import cats.effect.{IO}
import fs2.Stream
import fs2.aggregations.join.Fs2OneToOneJoiner
import fs2.aggregations.join.dynamo.base.DistributedDynamoJoiner
import fs2.aggregations.join.dynamo.clients.{Clients}
import fs2.aggregations.join.models.dynamo.{DynamoRecord, DynamoStoreConfig}
import fs2.aggregations.join.models.{JoinedResult, StreamSource}
import fs2.kafka.{CommittableConsumerRecord, CommittableOffset}
import meteor.codec.{Decoder}
import fs2.aggregations.join.models.FullJoinedResult
import fs2.aggregations.join.models.PartialJoinedResult

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

    distributedDynamoJoiner.sink(left, right, () => IO.pure("RIGHT"))(
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

      joined = joinItems(items, notification.offset)

    } yield joined

    Stream.eval(result)
  }

  private def joinItems(
      dynamoRecords: List[Either[DynamoRecord[X], DynamoRecord[Y]]],
      offset: CommittableOffset[IO]
  ): JoinedResult[X, Y, CommittableOffset[IO]] = {

    val left = dynamoRecords.find(x => x.isLeft).flatMap(x => x.left.toOption)
    val right =
      dynamoRecords.find(x => x.isRight).flatMap(x => x.right.toOption)

    (left, right) match {
      case (Some(left), Some(right)) =>
        FullJoinedResult[X, Y, CommittableOffset[IO]](
          (left.content, right.content),
          offset
        )
      case (x, y) => {
        val contentX = x.map(x => x.content)
        val contentY = y.map(y => y.content)

        PartialJoinedResult[X, Y, CommittableOffset[IO]](
          (contentX, contentY),
          offset
        )
      }

    }
  }
}
