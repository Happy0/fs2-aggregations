package fs2.aggregations.join.dynamo

import cats.effect.IO
import fs2.Stream
import fs2.aggregations.join.Fs2StreamJoiner
import fs2.aggregations.join.dynamo.base.DistributedDynamoJoinerUtils
import fs2.aggregations.join.dynamo.clients.Clients
import fs2.aggregations.join.models.dynamo.{DynamoRecord, DynamoStoreConfig}
import fs2.aggregations.join.models.{
  CommitResult,
  FullJoinedResult,
  JoinedResult,
  LeftStreamSource,
  RightStreamSource
}
import fs2.kafka.{CommittableConsumerRecord, CommittableOffset}
import meteor.codec.Decoder

final case class DistributedDynamoJoiner[X, Y, CommitMetadata](
    config: DynamoStoreConfig[X, Y]
) extends Fs2StreamJoiner[X, Y, CommitMetadata, CommittableOffset[IO]] {

  private val clients = Clients(config)

  private val distributedDynamoJoinerUtils =
    new DistributedDynamoJoinerUtils[X, Y, CommitMetadata](
      clients.dynamoRecordDB,
      clients.kafkaNotifier
    )
  override def sinkToStore(
      left: LeftStreamSource[X, CommitMetadata],
      right: RightStreamSource[Y, CommitMetadata]
  ): Stream[IO, Unit] = {

    distributedDynamoJoinerUtils.sink(left, right)(
      config.leftCodec,
      config.rightCodec
    )
  }

  override def streamFromStore(
  ): Stream[IO, JoinedResult[X, Y, CommittableOffset[IO]]] = {
    implicit val eitherDecoder =
      DynamoRecord.eitherDecoder[X, Y](config.leftCodec, config.rightCodec)

    implicit val leftDecoder =
      DynamoRecord.dynamoRecordDecoder(config.leftCodec)
    implicit val rightDecoder =
      DynamoRecord.dynamoRecordDecoder(config.rightCodec)

    distributedDynamoJoinerUtils.streamFromStore(x =>
      joinResults(x)(leftDecoder, rightDecoder, eitherDecoder)
    )
  }

  private def joinResults(
      notification: CommittableConsumerRecord[IO, String, String]
  )(implicit
      leftDecoder: Decoder[DynamoRecord[X]],
      rightDecoder: Decoder[DynamoRecord[Y]],
      eitherDecoder: Decoder[Either[DynamoRecord[X], DynamoRecord[Y]]]
  ): Stream[IO, JoinedResult[X, Y, CommittableOffset[IO]]] = {

    val pk = notification.record.key
    val sk = notification.record.value

    val commitPrompt: CommitResult[X, Y, CommittableOffset[IO]] =
      CommitResult[X, Y, CommittableOffset[IO]](notification.offset)

    val isLeftUpdate = sk == "LEFT"

    val x = for {
      left <- Stream.eval(
        clients.dynamoRecordDB.getItem[X](pk, "LEFT")(leftDecoder)
      )

      result: Stream[IO, JoinedResult[DynamoRecord[X], DynamoRecord[
        Y
      ], CommittableOffset[IO]]] = left match {
        case None => Stream.emit[IO, CommitResult[X, Y, CommittableOffset[IO]]](commitPrompt)
        case Some(x) => joinStream[X,Y](x, pk, commitPrompt)(eitherDecoder)
      }

    } yield result

    x.flatten

  }

  private def joinStream[X, Y](
      leftItem: DynamoRecord[X],
      joinKey: String,
      commitResult: CommitResult[X, Y, CommittableOffset[IO]]
  )(implicit
      eitherDecoder: Decoder[Either[DynamoRecord[X], DynamoRecord[Y]]]
  ): Stream[IO, JoinedResult[DynamoRecord[X], DynamoRecord[
    Y
  ], CommittableOffset[IO]]] = {

    val joinStream: Stream[IO, JoinedResult[DynamoRecord[X], DynamoRecord[
      Y
    ], CommittableOffset[IO]]] = clients.dynamoRecordDB
      .streamDynamoPartition(joinKey)
      .flatMap(x =>
        x match {
          case Left(x)  => Stream.empty[IO, DynamoRecord[Y]]
          case Right(x) => Stream.emit[IO, DynamoRecord[Y]](x)
        }
      )
      .map(x =>
        FullJoinedResult[DynamoRecord[X], DynamoRecord[Y], CommittableOffset[
          IO
        ]]((leftItem, x))
      )

    val commitPromptStream
        : Stream[IO, CommitResult[X, Y, CommittableOffset[IO]]] =
      Stream.emit(commitResult)

    joinStream.onComplete(commitPromptStream)
  }

}
