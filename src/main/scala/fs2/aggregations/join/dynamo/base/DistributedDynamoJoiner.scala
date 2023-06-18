package fs2.aggregations.join.dynamo.base

import cats.effect.IO
import fs2.Stream
import fs2.aggregations.join.dynamo.clients.{DynamoRecordDB, KafkaNotifier}
import fs2.aggregations.join.models.{JoinedResult, StreamSource}
import fs2.aggregations.join.models.dynamo.DynamoRecord
import fs2.aggregations.join.utils.StreamJoinUtils.concurrentlyUntilBothComplete
import fs2.kafka.{CommittableConsumerRecord, CommittableOffset}
import meteor.codec.Codec

class DistributedDynamoJoiner[X, Y, CommitMetadata](
    table: DynamoRecordDB,
    kafkaNotifier: KafkaNotifier
) {

  def streamFromStore(
      onUpdate: (
          CommittableConsumerRecord[IO, String, String]
      ) => Stream[IO, JoinedResult[X, Y, CommittableOffset[IO]]]
  ): Stream[IO, JoinedResult[X, Y, CommittableOffset[IO]]] = {
    kafkaNotifier
      .subscribeToNotifications()
      .flatMap(item => onUpdate(item))
  }

  private def sinkStream[Z](
      stream: StreamSource[Z, CommitMetadata],
      getSortKey: IO[String]
  )(implicit codec: Codec[Z]): Stream[IO, Unit] = {
    val decoder = DynamoRecord.dynamoRecordDecoder[Z]
    val encoder = DynamoRecord.dynamoRecordEncoder[Z]

    implicit val autoCodec: Codec[DynamoRecord[Z]] =
      Codec.dynamoCodecFromEncoderAndDecoder(encoder, decoder)

    val commitStream = for {
      x <- stream.source
      sortKey <- Stream.eval(getSortKey)
      _ <- Stream.eval[IO, Unit](
        table
          .writeToTable(stream.key(x.record), sortKey, x.record)(
            autoCodec
          )
      )
      _ <- Stream.eval(
        kafkaNotifier.publishNotificationToKafka(stream.key(x.record), sortKey)
      )
    } yield {
      x.commitMetadata
    }

    commitStream.through(stream.commitProcessed)
  }

  def sink(
      streamLeft: StreamSource[X, CommitMetadata],
      streamRight: StreamSource[Y, CommitMetadata],
      sortKeyRight: IO[String]
  )(implicit codecLeft: Codec[X], codecRight: Codec[Y]): Stream[IO, Unit] = {
    val leftSink = sinkStream(streamLeft, IO.pure("LEFT"))(codecLeft)
    val rightSink = sinkStream[Y](streamRight, sortKeyRight)(codecRight)

    concurrentlyUntilBothComplete(leftSink, rightSink)
  }

}
