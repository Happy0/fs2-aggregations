package fs2.aggregations.join.dynamo.base

import cats.effect.IO
import fs2.{Pipe, Stream}
import fs2.aggregations.join.dynamo.clients.{DynamoRecordDB, KafkaNotifier}
import fs2.aggregations.join.models.{
  JoinRecord,
  JoinedResult,
  LeftStreamSource,
  RightStreamSource
}
import fs2.aggregations.join.models.dynamo.DynamoRecord
import fs2.aggregations.join.utils.StreamJoinUtils.concurrentlyUntilBothComplete
import fs2.kafka.{CommittableConsumerRecord, CommittableOffset}
import meteor.codec.Codec

class DistributedDynamoJoinerUtils[X, Y, CommitMetadata](
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
      stream: Stream[IO, JoinRecord[Z, CommitMetadata]],
      joinKey: (Z) => String,
      getSortKey: (Z) => String,
      commitProcessed: Pipe[IO, CommitMetadata, Unit]
  )(implicit codec: Codec[Z]): Stream[IO, Unit] = {
    val decoder = DynamoRecord.dynamoRecordDecoder[Z]
    val encoder = DynamoRecord.dynamoRecordEncoder[Z]

    implicit val autoCodec: Codec[DynamoRecord[Z]] =
      Codec.dynamoCodecFromEncoderAndDecoder(encoder, decoder)

    stream
      .evalMap((x) =>
        table
          .writeToTable(
            joinKey(x.record),
            getSortKey(x.record),
            x.record
          )(
            autoCodec
          ) as x
      )
      .evalMap(x =>
        kafkaNotifier.publishNotificationToKafka(
          joinKey(x.record),
          getSortKey(x.record)
        ) as x.commitMetadata
      )
      .through(commitProcessed)
  }

  def sink(
      streamLeft: LeftStreamSource[X, CommitMetadata],
      streamRight: RightStreamSource[Y, CommitMetadata]
  )(implicit codecLeft: Codec[X], codecRight: Codec[Y]): Stream[IO, Unit] = {
    val leftSink = sinkStream[X](
      streamLeft.source,
      streamLeft.joinKey,
      (x: X) => "LEFT",
      streamLeft.commitProcessed
    )(codecLeft)

    val rightSink = sinkStream[Y](
      streamRight.source,
      streamRight.joinKey,
      streamRight.sortKey,
      streamRight.commitProcessed
    )(codecRight)

    concurrentlyUntilBothComplete(leftSink, rightSink)
  }

}
