package fs2.aggregations.join.dynamo.base

import cats.effect.IO
import fs2.Stream
import fs2.aggregations.join.JoinedResult
import fs2.aggregations.join.dynamo.clients.{DynamoRecordDB, KafkaNotifier}
import fs2.aggregations.join.models.StreamSource
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
      getSortKey: (Z) => String
  )(implicit codec: Codec[Z]): Stream[IO, Unit] = {
    val decoder = DynamoRecord.dynamoRecordDecoder[Z]
    val encoder = DynamoRecord.dynamoRecordEncoder[Z]

    implicit val autoCodec: Codec[DynamoRecord[Z]] =
      Codec.dynamoCodecFromEncoderAndDecoder(encoder, decoder)

    stream.source
      .evalMap((x) =>
        table
          .writeToTable(stream.key(x.record), getSortKey(x.record), x.record)(
            autoCodec
          ) as x
      )
      .evalMap(x =>
        kafkaNotifier.publishNotificationToKafka(
          stream.key(x.record),
          getSortKey(x.record)
        ) as x.commitMetadata
      )
      .through(stream.commitProcessed)
  }

  def sink(
      streamLeft: StreamSource[X, CommitMetadata],
      streamRight: StreamSource[Y, CommitMetadata],
      keyLeft: (X) => String,
      keyRight: (Y => String)
  )(implicit codecLeft: Codec[X], codecRight: Codec[Y]): Stream[IO, Unit] = {
    val leftSink = sinkStream(streamLeft, keyLeft)(codecLeft)
    val rightSink = sinkStream[Y](streamRight, keyRight)(codecRight)

    concurrentlyUntilBothComplete(leftSink, rightSink)
  }

}

