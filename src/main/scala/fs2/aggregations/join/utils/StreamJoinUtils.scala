package fs2.aggregations.join.utils

import cats.effect.{Deferred, IO}
import fs2.Stream
import fs2.aggregations.join.models.dynamo.DynamoRecord
import fs2.kafka.KafkaProducer
import meteor.api.hi.CompositeTable
import meteor.codec.Codec
object StreamJoinUtils {

  private def notifyFinished(deferred: Deferred[IO, Unit]) =
    Stream.eval(deferred.complete().void)

  private def awaitFinished(deferred: Deferred[IO, Unit]) =
    Stream.eval(deferred.get.void)

  def concurrentlyUntilBothComplete[A, B](
      outer: Stream[IO, A],
      inner: Stream[IO, B]
  ): Stream[IO, A] = {
    for {
      rightStreamCompleteNotifier <- Stream.eval(Deferred[IO, Unit])
      awaitFinishedNotifier = awaitFinished(rightStreamCompleteNotifier)
      notifyComplete = notifyFinished(rightStreamCompleteNotifier)

      left = outer.onComplete(awaitFinishedNotifier >> Stream.empty)
      right = inner.onComplete(notifyComplete >> Stream.empty)

      stream <- left concurrently right
    } yield {
      stream
    }

  }

  def writeToTable[Z](
      table: CompositeTable[IO, String, String],
      PK: String,
      SK: String,
      item: Z
  )(implicit
      itemCodec: Codec[DynamoRecord[Z]]
  ): IO[Unit] = {
    val record = DynamoRecord[Z](PK, SK, item)

    table.put(record)(itemCodec)
  }

  def publishNotificationToKafka(
      kafkaNotificationTopicProducer: KafkaProducer[IO, String, String],
      notificationTopic: String,
      PK: String,
      SK: String
  ): IO[Unit] = {

    kafkaNotificationTopicProducer
      .produceOne(notificationTopic, PK, SK)
      .flatten
      .void
  }

}
