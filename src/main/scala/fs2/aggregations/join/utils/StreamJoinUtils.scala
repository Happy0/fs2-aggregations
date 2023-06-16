package fs2.aggregations.join.utils

import cats.effect.{Deferred, IO}
import fs2.Stream
import fs2.aggregations.join.models.dynamo.DynamoRecord
import fs2.kafka.KafkaProducer
import meteor.api.hi.CompositeTable
import meteor.codec.{Codec, Decoder}
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




}
