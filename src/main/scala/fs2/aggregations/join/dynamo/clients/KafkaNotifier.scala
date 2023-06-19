package fs2.aggregations.join.dynamo.clients

import cats.effect.IO
import fs2.kafka.{
  CommittableConsumerRecord,
  CommittableOffset,
  KafkaConsumer,
  KafkaProducer
}
import fs2.Stream

class KafkaNotifier(
    kafkaConsumer: KafkaConsumer[IO, String, String],
    kafkaNotificationTopicProducer: KafkaProducer[IO, String, String],
    kafkaNotificationTopic: String
) {

  def subscribeToNotifications(
  ): Stream[IO, Stream[IO, CommittableConsumerRecord[IO, String, String]]] = {
    for {
      _ <- Stream.eval(
        kafkaConsumer.subscribeTo(
          kafkaNotificationTopic
        )
      )

      stream <- kafkaConsumer.partitionedRecords

    } yield { stream }
  }

  def publishNotificationToKafka(
      PK: String,
      SK: String
  ): IO[Unit] = {

    kafkaNotificationTopicProducer
      .produceOne(kafkaNotificationTopic, PK, SK)
      .flatten
      .void
  }

}
