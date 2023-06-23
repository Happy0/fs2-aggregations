package fs2.aggregations.join.models.dynamo.config

import cats.effect.IO
import fs2.kafka.{KafkaConsumer, KafkaProducer}
import meteor.codec.Codec
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

case class DynamoStoreConfig[X, Y](
    client: DynamoDbAsyncClient,
    tableName: String,
    kafkaNotificationTopic: String,
    kafkaNotificationTopicProducer: KafkaProducer[IO, String, String],
    kafkaNotificationTopicConsumer: KafkaConsumer[IO, String, String],
    leftCodec: Codec[X],
    rightCodec: Codec[Y]
)
