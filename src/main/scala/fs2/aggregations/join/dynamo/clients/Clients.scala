package fs2.aggregations.join.dynamo.clients

import cats.effect.IO
import fs2.aggregations.join.models.dynamo.config.DynamoStoreConfig
import meteor.{DynamoDbType, KeyDef}
import meteor.api.hi.CompositeTable

class Clients(val dynamoRecordDB: DynamoRecordDB, val kafkaNotifier: KafkaNotifier)
object Clients {
  def apply[X, Y, CommitableOffset](
      config: DynamoStoreConfig[X, Y]
  ): Clients = {
    val table: CompositeTable[IO, String, String] =
      CompositeTable[IO, String, String](
        config.tableName,
        KeyDef[String]("PK", DynamoDbType.S),
        KeyDef[String]("SK", DynamoDbType.S),
        config.client
      )

    val dynamoRecordDB = new DynamoRecordDB(table)

    val kafkaNotifier = new KafkaNotifier(
      config.kafkaNotificationTopicConsumer,
      config.kafkaNotificationTopicProducer,
      config.kafkaNotificationTopic
    )

    new Clients(dynamoRecordDB, kafkaNotifier)
  }

}
