package example.migration

import cats.effect.{ExitCode, IO, IOApp}
import example.onetomany.{Hing, User}
import example.migration.ExampleData
import fs2.aggregations.join.dynamo.migrations.DistributedDynamoJoinerInPlaceMigration
import fs2.aggregations.join.models.dynamo.DynamoRecord
import fs2.aggregations.join.models.dynamo.migration.DynamoInPlaceMigrationConfig
import meteor.codec.Codec
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import fs2.Stream
import fs2.aggregations.join.dynamo.DistributedDynamoJoiner
import fs2.aggregations.join.extensions.Fs2StreamJoinerExtensions.FS2StreamJoinMethods
import fs2.kafka.{
  AutoOffsetReset,
  ConsumerSettings,
  Deserializer,
  KafkaConsumer,
  KafkaProducer,
  ProducerSettings,
  Serializer,
  commitBatchWithin
}
import fs2.aggregations.join.models.dynamo.config.DynamoStoreConfig
import fs2.aggregations.join.extensions.dynamo.DistributedDynamoJoinerExtensions.DistributedDynamoJoinerMethods
import fs2.aggregations.join.models.{JoinedResult, OneToManyJoinConfig}

import scala.concurrent.duration.DurationInt

object MigrationExample extends IOApp {

  private val dynamoClient = DynamoDbAsyncClient.create()

  private val migrator =
    new DistributedDynamoJoinerInPlaceMigration[User, Hing, UserV2, Hing](
      DynamoInPlaceMigrationConfig(
        dynamoClient,
        "joinTableTest",
        "joinTableTest",
        "test-notifications",
        Codec[User],
        Codec[Hing],
        Codec[UserV2],
        Codec[Hing]
      )
    )

  private def buildDistributedDynamoJoiner(
      kafkaProducer: KafkaProducer[IO, String, String],
      kafkaConsumer: KafkaConsumer[IO, String, String]
  ): DistributedDynamoJoiner[UserV2, Hing, Unit] = {

    val dynamoClient = DynamoDbAsyncClient.create()

    DistributedDynamoJoiner[UserV2, Hing, Unit](
      config = DynamoStoreConfig(
        client = dynamoClient,
        tableName = "joinTableTest",
        "test-notifications",
        kafkaProducer,
        kafkaConsumer,
        leftCodec = Codec[UserV2],
        rightCodec = Codec[Hing]
      )
    )
  }

  private def doMigration(): IO[Unit] = {
    val left = (dynamoRecordLeft: DynamoRecord[User]) =>
      IO.randomUUID.map(pretendProfilePictureUUID =>
        DynamoRecord[UserV2](
          dynamoRecordLeft.PK,
          dynamoRecordLeft.SK,
          new UserV2(
            dynamoRecordLeft.content.userId,
            dynamoRecordLeft.content.name,
            pretendProfilePictureUUID.toString
          )
        )
      )

    val right = (dynamoRecordRight: DynamoRecord[Hing]) =>
      IO.pure(dynamoRecordRight)

    migrator.migrateInPlace(left, right)
  }
  def run(args: List[String]): IO[ExitCode] = {

    val producerSettings = ProducerSettings[IO, String, String](
      keySerializer = Serializer[IO, String],
      valueSerializer = Serializer[IO, String]
    )
      .withBootstrapServers("localhost:9092")
      .withClientId("produceraroonie")

    val consumerSettings = ConsumerSettings[IO, String, String](
      keyDeserializer = Deserializer[IO, String],
      valueDeserializer = Deserializer[IO, String]
    ).withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withBootstrapServers("localhost:9092")
      .withGroupId("consumergrouparoonie-1")

    val userStream = ExampleData.stream1
    val hingStream = ExampleData.stream2

    val streamApp = for {
      _ <- Stream.eval(doMigration())
      producer <- KafkaProducer.stream(producerSettings)
      consumer <- KafkaConsumer
        .stream(consumerSettings)

      joiner = buildDistributedDynamoJoiner(producer, consumer)

      joinedResult <- userStream
        .joinOneToMany(
          hingStream,
          joiner,
          OneToManyJoinConfig[UserV2, Hing, Unit](
            joinKeyLeft = (x) => x.userId,
            joinKeyRight = (y) => y.userId,
            sortKeyRight = (y) => y.hingId,
            commitStoreLeft = (x) => x,
            commitStoreRight = (x) => x
          )
        )
        .processJoin(
          stream =>
            stream.map { case (user, hing) =>
              UserHingV2(user.userId, user.name, hing.hing, user.displayPicture)
            },
          commitBatchWithin(100, 2.seconds)
        )

    } yield (joinedResult)

    streamApp
      .evalMap(x => IO.println(x))
      .compile
      .drain
      .void
      .map(_ => ExitCode.Success)
  }

}
