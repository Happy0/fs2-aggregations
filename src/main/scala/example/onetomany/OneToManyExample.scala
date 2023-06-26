package example.onetomany

import cats.effect.IO.pure
import cats.effect.{Async, ExitCode, IO, IOApp}
import fs2.Stream
import fs2.aggregations.join.dynamo.DistributedDynamoJoiner
import fs2.aggregations.join.extensions.Fs2StreamJoinerExtensions.FS2StreamJoinMethods
import fs2.aggregations.join.extensions.dynamo.DistributedDynamoJoinerExtensions.DistributedDynamoJoinerMethods
import fs2.aggregations.join.models.dynamo.config.DynamoStoreConfig
import fs2.aggregations.join.models.{
  JoinRecord,
  JoinedResult,
  OneToManyJoinConfig
}
import fs2.kafka._
import meteor.codec.{Codec, Decoder, Encoder}
import meteor.errors
import meteor.syntax._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import scala.concurrent.duration.DurationInt

object Main extends IOApp {

  private def buildDistributedDynamoJoiner(
      kafkaProducer: KafkaProducer[IO, String, String],
      kafkaConsumer: KafkaConsumer[IO, String, String]
  ): DistributedDynamoJoiner[User, Hing, Unit] = {

    val dynamoClient = DynamoDbAsyncClient.create()

    DistributedDynamoJoiner[User, Hing, Unit](
      config = DynamoStoreConfig(
        client = dynamoClient,
        tableName = "joinTableTest",
        "test-notifications",
        kafkaProducer,
        kafkaConsumer,
        leftCodec = Codec[User],
        rightCodec = Codec[Hing]
      )
    )
  }

  private def joinedUsersAndHingStream(
      joiner: DistributedDynamoJoiner[User, Hing, Unit]
  ): Stream[IO, JoinedResult[User, Hing, CommittableOffset[IO]]] = {

    val userStream: Stream[IO, JoinRecord[User, Unit]] = ExampleData.stream1
    val hingStream: Stream[IO, JoinRecord[Hing, Unit]] = ExampleData.stream2

    userStream
      .joinOneToMany(
        hingStream,
        joiner,
        OneToManyJoinConfig[User, Hing, Unit](
          joinKeyLeft = (x) => x.userId,
          joinKeyRight = (y) => y.userId,
          sortKeyRight = (y) => y.hingId,
          commitStoreLeft = (x) => x,
          commitStoreRight = (x) => x
        )
      )
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

    val appStream = for {
      producer <- KafkaProducer.stream(producerSettings)
      consumer <- KafkaConsumer
        .stream(consumerSettings)

      joiner = buildDistributedDynamoJoiner(producer, consumer)

      appStream <- joinedUsersAndHingStream(joiner)
        .processJoin(
          joinedStream =>
            joinedStream.map { case (user, hing) =>
              UserHing(user.userId, user.name, hing.hing)
            },
          commitBatchWithin(100, 2.seconds)
        )
    } yield { appStream }

    appStream.compile.drain.void.map(_ => ExitCode.Success)
  }
}
