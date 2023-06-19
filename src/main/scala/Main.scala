import Main.{Hing, User}
import cats.effect.IO.pure
import cats.effect.{Async, ExitCode, IO, IOApp}
import fs2.aggregations.join.Fs2StreamJoinerExtensions.FS2StreamJoinMethods
import fs2.{Stream, _}
import fs2.kafka.{
  AutoOffsetReset,
  CommittableOffset,
  ConsumerSettings,
  Deserializer,
  KafkaConsumer,
  KafkaProducer,
  ProducerSettings,
  Serializer,
  commitBatchWithin
}
import fs2.aggregations.join.models.{
  JoinRecord,
  JoinedResult,
  LeftStreamSource,
  OneToManyJoinConfig,
  OneToOneJoinConfig
}
import fs2.aggregations.join.dynamo.DistributedDynamoJoiner
import fs2.aggregations.join.models.dynamo.DynamoStoreConfig
import fs2.kafka.consumer.KafkaConsume
import meteor.codec.{Codec, Decoder, Encoder}
import meteor.errors
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import meteor.syntax._
import fs2._

import scala.concurrent.duration.DurationInt

object Main extends IOApp {

  implicit val F = Async[IO]

  case class User(userId: String, name: String)
  case class Hing(userId: String, hingId: String, hing: String)

  object User {

    implicit val userEncoder: Encoder[User] = new Encoder[User] {
      override def write(a: User): AttributeValue =
        Map("userId" -> a.userId, "name" -> a.name).asAttributeValue
    }

    implicit val userDecoder: Decoder[User] = new Decoder[User] {
      override def read(av: AttributeValue): Either[errors.DecoderError, User] =
        for {
          userId <- av.getAs[String]("userId")
          name <- av.getAs[String]("name")
        } yield User(userId, name)
    }

  }

  object Hing {
    implicit val userEncoder: Encoder[Hing] = new Encoder[Hing] {
      override def write(a: Hing): AttributeValue =
        Map(
          "userId" -> a.userId,
          "hing" -> a.hing,
          "hingId" -> a.hingId
        ).asAttributeValue
    }

    implicit val userDecoder: Decoder[Hing] = new Decoder[Hing] {
      override def read(av: AttributeValue): Either[errors.DecoderError, Hing] =
        for {
          userId <- av.getAs[String]("userId")
          hing <- av.getAs[String]("hing")
          hingId <- av.getAs[String]("hingId")
        } yield Hing(userId, hingId, hing)
    }
  }

  private def getAppStream(
      kafkaProducer: KafkaProducer[IO, String, String],
      kafkaConsumer: KafkaConsumer[IO, String, String]
  ): Stream[IO, JoinedResult[User, Hing, CommittableOffset[IO]]] = {

    val dynamoClient = DynamoDbAsyncClient.create()

    val joiner = DistributedDynamoJoiner[User, Hing, Unit](
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

    val stream1: Stream[IO, JoinRecord[User, Unit]] =
      Stream(
        User("1", "Jimmy"),
        User("2", "Michael"),
        User("1", "Jim")
      )
        .evalMap(x => IO.sleep(15.seconds) as x)
        .map(x => JoinRecord(x, ()))

    val stream2: Stream[IO, JoinRecord[Hing, Unit]] =
      Stream(
        Hing("1", "a", "Nose picking"),
        Hing("2", "b", "Cheese eating"),
        Hing("1", "c", "Fannying aboot"),
        Hing("1", "c", "Mair fannying aboot")
      )
        .evalMap(x => IO.sleep(5.seconds) >> pure(x))
        .map(x => JoinRecord(x, ()))

    stream1
      .joinOneToMany(
        stream2,
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
      appStream <- getAppStream(producer, consumer)
        .evalMap(x => IO.println(x) as x)
        .map(x => JoinedResult.getOffset(x))
        .flatMap({
          case None    => Stream.empty
          case Some(x) => Stream.emit(x)
        })
        .through(commitBatchWithin(100, 2.seconds))
    } yield { appStream }

    appStream.compile.drain.void.map(_ => ExitCode.Success)
  }
}
