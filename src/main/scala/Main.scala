import cats.effect.{ExitCode, IO, IOApp}
import fs2.aggregations.join.JoinConfig
import fs2.{Stream, _}
import fs2.kafka.commitBatchWithin
import fs2.aggregations.join.models.{JoinRecord, StreamSource}
import fs2.aggregations.join.dynamo.DistributedDynamoFs2OneToOneJoiner
import fs2.aggregations.join.models.dynamo.DynamoStoreConfig
import meteor.codec.{Codec, Decoder, Encoder}
import meteor.errors
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import meteor.syntax._
object Main extends IOApp {
  case class User(userId: String, name: String)
  case class Hing(userId: String, hing: String)

  object User {

    implicit val userEncoder: Encoder[User] = new Encoder[User] {
      override def write(a: User): AttributeValue = Map("userId" -> a.userId, "name" -> a.name).asAttributeValue
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
      override def write(a: Hing): AttributeValue = Map("userId" -> a.userId, "hing" -> a.hing).asAttributeValue
    }

    implicit val userDecoder: Decoder[Hing] = new Decoder[Hing] {
      override def read(av: AttributeValue): Either[errors.DecoderError, Hing] =
        for {
          userId <- av.getAs[String]("userId")
          hing <- av.getAs[String]("hing")
        } yield Hing(userId, hing)
    }
  }

  import fs2.aggregations.join.Fs2StreamJoinerExtensions._
  def run(args: List[String]): IO[ExitCode] = {

    val dynamoClient = DynamoDbAsyncClient.create()

    val joiner = DistributedDynamoFs2OneToOneJoiner[User, Hing, Unit](
      config = DynamoStoreConfig(
        client = dynamoClient,
        tableName = "joinTableTest",
        kafkaNotificationTopic = "test",
        leftCodec = Codec[User],
        rightCodec = Codec[Hing]
      )
    )

    val stream1: Stream[IO, JoinRecord[User, Unit]] =
      Stream(
        User("1", "Jimmy"),
        User("2", "Michael")
      ).map(x => JoinRecord(x, ()))


    val stream2: Stream[IO, JoinRecord[Hing, Unit]] =
      Stream(
        Hing("1", "Nose picking"),
        Hing("2", "Cheese eating")
      ).map(x => JoinRecord(x, ()))

    val joinStream = stream1
      .joinOneToOne(
        stream2,
        joiner,
        JoinConfig[User, Hing, Unit ](
          keyLeft = (x) => x.userId,
          keyRight = (y) => y.userId,
          commitStoreLeft = (x) => x,
          commitStoreRight = ((y) => y)
        )
      )

    joinStream.compile.drain.map(_ => ExitCode.Success)
  }

}
