import cats.effect.{ExitCode, IO, IOApp}
import fs2.aggregations.join.JoinConfig
import fs2.{Stream, _}
import fs2.kafka.commitBatchWithin
import fs2.aggregations.join.models.{JoinRecord, StreamSource}
import fs2.aggregations.join.dynamo.DistributedDynamoFs2OneToOneJoiner
import fs2.aggregations.join.models.dynamo.DynamoStoreConfig
import meteor.codec.Codec

object Main extends IOApp {

  import fs2.aggregations.join.Fs2StreamJoinerExtensions._
  def run(args: List[String]): IO[ExitCode] = {

    val joiner = DistributedDynamoFs2OneToOneJoiner[Int, Int, Unit](
      config = DynamoStoreConfig(
        client = ???,
        tableName = "joinTableTest",
        kafkaNotificationTopic = "test",
        leftCodec = Codec[Int],
        rightCodec = Codec[Int]
      )
    )

    val stream1: Stream[IO, JoinRecord[Int, Unit]] =
      Stream(1, 2, 3, 4).map(x => JoinRecord(x, Unit))
    val stream2: Stream[IO, JoinRecord[Int, Unit]] =
      Stream(1, 2, 3, 4).map(x => JoinRecord(x, Unit))

    val joinStream = stream1
      .joinOneToOne(
        stream2,
        joiner,
        JoinConfig(
          keyLeft = (x) => x.toString(),
          keyRight = (y: Int) => y.toString(),
          commitStoreLeft = (x) => x,
          commitStoreRight = ((y) => y)
        )
      )

    joinStream.compile.drain.map(_ => ExitCode.Success)
  }

}
