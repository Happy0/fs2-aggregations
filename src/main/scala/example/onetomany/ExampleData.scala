package example.onetomany

import cats.effect.IO
import cats.effect.IO.pure
import fs2.aggregations.join.models.JoinRecord
import fs2.Stream

import scala.concurrent.duration.DurationInt
import meteor.codec.{Codec, Decoder, Encoder}
object ExampleData {

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

}
