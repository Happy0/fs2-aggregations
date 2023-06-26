package example.migration

import cats.effect.IO
import example.onetomany.{Hing, User}
import fs2.Stream
import fs2.aggregations.join.models.JoinRecord

object ExampleData {

  val stream1: Stream[IO, JoinRecord[UserV2, Unit]] = Stream.empty

  val stream2: Stream[IO, JoinRecord[Hing, Unit]] = Stream
    .emit(
      Hing("1", "c", "My hobby is golf now")
    )
    .map(x => JoinRecord(x, ()))

}
