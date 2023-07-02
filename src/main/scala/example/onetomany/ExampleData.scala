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
      User("1", "Brian Limond"),
      User("2", "Paul"),
      User("1", "Limmy")
    )
      .evalMap(x => IO.sleep(15.seconds) as x)
      .map(x => JoinRecord(x, ()))

  val stream2: Stream[IO, JoinRecord[Hing, Unit]] =
    Stream(
      Hing("1", "a", "I've got a secret hamster. Keep it up in my loft. She thinks I'm up there trying to fix a leak in the roof, but, I'm up there feeding it. Giving it a wee clap."),
      Hing("2", "b", "See, on my way to work, I pass this dump. It's not a real dump. It's just like a bit of spare ground. Well, see in there, there's this wardrobe, right, It's been there for months. I get in the wardrobe,sometimes for ten minutes, sometimes for ten hours."),
      Hing("1", "c", "Wit's yer hing?"),
      Hing("1", "c", "Every Thursday, I pull down the shutters... phone the wife...")
    )
      .evalMap(x => IO.sleep(5.seconds) >> pure(x))
      .map(x => JoinRecord(x, ()))

}
