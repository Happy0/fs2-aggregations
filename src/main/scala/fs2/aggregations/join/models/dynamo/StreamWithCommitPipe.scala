package fs2.aggregations.join.models.dynamo

import fs2.Stream
import cats.effect.IO
import fs2.Pipe
import fs2.aggregations.join.models.JoinedResult
import fs2.kafka.CommittableOffset

case class StreamWithCommitPipe[X, Y](
    stream: Stream[
      IO,
      JoinedResult[X, Y, CommittableOffset[IO]],
    ],
    commitBatch: Pipe[IO, CommittableOffset[IO], Unit]
)
