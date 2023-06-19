package fs2.aggregations.join.models

import cats.effect.IO
import fs2.{Pipe, Stream}

case class LeftStreamSource[X, CommitMetadata](
    source: Stream[IO, JoinRecord[X, CommitMetadata]],
    joinKey: (X) => String,
    commitProcessed: Pipe[IO, CommitMetadata, Unit]
)

case class RightStreamSource[X, CommitMetadata](
    source: Stream[IO, JoinRecord[X, CommitMetadata]],
    joinKey: (X) => String,
    sortKey: (X) => String,
    commitProcessed: Pipe[IO, CommitMetadata, Unit]
)
