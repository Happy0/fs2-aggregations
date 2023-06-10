package fs2.aggregations.join.models

import cats.effect.IO
import fs2.{Pipe, Stream}

case class StreamSource[X, CommitMetadata](
    source: Stream[IO, JoinRecord[X, CommitMetadata]],
    key: (X) => String,
    commitProcessed: Pipe[IO, CommitMetadata, Unit])
