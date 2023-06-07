package fs2.aggregations.join.models

import cats.effect.IO
import fs2.Stream

case class StreamSource[X, CommitMetadata](
    source: Stream[IO, JoinRecord[X, CommitMetadata]],
    key: (X) => String,
    onStore: (JoinRecord[X, CommitMetadata] => IO[Unit])
)