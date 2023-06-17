package fs2.aggregations.join.models

import cats.effect.IO
import fs2.Pipe

case class JoinConfig[X, Y, CommitMetadata](
    keyLeft: (X) => String,
    keyRight: (Y) => String,
    commitStoreLeft: Pipe[IO, CommitMetadata, Unit],
    commitStoreRight: Pipe[IO, CommitMetadata, Unit]
)
