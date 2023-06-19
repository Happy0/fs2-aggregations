package fs2.aggregations.join.models

import cats.effect.IO
import fs2.Pipe

case class OneToOneJoinConfig[X, Y, CommitMetadata](
    joinKeyLeft: (X) => String,
    joinKeyRight: (Y) => String,
    commitStoreLeft: Pipe[IO, CommitMetadata, Unit],
    commitStoreRight: Pipe[IO, CommitMetadata, Unit]
)

case class OneToManyJoinConfig[X, Y, CommitMetadata](
    joinKeyLeft: (X) => String,
    joinKeyRight: (Y) => String,
    sortKeyRight: (Y) => String,
    commitStoreLeft: Pipe[IO, CommitMetadata, Unit],
    commitStoreRight: Pipe[IO, CommitMetadata, Unit]
)
