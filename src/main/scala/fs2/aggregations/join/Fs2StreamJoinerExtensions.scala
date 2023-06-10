package fs2.aggregations.join

import cats.effect.IO
import fs2.Stream
import fs2.aggregations.join.models.{JoinRecord, StreamSource}

case class JoinConfig[X, Y, CommitMetadata](
    keyLeft: (X) => String,
    keyRight: (Y) => String,
    onStoreLeft: (JoinRecord[X,CommitMetadata] => IO[Unit]),
    onStoreRight: (JoinRecord[Y,CommitMetadata] => IO[Unit])
)

object Fs2StreamJoinerExtensions {
  implicit class FS2StreamJoinMethods[X, Y, CommitMetadata](fs2Stream: Stream[IO, JoinRecord[X, CommitMetadata]]) {

    def joinOneToOne(
        right: Stream[IO, JoinRecord[Y, CommitMetadata]],
        joiner: Fs2OneToOneJoiner[X, Y, CommitMetadata],
        joinConfig: JoinConfig[X, Y, CommitMetadata]
    ): Stream[IO, JoinedResult[X,Y]] = {
      val leftSource =
        StreamSource[X, CommitMetadata](fs2Stream, joinConfig.keyLeft, joinConfig.onStoreLeft)
      val rightSource =
        StreamSource[Y, CommitMetadata](right, joinConfig.keyRight, joinConfig.onStoreRight)

      joiner.join(leftSource, rightSource)
    }

    def joinOneToMany(
        right: Stream[IO, JoinRecord[Y, CommitMetadata]],
        joiner: Fs2OneToManyJoiner[X, Y, CommitMetadata],
        joinConfig: JoinConfig[X, Y, CommitMetadata]
    ): Stream[IO, JoinedResult[X,Y]] = {

      val leftSource =
        StreamSource[X, CommitMetadata](fs2Stream, joinConfig.keyLeft, joinConfig.onStoreLeft)
      val rightSource =
        StreamSource[Y, CommitMetadata](right, joinConfig.keyRight, joinConfig.onStoreRight)

      joiner.join(leftSource, rightSource)
    }

  }

}
