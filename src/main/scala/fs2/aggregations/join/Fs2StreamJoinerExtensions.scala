package fs2.aggregations.join

import cats.effect.IO
import fs2.{Pipe, Stream}
import fs2.aggregations.join.models.{JoinRecord, JoinedResult, LeftStreamSource, OneToManyJoinConfig, OneToOneJoinConfig, RightStreamSource}
object Fs2StreamJoinerExtensions {
  implicit class FS2StreamJoinMethods[
      X,
      Y,
      SourceCommitMetadata,
      StoreCommitMetadata
  ](fs2Stream: Stream[IO, JoinRecord[X, SourceCommitMetadata]]) {

    def joinOneToOne(
        right: Stream[IO, JoinRecord[Y, SourceCommitMetadata]],
        joiner: Fs2StreamJoiner[
          X,
          Y,
          SourceCommitMetadata,
          StoreCommitMetadata
        ],
        joinConfig: OneToOneJoinConfig[X, Y, SourceCommitMetadata]
    ): Stream[IO, JoinedResult[X, Y, StoreCommitMetadata]] = {
      val leftSource =
        LeftStreamSource[X, SourceCommitMetadata](
          fs2Stream,
          joinConfig.joinKeyLeft,
          joinConfig.commitStoreLeft
        )

      val rightSource =
        RightStreamSource[Y, SourceCommitMetadata](
          right,
          joinConfig.joinKeyRight,
          (y: Y) => "RIGHT",
          joinConfig.commitStoreRight
        )

      joiner.join(leftSource, rightSource)
    }

    def joinOneToMany(
        right: Stream[IO, JoinRecord[Y, SourceCommitMetadata]],
        joiner: Fs2StreamJoiner[X, Y, SourceCommitMetadata, StoreCommitMetadata],
        joinConfig: OneToManyJoinConfig[X, Y, SourceCommitMetadata]
    ): Stream[IO, JoinedResult[X, Y, StoreCommitMetadata]] = {

      val leftSource =
        LeftStreamSource[X, SourceCommitMetadata](
          fs2Stream,
          joinConfig.joinKeyLeft,
          joinConfig.commitStoreLeft
        )
      val rightSource =
        RightStreamSource[Y, SourceCommitMetadata](
          right,
          joinConfig.joinKeyRight,
          joinConfig.sortKeyRight,
          joinConfig.commitStoreRight
        )

      joiner.join(leftSource, rightSource)
    }

  }

}
