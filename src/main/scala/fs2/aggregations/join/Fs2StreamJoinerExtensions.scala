package fs2.aggregations.join

import cats.effect.IO
import fs2.{Pipe, Stream}
import fs2.aggregations.join.models.{JoinConfig, JoinRecord, JoinedResult, LeftStreamSource}
object Fs2StreamJoinerExtensions {
  implicit class FS2StreamJoinMethods[
      X,
      Y,
      SourceCommitMetadata,
      StoreCommitMetadata
  ](fs2Stream: Stream[IO, JoinRecord[X, SourceCommitMetadata]]) {

    def joinOneToOne(
        right: Stream[IO, JoinRecord[Y, SourceCommitMetadata]],
        joiner: Fs2OneToOneJoiner[
          X,
          Y,
          SourceCommitMetadata,
          StoreCommitMetadata
        ],
        joinConfig: JoinConfig[X, Y, SourceCommitMetadata]
    ): Stream[IO, JoinedResult[X, Y, StoreCommitMetadata]] = {
      val leftSource =
        LeftStreamSource[X, SourceCommitMetadata](
          fs2Stream,
          joinConfig.keyLeft,
          joinConfig.commitStoreLeft
        )

      val rightSource =
        LeftStreamSource[Y, SourceCommitMetadata](
          right,
          joinConfig.keyRight,
          joinConfig.commitStoreRight
        )

      joiner.join(leftSource, rightSource)
    }

    def joinOneToMany(
        right: Stream[IO, JoinRecord[Y, SourceCommitMetadata]],
        joiner: Fs2OneToManyJoiner[X, Y, SourceCommitMetadata, StoreCommitMetadata],
        joinConfig: JoinConfig[X, Y, SourceCommitMetadata]
    ): Stream[IO, JoinedResult[X, Y, StoreCommitMetadata]] = {

      val leftSource =
        LeftStreamSource[X, SourceCommitMetadata](
          fs2Stream,
          joinConfig.keyLeft,
          joinConfig.commitStoreLeft
        )
      val rightSource =
        LeftStreamSource[Y, SourceCommitMetadata](
          right,
          joinConfig.keyRight,
          joinConfig.commitStoreRight
        )

      joiner.join(leftSource, rightSource)
    }

  }

}
