package fs2.aggregations.join.extensions

import cats.effect.IO
import fs2.aggregations.join.models.{
  JoinRecord,
  JoinedResult,
  OneToOneJoinConfig
}
import fs2.Stream
import fs2.aggregations.join.Fs2StreamJoiner
import fs2.aggregations.join.models._
class Fs2ParallelStreamJoinerExtensions {
  implicit class FS2StreamParallelJoinMethods[
      X,
      Y,
      SourceCommitMetadata,
      StoreCommitMetadata
  ](fs2Stream: Stream[IO, Stream[IO, JoinRecord[X, SourceCommitMetadata]]]) {

    def joinOneToOne(
        right: Stream[IO, Stream[IO, JoinRecord[Y, SourceCommitMetadata]]],
        joiner: Fs2StreamJoiner[
          X,
          Y,
          SourceCommitMetadata,
          StoreCommitMetadata
        ],
        joinConfig: OneToOneJoinConfig[X, Y, SourceCommitMetadata]
    ): Stream[IO, JoinedResult[X, Y, StoreCommitMetadata]] = {
      val leftSource = fs2Stream.map(x =>
        LeftStreamSource[X, SourceCommitMetadata](
          x,
          joinConfig.joinKeyLeft,
          joinConfig.commitStoreLeft
        )
      )

      val rightSource = right.map(x =>
        RightStreamSource[Y, SourceCommitMetadata](
          x,
          joinConfig.joinKeyRight,
          (y: Y) => "RIGHT",
          joinConfig.commitStoreRight
        )
      )

      joiner.joinParallel(leftSource, rightSource)
    }
    def joinOneToMany(
        right: Stream[IO, Stream[IO, JoinRecord[Y, SourceCommitMetadata]]],
        joiner: Fs2StreamJoiner[
          X,
          Y,
          SourceCommitMetadata,
          StoreCommitMetadata
        ],
        joinConfig: OneToManyJoinConfig[X, Y, SourceCommitMetadata]
    ): Stream[IO, JoinedResult[X, Y, StoreCommitMetadata]] = {

      val leftSource = fs2Stream.map(x =>
        LeftStreamSource[X, SourceCommitMetadata](
          x,
          joinConfig.joinKeyLeft,
          joinConfig.commitStoreLeft
        )
      )

      val rightSource = right.map(x =>
        RightStreamSource[Y, SourceCommitMetadata](
          x,
          joinConfig.joinKeyRight,
          joinConfig.sortKeyRight,
          joinConfig.commitStoreRight
        )
      )

      joiner.joinParallel(leftSource, rightSource)
    }

  }
}
