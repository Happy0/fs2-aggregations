package fs2.aggregations.join

import cats.effect.IO
import fs2.Stream
import fs2.aggregations.join.models.{JoinRecord, JoinedResult, LeftStreamSource, RightStreamSource}

trait Fs2StreamJoiner[X, Y, SourceCommitMetadata, StoreCommitMetadata] {
  def sinkToStore(
                   left: LeftStreamSource[X, SourceCommitMetadata],
                   right: RightStreamSource[Y, SourceCommitMetadata]
  ): Stream[IO, Unit]
  def streamFromStore(): Stream[IO, JoinedResult[X, Y, StoreCommitMetadata]]

  def join(
            left: LeftStreamSource[X, SourceCommitMetadata],
            right: RightStreamSource[Y, SourceCommitMetadata]
  ): Stream[IO, JoinedResult[X, Y, StoreCommitMetadata]] = {

    val sink = sinkToStore(left, right)
    val source = streamFromStore()

    source concurrently sink
  }
}
