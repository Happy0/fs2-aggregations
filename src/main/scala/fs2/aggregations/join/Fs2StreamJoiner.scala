package fs2.aggregations.join

import cats.effect.IO
import fs2.Stream
import fs2.aggregations.join.models.{JoinRecord, StreamSource}

case class JoinedResult[X, Y, CommitMetadata](
    value: (X, Y),
    commitMetadata: CommitMetadata
)

trait Fs2StreamJoiner[X, Y, SourceCommitMetadata, StoreCommitMetadata] {

  def sinkToStore(
      left: StreamSource[X, SourceCommitMetadata],
      right: StreamSource[Y, SourceCommitMetadata]
  ): Stream[IO, Unit]
  def streamFromStore(): Stream[IO, JoinedResult[X, Y, StoreCommitMetadata]]

  def join(
      left: StreamSource[X, SourceCommitMetadata],
      right: StreamSource[Y, SourceCommitMetadata]
  ): Stream[IO, JoinedResult[X, Y, StoreCommitMetadata]] = {

    val sink = sinkToStore(left, right)
    val source = streamFromStore()

    source concurrently sink
  }
}
trait Fs2OneToOneJoiner[X, Y, CommitMetadata, StoreCommitMetadata]
    extends Fs2StreamJoiner[X, Y, CommitMetadata, StoreCommitMetadata]

trait Fs2OneToManyJoiner[X, Y, CommitMetadata, StoreCommitMetadata]
    extends Fs2StreamJoiner[X, Y, CommitMetadata, StoreCommitMetadata]
