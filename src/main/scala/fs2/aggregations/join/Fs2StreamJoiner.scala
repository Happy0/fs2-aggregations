package fs2.aggregations.join

import cats.effect.IO
import fs2.Stream
import fs2.aggregations.join.models.{JoinRecord, StreamSource}

trait Fs2StreamJoiner[X, Y, CommitMetadata] {

  def sinkToStore(
      left: StreamSource[X, CommitMetadata],
      right: StreamSource[Y, CommitMetadata]
  ): Stream[IO, Unit]

  def streamFromStore(): Stream[IO, (X, Y)]

  def join(
      left: StreamSource[X, CommitMetadata],
      right: StreamSource[Y, CommitMetadata]
  ): Stream[IO, (X, Y)] = {

    val sink = sinkToStore(left, right)
    val source = streamFromStore()

    source concurrently sink
  }
}

trait Fs2OneToOneJoiner[X, Y, CommitMetadata] extends Fs2StreamJoiner[X, Y, CommitMetadata]

trait Fs2OneToManyJoiner[X, Y, CommitMetadata] extends Fs2StreamJoiner[X, Y, CommitMetadata]
