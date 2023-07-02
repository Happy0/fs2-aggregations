package fs2.aggregations.join.extensions.dynamo

import cats.effect.IO
import fs2.aggregations.join.models.dynamo.StreamWithCommitPipe
import fs2.{Pipe, Stream}
import fs2.aggregations.join.models.{CommitResult, JoinedResult, JoinedValueResult}
import fs2.kafka.CommittableOffset

object DistributedDynamoJoinerExtensions {

  implicit class WithCommitPipeMethod[X, Y](
      stream: Stream[
        IO,
        JoinedResult[X, Y, CommittableOffset[IO]]
      ]
  ) {

    def withCommitPipe(
        commitBatch: Pipe[IO, CommittableOffset[IO], Unit]
    ): StreamWithCommitPipe[X, Y] = StreamWithCommitPipe[X, Y](stream, commitBatch)

  }

  implicit class DistributedDynamoJoinerMethods[X, Y](
      streamWithCommitPipe: StreamWithCommitPipe[X,Y]
  ) {
    def processJoin(
        transform: Pipe[IO, (X, Y), Unit],
    ): Stream[IO, Unit] = {

      val x: Stream[IO, CommittableOffset[IO]] = streamWithCommitPipe.stream
        .flatMap({
          case joinedValueResult: JoinedValueResult[X, Y, CommittableOffset[
                IO
              ]] => {
            val result: Stream[IO, CommittableOffset[IO]] = Stream
              .emit(joinedValueResult.value)
              .through(transform)
              .flatMap(_ => Stream.empty)

            result
          }
          case y: CommitResult[X, Y, CommittableOffset[IO]] =>
            Stream.emit[IO, CommittableOffset[IO]](y.commitMetadata).map(x => x)
        })

      x.through(streamWithCommitPipe.commitBatch)
    }

  }

}
