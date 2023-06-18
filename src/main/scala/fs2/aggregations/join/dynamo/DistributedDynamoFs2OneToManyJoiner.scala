package fs2.aggregations.join.dynamo

import cats.effect.IO
import fs2.aggregations.join.dynamo.base.DistributedDynamoJoiner
import fs2.aggregations.join.dynamo.clients.Clients
import fs2.aggregations.join.models.{JoinedResult, StreamSource}
import fs2.aggregations.join.Fs2OneToOneJoiner
import fs2.aggregations.join.models.dynamo.DynamoStoreConfig
import fs2.kafka.CommittableOffset

class DistributedDynamoFs2OneToManyJoiner[X, Y, CommitMetadata](
    config: DynamoStoreConfig[X, Y]
) extends Fs2OneToOneJoiner[X, Y, CommitMetadata, CommittableOffset[IO]] {

  private val clients = Clients(config)

  private val distributedDynamoJoiner =
    new DistributedDynamoJoiner[X, Y, CommitMetadata](
      clients.dynamoRecordDB,
      clients.kafkaNotifier
    )
  override def sinkToStore(left: StreamSource[X, CommitMetadata], right: StreamSource[Y, CommitMetadata]): fs2.Stream[IO, Unit] = {

    val rightKey = IO.realTimeInstant.map(_.toEpochMilli)


    ???
  }
  override def streamFromStore(): fs2.Stream[IO, JoinedResult[X, Y, CommittableOffset[IO]]] = ???
}
