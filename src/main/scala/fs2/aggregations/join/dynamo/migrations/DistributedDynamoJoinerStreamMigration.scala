package fs2.aggregations.join.dynamo.migrations

import cats.effect.IO
import fs2.aggregations.join.models.dynamo.migration.DynamoStreamMigrationConfig
import fs2.aggregations.join.models.{LeftStreamSource, RightStreamSource}

class DistributedDynamoJoinerStreamMigration[X, Y, CommitMetadata](
    config: DynamoStreamMigrationConfig[X, Y]
) {
  def overwriteFromStream(
      left: LeftStreamSource[X, CommitMetadata],
      right: RightStreamSource[Y, CommitMetadata]
  ): IO[Unit] = ???

}
