package fs2.aggregations.join.dynamo.migrations

import cats.effect.IO
import fs2.aggregations.join.models.dynamo.migration.DynamoInPlaceMigrationConfig

class DistributedDynamoJoinerInPlaceMigration[
    OldTypeLeft,
    OldTypeRight,
    NewTypeLeft,
    NewTypeRight
](
    val config: DynamoInPlaceMigrationConfig[
      OldTypeLeft,
      OldTypeRight,
      NewTypeLeft,
      NewTypeRight
    ]
) {

  def migrateInPlace(
      transformLeft: (OldTypeLeft) => IO[NewTypeLeft],
      transformRight: (OldTypeRight) => IO[NewTypeRight]
  ): IO[Unit] = ???

}
