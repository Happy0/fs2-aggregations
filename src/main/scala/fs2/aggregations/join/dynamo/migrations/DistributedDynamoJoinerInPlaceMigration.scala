package fs2.aggregations.join.dynamo.migrations

import cats.effect.IO
import fs2.aggregations.join.models.dynamo.migration.DynamoInPlaceMigrationConfig

sealed trait MigratorRole
object Waiter extends MigratorRole
object Migrator extends MigratorRole

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

  private def attemptToWriteLock(): IO[Boolean] = {
    ???
  }

  private def getRole(): IO[MigratorRole] = for {
    lockTaken <- attemptToWriteLock()
  } yield if (lockTaken) Migrator else Waiter

  private def waitForMigrationToFinish(): IO[Unit] = ???

  private def performMigration(
      transformLeft: (OldTypeLeft) => IO[NewTypeLeft],
      transformRight: (OldTypeRight) => IO[NewTypeRight]
  ): IO[Unit] = ???
  
  def migrateInPlace(
      transformLeft: (OldTypeLeft) => IO[NewTypeLeft],
      transformRight: (OldTypeRight) => IO[NewTypeRight]
  ): IO[Unit] = for {
    role <- getRole()
    _ <- role match {
      case Waiter => waitForMigrationToFinish()
      case Migrator => performMigration(transformLeft, transformRight)
    }
  } yield {}

}
