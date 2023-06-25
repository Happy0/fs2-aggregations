package fs2.aggregations.join.models.dynamo.migration
sealed trait MigratorRole
object Waiter extends MigratorRole
object Migrator extends MigratorRole
