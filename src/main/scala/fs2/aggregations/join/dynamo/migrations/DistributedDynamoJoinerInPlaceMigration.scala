package fs2.aggregations.join.dynamo.migrations

import cats.effect.IO
import fs2.aggregations.join.models.dynamo.DynamoRecord
import fs2.aggregations.join.models.dynamo.migration.{
  DynamoInPlaceMigrationConfig,
  DynamoJoinRow,
  DynamoMigrationRow,
  LockRow,
  Migrator,
  MigratorRole,
  UnrecognisedRow,
  Waiter
}
import meteor.{Client, DynamoDbType, Expression, KeyDef}
import meteor.api.hi.CompositeTable
import meteor.errors.ConditionalCheckFailed

import java.time.Duration
import scala.concurrent.duration.DurationInt
import fs2.Stream

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

  val oldTable: CompositeTable[IO, String, String] =
    CompositeTable[IO, String, String](
      config.oldTableName,
      KeyDef[String]("PK", DynamoDbType.S),
      KeyDef[String]("SK", DynamoDbType.S),
      config.client
    )

  val newTable: CompositeTable[IO, String, String] =
    CompositeTable[IO, String, String](
      config.newTableName,
      KeyDef[String]("PK", DynamoDbType.S),
      KeyDef[String]("SK", DynamoDbType.S),
      config.client
    )

  val client: Client[IO] = Client.apply(config.client)

  private def attemptToWriteLock(): IO[Boolean] = {

    for {
      now <- IO.realTimeInstant
      lockRow = LockRow(
        "fs2-aggregations-lock-row",
        "LOCKED",
        now.toEpochMilli,
        None,
        now.plus(Duration.ofDays(1)).toEpochMilli / 1000
      )

      acquiredLock <- newTable
        .put[LockRow](
          lockRow,
          Expression(
            "attribute_not_exists(#id)",
            Map("#id" -> "PK"),
            Map.empty
          )
        )
        .attempt
        .flatMap({
          case Left(x) if x.isInstanceOf[ConditionalCheckFailed] =>
            IO.pure(false)
          case Left(x) => IO.raiseError(new Error("Unexpected error", x))
          case _       => IO.pure(true)
        })

    } yield acquiredLock
  }

  private def markMigrationCompleted(): IO[Unit] = {
    for {
      now <- IO.realTimeInstant
      row <- newTable.get[LockRow]("fs2-aggregations-lock-row", "LOCKED", true)
      updatedRow = row.map(_.copy(finishedAt = Some(now.toEpochMilli)))
      _ <- newTable.put(updatedRow)
    } yield ()
  }

  private def getRole(): IO[MigratorRole] = for {
    lockTaken <- attemptToWriteLock()
  } yield if (lockTaken) Migrator else Waiter

  private def migrationIsFinished(): IO[Boolean] = {
    for {
      row <- newTable.get[LockRow]("fs2-aggregations-lock-row", "LOCKED", true)
      rowContents <- IO.fromOption(row)(new Exception("Missing migration row"))
      finishedAt = rowContents.finishedAt
      _ <- finishedAt match {
        case None       => IO.println("Migration not yet finished")
        case Some(date) => IO.println(s"Migration finished at ${date}")
      }
    } yield { finishedAt.isDefined }

  }

  private def waitForMigrationToFinish(): IO[Unit] = {
    for {
      _ <- IO.sleep(1.minute)
      migrationFinished <- migrationIsFinished()
      _ <- if (!migrationFinished) waitForMigrationToFinish() else IO.unit
    } yield ()
  }

  private def performMigration(
      transformLeft: (
          DynamoRecord[OldTypeLeft]
      ) => IO[DynamoRecord[NewTypeLeft]],
      transformRight: (
          DynamoRecord[OldTypeRight]
      ) => IO[DynamoRecord[NewTypeRight]]
  ): IO[Unit] = {

    val oldRecordDecoder =
      DynamoRecord.eitherDecoder[OldTypeLeft, OldTypeRight](
        config.oldLeftCodec,
        config.oldRightCodec
      )

    implicit val migrationRowDecoder =
      DynamoMigrationRow.getDecoder(config.oldLeftCodec, config.oldRightCodec)

    implicit val newLeftEncoder =
      DynamoRecord.dynamoRecordEncoder(config.newLeftCodec)
    implicit val newRightEncoder =
      DynamoRecord.dynamoRecordEncoder(config.newRightCodec)

    val migrateStream = for {
      _ <- Stream.eval(IO.println("Starting migration"))
      item <-
        client
          .scan[DynamoMigrationRow[OldTypeLeft, OldTypeRight]](
            config.oldTableName,
            true,
            1
          )
          .attempt
          .flatMap({
            case Left(err) => Stream.empty
            case Right(x)  => Stream.emit(x)
          })

      _ <- item match {
        case UnrecognisedRow() => Stream.eval(IO.println("Unrecognised row"))
        case x: DynamoJoinRow[OldTypeLeft, OldTypeRight] =>
          x.row match {
            case Left(leftValue) =>
              Stream.eval(
                transformLeft(leftValue).flatMap(y =>
                  IO.println(s"Putting $y") >> newTable.put(y)(newLeftEncoder)
                )
              )
            case Right(rightValue) =>
              Stream.eval(
                transformRight(rightValue).flatMap(y =>
                  IO.println(s"Putting $y") >> newTable.put(y)(newRightEncoder)
                )
              )
          }
      }

    } yield {}

    migrateStream.compile.drain.void.flatMap(x => markMigrationCompleted())
  }

  def migrateInPlace(
      transformLeft: DynamoRecord[OldTypeLeft] => IO[DynamoRecord[NewTypeLeft]],
      transformRight: DynamoRecord[OldTypeRight] => IO[
        DynamoRecord[NewTypeRight]
      ]
  ): IO[Unit] = for {
    role <- getRole()

    _ <- role match {
      case Waiter   => waitForMigrationToFinish()
      case Migrator => performMigration(transformLeft, transformRight)
    }
  } yield {}

}
