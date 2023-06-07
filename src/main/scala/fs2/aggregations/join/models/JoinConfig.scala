package fs2.aggregations.join.models

import cats.effect.IO
case class JoinConfig[X, Y](
    keyLeft: (X) => String,
    keyRight: (Y) => String,
    onStoreLeft: (X => IO[Unit]),
    onStoreRight: (Y => IO[Unit])
)

object JoinConfig {
  def apply[X, Y](
      keyLeft: (X) => String,
      keyRight: (Y) => String
  ): JoinConfig[X, Y] =
    JoinConfig(keyLeft, keyRight, (x: X) => IO.unit, (y: Y) => IO.unit)

  def apply[X, Y](
      keyLeft: (X) => String,
      keyRight: (Y) => String,
      onStoreLeft: (X => IO[Unit]),
      onStoreRight: (Y => IO[Unit])
  ): JoinConfig[X, Y] =
    JoinConfig(
      keyLeft,
      keyRight,
      onStoreLeft,
      onStoreRight
    )
}
