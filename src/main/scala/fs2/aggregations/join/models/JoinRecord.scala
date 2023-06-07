package fs2.aggregations.join.models

import cats.effect.IO
case class JoinRecord[X,Y](record: X, commitMetadata: Y)
