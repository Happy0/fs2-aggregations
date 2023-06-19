package fs2.aggregations.join.models
sealed trait JoinedResult[X, Y, CommitMetadata]

case class JoinedValueResult[X, Y, CommitMetadata](
    value: (X, Y)
) extends JoinedResult[X, Y, CommitMetadata]

case class CommitResult[X, Y, CommitMetadata](
    commitMetadata: CommitMetadata
) extends JoinedResult[X,Y, CommitMetadata]

object JoinedResult {
  def getOffset[X, Y, CommitMetadata](
      joinedResult: JoinedResult[X, Y, CommitMetadata]
  ): Option[CommitMetadata] = {
    joinedResult match {
      case x: JoinedValueResult[X, Y, CommitMetadata]    => None
      case z: CommitResult[X,Y, CommitMetadata] => Some(z.commitMetadata)
    }
  }

}
