package fs2.aggregations.join.models
sealed trait JoinedResult[X, Y, CommitMetadata]
case class PartialJoinedResult[X, Y, CommitMetadata](
    value: (Option[X], Option[Y]),
    commitMetadata: CommitMetadata
) extends JoinedResult[X, Y, CommitMetadata]

case class FullJoinedResult[X, Y, CommitMetadata](
    value: (X, Y),
    commitMetadata: CommitMetadata
) extends JoinedResult[X, Y, CommitMetadata]

object JoinedResult {

  def getOffset[X, Y, CommitMetadata](
      joinedResult: JoinedResult[X, Y, CommitMetadata]
  ): CommitMetadata = {
    joinedResult match {
      case x: FullJoinedResult[X, Y, CommitMetadata]    => x.commitMetadata
      case y: PartialJoinedResult[X, Y, CommitMetadata] => y.commitMetadata
    }
  }

}
