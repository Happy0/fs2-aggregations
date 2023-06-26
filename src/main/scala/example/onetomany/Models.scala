package example.onetomany

import dynosaur.AttributeValue
import meteor.codec.{Decoder, Encoder}
import meteor.errors
import meteor.syntax._

case class User(userId: String, name: String)
case class Hing(userId: String, hingId: String, hing: String)
case class UserHing(userId: String, userName: String, hing: String)

object User {

  implicit val userEncoder: Encoder[User] = new Encoder[User] {
    override def write(a: User): AttributeValue =
      Map("userId" -> a.userId, "name" -> a.name).asAttributeValue
  }

  implicit val userDecoder: Decoder[User] = new Decoder[User] {
    override def read(av: AttributeValue): Either[errors.DecoderError, User] =
      for {
        userId <- av.getAs[String]("userId")
        name <- av.getAs[String]("name")
      } yield User(userId, name)
  }

}

object Hing {
  implicit val userEncoder: Encoder[Hing] = new Encoder[Hing] {
    override def write(a: Hing): AttributeValue =
      Map(
        "userId" -> a.userId,
        "hing" -> a.hing,
        "hingId" -> a.hingId
      ).asAttributeValue
  }

  implicit val userDecoder: Decoder[Hing] = new Decoder[Hing] {
    override def read(av: AttributeValue): Either[errors.DecoderError, Hing] =
      for {
        userId <- av.getAs[String]("userId")
        hing <- av.getAs[String]("hing")
        hingId <- av.getAs[String]("hingId")
      } yield Hing(userId, hingId, hing)
  }
}