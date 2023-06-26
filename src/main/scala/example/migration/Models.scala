package example.migration

import dynosaur.AttributeValue
import meteor.codec.{Decoder, Encoder}
import meteor.errors
import meteor.syntax._

case class UserV2(userId: String, name: String, displayPicture: String)
case class UserHingV2(userId: String, userName: String, hing: String, displayPicture: String)

object UserV2 {

  implicit val userEncoder: Encoder[UserV2] = new Encoder[UserV2] {
    override def write(a: UserV2): AttributeValue =
      Map("userId" -> a.userId, "name" -> a.name, "displayPicture" -> a.displayPicture).asAttributeValue
  }

  implicit val userDecoder: Decoder[UserV2] = new Decoder[UserV2] {
    override def read(av: AttributeValue): Either[errors.DecoderError, UserV2] =
      for {
        userId <- av.getAs[String]("userId")
        name <- av.getAs[String]("name")
        profilePicture <- av.getAs[String]("displayPicture")
      } yield UserV2(userId, name, profilePicture)
  }
}