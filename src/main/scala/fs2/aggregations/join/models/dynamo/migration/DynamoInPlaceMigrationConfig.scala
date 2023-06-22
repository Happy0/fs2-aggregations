package fs2.aggregations.join.models.dynamo.migration

import meteor.codec.Codec
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
case class DynamoInPlaceMigrationConfig[
    OldTypeLeft,
    OldTypeRight,
    NewTypeLeft,
    NewTypeRight
](
    client: DynamoDbAsyncClient,
    tableName: String,
    kafkaNotificationTopic: String,
    oldLeftCodec: Codec[OldTypeLeft],
    oldRightCodec: Codec[OldTypeRight],
    newLeftCodec: Codec[NewTypeLeft],
    newRightCodec: Codec[NewTypeRight],
)
