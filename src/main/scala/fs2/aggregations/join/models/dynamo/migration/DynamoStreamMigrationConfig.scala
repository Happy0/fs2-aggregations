package fs2.aggregations.join.models.dynamo.migration

import meteor.codec.Codec
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

class DynamoStreamMigrationConfig[X, Y](
    client: DynamoDbAsyncClient,
    tableName: String,
    leftCodec: Codec[X],
    rightCodec: Codec[Y],
) {}
