package fs2.aggregations.join.models.dynamo

import meteor.codec.Codec
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

case class DynamoStoreConfig[X, Y](
                                    client: DynamoDbAsyncClient,
                                    tableName: String,
                                    kafkaNotificationTopic: String,
                                    leftCodec: Codec[X],
                                    rightCodec: Codec[Y]
)
