package com.github.sco1237896.connector.it.aws

import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse
import software.amazon.awssdk.services.kinesis.model.Shard

final class ConnectorSupport {
    static String getShardIterator(KinesisClient kinesis, String topic) {
        def lastShardId = null

        DescribeStreamResponse describeStreamResponse;

        do {
            describeStreamResponse = kinesis.describeStream(b -> b.streamName(topic))
            List<Shard> shards = describeStreamResponse.streamDescription().shards();

            if (shards != null && shards.size() > 0) {
                lastShardId = shards.get(shards.size() - 1).shardId();
            }
        } while (describeStreamResponse.streamDescription().hasMoreShards());

        return kinesis
                .getShardIterator(b -> b.streamName(topic).shardIteratorType("TRIM_HORIZON").shardId(lastShardId))
                .shardIterator()
    }

    static void createStream(KinesisClient kinesis, String topic) {
        kinesis.createStream(b -> b.streamName(topic).shardCount(1))

        String status
        do {
            status = kinesis.describeStream(b -> b.streamName(topic)).streamDescription().streamStatus()
        } while (status != "ACTIVE");
    }
}
