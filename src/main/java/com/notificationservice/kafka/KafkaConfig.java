package com.notificationservice.kafka;

public class KafkaConfig {
    public static final String BOOTSTRAP_SERVERS     = "localhost:9092";
    public static final String TOPIC_NOTIFICATIONS   = "notifications";
    public static final String TOPIC_RETRY           = "notifications.retry";
    public static final String TOPIC_DLQ             = "notifications.dlq";
    public static final String CONSUMER_GROUP        = "notification-service";
    public static final int    PARTITIONS            = 3;
    public static final short  REPLICATION_FACTOR    = 1;
}
