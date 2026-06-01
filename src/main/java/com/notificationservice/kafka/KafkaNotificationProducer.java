package com.notificationservice.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.notificationservice.model.NotificationMessage;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaNotificationProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaNotificationProducer.class);

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;
    private final AtomicLong totalSent   = new AtomicLong(0);
    private final AtomicLong totalFailed = new AtomicLong(0);

    public KafkaNotificationProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,                   "all");  // strongest durability
        props.put(ProducerConfig.RETRIES_CONFIG,                3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,     true);   // exactly-once producer

        this.producer = new KafkaProducer<>(props);
        this.mapper   = new ObjectMapper().registerModule(new JavaTimeModule());
        log.info("KafkaNotificationProducer initialized");
    }

    public void send(NotificationMessage message) {
        send(KafkaConfig.TOPIC_NOTIFICATIONS, message);
    }

    public void sendToRetry(NotificationMessage message) {
        send(KafkaConfig.TOPIC_RETRY, message);
    }

    public void sendToDLQ(NotificationMessage message) {
        send(KafkaConfig.TOPIC_DLQ, message);
    }

    private void send(String topic, NotificationMessage message) {
        try {
            String json = mapper.writeValueAsString(message);
            ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, message.getMessageId(), json);

            producer.send(record, (metadata, ex) -> {
                if (ex != null) {
                    totalFailed.incrementAndGet();
                    log.error("Failed to send {} to topic {}: {}",
                        message.getMessageId(), topic, ex.getMessage());
                } else {
                    totalSent.incrementAndGet();
                    log.debug("Sent {} to topic={} partition={} offset={}",
                        message.getMessageId(), metadata.topic(),
                        metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            totalFailed.incrementAndGet();
            log.error("Serialization error for {}: {}", message.getMessageId(), e.getMessage());
        }
    }

    public void flush()                { producer.flush(); }
    public long getTotalSent()         { return totalSent.get(); }
    public long getTotalFailed()       { return totalFailed.get(); }

    @Override
    public void close() {
        producer.flush();
        producer.close();
        log.info("KafkaNotificationProducer closed — sent={} failed={}", totalSent, totalFailed);
    }
}
