package com.notificationservice.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.notificationservice.model.NotificationMessage;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaNotificationConsumer implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaNotificationConsumer.class);
    private static final int MAX_RETRIES = 3;

    private final String consumerId;
    private final KafkaConsumer<String, String> consumer;
    private final KafkaNotificationProducer producer;
    private final ObjectMapper mapper;
    private final AtomicBoolean running      = new AtomicBoolean(false);
    private final AtomicLong totalProcessed  = new AtomicLong(0);
    private final AtomicLong totalFailed     = new AtomicLong(0);
    private final AtomicLong totalDLQ        = new AtomicLong(0);

    public KafkaNotificationConsumer(String consumerId,
                                      List<String> topics,
                                      KafkaNotificationProducer producer) {
        this.consumerId = consumerId;
        this.producer   = producer;
        this.mapper     = new ObjectMapper().registerModule(new JavaTimeModule());

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           KafkaConfig.CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");   // manual commit for reliability

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(topics);
        log.info("KafkaConsumer [{}] subscribed to {}", consumerId, topics);
    }

    @Override
    public void run() {
        running.set(true);
        log.info("KafkaConsumer [{}] started", consumerId);

        while (running.get()) {
            try {
                ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }

                if (!records.isEmpty()) {
                    consumer.commitSync(); // commit after successful batch
                }
            } catch (Exception e) {
                log.error("Consumer [{}] error: {}", consumerId, e.getMessage());
            }
        }
        log.info("KafkaConsumer [{}] stopped", consumerId);
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        try {
            NotificationMessage message = mapper.readValue(
                record.value(), NotificationMessage.class);

            log.info("Consumer [{}] processing {} from topic={} partition={} offset={}",
                consumerId, message.getMessageId(),
                record.topic(), record.partition(), record.offset());

            deliver(message);
            totalProcessed.incrementAndGet();
            log.info("Consumer [{}] delivered {}", consumerId, message.getMessageId());

        } catch (Exception e) {
            totalFailed.incrementAndGet();
            log.error("Consumer [{}] failed record at offset {}: {}",
                consumerId, record.offset(), e.getMessage());
            handleFailure(record, e);
        }
    }

    private void deliver(NotificationMessage message) throws Exception {
        // Simulate 10% failure for retry testing
        if (Math.random() < 0.1) throw new Exception("Simulated delivery failure");
        Thread.sleep(5);
    }

    private void handleFailure(ConsumerRecord<String, String> record, Exception e) {
        try {
            NotificationMessage message = mapper.readValue(record.value(), NotificationMessage.class);
            message.markFailed();

            if (message.getRetryCount() < MAX_RETRIES) {
                producer.sendToRetry(message);
                log.warn("Sent {} to retry topic — attempt {}", message.getMessageId(), message.getRetryCount());
            } else {
                producer.sendToDLQ(message);
                totalDLQ.incrementAndGet();
                log.error("Sent {} to DLQ after {} retries", message.getMessageId(), message.getRetryCount());
            }
        } catch (Exception ex) {
            log.error("Failed to handle failure for record: {}", ex.getMessage());
        }
    }

    public void stop()                  { running.set(false); }
    public long getTotalProcessed()     { return totalProcessed.get(); }
    public long getTotalFailed()        { return totalFailed.get(); }
    public long getTotalDLQ()           { return totalDLQ.get(); }
    public String getConsumerId()       { return consumerId; }

    @Override
    public void close() { consumer.close(); }
}
