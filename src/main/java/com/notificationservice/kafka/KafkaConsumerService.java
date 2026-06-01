package com.notificationservice.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.notificationservice.model.NotificationMessage;
import com.notificationservice.websocket.NotificationWebSocketService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);
    private static final int MAX_RETRIES = 3;

    private final KafkaNotificationProducer producer;
    private final NotificationWebSocketService wsService;
    private final ObjectMapper mapper;
    private final ExecutorService executor = Executors.newFixedThreadPool(3);
    private final AtomicBoolean running    = new AtomicBoolean(false);
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong totalFailed    = new AtomicLong(0);
    private final AtomicLong totalDLQ       = new AtomicLong(0);

    public KafkaConsumerService(KafkaNotificationProducer producer,
                                 NotificationWebSocketService wsService) {
        this.producer  = producer;
        this.wsService = wsService;
        this.mapper    = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @PostConstruct
    public void start() {
        running.set(true);
        for (int i = 1; i <= 3; i++) {
            final String consumerId = "consumer-" + i;
            executor.submit(() -> runConsumer(consumerId));
        }
        log.info("KafkaConsumerService started with 3 consumers");
    }

    private void runConsumer(String consumerId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 KafkaConfig.CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(
                KafkaConfig.TOPIC_NOTIFICATIONS,
                KafkaConfig.TOPIC_RETRY));

            log.info("Consumer [{}] started", consumerId);

            while (running.get()) {
                ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    processRecord(consumerId, record);
                }

                if (!records.isEmpty()) consumer.commitSync();
            }
        } catch (Exception e) {
            log.error("Consumer [{}] error: {}", consumerId, e.getMessage());
        }
    }

    private void processRecord(String consumerId, ConsumerRecord<String, String> record) {
        try {
            NotificationMessage message = mapper.readValue(
                record.value(), NotificationMessage.class);

            log.info("[{}] processing {} type={} recipient={}",
                consumerId, message.getMessageId(), message.getType(), message.getRecipient());

            deliver(message);
            message.markDelivered();
            totalProcessed.incrementAndGet();

            // Push to WebSocket clients
            wsService.pushToClients(message);
            log.info("[{}] delivered and pushed {} to WebSocket", consumerId, message.getMessageId());

        } catch (Exception e) {
            totalFailed.incrementAndGet();
            handleFailure(consumerId, record, e);
        }
    }

    private void deliver(NotificationMessage message) throws Exception {
        if (Math.random() < 0.05) throw new Exception("Simulated delivery failure");
        Thread.sleep(5);
    }

    private void handleFailure(String consumerId,
                                ConsumerRecord<String, String> record, Exception e) {
        try {
            NotificationMessage message = mapper.readValue(
                record.value(), NotificationMessage.class);
            message.markFailed();

            if (message.getRetryCount() < MAX_RETRIES) {
                producer.sendToRetry(message);
                log.warn("[{}] sent {} to retry #{}", consumerId,
                    message.getMessageId(), message.getRetryCount());
            } else {
                producer.sendToDLQ(message);
                totalDLQ.incrementAndGet();
                wsService.pushAlert("Message " + message.getMessageId() + " moved to DLQ");
                log.error("[{}] sent {} to DLQ", consumerId, message.getMessageId());
            }
        } catch (Exception ex) {
            log.error("Failed to handle failure: {}", ex.getMessage());
        }
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        executor.shutdown();
        log.info("KafkaConsumerService stopped — processed={} failed={} dlq={}",
            totalProcessed, totalFailed, totalDLQ);
    }
}
