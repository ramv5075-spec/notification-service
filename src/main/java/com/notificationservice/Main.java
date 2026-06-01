package com.notificationservice;

import com.notificationservice.kafka.*;
import com.notificationservice.model.NotificationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        log.info("=== notification service — phase 2: kafka ===");

        KafkaNotificationProducer producer = new KafkaNotificationProducer();

        KafkaNotificationConsumer consumer1 = new KafkaNotificationConsumer(
            "consumer-1",
            List.of(KafkaConfig.TOPIC_NOTIFICATIONS, KafkaConfig.TOPIC_RETRY),
            producer);

        KafkaNotificationConsumer consumer2 = new KafkaNotificationConsumer(
            "consumer-2",
            List.of(KafkaConfig.TOPIC_NOTIFICATIONS, KafkaConfig.TOPIC_RETRY),
            producer);

        Thread t1 = new Thread(consumer1, "consumer-1");
        Thread t2 = new Thread(consumer2, "consumer-2");
        t1.start();
        t2.start();

        Thread.sleep(2000);

        log.info("--- producing 50 messages to kafka ---");
        for (int i = 0; i < 15; i++)
            producer.send(new NotificationMessage(
                NotificationMessage.Type.EMAIL,
                "user" + i + "@example.com", "Welcome!", "Hello " + i));
        for (int i = 0; i < 15; i++)
            producer.send(new NotificationMessage(
                NotificationMessage.Type.SMS,
                "+1908" + String.format("%07d", i), "Alert", "SMS " + i));
        for (int i = 0; i < 10; i++)
            producer.send(new NotificationMessage(
                NotificationMessage.Type.PUSH,
                "device-" + i, "Notification", "Push " + i));
        for (int i = 0; i < 10; i++)
            producer.send(new NotificationMessage(
                NotificationMessage.Type.WEBHOOK,
                "https://hook.example.com/" + i, "Event", "{\"id\":" + i + "}"));

        producer.flush();
        log.info("--- all 50 messages sent to kafka ---");

        Thread.sleep(5000);

        consumer1.stop();
        consumer2.stop();
        t1.join(2000);
        t2.join(2000);
        producer.close();

        log.info("=== results ===");
        log.info("Producer sent:          {}", producer.getTotalSent());
        log.info("Consumer-1 processed:   {} failed: {} dlq: {}",
            consumer1.getTotalProcessed(), consumer1.getTotalFailed(), consumer1.getTotalDLQ());
        log.info("Consumer-2 processed:   {} failed: {} dlq: {}",
            consumer2.getTotalProcessed(), consumer2.getTotalFailed(), consumer2.getTotalDLQ());
        log.info("=== phase 2 complete ===");
    }
}
