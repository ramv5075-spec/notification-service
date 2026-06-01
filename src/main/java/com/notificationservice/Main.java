package com.notificationservice;

import com.notificationservice.consumer.NotificationConsumer;
import com.notificationservice.model.NotificationMessage;
import com.notificationservice.producer.NotificationProducer;
import com.notificationservice.queue.DeadLetterQueue;
import com.notificationservice.queue.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        log.info("=== notification service — phase 1 ===");

        MessageQueue mainQueue  = new MessageQueue("main-queue", 1000);
        MessageQueue retryQueue = new MessageQueue("retry-queue", 1000);
        DeadLetterQueue dlq     = new DeadLetterQueue();

        NotificationProducer producer = new NotificationProducer(mainQueue);

        List<Thread> consumerThreads = new ArrayList<>();
        List<NotificationConsumer> consumers = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            NotificationConsumer consumer = new NotificationConsumer(
                "consumer-" + i, mainQueue, retryQueue, dlq);
            consumers.add(consumer);
            Thread t = new Thread(consumer, "consumer-" + i);
            consumerThreads.add(t);
            t.start();
        }

        // retry consumer
        NotificationConsumer retryConsumer = new NotificationConsumer(
            "retry-consumer", retryQueue, retryQueue, dlq);
        consumers.add(retryConsumer);
        Thread retryThread = new Thread(retryConsumer, "retry-consumer");
        consumerThreads.add(retryThread);
        retryThread.start();

        log.info("--- producing 100 messages ---");
        for (int i = 0; i < 25; i++) {
            producer.send(NotificationMessage.Type.EMAIL,
                "user" + i + "@example.com", "Welcome!", "Hello user " + i);
        }
        for (int i = 0; i < 25; i++) {
            producer.send(NotificationMessage.Type.SMS,
                "+1908" + String.format("%07d", i), "Alert", "SMS message " + i);
        }
        for (int i = 0; i < 25; i++) {
            producer.send(NotificationMessage.Type.PUSH,
                "device-token-" + i, "Notification", "Push message " + i);
        }
        for (int i = 0; i < 25; i++) {
            producer.send(NotificationMessage.Type.WEBHOOK,
                "https://webhook.example.com/" + i, "Event", "{\"event\":\"test\"}");
        }

        Thread.sleep(3000);

        consumers.forEach(NotificationConsumer::stop);
        consumerThreads.forEach(t -> {
            try { t.join(1000); } catch (InterruptedException ignored) {}
        });

        log.info("=== results ===");
        log.info("Total produced:   {}", producer.getTotalProduced());
        log.info("Main queue stats: {}", mainQueue.getStats());
        log.info("Retry queue stats:{}", retryQueue.getStats());
        log.info("DLQ size:         {}", dlq.size());
        consumers.forEach(c ->
            log.info("Consumer [{}] processed={} failed={}",
                c.getConsumerId(), c.getTotalProcessed(), c.getTotalFailed()));
        log.info("=== phase 1 complete ===");
    }
}
