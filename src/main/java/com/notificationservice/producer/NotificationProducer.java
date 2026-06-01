package com.notificationservice.producer;

import com.notificationservice.model.NotificationMessage;
import com.notificationservice.queue.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class NotificationProducer {

    private static final Logger log = LoggerFactory.getLogger(NotificationProducer.class);

    private final MessageQueue queue;
    private final AtomicLong totalProduced = new AtomicLong(0);

    public NotificationProducer(MessageQueue queue) {
        this.queue = queue;
        log.info("NotificationProducer initialized for queue [{}]", queue.getQueueName());
    }

    public boolean send(NotificationMessage.Type type,
                        String recipient,
                        String subject,
                        String payload) {
        NotificationMessage message = new NotificationMessage(type, recipient, subject, payload);
        boolean enqueued = queue.enqueue(message);
        if (enqueued) {
            totalProduced.incrementAndGet();
            log.info("Produced {} type={} recipient={}", message.getMessageId(), type, recipient);
        } else {
            log.error("Failed to produce message — queue full");
        }
        return enqueued;
    }

    public boolean send(NotificationMessage message) {
        boolean enqueued = queue.enqueue(message);
        if (enqueued) totalProduced.incrementAndGet();
        return enqueued;
    }

    public long getTotalProduced() { return totalProduced.get(); }
}
