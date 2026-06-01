package com.notificationservice.queue;

import com.notificationservice.model.NotificationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MessageQueue {

    private static final Logger log = LoggerFactory.getLogger(MessageQueue.class);

    private final String queueName;
    private final int capacity;
    private final BlockingQueue<NotificationMessage> queue;
    private final AtomicLong totalEnqueued  = new AtomicLong(0);
    private final AtomicLong totalDequeued  = new AtomicLong(0);
    private final AtomicLong totalDropped   = new AtomicLong(0);

    public MessageQueue(String queueName, int capacity) {
        this.queueName = queueName;
        this.capacity  = capacity;
        this.queue     = new LinkedBlockingQueue<>(capacity);
        log.info("Queue [{}] initialized with capacity {}", queueName, capacity);
    }

    public boolean enqueue(NotificationMessage message) {
        boolean added = queue.offer(message);
        if (added) {
            totalEnqueued.incrementAndGet();
            log.debug("Enqueued {} to [{}] size={}", message.getMessageId(), queueName, queue.size());
        } else {
            totalDropped.incrementAndGet();
            log.warn("Queue [{}] is full! Dropped message {}", queueName, message.getMessageId());
        }
        return added;
    }

    public NotificationMessage dequeue(long timeoutMs) throws InterruptedException {
        NotificationMessage message = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (message != null) {
            totalDequeued.incrementAndGet();
            log.debug("Dequeued {} from [{}] size={}", message.getMessageId(), queueName, queue.size());
        }
        return message;
    }

    public int size()              { return queue.size(); }
    public boolean isEmpty()       { return queue.isEmpty(); }
    public String getQueueName()   { return queueName; }
    public long getTotalEnqueued() { return totalEnqueued.get(); }
    public long getTotalDequeued() { return totalDequeued.get(); }
    public long getTotalDropped()  { return totalDropped.get(); }

    public QueueStats getStats() {
        return new QueueStats(queueName, queue.size(), capacity,
            totalEnqueued.get(), totalDequeued.get(), totalDropped.get());
    }

    public record QueueStats(
        String queueName,
        int currentSize,
        int capacity,
        long totalEnqueued,
        long totalDequeued,
        long totalDropped
    ) {}
}
