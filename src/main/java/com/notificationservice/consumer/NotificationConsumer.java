package com.notificationservice.consumer;

import com.notificationservice.model.NotificationMessage;
import com.notificationservice.queue.DeadLetterQueue;
import com.notificationservice.queue.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;

public class NotificationConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);
    private static final int MAX_RETRIES  = 3;
    private static final long POLL_TIMEOUT = 100;

    private final String consumerId;
    private final MessageQueue queue;
    private final MessageQueue retryQueue;
    private final DeadLetterQueue dlq;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong totalFailed    = new AtomicLong(0);

    public NotificationConsumer(String consumerId,
                                 MessageQueue queue,
                                 MessageQueue retryQueue,
                                 DeadLetterQueue dlq) {
        this.consumerId  = consumerId;
        this.queue       = queue;
        this.retryQueue  = retryQueue;
        this.dlq         = dlq;
    }

    @Override
    public void run() {
        running.set(true);
        log.info("Consumer [{}] started", consumerId);

        while (running.get()) {
            try {
                NotificationMessage message = queue.dequeue(POLL_TIMEOUT);
                if (message != null) {
                    process(message);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        log.info("Consumer [{}] stopped", consumerId);
    }

    private void process(NotificationMessage message) {
        message.markProcessing();
        log.info("Consumer [{}] processing {} type={} attempt={}",
            consumerId, message.getMessageId(), message.getType(), message.getRetryCount() + 1);

        try {
            deliver(message);
            message.markDelivered();
            totalProcessed.incrementAndGet();
            log.info("Consumer [{}] delivered {} to {}",
                consumerId, message.getMessageId(), message.getRecipient());
        } catch (Exception e) {
            message.markFailed();
            totalFailed.incrementAndGet();
            log.warn("Consumer [{}] failed {} — retries={} error={}",
                consumerId, message.getMessageId(), message.getRetryCount(), e.getMessage());

            if (message.getRetryCount() < MAX_RETRIES) {
                retryQueue.enqueue(message);
                log.info("Message {} queued for retry #{}", message.getMessageId(), message.getRetryCount());
            } else {
                dlq.add(message);
                log.error("Message {} exhausted retries — moved to DLQ", message.getMessageId());
            }
        }
    }

    private void deliver(NotificationMessage message) throws Exception {
        // Simulate delivery — in Phase 3 this becomes WebSocket push
        // For now: simulate 10% failure rate for testing retry logic
        if (Math.random() < 0.1) {
            throw new Exception("Simulated delivery failure");
        }
        Thread.sleep(5); // simulate network latency
    }

    public void stop()                  { running.set(false); }
    public boolean isRunning()          { return running.get(); }
    public long getTotalProcessed()     { return totalProcessed.get(); }
    public long getTotalFailed()        { return totalFailed.get(); }
    public String getConsumerId()       { return consumerId; }
}
