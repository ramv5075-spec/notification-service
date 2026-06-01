package com.notificationservice.queue;

import com.notificationservice.model.NotificationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class DeadLetterQueue {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueue.class);

    private final CopyOnWriteArrayList<NotificationMessage> dlq = new CopyOnWriteArrayList<>();
    private final AtomicLong totalDead = new AtomicLong(0);

    public void add(NotificationMessage message) {
        message.markDead();
        dlq.add(message);
        totalDead.incrementAndGet();
        log.warn("Message {} moved to DLQ after {} retries — type={} recipient={}",
            message.getMessageId(), message.getRetryCount(),
            message.getType(), message.getRecipient());
    }

    public List<NotificationMessage> getAll() {
        return Collections.unmodifiableList(new ArrayList<>(dlq));
    }

    public int size()            { return dlq.size(); }
    public long getTotalDead()   { return totalDead.get(); }
}
