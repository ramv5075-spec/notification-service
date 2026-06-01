package com.notificationservice.queue;

import com.notificationservice.model.NotificationMessage;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

class MessageQueueTest {

    @Test
    void enqueueAndDequeue() throws Exception {
        MessageQueue queue = new MessageQueue("test", 10);
        NotificationMessage msg = new NotificationMessage(
            NotificationMessage.Type.EMAIL, "test@test.com", "Subject", "Body");
        queue.enqueue(msg);
        NotificationMessage result = queue.dequeue(100);
        assertNotNull(result);
        assertEquals(msg.getMessageId(), result.getMessageId());
    }

    @Test
    void queueFullDropsMessage() {
        MessageQueue queue = new MessageQueue("test", 2);
        queue.enqueue(new NotificationMessage(NotificationMessage.Type.EMAIL, "a@a.com", "s", "p"));
        queue.enqueue(new NotificationMessage(NotificationMessage.Type.EMAIL, "b@b.com", "s", "p"));
        boolean added = queue.enqueue(new NotificationMessage(NotificationMessage.Type.EMAIL, "c@c.com", "s", "p"));
        assertFalse(added);
        assertEquals(1, queue.getTotalDropped());
    }

    @Test
    void statsAreAccurate() throws Exception {
        MessageQueue queue = new MessageQueue("test", 10);
        for (int i = 0; i < 5; i++)
            queue.enqueue(new NotificationMessage(NotificationMessage.Type.SMS, "r", "s", "p"));
        for (int i = 0; i < 3; i++)
            queue.dequeue(100);
        assertEquals(5, queue.getTotalEnqueued());
        assertEquals(3, queue.getTotalDequeued());
        assertEquals(2, queue.size());
    }

    @Test
    void deadLetterQueueTracksMessages() {
        DeadLetterQueue dlq = new DeadLetterQueue();
        NotificationMessage msg = new NotificationMessage(
            NotificationMessage.Type.PUSH, "device", "s", "p");
        dlq.add(msg);
        assertEquals(1, dlq.size());
        assertEquals(NotificationMessage.Status.DEAD, dlq.getAll().get(0).getStatus());
    }

    @Test
    void messageRetryCountIncrements() {
        NotificationMessage msg = new NotificationMessage(
            NotificationMessage.Type.WEBHOOK, "url", "s", "p");
        assertEquals(0, msg.getRetryCount());
        msg.markFailed();
        assertEquals(1, msg.getRetryCount());
        msg.markFailed();
        assertEquals(2, msg.getRetryCount());
    }
}
