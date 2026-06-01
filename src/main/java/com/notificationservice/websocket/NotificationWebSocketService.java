package com.notificationservice.websocket;

import com.notificationservice.model.NotificationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

@Service
public class NotificationWebSocketService {

    private static final Logger log = LoggerFactory.getLogger(NotificationWebSocketService.class);

    private final SimpMessagingTemplate messagingTemplate;
    private final AtomicLong totalPushed = new AtomicLong(0);

    public NotificationWebSocketService(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    public void pushToClients(NotificationMessage message) {
        messagingTemplate.convertAndSend("/topic/notifications", message);
        totalPushed.incrementAndGet();
        log.debug("Pushed {} to WebSocket clients", message.getMessageId());
    }

    public void pushAlert(String alert) {
        messagingTemplate.convertAndSend("/topic/alerts", alert);
        log.info("Alert pushed: {}", alert);
    }

    public long getTotalPushed() { return totalPushed.get(); }
}
