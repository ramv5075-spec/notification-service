package com.notificationservice.api;

import com.notificationservice.kafka.KafkaNotificationProducer;
import com.notificationservice.model.NotificationMessage;
import com.notificationservice.websocket.NotificationWebSocketService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class NotificationController {

    private final KafkaNotificationProducer producer;
    private final NotificationWebSocketService wsService;

    public NotificationController(KafkaNotificationProducer producer,
                                   NotificationWebSocketService wsService) {
        this.producer  = producer;
        this.wsService = wsService;
    }

    @PostMapping("/notify")
    public ResponseEntity<Map<String, Object>> send(@RequestBody Map<String, String> body) {
        String type      = body.getOrDefault("type", "EMAIL");
        String recipient = body.get("recipient");
        String subject   = body.get("subject");
        String payload   = body.get("payload");

        if (recipient == null || subject == null || payload == null) {
            return ResponseEntity.badRequest()
                .body(Map.of("success", false, "error", "recipient, subject, payload required"));
        }

        NotificationMessage message = new NotificationMessage(
            NotificationMessage.Type.valueOf(type.toUpperCase()),
            recipient, subject, payload);

        producer.send(message);
        producer.flush();

        return ResponseEntity.ok(Map.of(
            "success",   true,
            "messageId", message.getMessageId(),
            "type",      type,
            "recipient", recipient
        ));
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> stats() {
        return ResponseEntity.ok(Map.of(
            "totalSent",   producer.getTotalSent(),
            "totalPushed", wsService.getTotalPushed()
        ));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }
}
