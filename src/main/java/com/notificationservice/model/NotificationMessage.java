package com.notificationservice.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.UUID;

public class NotificationMessage {

    public enum Status { PENDING, PROCESSING, DELIVERED, FAILED, DEAD }
    public enum Type   { EMAIL, SMS, PUSH, WEBHOOK }

    private final String messageId;
    private final Type type;
    private final String recipient;
    private final String subject;
    private final String payload;
    private final Instant createdAt;
    private Status status;
    private int retryCount;
    private Instant lastAttemptAt;

    public NotificationMessage(Type type, String recipient, String subject, String payload) {
        this.messageId     = UUID.randomUUID().toString();
        this.type          = type;
        this.recipient     = recipient;
        this.subject       = subject;
        this.payload       = payload;
        this.createdAt     = Instant.now();
        this.status        = Status.PENDING;
        this.retryCount    = 0;
        this.lastAttemptAt = null;
    }

    @JsonCreator
    public NotificationMessage(
            @JsonProperty("messageId")     String messageId,
            @JsonProperty("type")          Type type,
            @JsonProperty("recipient")     String recipient,
            @JsonProperty("subject")       String subject,
            @JsonProperty("payload")       String payload,
            @JsonProperty("createdAt")     Instant createdAt,
            @JsonProperty("status")        Status status,
            @JsonProperty("retryCount")    int retryCount,
            @JsonProperty("lastAttemptAt") Instant lastAttemptAt) {
        this.messageId     = messageId;
        this.type          = type;
        this.recipient     = recipient;
        this.subject       = subject;
        this.payload       = payload;
        this.createdAt     = createdAt;
        this.status        = status;
        this.retryCount    = retryCount;
        this.lastAttemptAt = lastAttemptAt;
    }

    public void markProcessing() { this.status = Status.PROCESSING; this.lastAttemptAt = Instant.now(); }
    public void markDelivered()  { this.status = Status.DELIVERED; }
    public void markFailed()     { this.retryCount++; this.status = Status.FAILED; }
    public void markDead()       { this.status = Status.DEAD; }

    public String getMessageId()      { return messageId; }
    public Type getType()             { return type; }
    public String getRecipient()      { return recipient; }
    public String getSubject()        { return subject; }
    public String getPayload()        { return payload; }
    public Instant getCreatedAt()     { return createdAt; }
    public Status getStatus()         { return status; }
    public int getRetryCount()        { return retryCount; }
    public Instant getLastAttemptAt() { return lastAttemptAt; }

    @Override
    public String toString() {
        return String.format("Message[id=%s type=%s recipient=%s status=%s retries=%d]",
            messageId, type, recipient, status, retryCount);
    }
}
