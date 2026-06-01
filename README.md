# Real-Time Notification Service

A distributed notification service built from scratch in Java, implementing the core concepts behind Uber's notification infrastructure and Slack's real-time delivery.

## Architecture
Browser → REST API → Kafka → Consumer → WebSocket → Browser
↓
Retry Topic (failed messages)
↓
Dead Letter Queue (exhausted retries)

## Features

- **Kafka-backed message queue** — messages persisted to disk, survive crashes
- **At-least-once delivery** — manual offset commit ensures nothing is lost
- **Automatic retry** — failed messages retry up to 3 times with backoff
- **Dead Letter Queue** — exhausted messages tracked, never silently dropped
- **WebSocket real-time delivery** — browser receives notifications instantly via STOMP
- **4 notification types** — EMAIL, SMS, PUSH, WEBHOOK
- **Spring Boot REST API** — send notifications via POST /api/notify
- **React TypeScript dashboard** — live message feed, charts, stats

## Benchmark Results

| Test | Throughput | p50 | p95 | p99 |
|------|-----------|-----|-----|-----|
| EMAIL burst (10 threads) | 1,082 msg/sec | 7ms | 18ms | 46ms |
| Mixed types (20 threads) | 2,732 msg/sec | 6ms | 12ms | 13ms |
| High concurrency (50 threads) | 6,250 msg/sec | 6ms | 17ms | 19ms |

**Peak: 6,250 msg/sec · p99 under 19ms · 100% success rate**

Tested with 2,000 total messages across EMAIL, SMS, PUSH, WEBHOOK types on MacBook Air M1.

## Project Structure
src/main/java/com/notificationservice/
├── model/          Phase 1 — NotificationMessage (status, retry, DLQ)
├── queue/          Phase 1 — In-memory queue + DeadLetterQueue
├── producer/       Phase 1 — NotificationProducer
├── consumer/       Phase 1 — NotificationConsumer with retry logic
├── kafka/          Phase 2 — Kafka producer, consumer, config
├── websocket/      Phase 3 — WebSocket config + push service
├── api/            Phase 3 — REST API controller
└── benchmark/      Phase 4 — Load tester

## Running Locally

**Start Kafka:**
```bash
docker-compose up -d
```

**Start backend:**
```bash
mvn spring-boot:run
```

**Start dashboard:**
```bash
cd notification-dashboard
npm install
npm start
```

**Run load tests:**
```bash
mvn exec:java -Dexec.mainClass="com.notificationservice.benchmark.NotificationLoadTester"
```

## API
POST /api/notify   body: {type, recipient, subject, payload}
GET  /api/stats    returns total sent and pushed
GET  /api/health   health check
WS   /ws           STOMP WebSocket endpoint
/topic/notifications  → delivered messages
/topic/alerts         → DLQ alerts

## Key Design Decisions

**Why Kafka over in-memory queue?**
In-memory queues lose all messages on crash. Kafka persists to disk — messages survive restarts and can be replayed.

**Why manual offset commit?**
Auto-commit can mark messages as consumed before delivery succeeds. Manual commit ensures we only acknowledge after successful processing — guaranteeing at-least-once delivery.

**Why 3 retry attempts?**
Balances between transient failure recovery and not overwhelming downstream systems with repeated failures.

**Why WebSocket over polling?**
Polling creates unnecessary load and adds latency. WebSocket maintains a persistent connection — server pushes instantly when a message is delivered.

## Tech Stack

Java 17, Spring Boot 3.2, Apache Kafka 3.6, WebSocket/STOMP, SockJS, React TypeScript, Recharts, Docker
