# Kafka filings demo

A small TypeScript project that publishes sample company filing events to Kafka and consumes them to update a demo company context.

The producer sends JSON events to the `filings.realtime` topic. Each event contains an ID, event type, source, timestamp, real-time lane, company ISIN and name, and filing document type, title, and text. The ISIN is the Kafka message key.

Kafka runs locally as **1 broker** in Docker. The `filings.realtime` topic has **3 partitions** and replication factor **1**. This lets up to three consumers in the same group process partitions concurrently;

## Run

```powershell
npm install
docker compose up -d
npm run topic:create
```

```powershell
npm run consume
```

```powershell
npm run publish
```

```powershell
docker compose down
```
