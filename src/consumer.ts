import { Kafka } from "kafkajs";
import { filingsTopic, type FilingEvent } from "./types.js";

const kafka = new Kafka({ clientId: "company-context-manager-demo", brokers: ["localhost:9092"] });
const groupId = process.env.CONSUMER_GROUP_ID ?? "company-context-manager-v1";
const workerName = process.env.WORKER_NAME ?? `worker-${process.pid}`;
const consumer = kafka.consumer({ groupId });
const contextByIsin = new Map<string, string[]>();

consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
  console.log(`${workerName} joined ${groupId}; assigned partitions:`, payload.memberAssignment[filingsTopic] ?? []);
});

await consumer.connect();
await consumer.subscribe({ topic: filingsTopic, fromBeginning: true });
console.log(`${workerName} listening to ${filingsTopic}. Press Ctrl+C to stop.`);

await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    if (!message.value) return;

    try {
      const event = JSON.parse(message.value.toString()) as FilingEvent;
      const existingNotes = contextByIsin.get(event.company.isin) ?? [];
      const newNote = `${event.filing.documentType}: ${event.filing.title} — ${event.filing.text}`;
      contextByIsin.set(event.company.isin, [...existingNotes, newNote]);

      console.log(`\n${workerName} consumed ${topic}[${partition}] offset ${message.offset}`);
      console.log(`Company: ${event.company.name} (${event.company.isin})`);
      console.log("Updated demo context:");
      for (const note of contextByIsin.get(event.company.isin) ?? []) console.log(`  - ${note}`);
    } catch (error) {
      console.error(`Skipping invalid event at offset ${message.offset}:`, error);
    }
  },
});
