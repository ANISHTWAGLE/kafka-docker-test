import { randomUUID } from "node:crypto";
import { Kafka } from "kafkajs";
import { filingsTopic, type FilingEvent } from "./types.js";

const kafka = new Kafka({ clientId: "filings-publisher-demo", brokers: ["localhost:9092"] });
const producer = kafka.producer();

const companies = [
  { isin: "INE002A01018", name: "Reliance Industries" },
  { isin: "INE467B01029", name: "Tata Consultancy Services" },
  { isin: "INE009A01021", name: "Infosys" },
  { isin: "INE040A01034", name: "HDFC Bank" },
  { isin: "INE090A01021", name: "ICICI Bank" },
];
const count = Math.max(1, Number.parseInt(process.argv[2] ?? "1", 10) || 1);

try {
  await producer.connect();
  for (let index = 0; index < count; index++) {
    const company = companies[index % companies.length]!;
    const event: FilingEvent = {
      eventId: randomUUID(),
      eventType: "filing.published",
      source: "exchange-filings-simulator",
      occurredAt: new Date().toISOString(),
      lane: "real-time",
      company,
      filing: {
        documentType: "exchange-announcement",
        title: `Sample announcement ${index + 1}`,
        text: `${company.name} published a sample filing for the Kafka scaling demo.`,
      },
    };
    const [result] = await producer.send({
      topic: filingsTopic,
      messages: [{ key: event.company.isin, value: JSON.stringify(event) }],
    });
    console.log(`${event.company.name}: partition ${result?.partition}, offset ${result?.baseOffset}`);
  }
  console.log(`Published ${count} event(s).`);
} catch (error) {
  console.error("Could not publish. Is Kafka running and has the topic been created?", error);
  process.exitCode = 1;
} finally {
  await producer.disconnect();
}
