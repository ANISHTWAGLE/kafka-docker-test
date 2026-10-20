import { Kafka } from "kafkajs";
import { filingsPartitionCount, filingsTopic } from "./types.js";

const kafka = new Kafka({ clientId: "filings-learning-admin", brokers: ["localhost:9092"] });
const admin = kafka.admin();

try {
  await admin.connect();
  const created = await admin.createTopics({
    waitForLeaders: true,
    topics: [{ topic: filingsTopic, numPartitions: filingsPartitionCount, replicationFactor: 1 }],
  });
  if (created) {
    console.log(`Created ${filingsTopic} with ${filingsPartitionCount} partitions.`);
  } else {
    const metadata = await admin.fetchTopicMetadata({ topics: [filingsTopic] });
    const currentCount = metadata.topics[0]?.partitions.length ?? 0;
    if (currentCount < filingsPartitionCount) {
      await admin.createPartitions({
        topicPartitions: [{ topic: filingsTopic, count: filingsPartitionCount }],
      });
      console.log(`Expanded ${filingsTopic} from ${currentCount} to ${filingsPartitionCount} partitions.`);
    } else {
      console.log(`Topic already has ${currentCount} partitions: ${filingsTopic}`);
    }
  }
} catch (error) {
  console.error("Could not create the topic. Is Kafka running?", error);
  process.exitCode = 1;
} finally {
  await admin.disconnect();
}
