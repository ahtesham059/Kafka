import { Kafka } from "kafkajs";
import { CHAT_TOPIC, NOTIFICATION_TOPIC } from "./constants.js";

const kafka = new Kafka({
  clientId: 'kafka-admin',
  brokers: ['localhost:9092'],
});

const createTopics = async () => {
  const admin = kafka.admin();
  await admin.connect();

  try {
    await admin.createTopics({
      topics: [
        {
          topic: CHAT_TOPIC,
          numPartitions: 1,
          replicationFactor: 1,
        },
        {
          topic: NOTIFICATION_TOPIC,
          numPartitions: 1,
          replicationFactor: 1,
        }
      ],
    });
    // const topics = await admin.listTopics();
    // console.log('Existing topics:', topics);


    console.log('Topics created successfully!');
  } catch (error) {
    console.error('Error creating topics:', error);
  } finally {
    await admin.disconnect();
  }
};

createTopics();
