import { Kafka } from "kafkajs"
import express from 'express';
import { CHAT_TOPIC, NOTIFICATION_TOPIC } from "./constants.js";

const app = express();
const PORT = process.env.PORT;

const kafka = new Kafka({
  clientId: process.env.CLIENT_ID,
  brokers: ['localhost:9092']
})

const producer = kafka.producer()

const init = async () => {
    await producer.connect({
        allowAutoTopicCreation: false,
    })

    const chatConsumer = kafka.consumer({ groupId: process.env.CHAT_GROUP_ID })
    await chatConsumer.connect()
    await chatConsumer.subscribe({ topic: CHAT_TOPIC, fromBeginning: true })
    await chatConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(
                `[CHAT]
                offset: ${message.offset},
                value: ${message.value.toString()}`,
            )
        },
    })

    const notoficationConsumer = kafka.consumer({ groupId: process.env.NOTIFICATION_GROUP_ID })
    await notoficationConsumer.connect()
    await notoficationConsumer.subscribe({ topic: NOTIFICATION_TOPIC, fromBeginning: true })
    await notoficationConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(
                `[NOTIFICATION]
                offset: ${message.offset},
                value: ${message.value.toString()}`,
            )
        },
    })
}

init().catch(console.error)


app.use(express.json());

app.get('/health', (req, res) => {
    res.status(200).json({msg: "server is healthy"})
})

app.post('/send-message', async (req, res) => {
    const message = req.body.message;
    if(!message) return res.status(400).json({msg: "Message is required"});

    await producer.send({
        topic: CHAT_TOPIC,
        messages: [
          { value: message.toString() },
        ],
    })

    res.status(201).json({
        msg: "Message sent successfully"
    })
    return;
});

app.post("/send-notification", async (req, res) => {
    const notification = req.body.notification;
    if (!notification) return res.status(400).json({ msg: "Notification is required" });
  
    await producer.send({
      topic: NOTIFICATION_TOPIC,
      messages: [{ value: notification.toString() }],
    });
  
    res.status(201).json({
      msg: "Notification sent successfully",
    });
  });

app.listen(PORT, () => {
    console.log(`Server running on port: ${PORT}`);
})