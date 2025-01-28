import { Kafka } from "kafkajs"
import express from 'express';

const app = express();
const PORT = process.env.PORT;

const kafka = new Kafka({
  clientId: process.env.CLIENT_ID,
  brokers: ['localhost:9092']
})

const producer = kafka.producer()

const run = async () => {
    await producer.connect()

    const chatConsumer = kafka.consumer({ groupId: process.env.CHAT_GROUP_ID })
    await chatConsumer.connect()
    await chatConsumer.subscribe({ topic: 'chat-topic', fromBeginning: true })
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
    await notoficationConsumer.subscribe({ topic: 'notification-topic', fromBeginning: true })
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

run().catch(console.error)


app.use(express.json());

app.get('/health', (req, res) => {
    res.status(200).json({msg: "server is healthy"})
})

app.post('/send-message', async (req, res) => {
    const message = req.body.message;
    if(!message) return res.status(400).json({msg: "Message is required"});

    await producer.send({
        topic: 'chat-topic',
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
      topic: "notification-topic",
      messages: [{ value: notification.toString() }],
    });
  
    res.status(201).json({
      msg: "Notification sent successfully",
    });
  });

app.listen(PORT, () => {
    console.log(`Server running on port: ${PORT}`);
})