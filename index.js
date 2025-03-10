import { Kafka } from "kafkajs"
import express from 'express';
import WebSocket, { WebSocketServer } from 'ws';
import { CHAT_TOPIC, NOTIFICATION_TOPIC } from "./constants.js";

const app = express();
const PORT = process.env.PORT;

const httpServer = app.listen(PORT, () => {
  console.log(`Server running on port: ${PORT}`);
})

const wss = new WebSocketServer({ server: httpServer });

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
    await chatConsumer.subscribe({ topic: CHAT_TOPIC })
    await chatConsumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        wss.clients.forEach(function each(client) {
          if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(JSON.parse(message.value)));
          }
        });
      },
    })

    const notoficationConsumer = kafka.consumer({ groupId: process.env.NOTIFICATION_GROUP_ID })
    await notoficationConsumer.connect()
    await notoficationConsumer.subscribe({ topic: NOTIFICATION_TOPIC })
    await notoficationConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(
                `[NOTIFICATION]
                offset: ${message.offset},
                value: ${message.toString()}`,
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

wss.on('connection', function connection(ws) {
  ws.on('error', console.error);

  ws.on('message', async function message(data) {
    const parsedData = JSON.parse(data.toString());
        
    switch(parsedData.key) {
      case "chat" : {
        producer.send({
          topic: CHAT_TOPIC,
          messages: [
            { value: JSON.stringify(parsedData.data) },
          ],
        })
      }
    }

  });

  ws.on('close', function close() {
    console.log('Client disconnected');
  });
});