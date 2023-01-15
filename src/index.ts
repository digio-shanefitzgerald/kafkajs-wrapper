import { MyConsumer } from "./consumer"
import { MyProducer } from "./producer"

let producer
let consumer

(async function() {
  if (!producer) {
    producer = new MyProducer()
  }
  
  if (!consumer) {
    consumer = new MyConsumer({ groupId: 'test-group-id' })
  }

  // consumer connects to producer
	console.log("consumer connect")
  // TODO: delay in connecting to consumer ??
  await consumer.connect({ topics: ['test-topic'], fromBeginning: false });

  // producer sends messages to consumer
  console.log("producer send message")
  for (let index = 0; index < 10; index++) {
    producer.send({ topic: 'test-topic', messages: [{ value: `hello from kafkajs ${index}` }] })
  }

  // may need to do some cleanup on disconnect
  producer.onDisconnect()

  producer.disconnect()
})();


