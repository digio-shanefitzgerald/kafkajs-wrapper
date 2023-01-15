import { Consumer, EachMessagePayload, ConsumerConfig, ConsumerSubscribeTopics } from "kafkajs";

import kafka from "../kafkaConfig"

export interface SimpleConsumer {
  connect(subscription: ConsumerSubscribeTopics): Promise<void>;
  handle(message: EachMessagePayload): Promise<void>
  disconnect(): Promise<void>;
}

export class MyConsumer implements SimpleConsumer {
  private readonly consumer: Consumer;

  constructor(config: ConsumerConfig) {
    this.consumer = kafka.consumer(config)
  }

  async connect(subscription: ConsumerSubscribeTopics): Promise<void> {
    return this.consumer.connect()
      .then(() => this.consumer.subscribe(subscription))
      .then(() => this.consumer.run({ eachMessage: payload => this.handle(payload) }))
      .catch(e => console.log(`Can't connect ${e}`));
  }
  
  handle({ topic, partition, message }: EachMessagePayload): Promise<void> {
    // handling of received message
    console.log("topic:::", topic, "partition:::", partition, "message:::", message.value?.toString());
    // TODO: not sure if this is correct...
    return Promise.resolve()
  }

  disconnect(): Promise<void> {
    return this.consumer.disconnect()
      .catch(e => console.log(`Error on disconnect ${e}`));
  }
}