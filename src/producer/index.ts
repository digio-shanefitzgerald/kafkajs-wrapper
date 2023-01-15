import { Producer, ProducerRecord, ProducerConfig, RecordMetadata, RemoveInstrumentationEventListener } from "kafkajs";

import kafka from "../kafkaConfig"

export interface SimpleProducer {
  send(record: ProducerRecord): Promise<RecordMetadata[] | void>
  disconnect(): Promise<void>
  onDisconnect(): RemoveInstrumentationEventListener<"producer.producer.disconnect">
}

export class MyProducer implements SimpleProducer {
  private readonly producer: Producer

  constructor(config?: ProducerConfig) {
    this.producer = kafka.producer(config);
  }

  async send(record: ProducerRecord): Promise<RecordMetadata[] | void> {
    try {
      await this.producer.connect()
      return await this.producer.send(record)
    } catch (error) {
      console.log(`producer didn't connect:: ${error}`)
    }
  }
 
  disconnect(): Promise<void> {
      return this.producer.disconnect()
        .catch(e => console.log(`Error on producer disconnect ${e}`))
  }

  onDisconnect(): RemoveInstrumentationEventListener<"producer.disconnect"> {
    return this.producer.on('producer.disconnect', (event) => {
      console.log(`producer disconnect ${JSON.stringify(event)}`)
    })
  }
}