import registryFactory from 'avro-registry-client';
import kafka, { ConsumerOptions } from 'kafka-node';

export default class KafkaFactory {
  private registry;
  private kafkaHost: string;

  constructor(kafkaHost: string, registryUrl: string) {
    this.registry = registryFactory(registryUrl);
    this.kafkaHost = kafkaHost;
  }

  public buildProducer<Key, Subject>(
    descriptor: EventDescriptor<Key, Subject>
  ) {
    return this.buildProducerForTopic(descriptor.topic, descriptor);
  }

  public buildProducerForTopic<Key, Subject>(
    topic: string,
    descriptor: EventDescriptor<Key, Subject>
  ) {
    const kafkaClient = this.buildKafkaClient();
    const untypedProducer = this.registry
      .createProducer(new kafka.Producer(kafkaClient), topic)
      .valueSubject(descriptor.valueSubject, descriptor.valueSubjectVersion)
      .keySubject(descriptor.keySubject, descriptor.keySubjectVersion);

    return (key: Key, subject: Subject) => {
      return untypedProducer(key, subject);
    };
  }

  public buildConsumer<Key, Subject>(
    descriptor: EventDescriptor<Key, Subject>,
    handler: (message: KafkaMessage<Key, Subject>) => void,
    err: (err: any) => void
  ) {
    return this.buildConsumerForTopic(
      descriptor.topic,
      descriptor,
      handler,
      err
    );
  }

  public buildConsumerForTopic<Key, Subject>(
    topic: string,
    descriptor: EventDescriptor<Key, Subject>,
    handler: (message: KafkaMessage<Key, Subject>) => void,
    err: (err: any) => void // TODO see if this should be an error type ask keith
  ) {
    const kafkaClient = this.buildKafkaClient();
    const consumerDefaults: ConsumerOptions = {
      encoding: 'buffer',
      keyEncoding: 'buffer'
    };
    const consumerOptions: ConsumerOptions = descriptor.consumerGroupId
      ? {
          ...consumerDefaults,
          groupId: descriptor.consumerGroupId
        }
      : consumerDefaults;
    const kafkaConsumer = new kafka.Consumer(
      kafkaClient,
      [{ topic }],
      consumerOptions
    );
    this.registry
      .createConsumer(kafkaConsumer)
      .messageType()
      .valueSubject(descriptor.valueSubject, descriptor.valueSubjectVersion)
      .keySubject(descriptor.keySubject, descriptor.keySubjectVersion)
      .handler(handler)
      .listen(err);
    return { kafkaConsumer, kafkaClient };
  }

  public async primeSubjects<Key, Subject>(
    descriptor: EventDescriptor<Key, Subject>
  ) {
    if (descriptor.valueSubject) {
      await this.registry.primeSubjectByVersion(
        descriptor.valueSubject,
        descriptor.valueSubjectVersion
      );
    }
    if (descriptor.keySubject) {
      await this.registry.primeSubjectByVersion(
        descriptor.keySubject,
        descriptor.keySubjectVersion
      );
    }
  }

  private buildKafkaClient() {
    return new kafka.KafkaClient({
      kafkaHost: this.kafkaHost
    });
  }
}

export interface EventDescriptor<_Key, _Subject> {
  readonly keySubject: string;
  readonly keySubjectVersion: [string | number];
  readonly valueSubject: string;
  readonly valueSubjectVersion: [string | number];
  readonly consumerGroupId?: string;
  readonly topic?: string;
}

export interface KafkaMessage<Key, Subject> {
  readonly key: Key;
  readonly value: Subject;
  readonly message: any;
}
