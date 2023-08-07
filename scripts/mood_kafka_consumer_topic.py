#!/usr/bin/env python3

import confluent_kafka
import time
import warnings
import sys

def confluent_kafka_consumer_test():
    
    topic = 'fluentd_test'
    conf = {'bootstrap.servers': "qpr-sd-moo-mq01.itn.intraorange:9093",'security.protocol':"ssl",'ssl.key.location': "<PEM_FILE>",
    'ssl.certificate.location': "<CER_FILE>", 'enable.ssl.certificate.verification':"false",
    'ssl.endpoint.identification.algorithm':"none"}

    consumer = confluent_kafka.Consumer(**conf)
    ### list_topics returns object of type ClusterMetadata
    topics = consumer.list_topics().topics
    print("Number of topics {}\n".format(len(topics)))
    for topic in topics:
        metadata = topics[topic]
        print("name: {} (p{})".format(metadata.topic, len(metadata.partitions)))

def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

def consume_topic():
    conf = {'bootstrap.servers': "qpr-sd-moo-mq01.itn.intraorange:9093",'security.protocol':"ssl",'ssl.key.location': "<PEM_FILE>",
    'ssl.certificate.location': "<CER_FILE>", 'enable.ssl.certificate.verification':"false",
    'ssl.endpoint.identification.algorithm':"none", 'group.id': 'python_consumer_dev', 'session.timeout.ms': 6000, 'auto.offset.reset': 'earliest'}

    consumer = confluent_kafka.Consumer(**conf)
    consumer.subscribe(['<TOPIC>'], on_assign=print_assignment)
    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Proper message
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
                print(msg.value())

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

# confluent_kafka_producer_performance()
confluent_kafka_consumer_test()
print("Begin consumption...")
consume_topic()