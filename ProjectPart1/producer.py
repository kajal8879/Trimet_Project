#!/usr/bin/env python

import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import json
from urllib.request import urlopen


if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced message to topic {topic} with and event= {event}".format(
             topic=topic,
             event=msg.value().decode('utf-8')
            ))


    # Produce data by selecting random values from these lists.
    topic = "breadCrumb_1"
    url="http://www.psudataeng.com:8000/getBreadCrumbData"
    response= urlopen(url)
    data=json.load(response)
    count=0   
    # Print each JSON object
    for obj in data:
        event = json.dumps(obj, indent=2).encode('utf-8')
        producer.produce(topic, event, callback=delivery_callback)
        count= count+1
        producer.poll(0)

    # Block until the messages are sent.
    print(count)
    producer.flush()
