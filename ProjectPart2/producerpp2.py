#!/usr/bin/env python

import sys
from random import *
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from urllib.request import urlopen
import json

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
            print("Producing data...")
#            print("Produced message to topic {topic} with key = {key} and event =\n{event}".format(
#             topic=topic,
#             key=msg.key().decode('utf-8'),
#             event=msg.value().decode('utf-8')
#            ))


    # Produce data by selecting random values from these lists.
    topic = "in_class"
    url="http://www.psudataeng.com:8000/getBreadCrumbData"
    response= urlopen(url)
    data=json.load(response)
    count=0
    # Print each JSON object
    for obj in data:
        event = json.dumps(obj, indent=2).encode('utf-8')
        randomNumber=randint(1, 5)
        producer.produce(topic, event, str(randomNumber), callback=delivery_callback)
        count= count+1
        producer.poll(0)

    # Block until the messages are sent.
    producer.flush()

    print("Total Records: " + str(count))
