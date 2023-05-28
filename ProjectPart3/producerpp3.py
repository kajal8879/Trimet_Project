#!/usr/bin/env python

import sys
from random import *
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from urllib.request import urlopen
import json
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime


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
            print("Produced message to topic {topic} with key = {key} and event =\n{event}".format(
             topic=topic,
             key=msg.key().decode('utf-8'),
             event=msg.value().decode('utf-8')
            ))

    print("Transforming data...")

    topic = "in_class"
    url = "http://www.psudataeng.com:8000/getStopEvents"
    html = urlopen(url)
    soup = BeautifulSoup(html, 'lxml')

    dataframes = []
    trip_id = []
    tables = soup.find_all('table')
    tags = soup.find_all('h2')
    trip_date_string = soup.find_all('h1')[0].text.split()[-1]
    trip_date = datetime.strptime(trip_date_string, "%Y-%m-%d")
    day_name = trip_date.strftime('%A')

    # Extract trip IDs
    for tag in tags:
        text = tag.get_text(strip=True)
        stripped_text = re.findall(r'\d+', text)
        trip_id.append(stripped_text)

    # Process tables and assign trip IDs
    for i, table in enumerate(tables):
        df = pd.read_html(str(table))[0]
        required_columns = ['vehicle_number','service_key', 'route_number', 'direction']
        df = df[required_columns]
        trip_ids = [x[0] for x in [trip_id[i % len(trip_id)]]]* len(df)
        trip_ids = [x.strip('[]') for x in trip_ids]
        df['trip_id'] = trip_ids
        dataframes.append(df)

    final_df = pd.concat(dataframes, ignore_index=True)
    print(final_df)

    final_df['service_key'] = day_name
    print(final_df)
    final_df.loc[~final_df['service_key'].isin(['Saturday','Sunday']), 'service_key'] = 'Weekday'
    print(final_df)

    finalfi_df = final_df.copy()
    finalfi_df = finalfi_df.drop_duplicates()
    print(finalfi_df)
    column_names_new = {
        'vehicle_number': 'vehicle_id',
        'route_number': 'route_id'
    }
    finalfi_df = finalfi_df.rename(columns=column_names_new)
    finalfi_df = finalfi_df.reindex(columns=['trip_id', 'route_id', 'vehicle_id', 'service_key','direction'])
    finalfi_df.reset_index(drop=True, inplace=True)
    print(finalfi_df)

    count=0
    for index, row in finalfi_df.iterrows():
        obj = row.to_dict()
        event = json.dumps(obj, indent=2).encode('utf-8')
        randomNumber=randint(1, 5)
        producer.produce(topic, event, str(randomNumber), callback=delivery_callback)
        count= count+1
        producer.poll(0)

    # Block until the messages are sent.
    producer.flush()

    #prind record count
    print("Total Records: " + str(count))
