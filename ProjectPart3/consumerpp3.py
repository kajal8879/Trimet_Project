#!/usr/bin/env python

import psycopg2
from argparse import ArgumentParser, FileType
import argparse
from configparser import ConfigParser
import pandas as pd
import datetime
import json
import math
from confluent_kafka import Consumer, OFFSET_BEGINNING
DBname = "postgres"
DBuser = "postgres"
DBpwd = "pdxsec"
TableName="XYZ"

def dbconnect():
	connection = psycopg2.connect(
		host="localhost",
		database=DBname,
		user=DBuser,
		password=DBpwd,
	)
	connection.autocommit = True
	return connection

def createTable(conn):
    with conn.cursor() as cursor:
            cursor.execute(f"""              
                CREATE TABLE IF NOT EXISTS STOP (
                    trip_id INT PRIMARY KEY,
                    route_id INT,
                    vehicle_id INT,
                    service_key VARCHAR(10),
                    direction INT
                );
            """)

def loadStopTable(conn,df):
    with conn.cursor() as cursor:
        for row in df.itertuples():
            cursor.execute(f"""
                INSERT INTO STOP VALUES (
                '{row.trip_id}',
                '{row.route_id}',
                '{row.vehicle_id}',
                '{row.service_key}',
                '{row.direction}'
                );
            """)

def loadTripView(conn):
    with conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE OR REPLACE VIEW TRIP_VW AS (
                SELECT t1.trip_id,t2.route_id,t1.vehicle_id,t2.service_key,t2.direction
                FROM TRIP t1
                JOIN STOP t2 ON t1.trip_id=t2.trip_id AND t1.vehicle_id = t2.vehicle_id
                );
            """)




if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topic = "in_class"
    consumer.subscribe([topic], on_assign=reset_offset)

    messages=[]
    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                key = msg.key()
                event = json.loads(msg.value().decode('utf-8'))
                messages.append(event)
                print("Consuming data...")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print("\nTotal Records: " + str(len(messages)))
    df = pd.DataFrame(messages)
    print(df)

    #initialize()
    conn = dbconnect()
    print("Database connected")
#    createTable(conn)
#    print("Table stop created")
    loadStopTable(conn,df)
    print("Data loaded to table stop")
    loadTripView(conn)
    print("View trip_vw updated")
