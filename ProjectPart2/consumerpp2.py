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
DBpwd = "Qwerty@#23"
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
            DROP TABLE IF EXISTS BreadCrumb;
            DROP TABLE IF EXISTS Trip;
            CREATE TABLE Trip (
                trip_id INT PRIMARY KEY,
                route_id INT,
                vehicle_id INT,
                service_key VARCHAR(10),
                direction INT
            );
            CREATE TABLE BreadCrumb (
                tstamp TIMESTAMP,
                latitude FLOAT,
                longitude FLOAT,
                speed FLOAT,
                trip_id INTEGER REFERENCES Trip(trip_id)
            );
        """)

def getDecodedtDate(date):
    dt = datetime.datetime.strptime(date, "%d%b%Y:%H:%M:%S")
    return dt

def getTimeStamp(time):
    ts=datetime.timedelta(seconds=time)
    return ts

def loadTripTable(conn,unique_combinations_df):
    with conn.cursor() as cursor:
        for row in unique_combinations_df.itertuples():
            cursor.execute(f"""
                INSERT INTO TRIP VALUES (
                '{row.EVENT_NO_TRIP}',
                '0',
                '{row.VEHICLE_ID}',
                '0',
                '0'
                )
            """)

def loadBreadCrumbTable(conn,df):
    with conn.cursor() as cursor:
        for row in df.itertuples():
            cursor.execute(f"""
                INSERT INTO BreadCrumb VALUES (
                '{row.DateTimeStamp}',
                '{row.GPS_LATITUDE}',
                '{row.GPS_LONGITUDE}',
                '{row.speed}',
                '{row.EVENT_NO_TRIP}'
                )
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
    topic = "topic_vikings"
    consumer.subscribe([topic], on_assign=reset_offset)
    count=0
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
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group ad commit final offsets
        consumer.close()

    print(count)
    df = pd.DataFrame(messages)
    #initialize()
    conn = dbconnect()
    print("Database conenctiom")
    createTable(conn)
    print("Table created")
    df2=pd.DataFrame(data=df,columns = ['OPD_DATE'])
    print("df2")
    print(df2)
    df2 = df2.applymap(getDecodedtDate)
    df2= df2.rename(columns={'OPD_DATE': 'Date'})
    df3=pd.DataFrame(data=df,columns = ['ACT_TIME'])
    df3=df3.applymap(getTimeStamp)
    df3= df3.rename(columns={'ACT_TIME': 'Time'})
    df4 = pd.concat([df2, df3], axis=1)
    df4 = df4.assign(DateTimeStamp = df4['Date'] + df4['Time'])
    df['DateTimeStamp'] = pd.Series(df4['DateTimeStamp'])
    print("df4")
    print(df4)
    df['dMETERS'] = df.groupby('EVENT_NO_TRIP')['METERS'].diff()
    df['dDateTimeStamp'] = df.groupby('EVENT_NO_TRIP')['DateTimeStamp'].diff()
    calculateSpeed=  lambda x: round(x['dMETERS'] / x['dDateTimeStamp'].total_seconds(), 2) if not math.isnan(x['dMETERS']) and not math.isnan(x['dDateTimeStamp'].total_seconds()) else None
    df['speed']=df.apply(calculateSpeed,axis=1)
    df=df.drop(columns=['dMETERS','dDateTimeStamp'])
    #print(df)
    pd.set_option('display.float_format', '{:.2f}'.format)
    print("df")
    print(df)
    df=df.drop(columns=['GPS_HDOP','EVENT_NO_STOP','GPS_SATELLITES'])
    print(df)
    unique_combinations_df = df[['EVENT_NO_TRIP', 'VEHICLE_ID']].drop_duplicates()
    loadTripTable(conn,unique_combinations_df)
    loadBreadCrumbTable(conn,df)
    print("Data loaded")
