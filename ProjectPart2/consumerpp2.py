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
            DROP TABLE IF EXISTS BREADCRUMB;
            DROP TABLE IF EXISTS TRIP;
            CREATE TABLE TRIP (
                trip_id INT PRIMARY KEY,
                route_id INT,
                vehicle_id INT,
                service_key VARCHAR(10),
                direction INT
            );
            CREATE TABLE BREADCRUMB (
                tstamp TIMESTAMP,
                latitude FLOAT,
                longitude FLOAT,
                speed FLOAT,
                trip_id INTEGER REFERENCES TRIP(trip_id)
            );
        """)

def getDecodedDate(date):
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
                '{row.SERVICE_KEY}',
                '0'
                )
            """)

def loadBreadCrumbTable(conn,df):
    with conn.cursor() as cursor:
        for row in df.itertuples():
            cursor.execute(f"""
                INSERT INTO BREADCRUMB VALUES (
                '{row.DATETIMESTAMP}',
                '{row.GPS_LATITUDE}',
                '{row.GPS_LONGITUDE}',
                '{row.SPEED}',
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

    #initialize()
    conn = dbconnect()
    print("Database connected")
#    createTable(conn)
#    print("Tables trip and breadcrumb created")
    print("Transforming the consumed data...")
    df2 = pd.DataFrame(data=df,columns = ['OPD_DATE'])
#    print("\nDate")
#    print(df2[0])
    df2 = df2.applymap(getDecodedDate)
    df2 = df2.rename(columns={'OPD_DATE': 'DATE'})
    df3 = pd.DataFrame(data=df,columns = ['ACT_TIME'])
    df3 = df3.applymap(getTimeStamp)
    df3 = df3.rename(columns={'ACT_TIME': 'TIME'})
    df4 = pd.concat([df2, df3], axis=1)
    df4 = df4.assign(DATETIMESTAMP = df4['DATE'] + df4['TIME'])
    df['DATETIMESTAMP'] = pd.Series(df4['DATETIMESTAMP'])
    print("\nAdded timestamp")
    print(df4)
    df['dMETERS'] = df.groupby('EVENT_NO_TRIP')['METERS'].diff()
    df['dDATETIMESTAMP'] = df.groupby('EVENT_NO_TRIP')['DATETIMESTAMP'].diff()
    calculateSpeed=  lambda x: round(x['dMETERS'] / x['dDATETIMESTAMP'].total_seconds(), 2) if not math.isnan(x['dMETERS']) and not math.isnan(x['dDATETIMESTAMP'].total_seconds()) else None
    df['SPEED'] = df.apply(calculateSpeed,axis=1)
    df['SPEED'] = df['SPEED'].fillna(0)
    df = df.drop(columns=['dMETERS','dDATETIMESTAMP'])
#    print(df)
    df['SERVICE_KEY'] = df['DATETIMESTAMP'].dt.day_name
    df.loc[~df['SERVICE_KEY'].isin(['Saturday','Sunday']), 'SERVICE_KEY'] = 'Weekday'
#    print(df)
    pd.set_option('display.float_format', '{:.2f}'.format)
    print("\nAdded service key")
    print(df)
    df=df.drop(columns =['GPS_HDOP','EVENT_NO_STOP','GPS_SATELLITES'])
    print("\nDropped unnecessary columns")
    print(df)
    df1 = df.copy()
    unique_combinations_df = df1[['EVENT_NO_TRIP','VEHICLE_ID','SERVICE_KEY']].drop_duplicates()
    print("Loading data to table trip...")
    loadTripTable(conn,unique_combinations_df)
    print("Data loaded to table trip")
    print("Loading data to table breadcrumb...")
    loadBreadCrumbTable(conn,df)
    print("Data loaded to table breadcrumb")
