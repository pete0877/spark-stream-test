# Consumes daily stats computed by the Stream Processing and publised onto the yt_daily
from kafka import KafkaConsumer
import avro.schema
import avro.io
import io
from dateutil.parser import *
import datetime
import mysql.connector
from mysql.connector import errorcode

db_config = {
    'user': 'pixuser',
    'password': 'p8x#dbAC3ss',
    'host': '127.0.0.1',
    'database': 'yt'
}

input_topic = "yt_monthly"

yt_monthly_avro = """
{
 "namespace": "yt_monthly_avro",
 "type": "record",
 "name": "yt_monthly_avro",
 "fields": [
     {"name": "yt_channel_id", "type": "string"},
     {"name": "harvest_ts", "type": "string"},
     {"name": "month", "type": "string"},     
     {"name": "subs",  "type": "int"}
 ]
}
"""


schema = avro.schema.Parse(yt_monthly_avro)
consumer = KafkaConsumer(input_topic, bootstrap_servers='localhost:9092')

db_conn = mysql.connector.connect(**db_config)
db_conn.autocommit = True

cursor = db_conn.cursor()


def attempt_find_and_update_record(cursor, data):
    cursor.execute('SELECT harvest_ts FROM yt_monthly WHERE channel=%s and year_and_month=%s',
                   (data['yt_channel_id'], data['month']))
    current_row = cursor.fetchone()
    if not current_row:
        print('    .. did not find an existing record')
        return False
    else:
        print('    .. found existing record')
        existing_harvest_ts = current_row[0]
        incoming_harvest_ts = parse(data['harvest_ts'])

        if incoming_harvest_ts > existing_harvest_ts:
            print('        .. the received TS (%s) is newer then existing (%s), updating' %
                  (incoming_harvest_ts, existing_harvest_ts))
            cursor.execute('UPDATE yt_monthly set harvest_ts = %s, subs = %s WHERE channel = %s AND year_and_month = %s',
                           (data['harvest_ts'], data['subs'], data['yt_channel_id'], data['month']))
        else:
            print('        .. the received TS (%s) is older then existing (%s), ignoring' %
                  (incoming_harvest_ts, existing_harvest_ts))

        return True


def attempt_insert(cursor, data):
    try:
        print('    .. attempting to insert new record')
        cursor.execute('INSERT INTO yt_monthly (harvest_ts, channel, year_and_month, subs) values (%s, %s, %s, %s)',
                       (data['harvest_ts'], data['yt_channel_id'], data['month'], data['subs']))
        return True
    except Exception as error:
        print('    .. got ERROR in the insert: ', error)
        return False


def upsert_if_latest(cursor, yt_monthly):
    # Attempt to find and update existing record:
    if attempt_find_and_update_record(cursor, yt_monthly):
        return

    # IF record wasn't found, insert new one but if
    # race condition occurs and somebody else inserts another record in the meantime,
    # then attempt the find-and-update again:
    if not attempt_insert(cursor, yt_monthly):
        attempt_find_and_update_record(cursor, yt_monthly)


for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    yt_monthly = reader.read(decoder)
    print("RECEIVED yt_monthly: ", yt_monthly)
    upsert_if_latest(cursor, yt_monthly)

db_conn.close()
