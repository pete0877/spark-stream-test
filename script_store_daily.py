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

input_topic = "yt_daily"
yt_daily_avro = """
{
 "namespace": "yt_daily_avro",
 "type": "record",
 "name": "yt_daily_avro",
 "fields": [
     {"name": "yt_channel_id", "type": "string"},
     {"name": "harvest_ts", "type": "string"},
     {"name": "date", "type": "string"},     
     {"name": "subs",  "type": "int"}
 ]
}
"""

schema = avro.schema.Parse(yt_daily_avro)
consumer = KafkaConsumer(input_topic, bootstrap_servers='localhost:9092')

db_conn = mysql.connector.connect(**db_config)
db_conn.autocommit = True

cursor = db_conn.cursor()


def attempt_find_and_update_record(cursor, yt_daily):
    cursor.execute('SELECT harvest_ts FROM yt_daily WHERE channel=%s and date=%s',
                   (yt_daily['yt_channel_id'], yt_daily['date']))
    current_row = cursor.fetchone()
    if not current_row:
        print('    .. did not find an existing record')
        return False
    else:
        print('    .. found existing record')
        existing_harvest_ts = current_row[0]
        incoming_harvest_ts = parse(yt_daily['harvest_ts'])

        if incoming_harvest_ts > existing_harvest_ts:
            print('        .. the received TS (%s) is newer then existing (%s), updating' %
                  (incoming_harvest_ts, existing_harvest_ts))
            cursor.execute('UPDATE yt_daily set harvest_ts = %s, subs = %s WHERE channel = %s AND date = %s',
                           (yt_daily['harvest_ts'], yt_daily['subs'], yt_daily['yt_channel_id'], yt_daily['date']))
        else:
            print('        .. the received TS (%s) is older then existing (%s), ignoring' %
                  (incoming_harvest_ts, existing_harvest_ts))

        return True


def attempt_insert(cursor, yt_daily):
    try:
        print('    .. attempting to insert new record')
        cursor.execute('INSERT INTO yt_daily (harvest_ts, channel, date, subs) values (%s, %s, %s, %s)',
                       (yt_daily['harvest_ts'], yt_daily['yt_channel_id'], yt_daily['date'], yt_daily['subs']))
        return True
    except Exception as error:
        print('    .. got ERROR in the insert: ', error)
        return False


def upsert_if_latest(cursor, yt_daily):
    # Attempt to find and update existing record:
    if attempt_find_and_update_record(cursor, yt_daily):
        return

    # IF record wasn't found, insert new one but if
    # race condition occurs and somebody else inserts another record in the meantime,
    # then attempt the find-and-update again:
    if not attempt_insert(cursor, yt_daily):
        attempt_find_and_update_record(cursor, yt_daily)


for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    yt_daily = reader.read(decoder)
    print("RECEIVED yt_daily: ", yt_daily)
    upsert_if_latest(cursor, yt_daily)

db_conn.close()
