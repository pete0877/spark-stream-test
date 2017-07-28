# Consumes and aggregates (Spark Streaming) the point-in-time log / data points
# which would be produced by YT harvester of channels (see producer_log.py).
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
import json
import avro.schema, avro.io
import io
from dateutil.parser import *

zookeeper = "localhost:2181"
input_topic = "yt_daily_as_str"
output_topic = "yt_monthly"
app_name = "stream_agg_daily_to_monthly"

# yt_daily_avro = """
# {
#  "namespace": "yt_daily_avro",
#  "type": "record",
#  "name": "yt_daily_avro",
#  "fields": [
#      {"name": "yt_channel_id", "type": "string"},
#      {"name": "harvest_ts", "type": "string"},
#      {"name": "date", "type": "string"},
#      {"name": "subs",  "type": "int"}
#  ]
# }
# """

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

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", app_name)
ssc = StreamingContext(sc, 1)

producer = KafkaProducer(bootstrap_servers='localhost:9092')

# yt_daily_schema = avro.schema.parse(yt_daily_avro)
yt_monthly_schema = avro.schema.parse(yt_monthly_avro)


# def avro_value_decoder(avro_data):
#     bytes_reader = io.BytesIO(avro_data)
#     decoder = avro.io.BinaryDecoder(bytes_reader)
#     reader = avro.io.DatumReader(yt_daily_schema)
#     yt_daily = reader.read(decoder)
#     return yt_daily


def send_result_to_kafka(message):
    records = message.collect()
    for record in records:
        record['month'] = record['harvest_ts'][:7]  # E.g. '2017-07'

        writer = avro.io.DatumWriter(yt_monthly_schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(record, encoder)
        raw_bytes = bytes_writer.getvalue()
        producer.send(output_topic, raw_bytes)
        producer.flush()


def select_latest_data(doc1, doc2):
    date1 = parse(doc1['harvest_ts'])
    date2 = parse(doc2['harvest_ts'])

    if date1 > date2:
        return doc1
    else:
        return doc2


raw_stream = KafkaUtils.createStream(ssc, zookeeper, app_name, {input_topic: 1})
# raw_stream = KafkaUtils.createStream(ssc, zookeeper, app_name, {input_topic: 1}, valueDecoder=avro_value_decoder)

window_stream = raw_stream.window(30, 30)

parsed_stream = window_stream.map(lambda v: json.loads(v[1]))
# parsed_stream = window_stream.map(lambda v: v[1])

filtered_stream = parsed_stream  # .filter(lambda doc: doc['yt_channel_id'] in ['michelle_phan', 'new_york_times'])

# Mapping example:   key: michelle_phan|2019-07     value:  (the original doc)
# Reduction is selection the document for the latest date / timestamp
latest_daily_records = filtered_stream \
    .map(lambda doc: (doc['yt_channel_id'] + '|' + doc['harvest_ts'][:7], doc)) \
    .reduceByKey(select_latest_data) \
    .map(lambda key_value_tuple: key_value_tuple[1])

latest_daily_records.foreachRDD(send_result_to_kafka)

ssc.start()
ssc.awaitTermination()
