# Produces (via plain Kafka pub calls) point-in-time log / data points on number
# of subscribers found on a given channel at a given time-stamp.
# Loops forver and just keeps publishing new random data points.
# see config with vars: sleep_between_harvests, harvests_per_day, channels
from kafka import KafkaProducer
import datetime
import random
import time
import json

sleep_between_harvests = 1  # seconds
harvests_per_day = 3  # times per day every channel gets a harvest (max 20)
channels = ['michelle_phan', 'pew_die_pie', 'new_york_times']
kafka_topic = 'yt_raw_harvest'

producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Seed the subscriber data for each channel
state = {}
for channel in channels:
    state[channel] = {
        "subs": random.randrange(500, 1000)
    }

harvest_date = datetime.date.today()

while True:

    harvest_date = harvest_date + datetime.timedelta(days=1)  # move on to the next day

    harvest_ts = datetime.datetime.combine(harvest_date, datetime.datetime.min.time())

    for n in range(0, harvests_per_day):

        harvest_ts = harvest_ts + datetime.timedelta(hours=1)  # move on to the next hour

        # Harvest each channel:
        for channel in channels:
            current_subs = state[channel]['subs']
            new_subs = current_subs + random.randrange(50, 500)
            state[channel]['subs'] = new_subs

            channel_data = {'yt_channel_id': channel,
                            'harvest_ts': harvest_ts.isoformat(),
                            'subs': new_subs}

            channel_data_string = json.dumps(channel_data)
            print("PRODUCING: ", channel_data_string)
            producer.send(kafka_topic, channel_data_string.encode())

        time.sleep(sleep_between_harvests)
