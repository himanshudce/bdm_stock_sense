# ====== kafka consumer ==========
from kafka import KafkaConsumer
import json

topic_name = 'twitter'

consumer = KafkaConsumer(
    topic_name,
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     auto_commit_interval_ms=5000,
     fetch_max_bytes=128,
     max_poll_records=100,
     value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    tweets = json.loads(json.dumps(message.value))
    print(tweets)

    # # if we want to save a samole to local to analyse the tweet
    # with open('sample_stream_data.json', 'w') as f:
    #     json.dump(tweets, f,indent=2)
    # break
    