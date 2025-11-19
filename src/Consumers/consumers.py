from kafka import KafkaConsumer, KafkaClient
import json
import logging


consumer_config = {
    'bootstrap_servers': "localhost:9094",
    'group_id': "my_consumer_group",
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
    'key_deserializer': lambda x: x.decode('utf-8') if x else None,
}