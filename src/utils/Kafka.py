import json

producer_config = {
    'bootstrap_servers': "localhost:9094",
    'client_id': 'TestProducer',
    'compression_type': 'gzip',
    'batch_size': 20000,
    'buffer_memory': 35000000, 
    'linger_ms': 10,
    'acks': 'all',
    'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
    'key_serializer': lambda v: str(v).encode('utf-8') if v is not None else None
    
}


consumer_config = {
    'bootstrap_servers': "localhost:9094",
    'client_id': "TestConsumer",
    'enable_auto_commit': True,
    'value_deserializer': lambda v: json.loads(v.decode('utf-8')),
    'key_deserializer': lambda v: v.decode('utf-8') if v is not None else None,
    'max_poll_records': 500

}