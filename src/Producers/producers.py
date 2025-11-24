from kafka import KafkaProducer, KafkaClient
import json
import logging


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

logger = logging.getLogger("[Producer logger]")
producer = KafkaProducer(**producer_config)

def produceData(topic: str, key: str, data: dict):
    try:
        future = producer.send(
            topic, 
            key=key, 
            value=data
        )
        future.get(timeout=10)
        producer.flush()

        #print(data);
        logger.info(f"Data: {data} successfully sent!")
    except Exception as e:
        logger.error(f"Error: {e}");

