import json

from MQTT import MQTTClient
from Kafka.TopicProducer import ProducerClient
from utils.Kafka import producer_config


class MQTTBridge:
    def __init__(self, 
                 mqtt_broker: str,
                 kafka_brokers: str,
                ):
        self.conf = producer_config.copy()
        self.conf['bootstrap_servers'] = kafka_brokers

        self.kafkaproducer = ProducerClient(self.conf)

        self.mqtt_client = MQTTClient(
            broker=mqtt_broker,
            topics=["device/+/data"],
            KAFKA_PRODUCER=self.kafkaproducer
        )


    def start(self):
        print("Starting MQTT to Kafka Bridge...")
        self.mqtt_client.start()


    def stop(self):
        print("Stopping MQTT to Kafka Bridge...")
        self.kafkaproducer.close()
