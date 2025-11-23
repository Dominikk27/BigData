import paho.mqtt.publish as publish
from typing import List

class MQTTPublisher:
    def __init__(self, broker: str, topics: List):
        self.broker = broker
        self.port = 1883

    def on_connect(self):
        print("Connected to broker: ", self.broker)

    def send(self, topic: str, message: str):
        publish.single(topic, payload=message, hostname=self.broker, port=self.port)
        print(f"[SENT] {topic}: {message}")


""" if __name__ == "__main__":
    BROKER = "host.docker.internal"
    TOPIC = "test/topic"

    publisher = MQTTPublisher(BROKER, [TOPIC])
    publisher.on_connect()
    
    csvData = ReadCSV('../dataset/agroDataset.csv')
    #publisher.send(TOPIC, "Hello World! from Publisher") """