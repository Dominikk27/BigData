import paho.mqtt.client as mqtt
from typing import List

class MQTTPublisher:
    _instance = None

    def __new__(cls, 
                broker = "host.docker.internal", 
                port = 1883):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.broker = broker
            cls._instance.port = port
            cls._instance.client = mqtt.Client()
            cls._instance.client.on_connect = cls._instance._connect
            cls._instance.client.connect(cls._instance.broker, cls._instance.port, 60)
            cls._instance.client.loop_start()

        return cls._instance

    def _connect(self, client, userdata, flags, rc):
        print(f"[MQTT CONNECTED] Broker: {self.broker}, result code: {rc}")

    def send(self, topic: str, message: str):
        self.client.publish(topic, message)
        #print(f"[SENT] {topic}: {message}")


""" if __name__ == "__main__":
    BROKER = "host.docker.internal"
    TOPIC = "test/topic"

    publisher = MQTTPublisher(BROKER, [TOPIC])
    publisher.on_connect()
    
    csvData = ReadCSV('../dataset/agroDataset.csv')
    #publisher.send(TOPIC, "Hello World! from Publisher") """