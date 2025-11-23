from typing import List

import time
import paho.mqtt.client as mqtt

BROKER = "host.docker.internal"
TOPIC = "test/topic"

class MQTTClient:
    def __init__(self, broker: str, topics: List[str]):
        self.client = mqtt.Client()
        self.broker = broker
        self.port = 1883
        self.topics = topics
    

    def on_connect(self, client, userdata, flags, rc):
        print("Connected with code:", rc)
        for topic in self.topics:
            client.subscribe(topic)
            print(f"Subscribing topic: {topic}")

    def on_message(self, client, userdata, msg):
        print(f"[RECV] {msg.topic}: {msg.payload.decode()}")


    def start(self):
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()

        try:
            while True:
                #selfclient.send("test/topic", "Hello MQTT")
                time.sleep(1)
        except KeyboardInterrupt:
            print("Closing session...")
        finally:
            self.client.loop_stop()
            self.client.disconnect()


if __name__ == "__main__":
    mqtt_client = MQTTClient(BROKER, [TOPIC])
    mqtt_client.start() 