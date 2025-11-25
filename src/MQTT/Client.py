from typing import List

import time
import paho.mqtt.client as mqtt


from Kafka.TopicProducer import ProducerClient

#BROKER = "host.docker.internal"
#TOPIC = "test/topic"

class MQTTClient:
    def __init__(self, broker: str, topics: List[str], KAFKA_PRODUCER: ProducerClient):
        self.client = mqtt.Client()
        self.broker = broker
        self.port = 1883
        self.topics = topics
        self.keep_alive = 20
        self.kafka_producer = KAFKA_PRODUCER
    

    def on_connect(self, client, userdata, flags, rc):
        print("Connected with code:", rc)
        for topic in self.topics:
            client.subscribe(topic)
            print(f"Subscribing topic: {topic}")



    def on_message(self, client, userdata, msg):
        payload = msg.payload.decode()
        print(f"[RECIEVED] {msg.topic}: {msg.payload.decode()}")

        self.kafka_producer.produceData(
            key=msg.topic,
            data=payload
        )


    def start(self):
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.client.connect(self.broker, self.port, self.keep_alive)
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


""" if __name__ == "__main__":
    mqtt_client = MQTTClient(BROKER, [TOPIC])
    mqtt_client.start()  """