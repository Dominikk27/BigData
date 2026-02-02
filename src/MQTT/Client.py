from typing import List
import json

import time
import paho.mqtt.client as mqtt


from Kafka.TopicProducer import ProducerClient

#BROKER = "host.docker.internal"
#TOPIC = "test/topic"

class MQTTClient:
    def __init__(self, broker: str, topics: List[str], KAFKA_PRODUCER: ProducerClient):
        self.client = mqtt.Client()
        self.broker = broker
        self.topics = topics
        self.port = 1883
        self.keep_alive = 20
        self.kafka_producer = KAFKA_PRODUCER
    

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"[MQTT CONNECTED] Broker: {self.broker}, result code: {rc}")
            for topic in self.topics:
                client.subscribe(topic)
                print(f"[SUBSCRIBED] to topic: {topic}")
        else:
            print(f"[MQTT CONNECTION FAILED] Broker: {self.broker}, result code: {rc}")


    def on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode()

            full_topic = msg.topic.split('/')
            if len(full_topic) >= 2:
                device_id = full_topic[1]
                kafka_topic = f"Device_{device_id}"
                
            print(f"[RECIEVED] {msg.topic}: {msg.payload.decode()}")
            print(f"[FORWARDING TO KAFKA] Topic: {kafka_topic}")

            self.kafka_producer.produceData(
                key=device_id,
                topic=kafka_topic,
                data=json.loads(payload)
            )
        except Exception as e:
            print(f"[MQTT MESSAGE ERROR] {e}")
            return


    def start(self):
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.client.connect(self.broker, self.port, self.keep_alive)
        self.client.loop_forever()



""" if __name__ == "__main__":
    mqtt_client = MQTTClient(BROKER, [TOPIC])
    mqtt_client.start()  """
