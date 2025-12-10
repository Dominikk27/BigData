
from Kafka.TopicProducer import ProducerClient
from MQTT import MQTTClient

from Kafka.TopicConsumer import ConsumerClient

def main():
        
    #KAFKA PRODUCER
    kafka_producer = ProducerClient(
        BROKERS="localhost:9094",
        TOPIC="mqtt_topic"
    )

    # MQTT Topic Subscriber
    mqttClient = MQTTClient(
        broker="host.docker.internal",
        topics=["test/topic"],
        KAFKA_PRODUCER=kafka_producer

    )
    # Starting MQTT Topic Subscriber
    mqttClient.start()

if __name__ == "__main__":
    main()