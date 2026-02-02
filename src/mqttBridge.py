from MQTT import MQTTBridge


#from Kafka.TopicProducer import ProducerClient
#from Kafka.TopicConsumer import ConsumerClient

def main():
    bridge = MQTTBridge(
        mqtt_broker="host.docker.internal",
        kafka_brokers="localhost:9094")
    
    bridge.start()

if __name__ == "__main__":
    main()