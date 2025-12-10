from Kafka.TopicConsumer import ConsumerClient

def main():
    print("Starting Kafka consumer...")
    kafkaConsumer = ConsumerClient(
        "mqtt_topic"
    )
    kafkaConsumer.consumeData()

if __name__ == "__main__":
    main()