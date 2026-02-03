from Kafka import ConsumerClient

def main():
    print("Starting Kafka consumer...")
    
    topics = ["Device_URB", "Device_SSA3", "Device_SSA4", "Device_WeatherStation"]
    
    kafkaConsumer = ConsumerClient(TOPICS=topics)
    try:
        kafkaConsumer.consumeData() 
    except Exception as e:
        print("Failed to create Kafka consumer:", e)
        return

if __name__ == "__main__":
    main()