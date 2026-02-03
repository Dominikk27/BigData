from MQTT import MQTTBridge

def main():
    bridge = MQTTBridge(
        mqtt_broker="host.docker.internal",
        kafka_brokers="localhost:9094")
    
    bridge.start()

if __name__ == "__main__":
    main()