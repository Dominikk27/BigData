import os





class AppConfig:

    MQTT_BROKER = os.getenv("MQTT_BROKER", "host.docker.internal")
    MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
    MQTT_TOPICS = ["device/+/data"]
    

    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
    KAFKA_BRIDGE_CLIENT_ID = os.getenv("KAFKA_BRIDGE_CLIENT_ID", "mqtt_kafka_bridge")
    #KAFKA_MQTT_TOPIC = os.getenv("KAFKA_MQTT_TOPIC", "mqtt_topic")
    KAFKA_TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "Device_")


    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "databasename")
    DB_USER = os.getenv("DB_USER", "username")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "passowrd")
