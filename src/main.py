from kafka.admin import NewTopic, KafkaAdminClient
from readData import readCSV
import logging

logging.basicConfig(
    level=logging.INFO
)

logger = logging.getLogger("[MAIN]")    

def createTopic(topic_name):
    TOPIC_NAME = topic_name
    adminClient = KafkaAdminClient(
        bootstrap_servers  = "localhost:9094",
        client_id = "Admin"
    )

    try:
        metadata = adminClient.list_topics()
        if topic_name not in metadata:
            topic = NewTopic(
                name = TOPIC_NAME,
                num_partitions = 3,
                replication_factor = 1
            )

            fs = adminClient.create_topics(
                    new_topics = [topic],
                    validate_only = False
                )
            
            for topic, future in fs.items():
                try: 
                    future.result()
                    logger.info(f"Topic: {TOPIC_NAME} has been successfully created!")
                except Exception as e:
                    logger.error(f"Error: {e}")
        else:
            logger.info(f"Topic {topic_name} already exists!");
            

    except Exception as e:
        logger.error(f"Error: {e}")

    finally:
        adminClient.close()

if __name__ == "__main__":
    print("HELLO")
    createTopic("TEST_TOPIC")
    readCSV()