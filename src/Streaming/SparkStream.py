from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType
from pyspark.sql.functions import from_json, col


from utils.DataStruct import SENSOR_DATA_STRUCT, SPARK_SCHEMA_STRUCT


class SparkStream:
    def __init__(self, MASTER_IP, MASTER_PORT, KAFKA_BROKER, appName, TOPIC_NAME):
        self.masterURL = MASTER_IP
        self.masterPORT = MASTER_PORT
        self.appName = appName
        self.kafkaBroker = KAFKA_BROKER
        self.topicName = TOPIC_NAME
        self.sparkSession = None
    

    # CREATE SPARK SESSION
    def buildSparkSession(self):
        try:
            self.sparkSession = (
                SparkSession.builder
                .master(f"{self.masterURL}:{self.masterPORT}") #spark://host.docker.internal:7077
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
                .appName(self.appName)
                .getOrCreate()
            )
        except Exception as e:
            print(f"Failed to build Spark session! ERROR: {e}")
            raise e
        
        return self.sparkSession
    

    # SPARK DATA STRUCT (SCHEMA)
    def createDataStruct(self):
        self.dataStruct = StructType([
            StructField(name, SPARK_SCHEMA_STRUCT[dataType], True)
            for name, dataType in SENSOR_DATA_STRUCT.items()
        ])
        return self.dataStruct


    # READ KAFKA DATAFRAME
    def readDataFrame(self):
        try:
            dataFrame = (
                self.sparkSession.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.kafkaBroker)
                .option("subscribe", self.topicName)
                .option("startingOffsets", "earliest")
                .load()
            )
        except Exception as e:
            print(f"Error reading Kafka DataFrame ERROR: {e}")
            raise e
        
        return dataFrame


    # PROCESS STREAM DATA
    def processStream(self, dataFrame):
        raw_data = dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        parsed_data = raw_data.withColumn("value", from_json("value", schema=self.dataStruct)) \
        .select(col("key"), col("value"))

        return parsed_data
    

    def writeStream(self, dataFrame):
        try:
            query = (
                dataFrame.writeStream
                .outputMode("append")
                .format("console")
                .option("truncate", False)
                .start()
            )
        except Exception as e:
            print(f"Failed to write stream data! ERROR: {e}")
            raise e
        
        return query
        



def main():
    print("Hello World Stream!")

if __name__ == "__main__":
    main()
