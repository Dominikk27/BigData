from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType, ArrayType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col, explode, lit


class Spark:
    def __init__(self, MASTER_IP, MASTER_PORT, KAFKA_BROKER, APP_NAME, TOPICS):
        self.masterIP = MASTER_IP
        self.masterPort = MASTER_PORT
        self.kafkaBroker = KAFKA_BROKER
        self.appName = APP_NAME
        self.topics = TOPICS

        self.sparkSession = None

    #BUILD SESSION
    def build_session(self):
        try:
            self.sparkSession = (SparkSession.builder
                             .master(f"{self.masterIP}:{self.masterPort}")
                             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
                             .config("spark.network.timeout", "600s")
                             .config("spark.executor.heartbeatInterval", "30s")
                             .appName(self.appName)
                             .getOrCreate())
        except Exception as e:
            print("Failed to build SparkSession!")
            raise e
        
        return self.sparkSession


    # READ KAFKA DATAFRAME
    def readDataFrame(self):
        try:
            dataFrame = (
                self.sparkSession.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.kafkaBroker)
                .option("kafka.security.protocol", "PLAINTEXT")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("subscribePattern", self.topics)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load()
            )
        except Exception as e:
            print(f"Error reading Kafka DataFrame ERROR: {e}")
            raise e
        
        return dataFrame


    def process_stream(self, df):
        
        measurements_schema = StructType([
            StructField("sensor_id", IntegerType(), True),
            StructField("value", FloatType(), True),
            StructField("status", IntegerType(), True)
        ])
        
        batch_schema = StructType([
            StructField("device_code", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("measurements", ArrayType(measurements_schema), True)
        ])

        
        parsed_dataFrame = df.selectExpr("CAST(value AS STRING) as json_payload", "topic") \
                             .withColumn("data", from_json(col("json_payload"), batch_schema)) \
                             .select("data.*", "topic")
        
        final_df = parsed_dataFrame.withColumn("measurements", explode(col("measurements"))) \
                                   .select(
                                       col("timestamp").cast(TimestampType()),
                                       col("measurements.sensor_id").alias("sensor_id"),
                                       col("measurements.value").alias("measurements")
                                   )
        
        return final_df.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("checkpointLocation", "/tmp/checkpoints") \
                .start()
        
        
