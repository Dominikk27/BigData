from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType
from pyspark.sql.functions import from_json, col

from pyspark.sql.streaming import StreamingQueryException

from utils.DataStruct import SENSOR_DATA_STRUCT, SPARK_SCHEMA_STRUCT

# =============================
# CREATE SPARK SESSION
# =============================
def buildSparkSession():
    try:
        spark = (
            SparkSession.builder
            .master("spark://host.docker.internal:7077")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
            .appName("BigDataKafkaStreaming")
            .getOrCreate()
        )
    except Exception as e:
        print(f"Error creating Spark Session: {e}")
        raise
    return spark

# =============================
# SPARK DATA STRUCT
# =============================
def createDataSchema():
    """ schema = StructType([
        StructField(name, SPARK_SCHEMA_STRUCT[dataType], True)
        for name, dataType in SENSOR_DATA_STRUCT.items()
    ]) """

    schema = StructType([
        StructField("averageChlorophyll", FloatType(), True),
        StructField("heightRate", FloatType(), True),
        StructField("averageWeightWet", FloatType(), True),
        StructField("averageLeafArea", FloatType(), True),
        StructField("averageLeafCount", FloatType(), True),
        StructField("averageRootDiameter", FloatType(), True),
        StructField("averageDryWeight", FloatType(), True)
    ])
    return schema


# =============================
# READ KAFKA DATA FRAME
# =============================
def readDataFrame(sparkSession, KAFKA_BROKER, TOPIC_NAME):
    try:
        df = (
        sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    except Exception as e:
        print(f"Error reading Kafka DataFrame: {e}")
        raise

    return df


# =============================
# PROCESS STREAM DATA
# =============================
def processStream(dataFrame, dataStruct):
    json_df = dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    structured_df = json_df.withColumn("value", from_json("value", schema=dataStruct)) \
    .select(col("key"), col("value.*"))
    #parsedDataFrame = json_df.select(from_json(col("value"), dataStruct).alias("data")).select("data.*")
    return structured_df


# =============================
# WRITE STREAM DATA
# =============================
def writeStream(dataFrame):
    try:
        query = (
            dataFrame.writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", False)
            .start()
        )
    except Exception as e:
        print(f"Error writing stream data: {e}")
        raise
    return query

def main():
    sparkSession = buildSparkSession()

    dataStruct = createDataSchema()

    readDF = readDataFrame(sparkSession, "localhost:9094", "mqtt_topic")


    raw_df = readDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    raw_query = raw_df.writeStream \
        .format("console") \
        .option("truncate", False) \
        .option("checkpointLocation", "../docker/spark_checkpoints") \
        .start()

    print("🔹 Toto sú raw Kafka dáta (pred parsovaním JSON)")
    raw_query.awaitTermination(10)  # spustí 10 sekúnd, potom zastaví
    raw_query.stop()


    #json_df = readDF.selectExpr("CAST(value AS STRING)")


    # DATA STURUCTURE SCHEMA
    """ schema = StructType([
        # StructField("averageChlorophyll", FloatType()),
        # StructField("heightRate", FloatType()),
        # StructField("averageWeightWet", FloatType()),
        # StructField("averageDryWeight", FloatType())
        # StructField("averageLeafArea", FloatType()),
        # StructField("averageLeafCount", FloatType()),
        # StructField("averageRootDiameter", FloatType()),
        StructField(name, SPARK_SCHEMA_STRUCT[dataType], True)
        for name, dataType in SENSOR_DATA_STRUCT.items()

    ]) """

    #print(dataStruct)

    #parsed = json_df.select(from_json(col("value"), dataStruct).alias("data")).select("data.*")

    parsedData = processStream(readDF, dataStruct)

    """ 
        query = (
            parsedData.writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", False)
            .start()
        )
    """
    try:
        query = writeStream(parsedData)
        query.awaitTermination()
    except StreamingQueryException as e:
        print(f"Streaming query error: {e}")
        raise   
    except Exception as e:
        print(f"Error in streaming process: {e}")
        raise

    #print("STREAM STARTED! LISTENING FOR DATA...")
    #query.awaitTermination()

if __name__ == "__main__":
    main()
