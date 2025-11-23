from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType
from pyspark.sql.functions import from_json, col

def buildSparkSession():
    return (
        SparkSession.builder
        .master("spark://host.docker.internal:7077")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
        .appName("BigDataKafkaStreaming")
        .getOrCreate()
    )

def main():
    spark = buildSparkSession()

    # Kafka input stream
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9094")
        .option("subscribe", "SENSOR_DATA")
        .option("startingOffsets", "latest")
        .load()
    )

    json_df = df.selectExpr("CAST(value AS STRING)")

    schema = StructType([
        StructField("averageChlorophyll", FloatType()),
        StructField("heightRate", FloatType()),
        StructField("averageWeightWet", FloatType()),
        StructField("averageLeafArea", FloatType()),
        StructField("averageLeafCount", FloatType()),
        StructField("averageRootDiameter", FloatType()),
        StructField("averageDryWeight", FloatType())
    ])

    parsed = json_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    query = (
        parsed.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
    )

    print("Streaming beží a prijíma Kafka dáta...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
