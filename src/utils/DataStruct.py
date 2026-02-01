from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType, TimestampType


# DEFINE SPARK DATA SCHEMA
SPARK_SCHEMA_STRUCT = {
    float: FloatType(),
    int: IntegerType(),
    str: StringType()
}

KAFKA_SENSOR_SCHEMA = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("value", FloatType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("device_code", StringType(), True)
])