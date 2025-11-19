from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType

SENSOR_DATA_STRUCT = {
    "averageChlorophyll": float,
    "heightRate": float,
    "averageWeightWet": float,
    "averageLeafArea": float,
    "averageLeafCount": float,
    "averageRootDiameter": float,
    "averageDryWeight": float
}

SPARK_SCHEMA_STRUCT = {
    float: FloatType(),
    int: IntegerType(),
    str: StringType()
}