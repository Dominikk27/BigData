from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType


# DEFINE SENSOR DATA STRUCTURE
SENSOR_DATA_STRUCT = {
    "averageChlorophyll": float,
    "heightRate": float,
    "averageWeightWet": float,
    "averageLeafArea": float,
    "averageLeafCount": float,
    "averageRootDiameter": float,
    "averageDryWeight": float
}


# DEFINE SPARK DATA SCHEMA
SPARK_SCHEMA_STRUCT = {
    float: FloatType(),
    int: IntegerType(),
    str: StringType()

}

# DEFINE TABLE COLUMNS TO BE USED FROM CSV
USE_COLS = [
    "ID", 
    "averageChlorophyll", 
    "heightRate", 
    "averageWeightWet", 
    "averageLeafArea", 
    "averageLeafCount", 
    "averageRootDiameter", 
    "averageDryWeight"
]
