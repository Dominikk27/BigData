from Streaming.SparkStream import SparkStream


def main():
    #print("Hello World Stream!")
    spark = SparkStream("spark://host.docker.internal",7077,"host.docker.internal:9094","test12","mqtt_topic")
    spark.buildSparkSession()
    spark.createDataStruct()

    dataFrame = spark.readDataFrame()
    parsed_data = spark.processStream(dataFrame)

    query = spark.writeStream(parsed_data)
    query.awaitTermination()
    

if __name__ == "__main__":
    main()