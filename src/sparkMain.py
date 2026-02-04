#from src.Spark.SparkStreamOld import SparkStream
from Spark.Spark import Spark

def main():
    #print("Hello World Stream!")
    spark = Spark("spark://host.docker.internal",7077,"host.docker.internal:9094","test12","Device_.*")
    spark.build_session()
    #spark.createDataStruct()

    dataFrame = spark.readDataFrame()
    #parsed_data = spark.processStream(dataFrame)

    query = spark.process_stream(dataFrame)
    query.awaitTermination()
    

if __name__ == "__main__":
    main()