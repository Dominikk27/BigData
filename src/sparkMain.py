#from src.Spark.SparkStreamOld import SparkStream
from Spark.Spark import Spark

from Database import DatabaseManager

def main():
    #print("Hello World Stream!")

    dbManager = DatabaseManager()
    dbConn = dbManager.connect_database()

    spark = Spark("spark://host.docker.internal",7077,"host.docker.internal:9094","test12","Device_.*")
    spark.build_session()
    #spark.createDataStruct()

    dataFrame = spark.readDataFrame()
    #parsed_data = spark.processStream(dataFrame)

    parsedDF = spark.process_stream(dataFrame)
    analyseDF = spark.analyse_data(parsedDF)

    queryDB = (analyseDF.writeStream
                .foreachBatch(dbManager.insert_analysed_data,)
                .outputMode("update")
                .option("checkpointLocation", "./checkpoints/db_analytics")
                .start()
    )
    
    queryCONSOLE = spark.write_stream(analyseDF)
    queryDB.awaitTermination()
    queryCONSOLE.awaitTermination()
    

if __name__ == "__main__":
    main()