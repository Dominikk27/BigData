import pandas
import time
import sys, os

from Producers.producers import produceData

from utils.DataStruct import SENSOR_DATA_STRUCT   

import logging

sys.path.append(os.path.abspath(os.path.dirname(__file__)))



TOPIC = "SENSOR_DATA"
USE_COLS = ["ID", "averageChlorophyll", "heightRate", "averageWeightWet", "averageLeafArea", "averageLeafCount", "averageRootDiameter", "averageDryWeight"]

logger = logging.getLogger("READ-DATA Logger")

def readCSV():
    try:
        file = pandas.read_csv(
            '../dataset/agroDataset.csv',
            index_col = "ID",
            usecols = USE_COLS,
            dtype={
                  name: 'Float64' for name in SENSOR_DATA_STRUCT.keys()
            }
        )
    except Exception as e:
        logger.error(f"Error: {e}")
        return
    
    file = file.where(pandas.notnull(file), None)
    
    for ID, row in file.iterrows():
        #print(row.to_dict())
        data = row.to_dict()
        
        data = {k: round(v, 4) if isinstance(v, float) else v for k, v in data.items()}

        try:
            produceData(TOPIC, key=ID, data=data)
        except Exception as e:
            logger.error(f"Error: {e}")
        time.sleep(1)