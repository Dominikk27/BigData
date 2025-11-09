import pandas
import time
from producers import produceData

import logging

TOPIC = "TEST_TOPIC"
USE_COLS = ["ID", "averageChlorophyll", "heightRate", "averageWeightWet", "averageLeafArea", "averageLeafCount", "averageRootDiameter", "averageDryWeight"]

logger = logging.getLogger("READ-DATA Logger")

def readCSV():
    try:
        file = pandas.read_csv(
            '../dataset/agroDataset.csv',
            index_col = "ID",
            usecols = USE_COLS,
            dtype={
                'averageChlorophyll': 'Float64',
                'heightRate': 'Float64',
                'averageWeightWet': 'Float64',
                'averageLeafArea': 'Float64',
                'averageLeafCount': 'Float64',
                'averageRootDiameter': 'Float64',
                'averageDryWeight': 'Float64'   
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