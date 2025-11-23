import pandas
from utils.DataStruct import USE_COLS, SENSOR_DATA_STRUCT

def read_csv(file_path):
    try:
        data = pandas.read_csv(
            file_path,
            index_col = "ID",
            usecols=USE_COLS,
            dtype={
                name: 'Float64' for name in SENSOR_DATA_STRUCT.keys()
            }
        )
        return data
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None
    
    data = data.where(pandas.notnull(data), None)
    return data
 

