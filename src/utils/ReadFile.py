
import os
import re
import json
from pathlib import Path


class DatasetManager:
    def __init__(self):

        BASE_DIR = Path(__file__).resolve().parent.parent.parent
        self.folder_path = BASE_DIR / "dataset" / "preparedData"
        self.datasets = {}


    
    def load_dataset(self, device_code):

        device_code = device_code.lower()
        
        if device_code in self.datasets:
            return self.datasets[device_code]
        
        #file_path = os.path.join(self.folder_path, f"{device_code}.jsonl")

        file_path = self.folder_path / f"{device_code}.jsonl"

        print(f"TOTOT JE CESTA K DATASETU>>>>>>>> {file_path}")
        if not os.path.exists(file_path):
            print(f"Dataset file for device {device_code} not found at {file_path}")
            return None
           
                                      
        try:
            data = []
            with open(file_path, "r", encoding="utf-8") as file:
                for line in file:
                    if line.strip():
                        record = json.loads(line)
                        data.append(record)
            
            self.datasets[device_code] = data
            print(f"Loaded dataset for device {device_code} with {len(data)} records.")
            return data
        
        except Exception as e:
            print(f"Error loading dataset for device {device_code}: {e}")
            return None
    

    """     
    def get_data(self, device_code):
        return self.datasets.get(device_code, None)
    """