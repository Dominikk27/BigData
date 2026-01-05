import pandas as pd
import os

class DeviceDataset:
    def __init__(self, device_name):
        self.rows = []
        self.device_name = device_name
        
        self.dataFrame = self._load_dataset()
    

    def _load_dataset(self):
        path = f"../../dataset/{self.device_name.lower()}_stream.csv"

        if os.path.exists(path):
            dataFrame = pd.read_csv(path)

            dataFrame["timestamp"] = pd.to_datetime(dataFrame["timestamp"])
            dataFrame["value"] = pd.to_numeric(dataFrame["value"], errors='coerce')


        else:
            print(f"Dataset for device {self.device_name} not found at path: {path}")
