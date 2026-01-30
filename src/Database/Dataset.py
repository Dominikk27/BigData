import pandas as pd
from pathlib import Path
import os


class DeviceDataset:
    def __init__(self, device_name):
        self.rows = []
        self.device_name = device_name
        
        self.dataFrame = self._load_dataset()
    

    def _load_dataset(self):
        BASE_DIR = Path(__file__).resolve().parent.parent.parent
        absolute_dataset_path = BASE_DIR / "dataset" / "preparedData" / f"{self.device_name.lower()}_stream.csv"
        if os.path.exists(absolute_dataset_path):
            dataFrame = pd.read_csv(absolute_dataset_path)

            dataFrame["timestamp"] = pd.to_datetime(dataFrame["timestamp"])
            dataFrame["value"] = pd.to_numeric(dataFrame["value"], errors='coerce')

            if "sensor_index" in dataFrame:
                dataFrame["sensor_index"] = dataFrame["sensor_index"].astype("Int64")

            if "depth_cm" in dataFrame:
                dataFrame["depth_cm"] = dataFrame["depth_cm"].astype("Int64")

            return dataFrame.sort_values("timestamp").reset_index(drop=True)


        else:
            print(f"Dataset for device {self.device_name} not found at path: {absolute_dataset_path}")


    def iter_rows(self):
        for _, row in self.dataFrame.iterrows():
            yield row