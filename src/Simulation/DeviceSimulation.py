import time
import json

class DeviceSimulator:
    def __init__(self, mqtt_publisher):
        self.mqtt_publisher = mqtt_publisher

    def start_simulation(self, device):
        map_id = {}
        for sensor in device.sensors:
            key = (sensor['measurement_type'], sensor.get('depth_cm'))
            map_id[key] = sensor.get('db_id')

            print(f"[SIMULATION] SENSOR MAP: {map_id}")

            